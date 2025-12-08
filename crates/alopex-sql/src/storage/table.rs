use alopex_core::kv::KVTransaction;
use alopex_core::types::{Key, Value};

use crate::catalog::TableMetadata;

use super::error::{Result, StorageError};
use super::{KeyEncoder, RowCodec, SqlValue};

/// TableStorage provides CRUD and scan operations over a table backed by a KV store.
pub struct TableStorage<'a, T: KVTransaction<'a>> {
    txn: &'a mut T,
    table_meta: TableMetadata,
    table_id: u32,
}

impl<'a, T: KVTransaction<'a>> TableStorage<'a, T> {
    /// Create a new TableStorage wrapper for a given table.
    pub fn new(txn: &'a mut T, table_meta: &TableMetadata, table_id: u32) -> Self {
        Self {
            txn,
            table_meta: table_meta.clone(),
            table_id,
        }
    }

    /// Insert a row by RowID, enforcing primary key and NOT NULL constraints.
    pub fn insert(&mut self, row_id: u64, row: &[SqlValue]) -> Result<()> {
        self.validate_row(row)?;
        let key = self.row_key(row_id);

        if self.txn.get(&key)?.is_some() {
            return Err(StorageError::PrimaryKeyViolation {
                table_id: self.table_id,
                row_id,
            });
        }

        let encoded = RowCodec::encode(row);
        self.txn.put(key, encoded)?;
        Ok(())
    }

    /// Get a row by RowID.
    pub fn get(&mut self, row_id: u64) -> Result<Option<Vec<SqlValue>>> {
        let key = self.row_key(row_id);
        match self.txn.get(&key)? {
            Some(value) => {
                let row = RowCodec::decode(&value)?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    /// Update an existing row by RowID.
    pub fn update(&mut self, row_id: u64, row: &[SqlValue]) -> Result<()> {
        self.validate_row(row)?;
        let key = self.row_key(row_id);
        if self.txn.get(&key)?.is_none() {
            return Err(StorageError::CorruptedData {
                reason: "row not found for update".into(),
            });
        }
        let encoded = RowCodec::encode(row);
        self.txn.put(key, encoded)?;
        Ok(())
    }

    /// Delete a row by RowID.
    pub fn delete(&mut self, row_id: u64) -> Result<()> {
        let key = self.row_key(row_id);
        self.txn.delete(key)?;
        Ok(())
    }

    /// Scan all rows in the table.
    pub fn scan(&mut self) -> Result<TableScanIterator<'_>> {
        let prefix = KeyEncoder::table_prefix(self.table_id);
        let table_id = self.table_id;
        let inner = self.txn.scan_prefix(&prefix)?;
        Ok(TableScanIterator::new(inner, table_id))
    }

    /// Scan rows in the half-open RowID range [start_row_id, end_row_id].
    pub fn range_scan(&mut self, start_row_id: u64, end_row_id: u64) -> Result<TableScanIterator<'_>> {
        let start = KeyEncoder::row_key(self.table_id, start_row_id);
        let end = if end_row_id == u64::MAX {
            KeyEncoder::table_prefix(self.table_id.saturating_add(1))
        } else {
            KeyEncoder::row_key(self.table_id, end_row_id.saturating_add(1))
        };
        let table_id = self.table_id;
        let inner = self.txn.scan_range(&start, &end)?;
        Ok(TableScanIterator::new(inner, table_id))
    }

    /// Get the next auto-increment RowID, incrementing the stored sequence.
    pub fn next_row_id(&mut self) -> Result<u64> {
        let seq_key = KeyEncoder::sequence_key(self.table_id);
        let current = self
            .txn
            .get(&seq_key)?
            .map(|bytes| {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes);
                u64::from_be_bytes(arr)
            })
            .unwrap_or(0);
        let next = current.saturating_add(1);
        self.txn.put(seq_key, next.to_be_bytes().to_vec())?;
        Ok(next)
    }

    fn validate_row(&self, row: &[SqlValue]) -> Result<()> {
        let expected = self.table_meta.column_count();
        if row.len() != expected {
            return Err(StorageError::TypeMismatch {
                expected: format!("{} columns", expected),
                actual: format!("{} columns", row.len()),
            });
        }

        for (idx, col) in self.table_meta.columns.iter().enumerate() {
            if (col.not_null || col.primary_key) && row[idx].is_null() {
                return Err(StorageError::NullConstraintViolation {
                    column: col.name.clone(),
                });
            }
        }
        Ok(())
    }

    fn row_key(&self, row_id: u64) -> Key {
        KeyEncoder::row_key(self.table_id, row_id)
    }

}

/// Iterator over table rows that lazily decodes RowCodec.
pub struct TableScanIterator<'a> {
    inner: Box<dyn Iterator<Item = (Key, Value)> + 'a>,
    table_id: u32,
}

impl<'a> TableScanIterator<'a> {
    fn new(inner: Box<dyn Iterator<Item = (Key, Value)> + 'a>, table_id: u32) -> Self {
        Self { inner, table_id }
    }
}

impl<'a> Iterator for TableScanIterator<'a> {
    type Item = Result<(u64, Vec<SqlValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(key, value)| {
            let (table_id, row_id) = KeyEncoder::decode_row_key(&key)?;
            if table_id != self.table_id {
                return Err(StorageError::InvalidKeyFormat);
            }
            let row = RowCodec::decode(&value)?;
            Ok((row_id, row))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::kv::memory::MemoryKV;
    use alopex_core::kv::KVStore;
    use alopex_core::types::TxnMode;
    use crate::planner::types::ResolvedType;

    fn sample_table_meta() -> TableMetadata {
        TableMetadata::new(
            "users",
            vec![
                crate::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                    .with_primary_key(true)
                    .with_not_null(true),
                crate::catalog::ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
                crate::catalog::ColumnMetadata::new("age", ResolvedType::Integer),
            ],
        )
    }

    fn with_table<F>(store: &MemoryKV, meta: &TableMetadata, f: F)
    where
        F: FnOnce(&mut TableStorage<'static, <MemoryKV as alopex_core::kv::KVStore>::Transaction<'static>>),
    {
        let store_static: &'static MemoryKV = Box::leak(Box::new(store.clone()));
        let txn = store_static.begin(TxnMode::ReadWrite).unwrap();
        let txn_static: &'static mut _ = Box::leak(Box::new(txn));
        let mut table = TableStorage::new(txn_static, meta, 1);
        f(&mut table);
    }

    #[test]
    fn insert_and_get_roundtrip() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            let row = vec![
                SqlValue::Integer(1),
                SqlValue::Text("alice".into()),
                SqlValue::Integer(20),
            ];
            table.insert(1, &row).unwrap();
            let fetched = table.get(1).unwrap().unwrap();
            assert_eq!(fetched, row);
        });
    }

    #[test]
    fn duplicate_primary_key_is_rejected() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            let row = vec![
                SqlValue::Integer(1),
                SqlValue::Text("alice".into()),
                SqlValue::Integer(20),
            ];
            table.insert(1, &row).unwrap();
            let err = table.insert(1, &row).unwrap_err();
            matches!(err, StorageError::PrimaryKeyViolation { .. });
        });
    }

    #[test]
    fn not_null_constraint_is_enforced() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            let row = vec![SqlValue::Null, SqlValue::Text("bob".into()), SqlValue::Integer(30)];
            let err = table.insert(2, &row).unwrap_err();
            matches!(err, StorageError::NullConstraintViolation { .. });
        });
    }

    #[test]
    fn update_overwrites_existing_row() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            let row1 = vec![
                SqlValue::Integer(1),
                SqlValue::Text("alice".into()),
                SqlValue::Integer(20),
            ];
            table.insert(1, &row1).unwrap();

            let row2 = vec![
                SqlValue::Integer(1),
                SqlValue::Text("alice-updated".into()),
                SqlValue::Integer(25),
            ];
            table.update(1, &row2).unwrap();
            let fetched = table.get(1).unwrap().unwrap();
            assert_eq!(fetched, row2);
        });
    }

    #[test]
    fn delete_removes_row() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            let row = vec![
                SqlValue::Integer(1),
                SqlValue::Text("alice".into()),
                SqlValue::Integer(20),
            ];
            table.insert(1, &row).unwrap();
            table.delete(1).unwrap();
            assert!(table.get(1).unwrap().is_none());
        });
    }

    #[test]
    fn scan_returns_all_rows_in_order() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            for i in 1..=3 {
                let row = vec![
                    SqlValue::Integer(i as i32),
                    SqlValue::Text(format!("user{i}")),
                    SqlValue::Integer(10 + i as i32),
                ];
                table.insert(i, &row).unwrap();
            }

            let rows: Vec<_> = table
                .scan()
                .unwrap()
                .map(|res| res.unwrap().0)
                .collect();
            assert_eq!(rows, vec![1, 2, 3]);
        });
    }

    #[test]
    fn range_scan_respects_bounds() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            for i in 1..=5 {
                let row = vec![
                    SqlValue::Integer(i as i32),
                    SqlValue::Text(format!("user{i}")),
                    SqlValue::Integer(10 + i as i32),
                ];
                table.insert(i, &row).unwrap();
            }

            let rows: Vec<_> = table
                .range_scan(2, 4)
                .unwrap()
                .map(|res| res.unwrap().0)
                .collect();
            assert_eq!(rows, vec![2, 3, 4]);
        });
    }

    #[test]
    fn next_row_id_increments_sequence() {
        let store = MemoryKV::new();
        let meta = sample_table_meta();
        with_table(&store, &meta, |table| {
            let id1 = table.next_row_id().unwrap();
            let id2 = table.next_row_id().unwrap();
            assert_eq!((id1, id2), (1, 2));
        });
    }
}
