use std::convert::TryInto;
use std::marker::PhantomData;

use alopex_core::kv::KVTransaction;
use alopex_core::types::{Key, Value};

use super::error::{Result, StorageError};
use super::{KeyEncoder, SqlValue};

/// IndexStorage manages secondary index entries and lookups.
///
/// The two lifetime parameters serve distinct purposes:
/// - `'a`: The borrow duration of the transaction reference
/// - `'txn`: The lifetime parameter of the KVTransaction type itself
///
/// This separation is necessary to allow SqlTransaction to return IndexStorage
/// instances while maintaining proper lifetime relationships with GATs.
pub struct IndexStorage<'a, 'txn, T: KVTransaction<'txn>> {
    txn: &'a mut T,
    index_id: u32,
    unique: bool,
    column_indices: Vec<usize>,
    _txn_lifetime: PhantomData<&'txn ()>,
}

impl<'a, 'txn, T: KVTransaction<'txn>> IndexStorage<'a, 'txn, T> {
    /// Create a new IndexStorage for the given index definition.
    pub fn new(txn: &'a mut T, index_id: u32, unique: bool, column_indices: Vec<usize>) -> Self {
        Self {
            txn,
            index_id,
            unique,
            column_indices,
            _txn_lifetime: PhantomData,
        }
    }

    /// Insert an index entry for the provided row values and RowID.
    pub fn insert(&mut self, row: &[SqlValue], row_id: u64) -> Result<()> {
        let values = self.extract_values(row)?;
        let key = self.build_key(&values, row_id)?;
        if self.unique {
            let prefix = self.value_prefix(&values)?;
            self.ensure_unique(&prefix, row_id)?;
        }
        self.txn.put(key, Vec::new())?;
        Ok(())
    }

    /// Delete an index entry associated with the provided row values and RowID.
    pub fn delete(&mut self, row: &[SqlValue], row_id: u64) -> Result<()> {
        let values = self.extract_values(row)?;
        let key = self.build_key(&values, row_id)?;
        self.txn.delete(key)?;
        Ok(())
    }

    /// Equality lookup for single-column index.
    pub fn lookup(&mut self, value: &SqlValue) -> Result<Vec<u64>> {
        if self.column_indices.len() != 1 {
            return Err(StorageError::TypeMismatch {
                expected: "single-column index".into(),
                actual: format!("{} columns", self.column_indices.len()),
            });
        }
        self.lookup_internal(std::slice::from_ref(value))
    }

    /// Equality lookup for composite index.
    pub fn lookup_composite(&mut self, values: &[SqlValue]) -> Result<Vec<u64>> {
        if values.len() != self.column_indices.len() {
            return Err(StorageError::TypeMismatch {
                expected: format!("{} values", self.column_indices.len()),
                actual: format!("{} values", values.len()),
            });
        }
        self.lookup_internal(values)
    }

    /// Range lookup for single-column index.
    pub fn range_scan(
        &mut self,
        start: Option<&SqlValue>,
        end: Option<&SqlValue>,
        start_inclusive: bool,
        end_inclusive: bool,
    ) -> Result<IndexScanIterator<'_>> {
        // Caller contract: SQL BETWEEN a AND b should pass start_inclusive=true, end_inclusive=true
        // so the range aligns with SQL semantics. Exclusive bounds are available for internal use
        // (e.g., >, <) but should not be used for BETWEEN.
        if self.column_indices.len() != 1 {
            return Err(StorageError::TypeMismatch {
                expected: "single-column index".into(),
                actual: format!("{} columns", self.column_indices.len()),
            });
        }
        let (start_key, end_key) = self.range_bounds(start, end, start_inclusive, end_inclusive)?;
        let index_id = self.index_id;
        let inner = self.txn.scan_range(&start_key, &end_key)?;
        Ok(IndexScanIterator::new(inner, index_id))
    }

    fn lookup_internal(&mut self, values: &[SqlValue]) -> Result<Vec<u64>> {
        let prefix = self.value_prefix(values)?;
        let iter = self.txn.scan_prefix(&prefix)?;
        iter.map(|(key, _)| extract_row_id(&key, self.index_id))
            .collect()
    }

    fn ensure_unique(&mut self, prefix: &[u8], row_id: u64) -> Result<()> {
        let iter = self.txn.scan_prefix(prefix)?;
        for (key, _) in iter {
            let existing = extract_row_id(&key, self.index_id)?;
            if existing != row_id {
                return Err(StorageError::UniqueViolation {
                    index_id: self.index_id,
                });
            }
        }
        Ok(())
    }

    fn extract_values(&self, row: &[SqlValue]) -> Result<Vec<SqlValue>> {
        let max_index = self.column_indices.iter().copied().max().unwrap_or(0);
        if row.len() <= max_index {
            return Err(StorageError::TypeMismatch {
                expected: format!("row with >= {} columns", max_index + 1),
                actual: format!("{} columns", row.len()),
            });
        }
        Ok(self
            .column_indices
            .iter()
            .map(|idx| row[*idx].clone())
            .collect())
    }

    fn build_key(&self, values: &[SqlValue], row_id: u64) -> Result<Key> {
        if self.column_indices.len() == 1 {
            KeyEncoder::index_key(self.index_id, &values[0], row_id)
        } else {
            KeyEncoder::composite_index_key(self.index_id, values, row_id)
        }
    }

    fn value_prefix(&self, values: &[SqlValue]) -> Result<Vec<u8>> {
        if self.column_indices.len() == 1 {
            KeyEncoder::index_value_prefix(self.index_id, &values[0])
        } else {
            KeyEncoder::composite_index_prefix(self.index_id, values)
        }
    }

    fn range_bounds(
        &self,
        start: Option<&SqlValue>,
        end: Option<&SqlValue>,
        start_inclusive: bool,
        end_inclusive: bool,
    ) -> Result<(Key, Key)> {
        let start_key = match start {
            Some(value) if start_inclusive => KeyEncoder::index_key(self.index_id, value, 0)?,
            Some(value) => KeyEncoder::index_key(self.index_id, value, u64::MAX)?,
            None => KeyEncoder::index_prefix(self.index_id),
        };

        let end_key = match end {
            Some(value) if end_inclusive => {
                let mut prefix = KeyEncoder::index_value_prefix(self.index_id, value)?;
                // Ensure the exclusive upper bound sits after any RowID for the value.
                prefix.extend_from_slice(&[0xFF; 9]);
                prefix
            }
            Some(value) => KeyEncoder::index_key(self.index_id, value, 0)?,
            None if self.index_id == u32::MAX => vec![0x03],
            None => KeyEncoder::index_prefix(self.index_id.saturating_add(1)),
        };

        Ok((start_key, end_key))
    }
}

fn extract_row_id(key: &[u8], expected_index: u32) -> Result<u64> {
    if key.len() < 1 + 4 + 8 || key[0] != 0x02 {
        return Err(StorageError::InvalidKeyFormat);
    }
    let index_id = u32::from_be_bytes(key[1..5].try_into().unwrap());
    if index_id != expected_index {
        return Err(StorageError::InvalidKeyFormat);
    }
    let row_id_pos = key.len().saturating_sub(8);
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&key[row_id_pos..]);
    Ok(u64::from_be_bytes(buf))
}

/// Iterator over index entries that yields RowIDs.
pub struct IndexScanIterator<'a> {
    inner: Box<dyn Iterator<Item = (Key, Value)> + 'a>,
    index_id: u32,
}

impl<'a> IndexScanIterator<'a> {
    fn new(inner: Box<dyn Iterator<Item = (Key, Value)> + 'a>, index_id: u32) -> Self {
        Self { inner, index_id }
    }
}

impl<'a> Iterator for IndexScanIterator<'a> {
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(key, _)| extract_row_id(&key, self.index_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::kv::KVStore;
    use alopex_core::kv::memory::MemoryKV;
    use alopex_core::types::TxnMode;

    fn with_index<F>(unique: bool, column_indices: Vec<usize>, f: F)
    where
        F: FnOnce(&mut IndexStorage<'static, 'static, <MemoryKV as KVStore>::Transaction<'static>>),
    {
        let store = MemoryKV::new();
        let store_static: &'static MemoryKV = Box::leak(Box::new(store));
        let txn = store_static.begin(TxnMode::ReadWrite).unwrap();
        let txn_static: &'static mut _ = Box::leak(Box::new(txn));
        let mut index = IndexStorage::new(txn_static, 10, unique, column_indices);
        f(&mut index);
    }

    #[test]
    fn insert_lookup_and_delete_single_column() {
        with_index(false, vec![0], |index| {
            let row1 = vec![SqlValue::Integer(1), SqlValue::Text("a".into())];
            let row2 = vec![SqlValue::Integer(2), SqlValue::Text("b".into())];
            index.insert(&row1, 100).unwrap();
            index.insert(&row2, 200).unwrap();

            let mut results = index.lookup(&SqlValue::Integer(1)).unwrap();
            results.sort();
            assert_eq!(results, vec![100]);

            let mut range_ids = {
                let iter = index
                    .range_scan(
                        Some(&SqlValue::Integer(1)),
                        Some(&SqlValue::Integer(3)),
                        true,
                        false,
                    )
                    .unwrap();
                iter.collect::<Result<Vec<_>>>().unwrap()
            };
            range_ids.sort();
            assert_eq!(range_ids, vec![100, 200]);

            index.delete(&row1, 100).unwrap();
            assert!(index.lookup(&SqlValue::Integer(1)).unwrap().is_empty());
        });
    }

    #[test]
    fn unique_constraint_blocks_duplicates() {
        with_index(true, vec![0], |index| {
            let row = vec![SqlValue::Text("alice".into())];
            index.insert(&row, 1).unwrap();
            let err = index.insert(&row, 2).unwrap_err();
            matches!(err, StorageError::UniqueViolation { .. });
        });
    }

    #[test]
    fn composite_lookup_returns_matching_row_ids() {
        with_index(false, vec![0, 1], |index| {
            let row1 = vec![SqlValue::Text("tokyo".into()), SqlValue::Integer(1)];
            let row2 = vec![SqlValue::Text("tokyo".into()), SqlValue::Integer(2)];
            let row3 = vec![SqlValue::Text("osaka".into()), SqlValue::Integer(1)];
            index.insert(&row1, 10).unwrap();
            index.insert(&row2, 20).unwrap();
            index.insert(&row3, 30).unwrap();

            let ids = index
                .lookup_composite(&[SqlValue::Text("tokyo".into()), SqlValue::Integer(2)])
                .unwrap();
            assert_eq!(ids, vec![20]);
        });
    }

    #[test]
    fn range_scan_respects_inclusive_and_exclusive_bounds() {
        with_index(false, vec![0], |index| {
            for (i, val) in [1, 2, 3, 4, 5].iter().enumerate() {
                let row = vec![SqlValue::Integer(*val)];
                index.insert(&row, (i + 1) as u64).unwrap();
            }

            let ids = {
                let iter = index
                    .range_scan(
                        Some(&SqlValue::Integer(2)),
                        Some(&SqlValue::Integer(4)),
                        false,
                        true,
                    )
                    .unwrap();
                iter.collect::<Result<Vec<_>>>().unwrap()
            };
            // start_exclusive => 3, end_inclusive => include 4
            assert_eq!(ids, vec![3, 4]);
        });
    }

    #[test]
    fn between_semantics_are_inclusive_on_both_ends() {
        with_index(false, vec![0], |index| {
            for (i, val) in [10, 20, 30, 40].iter().enumerate() {
                let row = vec![SqlValue::Integer(*val)];
                index.insert(&row, (i + 1) as u64).unwrap();
            }

            let ids = {
                // Simulate SQL: WHERE col BETWEEN 20 AND 40
                let iter = index
                    .range_scan(
                        Some(&SqlValue::Integer(20)),
                        Some(&SqlValue::Integer(40)),
                        true,
                        true,
                    )
                    .unwrap();
                iter.collect::<Result<Vec<_>>>().unwrap()
            };
            assert_eq!(ids, vec![2, 3, 4]);
        });
    }
}
