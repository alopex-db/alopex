use alopex_core::KVTransaction;
use alopex_core::kv::KVStore;

use crate::catalog::TableMetadata;
use crate::executor::Result;
use crate::storage::{
    KeyEncoder, RangeBoundedScanIterator, SqlTransaction, SqlTxn, StorageRangeConstraint,
    TableScanIterator,
};

use super::Row;
use super::iterator::ScanIterator;

/// Execute a table scan and return rows with RowIDs.
pub fn execute_scan<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &crate::catalog::TableMetadata,
) -> Result<Vec<Row>> {
    Ok(txn.with_table(table_meta, |storage| {
        let iter = storage.range_scan(0, u64::MAX)?;
        let mut rows = Vec::new();
        for entry in iter {
            let (row_id, values) = entry?;
            rows.push(Row::new(row_id, values));
        }
        Ok(rows)
    })?)
}

/// Create a streaming scan iterator for FR-7 compliance.
///
/// This function creates a `ScanIterator` that streams rows directly from
/// the underlying storage without materializing all rows upfront.
///
/// # Lifetime
///
/// The returned iterator borrows from the transaction (`'a`), so the
/// transaction must remain valid while the iterator is in use.
pub fn create_scan_iterator<'a, 'txn: 'a, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &'a mut T,
    table_meta: &TableMetadata,
) -> Result<ScanIterator<'a>> {
    let table_id = table_meta.table_id;
    let prefix = KeyEncoder::table_prefix(table_id);
    let inner = txn.inner_mut().scan_prefix(&prefix)?;
    let table_scan_iter = TableScanIterator::new(inner, table_id);
    Ok(ScanIterator::new(table_scan_iter, table_meta))
}

/// Creates the only scan iterator intended for a fenced remote range worker.
///
/// Unlike [`create_scan_iterator`], this entry point rejects an ordinary local
/// transaction and always uses concrete `scan_range` bounds.  The caller has
/// already pinned the catalog, schema, and index identities in `constraint`.
pub fn create_fenced_range_scan_iterator<'a, 'txn: 'a, S: KVStore + 'txn>(
    txn: &'a mut SqlTransaction<'txn, S>,
    table_meta: &TableMetadata,
    constraint: &StorageRangeConstraint,
) -> Result<RangeBoundedScanIterator<'a>> {
    constraint.validate_table(table_meta)?;
    constraint.validate_read_at(txn.read_at_point())?;
    let (lower, upper) = constraint.encoded_bounds();
    let inner = txn.inner_mut().scan_range(lower, upper)?;
    Ok(RangeBoundedScanIterator::new(
        TableScanIterator::new(inner, constraint.table_id()),
        constraint.clone(),
    ))
}

/// Executes a materialized fenced range scan for a remote worker.
///
/// This function is intentionally separate from the legacy local scan.  It
/// cannot broaden a worker into a whole-table prefix scan and checks every
/// returned primary row key before returning it.
pub fn execute_fenced_range_scan<'txn, S: KVStore + 'txn>(
    txn: &mut SqlTransaction<'txn, S>,
    table_meta: &TableMetadata,
    constraint: &StorageRangeConstraint,
) -> Result<Vec<Row>> {
    let iter = create_fenced_range_scan_iterator(txn, table_meta, constraint)?;
    iter.map(|entry| entry.map(|(row_id, values)| Row::new(row_id, values)))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alopex_core::ReadAtPoint;
    use alopex_core::kv::{KVStore, memory::MemoryKV};
    use alopex_core::types::TxnMode;

    use super::*;
    use crate::catalog::ColumnMetadata;
    use crate::planner::types::ResolvedType;
    use crate::storage::{RangeReadSnapshot, SqlValue, TxnBridge};

    fn table() -> TableMetadata {
        TableMetadata::new(
            "users",
            vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        )
        .with_table_id(7)
    }

    fn constraint(point: ReadAtPoint) -> StorageRangeConstraint {
        StorageRangeConstraint::new(
            "range-a",
            3,
            alopex_core::RowKeyRange::new(7, Some(2), Some(4)).unwrap(),
            RangeReadSnapshot::new(point, "schema-13").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn fenced_scan_uses_half_open_range_and_rechecks_each_row() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store.clone());
        let table = table();
        let mut write = bridge.begin_write().unwrap();
        write
            .with_table(&table, |storage| {
                for row_id in 1..=4 {
                    storage.insert(row_id, &[SqlValue::Integer(row_id as i32)])?;
                }
                Ok(())
            })
            .unwrap();
        write.commit().unwrap();

        let point = ReadAtPoint::new(7, 11, 13, 17);
        // `from_read_at` models the transaction a capable remote backend has
        // already opened.  MemoryKV itself is deliberately not read-at capable.
        let inner = store.begin(TxnMode::ReadOnly).unwrap();
        let mut read = TxnBridge::<MemoryKV>::from_read_at(inner, point);
        let rows = execute_fenced_range_scan(&mut read, &table, &constraint(point)).unwrap();

        assert_eq!(
            rows.into_iter().map(|row| row.row_id).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn fenced_scan_rejects_an_unfenced_local_transaction_before_scanning() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store);
        let table = table();
        let point = ReadAtPoint::new(7, 11, 13, 17);
        let mut local_read = bridge.begin_read().unwrap();

        let error =
            execute_fenced_range_scan(&mut local_read, &table, &constraint(point)).unwrap_err();
        assert!(error.to_string().contains("requires a transaction opened"));
    }

    #[test]
    fn fenced_scan_rejects_a_catalog_or_index_fence_mismatch_before_scanning() {
        let store = Arc::new(MemoryKV::new());
        let table = table();
        let expected = ReadAtPoint::new(7, 11, 13, 17);
        let inner = store.begin(TxnMode::ReadOnly).unwrap();
        let mut read = TxnBridge::<MemoryKV>::from_read_at(inner, ReadAtPoint::new(7, 11, 13, 18));

        let error =
            execute_fenced_range_scan(&mut read, &table, &constraint(expected)).unwrap_err();
        assert!(error.to_string().contains("read-at fence mismatch"));
    }
}
