use alopex_core::KVTransaction;
use alopex_core::kv::KVStore;

use crate::catalog::TableMetadata;
use crate::executor::Result;
use crate::storage::{KeyEncoder, SqlTxn, TableScanIterator};

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
