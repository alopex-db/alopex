use alopex_core::kv::KVStore;

use crate::executor::Result;
use crate::storage::SqlTransaction;

use super::Row;

/// Execute a table scan and return rows with RowIDs.
pub fn execute_scan<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    table_meta: &crate::catalog::TableMetadata,
) -> Result<Vec<Row>> {
    Ok(txn.with_table(table_meta, |storage| {
        let mut iter = storage.range_scan(0, u64::MAX)?;
        let mut rows = Vec::new();
        while let Some(entry) = iter.next() {
            let (row_id, values) = entry?;
            rows.push(Row::new(row_id, values));
        }
        Ok(rows)
    })?)
}
