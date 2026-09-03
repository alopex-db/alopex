use alopex_core::kv::KVStore;

use crate::catalog::Catalog;
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::{KeyEncoder, SqlTxn};

pub fn execute_truncate_table<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &mut C,
    table_name: &str,
    if_exists: bool,
) -> Result<ExecutionResult> {
    let table_meta = match catalog.get_table(table_name) {
        Some(table) => table.clone(),
        None => {
            return if if_exists {
                Ok(ExecutionResult::Success)
            } else {
                Err(ExecutorError::TableNotFound(table_name.to_string()))
            };
        }
    };

    txn.delete_prefix(&KeyEncoder::table_prefix(table_meta.table_id))?;
    txn.delete_prefix(&KeyEncoder::sequence_key(table_meta.table_id))?;
    Ok(ExecutionResult::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::types::ResolvedType;
    use crate::storage::{SqlValue, TxnBridge};
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    #[test]
    fn truncate_table_removes_rows() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut catalog = MemoryCatalog::new();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        let mut txn = bridge.begin_write().unwrap();
        execute_create_table(&mut txn, &mut catalog, table, vec![], false).unwrap();
        txn.commit().unwrap();

        let table_meta = catalog.get_table("users").unwrap().clone();
        let mut txn = bridge.begin_write().unwrap();
        txn.table_storage(&table_meta)
            .insert(1, &[SqlValue::Integer(1), SqlValue::Text("alice".into())])
            .unwrap();
        txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        execute_truncate_table(&mut txn, &mut catalog, "users", false).unwrap();
        txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let mut table_storage = txn.table_storage(&table_meta);
        assert!(table_storage.scan().unwrap().next().is_none());
        txn.commit().unwrap();
    }
}
