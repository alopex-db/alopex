use alopex_core::kv::KVStore;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{Catalog, IndexMetadata};
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::{KeyEncoder, SqlTransaction};

/// Execute DROP TABLE.
pub fn execute_drop_table<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
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

    let indexes: Vec<IndexMetadata> = catalog
        .get_indexes_for_table(table_name)
        .into_iter()
        .cloned()
        .collect();

    // Remove index keyspaces via prefix deletion to avoid buffering rows.
    for index in &indexes {
        if matches!(index.method, Some(IndexMethod::Hnsw)) {
            HnswBridge::drop_index(txn, index, false)?;
        } else {
            let prefix = KeyEncoder::index_prefix(index.index_id);
            txn.delete_prefix(&prefix)?;
        }
    }

    // Remove table rows and sequence key by prefix.
    let table_prefix = KeyEncoder::table_prefix(table_meta.table_id);
    txn.delete_prefix(&table_prefix)?;
    let seq_key = KeyEncoder::sequence_key(table_meta.table_id);
    txn.delete_prefix(&seq_key)?;

    // Finally drop from catalog (removes metadata + indexes).
    catalog.drop_table(table_name)?;

    Ok(ExecutionResult::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TableMetadata;
    use crate::catalog::{ColumnMetadata, MemoryCatalog};
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::types::ResolvedType;
    use crate::storage::{SqlValue, TxnBridge};
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    fn setup() -> (TxnBridge<MemoryKV>, MemoryCatalog, TableMetadata) {
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
        execute_create_table(&mut txn, &mut catalog, table.clone(), vec![], false).unwrap();
        txn.commit().unwrap();

        let stored = catalog.get_table("users").unwrap().clone();
        (bridge, catalog, stored)
    }

    #[test]
    fn drop_table_removes_rows_and_indexes() {
        let (bridge, mut catalog, table_meta) = setup();
        let pk_index = catalog.get_index("__pk_users").expect("pk index").clone();

        // Insert a row and index entry manually.
        let row = vec![SqlValue::Integer(1), SqlValue::Text("alice".into())];
        {
            let mut txn = bridge.begin_write().unwrap();
            let mut table = txn.table_storage(&table_meta);
            table.insert(1, &row).unwrap();

            let mut index = txn.index_storage(
                pk_index.index_id,
                pk_index.unique,
                pk_index.column_indices.clone(),
            );
            index.insert(&row, 1).unwrap();
            txn.commit().unwrap();
        }

        // Drop table.
        let mut txn = bridge.begin_write().unwrap();
        let result = execute_drop_table(&mut txn, &mut catalog, "users", false);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        assert!(!catalog.table_exists("users"));
        assert!(!catalog.index_exists("__pk_users"));

        // Ensure storage is cleared.
        let mut txn = bridge.begin_write().unwrap();
        let mut table = txn.table_storage(&table_meta);
        assert!(table.scan().unwrap().next().is_none());

        let mut index = txn.index_storage(
            pk_index.index_id,
            pk_index.unique,
            pk_index.column_indices.clone(),
        );
        assert!(index.lookup(&SqlValue::Integer(1)).unwrap().is_empty());
        txn.commit().unwrap();
    }

    #[test]
    fn drop_table_if_exists_succeeds_on_missing() {
        let (bridge, mut catalog, _) = setup();
        let mut txn = bridge.begin_write().unwrap();
        let result = execute_drop_table(&mut txn, &mut catalog, "missing", true);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();
    }
}
