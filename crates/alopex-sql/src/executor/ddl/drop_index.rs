use alopex_core::kv::KVStore;

use crate::ast::ddl::IndexMethod;
use crate::catalog::Catalog;
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::{KeyEncoder, SqlTransaction};

use super::is_implicit_pk_index;

/// Execute DROP INDEX.
pub fn execute_drop_index<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &mut C,
    index_name: &str,
    if_exists: bool,
) -> Result<ExecutionResult> {
    if is_implicit_pk_index(index_name) {
        return Err(ExecutorError::InvalidOperation {
            operation: "DROP INDEX".into(),
            reason: "Cannot drop implicit PRIMARY KEY index directly; use DROP TABLE".into(),
        });
    }

    let index = match catalog.get_index(index_name) {
        Some(index) => index.clone(),
        None => {
            return if if_exists {
                Ok(ExecutionResult::Success)
            } else {
                Err(ExecutorError::IndexNotFound(index_name.to_string()))
            };
        }
    };

    if matches!(index.method, Some(IndexMethod::Hnsw)) {
        HnswBridge::drop_index(txn, &index, if_exists)?;
    } else {
        // Delete all index entries via prefix to avoid buffering rows.
        let prefix = KeyEncoder::index_prefix(index.index_id);
        txn.delete_prefix(&prefix)?;
    }

    catalog.drop_index(index_name)?;

    Ok(ExecutionResult::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, IndexMetadata, MemoryCatalog, TableMetadata};
    use crate::executor::ddl::create_index::execute_create_index;
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::types::ResolvedType;
    use crate::storage::{SqlValue, TxnBridge};
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    fn setup_table_and_index() -> (
        TxnBridge<MemoryKV>,
        MemoryCatalog,
        IndexMetadata,
        TableMetadata,
    ) {
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

        let mut txn = bridge.begin_write().unwrap();
        let index = IndexMetadata::new(0, "idx_users_name", "users", vec!["name".into()]);
        execute_create_index(&mut txn, &mut catalog, index, false).unwrap();
        txn.commit().unwrap();

        let table_meta = catalog.get_table("users").unwrap().clone();
        let index_meta = catalog.get_index("idx_users_name").unwrap().clone();
        (bridge, catalog, index_meta, table_meta)
    }

    #[test]
    fn drop_index_removes_entries_and_metadata() {
        let (bridge, mut catalog, index_meta, table_meta) = setup_table_and_index();
        let pk_index = catalog.get_index("__pk_users").unwrap().clone();

        // Insert row and index entries.
        let row = vec![SqlValue::Integer(1), SqlValue::Text("alice".into())];
        {
            let mut txn = bridge.begin_write().unwrap();
            let mut table = txn.table_storage(&table_meta);
            table.insert(1, &row).unwrap();

            let mut user_index = txn.index_storage(
                index_meta.index_id,
                index_meta.unique,
                index_meta.column_indices.clone(),
            );
            user_index.insert(&row, 1).unwrap();

            let mut pk = txn.index_storage(
                pk_index.index_id,
                pk_index.unique,
                pk_index.column_indices.clone(),
            );
            pk.insert(&row, 1).unwrap();
            txn.commit().unwrap();
        }

        let mut txn = bridge.begin_write().unwrap();
        let result = execute_drop_index(&mut txn, &mut catalog, "idx_users_name", false);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        assert!(!catalog.index_exists("idx_users_name"));
        assert!(catalog.index_exists("__pk_users"));

        let mut txn = bridge.begin_write().unwrap();
        let mut user_index = txn.index_storage(
            index_meta.index_id,
            index_meta.unique,
            index_meta.column_indices.clone(),
        );
        assert!(
            user_index
                .lookup(&SqlValue::Text("alice".into()))
                .unwrap()
                .is_empty()
        );

        {
            let mut table = txn.table_storage(&table_meta);
            let mut scan = table.scan().unwrap();
            assert!(scan.next().is_some()); // row remains
        }
        txn.commit().unwrap();
    }

    #[test]
    fn drop_index_rejects_implicit_pk() {
        let (bridge, mut catalog, _index_meta, _table_meta) = setup_table_and_index();
        let mut txn = bridge.begin_write().unwrap();
        let err = execute_drop_index(&mut txn, &mut catalog, "__pk_users", false).unwrap_err();
        txn.rollback().unwrap();
        assert!(matches!(err, ExecutorError::InvalidOperation { .. }));
    }

    #[test]
    fn drop_index_if_exists_succeeds_when_missing() {
        let (bridge, mut catalog, _index_meta, _table_meta) = setup_table_and_index();
        let mut txn = bridge.begin_write().unwrap();
        let result = execute_drop_index(&mut txn, &mut catalog, "missing_index", true);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();
    }
}
