use alopex_core::kv::KVStore;

use crate::catalog::{Catalog, IndexMetadata, TableMetadata};
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::{KeyEncoder, SqlTransaction};

use super::create_pk_index_name;

/// Execute CREATE TABLE.
pub fn execute_create_table<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &mut C,
    mut table: TableMetadata,
    if_not_exists: bool,
) -> Result<ExecutionResult> {
    if catalog.table_exists(&table.name) {
        return if if_not_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::TableAlreadyExists(table.name))
        };
    }

    // Resolve PK column indices before mutating the catalog to avoid partial writes.
    let pk_index = if let Some(pk_columns) = table.primary_key.clone() {
        let column_indices = resolve_column_indices(&table, &pk_columns)?;
        let index_id = catalog.next_index_id();
        let index_name = create_pk_index_name(&table.name);

        Some(
            IndexMetadata::new(index_id, index_name, table.name.clone(), pk_columns)
                .with_column_indices(column_indices)
                .with_unique(true),
        )
    } else {
        None
    };

    let table_id = catalog.next_table_id();
    table = table.with_table_id(table_id);

    // Ensure storage keyspace is clean for this table_id (defensive for future persistence).
    let table_prefix = KeyEncoder::table_prefix(table_id);
    txn.delete_prefix(&table_prefix)?;
    let seq_key = KeyEncoder::sequence_key(table_id);
    txn.delete_prefix(&seq_key)?;

    catalog.create_table(table.clone())?;

    if let Some(index) = pk_index {
        if let Err(err) = catalog.create_index(index) {
            // Best-effort rollback to keep catalog consistent.
            let _ = catalog.drop_table(&table.name);
            return Err(err.into());
        }
    }

    Ok(ExecutionResult::Success)
}

fn resolve_column_indices(table: &TableMetadata, columns: &[String]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|name| {
            table
                .get_column_index(name)
                .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, MemoryCatalog};
    use crate::executor::ExecutorError;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    fn bridge() -> (TxnBridge<MemoryKV>, MemoryCatalog) {
        (
            TxnBridge::new(Arc::new(MemoryKV::new())),
            MemoryCatalog::new(),
        )
    }

    #[test]
    fn create_table_assigns_ids_and_pk_index() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        let mut txn = bridge.begin_write().unwrap();
        let result = execute_create_table(&mut txn, &mut catalog, table.clone(), false);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();
        let stored = catalog.get_table("users").expect("table stored");
        assert_eq!(stored.table_id, 1);

        let pk_index = catalog.get_index("__pk_users").expect("pk index stored");
        assert_eq!(pk_index.index_id, 1);
        assert!(pk_index.unique);
        assert_eq!(pk_index.column_indices, vec![0]);
    }

    #[test]
    fn create_table_if_not_exists_is_noop() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "items",
            vec![ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true)],
        )
        .with_primary_key(vec!["id".into()]);

        let mut txn = bridge.begin_write().unwrap();
        let first = execute_create_table(&mut txn, &mut catalog, table.clone(), false);
        assert!(first.is_ok());
        txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let second = execute_create_table(&mut txn, &mut catalog, table.clone(), true);
        assert!(matches!(second, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        assert_eq!(catalog.table_count(), 1);
        assert_eq!(catalog.index_count(), 1);
    }

    #[test]
    fn create_table_validates_pk_columns() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "bad",
            vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        )
        .with_primary_key(vec!["missing".into()]);

        let mut txn = bridge.begin_write().unwrap();
        let err = execute_create_table(&mut txn, &mut catalog, table, false).unwrap_err();
        txn.rollback().unwrap();

        assert!(matches!(
            err,
            ExecutorError::ColumnNotFound(name) if name == "missing"
        ));
        assert!(!catalog.table_exists("bad"));
    }
}
