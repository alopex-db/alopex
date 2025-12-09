use alopex_core::kv::KVStore;

use crate::catalog::{Catalog, IndexMetadata, TableMetadata};
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::storage::{SqlTransaction, SqlValue, StorageError};

use super::is_implicit_pk_index;

/// Execute CREATE INDEX.
pub fn execute_create_index<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &mut C,
    mut index: IndexMetadata,
    if_not_exists: bool,
) -> Result<ExecutionResult> {
    if is_implicit_pk_index(&index.name) {
        return Err(ExecutorError::InvalidIndexName {
            name: index.name.clone(),
            reason: "Index names starting with '__pk_' are reserved for PRIMARY KEY".into(),
        });
    }

    if catalog.index_exists(&index.name) {
        return if if_not_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::IndexAlreadyExists(index.name.clone()))
        };
    }

    let table = catalog
        .get_table(&index.table)
        .ok_or_else(|| ExecutorError::TableNotFound(index.table.clone()))?
        .clone();

    let column_indices = resolve_column_indices(&table, &index)?;

    let index_id = catalog.next_index_id();
    index.index_id = index_id;
    index.column_indices = column_indices.clone();

    // Populate index entries for existing rows before publishing metadata.
    build_index_for_existing_rows(txn, &table, &index, column_indices)?;

    catalog.create_index(index)?;

    Ok(ExecutionResult::Success)
}

fn resolve_column_indices(
    table: &crate::catalog::TableMetadata,
    index: &IndexMetadata,
) -> Result<Vec<usize>> {
    index
        .columns
        .iter()
        .map(|name| {
            table
                .get_column_index(name)
                .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
        })
        .collect()
}

fn should_skip_unique_index_for_null(index: &IndexMetadata, row: &[SqlValue]) -> bool {
    index.unique
        && index
            .column_indices
            .iter()
            .any(|&idx| row.get(idx).map_or(true, SqlValue::is_null))
}

fn build_index_for_existing_rows<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    table: &TableMetadata,
    index: &IndexMetadata,
    column_indices: Vec<usize>,
) -> Result<()> {
    const CHUNK_SIZE: u64 = 2048;
    let mut start_row_id = 0u64;

    loop {
        let rows = fetch_rows_chunk(txn, table, start_row_id + 1, CHUNK_SIZE)?;
        if rows.is_empty() {
            break;
        }

        let insert_result = txn.with_index(
            index.index_id,
            index.unique,
            column_indices.clone(),
            |storage| {
                for (row_id, row) in rows {
                    if should_skip_unique_index_for_null(index, &row) {
                        start_row_id = row_id;
                        continue;
                    }
                    storage.insert(&row, row_id)?;
                    start_row_id = row_id;
                }
                Ok(())
            },
        );

        match insert_result {
            Ok(()) => {}
            Err(StorageError::UniqueViolation { .. }) => {
                return Err(ExecutorError::ConstraintViolation(
                    ConstraintViolation::Unique {
                        index_name: index.name.clone(),
                        columns: index.columns.clone(),
                        value: None,
                    },
                ));
            }
            Err(other) => return Err(other.into()),
        }
    }

    Ok(())
}

fn fetch_rows_chunk<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    table: &TableMetadata,
    start_row_id: u64,
    chunk_size: u64,
) -> Result<Vec<(u64, Vec<SqlValue>)>> {
    Ok(txn.with_table(table, |table_storage| {
        let end_row_id = start_row_id.saturating_add(chunk_size - 1);
        let mut scan = table_storage.range_scan(start_row_id, end_row_id)?;
        let mut rows = Vec::new();
        while let Some(entry) = scan.next() {
            let (row_id, row) = entry?;
            rows.push((row_id, row));
        }
        Ok(rows)
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    fn setup_table() -> (TxnBridge<MemoryKV>, MemoryCatalog, TableMetadata) {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut catalog = MemoryCatalog::new();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
                ColumnMetadata::new("age", ResolvedType::Integer),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        let mut txn = bridge.begin_write().unwrap();
        execute_create_table(&mut txn, &mut catalog, table.clone(), false).unwrap();
        txn.commit().unwrap();

        let stored = catalog.get_table("users").unwrap().clone();
        (bridge, catalog, stored)
    }

    #[test]
    fn create_index_resolves_columns_and_assigns_id() {
        let (bridge, mut catalog, _table_meta) = setup_table();
        let mut txn = bridge.begin_write().unwrap();

        let index = IndexMetadata::new(0, "idx_users_name", "users", vec!["name".into()]);
        let result = execute_create_index(&mut txn, &mut catalog, index, false);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        let stored = catalog
            .get_index("idx_users_name")
            .expect("index stored")
            .clone();
        assert_eq!(stored.index_id, 2); // pk index consumes 1
        assert_eq!(stored.column_indices, vec![1]);
    }

    #[test]
    fn create_index_populates_existing_rows() {
        let (bridge, mut catalog, table_meta) = setup_table();

        // Insert a row before creating the index.
        {
            let mut txn = bridge.begin_write().unwrap();
            let mut table = txn.table_storage(&table_meta);
            table
                .insert(
                    1,
                    &[
                        SqlValue::Integer(1),
                        SqlValue::Text("alice".into()),
                        SqlValue::Integer(30),
                    ],
                )
                .unwrap();
            txn.commit().unwrap();
        }

        let mut txn = bridge.begin_write().unwrap();
        let index = IndexMetadata::new(0, "idx_users_name", "users", vec!["name".into()]);
        execute_create_index(&mut txn, &mut catalog, index, false).unwrap();
        txn.commit().unwrap();

        let stored = catalog.get_index("idx_users_name").unwrap().clone();
        let mut txn = bridge.begin_write().unwrap();
        let mut index_storage = txn.index_storage(
            stored.index_id,
            stored.unique,
            stored.column_indices.clone(),
        );
        let rows = index_storage
            .lookup(&SqlValue::Text("alice".into()))
            .unwrap();
        assert_eq!(rows, vec![1]);
        txn.commit().unwrap();
    }

    #[test]
    fn create_index_rejects_reserved_prefix() {
        let (bridge, mut catalog, _table_meta) = setup_table();
        let mut txn = bridge.begin_write().unwrap();
        let index = IndexMetadata::new(0, "__pk_users", "users", vec!["name".into()]);

        let err = execute_create_index(&mut txn, &mut catalog, index, false).unwrap_err();
        txn.rollback().unwrap();
        assert!(matches!(err, ExecutorError::InvalidIndexName { .. }));
    }

    #[test]
    fn create_index_if_not_exists_is_noop() {
        let (bridge, mut catalog, _table_meta) = setup_table();
        let mut txn = bridge.begin_write().unwrap();
        let index = IndexMetadata::new(0, "idx_users_age", "users", vec!["age".into()]);
        execute_create_index(&mut txn, &mut catalog, index.clone(), false).unwrap();
        txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let result = execute_create_index(&mut txn, &mut catalog, index, true);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        assert!(catalog.index_exists("idx_users_age"));
    }
}
