use alopex_core::kv::KVStore;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{Catalog, IndexMetadata, TableMetadata};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::planner::typed_expr::{TypedAssignment, TypedExpr};
use crate::storage::{SqlTxn, SqlValue, StorageError};

/// Execute UPDATE statements.
pub fn execute_update<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    table_name: &str,
    assignments: Vec<TypedAssignment>,
    filter: Option<TypedExpr>,
) -> Result<ExecutionResult> {
    let table = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    let indexes: Vec<IndexMetadata> = catalog
        .get_indexes_for_table(table_name)
        .into_iter()
        .cloned()
        .collect();
    let (hnsw_indexes, btree_indexes): (Vec<_>, Vec<_>) = indexes
        .into_iter()
        .partition(|idx| matches!(idx.method, Some(IndexMethod::Hnsw)));

    let mut rows_affected = 0u64;
    let mut next_row_id = 0u64;
    const BATCH: usize = 512;

    loop {
        let batch = fetch_batch(txn, &table, next_row_id, BATCH)?;

        if batch.is_empty() {
            break;
        }

        let mut changes = Vec::new();

        for (row_id, row) in batch {
            next_row_id = row_id.saturating_add(1);
            if !predicate_matches(&filter, &row)? {
                continue;
            }

            let ctx = EvalContext::new(&row);
            let mut new_row = row.clone();

            for assignment in &assignments {
                let value = evaluate(&assignment.value, &ctx)?;
                enforce_not_null(&table, assignment.column_index, &value)?;
                new_row[assignment.column_index] = value;
            }

            if new_row == row {
                continue;
            }

            changes.push((row_id, row, new_row));
        }

        if changes.is_empty() {
            continue;
        }

        update_indexes_batch(txn, &btree_indexes, &changes)?;
        update_hnsw_indexes(txn, &table, &hnsw_indexes, &changes)?;
        apply_table_updates(txn, &table, &changes)?;

        rows_affected += changes.len() as u64;
    }

    Ok(ExecutionResult::RowsAffected(rows_affected))
}

fn fetch_batch<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    table: &TableMetadata,
    start_row_id: u64,
    batch_size: usize,
) -> Result<Vec<(u64, Vec<SqlValue>)>> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut table_storage = txn.table_storage(table);
    let mut iter = table_storage.range_scan(start_row_id, u64::MAX)?;
    for _ in 0..batch_size {
        if let Some(res) = iter.next() {
            let (row_id, row) = res?;
            batch.push((row_id, row));
        } else {
            break;
        }
    }
    Ok(batch)
}

fn predicate_matches(filter: &Option<TypedExpr>, row: &[SqlValue]) -> Result<bool> {
    if let Some(expr) = filter {
        let ctx = EvalContext::new(row);
        let value = evaluate(expr, &ctx)?;
        Ok(matches!(value, SqlValue::Boolean(true)))
    } else {
        Ok(true)
    }
}

fn enforce_not_null(table: &TableMetadata, column_index: usize, value: &SqlValue) -> Result<()> {
    let column = table
        .columns
        .get(column_index)
        .ok_or_else(|| ExecutorError::ColumnNotFound(format!("index {}", column_index)))?;
    if (column.not_null || column.primary_key) && value.is_null() {
        return Err(ConstraintViolation::NotNull {
            column: column.name.clone(),
        }
        .into());
    }
    Ok(())
}

fn map_storage_error(table: &TableMetadata, err: StorageError) -> ExecutorError {
    match err {
        StorageError::NullConstraintViolation { column } => {
            ConstraintViolation::NotNull { column }.into()
        }
        StorageError::PrimaryKeyViolation { .. } => ConstraintViolation::PrimaryKey {
            columns: table.primary_key.clone().unwrap_or_default(),
            value: None,
        }
        .into(),
        StorageError::TransactionConflict => ExecutorError::TransactionConflict,
        other => ExecutorError::Storage(other),
    }
}

fn map_index_error(index: &IndexMetadata, err: StorageError) -> ExecutorError {
    match err {
        StorageError::UniqueViolation { .. } => {
            if index.name.starts_with("__pk_") {
                ConstraintViolation::PrimaryKey {
                    columns: index.columns.clone(),
                    value: None,
                }
                .into()
            } else {
                ConstraintViolation::Unique {
                    index_name: index.name.clone(),
                    columns: index.columns.clone(),
                    value: None,
                }
                .into()
            }
        }
        StorageError::NullConstraintViolation { column } => {
            ConstraintViolation::NotNull { column }.into()
        }
        StorageError::TransactionConflict => ExecutorError::TransactionConflict,
        other => ExecutorError::Storage(other),
    }
}

fn should_skip_unique_index_for_null(index: &IndexMetadata, row: &[SqlValue]) -> bool {
    index.unique
        && index
            .column_indices
            .iter()
            .any(|&idx| row.get(idx).is_none_or(SqlValue::is_null))
}

fn update_indexes_batch<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    indexes: &[IndexMetadata],
    changes: &[(u64, Vec<SqlValue>, Vec<SqlValue>)],
) -> Result<()> {
    for index in indexes {
        let mut storage =
            txn.index_storage(index.index_id, index.unique, index.column_indices.clone());
        for (row_id, old_row, new_row) in changes {
            let old_skip = should_skip_unique_index_for_null(index, old_row);
            let new_skip = should_skip_unique_index_for_null(index, new_row);
            let changed = index
                .column_indices
                .iter()
                .any(|&idx| old_row[idx] != new_row[idx]);

            if !changed && old_skip == new_skip {
                continue;
            }

            if !old_skip {
                storage
                    .delete(old_row, *row_id)
                    .map_err(|e| map_index_error(index, e))?;
            }
            if !new_skip {
                storage
                    .insert(new_row, *row_id)
                    .map_err(|e| map_index_error(index, e))?;
            }
        }
    }
    Ok(())
}

fn apply_table_updates<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    table: &TableMetadata,
    changes: &[(u64, Vec<SqlValue>, Vec<SqlValue>)],
) -> Result<()> {
    let mut table_storage = txn.table_storage(table);
    for (row_id, _, new_row) in changes {
        table_storage
            .update(*row_id, new_row)
            .map_err(|e| map_storage_error(table, e))?;
    }
    Ok(())
}

fn update_hnsw_indexes<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    table: &TableMetadata,
    indexes: &[IndexMetadata],
    changes: &[(u64, Vec<SqlValue>, Vec<SqlValue>)],
) -> Result<()> {
    for index in indexes {
        for (row_id, old_row, new_row) in changes {
            HnswBridge::on_update(txn, table, index, *row_id, old_row, new_row)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;
    use crate::catalog::{ColumnMetadata, MemoryCatalog};
    use crate::executor::ddl::create_index::execute_create_index;
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::typed_expr::TypedExprKind;
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

    fn literal(kind: TypedExprKind, ty: ResolvedType) -> TypedExpr {
        TypedExpr {
            kind,
            resolved_type: ty,
            span: Span::default(),
        }
    }

    #[test]
    fn update_modifies_rows_and_indexes() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        // DDL setup: table + pk index
        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
        ddl_txn.commit().unwrap();

        let stored_table = catalog.get_table("users").unwrap().clone();
        {
            let mut write_txn = bridge.begin_write().unwrap();
            // Insert initial row
            crate::executor::dml::insert::execute_insert(
                &mut write_txn,
                &catalog,
                "users",
                vec!["id".into(), "name".into()],
                vec![vec![
                    literal(
                        TypedExprKind::Literal(crate::ast::expr::Literal::Number("1".into())),
                        ResolvedType::Integer,
                    ),
                    literal(
                        TypedExprKind::Literal(crate::ast::expr::Literal::String("alice".into())),
                        ResolvedType::Text,
                    ),
                ]],
            )
            .unwrap();
            write_txn.commit().unwrap();
        }

        // Add secondary unique index on name
        {
            let mut ddl_txn = bridge.begin_write().unwrap();
            let index_meta = crate::catalog::IndexMetadata::new(
                0,
                "idx_users_name",
                "users",
                vec!["name".into()],
            )
            .with_column_indices(vec![1])
            .with_unique(true);
            execute_create_index(&mut ddl_txn, &mut catalog, index_meta, false).unwrap();
            ddl_txn.commit().unwrap();
        }

        let mut write_txn = bridge.begin_write().unwrap();

        // Update row
        let result = execute_update(
            &mut write_txn,
            &catalog,
            "users",
            vec![TypedAssignment {
                column: "name".into(),
                column_index: 1,
                value: literal(
                    TypedExprKind::Literal(crate::ast::expr::Literal::String("bob".into())),
                    ResolvedType::Text,
                ),
            }],
            None,
        )
        .unwrap();

        assert!(matches!(result, ExecutionResult::RowsAffected(1)));

        // Verify storage updated
        {
            let mut table_storage = write_txn.table_storage(&stored_table);
            let row = table_storage.get(1).unwrap().expect("row exists");
            assert_eq!(
                row,
                vec![SqlValue::Integer(1), SqlValue::Text("bob".into())]
            );
        }

        write_txn.commit().unwrap();
    }

    #[test]
    fn update_enforces_not_null() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "items",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        crate::executor::dml::insert::execute_insert(
            &mut txn,
            &catalog,
            "items",
            vec!["id".into(), "name".into()],
            vec![vec![
                literal(
                    TypedExprKind::Literal(crate::ast::expr::Literal::Number("1".into())),
                    ResolvedType::Integer,
                ),
                literal(
                    TypedExprKind::Literal(crate::ast::expr::Literal::String("widget".into())),
                    ResolvedType::Text,
                ),
            ]],
        )
        .unwrap();

        let err = execute_update(
            &mut txn,
            &catalog,
            "items",
            vec![TypedAssignment {
                column: "name".into(),
                column_index: 1,
                value: literal(
                    TypedExprKind::Literal(crate::ast::expr::Literal::Null),
                    ResolvedType::Null,
                ),
            }],
            None,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ExecutorError::ConstraintViolation(ConstraintViolation::NotNull { .. })
        ));
        txn.rollback().unwrap();
    }
}
