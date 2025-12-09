use alopex_core::kv::KVStore;

use crate::catalog::{Catalog, IndexMetadata, TableMetadata};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::{SqlTransaction, SqlValue, StorageError};

/// Execute INSERT statements.
pub fn execute_insert<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    table_name: &str,
    columns: Vec<String>,
    values: Vec<Vec<TypedExpr>>,
) -> Result<ExecutionResult> {
    let table = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    validate_columns(&table, &columns)?;

    let indexes: Vec<IndexMetadata> = catalog
        .get_indexes_for_table(table_name)
        .into_iter()
        .cloned()
        .collect();

    let mut staged: Vec<(u64, Vec<SqlValue>)> = Vec::with_capacity(values.len());

    // Insert into table using a single handle; stage for index population.
    {
        let mut table_storage = txn.table_storage(&table);
        for row_exprs in values {
            let row = build_row(&table, &columns, row_exprs)?;
            let row_id = table_storage
                .next_row_id()
                .map_err(|e| map_storage_error(&table, e))?;
            table_storage
                .insert(row_id, &row)
                .map_err(|e| map_storage_error(&table, e))?;
            staged.push((row_id, row));
        }
    }

    // Populate indexes using one handle per index for the whole batch.
    populate_indexes(txn, &indexes, &staged)?;

    Ok(ExecutionResult::RowsAffected(staged.len() as u64))
}

fn validate_columns(table: &TableMetadata, columns: &[String]) -> Result<()> {
    // All provided columns must exist.
    for col in columns {
        if table.get_column(col).is_none() {
            return Err(ExecutorError::ColumnNotFound(col.clone()));
        }
    }

    // DEFAULT is not supported; missing columns are an error.
    if columns.len() != table.column_count() {
        let missing = table
            .columns
            .iter()
            .find(|c| !columns.iter().any(|col| col == &c.name))
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        return Err(ExecutorError::ColumnRequired { column: missing });
    }

    Ok(())
}

fn build_row(
    table: &TableMetadata,
    columns: &[String],
    exprs: Vec<TypedExpr>,
) -> Result<Vec<SqlValue>> {
    if exprs.len() != columns.len() {
        return Err(ExecutorError::InvalidOperation {
            operation: "INSERT".into(),
            reason: format!(
                "column/value count mismatch: {} vs {}",
                columns.len(),
                exprs.len()
            ),
        });
    }

    let mut row = vec![SqlValue::Null; table.column_count()];
    let ctx = EvalContext::new(&[]);

    for (idx, expr) in exprs.into_iter().enumerate() {
        let col_name = &columns[idx];
        let col_index = table
            .get_column_index(col_name)
            .ok_or_else(|| ExecutorError::ColumnNotFound(col_name.clone()))?;
        let value = evaluate(&expr, &ctx)?;
        row[col_index] = value;
    }

    Ok(row)
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
            .any(|&idx| row.get(idx).map_or(true, SqlValue::is_null))
}

fn populate_indexes<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    indexes: &[IndexMetadata],
    rows: &[(u64, Vec<SqlValue>)],
) -> Result<()> {
    for index in indexes {
        let mut storage =
            txn.index_storage(index.index_id, index.unique, index.column_indices.clone());
        for (row_id, row) in rows {
            if should_skip_unique_index_for_null(index, row) {
                continue;
            }
            storage
                .insert(row, *row_id)
                .map_err(|e| map_index_error(index, e))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;
    use crate::catalog::{ColumnMetadata, MemoryCatalog};
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

    fn literal(value: TypedExprKind, ty: ResolvedType) -> TypedExpr {
        TypedExpr {
            kind: value,
            resolved_type: ty,
            span: Span::default(),
        }
    }

    #[test]
    fn insert_inserts_row_and_indexes() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        // Prepare table + PK index
        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), false).unwrap();
        ddl_txn.commit().unwrap();
        let stored_table = catalog.get_table("users").unwrap().clone();

        // Execute insert
        let mut txn = bridge.begin_write().unwrap();
        let result = execute_insert(
            &mut txn,
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
        );
        assert!(matches!(result, Ok(ExecutionResult::RowsAffected(1))));

        // Verify storage
        {
            let mut table_storage = txn.table_storage(&stored_table);
            let row = table_storage.get(1).unwrap().expect("row stored");
            assert_eq!(
                row,
                vec![SqlValue::Integer(1), SqlValue::Text("alice".into())]
            );
        }

        txn.commit().unwrap();
    }

    #[test]
    fn insert_missing_column_errors() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "items",
            vec![ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true)],
        )
        .with_primary_key(vec!["id".into()]);

        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), false).unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let err = execute_insert(
            &mut txn,
            &catalog,
            "items",
            vec![], // omitting columns should error (DEFAULT unsupported)
            vec![vec![]],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ExecutorError::ColumnRequired { column } if column == "id"
        ));
        txn.rollback().unwrap();
    }

    #[test]
    fn insert_unique_violation_maps_to_constraint_violation() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "users",
            vec![ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true)],
        )
        .with_primary_key(vec!["id".into()]);

        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), false).unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let row = vec![literal(
            TypedExprKind::Literal(crate::ast::expr::Literal::Number("1".into())),
            ResolvedType::Integer,
        )];

        execute_insert(
            &mut txn,
            &catalog,
            "users",
            vec!["id".into()],
            vec![row.clone()],
        )
        .unwrap();

        let err =
            execute_insert(&mut txn, &catalog, "users", vec!["id".into()], vec![row]).unwrap_err();

        assert!(matches!(
            err,
            ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { .. })
        ));
        txn.rollback().unwrap();
    }
}
