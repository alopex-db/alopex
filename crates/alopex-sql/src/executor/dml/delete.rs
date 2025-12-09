use alopex_core::kv::KVStore;

use crate::catalog::{Catalog, IndexMetadata};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::{SqlTransaction, SqlValue, StorageError};

/// Execute DELETE statements.
pub fn execute_delete<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    table_name: &str,
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

    let mut rows_affected = 0u64;
    let mut next_row_id = 0u64;
    const BATCH: usize = 512;

    loop {
        let mut batch = Vec::with_capacity(BATCH);
        {
            let mut table_storage = txn.table_storage(&table);
            let mut iter = table_storage.range_scan(next_row_id, u64::MAX)?;
            for _ in 0..BATCH {
                if let Some(res) = iter.next() {
                    let (row_id, row) = res?;
                    next_row_id = row_id.saturating_add(1);
                    batch.push((row_id, row));
                } else {
                    break;
                }
            }
        }

        if batch.is_empty() {
            break;
        }

        for (row_id, row) in batch {
            if !predicate_matches(&filter, &row)? {
                continue;
            }

            remove_indexes(txn, &indexes, row_id, &row)?;

            let mut table_storage = txn.table_storage(&table);
            table_storage
                .delete(row_id)
                .map_err(|e| map_storage_error(&table, e))?;

            rows_affected += 1;
        }
    }

    Ok(ExecutionResult::RowsAffected(rows_affected))
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

fn remove_indexes<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    indexes: &[IndexMetadata],
    row_id: u64,
    row: &[SqlValue],
) -> Result<()> {
    for index in indexes {
        if should_skip_unique_index_for_null(index, row) {
            continue;
        }
        txn.with_index(
            index.index_id,
            index.unique,
            index.column_indices.clone(),
            |storage| storage.delete(row, row_id),
        )
        .map_err(|e| map_index_error(index, e))?;
    }

    Ok(())
}

fn map_storage_error(table: &crate::catalog::TableMetadata, err: StorageError) -> ExecutorError {
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

    fn literal(kind: TypedExprKind, ty: ResolvedType) -> TypedExpr {
        TypedExpr {
            kind,
            resolved_type: ty,
            span: Span::default(),
        }
    }

    #[test]
    fn delete_removes_rows_and_indexes() {
        let (bridge, mut catalog) = bridge();
        let table = crate::catalog::TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        // DDL setup
        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), false).unwrap();
        ddl_txn.commit().unwrap();

        let stored_table = catalog.get_table("users").unwrap().clone();

        // Insert two rows
        {
            let mut txn = bridge.begin_write().unwrap();
            crate::executor::dml::insert::execute_insert(
                &mut txn,
                &catalog,
                "users",
                vec!["id".into(), "name".into()],
                vec![
                    vec![
                        literal(
                            TypedExprKind::Literal(crate::ast::expr::Literal::Number("1".into())),
                            ResolvedType::Integer,
                        ),
                        literal(
                            TypedExprKind::Literal(crate::ast::expr::Literal::String(
                                "alice".into(),
                            )),
                            ResolvedType::Text,
                        ),
                    ],
                    vec![
                        literal(
                            TypedExprKind::Literal(crate::ast::expr::Literal::Number("2".into())),
                            ResolvedType::Integer,
                        ),
                        literal(
                            TypedExprKind::Literal(crate::ast::expr::Literal::String("bob".into())),
                            ResolvedType::Text,
                        ),
                    ],
                ],
            )
            .unwrap();
            txn.commit().unwrap();
        }

        // Delete row with id = 1
        let mut txn = bridge.begin_write().unwrap();
        let predicate = TypedExpr {
            kind: TypedExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: TypedExprKind::ColumnRef {
                        table: "users".into(),
                        column: "id".into(),
                        column_index: 0,
                    },
                    resolved_type: ResolvedType::Integer,
                    span: Span::default(),
                }),
                op: crate::ast::expr::BinaryOp::Eq,
                right: Box::new(literal(
                    TypedExprKind::Literal(crate::ast::expr::Literal::Number("1".into())),
                    ResolvedType::Integer,
                )),
            },
            resolved_type: ResolvedType::Boolean,
            span: Span::default(),
        };
        let result = execute_delete(&mut txn, &catalog, "users", Some(predicate)).unwrap();
        assert!(matches!(result, ExecutionResult::RowsAffected(1)));

        {
            let mut table_storage = txn.table_storage(&stored_table);
            assert!(table_storage.get(1).unwrap().is_none());
            assert!(table_storage.get(2).unwrap().is_some());
        }
        txn.commit().unwrap();
    }
}
