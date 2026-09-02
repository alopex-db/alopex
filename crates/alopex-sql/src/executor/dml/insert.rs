use alopex_core::kv::KVStore;

use crate::ast::ddl::IndexMethod;
use crate::ast::expr::Expr;
use crate::catalog::{Catalog, ColumnMetadata, IndexMetadata, TableMetadata};
use crate::executor::Row;
use crate::executor::evaluator::{EvalContext, coerce_value, evaluate};
use crate::executor::fts_bridge::FtsBridge;
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::query::{project_row_values, projected_columns};
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::planner::logical_plan::{OnConflictActionPlan, OnConflictPlan};
use crate::planner::type_checker::TypeChecker;
use crate::planner::typed_expr::Projection;
use crate::planner::typed_expr::TypedExpr;
use crate::storage::{SqlTxn, SqlValue, StorageError};

/// Execute INSERT statements.
#[allow(dead_code)]
pub fn execute_insert<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    table_name: &str,
    columns: Vec<String>,
    values: Vec<Vec<TypedExpr>>,
) -> Result<ExecutionResult> {
    execute_insert_with_returning(txn, catalog, table_name, columns, values, None)
}

pub fn execute_insert_with_returning<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    table_name: &str,
    columns: Vec<String>,
    values: Vec<Vec<TypedExpr>>,
    returning: Option<Projection>,
) -> Result<ExecutionResult> {
    let table = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    validate_columns(&table, &columns)?;

    // NOW() and other statement-scoped values are evaluated once per statement.
    let ctx = EvalContext::new(&[]);
    let rows = values
        .into_iter()
        .map(|row_exprs| build_row(catalog, &table, &columns, row_exprs, &ctx))
        .collect::<Result<Vec<_>>>()?;

    insert_rows(txn, catalog, &table, table_name, rows, None, returning)
}

fn validate_columns(table: &TableMetadata, columns: &[String]) -> Result<()> {
    // All provided columns must exist.
    for col in columns {
        if table.get_column(col).is_none() {
            return Err(ExecutorError::ColumnNotFound(col.clone()));
        }
    }

    if columns.len() != table.column_count()
        && let Some(missing) = table
            .columns
            .iter()
            .find(|c| {
                !columns.iter().any(|col| col == &c.name)
                    && c.default.is_none()
                    && (c.not_null || c.primary_key)
            })
            .map(|c| c.name.clone())
    {
        return Err(ExecutorError::ColumnRequired { column: missing });
    }

    Ok(())
}

/// Insert already-evaluated SELECT output rows.
#[allow(dead_code)]
pub fn execute_insert_rows<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    table_name: &str,
    columns: Vec<String>,
    values: Vec<Vec<SqlValue>>,
) -> Result<ExecutionResult> {
    execute_insert_rows_with_returning(txn, catalog, table_name, columns, values, None)
}

pub fn execute_insert_rows_with_returning<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    table_name: &str,
    columns: Vec<String>,
    values: Vec<Vec<SqlValue>>,
    returning: Option<Projection>,
) -> Result<ExecutionResult> {
    let table = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    validate_columns(&table, &columns)?;

    let ctx = EvalContext::new(&[]);
    let rows = values
        .into_iter()
        .map(|row_values| build_row_from_values(catalog, &table, &columns, row_values, &ctx))
        .collect::<Result<Vec<_>>>()?;

    insert_rows(txn, catalog, &table, table_name, rows, None, returning)
}

pub fn execute_insert_rows_with_plan<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    table_name: &str,
    columns: Vec<String>,
    values: Vec<Vec<SqlValue>>,
    conflict: Option<OnConflictPlan>,
    returning: Option<Projection>,
) -> Result<ExecutionResult> {
    let table = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    validate_columns(&table, &columns)?;
    let ctx = EvalContext::new(&[]);
    let rows = values
        .into_iter()
        .map(|row| build_row_from_values(catalog, &table, &columns, row, &ctx))
        .collect::<Result<Vec<_>>>()?;
    insert_rows(
        txn,
        catalog,
        &table,
        table_name,
        rows,
        conflict.as_ref(),
        returning,
    )
}

pub fn execute_insert_with_plan<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    table_name: &str,
    columns: Vec<String>,
    values: Vec<Vec<TypedExpr>>,
    conflict: Option<OnConflictPlan>,
    returning: Option<Projection>,
) -> Result<ExecutionResult> {
    let table = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    validate_columns(&table, &columns)?;
    let ctx = EvalContext::new(&[]);
    let rows = values
        .into_iter()
        .map(|row| build_row(catalog, &table, &columns, row, &ctx))
        .collect::<Result<Vec<_>>>()?;
    insert_rows(
        txn,
        catalog,
        &table,
        table_name,
        rows,
        conflict.as_ref(),
        returning,
    )
}

fn insert_rows<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    table: &TableMetadata,
    table_name: &str,
    rows: Vec<Vec<SqlValue>>,
    conflict: Option<&OnConflictPlan>,
    returning: Option<Projection>,
) -> Result<ExecutionResult> {
    let mut insert_rows = Vec::with_capacity(rows.len());
    let mut updated_rows: Vec<(u64, Vec<SqlValue>)> = Vec::new();
    for row in rows {
        if let Some(plan) = conflict {
            if let Some((row_id, old_row)) = find_conflict(txn, table, plan, &row)? {
                match &plan.action {
                    OnConflictActionPlan::DoNothing => continue,
                    OnConflictActionPlan::DoUpdate {
                        assignments,
                        selection,
                    } => {
                        if !predicate_matches(selection, &old_row)? {
                            continue;
                        }
                        let ctx = EvalContext::new(&old_row);
                        let mut new_row = old_row.clone();
                        for assignment in assignments {
                            let value = evaluate(&assignment.value, &ctx)?;
                            new_row[assignment.column_index] = normalize_assignment_value(
                                value,
                                &table.columns[assignment.column_index].data_type,
                            )?;
                        }
                        super::constraints::validate_row::<S, C, T>(
                            txn,
                            catalog,
                            table,
                            &new_row,
                            &[],
                        )?;
                        super::constraints::apply_parent_update::<S, C, T>(
                            txn, catalog, table, &old_row, &new_row, 0,
                        )?;
                        super::update::apply_changes(
                            txn,
                            catalog,
                            table,
                            &[(row_id, old_row, new_row.clone())],
                        )?;
                        updated_rows.push((row_id, new_row));
                        continue;
                    }
                }
            }
        }
        super::constraints::validate_row::<S, C, T>(txn, catalog, table, &row, &insert_rows)?;
        insert_rows.push(row);
    }
    let indexes: Vec<IndexMetadata> = catalog
        .get_indexes_for_table(table_name)
        .into_iter()
        .cloned()
        .collect();
    let (hnsw_indexes, indexes): (Vec<_>, Vec<_>) = indexes
        .into_iter()
        .partition(|idx| matches!(idx.method, Some(IndexMethod::Hnsw)));
    let (fts_indexes, btree_indexes): (Vec<_>, Vec<_>) = indexes
        .into_iter()
        .partition(|idx| matches!(idx.method, Some(IndexMethod::Fts)));

    let mut staged: Vec<(u64, Vec<SqlValue>)> = Vec::with_capacity(insert_rows.len());

    // Insert into table using a single handle; stage for index population.
    {
        let mut table_storage = txn.table_storage(table);
        for row in insert_rows {
            let row_id = table_storage
                .next_row_id()
                .map_err(|e| map_storage_error(table, e))?;
            table_storage
                .insert(row_id, &row)
                .map_err(|e| map_storage_error(table, e))?;
            staged.push((row_id, row));
        }
    }

    // Populate indexes using one handle per index for the whole batch.
    populate_indexes(txn, &btree_indexes, &staged)?;
    populate_fts_indexes(txn, &fts_indexes, &staged)?;
    populate_hnsw_indexes(txn, table, &hnsw_indexes, &staged)?;

    if let Some(projection) = returning {
        let columns = projected_columns(&projection, &table.columns)?;
        let mut rows = updated_rows
            .iter()
            .map(|(row_id, values)| {
                project_row_values(
                    &Row::new(*row_id, values.clone()),
                    &projection,
                    &table.columns,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        rows.extend(
            staged
                .iter()
                .map(|(row_id, values)| {
                    project_row_values(
                        &Row::new(*row_id, values.clone()),
                        &projection,
                        &table.columns,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        );
        Ok(ExecutionResult::Query(crate::executor::QueryResult::new(
            columns, rows,
        )))
    } else {
        Ok(ExecutionResult::RowsAffected(staged.len() as u64))
    }
}

fn find_conflict<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    table: &TableMetadata,
    plan: &OnConflictPlan,
    row: &[SqlValue],
) -> Result<Option<(u64, Vec<SqlValue>)>> {
    let names = if plan.columns.is_empty() {
        table.primary_key.clone().unwrap_or_default()
    } else {
        plan.columns.clone()
    };
    if names.is_empty() {
        return Ok(None);
    }
    let indices = names
        .iter()
        .filter_map(|name| table.get_column_index(name))
        .collect::<Vec<_>>();
    if indices.len() != names.len() {
        return Ok(None);
    }
    let mut storage = txn.table_storage(table);
    let mut iter = storage.range_scan(0, u64::MAX)?;
    while let Some(item) = iter.next() {
        let (row_id, existing) = item?;
        if indices
            .iter()
            .all(|&idx| !row[idx].is_null() && row[idx] == existing[idx])
        {
            return Ok(Some((row_id, existing)));
        }
    }
    Ok(None)
}

fn predicate_matches(filter: &Option<TypedExpr>, row: &[SqlValue]) -> Result<bool> {
    if let Some(expr) = filter {
        Ok(matches!(
            evaluate(expr, &EvalContext::new(row))?,
            SqlValue::Boolean(true)
        ))
    } else {
        Ok(true)
    }
}

fn populate_fts_indexes<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    indexes: &[IndexMetadata],
    rows: &[(u64, Vec<SqlValue>)],
) -> Result<()> {
    for index in indexes {
        for (row_id, row) in rows {
            FtsBridge::on_insert(txn, index, *row_id, row)?;
        }
    }
    Ok(())
}

fn build_row_from_values<C: Catalog + ?Sized>(
    catalog: &C,
    table: &TableMetadata,
    columns: &[String],
    values: Vec<SqlValue>,
    ctx: &EvalContext<'_>,
) -> Result<Vec<SqlValue>> {
    if values.len() != columns.len() {
        return Err(ExecutorError::InvalidOperation {
            operation: "INSERT".into(),
            reason: format!(
                "column/value count mismatch: {} vs {}",
                columns.len(),
                values.len()
            ),
        });
    }

    let mut row = vec![SqlValue::Null; table.column_count()];
    for (idx, value) in values.into_iter().enumerate() {
        let col_name = &columns[idx];
        let col_index = table
            .get_column_index(col_name)
            .ok_or_else(|| ExecutorError::ColumnNotFound(col_name.clone()))?;
        row[col_index] = normalize_assignment_value(value, &table.columns[col_index].data_type)?;
    }

    for (col_index, column) in table.columns.iter().enumerate() {
        if columns.iter().any(|name| name == &column.name) {
            continue;
        }
        if let Some(default) = column.default.as_ref() {
            row[col_index] = evaluate_default(catalog, table, default, column, ctx)?;
        }
    }

    Ok(row)
}

fn build_row<C: Catalog + ?Sized>(
    catalog: &C,
    table: &TableMetadata,
    columns: &[String],
    exprs: Vec<TypedExpr>,
    ctx: &EvalContext<'_>,
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

    for (idx, expr) in exprs.into_iter().enumerate() {
        let col_name = &columns[idx];
        let col_index = table
            .get_column_index(col_name)
            .ok_or_else(|| ExecutorError::ColumnNotFound(col_name.clone()))?;
        let value =
            normalize_assignment_value(evaluate(&expr, ctx)?, &table.columns[col_index].data_type)?;
        row[col_index] = value;
    }

    for (col_index, column) in table.columns.iter().enumerate() {
        if columns.iter().any(|name| name == &column.name) {
            continue;
        }
        if let Some(default) = column.default.as_ref() {
            row[col_index] = evaluate_default(catalog, table, default, column, ctx)?;
        }
    }

    Ok(row)
}

pub(crate) fn normalize_assignment_value(
    value: SqlValue,
    target_type: &crate::planner::types::ResolvedType,
) -> Result<SqlValue> {
    let compatible_vector = matches!(
        (target_type, &value),
        (
            crate::planner::types::ResolvedType::Vector { dimension, .. },
            SqlValue::Vector(values)
        ) if *dimension == values.len() as u32
    );
    if value.is_null() || value.resolved_type() == *target_type || compatible_vector {
        Ok(value)
    } else {
        coerce_value(value, target_type)
    }
}

pub(crate) fn evaluate_default<C: Catalog + ?Sized>(
    catalog: &C,
    table: &TableMetadata,
    default: &Expr,
    column: &ColumnMetadata,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let typed = TypeChecker::new(catalog).infer_type(default, table)?;
    normalize_assignment_value(evaluate(&typed, ctx)?, &column.data_type)
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

fn populate_indexes<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
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

fn populate_hnsw_indexes<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    table: &TableMetadata,
    indexes: &[IndexMetadata],
    rows: &[(u64, Vec<SqlValue>)],
) -> Result<()> {
    for index in indexes {
        for (row_id, row) in rows {
            HnswBridge::on_insert(txn, table, index, *row_id, row)?;
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
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
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
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let err = execute_insert(
            &mut txn,
            &catalog,
            "items",
            vec![], // omitting a primary key must fail
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
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
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
