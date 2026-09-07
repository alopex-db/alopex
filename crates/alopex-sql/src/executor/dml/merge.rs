use std::collections::HashSet;

use alopex_core::kv::KVStore;

use crate::catalog::{Catalog, TableMetadata};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::planner::{MergeActionPlan, MergeClausePlan, TypedExpr};
use crate::storage::{SqlTxn, SqlValue};

pub fn execute_merge<'txn, S, C, T>(
    txn: &mut T,
    catalog: &C,
    target_name: &str,
    source_name: &str,
    on: TypedExpr,
    clauses: Vec<MergeClausePlan>,
) -> Result<ExecutionResult>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
{
    let target = catalog
        .get_table(target_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(target_name.to_string()))?;
    let source = catalog
        .get_table(source_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(source_name.to_string()))?;
    let target_rows = read_rows(txn, &target)?;
    let source_rows = read_rows(txn, &source)?;

    let mut matched_targets = HashSet::new();
    let mut changes = Vec::new();
    let mut inserts: Vec<(Vec<String>, Vec<Vec<SqlValue>>)> = Vec::new();

    for (_, source_row) in source_rows {
        let mut matches = Vec::new();
        for (row_id, target_row) in &target_rows {
            let joined = joined_row(target_row, &source_row);
            if expression_is_true(&on, &joined)? {
                matches.push((*row_id, target_row));
            }
        }

        if matches.is_empty() {
            let mut joined = vec![SqlValue::Null; target.column_count()];
            joined.extend(source_row);
            if let Some(clause) = applicable_clause(&clauses, false, &joined)? {
                match &clause.action {
                    MergeActionPlan::Insert { columns, values } => {
                        let row = values
                            .iter()
                            .map(|value| evaluate(value, &EvalContext::new(&joined)))
                            .collect::<Result<Vec<_>>>()?;
                        if let Some((_, rows)) = inserts
                            .iter_mut()
                            .find(|(existing_columns, _)| existing_columns == columns)
                        {
                            rows.push(row);
                        } else {
                            inserts.push((columns.clone(), vec![row]));
                        }
                    }
                    MergeActionPlan::DoNothing => {}
                    MergeActionPlan::Update { .. } => unreachable!("planner rejects this clause"),
                }
            }
            continue;
        }

        for (row_id, target_row) in matches {
            if !matched_targets.insert(row_id) {
                return Err(ExecutorError::InvalidOperation {
                    operation: "MERGE".into(),
                    reason: "target row matched more than once".into(),
                });
            }
            let joined = joined_row(target_row, &source_row);
            let Some(clause) = applicable_clause(&clauses, true, &joined)? else {
                continue;
            };
            match &clause.action {
                MergeActionPlan::Update { assignments } => {
                    let mut new_row = target_row.clone();
                    for assignment in assignments {
                        let value = evaluate(&assignment.value, &EvalContext::new(&joined))?;
                        let column = &target.columns[assignment.column_index];
                        let value = super::normalize_assignment_value(value, &column.data_type)?;
                        if (column.not_null || column.primary_key) && value.is_null() {
                            return Err(ConstraintViolation::NotNull {
                                column: column.name.clone(),
                            }
                            .into());
                        }
                        new_row[assignment.column_index] = value;
                    }
                    if new_row != *target_row {
                        changes.push((row_id, target_row.clone(), new_row));
                    }
                }
                MergeActionPlan::DoNothing => {}
                MergeActionPlan::Insert { .. } => unreachable!("planner rejects this clause"),
            }
        }
    }

    for (_, _, new_row) in &changes {
        super::constraints::validate_row::<S, C, T>(txn, catalog, &target, new_row, &[])?;
    }
    for (_, old_row, new_row) in &changes {
        super::constraints::apply_parent_update::<S, C, T>(
            txn, catalog, &target, old_row, new_row, 0,
        )?;
    }
    super::update::apply_changes(txn, catalog, &target, &changes)?;

    let mut rows_affected = changes.len() as u64;
    for (columns, rows) in inserts {
        rows_affected += rows.len() as u64;
        super::insert::execute_insert_rows_with_plan(
            txn,
            catalog,
            target_name,
            columns,
            rows,
            None,
            None,
        )?;
    }
    Ok(ExecutionResult::RowsAffected(rows_affected))
}

fn read_rows<'txn, S, T>(txn: &mut T, table: &TableMetadata) -> Result<Vec<(u64, Vec<SqlValue>)>>
where
    S: KVStore + 'txn,
    T: SqlTxn<'txn, S>,
{
    let mut storage = txn.table_storage(table);
    let iterator = storage.range_scan(0, u64::MAX)?;
    let mut rows = Vec::new();
    for row in iterator {
        rows.push(row?);
    }
    Ok(rows)
}

fn applicable_clause<'a>(
    clauses: &'a [MergeClausePlan],
    matched: bool,
    row: &[SqlValue],
) -> Result<Option<&'a MergeClausePlan>> {
    for clause in clauses.iter().filter(|clause| clause.matched == matched) {
        if clause
            .condition
            .as_ref()
            .map(|condition| expression_is_true(condition, row))
            .transpose()?
            .unwrap_or(true)
        {
            return Ok(Some(clause));
        }
    }
    Ok(None)
}

fn joined_row(target: &[SqlValue], source: &[SqlValue]) -> Vec<SqlValue> {
    let mut joined = Vec::with_capacity(target.len() + source.len());
    joined.extend_from_slice(target);
    joined.extend_from_slice(source);
    joined
}

fn expression_is_true(expression: &TypedExpr, row: &[SqlValue]) -> Result<bool> {
    Ok(matches!(
        evaluate(expression, &EvalContext::new(row))?,
        SqlValue::Boolean(true)
    ))
}
