use std::collections::HashSet;

use alopex_core::kv::KVStore;

use crate::catalog::{Catalog, StorageType, TableMetadata};
use crate::executor::query::columnar_scan::{ColumnarScan, create_columnar_scan_iterator};
use crate::executor::query::statement_subqueries::DmlSubqueries;
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::executor::{Row, RowIterator};
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
    super::reject_columnar_dml(&target, "MERGE")?;
    let source = catalog
        .get_table(source_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(source_name.to_string()))?;
    let target_rows = read_rows(txn, &target)?;
    let source_rows = read_rows(txn, &source)?;

    let mut expressions = vec![&on];
    for clause in &clauses {
        expressions.extend(clause.condition.iter());
        match &clause.action {
            MergeActionPlan::Update { assignments } => {
                expressions.extend(assignments.iter().map(|assignment| &assignment.value));
            }
            MergeActionPlan::Insert { values, .. } => expressions.extend(values.iter()),
            MergeActionPlan::DoNothing => {}
        }
    }
    let subqueries = DmlSubqueries::new(txn, catalog, expressions);

    let mut matched_targets = HashSet::new();
    let mut changes = Vec::new();
    let mut inserts: Vec<(Vec<String>, Vec<Vec<SqlValue>>)> = Vec::new();

    for (source_row_id, source_row) in source_rows {
        let mut matches = Vec::new();
        for (row_id, target_row) in &target_rows {
            let joined = Row::new(*row_id, joined_row(target_row, &source_row));
            if expression_is_true(txn, catalog, &subqueries, &on, &joined)? {
                matches.push((*row_id, target_row));
            }
        }

        if matches.is_empty() {
            let mut joined = vec![SqlValue::Null; target.column_count()];
            joined.extend(source_row);
            // No target row exists for an unmatched source row.
            let joined = Row::new(source_row_id, joined);
            if let Some(clause) =
                applicable_clause(txn, catalog, &subqueries, &clauses, false, &joined)?
            {
                match &clause.action {
                    MergeActionPlan::Insert { columns, values } => {
                        let row = values
                            .iter()
                            .map(|value| subqueries.evaluate(txn, catalog, value, &joined))
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
            let joined = Row::new(row_id, joined_row(target_row, &source_row));
            let Some(clause) =
                applicable_clause(txn, catalog, &subqueries, &clauses, true, &joined)?
            else {
                continue;
            };
            match &clause.action {
                MergeActionPlan::Update { assignments } => {
                    let mut new_row = target_row.clone();
                    for assignment in assignments {
                        let value =
                            subqueries.evaluate(txn, catalog, &assignment.value, &joined)?;
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

    subqueries.finish()?;
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
    if table.storage_options.storage_type == StorageType::Columnar {
        let scan = ColumnarScan::new(
            table.table_id,
            (0..table.column_count()).collect(),
            None,
            None,
        );
        let mut iterator = create_columnar_scan_iterator(txn, table, &scan)?;
        let mut rows = Vec::new();
        while let Some(row) = iterator.next_row() {
            let row = row?;
            rows.push((row.row_id, row.values));
        }
        return Ok(rows);
    }
    let mut storage = txn.table_storage(table);
    let iterator = storage.range_scan(0, u64::MAX)?;
    let mut rows = Vec::new();
    for row in iterator {
        rows.push(row?);
    }
    Ok(rows)
}

fn applicable_clause<'a, 'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    subqueries: &DmlSubqueries<'_>,
    clauses: &'a [MergeClausePlan],
    matched: bool,
    row: &Row,
) -> Result<Option<&'a MergeClausePlan>> {
    for clause in clauses.iter().filter(|clause| clause.matched == matched) {
        if clause
            .condition
            .as_ref()
            .map(|condition| expression_is_true(txn, catalog, subqueries, condition, row))
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

fn expression_is_true<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    subqueries: &DmlSubqueries<'_>,
    expression: &TypedExpr,
    row: &Row,
) -> Result<bool> {
    Ok(matches!(
        subqueries.evaluate(txn, catalog, expression, row)?,
        SqlValue::Boolean(true)
    ))
}
