#![allow(clippy::single_match, clippy::while_let_on_iterator)]

use alopex_core::kv::KVStore;

use crate::ast::ddl::{ReferentialAction, TableConstraint};
use crate::catalog::{Catalog, TableMetadata};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{ConstraintViolation, ExecutorError, Result};
use crate::planner::type_checker::TypeChecker;
use crate::storage::{SqlTxn, SqlValue};

pub(super) fn validate_row<'txn, S, C, T>(
    txn: &mut T,
    catalog: &C,
    table: &TableMetadata,
    row: &[SqlValue],
    pending: &[Vec<SqlValue>],
) -> Result<()>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
{
    validate_checks(catalog, table, row)?;
    for constraint in &table.constraints {
        match constraint {
            TableConstraint::ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_columns,
                ..
            } => {
                let local = column_values(table, columns, row)?;
                if local.iter().any(SqlValue::is_null) {
                    continue;
                }
                let target = catalog
                    .get_table(referenced_table)
                    .ok_or_else(|| ExecutorError::TableNotFound(referenced_table.clone()))?;
                let target_indexes = column_indexes(target, referenced_columns)?;
                let pending_match = referenced_table == &table.name
                    && pending
                        .iter()
                        .any(|candidate| values_match(candidate, &target_indexes, &local));
                if !pending_match && !stored_match::<S, T>(txn, target, &target_indexes, &local)? {
                    return Err(ConstraintViolation::ForeignKey {
                        constraint: name
                            .clone()
                            .unwrap_or_else(|| format!("{}({})", table.name, columns.join(", "))),
                    }
                    .into());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_checks<C: Catalog + ?Sized>(
    catalog: &C,
    table: &TableMetadata,
    row: &[SqlValue],
) -> Result<()> {
    for constraint in &table.constraints {
        if let TableConstraint::Check {
            name, expression, ..
        } = constraint
        {
            let typed = TypeChecker::new(catalog).infer_type(expression, table)?;
            if matches!(
                evaluate(&typed, &EvalContext::new(row))?,
                SqlValue::Boolean(false)
            ) {
                return Err(ConstraintViolation::Check {
                    constraint: name.clone().unwrap_or_else(|| "CHECK".into()),
                }
                .into());
            }
        }
    }
    Ok(())
}

fn column_indexes(table: &TableMetadata, columns: &[String]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|column| {
            table
                .get_column_index(column)
                .ok_or_else(|| ExecutorError::ColumnNotFound(column.clone()))
        })
        .collect()
}

fn column_values(
    table: &TableMetadata,
    columns: &[String],
    row: &[SqlValue],
) -> Result<Vec<SqlValue>> {
    Ok(column_indexes(table, columns)?
        .into_iter()
        .map(|index| row[index].clone())
        .collect())
}

fn values_match(row: &[SqlValue], indexes: &[usize], values: &[SqlValue]) -> bool {
    indexes
        .iter()
        .zip(values)
        .all(|(&index, value)| row.get(index) == Some(value))
}

fn stored_match<'txn, S, T>(
    txn: &mut T,
    table: &TableMetadata,
    indexes: &[usize],
    values: &[SqlValue],
) -> Result<bool>
where
    S: KVStore + 'txn,
    T: SqlTxn<'txn, S>,
{
    let mut storage = txn.table_storage(table);
    let mut rows = storage.range_scan(0, u64::MAX)?;
    while let Some(row) = rows.next() {
        if values_match(&row?.1, indexes, values) {
            return Ok(true);
        }
    }
    Ok(false)
}

const MAX_CASCADE_DEPTH: usize = 64;

pub(super) fn apply_parent_delete<'txn, S, C, T>(
    txn: &mut T,
    catalog: &C,
    parent: &TableMetadata,
    parent_row: &[SqlValue],
    depth: usize,
) -> Result<()>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
{
    ensure_cascade_depth(depth)?;
    for child in catalog.list_tables() {
        for constraint in child.constraints.clone() {
            let TableConstraint::ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                ..
            } = constraint
            else {
                continue;
            };
            if referenced_table != parent.name {
                continue;
            }
            let key = column_values(parent, &referenced_columns, parent_row)?;
            let child_indexes = column_indexes(&child, &columns)?;
            let matches = matching_rows::<S, T>(txn, &child, &child_indexes, &key)?;
            if matches.is_empty() {
                continue;
            }
            match on_delete {
                ReferentialAction::NoAction | ReferentialAction::Restrict => {
                    return Err(foreign_key_error(name, &child, &columns));
                }
                ReferentialAction::Cascade => {
                    for (_, row) in &matches {
                        apply_parent_delete::<S, C, T>(txn, catalog, &child, row, depth + 1)?;
                    }
                    super::delete::apply_deletes::<S, C, T>(txn, catalog, &child, &matches)?;
                }
                ReferentialAction::SetNull => {
                    let mut changes = Vec::with_capacity(matches.len());
                    for (row_id, old) in matches {
                        let mut new = old.clone();
                        for &index in &child_indexes {
                            new[index] = SqlValue::Null;
                        }
                        validate_checks(catalog, &child, &new)?;
                        apply_parent_update::<S, C, T>(
                            txn,
                            catalog,
                            &child,
                            &old,
                            &new,
                            depth + 1,
                        )?;
                        changes.push((row_id, old, new));
                    }
                    super::update::apply_changes::<S, C, T>(txn, catalog, &child, &changes)?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn apply_parent_update<'txn, S, C, T>(
    txn: &mut T,
    catalog: &C,
    parent: &TableMetadata,
    old_parent: &[SqlValue],
    new_parent: &[SqlValue],
    depth: usize,
) -> Result<()>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
{
    ensure_cascade_depth(depth)?;
    for child in catalog.list_tables() {
        for constraint in child.constraints.clone() {
            let TableConstraint::ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_columns,
                on_update,
                ..
            } = constraint
            else {
                continue;
            };
            if referenced_table != parent.name {
                continue;
            }
            let old_key = column_values(parent, &referenced_columns, old_parent)?;
            let new_key = column_values(parent, &referenced_columns, new_parent)?;
            if old_key == new_key {
                continue;
            }
            let child_indexes = column_indexes(&child, &columns)?;
            let matches = matching_rows::<S, T>(txn, &child, &child_indexes, &old_key)?;
            if matches.is_empty() {
                continue;
            }
            if matches!(
                on_update,
                ReferentialAction::NoAction | ReferentialAction::Restrict
            ) {
                return Err(foreign_key_error(name, &child, &columns));
            }
            let mut changes = Vec::with_capacity(matches.len());
            for (row_id, old) in matches {
                let mut new = old.clone();
                for (position, &index) in child_indexes.iter().enumerate() {
                    new[index] = if on_update == ReferentialAction::SetNull {
                        SqlValue::Null
                    } else {
                        new_key[position].clone()
                    };
                }
                validate_checks(catalog, &child, &new)?;
                apply_parent_update::<S, C, T>(txn, catalog, &child, &old, &new, depth + 1)?;
                changes.push((row_id, old, new));
            }
            super::update::apply_changes::<S, C, T>(txn, catalog, &child, &changes)?;
        }
    }
    Ok(())
}

fn matching_rows<'txn, S, T>(
    txn: &mut T,
    table: &TableMetadata,
    indexes: &[usize],
    values: &[SqlValue],
) -> Result<Vec<(u64, Vec<SqlValue>)>>
where
    S: KVStore + 'txn,
    T: SqlTxn<'txn, S>,
{
    let mut result = Vec::new();
    let mut storage = txn.table_storage(table);
    let mut rows = storage.range_scan(0, u64::MAX)?;
    while let Some(row) = rows.next() {
        let row = row?;
        if values_match(&row.1, indexes, values) {
            result.push(row);
        }
    }
    Ok(result)
}

fn ensure_cascade_depth(depth: usize) -> Result<()> {
    if depth <= MAX_CASCADE_DEPTH {
        Ok(())
    } else {
        Err(ExecutorError::InvalidOperation {
            operation: "FOREIGN KEY CASCADE".into(),
            reason: format!("cascade depth exceeds {MAX_CASCADE_DEPTH}"),
        })
    }
}

fn foreign_key_error(
    name: Option<String>,
    table: &TableMetadata,
    columns: &[String],
) -> ExecutorError {
    ConstraintViolation::ForeignKey {
        constraint: name.unwrap_or_else(|| format!("{}({})", table.name, columns.join(", "))),
    }
    .into()
}
