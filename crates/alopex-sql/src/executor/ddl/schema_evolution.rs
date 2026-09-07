use alopex_core::kv::KVStore;

use crate::TableType;
use crate::ast::{AlterColumnAction, AlterTableAction, IndexMethod};
use crate::catalog::{Catalog, IndexMetadata, TableMetadata, VIEW_DEPENDENCIES_PROPERTY};
use crate::executor::dml::{evaluate_default, normalize_assignment_value};
use crate::executor::evaluator::EvalContext;
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::planner::column_metadata_from_def;
use crate::storage::{KeyEncoder, SqlTxn, SqlValue};

use super::persistence::{delete_index, delete_table, persist_index, persist_table};

pub struct AlterOutcome {
    pub old_table: TableMetadata,
    pub new_table: TableMetadata,
    pub updated_indexes: Vec<(IndexMetadata, IndexMetadata)>,
}

pub fn dependent_views<C: Catalog + ?Sized>(catalog: &C, relation: &str) -> Vec<String> {
    let mut dependents = catalog
        .list_tables()
        .into_iter()
        .filter(|table| table.table_type == TableType::View)
        .filter(|table| {
            table
                .properties
                .get(VIEW_DEPENDENCIES_PROPERTY)
                .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
                .is_some_and(|dependencies| dependencies.iter().any(|name| name == relation))
        })
        .map(|table| table.name)
        .collect::<Vec<_>>();
    dependents.sort();
    dependents
}

pub fn ensure_no_dependent_views<C: Catalog + ?Sized>(catalog: &C, relation: &str) -> Result<()> {
    let dependents = dependent_views(catalog, relation);
    if dependents.is_empty() {
        Ok(())
    } else {
        Err(ExecutorError::InvalidOperation {
            operation: "DependencyCheck".into(),
            reason: format!(
                "relation '{relation}' is referenced by view(s): {}",
                dependents.join(", ")
            ),
        })
    }
}

pub fn prepare_drop_view<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    name: &str,
    if_exists: bool,
) -> Result<Option<TableMetadata>> {
    let Some(view) = catalog.get_table(name).cloned() else {
        return if if_exists {
            Ok(None)
        } else {
            Err(ExecutorError::TableNotFound(name.to_string()))
        };
    };
    if view.table_type != TableType::View {
        return Err(ExecutorError::InvalidOperation {
            operation: "DROP VIEW".into(),
            reason: format!("'{name}' is not a view"),
        });
    }
    ensure_no_dependent_views(catalog, name)?;
    delete_table(txn.inner_mut(), &view)?;
    Ok(Some(view))
}

pub fn prepare_alter<'txn, S, C>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    table_name: &str,
    action: AlterTableAction,
) -> Result<AlterOutcome>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
{
    let old_table = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    if old_table.table_type == TableType::View {
        return Err(ExecutorError::InvalidOperation {
            operation: "ALTER TABLE".into(),
            reason: format!("'{}' is a view", old_table.name),
        });
    }
    ensure_no_dependent_views(catalog, table_name)?;

    let mut new_table = old_table.clone();
    let mut updated_indexes = catalog
        .get_indexes_for_table(table_name)
        .into_iter()
        .map(|index| (index.clone(), index.clone()))
        .collect::<Vec<_>>();
    match action {
        AlterTableAction::AddColumn {
            if_not_exists,
            column,
        } => {
            if new_table.get_column(&column.name).is_some() {
                if if_not_exists {
                    return Ok(AlterOutcome {
                        old_table: old_table.clone(),
                        new_table: old_table,
                        updated_indexes,
                    });
                }
                return Err(ExecutorError::InvalidOperation {
                    operation: "ALTER TABLE ADD COLUMN".into(),
                    reason: format!("column '{}' already exists", column.name),
                });
            }
            let metadata = column_metadata_from_def(&column);
            if metadata.primary_key || metadata.unique {
                return Err(ExecutorError::UnsupportedOperation(
                    "ALTER TABLE ADD COLUMN supports DEFAULT and NOT NULL; add key indexes separately"
                        .into(),
                ));
            }
            let ctx = EvalContext::new(&[]);
            let value = match metadata.default.as_ref() {
                Some(default) => evaluate_default(catalog, &new_table, default, &metadata, &ctx)?,
                None => SqlValue::Null,
            };
            let rows = scan_rows(txn, &old_table)?;
            if metadata.not_null && value.is_null() && !rows.is_empty() {
                return Err(ExecutorError::InvalidOperation {
                    operation: "ALTER TABLE ADD COLUMN".into(),
                    reason: format!(
                        "NOT NULL column '{}' requires a non-NULL default",
                        metadata.name
                    ),
                });
            }
            new_table.columns.push(metadata);
            rewrite_rows(txn, &new_table, rows, |mut row| {
                row.push(value.clone());
                Ok(row)
            })?;
        }
        AlterTableAction::DropColumn { if_exists, name } => {
            let Some(index) = new_table.get_column_index(&name) else {
                if if_exists {
                    return Ok(AlterOutcome {
                        old_table: old_table.clone(),
                        new_table: old_table,
                        updated_indexes,
                    });
                }
                return Err(ExecutorError::ColumnNotFound(name));
            };
            ensure_column_not_indexed(catalog, table_name, &name)?;
            if new_table.columns.len() == 1 {
                return Err(ExecutorError::InvalidOperation {
                    operation: "ALTER TABLE DROP COLUMN".into(),
                    reason: "a table must retain at least one column".into(),
                });
            }
            new_table.columns.remove(index);
            let rows = scan_rows(txn, &old_table)?;
            rewrite_rows(txn, &new_table, rows, |mut row| {
                row.remove(index);
                Ok(row)
            })?;
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            if new_table.get_column(&new_name).is_some() {
                return Err(ExecutorError::InvalidOperation {
                    operation: "ALTER TABLE RENAME COLUMN".into(),
                    reason: format!("column '{new_name}' already exists"),
                });
            }
            let column = new_table
                .columns
                .iter_mut()
                .find(|column| column.name == old_name)
                .ok_or_else(|| ExecutorError::ColumnNotFound(old_name.clone()))?;
            column.name = new_name.clone();
            if let Some(primary_key) = &mut new_table.primary_key {
                for name in primary_key {
                    if *name == old_name {
                        *name = new_name.clone();
                    }
                }
            }
            for (_, updated) in &mut updated_indexes {
                if updated.columns.iter().any(|column| column == &old_name) {
                    for column in &mut updated.columns {
                        if *column == old_name {
                            *column = new_name.clone();
                        }
                    }
                }
            }
        }
        AlterTableAction::RenameTable { new_name } => {
            if catalog.table_exists(&new_name) {
                return Err(ExecutorError::TableAlreadyExists(new_name));
            }
            new_table.name = new_name.clone();
            for (_, updated) in &mut updated_indexes {
                updated.table = new_name.clone();
            }
        }
        AlterTableAction::AlterColumn { name, action } => {
            let index = new_table
                .get_column_index(&name)
                .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))?;
            match action {
                AlterColumnAction::SetDataType { data_type } => {
                    ensure_column_not_indexed(catalog, table_name, &name)?;
                    let target = crate::planner::ResolvedType::from_ast(&data_type);
                    let rows = scan_rows(txn, &old_table)?;
                    new_table.columns[index].data_type = target.clone();
                    rewrite_rows(txn, &new_table, rows, |mut row| {
                        row[index] = normalize_assignment_value(row[index].clone(), &target)?;
                        Ok(row)
                    })?;
                }
                AlterColumnAction::SetDefault { value } => {
                    new_table.columns[index].default = Some(*value);
                }
                AlterColumnAction::DropDefault => new_table.columns[index].default = None,
                AlterColumnAction::SetNotNull => {
                    if scan_rows(txn, &old_table)?
                        .iter()
                        .any(|(_, row)| row[index].is_null())
                    {
                        return Err(ExecutorError::InvalidOperation {
                            operation: "ALTER TABLE SET NOT NULL".into(),
                            reason: format!("column '{name}' contains NULL values"),
                        });
                    }
                    new_table.columns[index].not_null = true;
                }
                AlterColumnAction::DropNotNull => {
                    if new_table.columns[index].primary_key {
                        return Err(ExecutorError::InvalidOperation {
                            operation: "ALTER TABLE DROP NOT NULL".into(),
                            reason: format!("primary key column '{name}' must remain NOT NULL"),
                        });
                    }
                    new_table.columns[index].not_null = false;
                }
            }
        }
    }

    if old_table.name != new_table.name {
        delete_table(txn.inner_mut(), &old_table)?;
    }
    persist_table(txn.inner_mut(), &new_table)?;
    for (old, new) in &updated_indexes {
        if old.table != new.table {
            delete_index(txn.inner_mut(), old)?;
        }
        persist_index(txn.inner_mut(), new)?;
    }

    Ok(AlterOutcome {
        old_table,
        new_table,
        updated_indexes,
    })
}

pub fn execute_truncate<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    table_name: &str,
) -> Result<ExecutionResult> {
    let table = catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    if table.table_type == TableType::View {
        return Err(ExecutorError::InvalidOperation {
            operation: "TRUNCATE".into(),
            reason: format!("'{table_name}' is a view"),
        });
    }
    txn.delete_prefix(&KeyEncoder::table_prefix(table.table_id))?;
    txn.delete_prefix(&KeyEncoder::sequence_key(table.table_id))?;
    for index in catalog.get_indexes_for_table(table_name) {
        if matches!(index.method, Some(IndexMethod::Hnsw)) {
            HnswBridge::drop_index(txn, index, false)?;
            HnswBridge::create_index(txn, table, index)?;
        } else {
            txn.delete_prefix(&KeyEncoder::index_prefix(index.index_id))?;
        }
    }
    Ok(ExecutionResult::Success)
}

fn ensure_column_not_indexed<C: Catalog + ?Sized>(
    catalog: &C,
    table: &str,
    column: &str,
) -> Result<()> {
    if let Some(index) = catalog
        .get_indexes_for_table(table)
        .into_iter()
        .find(|index| index.columns.iter().any(|name| name == column))
    {
        Err(ExecutorError::InvalidOperation {
            operation: "ALTER TABLE".into(),
            reason: format!("column '{column}' is referenced by index '{}'", index.name),
        })
    } else {
        Ok(())
    }
}

fn scan_rows<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table: &TableMetadata,
) -> Result<Vec<(u64, Vec<SqlValue>)>> {
    txn.table_storage(table)
        .scan()?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(ExecutorError::from)
}

fn rewrite_rows<'txn, S, F>(
    txn: &mut impl SqlTxn<'txn, S>,
    table: &TableMetadata,
    rows: Vec<(u64, Vec<SqlValue>)>,
    mut transform: F,
) -> Result<()>
where
    S: KVStore + 'txn,
    F: FnMut(Vec<SqlValue>) -> Result<Vec<SqlValue>>,
{
    let mut storage = txn.table_storage(table);
    for (row_id, row) in rows {
        storage.update(row_id, &transform(row)?)?;
    }
    Ok(())
}
