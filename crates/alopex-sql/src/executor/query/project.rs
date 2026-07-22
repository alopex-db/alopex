use crate::catalog::ColumnMetadata;
use crate::executor::{ColumnInfo, Result};
use crate::planner::typed_expr::Projection;
use crate::storage::SqlValue;

use super::{Row, column_info_from_projection, column_infos_from_all, eval_expr};

/// Build the public output schema for a projection without executing rows.
pub fn projected_columns(
    projection: &Projection,
    schema: &[ColumnMetadata],
) -> Result<Vec<ColumnInfo>> {
    match projection {
        Projection::All(names) => column_infos_from_all(schema, names),
        Projection::Columns(cols) => Ok(cols
            .iter()
            .enumerate()
            .map(|(index, column)| column_info_from_projection(column, index))
            .collect()),
    }
}

/// Apply one projection with the same expression kernel as local query
/// execution. The distributed worker/assembler boundary uses this helper for
/// its already fenced, already authorized rows; it does not deserialize a
/// logical plan.
pub fn project_row_values(
    row: &Row,
    projection: &Projection,
    schema: &[ColumnMetadata],
) -> Result<Vec<SqlValue>> {
    match projection {
        Projection::All(names) if names.len() == schema.len() => Ok(row.values.clone()),
        Projection::All(names) => names
            .iter()
            .map(|name| {
                let index = schema
                    .iter()
                    .position(|column| &column.name == name)
                    .ok_or_else(|| crate::executor::ExecutorError::ColumnNotFound(name.clone()))?;
                row.values.get(index).cloned().ok_or_else(|| {
                    crate::executor::ExecutorError::InvalidOperation {
                        operation: "project".into(),
                        reason: format!("row is missing projected column '{name}'"),
                    }
                })
            })
            .collect(),
        Projection::Columns(columns) => columns
            .iter()
            .map(|column| eval_expr(&column.expr, row))
            .collect(),
    }
}

/// Project rows according to Projection, returning QueryResult.
pub fn execute_project(
    rows: Vec<Row>,
    projection: &Projection,
    schema: &[crate::catalog::ColumnMetadata],
) -> Result<crate::executor::QueryResult> {
    match projection {
        Projection::All(names) => project_all(rows, schema, names),
        Projection::Columns(cols) => project_columns(rows, cols),
    }
}

fn project_all(
    rows: Vec<Row>,
    schema: &[crate::catalog::ColumnMetadata],
    names: &[String],
) -> Result<crate::executor::QueryResult> {
    let projection = Projection::All(names.to_vec());
    let columns = projected_columns(&projection, schema)?;
    let projected_rows = rows
        .iter()
        .map(|row| project_row_values(row, &projection, schema))
        .collect::<Result<Vec<_>>>()?;
    Ok(crate::executor::QueryResult::new(columns, projected_rows))
}

fn project_columns(
    rows: Vec<Row>,
    cols: &[crate::planner::typed_expr::ProjectedColumn],
) -> Result<crate::executor::QueryResult> {
    let projection = Projection::Columns(cols.to_vec());
    let columns = projected_columns(&projection, &[])?;
    let projected_rows = rows
        .iter()
        .map(|row| project_row_values(row, &projection, &[]))
        .collect::<Result<Vec<_>>>()?;

    Ok(crate::executor::QueryResult::new(columns, projected_rows))
}
