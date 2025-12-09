use crate::executor::Result;
use crate::planner::typed_expr::Projection;

use super::{Row, column_info_from_projection, column_infos_from_all, eval_expr};

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
    let columns = column_infos_from_all(schema, names)?;
    let mut projected_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(names.len());
        for name in names {
            let idx = schema
                .iter()
                .position(|c| &c.name == name)
                .ok_or_else(|| crate::executor::ExecutorError::ColumnNotFound(name.clone()))?;
            values.push(row.values[idx].clone());
        }
        projected_rows.push(values);
    }
    Ok(crate::executor::QueryResult::new(columns, projected_rows))
}

fn project_columns(
    rows: Vec<Row>,
    cols: &[crate::planner::typed_expr::ProjectedColumn],
) -> Result<crate::executor::QueryResult> {
    let columns: Vec<_> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| column_info_from_projection(c, i))
        .collect();

    let mut projected_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(cols.len());
        for col in cols {
            values.push(eval_expr(&col.expr, &row)?);
        }
        projected_rows.push(values);
    }

    Ok(crate::executor::QueryResult::new(columns, projected_rows))
}
