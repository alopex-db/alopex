use alopex_core::kv::KVStore;

use crate::catalog::Catalog;
use crate::executor::evaluator::EvalContext;
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::Projection;
use crate::storage::{SqlTransaction, SqlValue};

use super::{ColumnInfo, Row};

mod filter;
mod limit;
mod project;
mod scan;
mod sort;

/// Execute a SELECT logical plan and return a query result.
pub fn execute_query<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    plan: LogicalPlan,
) -> Result<ExecutionResult> {
    let (rows, schema, projection) = execute_plan(txn, catalog, plan)?;
    let result = project::execute_project(rows, &projection, &schema)?;
    Ok(ExecutionResult::Query(result))
}

fn execute_plan<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    plan: LogicalPlan,
) -> Result<(Vec<Row>, Vec<crate::catalog::ColumnMetadata>, Projection)> {
    match plan {
        LogicalPlan::Scan { table, projection } => {
            let table_meta = catalog
                .get_table(&table)
                .cloned()
                .ok_or_else(|| ExecutorError::TableNotFound(table))?;
            let schema = table_meta.columns.clone();
            let rows = scan::execute_scan(txn, &table_meta)?;
            Ok((rows, schema, projection))
        }
        LogicalPlan::Filter { input, predicate } => {
            let (rows, schema, projection) = execute_plan(txn, catalog, *input)?;
            let filtered = filter::execute_filter(rows, &predicate)?;
            Ok((filtered, schema, projection))
        }
        LogicalPlan::Sort { input, order_by } => {
            let (rows, schema, projection) = execute_plan(txn, catalog, *input)?;
            let sorted = sort::execute_sort(rows, &order_by)?;
            Ok((sorted, schema, projection))
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let (rows, schema, projection) = execute_plan(txn, catalog, *input)?;
            let limited = limit::execute_limit(rows, limit, offset);
            Ok((limited, schema, projection))
        }
        other => Err(ExecutorError::UnsupportedOperation(format!(
            "unsupported query plan: {other:?}"
        ))),
    }
}

/// Evaluate a typed expression against a row, returning SqlValue.
fn eval_expr(expr: &crate::planner::typed_expr::TypedExpr, row: &Row) -> Result<SqlValue> {
    let ctx = EvalContext::new(&row.values);
    crate::executor::evaluator::evaluate(expr, &ctx)
}

/// Build column info name using alias fallback.
fn column_name_from_projection(
    projected: &crate::planner::typed_expr::ProjectedColumn,
    idx: usize,
) -> String {
    projected
        .alias
        .clone()
        .or_else(|| match &projected.expr.kind {
            crate::planner::typed_expr::TypedExprKind::ColumnRef { column, .. } => {
                Some(column.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| format!("col_{idx}"))
}

/// Build ColumnInfo from projection.
fn column_info_from_projection(
    projected: &crate::planner::typed_expr::ProjectedColumn,
    idx: usize,
) -> ColumnInfo {
    ColumnInfo::new(
        column_name_from_projection(projected, idx),
        projected.expr.resolved_type.clone(),
    )
}

/// Build ColumnInfo for Projection::All using schema.
fn column_infos_from_all(
    schema: &[crate::catalog::ColumnMetadata],
    names: &[String],
) -> Result<Vec<ColumnInfo>> {
    names
        .iter()
        .map(|name| {
            let col = schema
                .iter()
                .find(|c| &c.name == name)
                .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))?;
            Ok(ColumnInfo::new(name.clone(), col.data_type.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::typed_expr::TypedExpr;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    #[test]
    fn execute_query_scan_only_returns_rows() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut catalog = MemoryCatalog::new();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        );
        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), false).unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        crate::executor::dml::execute_insert(
            &mut txn,
            &catalog,
            "users",
            vec!["id".into(), "name".into()],
            vec![vec![
                TypedExpr::literal(
                    crate::ast::expr::Literal::Number("1".into()),
                    ResolvedType::Integer,
                    crate::Span::default(),
                ),
                TypedExpr::literal(
                    crate::ast::expr::Literal::String("alice".into()),
                    ResolvedType::Text,
                    crate::Span::default(),
                ),
            ]],
        )
        .unwrap();

        let result = execute_query(
            &mut txn,
            &catalog,
            LogicalPlan::scan(
                "users".into(),
                Projection::All(vec!["id".into(), "name".into()]),
            ),
        )
        .unwrap();

        match result {
            ExecutionResult::Query(q) => {
                assert_eq!(q.rows.len(), 1);
                assert_eq!(q.columns.len(), 2);
                assert_eq!(
                    q.rows[0],
                    vec![SqlValue::Integer(1), SqlValue::Text("alice".into())]
                );
            }
            other => panic!("unexpected result {other:?}"),
        }
    }
}
