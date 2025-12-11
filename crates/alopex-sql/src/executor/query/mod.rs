use alopex_core::kv::KVStore;

use crate::catalog::{Catalog, StorageType};
use crate::executor::evaluator::EvalContext;
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::Projection;
use crate::storage::{SqlTransaction, SqlValue};

use super::{ColumnInfo, Row};

mod columnar_scan;
pub mod iterator;
mod project;
mod scan;

pub use iterator::{FilterIterator, LimitIterator, RowIterator, SortIterator};

/// Execute a SELECT logical plan and return a query result.
///
/// This function uses an iterator-based execution model that processes rows
/// through a pipeline of operators. This approach:
/// - Enables early termination for LIMIT queries
/// - Provides streaming execution after the initial scan
/// - Allows composable query operators
///
/// Note: The Scan stage reads all matching rows into memory, but subsequent
/// operators (Filter, Sort, Limit) process rows through an iterator pipeline.
/// Sort operations additionally require materializing all input rows.
pub fn execute_query<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    plan: LogicalPlan,
) -> Result<ExecutionResult> {
    let (mut iter, projection, schema) = build_iterator_pipeline(txn, catalog, plan)?;

    // Collect rows from iterator and apply projection
    let mut rows = Vec::new();
    while let Some(result) = iter.next_row() {
        rows.push(result?);
    }

    let result = project::execute_project(rows, &projection, &schema)?;
    Ok(ExecutionResult::Query(result))
}

/// Build an iterator pipeline from a logical plan.
///
/// This recursively constructs a tree of iterators that mirrors the logical plan
/// structure. The scan phase reads rows into memory, then subsequent operators
/// process them through an iterator pipeline enabling streaming execution and
/// early termination.
fn build_iterator_pipeline<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    plan: LogicalPlan,
) -> Result<(
    Box<dyn RowIterator>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    match plan {
        LogicalPlan::Scan { table, projection } => {
            let table_meta = catalog
                .get_table(&table)
                .cloned()
                .ok_or_else(|| ExecutorError::TableNotFound(table.clone()))?;

            if table_meta.storage_options.storage_type == StorageType::Columnar {
                let columnar_scan = columnar_scan::build_columnar_scan(&table_meta, &projection);
                let rows = columnar_scan::execute_columnar_scan(txn, &table_meta, &columnar_scan)?;
                let schema = table_meta.columns.clone();
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }

            // TODO: 現状は Scan で一度全件をメモリに載せてから iterator に渡しています。
            // 将来ストリーミングを徹底する場合は、ScanIterator を活用できるよう
            // トランザクションのライフタイム設計を見直すとよいです。
            let rows = scan::execute_scan(txn, &table_meta)?;
            let schema = table_meta.columns.clone();

            // Wrap in VecIterator for consistent iterator-based processing
            let iter = iterator::VecIterator::new(rows, schema.clone());
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::Scan { table, projection } = input.as_ref() {
                if let Some(table_meta) = catalog.get_table(table) {
                    if table_meta.storage_options.storage_type == StorageType::Columnar {
                        let columnar_scan = columnar_scan::build_columnar_scan_for_filter(
                            table_meta,
                            projection.clone(),
                            &predicate,
                        );
                        let rows =
                            columnar_scan::execute_columnar_scan(txn, table_meta, &columnar_scan)?;
                        let schema = table_meta.columns.clone();
                        let iter = iterator::VecIterator::new(rows, schema.clone());
                        return Ok((Box::new(iter), projection.clone(), schema));
                    }
                }
            }
            let (input_iter, projection, schema) = build_iterator_pipeline(txn, catalog, *input)?;
            let filter_iter = FilterIterator::new(input_iter, predicate);
            Ok((Box::new(filter_iter), projection, schema))
        }
        LogicalPlan::Sort { input, order_by } => {
            let (input_iter, projection, schema) = build_iterator_pipeline(txn, catalog, *input)?;
            let sort_iter = SortIterator::new(input_iter, &order_by)?;
            Ok((Box::new(sort_iter), projection, schema))
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let (input_iter, projection, schema) = build_iterator_pipeline(txn, catalog, *input)?;
            let limit_iter = LimitIterator::new(input_iter, limit, offset);
            Ok((Box::new(limit_iter), projection, schema))
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
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
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
