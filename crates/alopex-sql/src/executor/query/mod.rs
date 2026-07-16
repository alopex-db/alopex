use alopex_core::kv::KVStore;

use crate::ast::LITERAL_TABLE;
use crate::catalog::{Catalog, StorageType};
use crate::executor::evaluator::EvalContext;
use crate::executor::memory::MemoryPolicy;
use crate::executor::{ExecutionResult, ExecutorError, QueryResult, QueryRowIterator, Result};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::{Projection, SortExpr};
use crate::storage::{SqlTxn, SqlValue};

use super::{ColumnInfo, Row};

pub mod aggregate;
pub mod columnar_scan;
pub mod iterator;
pub mod join;
mod knn;
mod project;
mod scan;
pub mod subquery;

pub use columnar_scan::{ColumnarScanIterator, create_columnar_scan_iterator};
pub use iterator::{FilterIterator, LimitIterator, RowIterator, ScanIterator, SortIterator};
pub use scan::create_scan_iterator;

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
pub fn execute_query<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
) -> Result<ExecutionResult> {
    execute_query_with_policy(txn, catalog, plan, None)
}

pub fn execute_query_with_policy<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
    memory: Option<&MemoryPolicy>,
) -> Result<ExecutionResult> {
    if let Some((pattern, projection, filter)) = knn::extract_knn_context(&plan) {
        return knn::execute_knn_query(txn, catalog, &pattern, &projection, filter.as_ref());
    }

    let result = execute_query_result_with_outer_and_policy(txn, catalog, plan, None, memory)?;
    Ok(ExecutionResult::Query(result))
}

pub(crate) fn execute_query_result_with_outer<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
    outer: Option<&Row>,
) -> Result<QueryResult> {
    execute_query_result_with_outer_and_policy(txn, catalog, plan, outer, None)
}

fn execute_query_result_with_outer_and_policy<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
    outer: Option<&Row>,
    memory: Option<&MemoryPolicy>,
) -> Result<QueryResult> {
    let (mut iter, projection, schema) =
        build_iterator_pipeline_with_outer(txn, catalog, plan, memory, outer)?;
    let mut rows = Vec::new();
    while let Some(result) = iter.next_row() {
        rows.push(result?);
    }
    execute_project_with_subqueries(txn, catalog, rows, &projection, &schema, outer)
}

/// Execute a SELECT logical plan and return a streaming query result.
///
/// This function returns a `QueryRowIterator` that yields rows one at a time,
/// enabling true streaming output without materializing all rows upfront.
///
/// # FR-7 Streaming Output
///
/// This function implements the FR-7 requirement for streaming output.
/// Rows are yielded through an iterator interface, and projection is applied
/// on-the-fly as each row is consumed.
///
/// # Note
///
/// KNN queries currently fall back to the non-streaming path as they require
/// specialized handling.
pub fn execute_query_streaming<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
) -> Result<QueryRowIterator<'static>> {
    execute_query_streaming_with_policy(txn, catalog, plan, None)
}

pub fn execute_query_streaming_with_policy<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
    memory: Option<&MemoryPolicy>,
) -> Result<QueryRowIterator<'static>> {
    // KNN queries not yet supported for streaming - fall back would need different handling
    if knn::extract_knn_context(&plan).is_some() {
        // For KNN, we materialize and wrap in VecIterator
        let result = execute_query_with_policy(txn, catalog, plan, memory)?;
        if let ExecutionResult::Query(qr) = result {
            let (iter, projection, schema) = materialize_query_result(qr);
            return Ok(QueryRowIterator::new(iter, projection, schema));
        }
        return Err(ExecutorError::InvalidOperation {
            operation: "execute_query_streaming".into(),
            reason: "KNN query did not return Query result".into(),
        });
    }

    // Subqueries need transaction access during evaluation, which streaming
    // iterators borrow exclusively. Execute through the materializing path
    // (the same one used by `execute_query`) so results are identical to the
    // non-streaming API instead of failing or silently dropping rows.
    if subquery::plan_contains_subquery(&plan) {
        let result = execute_query_result_with_outer_and_policy(txn, catalog, plan, None, memory)?;
        let (iter, projection, schema) = materialize_query_result(result);
        return Ok(QueryRowIterator::new(iter, projection, schema));
    }

    let (iter, projection, schema) = build_iterator_pipeline(txn, catalog, plan, memory)?;

    Ok(QueryRowIterator::new(iter, projection, schema))
}

/// Convert a materialized `QueryResult` into pipeline outputs.
///
/// The resulting rows are already fully projected, so the returned projection
/// is `Projection::All` over the output column names.
fn materialize_query_result(
    result: QueryResult,
) -> (
    Box<dyn RowIterator>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
) {
    let column_names: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
    let schema: Vec<crate::catalog::ColumnMetadata> = result
        .columns
        .iter()
        .map(|c| crate::catalog::ColumnMetadata::new(&c.name, c.data_type.clone()))
        .collect();
    let rows: Vec<Row> = result
        .rows
        .into_iter()
        .enumerate()
        .map(|(i, values)| Row::new(i as u64, values))
        .collect();
    let iter = iterator::VecIterator::new(rows, schema.clone());
    (Box::new(iter), Projection::All(column_names), schema)
}

/// Build an iterator pipeline from a logical plan.
///
/// This recursively constructs a tree of iterators that mirrors the logical plan
/// structure. The scan phase reads rows into memory, then subsequent operators
/// process them through an iterator pipeline enabling streaming execution and
/// early termination.
fn build_iterator_pipeline<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
    memory: Option<&MemoryPolicy>,
) -> Result<(
    Box<dyn RowIterator>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    build_iterator_pipeline_with_outer(txn, catalog, plan, memory, None)
}

fn build_iterator_pipeline_with_outer<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    plan: LogicalPlan,
    memory: Option<&MemoryPolicy>,
    outer: Option<&Row>,
) -> Result<(
    Box<dyn RowIterator>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    match plan {
        LogicalPlan::Scan { table, projection } => {
            if table == LITERAL_TABLE {
                let schema = Vec::new();
                let rows = vec![Row::new(0, Vec::new())];
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
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
            if let LogicalPlan::Scan { table, projection } = input.as_ref()
                && let Some(table_meta) = catalog.get_table(table)
                && table_meta.storage_options.storage_type == StorageType::Columnar
            {
                let columnar_scan = columnar_scan::build_columnar_scan_for_filter(
                    table_meta,
                    projection.clone(),
                    &predicate,
                );
                let rows = columnar_scan::execute_columnar_scan(txn, table_meta, &columnar_scan)?;
                let schema = table_meta.columns.clone();
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection.clone(), schema));
            }
            let (mut input_iter, projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer)?;
            if outer.is_some() || subquery::contains_subquery(&predicate) {
                let mut rows = Vec::new();
                while let Some(result) = input_iter.next_row() {
                    let row = result?;
                    let eval_row = combine_outer_for_eval(&row, outer);
                    if let SqlValue::Boolean(true) = subquery::evaluate_expr_with_subqueries(
                        txn, catalog, &predicate, &eval_row,
                    )? {
                        rows.push(row);
                    }
                }
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
            let filter_iter = FilterIterator::new(input_iter, predicate);
            Ok((Box::new(filter_iter), projection, schema))
        }
        LogicalPlan::Project { input, projection } => {
            let (mut input_iter, _input_projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer)?;
            let mut rows = Vec::new();
            while let Some(result) = input_iter.next_row() {
                rows.push(result?);
            }
            let projected =
                execute_project_with_subqueries(txn, catalog, rows, &projection, &schema, outer)?;
            let output_schema = projected
                .columns
                .iter()
                .map(|col| crate::catalog::ColumnMetadata::new(&col.name, col.data_type.clone()))
                .collect::<Vec<_>>();
            let rows = projected
                .rows
                .into_iter()
                .enumerate()
                .map(|(idx, values)| Row::new(idx as u64, values))
                .collect::<Vec<_>>();
            let output_projection =
                Projection::All(output_schema.iter().map(|col| col.name.clone()).collect());
            let iter = iterator::VecIterator::new(rows, output_schema.clone());
            Ok((Box::new(iter), output_projection, output_schema))
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            using: _,
        } => {
            let (mut left_iter, _left_projection, left_schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *left, memory, outer)?;
            let (mut right_iter, _right_projection, right_schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *right, memory, outer)?;
            let mut left_rows = Vec::new();
            while let Some(result) = left_iter.next_row() {
                left_rows.push(result?);
            }
            let mut right_rows = Vec::new();
            while let Some(result) = right_iter.next_row() {
                right_rows.push(result?);
            }
            let left_width = left_schema.len();
            let right_width = right_schema.len();
            let rows = join::execute_join_with_widths(
                left_rows,
                right_rows,
                join_type,
                condition.as_ref(),
                left_width,
                right_width,
            )?;
            let mut schema = left_schema;
            schema.extend(right_schema);
            let projection = Projection::All(schema.iter().map(|col| col.name.clone()).collect());
            let iter = iterator::VecIterator::new(rows, schema.clone());
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
            projection,
        } => {
            let (input_iter, _projection, _schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer)?;
            let schema = aggregate::build_aggregate_schema(&group_keys, &aggregates);
            if let Some(policy) = memory
                && policy.spill_directory().is_some()
            {
                if group_keys.is_empty() {
                    let iter = aggregate::StreamingAggregateIterator::new(
                        input_iter,
                        group_keys,
                        aggregates,
                        having,
                        schema.clone(),
                    );
                    return Ok((Box::new(iter), projection, schema));
                }
                let order_by = group_keys
                    .iter()
                    .cloned()
                    .map(|expr| SortExpr {
                        expr,
                        asc: true,
                        nulls_first: false,
                    })
                    .collect::<Vec<_>>();
                let sort_iter =
                    SortIterator::new_with_policy(input_iter, &order_by, Some(policy.clone()))?;
                let iter = aggregate::StreamingAggregateIterator::new(
                    Box::new(sort_iter),
                    group_keys,
                    aggregates,
                    having,
                    schema.clone(),
                );
                return Ok((Box::new(iter), projection, schema));
            }

            let parallelism = std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1);
            if !aggregate::should_use_single_for_parallel(parallelism, &aggregates) {
                let rows = aggregate::execute_parallel_aggregate_rows_with_policy(
                    input_iter,
                    group_keys,
                    aggregates,
                    having,
                    schema.clone(),
                    parallelism,
                    memory.cloned(),
                    1_000_000,
                )?;
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }

            let mut iter = aggregate::AggregateIterator::new(
                input_iter,
                group_keys,
                aggregates,
                having,
                schema.clone(),
            );
            if let Some(policy) = memory {
                iter = iter.with_memory_policy(Some(policy.clone()));
            }
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Sort { input, order_by } => {
            let (input_iter, projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer)?;
            let sort_iter = if let Some(policy) = memory {
                SortIterator::new_with_policy(input_iter, &order_by, Some(policy.clone()))?
            } else {
                SortIterator::new(input_iter, &order_by)?
            };
            Ok((Box::new(sort_iter), projection, schema))
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let (input_iter, projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer)?;
            let limit_iter = LimitIterator::new(input_iter, limit, offset);
            Ok((Box::new(limit_iter), projection, schema))
        }
        other => Err(ExecutorError::UnsupportedOperation(format!(
            "unsupported query plan: {other:?}"
        ))),
    }
}

/// Build a streaming iterator pipeline from a logical plan (FR-7).
///
/// This version uses `ScanIterator` for row-based tables to enable true
/// streaming without materializing all rows upfront. The returned iterator
/// has lifetime `'a` tied to the transaction borrow.
///
/// # Limitations
///
/// - Columnar storage still materializes rows (uses VecIterator)
/// - Sort operations materialize all input rows
/// - KNN queries are not supported (use `build_iterator_pipeline` instead)
pub fn build_streaming_pipeline<
    'a,
    'txn: 'a,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &'a mut T,
    catalog: &C,
    plan: LogicalPlan,
) -> Result<(
    Box<dyn RowIterator + 'a>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    build_streaming_pipeline_with_policy(txn, catalog, plan, None)
}

pub fn build_streaming_pipeline_with_policy<
    'a,
    'txn: 'a,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &'a mut T,
    catalog: &C,
    plan: LogicalPlan,
    memory: Option<&MemoryPolicy>,
) -> Result<(
    Box<dyn RowIterator + 'a>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    // Subqueries need transaction access during evaluation, which streaming
    // iterators borrow exclusively. Execute through the materializing path
    // (the same one used by `execute_query`) so results are identical to the
    // non-streaming API instead of failing or silently dropping rows
    // (GitHub issues #23 / #24).
    if subquery::plan_contains_subquery(&plan) {
        let result = execute_query_result_with_outer_and_policy(txn, catalog, plan, None, memory)?;
        return Ok(materialize_query_result(result));
    }

    build_streaming_pipeline_inner(txn, catalog, plan, memory)
}

/// Inner implementation of streaming pipeline builder.
fn build_streaming_pipeline_inner<
    'a,
    'txn: 'a,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &'a mut T,
    catalog: &C,
    plan: LogicalPlan,
    memory: Option<&MemoryPolicy>,
) -> Result<(
    Box<dyn RowIterator + 'a>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    match plan {
        LogicalPlan::Scan { table, projection } => {
            if table == LITERAL_TABLE {
                let schema = Vec::new();
                let rows = vec![Row::new(0, Vec::new())];
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
            let table_meta = catalog
                .get_table(&table)
                .cloned()
                .ok_or_else(|| ExecutorError::TableNotFound(table.clone()))?;

            if table_meta.storage_options.storage_type == StorageType::Columnar {
                // Columnar storage: use ColumnarScanIterator for FR-7 streaming
                let columnar_scan = columnar_scan::build_columnar_scan(&table_meta, &projection);
                let schema = table_meta.columns.clone();
                let iter =
                    columnar_scan::create_columnar_scan_iterator(txn, &table_meta, &columnar_scan)?;
                return Ok((Box::new(iter), projection, schema));
            }

            // Row-based storage: use ScanIterator for true streaming (FR-7)
            let schema = table_meta.columns.clone();
            let scan_iter = scan::create_scan_iterator(txn, &table_meta)?;
            Ok((Box::new(scan_iter), projection, schema))
        }
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::Scan { table, projection } = input.as_ref()
                && let Some(table_meta) = catalog.get_table(table)
                && table_meta.storage_options.storage_type == StorageType::Columnar
            {
                // Columnar storage with filter: use ColumnarScanIterator for FR-7 streaming
                let columnar_scan = columnar_scan::build_columnar_scan_for_filter(
                    table_meta,
                    projection.clone(),
                    &predicate,
                );
                let schema = table_meta.columns.clone();
                let iter =
                    columnar_scan::create_columnar_scan_iterator(txn, table_meta, &columnar_scan)?;
                return Ok((Box::new(iter), projection.clone(), schema));
            }
            let (input_iter, projection, schema) =
                build_streaming_pipeline_inner(txn, catalog, *input, memory)?;
            let filter_iter = FilterIterator::new(input_iter, predicate);
            Ok((Box::new(filter_iter), projection, schema))
        }
        LogicalPlan::Project { input, projection } => {
            let (mut input_iter, _input_projection, schema) =
                build_streaming_pipeline_inner(txn, catalog, *input, memory)?;
            let mut rows = Vec::new();
            while let Some(result) = input_iter.next_row() {
                rows.push(result?);
            }
            let projected = project::execute_project(rows, &projection, &schema)?;
            let output_schema = projected
                .columns
                .iter()
                .map(|col| crate::catalog::ColumnMetadata::new(&col.name, col.data_type.clone()))
                .collect::<Vec<_>>();
            let rows = projected
                .rows
                .into_iter()
                .enumerate()
                .map(|(idx, values)| Row::new(idx as u64, values))
                .collect::<Vec<_>>();
            let output_projection =
                Projection::All(output_schema.iter().map(|col| col.name.clone()).collect());
            let iter = iterator::VecIterator::new(rows, output_schema.clone());
            Ok((Box::new(iter), output_projection, output_schema))
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            using: _,
        } => {
            let (mut left_iter, _left_projection, left_schema) =
                build_streaming_pipeline_inner(txn, catalog, *left, memory)?;
            let mut left_rows = Vec::new();
            while let Some(result) = left_iter.next_row() {
                left_rows.push(result?);
            }
            drop(left_iter);
            let (mut right_iter, _right_projection, right_schema) =
                build_streaming_pipeline_inner(txn, catalog, *right, memory)?;
            let mut right_rows = Vec::new();
            while let Some(result) = right_iter.next_row() {
                right_rows.push(result?);
            }
            let rows = join::execute_join_with_widths(
                left_rows,
                right_rows,
                join_type,
                condition.as_ref(),
                left_schema.len(),
                right_schema.len(),
            )?;
            let mut schema = left_schema;
            schema.extend(right_schema);
            let projection = Projection::All(schema.iter().map(|col| col.name.clone()).collect());
            let iter = iterator::VecIterator::new(rows, schema.clone());
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
            projection,
        } => {
            let (input_iter, _projection, _schema) =
                build_streaming_pipeline_inner(txn, catalog, *input, memory)?;
            let schema = aggregate::build_aggregate_schema(&group_keys, &aggregates);
            if let Some(policy) = memory
                && policy.spill_directory().is_some()
            {
                if group_keys.is_empty() {
                    let iter = aggregate::StreamingAggregateIterator::new(
                        input_iter,
                        group_keys,
                        aggregates,
                        having,
                        schema.clone(),
                    );
                    return Ok((Box::new(iter), projection, schema));
                }
                let order_by = group_keys
                    .iter()
                    .cloned()
                    .map(|expr| SortExpr {
                        expr,
                        asc: true,
                        nulls_first: false,
                    })
                    .collect::<Vec<_>>();
                let sort_iter =
                    SortIterator::new_with_policy(input_iter, &order_by, Some(policy.clone()))?;
                let iter = aggregate::StreamingAggregateIterator::new(
                    Box::new(sort_iter),
                    group_keys,
                    aggregates,
                    having,
                    schema.clone(),
                );
                return Ok((Box::new(iter), projection, schema));
            }

            let parallelism = std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1);
            if !aggregate::should_use_single_for_parallel(parallelism, &aggregates) {
                let rows = aggregate::execute_parallel_aggregate_rows_with_policy(
                    input_iter,
                    group_keys,
                    aggregates,
                    having,
                    schema.clone(),
                    parallelism,
                    memory.cloned(),
                    1_000_000,
                )?;
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }

            let mut iter = aggregate::AggregateIterator::new(
                input_iter,
                group_keys,
                aggregates,
                having,
                schema.clone(),
            );
            if let Some(policy) = memory {
                iter = iter.with_memory_policy(Some(policy.clone()));
            }
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Sort { input, order_by } => {
            let (input_iter, projection, schema) =
                build_streaming_pipeline_inner(txn, catalog, *input, memory)?;
            let sort_iter = if let Some(policy) = memory {
                SortIterator::new_with_policy(input_iter, &order_by, Some(policy.clone()))?
            } else {
                SortIterator::new(input_iter, &order_by)?
            };
            Ok((Box::new(sort_iter), projection, schema))
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let (input_iter, projection, schema) =
                build_streaming_pipeline_inner(txn, catalog, *input, memory)?;
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

fn combine_outer_for_eval(row: &Row, outer: Option<&Row>) -> Row {
    let Some(outer) = outer else {
        return row.clone();
    };
    let mut values = Vec::with_capacity(row.len() + outer.len());
    values.extend(row.values.clone());
    values.extend(outer.values.clone());
    Row::new(row.row_id, values)
}

fn execute_project_with_subqueries<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    rows: Vec<Row>,
    projection: &Projection,
    schema: &[crate::catalog::ColumnMetadata],
    outer: Option<&Row>,
) -> Result<QueryResult> {
    match projection {
        Projection::All(_) => project::execute_project(rows, projection, schema),
        Projection::Columns(cols)
            if outer.is_some() || cols.iter().any(|c| subquery::contains_subquery(&c.expr)) =>
        {
            let columns: Vec<_> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| column_info_from_projection(c, i))
                .collect();
            let mut projected_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let eval_row = combine_outer_for_eval(&row, outer);
                let mut values = Vec::with_capacity(cols.len());
                for col in cols {
                    values.push(subquery::evaluate_expr_with_subqueries(
                        txn, catalog, &col.expr, &eval_row,
                    )?);
                }
                projected_rows.push(values);
            }
            Ok(QueryResult::new(columns, projected_rows))
        }
        Projection::Columns(_) => project::execute_project(rows, projection, schema),
    }
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
