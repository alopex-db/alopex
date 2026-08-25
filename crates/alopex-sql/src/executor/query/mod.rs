use alopex_core::kv::KVStore;
use alopex_core::sql::stream::ByteSized;
use std::collections::{HashMap, HashSet};

use crate::ast::LITERAL_TABLE;
use crate::catalog::{Catalog, StorageType};
use crate::executor::evaluator::EvalContext;
use crate::executor::memory::{MemoryPolicy, MemoryTracker, map_core_memory_error};
use crate::executor::{ExecutionResult, ExecutorError, QueryResult, QueryRowIterator, Result};
use crate::planner::logical_plan::{LogicalPlan, RecursiveCteLimits, SetOperator};
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
pub mod window;

pub use columnar_scan::{ColumnarScanIterator, create_columnar_scan_iterator};
pub use iterator::{
    DistinctOnIterator, FilterIterator, LimitIterator, RowIterator, ScanIterator, SortIterator,
};
pub use project::{project_row_values, projected_columns};
pub use scan::{
    create_fenced_range_scan_iterator, create_scan_iterator, execute_fenced_range_scan,
};

#[derive(Clone)]
struct RecursiveWorkingTable {
    rows: Vec<Vec<SqlValue>>,
    schema: Vec<crate::catalog::ColumnMetadata>,
}

/// Per-query state supplied explicitly to operators that can read a recursive
/// working table. A fresh context is created at each public execution entry.
#[derive(Clone, Default)]
struct QueryExecutionContext {
    recursive_tables: HashMap<String, RecursiveWorkingTable>,
}

struct RecursiveCteExecution {
    name: String,
    anchor: LogicalPlan,
    recursive_term: LogicalPlan,
    union_all: bool,
    schema: Vec<crate::catalog::ColumnMetadata>,
    limits: RecursiveCteLimits,
}

impl QueryExecutionContext {
    fn with_recursive_table(
        &self,
        name: String,
        table: RecursiveWorkingTable,
    ) -> QueryExecutionContext {
        let mut next = self.clone();
        next.recursive_tables.insert(name, table);
        next
    }
}

fn plan_contains_recursive_cte(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::RecursiveCte { .. } | LogicalPlan::RecursiveReference { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::Limit { input, .. } => plan_contains_recursive_cte(input),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::LateralJoin { left, right, .. }
        | LogicalPlan::SetOperation { left, right, .. } => {
            plan_contains_recursive_cte(left) || plan_contains_recursive_cte(right)
        }
        _ => false,
    }
}

/// Whether the plan holds a LATERAL join or a table function.
///
/// Both are evaluated per outer row through the materializing path, which owns
/// the transaction while it re-executes the correlated side. The streaming
/// pipeline borrows the transaction exclusively and cannot do that, so plans
/// containing either are routed the same way subquery plans are.
fn plan_contains_lateral(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::LateralJoin { .. } | LogicalPlan::TableFunction { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::Limit { input, .. } => plan_contains_lateral(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOperation { left, right, .. } => {
            plan_contains_lateral(left) || plan_contains_lateral(right)
        }
        LogicalPlan::RecursiveCte {
            anchor,
            recursive_term,
            ..
        } => plan_contains_lateral(anchor) || plan_contains_lateral(recursive_term),
        _ => false,
    }
}

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
    execute_query_result_with_context(
        txn,
        catalog,
        plan,
        outer,
        memory,
        &QueryExecutionContext::default(),
    )
}

fn execute_query_result_with_context<
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
    context: &QueryExecutionContext,
) -> Result<QueryResult> {
    let (mut iter, projection, schema) =
        build_iterator_pipeline_with_outer(txn, catalog, plan, memory, outer, context)?;
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
    if subquery::plan_contains_subquery(&plan)
        || plan_contains_recursive_cte(&plan)
        || plan_contains_lateral(&plan)
    {
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

fn execute_recursive_cte_result<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    context: &QueryExecutionContext,
    outer: Option<&Row>,
    memory: Option<&MemoryPolicy>,
    execution: RecursiveCteExecution,
) -> Result<QueryResult> {
    let RecursiveCteExecution {
        name,
        anchor,
        recursive_term,
        union_all,
        schema,
        limits,
    } = execution;
    let anchor_result =
        execute_query_result_with_context(txn, catalog, anchor, outer, memory, context)?;
    let mut accumulated = Vec::new();
    let mut seen = HashSet::new();
    let mut accumulated_bytes = 0u64;
    let mut seen_bytes = 0u64;
    for row in anchor_result.rows {
        let should_accumulate = if union_all {
            true
        } else {
            let key = aggregate::encode_group_key(&row)?;
            let key_bytes = key.len() as u64;
            if seen.insert(key) {
                seen_bytes = seen_bytes.saturating_add(key_bytes);
                true
            } else {
                false
            }
        };
        if should_accumulate {
            accumulated_bytes = accumulated_bytes.saturating_add(estimated_row_bytes(&row));
            accumulated.push(row);
        }
    }
    ensure_recursive_row_limit(&name, accumulated.len(), limits.max_rows)?;

    let mut working = accumulated.clone();
    let mut working_bytes = accumulated_bytes;
    enforce_recursive_memory(
        memory,
        accumulated_bytes
            .saturating_add(working_bytes)
            .saturating_add(seen_bytes),
    )?;
    let mut iterations = 0usize;
    while !working.is_empty() {
        if iterations >= limits.max_iterations {
            return Err(ExecutorError::ResourceExhausted {
                message: format!(
                    "recursive CTE '{name}' reached iteration limit {}",
                    limits.max_iterations
                ),
            });
        }
        iterations += 1;

        // The iteration context owns the delta rows, and RecursiveReference
        // clones them into its iterator. Account for both retained copies at
        // the point where their lifetimes overlap.
        enforce_recursive_memory(
            memory,
            accumulated_bytes
                .saturating_add(working_bytes.saturating_mul(2))
                .saturating_add(seen_bytes),
        )?;

        let iteration_context = context.with_recursive_table(
            name.clone(),
            RecursiveWorkingTable {
                rows: working,
                schema: schema.clone(),
            },
        );
        let recursive_result = execute_query_result_with_context(
            txn,
            catalog,
            recursive_term.clone(),
            outer,
            memory,
            &iteration_context,
        )?;
        let recursive_result_bytes = recursive_result
            .rows
            .iter()
            .map(|row| estimated_row_bytes(row))
            .sum::<u64>();
        // The materialized recursive result and the context's delta rows are
        // both retained until the recursive operator returns. Inner operator
        // buffers enforce the same MemoryPolicy independently; the current
        // policy API has no shared remaining-budget tracker to combine their
        // transient high-water marks with these retained recursive sets.
        enforce_recursive_memory(
            memory,
            accumulated_bytes
                .saturating_add(working_bytes)
                .saturating_add(recursive_result_bytes)
                .saturating_add(seen_bytes),
        )?;
        drop(iteration_context);
        let mut next = Vec::new();
        let mut next_bytes = 0u64;
        for row in recursive_result.rows {
            let should_accumulate = if union_all {
                true
            } else {
                let key = aggregate::encode_group_key(&row)?;
                let key_bytes = key.len() as u64;
                if seen.insert(key) {
                    seen_bytes = seen_bytes.saturating_add(key_bytes);
                    true
                } else {
                    false
                }
            };
            if should_accumulate {
                next_bytes = next_bytes.saturating_add(estimated_row_bytes(&row));
                next.push(row);
            }
        }
        ensure_recursive_row_limit(
            &name,
            accumulated.len().saturating_add(next.len()),
            limits.max_rows,
        )?;
        accumulated.extend(next.iter().cloned());
        accumulated_bytes = accumulated_bytes.saturating_add(next_bytes);
        working_bytes = next_bytes;
        enforce_recursive_memory(
            memory,
            accumulated_bytes
                .saturating_add(working_bytes)
                .saturating_add(seen_bytes),
        )?;
        working = next;
    }

    let columns = schema
        .into_iter()
        .map(|column| ColumnInfo::new(column.name, column.data_type))
        .collect();
    Ok(QueryResult::new(columns, accumulated))
}

fn estimated_row_bytes(row: &[SqlValue]) -> u64 {
    row.iter().map(ByteSized::estimated_bytes).sum()
}

fn enforce_recursive_memory(memory: Option<&MemoryPolicy>, bytes: u64) -> Result<()> {
    let Some(policy) = memory else {
        return Ok(());
    };
    let mut tracker = MemoryTracker::new(policy.clone());
    tracker.add_bytes(bytes).map_err(map_core_memory_error)?;
    if tracker.over_limit() {
        return Err(ExecutorError::ResourceExhausted {
            message: format!(
                "query memory limit exceeded by recursive CTE materialization at {} bytes",
                tracker.used_bytes()
            ),
        });
    }
    Ok(())
}

fn ensure_recursive_row_limit(name: &str, rows: usize, max_rows: usize) -> Result<()> {
    if rows <= max_rows {
        return Ok(());
    }
    Err(ExecutorError::ResourceExhausted {
        message: format!("recursive CTE '{name}' reached row limit {max_rows}"),
    })
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
    build_iterator_pipeline_with_outer(
        txn,
        catalog,
        plan,
        memory,
        None,
        &QueryExecutionContext::default(),
    )
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
    context: &QueryExecutionContext,
) -> Result<(
    Box<dyn RowIterator>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    match plan {
        LogicalPlan::RecursiveReference { name, schema } => {
            let table = context.recursive_tables.get(&name).ok_or_else(|| {
                ExecutorError::InvalidOperation {
                    operation: "recursive common table expression".into(),
                    reason: format!("working table '{name}' is not active"),
                }
            })?;
            if table.schema.len() != schema.len()
                || table.schema.iter().zip(&schema).any(|(left, right)| {
                    left.name != right.name || left.data_type != right.data_type
                })
            {
                return Err(ExecutorError::InvalidOperation {
                    operation: "recursive common table expression".into(),
                    reason: format!("working table '{name}' schema changed during evaluation"),
                });
            }
            let rows = table
                .rows
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, values)| Row::new(index as u64, values))
                .collect();
            let projection =
                Projection::All(schema.iter().map(|column| column.name.clone()).collect());
            Ok((
                Box::new(iterator::VecIterator::new(rows, schema.clone())),
                projection,
                schema,
            ))
        }
        LogicalPlan::RecursiveCte {
            name,
            anchor,
            recursive_term,
            union_all,
            schema,
            limits,
        } => {
            let result = execute_recursive_cte_result(
                txn,
                catalog,
                context,
                outer,
                memory,
                RecursiveCteExecution {
                    name,
                    anchor: *anchor,
                    recursive_term: *recursive_term,
                    union_all,
                    schema,
                    limits,
                },
            )?;
            Ok(materialize_query_result(result))
        }
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
        LogicalPlan::Values { rows, schema } => {
            let projection =
                Projection::All(schema.iter().map(|column| column.name.clone()).collect());
            let iterator = iterator::ValuesIterator::new(rows, schema.clone(), outer);
            Ok((Box::new(iterator), projection, schema))
        }
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::Scan { table, projection } = input.as_ref()
                && let Some(table_meta) = catalog.get_table(table)
                && table_meta.storage_options.storage_type == StorageType::Columnar
            {
                // Columnar filter fusion evaluates the predicate inside the
                // scan, which resolves every column index against the scanned
                // table. A correlated predicate also carries outer-row indexes
                // past that width, and a subquery predicate needs transaction
                // access the scan does not have, so both are evaluated here
                // over a scan widened to the local columns they read
                // (issue #151, D15).
                let evaluated_here = outer.is_some() || subquery::contains_subquery(&predicate);
                let projection = projection.clone();
                let schema = table_meta.columns.clone();
                let columnar_scan = if evaluated_here {
                    columnar_scan::build_columnar_scan_for_external_filter(
                        table_meta,
                        projection.clone(),
                        &predicate,
                    )
                } else {
                    columnar_scan::build_columnar_scan_for_filter(
                        table_meta,
                        projection.clone(),
                        &predicate,
                    )
                };
                let rows = columnar_scan::execute_columnar_scan(txn, table_meta, &columnar_scan)?;
                if !evaluated_here {
                    let iter = iterator::VecIterator::new(rows, schema.clone());
                    return Ok((Box::new(iter), projection, schema));
                }
                let mut kept = Vec::new();
                for row in rows {
                    let eval_row = combine_outer_for_eval(&row, outer);
                    if let SqlValue::Boolean(true) = subquery::evaluate_expr_with_subqueries(
                        txn, catalog, &predicate, &eval_row,
                    )? {
                        kept.push(row);
                    }
                }
                let iter = iterator::VecIterator::new(kept, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
            let (mut input_iter, projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer, context)?;
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
            let input_result =
                execute_query_result_with_context(txn, catalog, *input, outer, memory, context)?;
            let (mut input_iter, _input_projection, schema) =
                materialize_query_result(input_result);
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
                build_iterator_pipeline_with_outer(txn, catalog, *left, memory, outer, context)?;
            let (mut right_iter, _right_projection, right_schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *right, memory, outer, context)?;
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
        LogicalPlan::LateralJoin {
            left,
            right,
            join_type,
            condition,
            right_schema,
        } => {
            let (mut left_iter, _left_projection, left_schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *left, memory, outer, context)?;
            let mut left_rows = Vec::new();
            while let Some(result) = left_iter.next_row() {
                left_rows.push(result?);
            }
            drop(left_iter);
            let rows = execute_lateral_join(
                txn,
                catalog,
                left_rows,
                &right,
                join_type,
                condition.as_ref(),
                right_schema.len(),
                outer,
                memory,
                context,
            )?;
            let mut schema = left_schema;
            schema.extend(right_schema);
            let projection = Projection::All(schema.iter().map(|col| col.name.clone()).collect());
            let iter = iterator::VecIterator::new(rows, schema.clone());
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::TableFunction {
            function,
            args,
            schema,
        } => {
            let rows = execute_table_function(txn, catalog, function, &args, outer)?;
            let projection =
                Projection::All(schema.iter().map(|column| column.name.clone()).collect());
            let iter = iterator::VecIterator::new(rows, schema.clone());
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
            projection,
            grouping_sets,
        } => {
            let (input_iter, _projection, input_schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer, context)?;
            // A correlated group key, aggregate argument, aggregate FILTER
            // predicate or aggregate-local ORDER BY key addresses the outer
            // row past the input width, so widen the input once (D16). The
            // aggregate output is the group-key/aggregate schema, so no
            // narrowing is needed afterwards.
            let input_iter: Box<dyn RowIterator> = match outer {
                Some(outer_row) => {
                    let rows = widen_rows_with_outer(input_iter, outer_row)?;
                    Box::new(iterator::VecIterator::new(rows, input_schema))
                }
                None => input_iter,
            };
            let mut schema = aggregate::build_aggregate_schema(&group_keys, &aggregates);
            if grouping_sets.is_some() {
                schema.push(crate::catalog::ColumnMetadata::new(
                    crate::planner::GROUPING_ID_COLUMN,
                    crate::planner::ResolvedType::BigInt,
                ));
            }
            // GROUPING SETS runs single-pass on AggregateIterator only
            // (issue #149, D9): spill/streaming and parallel execution are
            // mask-unaware and stay on the grouping_sets == None path.
            if grouping_sets.is_none()
                && let Some(policy) = memory
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
            if grouping_sets.is_none()
                && !aggregate::should_use_single_for_parallel(parallelism, &aggregates)
            {
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
            )
            .with_grouping_sets(grouping_sets);
            if let Some(policy) = memory {
                iter = iter.with_memory_policy(Some(policy.clone()));
            }
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Window { input, windows } => {
            let (input_iter, _projection, input_schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer, context)?;
            // A correlated PARTITION BY / ORDER BY key addresses the outer row
            // past the input width (D16). The window iterator appends its
            // results after the whole input row, so the outer values are cut
            // back out of the middle and the declared schema stays
            // `input columns + window columns`.
            if let Some(outer_row) = outer {
                let inner_width = input_schema.len();
                let outer_width = outer_row.len();
                let mut wide_schema = input_schema.clone();
                wide_schema.extend((0..outer_width).map(|index| {
                    crate::catalog::ColumnMetadata::new(
                        format!("__outer_{index}"),
                        crate::planner::ResolvedType::Text,
                    )
                }));
                let rows = widen_rows_with_outer(input_iter, outer_row)?;
                let wide_iter = iterator::VecIterator::new(rows, wide_schema.clone());
                let window_iter = window::WindowIterator::new(wide_iter, windows, memory)?;
                let mut schema = input_schema;
                schema.extend_from_slice(&window_iter.schema()[wide_schema.len()..]);
                let rows = drain_rows(Box::new(window_iter))?
                    .into_iter()
                    .map(|mut row| {
                        let end = (inner_width + outer_width).min(row.values.len());
                        row.values.drain(inner_width.min(end)..end);
                        row
                    })
                    .collect::<Vec<_>>();
                let projection =
                    Projection::All(schema.iter().map(|column| column.name.clone()).collect());
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
            let iter = window::WindowIterator::new(input_iter, windows, memory)?;
            let schema = iter.schema().to_vec();
            let projection =
                Projection::All(schema.iter().map(|column| column.name.clone()).collect());
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::SetOperation {
            left,
            right,
            operator,
            all,
        } => {
            let left_result =
                execute_query_result_with_context(txn, catalog, *left, outer, memory, context)?;
            let right_result =
                execute_query_result_with_context(txn, catalog, *right, outer, memory, context)?;
            let rows = execute_set_operation(operator, all, left_result.rows, right_result.rows)?;
            let schema = left_result
                .columns
                .iter()
                .map(|column| {
                    crate::catalog::ColumnMetadata::new(&column.name, column.data_type.clone())
                })
                .collect::<Vec<_>>();
            let projection = Projection::All(
                left_result
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
            );
            let rows = rows
                .into_iter()
                .enumerate()
                .map(|(index, values)| Row::new(index as u64, values))
                .collect();
            Ok((
                Box::new(iterator::VecIterator::new(rows, schema.clone())),
                projection,
                schema,
            ))
        }
        LogicalPlan::Sort { input, order_by } => {
            let (input_iter, projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer, context)?;
            // A correlated sort key addresses the outer row past the input
            // width, so sort over widened rows and narrow the output back to
            // the operator's own width (D16).
            if let Some(outer_row) = outer {
                let rows = widen_rows_with_outer(input_iter, outer_row)?;
                let wide_iter = iterator::VecIterator::new(rows, schema.clone());
                let sort_iter = if let Some(policy) = memory {
                    SortIterator::new_with_policy(wide_iter, &order_by, Some(policy.clone()))?
                } else {
                    SortIterator::new(wide_iter, &order_by)?
                };
                let rows =
                    narrow_rows_from_outer(drain_rows(Box::new(sort_iter))?, outer_row.len());
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
            let sort_iter = if let Some(policy) = memory {
                SortIterator::new_with_policy(input_iter, &order_by, Some(policy.clone()))?
            } else {
                SortIterator::new(input_iter, &order_by)?
            };
            Ok((Box::new(sort_iter), projection, schema))
        }
        LogicalPlan::DistinctOn {
            input,
            key_count,
            order_by,
        } => {
            let (input_iter, projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer, context)?;
            if let Some(outer_row) = outer {
                let rows = widen_rows_with_outer(input_iter, outer_row)?;
                let wide_iter = iterator::VecIterator::new(rows, schema.clone());
                let distinct_iter =
                    DistinctOnIterator::new(wide_iter, &order_by, key_count, memory.cloned())?;
                let rows =
                    narrow_rows_from_outer(drain_rows(Box::new(distinct_iter))?, outer_row.len());
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
            let distinct_iter =
                DistinctOnIterator::new(input_iter, &order_by, key_count, memory.cloned())?;
            Ok((Box::new(distinct_iter), projection, schema))
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
            ties,
        } => {
            let (input_iter, projection, schema) =
                build_iterator_pipeline_with_outer(txn, catalog, *input, memory, outer, context)?;
            // Only WITH TIES evaluates expressions here, so a plain
            // LIMIT/OFFSET needs no widening even when correlated (D16).
            if let (Some(outer_row), Some(tie_keys)) = (outer, ties.as_ref()) {
                let rows = widen_rows_with_outer(input_iter, outer_row)?;
                let wide_iter = iterator::VecIterator::new(rows, schema.clone());
                let limit_iter =
                    LimitIterator::with_ties(wide_iter, limit, offset, tie_keys.clone());
                let rows =
                    narrow_rows_from_outer(drain_rows(Box::new(limit_iter))?, outer_row.len());
                let iter = iterator::VecIterator::new(rows, schema.clone());
                return Ok((Box::new(iter), projection, schema));
            }
            let limit_iter = match ties {
                Some(tie_keys) => LimitIterator::with_ties(input_iter, limit, offset, tie_keys),
                None => LimitIterator::new(input_iter, limit, offset),
            };
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
    if subquery::plan_contains_subquery(&plan)
        || plan_contains_recursive_cte(&plan)
        || plan_contains_lateral(&plan)
    {
        let result = execute_query_result_with_outer_and_policy(txn, catalog, plan, None, memory)?;
        return Ok(materialize_query_result(result));
    }

    match plan {
        LogicalPlan::SetOperation {
            left,
            right,
            operator,
            all,
        } => build_streaming_set_operation(txn, catalog, *left, *right, operator, all, memory),
        other => build_streaming_pipeline_inner(txn, catalog, other, memory),
    }
}

fn build_streaming_set_operation<
    'a,
    'txn: 'a,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &'a mut T,
    catalog: &C,
    left: LogicalPlan,
    right: LogicalPlan,
    operator: SetOperator,
    all: bool,
    memory: Option<&MemoryPolicy>,
) -> Result<(
    Box<dyn RowIterator + 'a>,
    Projection,
    Vec<crate::catalog::ColumnMetadata>,
)> {
    let (mut left_iter, left_projection, left_schema) =
        build_streaming_pipeline_with_policy(txn, catalog, left, memory)?;
    let mut left_rows = Vec::new();
    while let Some(result) = left_iter.next_row() {
        left_rows.push(result?);
    }
    drop(left_iter);
    let left_result = project::execute_project(left_rows, &left_projection, &left_schema)?;

    let (mut right_iter, right_projection, right_schema) =
        build_streaming_pipeline_with_policy(txn, catalog, right, memory)?;
    let mut right_rows = Vec::new();
    while let Some(result) = right_iter.next_row() {
        right_rows.push(result?);
    }
    let right_result = project::execute_project(right_rows, &right_projection, &right_schema)?;

    let rows = execute_set_operation(operator, all, left_result.rows, right_result.rows)?;
    let schema = left_result
        .columns
        .iter()
        .map(|column| crate::catalog::ColumnMetadata::new(&column.name, column.data_type.clone()))
        .collect::<Vec<_>>();
    let projection = Projection::All(
        left_result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
    );
    let rows = rows
        .into_iter()
        .enumerate()
        .map(|(index, values)| Row::new(index as u64, values))
        .collect();
    Ok((
        Box::new(iterator::VecIterator::new(rows, schema.clone())),
        projection,
        schema,
    ))
}

fn execute_set_operation(
    operator: SetOperator,
    all: bool,
    mut left: Vec<Vec<SqlValue>>,
    right: Vec<Vec<SqlValue>>,
) -> Result<Vec<Vec<SqlValue>>> {
    if all {
        match operator {
            SetOperator::Union => {
                left.extend(right);
                return Ok(left);
            }
            SetOperator::Intersect | SetOperator::Except => {
                let mut right_counts = HashMap::<Vec<u8>, usize>::new();
                for row in right {
                    *right_counts
                        .entry(aggregate::encode_group_key(&row)?)
                        .or_default() += 1;
                }
                let mut output = Vec::new();
                for row in left {
                    let key = aggregate::encode_group_key(&row)?;
                    let remaining = right_counts.entry(key).or_default();
                    if *remaining > 0 {
                        *remaining -= 1;
                        if operator == SetOperator::Intersect {
                            output.push(row);
                        }
                    } else if operator == SetOperator::Except {
                        output.push(row);
                    }
                }
                return Ok(output);
            }
        }
    }

    let right_keys = right
        .iter()
        .map(|row| aggregate::encode_group_key(row))
        .collect::<Result<HashSet<_>>>()?;
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    match operator {
        SetOperator::Union => {
            for row in left.into_iter().chain(right) {
                if seen.insert(aggregate::encode_group_key(&row)?) {
                    output.push(row);
                }
            }
        }
        SetOperator::Intersect => {
            for row in left {
                let key = aggregate::encode_group_key(&row)?;
                if right_keys.contains(&key) && seen.insert(key) {
                    output.push(row);
                }
            }
        }
        SetOperator::Except => {
            for row in left {
                let key = aggregate::encode_group_key(&row)?;
                if !right_keys.contains(&key) && seen.insert(key) {
                    output.push(row);
                }
            }
        }
    }
    Ok(output)
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
        LogicalPlan::Values { rows, schema } => {
            let projection =
                Projection::All(schema.iter().map(|column| column.name.clone()).collect());
            let iterator = iterator::ValuesIterator::new(rows, schema.clone(), None);
            Ok((Box::new(iterator), projection, schema))
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
                build_streaming_pipeline_with_policy(txn, catalog, *input, memory)?;
            let filter_iter = FilterIterator::new(input_iter, predicate);
            Ok((Box::new(filter_iter), projection, schema))
        }
        LogicalPlan::Project { input, projection } => {
            let (mut input_iter, _input_projection, schema) =
                build_streaming_pipeline_with_policy(txn, catalog, *input, memory)?;
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
                build_streaming_pipeline_with_policy(txn, catalog, *left, memory)?;
            let mut left_rows = Vec::new();
            while let Some(result) = left_iter.next_row() {
                left_rows.push(result?);
            }
            drop(left_iter);
            let (mut right_iter, _right_projection, right_schema) =
                build_streaming_pipeline_with_policy(txn, catalog, *right, memory)?;
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
            grouping_sets,
        } => {
            let (input_iter, _projection, _schema) =
                build_streaming_pipeline_with_policy(txn, catalog, *input, memory)?;
            let mut schema = aggregate::build_aggregate_schema(&group_keys, &aggregates);
            if grouping_sets.is_some() {
                schema.push(crate::catalog::ColumnMetadata::new(
                    crate::planner::GROUPING_ID_COLUMN,
                    crate::planner::ResolvedType::BigInt,
                ));
            }
            // Same single-pass gate as the materialized pipeline (D9).
            if grouping_sets.is_none()
                && let Some(policy) = memory
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
            if grouping_sets.is_none()
                && !aggregate::should_use_single_for_parallel(parallelism, &aggregates)
            {
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
            )
            .with_grouping_sets(grouping_sets);
            if let Some(policy) = memory {
                iter = iter.with_memory_policy(Some(policy.clone()));
            }
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Window { input, windows } => {
            let (input_iter, _projection, _schema) =
                build_streaming_pipeline_inner(txn, catalog, *input, memory)?;
            let iter = window::WindowIterator::new(input_iter, windows, memory)?;
            let schema = iter.schema().to_vec();
            let projection =
                Projection::All(schema.iter().map(|column| column.name.clone()).collect());
            Ok((Box::new(iter), projection, schema))
        }
        LogicalPlan::Sort { input, order_by } => {
            let (input_iter, projection, schema) =
                build_streaming_pipeline_with_policy(txn, catalog, *input, memory)?;
            let sort_iter = if let Some(policy) = memory {
                SortIterator::new_with_policy(input_iter, &order_by, Some(policy.clone()))?
            } else {
                SortIterator::new(input_iter, &order_by)?
            };
            Ok((Box::new(sort_iter), projection, schema))
        }
        LogicalPlan::DistinctOn {
            input,
            key_count,
            order_by,
        } => {
            let (input_iter, projection, schema) =
                build_streaming_pipeline_with_policy(txn, catalog, *input, memory)?;
            let distinct_iter =
                DistinctOnIterator::new(input_iter, &order_by, key_count, memory.cloned())?;
            Ok((Box::new(distinct_iter), projection, schema))
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
            ties,
        } => {
            let (input_iter, projection, schema) =
                build_streaming_pipeline_with_policy(txn, catalog, *input, memory)?;
            let limit_iter = match ties {
                Some(tie_keys) => LimitIterator::with_ties(input_iter, limit, offset, tie_keys),
                None => LimitIterator::new(input_iter, limit, offset),
            };
            Ok((Box::new(limit_iter), projection, schema))
        }
        other => Err(ExecutorError::UnsupportedOperation(format!(
            "unsupported query plan: {other:?}"
        ))),
    }
}

/// Run a LATERAL join by re-executing the correlated right side per left row.
///
/// The right plan is correlated, so it cannot be materialized once and joined:
/// each left row is supplied as the outer row and the right side is executed
/// again. `right_width` comes from the planned right schema so a LEFT join can
/// pad even when the right side produced nothing.
#[allow(clippy::too_many_arguments)]
fn execute_lateral_join<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    left_rows: Vec<Row>,
    right: &LogicalPlan,
    join_type: crate::planner::JoinType,
    condition: Option<&crate::planner::typed_expr::TypedExpr>,
    right_width: usize,
    outer: Option<&Row>,
    memory: Option<&MemoryPolicy>,
    context: &QueryExecutionContext,
) -> Result<Vec<Row>> {
    use crate::planner::JoinType;

    let mut rows = Vec::new();
    let mut row_id = 0u64;
    for left_row in left_rows {
        // The correlated side reads the left row through the outer-row
        // convention shared with correlated subqueries.
        let eval_outer = combine_outer_for_eval(&left_row, outer);
        let right_result = execute_query_result_with_context(
            txn,
            catalog,
            right.clone(),
            Some(&eval_outer),
            memory,
            context,
        )?;
        let mut matched = false;
        for right_values in right_result.rows {
            let mut values = left_row.values.clone();
            values.extend(right_values);
            let combined = Row::new(row_id, values);
            if let Some(condition) = condition {
                let eval_row = combine_outer_for_eval(&combined, outer);
                let keep =
                    subquery::evaluate_expr_with_subqueries(txn, catalog, condition, &eval_row)?;
                if !matches!(keep, SqlValue::Boolean(true)) {
                    continue;
                }
            }
            matched = true;
            rows.push(combined);
            row_id += 1;
        }
        if !matched && matches!(join_type, JoinType::Left) {
            let mut values = left_row.values.clone();
            values.extend(std::iter::repeat_n(SqlValue::Null, right_width));
            rows.push(Row::new(row_id, values));
            row_id += 1;
        }
    }
    Ok(rows)
}

/// Evaluate a FROM-clause table function.
///
/// Arguments are evaluated against the outer row alone: the node has no input
/// of its own, so its expression indexes address the outer row directly.
fn execute_table_function<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    function: crate::planner::TableFunctionKind,
    args: &[crate::planner::typed_expr::TypedExpr],
    outer: Option<&Row>,
) -> Result<Vec<Row>> {
    use crate::planner::TableFunctionKind;

    let empty = Row::new(0, Vec::new());
    let eval_row = combine_outer_for_eval(&empty, outer);
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(subquery::evaluate_expr_with_subqueries(
            txn, catalog, arg, &eval_row,
        )?);
    }

    match function {
        TableFunctionKind::Unnest => {
            let Some(argument) = values.into_iter().next() else {
                return Err(ExecutorError::InvalidOperation {
                    operation: "UNNEST".into(),
                    reason: "UNNEST requires exactly one argument".into(),
                });
            };
            match argument {
                // A NULL argument yields no rows, as it does in PostgreSQL.
                SqlValue::Null => Ok(Vec::new()),
                SqlValue::Vector(elements) => Ok(elements
                    .into_iter()
                    .enumerate()
                    .map(|(index, element)| Row::new(index as u64, vec![SqlValue::Float(element)]))
                    .collect()),
                other => Err(ExecutorError::InvalidOperation {
                    operation: "UNNEST".into(),
                    reason: format!(
                        "UNNEST requires a VECTOR argument, found {}",
                        other.type_name()
                    ),
                }),
            }
        }
        // Planning rejects GENERATE_SERIES (issue #157); this arm keeps the
        // executor total if a hand-built plan reaches it.
        TableFunctionKind::GenerateSeries => Err(ExecutorError::UnsupportedOperation(
            "table function GENERATE_SERIES".into(),
        )),
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

/// Materialize an operator's input with the outer row appended to every row.
///
/// The planner addresses a correlated reference as `inner_width + outer_index`
/// (the `combine_outer_for_eval` convention shared with correlated
/// subqueries). Operators that build their own `EvalContext` straight from
/// `row.values` — Aggregate (group keys, aggregate arguments, `FILTER`
/// predicates and aggregate-local `ORDER BY`), Sort, DistinctOn and
/// `LIMIT ... WITH TIES` — would otherwise index past the row and fail with an
/// internal `invalid column reference` (issue #151, D16). Widening the input
/// once makes every one of those indexes resolve without teaching each
/// operator about correlation.
fn widen_rows_with_outer(mut input: Box<dyn RowIterator>, outer: &Row) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    while let Some(result) = input.next_row() {
        rows.push(combine_outer_for_eval(&result?, Some(outer)));
    }
    Ok(rows)
}

/// Undo [`widen_rows_with_outer`] on an operator's output rows.
///
/// Row-preserving operators (Sort, DistinctOn, Limit) emit the widened rows
/// themselves, so the trailing outer values are dropped again before the rows
/// leave the operator and the declared schema keeps matching the row width.
fn narrow_rows_from_outer(rows: Vec<Row>, outer_width: usize) -> Vec<Row> {
    rows.into_iter()
        .map(|mut row| {
            let keep = row.values.len().saturating_sub(outer_width);
            row.values.truncate(keep);
            row
        })
        .collect()
}

fn drain_rows(mut input: Box<dyn RowIterator>) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    while let Some(result) = input.next_row() {
        rows.push(result?);
    }
    Ok(rows)
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
    use crate::planner::typed_expr::TypedExprKind;

    projected
        .alias
        .clone()
        .or_else(|| match &projected.expr.kind {
            TypedExprKind::ColumnRef { column, .. } => Some(column.clone()),
            // A USING/NATURAL common column is planned as
            // COALESCE(left, right); it still names the merged column.
            TypedExprKind::FunctionCall { name, args, .. }
                if name == "coalesce" && !args.is_empty() =>
            {
                let first_column = match &args[0].kind {
                    TypedExprKind::ColumnRef { column, .. } => Some(column),
                    _ => None,
                };
                first_column
                    .filter(|column| {
                        args.iter().all(|arg| {
                            matches!(
                                &arg.kind,
                                TypedExprKind::ColumnRef { column: other, .. } if other == *column
                            )
                        })
                    })
                    .cloned()
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
    if names.len() == schema.len() {
        return Ok(names
            .iter()
            .zip(schema)
            .map(|(name, column)| ColumnInfo::new(name.clone(), column.data_type.clone()))
            .collect());
    }
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
    use crate::executor::SpillPolicy;
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::typed_expr::{ProjectedColumn, TypedExpr};
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    fn text_literal_plan(value: &str) -> LogicalPlan {
        LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: LITERAL_TABLE.into(),
                projection: Projection::All(Vec::new()),
            }),
            projection: Projection::Columns(vec![ProjectedColumn::new(TypedExpr::literal(
                crate::ast::expr::Literal::String(value.into()),
                ResolvedType::Text,
                crate::Span::default(),
            ))]),
        }
    }

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

    #[test]
    fn recursive_cte_accounts_for_reference_clone_high_water() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let catalog = MemoryCatalog::new();
        let schema = vec![ColumnMetadata::new("value", ResolvedType::Text)];
        let anchor = text_literal_plan(&"x".repeat(32));
        let recursive_term = LogicalPlan::RecursiveReference {
            name: "memory_cycle".into(),
            schema: schema.clone(),
        };
        let plan = LogicalPlan::RecursiveCte {
            name: "memory_cycle".into(),
            anchor: Box::new(anchor),
            recursive_term: Box::new(recursive_term),
            union_all: false,
            schema,
            limits: RecursiveCteLimits::default(),
        };
        let policy = MemoryPolicy::new(Some(115), SpillPolicy::FailFast);
        let mut txn = bridge.begin_write().unwrap();

        let error = execute_query_with_policy(&mut txn, &catalog, plan, Some(&policy))
            .expect_err("the working table and its iterator clone must both be accounted");

        assert!(
            matches!(&error, ExecutorError::ResourceExhausted { message }
                if message.contains("query memory limit exceeded")),
            "expected recursive materialization to honor the query memory limit, got: {error}"
        );
    }

    #[test]
    fn recursive_cte_enforces_accumulated_row_limit() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let catalog = MemoryCatalog::new();
        let schema = vec![ColumnMetadata::new("value", ResolvedType::Text)];
        let anchor = LogicalPlan::SetOperation {
            left: Box::new(text_literal_plan("first")),
            right: Box::new(text_literal_plan("second")),
            operator: SetOperator::Union,
            all: true,
        };
        let plan = LogicalPlan::RecursiveCte {
            name: "bounded".into(),
            anchor: Box::new(anchor),
            recursive_term: Box::new(LogicalPlan::RecursiveReference {
                name: "bounded".into(),
                schema: schema.clone(),
            }),
            union_all: true,
            schema,
            limits: RecursiveCteLimits {
                max_iterations: 10,
                max_rows: 1,
            },
        };
        let mut txn = bridge.begin_write().unwrap();

        let error = execute_query(&mut txn, &catalog, plan)
            .expect_err("the accumulated row limit must be checked before iteration");

        assert!(
            matches!(&error, ExecutorError::ResourceExhausted { message }
                if message.contains("recursive CTE 'bounded' reached row limit 1")),
            "expected a named recursive row-limit error, got: {error}"
        );
    }

    #[test]
    fn recursive_cte_accounts_for_result_before_releasing_working_table() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let catalog = MemoryCatalog::new();
        let value = "y".repeat(16);
        let schema = vec![ColumnMetadata::new("value", ResolvedType::Text)];
        let reference = LogicalPlan::RecursiveReference {
            name: "result_overlap".into(),
            schema: schema.clone(),
        };
        let two_literals = LogicalPlan::SetOperation {
            left: Box::new(text_literal_plan(&value)),
            right: Box::new(text_literal_plan(&value)),
            operator: SetOperator::Union,
            all: true,
        };
        let recursive_term = LogicalPlan::SetOperation {
            left: Box::new(reference),
            right: Box::new(two_literals),
            operator: SetOperator::Union,
            all: true,
        };
        let plan = LogicalPlan::RecursiveCte {
            name: "result_overlap".into(),
            anchor: Box::new(text_literal_plan(&value)),
            recursive_term: Box::new(recursive_term),
            union_all: false,
            schema,
            limits: RecursiveCteLimits::default(),
        };
        let policy = MemoryPolicy::new(Some(92), SpillPolicy::FailFast);
        let mut txn = bridge.begin_write().unwrap();

        let error = execute_query_with_policy(&mut txn, &catalog, plan, Some(&policy))
            .expect_err("recursive result must be counted before the working table is released");

        assert!(
            matches!(&error, ExecutorError::ResourceExhausted { message }
                if message.contains("query memory limit exceeded")),
            "expected result/working-table overlap to honor the query memory limit, got: {error}"
        );
    }
}
