//! Execution of SQL window functions.

use crate::catalog::ColumnMetadata;
use crate::executor::evaluator::{self, EvalContext};
use crate::executor::memory::MemoryPolicy;
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::logical_plan::{WindowExpr, WindowFunction};
use crate::planner::typed_expr::SortExpr;
use crate::storage::SqlValue;

use super::aggregate::{create_accumulator_for_aggregate, encode_group_key};
use super::iterator::{RowIterator, SortIterator, VecIterator};

/// Materializing window iterator. Input order is restored after each
/// window-local sort so the outer query controls final ordering independently.
pub struct WindowIterator {
    rows: std::vec::IntoIter<Row>,
    schema: Vec<ColumnMetadata>,
}

impl WindowIterator {
    pub fn new<I: RowIterator>(
        mut input: I,
        windows: Vec<WindowExpr>,
        memory: Option<&MemoryPolicy>,
    ) -> Result<Self> {
        let input_schema = input.schema().to_vec();
        let mut rows = Vec::new();
        while let Some(row) = input.next_row() {
            rows.push(row?);
        }

        for (window_index, window) in windows.iter().enumerate() {
            let mut sortable_rows = rows
                .iter()
                .enumerate()
                .map(|(index, row)| Row::new(index as u64, row.values.clone()))
                .collect::<Vec<_>>();
            let mut sort_exprs = window
                .partition_by
                .iter()
                .cloned()
                .map(SortExpr::asc)
                .collect::<Vec<_>>();
            sort_exprs.extend(window.order_by.clone());

            let input = VecIterator::new(std::mem::take(&mut sortable_rows), input_schema.clone());
            let mut sorted: Box<dyn RowIterator> = if let Some(policy) = memory {
                Box::new(SortIterator::new_with_policy(
                    input,
                    &sort_exprs,
                    Some(policy.clone()),
                )?)
            } else {
                Box::new(SortIterator::new(input, &sort_exprs)?)
            };

            let mut values = vec![SqlValue::Null; rows.len()];
            let mut partition = Vec::new();
            let mut current_key = None;
            while let Some(row) = sorted.next_row() {
                let row = row?;
                let key = partition_key(&row, &window.partition_by)?;
                if current_key.as_ref().is_some_and(|current| current != &key) {
                    evaluate_partition(window, &partition, &mut values)?;
                    partition.clear();
                }
                current_key = Some(key);
                partition.push(row);
            }
            if !partition.is_empty() {
                evaluate_partition(window, &partition, &mut values)?;
            }

            for (row, value) in rows.iter_mut().zip(values) {
                row.values.push(value);
            }

            debug_assert!(
                rows.iter()
                    .all(|row| { row.values.len() == input_schema.len() + window_index + 1 })
            );
        }

        let mut schema = input_schema;
        schema.extend(windows.iter().enumerate().map(|(index, window)| {
            ColumnMetadata::new(format!("__window_{index}"), window.result_type.clone())
        }));
        Ok(Self {
            rows: rows.into_iter(),
            schema,
        })
    }
}

impl RowIterator for WindowIterator {
    fn next_row(&mut self) -> Option<Result<Row>> {
        self.rows.next().map(Ok)
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

fn partition_key(row: &Row, partition_by: &[crate::planner::TypedExpr]) -> Result<Vec<u8>> {
    let values = evaluate_exprs(row, partition_by)?;
    encode_group_key(&values)
}

fn evaluate_exprs(row: &Row, exprs: &[crate::planner::TypedExpr]) -> Result<Vec<SqlValue>> {
    let context = EvalContext::new(&row.values);
    exprs
        .iter()
        .map(|expr| evaluator::evaluate(expr, &context))
        .collect()
}

fn evaluate_partition(
    window: &WindowExpr,
    partition: &[Row],
    output: &mut [SqlValue],
) -> Result<()> {
    match &window.function {
        WindowFunction::RowNumber => {
            for (position, row) in partition.iter().enumerate() {
                set_output(output, row, SqlValue::BigInt((position + 1) as i64))?;
            }
        }
        WindowFunction::Rank | WindowFunction::DenseRank => {
            let mut previous_key: Option<Vec<u8>> = None;
            let mut rank = 1_i64;
            let mut dense_rank = 1_i64;
            for (position, row) in partition.iter().enumerate() {
                let order_values = window
                    .order_by
                    .iter()
                    .map(|sort| evaluator::evaluate(&sort.expr, &EvalContext::new(&row.values)))
                    .collect::<Result<Vec<_>>>()?;
                let key = encode_group_key(&order_values)?;
                if position > 0
                    && previous_key
                        .as_ref()
                        .is_some_and(|previous| previous != &key)
                {
                    rank = (position + 1) as i64;
                    dense_rank += 1;
                }
                previous_key = Some(key);
                let value = match window.function {
                    WindowFunction::Rank => rank,
                    WindowFunction::DenseRank => dense_rank,
                    _ => unreachable!(),
                };
                set_output(output, row, SqlValue::BigInt(value))?;
            }
        }
        WindowFunction::Aggregate(aggregate) => {
            let mut accumulator = create_accumulator_for_aggregate(aggregate);
            if window.order_by.is_empty() {
                for row in partition {
                    accumulator.update(aggregate_value(aggregate, row)?)?;
                }
                let value = accumulator.finalize()?;
                for row in partition {
                    set_output(output, row, value.clone())?;
                }
            } else {
                for row in partition {
                    accumulator.update(aggregate_value(aggregate, row)?)?;
                    set_output(output, row, accumulator.finalize()?)?;
                }
            }
        }
    }
    Ok(())
}

fn aggregate_value(
    aggregate: &crate::planner::AggregateExpr,
    row: &Row,
) -> Result<Option<SqlValue>> {
    aggregate
        .arg
        .as_ref()
        .map(|arg| evaluator::evaluate(arg, &EvalContext::new(&row.values)))
        .transpose()
}

fn set_output(output: &mut [SqlValue], row: &Row, value: SqlValue) -> Result<()> {
    let index = usize::try_from(row.row_id).map_err(|_| ExecutorError::InvalidOperation {
        operation: "window function".into(),
        reason: "input row index exceeds usize".into(),
    })?;
    let slot = output
        .get_mut(index)
        .ok_or_else(|| ExecutorError::InvalidOperation {
            operation: "window function".into(),
            reason: format!("input row index {index} is out of bounds"),
        })?;
    *slot = value;
    Ok(())
}
