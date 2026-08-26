//! Execution of SQL window functions.

use std::cmp::Ordering;
use std::ops::RangeInclusive;

use alopex_core::sql::stream::ByteSized;

use crate::ast::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use crate::catalog::ColumnMetadata;
use crate::executor::evaluator::{self, EvalContext};
use crate::executor::memory::{MemoryPolicy, MemoryTracker, map_core_memory_error};
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::logical_plan::{
    OffsetWindowFunction, ValueWindowFunction, WindowExpr, WindowFunction,
};
use crate::planner::typed_expr::SortExpr;
use crate::storage::SqlValue;

use super::aggregate::{Accumulator, create_accumulator_for_aggregate};
use super::iterator::{RowIterator, SortIterator, VecIterator, compare_key_values, compare_single};

/// Upper bound on aggregate input visits for one explicit frame expression.
/// This turns the generic O(partition_rows * average_frame_width) evaluator
/// into a deterministic resource failure instead of unbounded CPU work.
const MAX_EXPLICIT_FRAME_VISITS: u64 = 1_000_000;

#[derive(Default)]
struct ExplicitFrameBudget {
    aggregate_visits: u64,
    range_boundary_probes: u64,
}

impl ExplicitFrameBudget {
    fn charge_aggregate_visits(&mut self, visits: u64) -> Result<()> {
        self.aggregate_visits = self
            .aggregate_visits
            .checked_add(visits)
            .ok_or_else(frame_overflow)?;
        if self.aggregate_visits > MAX_EXPLICIT_FRAME_VISITS {
            return Err(ExecutorError::ResourceExhausted {
                message: format!(
                    "explicit window frame requires more than \
                     {MAX_EXPLICIT_FRAME_VISITS} aggregate input visits"
                ),
            });
        }
        Ok(())
    }

    fn charge_range_boundary_probes(&mut self, probes: u64) -> Result<()> {
        self.range_boundary_probes = self
            .range_boundary_probes
            .checked_add(probes)
            .ok_or_else(frame_overflow)?;
        if self.range_boundary_probes > MAX_EXPLICIT_FRAME_VISITS {
            return Err(ExecutorError::ResourceExhausted {
                message: format!(
                    "explicit RANGE frame requires more than \
                     {MAX_EXPLICIT_FRAME_VISITS} boundary probes"
                ),
            });
        }
        Ok(())
    }
}

/// Materializing window iterator. Input order is restored after each
/// window-local sort so the outer query controls final ordering independently.
pub struct WindowIterator {
    rows: std::vec::IntoIter<Row>,
    schema: Vec<ColumnMetadata>,
    memory: WindowMemory,
}

#[derive(Debug)]
struct WindowMemory {
    tracker: Option<MemoryTracker>,
}

impl WindowMemory {
    fn new(policy: Option<&MemoryPolicy>) -> Self {
        Self {
            tracker: policy.cloned().map(MemoryTracker::new),
        }
    }

    fn reserve_bytes(&mut self, bytes: u64) -> Result<()> {
        let Some(tracker) = &mut self.tracker else {
            return Ok(());
        };
        tracker.add_bytes(bytes).map_err(map_core_memory_error)?;
        if tracker.over_limit() {
            let limit = tracker.policy().limit_bytes().unwrap_or(u64::MAX);
            return Err(ExecutorError::ResourceExhausted {
                message: format!(
                    "window materialization requires {} bytes (limit {limit}); spilling this \
                     operator is not supported",
                    tracker.used_bytes()
                ),
            });
        }
        Ok(())
    }

    fn release_bytes(&mut self, bytes: u64) -> Result<()> {
        let Some(tracker) = &mut self.tracker else {
            return Ok(());
        };
        let retained = tracker.used_bytes().checked_sub(bytes).ok_or_else(|| {
            ExecutorError::InvalidOperation {
                operation: "window memory accounting".into(),
                reason: "released more bytes than were reserved".into(),
            }
        })?;
        tracker.reset();
        tracker.add_bytes(retained).map_err(map_core_memory_error)
    }

    fn clear(&mut self) {
        if let Some(tracker) = &mut self.tracker {
            tracker.reset();
        }
    }

    #[cfg(test)]
    fn used_bytes(&self) -> u64 {
        self.tracker
            .as_ref()
            .map(MemoryTracker::used_bytes)
            .unwrap_or(0)
    }
}

impl WindowIterator {
    pub fn new<I: RowIterator>(
        mut input: I,
        windows: Vec<WindowExpr>,
        memory: Option<&MemoryPolicy>,
    ) -> Result<Self> {
        let input_schema = input.schema().to_vec();
        let memory_policy = memory.cloned();
        let mut memory = WindowMemory::new(memory);
        let mut rows = Vec::new();
        while let Some(row) = input.next_row() {
            let row = row?;
            let previous_capacity = rows.capacity();
            rows.push(row);
            if rows.capacity() > previous_capacity {
                memory.reserve_bytes(estimated_slots_bytes::<Row>(
                    rows.capacity() - previous_capacity,
                )?)?;
            }
            memory.reserve_bytes(estimated_row_payload_bytes(
                rows.last().expect("row was just pushed"),
            )?)?;
        }

        for (window_index, window) in windows.iter().enumerate() {
            let mut frame_budget = ExplicitFrameBudget::default();
            let sortable_bytes = estimated_cloned_rows_bytes(&rows)?;
            let sort_key_bytes = estimated_sort_key_bytes(&rows, window)?;
            // Reserve the full partition once. `clear` below retains this
            // allocation between partitions, so there is only one overlapping
            // row-slot buffer to account for during this window expression.
            let mut partition = Vec::with_capacity(rows.len());
            let partition_slots = estimated_slots_bytes::<Row>(partition.capacity())?;
            let output_slots = estimated_slots_bytes::<SqlValue>(rows.len())?;
            let range_slots = if window.frame.is_some()
                && matches!(&window.function, WindowFunction::Aggregate(_))
            {
                estimated_slots_bytes::<Option<RangeInclusive<usize>>>(rows.len())?
            } else {
                0
            };
            let temporary_bytes = sortable_bytes
                .checked_add(sort_key_bytes)
                .and_then(|bytes| bytes.checked_add(partition_slots))
                .and_then(|bytes| bytes.checked_add(output_slots))
                .and_then(|bytes| bytes.checked_add(range_slots))
                .ok_or_else(window_memory_overflow)?;
            memory.reserve_bytes(temporary_bytes)?;

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
            let mut sorted: Box<dyn RowIterator> = if let Some(policy) = &memory_policy {
                Box::new(SortIterator::new_with_policy(
                    input,
                    &sort_exprs,
                    Some(policy.clone()),
                )?)
            } else {
                Box::new(SortIterator::new(input, &sort_exprs)?)
            };

            let mut values = vec![SqlValue::Null; rows.len()];
            let mut current_partition_values: Option<Vec<SqlValue>> = None;
            while let Some(row) = sorted.next_row() {
                let row = row?;
                let partition_values = evaluate_exprs(&row, &window.partition_by)?;
                if current_partition_values
                    .as_ref()
                    .is_some_and(|current| !partition_values_equal(current, &partition_values))
                {
                    evaluate_partition(
                        window,
                        &partition,
                        &mut values,
                        &mut frame_budget,
                        &mut memory,
                    )?;
                    partition.clear();
                }
                current_partition_values = Some(partition_values);
                partition.push(row);
            }
            if !partition.is_empty() {
                evaluate_partition(
                    window,
                    &partition,
                    &mut values,
                    &mut frame_budget,
                    &mut memory,
                )?;
            }

            for (row, value) in rows.iter_mut().zip(values) {
                let previous_capacity = row.values.capacity();
                row.values.push(value);
                if row.values.capacity() > previous_capacity {
                    memory.reserve_bytes(estimated_slots_bytes::<SqlValue>(
                        row.values.capacity() - previous_capacity,
                    )?)?;
                }
            }
            memory.release_bytes(temporary_bytes)?;

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
            memory,
        })
    }

    #[cfg(test)]
    fn accounted_memory_bytes(&self) -> u64 {
        self.memory.used_bytes()
    }
}

impl RowIterator for WindowIterator {
    fn next_row(&mut self) -> Option<Result<Row>> {
        let row = self.rows.next()?;
        if self.rows.as_slice().is_empty() {
            self.rows = Vec::new().into_iter();
            self.memory.clear();
        } else {
            let payload_bytes = match estimated_row_payload_bytes(&row) {
                Ok(bytes) => bytes,
                Err(error) => return Some(Err(error)),
            };
            if let Err(error) = self.memory.release_bytes(payload_bytes) {
                return Some(Err(error));
            }
        }
        Some(Ok(row))
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

fn estimated_slots_bytes<T>(len: usize) -> Result<u64> {
    u64::try_from(len)
        .ok()
        .and_then(|len| len.checked_mul(std::mem::size_of::<T>() as u64))
        .ok_or_else(window_memory_overflow)
}

fn estimated_dynamic_values_bytes(values: &[SqlValue]) -> Result<u64> {
    values.iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value.estimated_bytes())
            .ok_or_else(window_memory_overflow)
    })
}

fn estimated_row_payload_bytes(row: &Row) -> Result<u64> {
    estimated_slots_bytes::<SqlValue>(row.values.capacity())?
        .checked_add(estimated_dynamic_values_bytes(&row.values)?)
        .ok_or_else(window_memory_overflow)
}

fn estimated_cloned_rows_bytes(rows: &[Row]) -> Result<u64> {
    let row_slots = estimated_slots_bytes::<Row>(rows.len())?;
    rows.iter().try_fold(row_slots, |total, row| {
        let value_slots = estimated_slots_bytes::<SqlValue>(row.values.len())?;
        let dynamic = estimated_dynamic_values_bytes(&row.values)?;
        total
            .checked_add(value_slots)
            .and_then(|bytes| bytes.checked_add(dynamic))
            .ok_or_else(window_memory_overflow)
    })
}

fn estimated_sort_key_bytes(rows: &[Row], window: &WindowExpr) -> Result<u64> {
    let expressions = window
        .partition_by
        .iter()
        .chain(window.order_by.iter().map(|sort| &sort.expr));
    let expressions = expressions.collect::<Vec<_>>();
    let key_slots = estimated_slots_bytes::<SqlValue>(expressions.len())?;
    rows.iter().try_fold(0_u64, |total, row| {
        let context = EvalContext::new(&row.values);
        let dynamic = expressions.iter().try_fold(0_u64, |bytes, expr| {
            let value = evaluator::evaluate(expr, &context)?;
            bytes
                .checked_add(value.estimated_bytes())
                .ok_or_else(window_memory_overflow)
        })?;
        total
            .checked_add(key_slots)
            .and_then(|bytes| bytes.checked_add(dynamic))
            .ok_or_else(window_memory_overflow)
    })
}

fn window_memory_overflow() -> ExecutorError {
    ExecutorError::ResourceExhausted {
        message: "window materialization byte estimate overflow".into(),
    }
}

fn partition_values_equal(left: &[SqlValue], right: &[SqlValue]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| compare_single(left, right, true, false) == Ordering::Equal)
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
    frame_budget: &mut ExplicitFrameBudget,
    memory: &mut WindowMemory,
) -> Result<()> {
    match &window.function {
        WindowFunction::RowNumber => {
            for (position, row) in partition.iter().enumerate() {
                set_output(output, row, SqlValue::BigInt((position + 1) as i64), memory)?;
            }
        }
        WindowFunction::Rank | WindowFunction::DenseRank => {
            let mut previous_key: Option<Vec<SqlValue>> = None;
            let mut rank = 1_i64;
            let mut dense_rank = 1_i64;
            for (position, row) in partition.iter().enumerate() {
                let key = order_values(row, &window.order_by)?;
                if position > 0
                    && previous_key.as_ref().is_some_and(|previous| {
                        compare_key_values(previous, &key, &window.order_by) != Ordering::Equal
                    })
                {
                    rank = (position + 1) as i64;
                    dense_rank += 1;
                }
                previous_key = Some(key);
                let value = match &window.function {
                    WindowFunction::Rank => rank,
                    WindowFunction::DenseRank => dense_rank,
                    _ => unreachable!(),
                };
                set_output(output, row, SqlValue::BigInt(value), memory)?;
            }
        }
        WindowFunction::PercentRank => {
            evaluate_percent_rank(window, partition, output, memory)?;
        }
        WindowFunction::CumeDist => {
            evaluate_cume_dist(window, partition, output, memory)?;
        }
        WindowFunction::Ntile(argument) => {
            evaluate_ntile(argument, partition, output, memory)?;
        }
        WindowFunction::Aggregate(aggregate) => {
            if aggregate.filter.is_some() || !aggregate.order_by.is_empty() {
                // The planner rejects FILTER / aggregate ORDER BY with OVER
                // (issue #148, D2); this guard keeps the window executor from
                // silently ignoring them if that validation ever regresses.
                return Err(ExecutorError::InvalidOperation {
                    operation: "window aggregate".into(),
                    reason: "FILTER and aggregate ORDER BY are not supported in window \
                             aggregate calls"
                        .into(),
                });
            }
            if let Some(frame) = &window.frame
                && !is_default_ordered_frame(frame)
            {
                evaluate_framed_aggregate(
                    window,
                    aggregate,
                    frame,
                    partition,
                    output,
                    frame_budget,
                    memory,
                )?;
            } else {
                let mut accumulator = create_accumulator_for_aggregate(aggregate);
                let mut accumulator_bytes = accumulator.retained_bytes();
                memory.reserve_bytes(accumulator_bytes)?;
                if window.order_by.is_empty() {
                    for row in partition {
                        update_accumulator(
                            accumulator.as_mut(),
                            aggregate_values(aggregate, row)?,
                            memory,
                            &mut accumulator_bytes,
                        )?;
                    }
                    let value = accumulator.finalize()?;
                    set_repeated_output(output, partition, value, memory)?;
                } else {
                    let mut peer_start = 0;
                    while peer_start < partition.len() {
                        let peer_key = order_values(&partition[peer_start], &window.order_by)?;
                        let mut peer_end = peer_start + 1;
                        while peer_end < partition.len() {
                            let candidate = order_values(&partition[peer_end], &window.order_by)?;
                            if compare_key_values(&candidate, &peer_key, &window.order_by)
                                != Ordering::Equal
                            {
                                break;
                            }
                            peer_end += 1;
                        }

                        for row in &partition[peer_start..peer_end] {
                            update_accumulator(
                                accumulator.as_mut(),
                                aggregate_values(aggregate, row)?,
                                memory,
                                &mut accumulator_bytes,
                            )?;
                        }
                        let value = accumulator.finalize()?;
                        set_repeated_output(
                            output,
                            &partition[peer_start..peer_end],
                            value,
                            memory,
                        )?;
                        peer_start = peer_end;
                    }
                }
                memory.release_bytes(accumulator_bytes)?;
            }
        }
        WindowFunction::Value(function) => {
            evaluate_value_window(window, function, partition, output, frame_budget, memory)?;
        }
        WindowFunction::Lag(function) => evaluate_offset_window(
            function,
            OffsetDirection::Preceding,
            partition,
            output,
            memory,
        )?,
        WindowFunction::Lead(function) => evaluate_offset_window(
            function,
            OffsetDirection::Following,
            partition,
            output,
            memory,
        )?,
    }
    Ok(())
}

fn evaluate_percent_rank(
    window: &WindowExpr,
    partition: &[Row],
    output: &mut [SqlValue],
    memory: &mut WindowMemory,
) -> Result<()> {
    let denominator = partition.len().saturating_sub(1) as f64;
    let mut previous_key: Option<Vec<SqlValue>> = None;
    let mut rank = 1_usize;
    for (position, row) in partition.iter().enumerate() {
        let key = order_values(row, &window.order_by)?;
        if position > 0
            && previous_key.as_ref().is_some_and(|previous| {
                compare_key_values(previous, &key, &window.order_by) != Ordering::Equal
            })
        {
            rank = position + 1;
        }
        previous_key = Some(key);
        let value = if denominator == 0.0 {
            0.0
        } else {
            rank.saturating_sub(1) as f64 / denominator
        };
        set_output(output, row, SqlValue::Double(value), memory)?;
    }
    Ok(())
}

fn evaluate_cume_dist(
    window: &WindowExpr,
    partition: &[Row],
    output: &mut [SqlValue],
    memory: &mut WindowMemory,
) -> Result<()> {
    let denominator = partition.len() as f64;
    let mut peer_start = 0;
    while peer_start < partition.len() {
        let peer_key = order_values(&partition[peer_start], &window.order_by)?;
        let mut peer_end = peer_start + 1;
        while peer_end < partition.len() {
            let candidate = order_values(&partition[peer_end], &window.order_by)?;
            if compare_key_values(&candidate, &peer_key, &window.order_by) != Ordering::Equal {
                break;
            }
            peer_end += 1;
        }
        let value = SqlValue::Double(peer_end as f64 / denominator);
        set_repeated_output(output, &partition[peer_start..peer_end], value, memory)?;
        peer_start = peer_end;
    }
    Ok(())
}

fn evaluate_ntile(
    argument: &crate::planner::TypedExpr,
    partition: &[Row],
    output: &mut [SqlValue],
    memory: &mut WindowMemory,
) -> Result<()> {
    let buckets = partition_constant_positive_integer("NTILE", argument, partition)?;
    let rows = u64::try_from(partition.len()).map_err(|_| window_argument_overflow("NTILE"))?;
    let larger_bucket_count = rows % buckets;
    let smaller_bucket_size = rows / buckets;
    let larger_bucket_size = smaller_bucket_size
        .checked_add(1)
        .ok_or_else(|| window_argument_overflow("NTILE"))?;
    let larger_rows = larger_bucket_count
        .checked_mul(larger_bucket_size)
        .ok_or_else(|| window_argument_overflow("NTILE"))?;

    for (position, row) in partition.iter().enumerate() {
        let position = u64::try_from(position).map_err(|_| window_argument_overflow("NTILE"))?;
        let bucket = if position < larger_rows {
            position / larger_bucket_size + 1
        } else {
            debug_assert!(smaller_bucket_size > 0);
            larger_bucket_count + (position - larger_rows) / smaller_bucket_size + 1
        };
        let bucket = i64::try_from(bucket).map_err(|_| window_argument_overflow("NTILE"))?;
        set_output(output, row, SqlValue::BigInt(bucket), memory)?;
    }
    Ok(())
}

fn partition_constant_positive_integer(
    name: &str,
    argument: &crate::planner::TypedExpr,
    partition: &[Row],
) -> Result<u64> {
    let first = positive_integer_argument(
        name,
        evaluator::evaluate(argument, &EvalContext::new(&partition[0].values))?,
    )?;
    for row in &partition[1..] {
        let current = positive_integer_argument(
            name,
            evaluator::evaluate(argument, &EvalContext::new(&row.values))?,
        )?;
        if current != first {
            return Err(ExecutorError::InvalidOperation {
                operation: format!("{name} window function"),
                reason: "argument must be constant within a partition".into(),
            });
        }
    }
    Ok(first)
}

fn positive_integer_argument(name: &str, value: SqlValue) -> Result<u64> {
    let value = match value {
        SqlValue::Integer(value) => i64::from(value),
        SqlValue::BigInt(value) => value,
        _ => {
            return Err(ExecutorError::InvalidOperation {
                operation: format!("{name} window function"),
                reason: "argument must be a positive INTEGER".into(),
            });
        }
    };
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ExecutorError::InvalidOperation {
            operation: format!("{name} window function"),
            reason: "argument must be a positive INTEGER".into(),
        })
}

fn window_argument_overflow(name: &str) -> ExecutorError {
    ExecutorError::InvalidOperation {
        operation: format!("{name} window function"),
        reason: "partition or argument exceeds supported range".into(),
    }
}

fn evaluate_value_window(
    window: &WindowExpr,
    function: &ValueWindowFunction,
    partition: &[Row],
    output: &mut [SqlValue],
    frame_budget: &mut ExplicitFrameBudget,
    memory: &mut WindowMemory,
) -> Result<()> {
    if let Some(frame) = &window.frame
        && !is_default_ordered_frame(frame)
    {
        if frame.units == WindowFrameUnits::Range {
            ensure_range_boundary_budget(partition.len(), frame_budget)?;
        }
        for (position, current_row) in partition.iter().enumerate() {
            let range = match frame.units {
                WindowFrameUnits::Rows => rows_frame_range(position, partition.len(), frame)?,
                WindowFrameUnits::Range => {
                    range_frame_range(position, partition, frame, &window.order_by)?
                }
            };
            let value = value_from_frame(function, current_row, partition, range.as_ref())?;
            set_output(output, current_row, value, memory)?;
        }
        return Ok(());
    }

    if window.order_by.is_empty() {
        let range = 0..=partition.len() - 1;
        for current_row in partition {
            let value = value_from_frame(function, current_row, partition, Some(&range))?;
            set_output(output, current_row, value, memory)?;
        }
        return Ok(());
    }

    let mut peer_start = 0;
    while peer_start < partition.len() {
        let peer_key = order_values(&partition[peer_start], &window.order_by)?;
        let mut peer_end = peer_start + 1;
        while peer_end < partition.len() {
            let candidate = order_values(&partition[peer_end], &window.order_by)?;
            if compare_key_values(&candidate, &peer_key, &window.order_by) != Ordering::Equal {
                break;
            }
            peer_end += 1;
        }
        let range = 0..=peer_end - 1;
        for current_row in &partition[peer_start..peer_end] {
            let value = value_from_frame(function, current_row, partition, Some(&range))?;
            set_output(output, current_row, value, memory)?;
        }
        peer_start = peer_end;
    }
    Ok(())
}

fn value_from_frame(
    function: &ValueWindowFunction,
    current_row: &Row,
    partition: &[Row],
    range: Option<&RangeInclusive<usize>>,
) -> Result<SqlValue> {
    let target = match function {
        ValueWindowFunction::FirstValue(_) => range.map(|range| *range.start()),
        ValueWindowFunction::LastValue(_) => range.map(|range| *range.end()),
        ValueWindowFunction::NthValue { nth, .. } => {
            let nth = positive_integer_argument(
                "NTH_VALUE",
                evaluator::evaluate(nth, &EvalContext::new(&current_row.values))?,
            )?;
            range.and_then(|range| {
                let offset = usize::try_from(nth.checked_sub(1)?).ok()?;
                range
                    .start()
                    .checked_add(offset)
                    .filter(|target| target <= range.end())
            })
        }
    };
    let Some(target) = target else {
        return Ok(SqlValue::Null);
    };
    let value = match function {
        ValueWindowFunction::FirstValue(value)
        | ValueWindowFunction::LastValue(value)
        | ValueWindowFunction::NthValue { value, .. } => value,
    };
    evaluator::evaluate(value, &EvalContext::new(&partition[target].values))
}

fn is_default_ordered_frame(frame: &WindowFrame) -> bool {
    frame.units == WindowFrameUnits::Range
        && frame.start_bound == WindowFrameBound::UnboundedPreceding
        && frame.end_bound == WindowFrameBound::CurrentRow
}

fn evaluate_framed_aggregate(
    window: &WindowExpr,
    aggregate: &crate::planner::AggregateExpr,
    frame: &WindowFrame,
    partition: &[Row],
    output: &mut [SqlValue],
    frame_budget: &mut ExplicitFrameBudget,
    memory: &mut WindowMemory,
) -> Result<()> {
    if frame.units == WindowFrameUnits::Range {
        ensure_range_boundary_budget(partition.len(), frame_budget)?;
    }
    let mut ranges = Vec::with_capacity(partition.len());
    for position in 0..partition.len() {
        let range = match frame.units {
            WindowFrameUnits::Rows => rows_frame_range(position, partition.len(), frame)?,
            WindowFrameUnits::Range => {
                range_frame_range(position, partition, frame, &window.order_by)?
            }
        };
        if let Some(range) = &range {
            let width = range
                .end()
                .checked_sub(*range.start())
                .and_then(|width| width.checked_add(1))
                .ok_or_else(frame_overflow)?;
            frame_budget
                .charge_aggregate_visits(u64::try_from(width).map_err(|_| frame_overflow())?)?;
        }
        ranges.push(range);
    }

    for (current_row, range) in partition.iter().zip(ranges) {
        let mut accumulator = create_accumulator_for_aggregate(aggregate);
        let mut accumulator_bytes = accumulator.retained_bytes();
        memory.reserve_bytes(accumulator_bytes)?;
        if let Some(range) = range {
            for row in &partition[range] {
                update_accumulator(
                    accumulator.as_mut(),
                    aggregate_values(aggregate, row)?,
                    memory,
                    &mut accumulator_bytes,
                )?;
            }
        }
        set_output(output, current_row, accumulator.finalize()?, memory)?;
        memory.release_bytes(accumulator_bytes)?;
    }
    Ok(())
}

fn ensure_range_boundary_budget(
    partition_len: usize,
    budget: &mut ExplicitFrameBudget,
) -> Result<()> {
    let rows = u64::try_from(partition_len).map_err(|_| frame_overflow())?;
    let boundary_probes = rows
        .checked_mul(rows)
        // Each row can scan its peer range plus both finite boundaries.
        .and_then(|probes| probes.checked_mul(3))
        .ok_or_else(frame_overflow)?;
    budget.charge_range_boundary_probes(boundary_probes)
}

fn rows_frame_range(
    position: usize,
    partition_len: usize,
    frame: &WindowFrame,
) -> Result<Option<RangeInclusive<usize>>> {
    let start = rows_start(position, partition_len, frame.start_bound)?;
    let end = rows_end(position, partition_len, frame.end_bound)?;
    Ok(match (start, end) {
        (Some(start), Some(end)) if start <= end => Some(start..=end),
        _ => None,
    })
}

fn rows_start(
    position: usize,
    partition_len: usize,
    bound: WindowFrameBound,
) -> Result<Option<usize>> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Ok(Some(0)),
        WindowFrameBound::Preceding(offset) => {
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            Ok(Some(position.saturating_sub(offset)))
        }
        WindowFrameBound::CurrentRow => Ok(Some(position)),
        WindowFrameBound::Following(offset) => checked_following(position, offset, partition_len),
        WindowFrameBound::UnboundedFollowing => Ok(Some(partition_len.saturating_sub(1))),
    }
}

fn rows_end(
    position: usize,
    partition_len: usize,
    bound: WindowFrameBound,
) -> Result<Option<usize>> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Ok(Some(0)),
        WindowFrameBound::Preceding(offset) => {
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            Ok(position.checked_sub(offset))
        }
        WindowFrameBound::CurrentRow => Ok(Some(position)),
        WindowFrameBound::Following(offset) => {
            Ok(checked_following(position, offset, partition_len)?
                .or_else(|| partition_len.checked_sub(1)))
        }
        WindowFrameBound::UnboundedFollowing => Ok(partition_len.checked_sub(1)),
    }
}

fn checked_following(position: usize, offset: u64, partition_len: usize) -> Result<Option<usize>> {
    let position = u64::try_from(position).map_err(|_| frame_overflow())?;
    let target = position.checked_add(offset).ok_or_else(frame_overflow)?;
    let target = usize::try_from(target).map_err(|_| frame_overflow())?;
    Ok((target < partition_len).then_some(target))
}

fn range_frame_range(
    position: usize,
    partition: &[Row],
    frame: &WindowFrame,
    order_by: &[SortExpr],
) -> Result<Option<RangeInclusive<usize>>> {
    let sort = order_by
        .first()
        .ok_or_else(|| ExecutorError::InvalidOperation {
            operation: "RANGE window frame".into(),
            reason: "ORDER BY is required".into(),
        })?;
    let current = evaluator::evaluate(&sort.expr, &EvalContext::new(&partition[position].values))?;
    let peer = peer_range(position, partition, order_by)?;
    let start = range_boundary(&current, frame.start_bound, true, partition, sort, &peer)?;
    let end = range_boundary(&current, frame.end_bound, false, partition, sort, &peer)?;
    Ok(match (start, end) {
        (Some(start), Some(end)) if start <= end => Some(start..=end),
        _ => None,
    })
}

fn peer_range(
    position: usize,
    partition: &[Row],
    order_by: &[SortExpr],
) -> Result<RangeInclusive<usize>> {
    let key = order_values(&partition[position], order_by)?;
    let mut start = position;
    while start > 0 {
        let candidate = order_values(&partition[start - 1], order_by)?;
        if compare_key_values(&candidate, &key, order_by) != Ordering::Equal {
            break;
        }
        start -= 1;
    }
    let mut end = position;
    while end + 1 < partition.len() {
        let candidate = order_values(&partition[end + 1], order_by)?;
        if compare_key_values(&candidate, &key, order_by) != Ordering::Equal {
            break;
        }
        end += 1;
    }
    Ok(start..=end)
}

fn range_boundary(
    current: &SqlValue,
    bound: WindowFrameBound,
    is_start: bool,
    partition: &[Row],
    sort: &SortExpr,
    peer: &RangeInclusive<usize>,
) -> Result<Option<usize>> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Ok(Some(0)),
        WindowFrameBound::UnboundedFollowing => Ok(partition.len().checked_sub(1)),
        WindowFrameBound::CurrentRow => {
            Ok(Some(if is_start { *peer.start() } else { *peer.end() }))
        }
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) if current.is_null() => {
            Ok(Some(if is_start { *peer.start() } else { *peer.end() }))
        }
        WindowFrameBound::Preceding(offset) | WindowFrameBound::Following(offset) => {
            let target = range_target(current, bound, offset, sort.asc)?;
            let mut matched = None;
            for (index, row) in partition.iter().enumerate() {
                let candidate = evaluator::evaluate(&sort.expr, &EvalContext::new(&row.values))?;
                let Some(ordering) = compare_range_numeric(&candidate, &target)? else {
                    continue;
                };
                let ordering = if sort.asc {
                    ordering
                } else {
                    ordering.reverse()
                };
                if is_start {
                    if ordering != Ordering::Less {
                        return Ok(Some(index));
                    }
                } else if ordering != Ordering::Greater {
                    matched = Some(index);
                } else if matched.is_some() {
                    break;
                }
            }
            Ok(matched)
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RangeNumeric {
    Integer(i128),
    Float(f64),
}

fn range_numeric(value: &SqlValue) -> Result<Option<RangeNumeric>> {
    Ok(match value {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(RangeNumeric::Integer(i128::from(*value))),
        SqlValue::BigInt(value) => Some(RangeNumeric::Integer(i128::from(*value))),
        SqlValue::Float(value) => Some(RangeNumeric::Float(f64::from(*value))),
        SqlValue::Double(value) => Some(RangeNumeric::Float(*value)),
        other => {
            return Err(ExecutorError::InvalidOperation {
                operation: "RANGE window frame".into(),
                reason: format!("ORDER BY value must be numeric, got {}", other.type_name()),
            });
        }
    })
}

fn range_target(
    current: &SqlValue,
    bound: WindowFrameBound,
    offset: u64,
    asc: bool,
) -> Result<RangeNumeric> {
    let value = range_numeric(current)?.ok_or_else(|| ExecutorError::InvalidOperation {
        operation: "RANGE window frame".into(),
        reason: "NULL offset target must use its peer group".into(),
    })?;
    let preceding = matches!(bound, WindowFrameBound::Preceding(_));
    let subtract = preceding == asc;
    match value {
        RangeNumeric::Integer(value) => {
            let offset = i128::from(offset);
            Ok(RangeNumeric::Integer(
                if subtract {
                    value.checked_sub(offset)
                } else {
                    value.checked_add(offset)
                }
                .ok_or_else(frame_overflow)?,
            ))
        }
        RangeNumeric::Float(value) => {
            let target = if subtract {
                value - offset as f64
            } else {
                value + offset as f64
            };
            if target.is_finite() {
                Ok(RangeNumeric::Float(target))
            } else {
                Err(frame_overflow())
            }
        }
    }
}

fn compare_range_numeric(left: &SqlValue, right: &RangeNumeric) -> Result<Option<Ordering>> {
    let Some(left) = range_numeric(left)? else {
        return Ok(None);
    };
    let ordering = match (left, *right) {
        (RangeNumeric::Integer(left), RangeNumeric::Integer(right)) => left.cmp(&right),
        (RangeNumeric::Integer(left), RangeNumeric::Float(right)) => (left as f64)
            .partial_cmp(&right)
            .ok_or_else(frame_overflow)?,
        (RangeNumeric::Float(left), RangeNumeric::Integer(right)) => left
            .partial_cmp(&(right as f64))
            .ok_or_else(frame_overflow)?,
        (RangeNumeric::Float(left), RangeNumeric::Float(right)) => {
            left.partial_cmp(&right).ok_or_else(frame_overflow)?
        }
    };
    Ok(Some(ordering))
}

fn frame_overflow() -> ExecutorError {
    ExecutorError::InvalidOperation {
        operation: "window frame".into(),
        reason: "window frame offset or resource count overflow".into(),
    }
}

#[derive(Debug, Clone, Copy)]
enum OffsetDirection {
    Preceding,
    Following,
}

fn evaluate_offset_window(
    function: &OffsetWindowFunction,
    direction: OffsetDirection,
    partition: &[Row],
    output: &mut [SqlValue],
    memory: &mut WindowMemory,
) -> Result<()> {
    for (position, current_row) in partition.iter().enumerate() {
        let current_context = EvalContext::new(&current_row.values);
        let value = match evaluate_offset(function.offset.as_ref(), &current_context)? {
            None => SqlValue::Null,
            Some(offset) => {
                match addressed_position(position, offset, direction, partition.len()) {
                    Some(target) => evaluator::evaluate(
                        &function.value,
                        &EvalContext::new(&partition[target].values),
                    )?,
                    None => function
                        .default
                        .as_ref()
                        .map(|default| evaluator::evaluate(default, &current_context))
                        .transpose()?
                        .unwrap_or(SqlValue::Null),
                }
            }
        };
        set_output(output, current_row, value, memory)?;
    }
    Ok(())
}

fn evaluate_offset(
    offset: Option<&crate::planner::TypedExpr>,
    context: &EvalContext<'_>,
) -> Result<Option<u64>> {
    let value = offset
        .map(|expr| evaluator::evaluate(expr, context))
        .transpose()?
        .unwrap_or(SqlValue::Integer(1));
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Integer(value) => non_negative_offset(i64::from(value)).map(Some),
        SqlValue::BigInt(value) => non_negative_offset(value).map(Some),
        other => Err(ExecutorError::InvalidOperation {
            operation: "window offset".into(),
            reason: format!("offset must be INTEGER, got {}", other.type_name()),
        }),
    }
}

fn non_negative_offset(offset: i64) -> Result<u64> {
    u64::try_from(offset).map_err(|_| ExecutorError::InvalidOperation {
        operation: "window offset".into(),
        reason: "offset must be non-negative".into(),
    })
}

fn addressed_position(
    position: usize,
    offset: u64,
    direction: OffsetDirection,
    partition_len: usize,
) -> Option<usize> {
    let position = u64::try_from(position).ok()?;
    let target = match direction {
        OffsetDirection::Preceding => position.checked_sub(offset)?,
        OffsetDirection::Following => position.checked_add(offset)?,
    };
    let target = usize::try_from(target).ok()?;
    (target < partition_len).then_some(target)
}

fn order_values(row: &Row, order_by: &[SortExpr]) -> Result<Vec<SqlValue>> {
    order_by
        .iter()
        .map(|sort| evaluator::evaluate(&sort.expr, &EvalContext::new(&row.values)))
        .collect()
}

fn aggregate_values(aggregate: &crate::planner::AggregateExpr, row: &Row) -> Result<Vec<SqlValue>> {
    aggregate
        .arg
        .iter()
        .chain(&aggregate.extra_args)
        .map(|arg| evaluator::evaluate(arg, &EvalContext::new(&row.values)))
        .collect()
}

fn update_accumulator(
    accumulator: &mut dyn Accumulator,
    values: Vec<SqlValue>,
    memory: &mut WindowMemory,
    accounted_bytes: &mut u64,
) -> Result<()> {
    let temporary_bytes = values.iter().map(ByteSized::estimated_bytes).sum();
    memory.reserve_bytes(temporary_bytes)?;
    accumulator.update_values(&values)?;
    let retained_bytes = accumulator.retained_bytes();
    if retained_bytes > *accounted_bytes {
        memory.reserve_bytes(retained_bytes - *accounted_bytes)?;
    } else if retained_bytes < *accounted_bytes {
        memory.release_bytes(*accounted_bytes - retained_bytes)?;
    }
    *accounted_bytes = retained_bytes;
    memory.release_bytes(temporary_bytes)?;
    Ok(())
}

fn set_repeated_output(
    output: &mut [SqlValue],
    rows: &[Row],
    value: SqlValue,
    memory: &mut WindowMemory,
) -> Result<()> {
    let template_bytes = value.estimated_bytes();
    memory.reserve_bytes(template_bytes)?;
    for row in rows {
        set_output(output, row, value.clone(), memory)?;
    }
    memory.release_bytes(template_bytes)
}

fn set_output(
    output: &mut [SqlValue],
    row: &Row,
    value: SqlValue,
    memory: &mut WindowMemory,
) -> Result<()> {
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
    memory.reserve_bytes(value.estimated_bytes())?;
    *slot = value;
    Ok(())
}

#[cfg(test)]
mod frame_tests {
    use super::*;
    use crate::Span;
    use crate::executor::memory::SpillPolicy;
    use crate::planner::aggregate_expr::AggregateExpr;
    use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
    use crate::planner::types::ResolvedType;

    fn row_number_window() -> WindowExpr {
        WindowExpr {
            function: WindowFunction::RowNumber,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
            result_type: crate::planner::types::ResolvedType::BigInt,
        }
    }

    fn text_rows() -> Vec<Row> {
        vec![
            Row::new(0, vec![SqlValue::Text("a".repeat(100))]),
            Row::new(1, vec![SqlValue::Text("b".repeat(100))]),
        ]
    }

    fn rows(start_bound: WindowFrameBound, end_bound: WindowFrameBound) -> WindowFrame {
        WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound,
            end_bound,
        }
    }

    fn column(index: usize, resolved_type: ResolvedType) -> TypedExpr {
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "test".into(),
                column: format!("column_{index}"),
                column_index: index,
            },
            resolved_type,
            span: Span::default(),
        }
    }

    #[test]
    fn rows_boundaries_clamp_and_empty_without_crossing_a_partition() {
        let frame = rows(
            WindowFrameBound::Preceding(2),
            WindowFrameBound::Following(1),
        );
        assert_eq!(rows_frame_range(0, 3, &frame).unwrap(), Some(0..=1));
        assert_eq!(rows_frame_range(2, 3, &frame).unwrap(), Some(0..=2));

        let empty = rows(
            WindowFrameBound::Following(2),
            WindowFrameBound::Following(1),
        );
        assert_eq!(rows_frame_range(0, 3, &empty).unwrap(), None);
        assert_eq!(rows_frame_range(2, 3, &empty).unwrap(), None);
    }

    #[test]
    fn following_offset_arithmetic_overflow_is_a_controlled_error() {
        let error = rows_frame_range(
            1,
            3,
            &rows(
                WindowFrameBound::Following(u64::MAX),
                WindowFrameBound::UnboundedFollowing,
            ),
        )
        .expect_err("position + offset must not wrap");
        assert!(error.to_string().contains("overflow"));
    }

    #[test]
    fn range_boundary_work_is_rejected_before_quadratic_scanning() {
        let mut budget = ExplicitFrameBudget::default();
        ensure_range_boundary_budget(577, &mut budget)
            .expect("577 rows stay within one million worst-case probes");
        let mut budget = ExplicitFrameBudget::default();
        let error = ensure_range_boundary_budget(578, &mut budget)
            .expect_err("578 rows exceed three times n squared probe budget");
        assert!(matches!(error, ExecutorError::ResourceExhausted { .. }));
    }

    #[test]
    fn overlapping_window_materializations_are_fail_closed_by_byte_limit() {
        let rows = text_rows();
        let single_copy_bytes = estimated_cloned_rows_bytes(&rows).unwrap();
        let input = VecIterator::new(
            rows,
            vec![ColumnMetadata::new(
                "payload",
                crate::planner::types::ResolvedType::Text,
            )],
        );
        // Each row set fits independently. The original rows, sortable copy,
        // partition storage, and output overlap, so the operator as a whole
        // must reject a limit just below two standalone copies.
        let spill_dir = tempfile::tempdir().unwrap();
        let policy = MemoryPolicy::new(
            Some(single_copy_bytes.checked_mul(2).unwrap() - 1),
            SpillPolicy::SpillToDisk {
                directory: spill_dir.path().to_path_buf(),
            },
        );

        let error = match WindowIterator::new(input, vec![row_number_window()], Some(&policy)) {
            Ok(_) => panic!("overlapping row copies must share one byte budget"),
            Err(error) => error,
        };

        assert!(matches!(error, ExecutorError::ResourceExhausted { .. }));
    }

    #[test]
    fn window_memory_accounting_is_released_as_output_is_drained() {
        let input = VecIterator::new(
            text_rows(),
            vec![ColumnMetadata::new(
                "payload",
                crate::planner::types::ResolvedType::Text,
            )],
        );
        let policy = MemoryPolicy::new(Some(10_000), SpillPolicy::FailFast);
        let mut window =
            WindowIterator::new(input, vec![row_number_window()], Some(&policy)).unwrap();
        let materialized_bytes = window.accounted_memory_bytes();

        window.next_row().unwrap().unwrap();
        let one_row_remaining_bytes = window.accounted_memory_bytes();
        window.next_row().unwrap().unwrap();

        assert!(materialized_bytes > one_row_remaining_bytes);
        assert!(one_row_remaining_bytes > 0);
        assert_eq!(window.accounted_memory_bytes(), 0);
    }

    #[test]
    fn explicit_frame_visit_budget_is_shared_across_partitions() {
        let input_rows = (0_u64..2_000)
            .map(|row_id| {
                Row::new(
                    row_id,
                    vec![
                        SqlValue::Integer((row_id / 1_000) as i32),
                        SqlValue::Integer(row_id as i32),
                    ],
                )
            })
            .collect::<Vec<_>>();
        let input = VecIterator::new(
            input_rows,
            vec![
                ColumnMetadata::new("partition", ResolvedType::Integer),
                ColumnMetadata::new("value", ResolvedType::Integer),
            ],
        );
        let window = WindowExpr {
            function: WindowFunction::Aggregate(AggregateExpr::count_star()),
            partition_by: vec![column(0, ResolvedType::Integer)],
            order_by: Vec::new(),
            frame: Some(rows(
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
            result_type: ResolvedType::BigInt,
        };

        let error = match WindowIterator::new(input, vec![window], None) {
            Ok(_) => panic!("all partitions of one window expression must share the visit cap"),
            Err(error) => error,
        };

        assert!(matches!(error, ExecutorError::ResourceExhausted { .. }));
    }

    #[test]
    fn distinct_frame_retention_shares_the_window_memory_budget() {
        let input_rows = (0_u64..3)
            .map(|row_id| {
                Row::new(
                    row_id,
                    vec![SqlValue::Text(format!("{row_id}-{}", "x".repeat(600)))],
                )
            })
            .collect::<Vec<_>>();
        let input = VecIterator::new(
            input_rows,
            vec![ColumnMetadata::new("payload", ResolvedType::Text)],
        );
        let window = WindowExpr {
            function: WindowFunction::Aggregate(AggregateExpr::count(
                column(0, ResolvedType::Text),
                true,
            )),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: Some(rows(
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
            result_type: ResolvedType::BigInt,
        };
        let policy = MemoryPolicy::new(Some(5_500), SpillPolicy::FailFast);

        let error = match WindowIterator::new(input, vec![window], Some(&policy)) {
            Ok(_) => panic!("DISTINCT dedup keys must overlap the materialized window rows"),
            Err(error) => error,
        };

        assert!(matches!(error, ExecutorError::ResourceExhausted { .. }));
    }

    #[test]
    fn min_text_frame_retention_shares_the_window_memory_budget() {
        let input_rows = (0_u64..3)
            .map(|row_id| {
                Row::new(
                    row_id,
                    vec![SqlValue::Text(format!("{row_id}-{}", "x".repeat(600)))],
                )
            })
            .collect::<Vec<_>>();
        let input = VecIterator::new(
            input_rows,
            vec![ColumnMetadata::new("payload", ResolvedType::Text)],
        );
        let aggregate = AggregateExpr::min(column(0, ResolvedType::Text));
        let window = WindowExpr {
            function: WindowFunction::Aggregate(aggregate),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: Some(rows(
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
            result_type: ResolvedType::Text,
        };
        let policy = MemoryPolicy::new(Some(6_500), SpillPolicy::FailFast);

        let error = match WindowIterator::new(input, vec![window], Some(&policy)) {
            Ok(_) => panic!("MIN text state must overlap materialized rows and output values"),
            Err(error) => error,
        };

        assert!(matches!(error, ExecutorError::ResourceExhausted { .. }));
    }

    #[test]
    fn implicit_min_text_temporaries_share_the_window_memory_budget() {
        let input_rows = (0_u64..3)
            .map(|row_id| {
                Row::new(
                    row_id,
                    vec![SqlValue::Text(format!("{row_id}-{}", "x".repeat(600)))],
                )
            })
            .collect::<Vec<_>>();
        let input = VecIterator::new(
            input_rows,
            vec![ColumnMetadata::new("payload", ResolvedType::Text)],
        );
        let aggregate = AggregateExpr::min(column(0, ResolvedType::Text));
        let window = WindowExpr {
            function: WindowFunction::Aggregate(aggregate),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
            result_type: ResolvedType::Text,
        };
        let policy = MemoryPolicy::new(Some(7_000), SpillPolicy::FailFast);

        let error = match WindowIterator::new(input, vec![window], Some(&policy)) {
            Ok(_) => panic!("MIN input and finalize templates must overlap retained state"),
            Err(error) => error,
        };

        assert!(matches!(error, ExecutorError::ResourceExhausted { .. }));
    }

    #[test]
    fn signed_zero_partition_values_have_sql_sort_equality() {
        let exprs = vec![column(0, ResolvedType::Double)];
        let negative = Row::new(0, vec![SqlValue::Double(-0.0)]);
        let positive = Row::new(1, vec![SqlValue::Double(0.0)]);

        let negative = evaluate_exprs(&negative, &exprs).unwrap();
        let positive = evaluate_exprs(&positive, &exprs).unwrap();
        assert!(partition_values_equal(&negative, &positive));
    }

    #[test]
    fn signed_zero_range_current_row_values_share_one_peer_group() {
        let partition = vec![
            Row::new(0, vec![SqlValue::Double(-0.0)]),
            Row::new(1, vec![SqlValue::Double(0.0)]),
        ];
        let order_by = vec![SortExpr::asc(column(0, ResolvedType::Double))];

        assert_eq!(peer_range(0, &partition, &order_by).unwrap(), 0..=1);
        assert_eq!(peer_range(1, &partition, &order_by).unwrap(), 0..=1);
    }

    #[test]
    fn signed_zero_partition_wiring_keeps_one_partition() {
        let input = VecIterator::new(
            vec![
                Row::new(0, vec![SqlValue::Double(-0.0)]),
                Row::new(1, vec![SqlValue::Double(0.0)]),
            ],
            vec![ColumnMetadata::new("value", ResolvedType::Double)],
        );
        let window = WindowExpr {
            function: WindowFunction::Aggregate(AggregateExpr::count_star()),
            partition_by: vec![column(0, ResolvedType::Double)],
            order_by: Vec::new(),
            frame: None,
            result_type: ResolvedType::BigInt,
        };
        let mut iterator = WindowIterator::new(input, vec![window], None).unwrap();
        let mut counts = Vec::new();
        while let Some(row) = iterator.next_row() {
            counts.push(row.unwrap().values[1].clone());
        }

        assert_eq!(counts, vec![SqlValue::BigInt(2), SqlValue::BigInt(2)]);
    }

    #[test]
    fn incomparable_values_are_not_arbitrary_peers_or_partitions() {
        let scalar_exprs = [column(0, ResolvedType::Double)];
        assert!(!partition_values_equal(
            &[SqlValue::Double(1.0)],
            &[SqlValue::Double(f64::NAN)]
        ));
        assert!(partition_values_equal(
            &[SqlValue::Double(f64::NAN)],
            &[SqlValue::Double(f64::NAN)]
        ));
        assert!(!partition_values_equal(
            &[SqlValue::Vector(vec![0.0])],
            &[SqlValue::Vector(vec![1.0])]
        ));

        let partition = vec![
            Row::new(0, vec![SqlValue::Double(1.0)]),
            Row::new(1, vec![SqlValue::Double(f64::NAN)]),
            Row::new(2, vec![SqlValue::Double(f64::NAN)]),
        ];
        let order_by = vec![SortExpr::asc(scalar_exprs[0].clone())];
        assert_eq!(peer_range(0, &partition, &order_by).unwrap(), 0..=0);
        assert_eq!(peer_range(1, &partition, &order_by).unwrap(), 1..=2);

        let rank_window = WindowExpr {
            function: WindowFunction::Rank,
            partition_by: Vec::new(),
            order_by,
            frame: None,
            result_type: ResolvedType::BigInt,
        };
        let mut output = vec![SqlValue::Null; partition.len()];
        evaluate_partition(
            &rank_window,
            &partition,
            &mut output,
            &mut ExplicitFrameBudget::default(),
            &mut WindowMemory::new(None),
        )
        .unwrap();
        assert_eq!(
            output,
            vec![
                SqlValue::BigInt(1),
                SqlValue::BigInt(2),
                SqlValue::BigInt(2)
            ]
        );
    }
}
