use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::catalog::ColumnMetadata;
use crate::executor::evaluator::EvalContext;
use crate::executor::memory::{MemoryPolicy, MemoryTracker};
use crate::executor::{ExecutorError, Result};
use crate::planner::aggregate_expr::{AggregateExpr, AggregateFunction};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::{RowCodec, SqlValue};

use super::{Row, RowIterator};

/// Byte-encoded group key for hash-based aggregation.
pub type GroupKeyBytes = Vec<u8>;

fn encode_group_value(value: &SqlValue, buf: &mut Vec<u8>) -> Result<()> {
    buf.push(value.type_tag());
    match value {
        SqlValue::Null => Ok(()),
        SqlValue::Integer(v) => {
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        SqlValue::BigInt(v) => {
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        SqlValue::Float(v) => {
            buf.extend_from_slice(&v.to_bits().to_le_bytes());
            Ok(())
        }
        SqlValue::Double(v) => {
            buf.extend_from_slice(&v.to_bits().to_le_bytes());
            Ok(())
        }
        SqlValue::Text(s) => {
            let len = u32::try_from(s.len()).map_err(|_| ExecutorError::InvalidOperation {
                operation: "aggregate".into(),
                reason: "text length exceeds u32::MAX".into(),
            })?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
            Ok(())
        }
        SqlValue::Blob(bytes) => {
            let len = u32::try_from(bytes.len()).map_err(|_| ExecutorError::InvalidOperation {
                operation: "aggregate".into(),
                reason: "blob length exceeds u32::MAX".into(),
            })?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        SqlValue::Boolean(b) => {
            buf.push(u8::from(*b));
            Ok(())
        }
        SqlValue::Timestamp(v) => {
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        SqlValue::Vector(values) => {
            let len = u32::try_from(values.len()).map_err(|_| ExecutorError::InvalidOperation {
                operation: "aggregate".into(),
                reason: "vector length exceeds u32::MAX".into(),
            })?;
            buf.extend_from_slice(&len.to_le_bytes());
            for f in values {
                buf.extend_from_slice(&f.to_bits().to_le_bytes());
            }
            Ok(())
        }
    }
}

/// Encode group key values into a deterministic byte sequence.
pub fn encode_group_key(values: &[SqlValue]) -> Result<GroupKeyBytes> {
    let mut buf = Vec::new();
    for value in values {
        encode_group_value(value, &mut buf)?;
    }
    Ok(buf)
}

/// Accumulator interface for aggregate function execution.
pub trait Accumulator: Send {
    /// Update the accumulator with a new value (None for COUNT(*) rows).
    fn update(&mut self, value: Option<SqlValue>) -> Result<()>;
    /// Finalize the accumulator and return the resulting SqlValue.
    fn finalize(&self) -> Result<SqlValue>;
    /// Clone the accumulator as a trait object.
    fn clone_box(&self) -> Box<dyn Accumulator>;
}

impl Clone for Box<dyn Accumulator> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Accumulator for COUNT / COUNT(DISTINCT).
#[derive(Debug, Clone)]
pub struct CountAccumulator {
    count: usize,
    distinct_values: Option<HashSet<Vec<u8>>>,
}

impl CountAccumulator {
    /// Create a new count accumulator.
    pub fn new(distinct: bool) -> Self {
        Self {
            count: 0,
            distinct_values: if distinct { Some(HashSet::new()) } else { None },
        }
    }
}

impl Accumulator for CountAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        match (&mut self.distinct_values, value) {
            (Some(distinct), Some(value)) => {
                if value.is_null() {
                    return Ok(());
                }
                let encoded = RowCodec::encode(std::slice::from_ref(&value));
                if distinct.insert(encoded) {
                    self.count += 1;
                }
            }
            (Some(_), None) => {
                self.count += 1;
            }
            (None, Some(value)) => {
                if !value.is_null() {
                    self.count += 1;
                }
            }
            (None, None) => {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<SqlValue> {
        Ok(SqlValue::BigInt(self.count as i64))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Accumulator for SUM.
#[derive(Debug, Clone)]
pub struct SumAccumulator {
    sum: Option<f64>,
}

impl SumAccumulator {
    /// Create a new sum accumulator.
    pub fn new() -> Self {
        Self { sum: None }
    }
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for SumAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        if value.is_null() {
            return Ok(());
        }
        let numeric = numeric_to_f64(&value)?;
        self.sum = Some(self.sum.unwrap_or(0.0) + numeric);
        Ok(())
    }

    fn finalize(&self) -> Result<SqlValue> {
        Ok(self.sum.map_or(SqlValue::Null, SqlValue::Double))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Accumulator for TOTAL (SUM that returns 0.0 on empty/all-NULL input).
#[derive(Debug, Clone)]
pub struct TotalAccumulator {
    sum: Option<f64>,
}

impl TotalAccumulator {
    /// Create a new total accumulator.
    pub fn new() -> Self {
        Self { sum: None }
    }
}

impl Default for TotalAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for TotalAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        if value.is_null() {
            return Ok(());
        }
        let numeric = numeric_to_f64(&value)?;
        self.sum = Some(self.sum.unwrap_or(0.0) + numeric);
        Ok(())
    }

    fn finalize(&self) -> Result<SqlValue> {
        Ok(SqlValue::Double(self.sum.unwrap_or(0.0)))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Accumulator for AVG.
#[derive(Debug, Clone)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: usize,
}

impl AvgAccumulator {
    /// Create a new average accumulator.
    pub fn new() -> Self {
        Self {
            sum: None,
            count: 0,
        }
    }
}

impl Default for AvgAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for AvgAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        if value.is_null() {
            return Ok(());
        }
        let numeric = numeric_to_f64(&value)?;
        self.sum = Some(self.sum.unwrap_or(0.0) + numeric);
        self.count += 1;
        Ok(())
    }

    fn finalize(&self) -> Result<SqlValue> {
        if self.count == 0 {
            return Ok(SqlValue::Null);
        }
        let sum = self.sum.unwrap_or(0.0);
        Ok(SqlValue::Double(sum / self.count as f64))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

fn numeric_to_f64(value: &SqlValue) -> Result<f64> {
    match value {
        SqlValue::Integer(v) => Ok(*v as f64),
        SqlValue::BigInt(v) => Ok(*v as f64),
        SqlValue::Float(v) => Ok(*v as f64),
        SqlValue::Double(v) => Ok(*v),
        _ => Err(ExecutorError::Evaluation(
            crate::executor::EvaluationError::TypeMismatch {
                expected: "numeric".into(),
                actual: value.type_name().into(),
            },
        )),
    }
}

/// Accumulator for MIN / MAX.
#[derive(Debug, Clone)]
pub struct MinMaxAccumulator {
    value: Option<SqlValue>,
    is_min: bool,
}

impl MinMaxAccumulator {
    /// Create a new min/max accumulator.
    pub fn new(is_min: bool) -> Self {
        Self {
            value: None,
            is_min,
        }
    }
}

impl Accumulator for MinMaxAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        if value.is_null() {
            return Ok(());
        }

        match &self.value {
            None => {
                self.value = Some(value);
            }
            Some(current) => {
                if std::mem::discriminant(current) != std::mem::discriminant(&value) {
                    return Err(ExecutorError::Evaluation(
                        crate::executor::EvaluationError::TypeMismatch {
                            expected: current.type_name().into(),
                            actual: value.type_name().into(),
                        },
                    ));
                }
                let ordering = value.partial_cmp(current).ok_or_else(|| {
                    ExecutorError::Evaluation(crate::executor::EvaluationError::TypeMismatch {
                        expected: current.type_name().into(),
                        actual: value.type_name().into(),
                    })
                })?;
                let should_replace = matches!(
                    (self.is_min, ordering),
                    (true, Ordering::Less) | (false, Ordering::Greater)
                );
                if should_replace {
                    self.value = Some(value);
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<SqlValue> {
        Ok(self.value.clone().unwrap_or(SqlValue::Null))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Accumulator for GROUP_CONCAT.
#[derive(Debug, Clone)]
pub struct GroupConcatAccumulator {
    values: Vec<String>,
    separator: String,
}

impl GroupConcatAccumulator {
    /// Create a new GROUP_CONCAT accumulator with the given separator.
    pub fn new(separator: String) -> Self {
        Self {
            values: Vec::new(),
            separator,
        }
    }
}

impl Accumulator for GroupConcatAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        match value {
            SqlValue::Null => Ok(()),
            SqlValue::Text(text) => {
                self.values.push(text);
                Ok(())
            }
            other => Err(ExecutorError::Evaluation(
                crate::executor::EvaluationError::TypeMismatch {
                    expected: "Text".into(),
                    actual: other.type_name().into(),
                },
            )),
        }
    }

    fn finalize(&self) -> Result<SqlValue> {
        if self.values.is_empty() {
            return Ok(SqlValue::Null);
        }
        Ok(SqlValue::Text(self.values.join(&self.separator)))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Accumulator for STRING_AGG.
#[derive(Debug, Clone)]
pub struct StringAggAccumulator {
    values: Vec<String>,
    separator: String,
}

impl StringAggAccumulator {
    /// Create a new string_agg accumulator.
    pub fn new(separator: String) -> Self {
        Self {
            values: Vec::new(),
            separator,
        }
    }
}

impl Accumulator for StringAggAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        match value {
            SqlValue::Null => Ok(()),
            SqlValue::Text(s) => {
                self.values.push(s);
                Ok(())
            }
            other => Err(ExecutorError::Evaluation(
                crate::executor::EvaluationError::TypeMismatch {
                    expected: "Text".into(),
                    actual: other.type_name().into(),
                },
            )),
        }
    }

    fn finalize(&self) -> Result<SqlValue> {
        if self.values.is_empty() {
            return Ok(SqlValue::Null);
        }
        Ok(SqlValue::Text(self.values.join(&self.separator)))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Create a new accumulator instance for the aggregate function.
pub fn create_accumulator(function: &AggregateFunction, distinct: bool) -> Box<dyn Accumulator> {
    match function {
        AggregateFunction::Count => Box::new(CountAccumulator::new(distinct)),
        AggregateFunction::Sum => Box::new(SumAccumulator::new()),
        AggregateFunction::Total => Box::new(TotalAccumulator::new()),
        AggregateFunction::Avg => Box::new(AvgAccumulator::new()),
        AggregateFunction::Min => Box::new(MinMaxAccumulator::new(true)),
        AggregateFunction::Max => Box::new(MinMaxAccumulator::new(false)),
        AggregateFunction::GroupConcat { separator } => {
            let sep = separator.clone().unwrap_or_else(|| ",".to_string());
            Box::new(GroupConcatAccumulator::new(sep))
        }
        AggregateFunction::StringAgg { separator } => {
            let sep = separator.clone().unwrap_or_else(|| ",".to_string());
            Box::new(StringAggAccumulator::new(sep))
        }
    }
}

const DEFAULT_GROUP_LIMIT: usize = 1_000_000;
const AGGREGATE_ACCUMULATOR_OVERHEAD_BYTES: u64 = 32;

struct AggregateGroup {
    key_values: Vec<SqlValue>,
    accumulators: Vec<Box<dyn Accumulator>>,
}

/// Iterator that performs hash-based aggregation over input rows.
pub struct AggregateIterator<'a> {
    input: Box<dyn RowIterator + 'a>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<TypedExpr>,
    hash_table: Option<HashMap<GroupKeyBytes, AggregateGroup>>,
    result_rows: Vec<Row>,
    index: usize,
    schema: Vec<ColumnMetadata>,
    group_limit: usize,
    memory_tracker: Option<MemoryTracker>,
}

impl<'a> AggregateIterator<'a> {
    /// Create a new aggregate iterator with the default group limit.
    pub fn new(
        input: Box<dyn RowIterator + 'a>,
        group_keys: Vec<TypedExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<TypedExpr>,
        schema: Vec<ColumnMetadata>,
    ) -> Self {
        Self {
            input,
            group_keys,
            aggregates,
            having,
            hash_table: None,
            result_rows: Vec::new(),
            index: 0,
            schema,
            group_limit: DEFAULT_GROUP_LIMIT,
            memory_tracker: None,
        }
    }

    /// Override the maximum number of groups allowed during aggregation.
    pub fn with_group_limit(mut self, limit: usize) -> Self {
        self.group_limit = limit;
        self
    }

    /// Attach a memory policy for enforcing in-flight aggregation limits.
    pub fn with_memory_policy(mut self, policy: Option<MemoryPolicy>) -> Self {
        self.memory_tracker = policy.map(MemoryTracker::new);
        self
    }

    fn build_hash_table(&mut self) -> Result<()> {
        let mut table: HashMap<GroupKeyBytes, AggregateGroup> = HashMap::new();
        let mut next_row_id = 0u64;

        while let Some(result) = self.input.next_row() {
            let row = result?;
            let ctx = EvalContext::new(&row.values);

            let mut key_values = Vec::with_capacity(self.group_keys.len());
            for expr in &self.group_keys {
                key_values.push(crate::executor::evaluator::evaluate(expr, &ctx)?);
            }
            let key_bytes = encode_group_key(&key_values)?;

            if !table.contains_key(&key_bytes) {
                if table.len() + 1 > self.group_limit {
                    return Err(ExecutorError::ResourceExhausted {
                        message: format!(
                            "GROUP BY result exceeds memory limit (max groups: {})",
                            self.group_limit
                        ),
                    });
                }
                if let Some(tracker) = &mut self.memory_tracker {
                    tracker.add_values(&key_values)?;
                    tracker.add_bytes(
                        self.aggregates.len() as u64 * AGGREGATE_ACCUMULATOR_OVERHEAD_BYTES,
                    )?;
                }
                let accumulators = self
                    .aggregates
                    .iter()
                    .map(|agg| create_accumulator(&agg.function, agg.distinct))
                    .collect::<Vec<_>>();
                table.insert(
                    key_bytes.clone(),
                    AggregateGroup {
                        key_values: key_values.clone(),
                        accumulators,
                    },
                );
            }

            if let Some(group) = table.get_mut(&key_bytes) {
                for (idx, agg) in self.aggregates.iter().enumerate() {
                    let value = match &agg.arg {
                        None => None,
                        Some(expr) => Some(crate::executor::evaluator::evaluate(expr, &ctx)?),
                    };
                    if let Some(tracker) = &mut self.memory_tracker
                        && matches!(
                            agg.function,
                            AggregateFunction::GroupConcat { .. }
                                | AggregateFunction::StringAgg { .. }
                        )
                        && let Some(value_ref) = value.as_ref()
                    {
                        tracker.add_value(value_ref)?;
                    }
                    group.accumulators[idx].update(value)?;
                }
            }
        }

        if table.is_empty() && self.group_keys.is_empty() {
            if let Some(tracker) = &mut self.memory_tracker {
                tracker.add_bytes(
                    self.aggregates.len() as u64 * AGGREGATE_ACCUMULATOR_OVERHEAD_BYTES,
                )?;
            }
            let accumulators = self
                .aggregates
                .iter()
                .map(|agg| create_accumulator(&agg.function, agg.distinct))
                .collect::<Vec<_>>();
            table.insert(
                Vec::new(),
                AggregateGroup {
                    key_values: Vec::new(),
                    accumulators,
                },
            );
        }

        let mut rows = Vec::with_capacity(table.len());
        for group in table.values() {
            let mut values = Vec::with_capacity(self.group_keys.len() + self.aggregates.len());
            values.extend(group.key_values.iter().cloned());
            for acc in &group.accumulators {
                values.push(acc.finalize()?);
            }
            let row = Row::new(next_row_id, values);
            next_row_id += 1;
            if let Some(tracker) = &mut self.memory_tracker {
                tracker.add_row(&row.values)?;
            }

            if let Some(having) = &self.having {
                let ctx = EvalContext::new(&row.values);
                match crate::executor::evaluator::evaluate(having, &ctx)? {
                    SqlValue::Boolean(true) => rows.push(row),
                    SqlValue::Boolean(false) | SqlValue::Null => {}
                    other => {
                        return Err(ExecutorError::Evaluation(
                            crate::executor::EvaluationError::TypeMismatch {
                                expected: "Boolean".into(),
                                actual: other.type_name().into(),
                            },
                        ));
                    }
                }
            } else {
                rows.push(row);
            }
        }

        self.hash_table = Some(table);
        self.result_rows = rows;
        Ok(())
    }
}

impl<'a> RowIterator for AggregateIterator<'a> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        if self.hash_table.is_none()
            && let Err(err) = self.build_hash_table()
        {
            return Some(Err(err));
        }

        if self.index >= self.result_rows.len() {
            return None;
        }
        let row = self.result_rows[self.index].clone();
        self.index += 1;
        Some(Ok(row))
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

/// Iterator that performs streaming aggregation over sorted input.
pub struct StreamingAggregateIterator<'a> {
    input: Box<dyn RowIterator + 'a>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<TypedExpr>,
    schema: Vec<ColumnMetadata>,
    current_key: Option<Vec<SqlValue>>,
    accumulators: Vec<Box<dyn Accumulator>>,
    pending_row: Option<Row>,
    finished: bool,
    next_row_id: u64,
    saw_row: bool,
}

impl<'a> StreamingAggregateIterator<'a> {
    pub fn new(
        input: Box<dyn RowIterator + 'a>,
        group_keys: Vec<TypedExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<TypedExpr>,
        schema: Vec<ColumnMetadata>,
    ) -> Self {
        Self {
            input,
            group_keys,
            aggregates,
            having,
            schema,
            current_key: None,
            accumulators: Vec::new(),
            pending_row: None,
            finished: false,
            next_row_id: 0,
            saw_row: false,
        }
    }

    fn init_accumulators(&self) -> Vec<Box<dyn Accumulator>> {
        self.aggregates
            .iter()
            .map(|agg| create_accumulator(&agg.function, agg.distinct))
            .collect()
    }

    fn update_accumulators(&mut self, ctx: &EvalContext<'_>) -> Result<()> {
        for (idx, agg) in self.aggregates.iter().enumerate() {
            let value = match &agg.arg {
                None => None,
                Some(expr) => Some(crate::executor::evaluator::evaluate(expr, ctx)?),
            };
            self.accumulators[idx].update(value)?;
        }
        Ok(())
    }

    fn finalize_group(&mut self, key_values: &[SqlValue]) -> Result<Option<Row>> {
        let mut values = Vec::with_capacity(self.group_keys.len() + self.aggregates.len());
        values.extend(key_values.iter().cloned());
        for acc in &self.accumulators {
            values.push(acc.finalize()?);
        }
        let row = Row::new(self.next_row_id, values);
        self.next_row_id = self.next_row_id.saturating_add(1);

        if let Some(having) = &self.having {
            let ctx = EvalContext::new(&row.values);
            match crate::executor::evaluator::evaluate(having, &ctx)? {
                SqlValue::Boolean(true) => Ok(Some(row)),
                SqlValue::Boolean(false) | SqlValue::Null => Ok(None),
                other => Err(ExecutorError::Evaluation(
                    crate::executor::EvaluationError::TypeMismatch {
                        expected: "Boolean".into(),
                        actual: other.type_name().into(),
                    },
                )),
            }
        } else {
            Ok(Some(row))
        }
    }
}

impl<'a> RowIterator for StreamingAggregateIterator<'a> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        if let Some(row) = self.pending_row.take() {
            return Some(Ok(row));
        }
        if self.finished {
            return None;
        }

        loop {
            match self.input.next_row() {
                Some(Ok(row)) => {
                    self.saw_row = true;
                    let ctx = EvalContext::new(&row.values);
                    let mut key_values = Vec::with_capacity(self.group_keys.len());
                    for expr in &self.group_keys {
                        match crate::executor::evaluator::evaluate(expr, &ctx) {
                            Ok(value) => key_values.push(value),
                            Err(err) => return Some(Err(err)),
                        }
                    }

                    match &self.current_key {
                        None => {
                            self.current_key = Some(key_values);
                            self.accumulators = self.init_accumulators();
                            if let Err(err) = self.update_accumulators(&ctx) {
                                return Some(Err(err));
                            }
                        }
                        Some(current_key) if *current_key == key_values => {
                            if let Err(err) = self.update_accumulators(&ctx) {
                                return Some(Err(err));
                            }
                        }
                        Some(_) => {
                            let current_key = self.current_key.clone().unwrap_or_default();
                            let output = match self.finalize_group(&current_key) {
                                Ok(value) => value,
                                Err(err) => return Some(Err(err)),
                            };
                            self.current_key = Some(key_values);
                            self.accumulators = self.init_accumulators();
                            if let Err(err) = self.update_accumulators(&ctx) {
                                return Some(Err(err));
                            }
                            if let Some(row) = output {
                                return Some(Ok(row));
                            }
                        }
                    }
                }
                Some(Err(err)) => return Some(Err(err)),
                None => {
                    self.finished = true;
                    if let Some(current_key) = self.current_key.take() {
                        return match self.finalize_group(&current_key) {
                            Ok(Some(row)) => Some(Ok(row)),
                            Ok(None) => None,
                            Err(err) => Some(Err(err)),
                        };
                    }

                    if self.group_keys.is_empty() && !self.saw_row {
                        self.accumulators = self.init_accumulators();
                        return match self.finalize_group(&[]) {
                            Ok(Some(row)) => Some(Ok(row)),
                            Ok(None) => None,
                            Err(err) => Some(Err(err)),
                        };
                    }

                    return None;
                }
            }
        }
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

/// Build output schema for aggregate results.
pub fn build_aggregate_schema(
    group_keys: &[TypedExpr],
    aggregates: &[AggregateExpr],
) -> Vec<ColumnMetadata> {
    let mut schema = Vec::new();
    for (idx, key) in group_keys.iter().enumerate() {
        let name = match &key.kind {
            crate::planner::typed_expr::TypedExprKind::ColumnRef { column, .. } => column.clone(),
            _ => format!("group_{idx}"),
        };
        schema.push(ColumnMetadata::new(name, key.resolved_type.clone()));
    }
    for (idx, agg) in aggregates.iter().enumerate() {
        let name = match &agg.function {
            AggregateFunction::Count => format!("count_{idx}"),
            AggregateFunction::Sum => format!("sum_{idx}"),
            AggregateFunction::Total => format!("total_{idx}"),
            AggregateFunction::Avg => format!("avg_{idx}"),
            AggregateFunction::Min => format!("min_{idx}"),
            AggregateFunction::Max => format!("max_{idx}"),
            AggregateFunction::GroupConcat { .. } => format!("group_concat_{idx}"),
            AggregateFunction::StringAgg { .. } => format!("string_agg_{idx}"),
        };
        schema.push(ColumnMetadata::new(name, agg.result_type.clone()));
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_accumulator_counts_rows_and_skips_nulls() {
        let mut acc = CountAccumulator::new(false);
        acc.update(None).unwrap();
        acc.update(Some(SqlValue::Null)).unwrap();
        acc.update(Some(SqlValue::Integer(1))).unwrap();
        assert_eq!(acc.finalize().unwrap(), SqlValue::BigInt(2));
    }

    #[test]
    fn count_accumulator_distinct_deduplicates() {
        let mut acc = CountAccumulator::new(true);
        acc.update(Some(SqlValue::Integer(1))).unwrap();
        acc.update(Some(SqlValue::Integer(1))).unwrap();
        acc.update(Some(SqlValue::Integer(2))).unwrap();
        assert_eq!(acc.finalize().unwrap(), SqlValue::BigInt(2));
    }

    #[test]
    fn sum_accumulator_aggregates_numeric_values() {
        let mut acc = SumAccumulator::new();
        acc.update(Some(SqlValue::Integer(2))).unwrap();
        acc.update(Some(SqlValue::Double(3.5))).unwrap();
        acc.update(Some(SqlValue::Null)).unwrap();
        assert_eq!(acc.finalize().unwrap(), SqlValue::Double(5.5));
    }

    #[test]
    fn total_accumulator_returns_zero_for_empty() {
        let acc = TotalAccumulator::new();
        assert_eq!(acc.finalize().unwrap(), SqlValue::Double(0.0));
    }

    #[test]
    fn total_accumulator_aggregates_numeric_values() {
        let mut acc = TotalAccumulator::new();
        acc.update(Some(SqlValue::Integer(2))).unwrap();
        acc.update(Some(SqlValue::Null)).unwrap();
        acc.update(Some(SqlValue::Double(1.5))).unwrap();
        assert_eq!(acc.finalize().unwrap(), SqlValue::Double(3.5));
    }

    #[test]
    fn avg_accumulator_handles_empty_and_nulls() {
        let mut acc = AvgAccumulator::new();
        assert_eq!(acc.finalize().unwrap(), SqlValue::Null);
        acc.update(Some(SqlValue::Null)).unwrap();
        acc.update(Some(SqlValue::BigInt(4))).unwrap();
        acc.update(Some(SqlValue::Integer(2))).unwrap();
        assert_eq!(acc.finalize().unwrap(), SqlValue::Double(3.0));
    }

    #[test]
    fn min_max_accumulator_tracks_extremes() {
        let mut min_acc = MinMaxAccumulator::new(true);
        let mut max_acc = MinMaxAccumulator::new(false);
        for value in [3, 1, 2] {
            min_acc.update(Some(SqlValue::Integer(value))).unwrap();
            max_acc.update(Some(SqlValue::Integer(value))).unwrap();
        }
        assert_eq!(min_acc.finalize().unwrap(), SqlValue::Integer(1));
        assert_eq!(max_acc.finalize().unwrap(), SqlValue::Integer(3));
    }

    #[test]
    fn min_max_accumulator_rejects_type_mismatch() {
        let mut acc = MinMaxAccumulator::new(true);
        acc.update(Some(SqlValue::Integer(1))).unwrap();
        let err = acc.update(Some(SqlValue::Text("bad".into()))).unwrap_err();
        match err {
            ExecutorError::Evaluation(crate::executor::EvaluationError::TypeMismatch {
                ..
            }) => {}
            other => panic!("unexpected error {:?}", other),
        }
    }

    #[test]
    fn group_concat_accumulator_joins_values() {
        let mut acc = GroupConcatAccumulator::new("|".into());
        acc.update(Some(SqlValue::Text("a".into()))).unwrap();
        acc.update(Some(SqlValue::Null)).unwrap();
        acc.update(Some(SqlValue::Text("b".into()))).unwrap();
        assert_eq!(acc.finalize().unwrap(), SqlValue::Text("a|b".into()));
    }

    #[test]
    fn group_concat_accumulator_empty_returns_null() {
        let acc = GroupConcatAccumulator::new(",".into());
        assert_eq!(acc.finalize().unwrap(), SqlValue::Null);
    }

    #[test]
    fn string_agg_accumulator_joins_values() {
        let mut acc = StringAggAccumulator::new("::".into());
        acc.update(Some(SqlValue::Text("a".into()))).unwrap();
        acc.update(Some(SqlValue::Null)).unwrap();
        acc.update(Some(SqlValue::Text("b".into()))).unwrap();
        assert_eq!(acc.finalize().unwrap(), SqlValue::Text("a::b".into()));
    }

    #[test]
    fn string_agg_accumulator_empty_returns_null() {
        let acc = StringAggAccumulator::new(",".into());
        assert_eq!(acc.finalize().unwrap(), SqlValue::Null);
    }

    #[test]
    fn encode_group_key_is_deterministic() {
        let values = vec![
            SqlValue::Integer(1),
            SqlValue::Text("a".into()),
            SqlValue::Null,
        ];
        let first = encode_group_key(&values).unwrap();
        let second = encode_group_key(&values).unwrap();
        assert_eq!(first, second);
    }
}
