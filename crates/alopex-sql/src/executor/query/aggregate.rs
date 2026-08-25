use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use crate::catalog::ColumnMetadata;
use crate::executor::evaluator::EvalContext;
use crate::executor::memory::{MemoryPolicy, MemoryTracker, map_core_memory_error};
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::aggregate_expr::{AggregateExpr, AggregateFunction};
use crate::planner::typed_expr::TypedExpr;
use crate::planner::types::ResolvedType;
use crate::storage::SqlValue;
use alopex_core::sql::stream::ByteSized;

use super::{Row, RowIterator, iterator::VecIterator};

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
    /// Update with the row's aggregate-local sort key values (issue #148).
    ///
    /// Order-insensitive accumulators ignore the keys and defer to
    /// [`Accumulator::update`]; order-sensitive accumulators buffer
    /// `(keys, value)` pairs and sort in [`Accumulator::finalize`].
    fn update_ordered(&mut self, value: Option<SqlValue>, _sort_keys: &[SqlValue]) -> Result<()> {
        self.update(value)
    }
    /// Return the serializable partial aggregate state.
    fn state(&self) -> Result<Vec<SqlValue>>;
    /// Merge a partial state produced by an accumulator of the same function.
    fn merge(&mut self, state: &[SqlValue]) -> Result<()>;
    /// Finalize the accumulator and return the resulting SqlValue.
    fn finalize(&self) -> Result<SqlValue>;
    /// Clone the accumulator as a trait object.
    fn clone_box(&self) -> Box<dyn Accumulator>;
    /// Estimated bytes retained by dynamically allocated accumulator state.
    ///
    /// Window execution uses this to charge the shared operator memory budget
    /// while an accumulator is alive.
    fn retained_bytes(&self) -> u64 {
        0
    }
}

impl Clone for Box<dyn Accumulator> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

fn invalid_aggregate_state(function: &str, reason: impl Into<String>) -> ExecutorError {
    ExecutorError::InvalidOperation {
        operation: function.into(),
        reason: reason.into(),
    }
}

fn expect_state_arity(function: &str, state: &[SqlValue], expected: usize) -> Result<()> {
    if state.len() == expected {
        Ok(())
    } else {
        Err(invalid_aggregate_state(
            function,
            format!("expected {expected} state value(s), got {}", state.len()),
        ))
    }
}

fn state_bigint(function: &str, value: &SqlValue, index: usize) -> Result<i64> {
    match value {
        SqlValue::BigInt(v) => Ok(*v),
        other => Err(invalid_aggregate_state(
            function,
            format!(
                "state value {index} expected BigInt, got {}",
                other.type_name()
            ),
        )),
    }
}

fn state_double(function: &str, value: &SqlValue, index: usize) -> Result<f64> {
    match value {
        SqlValue::Double(v) => Ok(*v),
        other => Err(invalid_aggregate_state(
            function,
            format!(
                "state value {index} expected Double, got {}",
                other.type_name()
            ),
        )),
    }
}

fn state_text<'a>(function: &str, value: &'a SqlValue, index: usize) -> Result<&'a str> {
    match value {
        SqlValue::Text(v) => Ok(v),
        other => Err(invalid_aggregate_state(
            function,
            format!(
                "state value {index} expected Text, got {}",
                other.type_name()
            ),
        )),
    }
}

fn distinct_allows(
    distinct_values: &mut Option<HashSet<Vec<u8>>>,
    value: &SqlValue,
) -> Result<bool> {
    if value.is_null() {
        return Ok(false);
    }
    let Some(distinct) = distinct_values else {
        return Ok(true);
    };
    let encoded = encode_group_key(std::slice::from_ref(value))?;
    Ok(distinct.insert(encoded))
}

fn estimated_distinct_retained_bytes(distinct_values: &Option<HashSet<Vec<u8>>>) -> u64 {
    let Some(values) = distinct_values else {
        return 0;
    };
    // Charge every allocated hash bucket conservatively as a stored Vec plus
    // hash/control metadata, then add each encoded key's owned allocation.
    let bucket_bytes = values
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<u64>() + 1);
    values.iter().fold(
        u64::try_from(bucket_bytes).unwrap_or(u64::MAX),
        |total, value| total.saturating_add(u64::try_from(value.capacity()).unwrap_or(u64::MAX)),
    )
}

fn estimated_string_collection_bytes(
    values: &[String],
    values_capacity: usize,
    separator_capacity: usize,
) -> u64 {
    let slots = values_capacity.saturating_mul(std::mem::size_of::<String>());
    values
        .iter()
        .fold(u64::try_from(slots).unwrap_or(u64::MAX), |total, value| {
            total.saturating_add(u64::try_from(value.capacity()).unwrap_or(u64::MAX))
        })
        .saturating_add(u64::try_from(separator_capacity).unwrap_or(u64::MAX))
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
                let encoded = encode_group_key(std::slice::from_ref(&value))?;
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

    fn state(&self) -> Result<Vec<SqlValue>> {
        Ok(vec![SqlValue::BigInt(self.count as i64)])
    }

    fn merge(&mut self, state: &[SqlValue]) -> Result<()> {
        expect_state_arity("count", state, 1)?;
        let count = state_bigint("count", &state[0], 0)?;
        if count < 0 {
            return Err(invalid_aggregate_state(
                "count",
                "state count must be non-negative",
            ));
        }
        self.count = self.count.saturating_add(count as usize);
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn retained_bytes(&self) -> u64 {
        estimated_distinct_retained_bytes(&self.distinct_values)
    }
}

/// Accumulator for SUM.
#[derive(Debug, Clone)]
pub struct SumAccumulator {
    sum: Option<SqlValue>,
    result_type: ResolvedType,
    distinct_values: Option<HashSet<Vec<u8>>>,
}

impl SumAccumulator {
    /// Create a new sum accumulator.
    pub fn new() -> Self {
        Self::with_distinct(false)
    }

    pub fn with_distinct(distinct: bool) -> Self {
        Self::with_distinct_for_type(distinct, ResolvedType::Double)
    }

    pub fn with_distinct_for_type(distinct: bool, result_type: ResolvedType) -> Self {
        Self {
            sum: None,
            result_type,
            distinct_values: if distinct { Some(HashSet::new()) } else { None },
        }
    }

    fn add_value(&mut self, value: SqlValue) -> Result<()> {
        let next = match &self.result_type {
            ResolvedType::Integer => {
                let SqlValue::Integer(value) = value else {
                    return sum_type_mismatch("Integer", &value);
                };
                let sum = match self.sum.as_ref() {
                    None => value,
                    Some(SqlValue::Integer(current)) => current
                        .checked_add(value)
                        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    Some(other) => return sum_type_mismatch("Integer", other),
                };
                SqlValue::Integer(sum)
            }
            ResolvedType::BigInt => {
                let value = match value {
                    SqlValue::Integer(value) => i64::from(value),
                    SqlValue::BigInt(value) => value,
                    other => return sum_type_mismatch("BigInt", &other),
                };
                let sum = match self.sum.as_ref() {
                    None => value,
                    Some(SqlValue::BigInt(current)) => current
                        .checked_add(value)
                        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    Some(other) => return sum_type_mismatch("BigInt", other),
                };
                SqlValue::BigInt(sum)
            }
            _ => {
                let value = numeric_to_f64(&value)?;
                let sum = match self.sum.as_ref() {
                    None => value,
                    Some(SqlValue::Double(current)) => *current + value,
                    Some(other) => return sum_type_mismatch("Double", other),
                };
                SqlValue::Double(sum)
            }
        };
        self.sum = Some(next);
        Ok(())
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
        if !distinct_allows(&mut self.distinct_values, &value)? {
            return Ok(());
        }
        self.add_value(value)
    }

    fn finalize(&self) -> Result<SqlValue> {
        Ok(self.sum.clone().unwrap_or(SqlValue::Null))
    }

    fn state(&self) -> Result<Vec<SqlValue>> {
        Ok(vec![self.sum.clone().unwrap_or(SqlValue::Null)])
    }

    fn merge(&mut self, state: &[SqlValue]) -> Result<()> {
        expect_state_arity("sum", state, 1)?;
        if state[0].is_null() {
            return Ok(());
        }
        self.add_value(state[0].clone())
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn retained_bytes(&self) -> u64 {
        estimated_distinct_retained_bytes(&self.distinct_values).saturating_add(
            self.sum
                .as_ref()
                .map(ByteSized::estimated_bytes)
                .unwrap_or(0),
        )
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

    fn state(&self) -> Result<Vec<SqlValue>> {
        Ok(vec![SqlValue::Double(self.sum.unwrap_or(0.0))])
    }

    fn merge(&mut self, state: &[SqlValue]) -> Result<()> {
        expect_state_arity("total", state, 1)?;
        let value = state_double("total", &state[0], 0)?;
        self.sum = Some(self.sum.unwrap_or(0.0) + value);
        Ok(())
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
    distinct_values: Option<HashSet<Vec<u8>>>,
}

impl AvgAccumulator {
    /// Create a new average accumulator.
    pub fn new() -> Self {
        Self::with_distinct(false)
    }

    pub fn with_distinct(distinct: bool) -> Self {
        Self {
            sum: None,
            count: 0,
            distinct_values: if distinct { Some(HashSet::new()) } else { None },
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
        if !distinct_allows(&mut self.distinct_values, &value)? {
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

    fn state(&self) -> Result<Vec<SqlValue>> {
        Ok(vec![
            SqlValue::Double(self.sum.unwrap_or(0.0)),
            SqlValue::BigInt(self.count as i64),
        ])
    }

    fn merge(&mut self, state: &[SqlValue]) -> Result<()> {
        expect_state_arity("avg", state, 2)?;
        let sum = state_double("avg", &state[0], 0)?;
        let count = state_bigint("avg", &state[1], 1)?;
        if count < 0 {
            return Err(invalid_aggregate_state(
                "avg",
                "state count must be non-negative",
            ));
        }
        self.sum = Some(self.sum.unwrap_or(0.0) + sum);
        self.count = self.count.saturating_add(count as usize);
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn retained_bytes(&self) -> u64 {
        estimated_distinct_retained_bytes(&self.distinct_values)
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

fn sum_type_mismatch<T>(expected: &str, actual: &SqlValue) -> Result<T> {
    Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
        expected: expected.into(),
        actual: actual.type_name().into(),
    }))
}

/// Accumulator for MIN / MAX.
#[derive(Debug, Clone)]
pub struct MinMaxAccumulator {
    value: Option<SqlValue>,
    is_min: bool,
    distinct_values: Option<HashSet<Vec<u8>>>,
}

impl MinMaxAccumulator {
    /// Create a new min/max accumulator.
    pub fn new(is_min: bool) -> Self {
        Self::with_distinct(is_min, false)
    }

    pub fn with_distinct(is_min: bool, distinct: bool) -> Self {
        Self {
            value: None,
            is_min,
            distinct_values: if distinct { Some(HashSet::new()) } else { None },
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
        if !distinct_allows(&mut self.distinct_values, &value)? {
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

    fn state(&self) -> Result<Vec<SqlValue>> {
        Ok(vec![self.value.clone().unwrap_or(SqlValue::Null)])
    }

    fn merge(&mut self, state: &[SqlValue]) -> Result<()> {
        expect_state_arity(if self.is_min { "min" } else { "max" }, state, 1)?;
        if state[0].is_null() {
            return Ok(());
        }
        self.update(Some(state[0].clone()))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn retained_bytes(&self) -> u64 {
        estimated_distinct_retained_bytes(&self.distinct_values).saturating_add(
            self.value
                .as_ref()
                .map(ByteSized::estimated_bytes)
                .unwrap_or(0),
        )
    }
}

/// Compare aggregate-local sort keys under per-key `(asc, nulls_first)`
/// specifications (issue #148).
fn compare_ordered_keys(left: &[SqlValue], right: &[SqlValue], specs: &[(bool, bool)]) -> Ordering {
    for (idx, (asc, nulls_first)) in specs.iter().enumerate() {
        let left_value = left.get(idx).unwrap_or(&SqlValue::Null);
        let right_value = right.get(idx).unwrap_or(&SqlValue::Null);
        let cmp = super::iterator::compare_single(left_value, right_value, *asc, *nulls_first);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn estimated_ordered_string_bytes(values: &[(Vec<SqlValue>, String)]) -> u64 {
    values.iter().fold(0u64, |total, (keys, value)| {
        let key_bytes: u64 = keys.iter().map(ByteSized::estimated_bytes).sum();
        total
            .saturating_add(key_bytes)
            .saturating_add(u64::try_from(value.capacity()).unwrap_or(u64::MAX))
    })
}

/// Accumulator for GROUP_CONCAT.
#[derive(Debug, Clone)]
pub struct GroupConcatAccumulator {
    values: Vec<String>,
    separator: String,
    distinct_values: Option<HashSet<Vec<u8>>>,
    /// Non-empty selects the ordered mode: pairs are buffered and stably
    /// sorted at finalize (issue #148).
    sort_specs: Vec<(bool, bool)>,
    ordered_values: Vec<(Vec<SqlValue>, String)>,
}

impl GroupConcatAccumulator {
    /// Create a new GROUP_CONCAT accumulator with the given separator.
    pub fn new(separator: String) -> Self {
        Self::with_distinct(separator, false)
    }

    pub fn with_distinct(separator: String, distinct: bool) -> Self {
        Self::with_order(separator, distinct, Vec::new())
    }

    pub fn with_order(separator: String, distinct: bool, sort_specs: Vec<(bool, bool)>) -> Self {
        Self {
            values: Vec::new(),
            separator,
            distinct_values: if distinct { Some(HashSet::new()) } else { None },
            sort_specs,
            ordered_values: Vec::new(),
        }
    }
}

impl Accumulator for GroupConcatAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        if !self.sort_specs.is_empty() {
            return Err(invalid_aggregate_state(
                "group_concat",
                "ordered aggregation requires the sort keys of every row",
            ));
        }
        let Some(value) = value else {
            return Ok(());
        };
        match value {
            SqlValue::Null => Ok(()),
            SqlValue::Text(text) => {
                let value = SqlValue::Text(text.clone());
                if !distinct_allows(&mut self.distinct_values, &value)? {
                    return Ok(());
                }
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

    fn update_ordered(&mut self, value: Option<SqlValue>, sort_keys: &[SqlValue]) -> Result<()> {
        if self.sort_specs.is_empty() {
            return self.update(value);
        }
        let Some(value) = value else {
            return Ok(());
        };
        match value {
            SqlValue::Null => Ok(()),
            SqlValue::Text(text) => {
                let value = SqlValue::Text(text.clone());
                if !distinct_allows(&mut self.distinct_values, &value)? {
                    return Ok(());
                }
                self.ordered_values.push((sort_keys.to_vec(), text));
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
        if !self.sort_specs.is_empty() {
            if self.ordered_values.is_empty() {
                return Ok(SqlValue::Null);
            }
            let mut sorted = self.ordered_values.clone();
            sorted.sort_by(|left, right| compare_ordered_keys(&left.0, &right.0, &self.sort_specs));
            let joined = sorted
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>()
                .join(&self.separator);
            return Ok(SqlValue::Text(joined));
        }
        if self.values.is_empty() {
            return Ok(SqlValue::Null);
        }
        Ok(SqlValue::Text(self.values.join(&self.separator)))
    }

    fn state(&self) -> Result<Vec<SqlValue>> {
        if !self.sort_specs.is_empty() {
            return Err(invalid_aggregate_state(
                "group_concat",
                "ordered aggregation cannot produce partial state",
            ));
        }
        Ok(vec![
            if self.values.is_empty() {
                SqlValue::Null
            } else {
                SqlValue::Text(self.values.join(&self.separator))
            },
            SqlValue::Text(self.separator.clone()),
        ])
    }

    fn merge(&mut self, state: &[SqlValue]) -> Result<()> {
        if !self.sort_specs.is_empty() {
            return Err(invalid_aggregate_state(
                "group_concat",
                "ordered aggregation cannot merge partial state",
            ));
        }
        expect_state_arity("group_concat", state, 2)?;
        let separator = state_text("group_concat", &state[1], 1)?;
        if separator != self.separator {
            return Err(invalid_aggregate_state(
                "group_concat",
                "state separator differs from accumulator separator",
            ));
        }
        match &state[0] {
            SqlValue::Null => Ok(()),
            SqlValue::Text(text) => {
                self.values.push(text.clone());
                Ok(())
            }
            other => Err(invalid_aggregate_state(
                "group_concat",
                format!(
                    "state value 0 expected Text or Null, got {}",
                    other.type_name()
                ),
            )),
        }
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn retained_bytes(&self) -> u64 {
        estimated_distinct_retained_bytes(&self.distinct_values)
            .saturating_add(estimated_string_collection_bytes(
                &self.values,
                self.values.capacity(),
                self.separator.capacity(),
            ))
            .saturating_add(estimated_ordered_string_bytes(&self.ordered_values))
    }
}

/// Accumulator for STRING_AGG.
#[derive(Debug, Clone)]
pub struct StringAggAccumulator {
    values: Vec<String>,
    separator: String,
    distinct_values: Option<HashSet<Vec<u8>>>,
    /// Non-empty selects the ordered mode (see [`GroupConcatAccumulator`]).
    sort_specs: Vec<(bool, bool)>,
    ordered_values: Vec<(Vec<SqlValue>, String)>,
}

impl StringAggAccumulator {
    /// Create a new string_agg accumulator.
    pub fn new(separator: String) -> Self {
        Self::with_distinct(separator, false)
    }

    pub fn with_distinct(separator: String, distinct: bool) -> Self {
        Self::with_order(separator, distinct, Vec::new())
    }

    pub fn with_order(separator: String, distinct: bool, sort_specs: Vec<(bool, bool)>) -> Self {
        Self {
            values: Vec::new(),
            separator,
            distinct_values: if distinct { Some(HashSet::new()) } else { None },
            sort_specs,
            ordered_values: Vec::new(),
        }
    }
}

impl Accumulator for StringAggAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        if !self.sort_specs.is_empty() {
            return Err(invalid_aggregate_state(
                "string_agg",
                "ordered aggregation requires the sort keys of every row",
            ));
        }
        let Some(value) = value else {
            return Ok(());
        };
        match value {
            SqlValue::Null => Ok(()),
            SqlValue::Text(s) => {
                let value = SqlValue::Text(s.clone());
                if !distinct_allows(&mut self.distinct_values, &value)? {
                    return Ok(());
                }
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

    fn update_ordered(&mut self, value: Option<SqlValue>, sort_keys: &[SqlValue]) -> Result<()> {
        if self.sort_specs.is_empty() {
            return self.update(value);
        }
        let Some(value) = value else {
            return Ok(());
        };
        match value {
            SqlValue::Null => Ok(()),
            SqlValue::Text(s) => {
                let value = SqlValue::Text(s.clone());
                if !distinct_allows(&mut self.distinct_values, &value)? {
                    return Ok(());
                }
                self.ordered_values.push((sort_keys.to_vec(), s));
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
        if !self.sort_specs.is_empty() {
            if self.ordered_values.is_empty() {
                return Ok(SqlValue::Null);
            }
            let mut sorted = self.ordered_values.clone();
            sorted.sort_by(|left, right| compare_ordered_keys(&left.0, &right.0, &self.sort_specs));
            let joined = sorted
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>()
                .join(&self.separator);
            return Ok(SqlValue::Text(joined));
        }
        if self.values.is_empty() {
            return Ok(SqlValue::Null);
        }
        Ok(SqlValue::Text(self.values.join(&self.separator)))
    }

    fn state(&self) -> Result<Vec<SqlValue>> {
        if !self.sort_specs.is_empty() {
            return Err(invalid_aggregate_state(
                "string_agg",
                "ordered aggregation cannot produce partial state",
            ));
        }
        Ok(vec![
            if self.values.is_empty() {
                SqlValue::Null
            } else {
                SqlValue::Text(self.values.join(&self.separator))
            },
            SqlValue::Text(self.separator.clone()),
        ])
    }

    fn merge(&mut self, state: &[SqlValue]) -> Result<()> {
        if !self.sort_specs.is_empty() {
            return Err(invalid_aggregate_state(
                "string_agg",
                "ordered aggregation cannot merge partial state",
            ));
        }
        expect_state_arity("string_agg", state, 2)?;
        let separator = state_text("string_agg", &state[1], 1)?;
        if separator != self.separator {
            return Err(invalid_aggregate_state(
                "string_agg",
                "state separator differs from accumulator separator",
            ));
        }
        match &state[0] {
            SqlValue::Null => Ok(()),
            SqlValue::Text(text) => {
                self.values.push(text.clone());
                Ok(())
            }
            other => Err(invalid_aggregate_state(
                "string_agg",
                format!(
                    "state value 0 expected Text or Null, got {}",
                    other.type_name()
                ),
            )),
        }
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn retained_bytes(&self) -> u64 {
        estimated_distinct_retained_bytes(&self.distinct_values)
            .saturating_add(estimated_string_collection_bytes(
                &self.values,
                self.values.capacity(),
                self.separator.capacity(),
            ))
            .saturating_add(estimated_ordered_string_bytes(&self.ordered_values))
    }
}

/// Accumulator for the ordered-set aggregate PERCENTILE_DISC (issue #148).
///
/// Buffers `(sort_keys, value)` pairs, sorts them at finalize, and returns the
/// first value whose cumulative distribution reaches the fraction:
/// `index = max(ceil(fraction * n) - 1, 0)` — PostgreSQL 16 semantics (D5).
/// NULL sort values are excluded; an empty group yields NULL. Partial state is
/// rejected: ordered-set aggregation always runs in Single mode (D11).
#[derive(Debug, Clone)]
pub struct PercentileDiscAccumulator {
    fraction: f64,
    sort_specs: Vec<(bool, bool)>,
    values: Vec<(Vec<SqlValue>, SqlValue)>,
}

impl PercentileDiscAccumulator {
    pub fn new(fraction: f64, sort_specs: Vec<(bool, bool)>) -> Self {
        let sort_specs = if sort_specs.is_empty() {
            vec![(true, false)]
        } else {
            sort_specs
        };
        Self {
            fraction,
            sort_specs,
            values: Vec::new(),
        }
    }
}

impl Accumulator for PercentileDiscAccumulator {
    fn update(&mut self, value: Option<SqlValue>) -> Result<()> {
        // The sort value is the aggregated value, so it doubles as its own
        // key when a caller has no separate key row.
        let keys = match &value {
            Some(inner) if !inner.is_null() => vec![inner.clone()],
            _ => return Ok(()),
        };
        self.update_ordered(value, &keys)
    }

    fn update_ordered(&mut self, value: Option<SqlValue>, sort_keys: &[SqlValue]) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        if value.is_null() {
            return Ok(());
        }
        self.values.push((sort_keys.to_vec(), value));
        Ok(())
    }

    fn finalize(&self) -> Result<SqlValue> {
        if self.values.is_empty() {
            return Ok(SqlValue::Null);
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|left, right| compare_ordered_keys(&left.0, &right.0, &self.sort_specs));
        let count = sorted.len();
        let index = (self.fraction * count as f64).ceil() as usize;
        let index = index.saturating_sub(1).min(count - 1);
        Ok(sorted[index].1.clone())
    }

    fn state(&self) -> Result<Vec<SqlValue>> {
        Err(invalid_aggregate_state(
            "percentile_disc",
            "ordered-set aggregation cannot produce partial state",
        ))
    }

    fn merge(&mut self, _state: &[SqlValue]) -> Result<()> {
        Err(invalid_aggregate_state(
            "percentile_disc",
            "ordered-set aggregation cannot merge partial state",
        ))
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn retained_bytes(&self) -> u64 {
        self.values.iter().fold(0u64, |total, (keys, value)| {
            let key_bytes: u64 = keys.iter().map(ByteSized::estimated_bytes).sum();
            total
                .saturating_add(key_bytes)
                .saturating_add(value.estimated_bytes())
        })
    }
}

/// Create a new accumulator instance for the aggregate function.
pub fn create_accumulator(function: &AggregateFunction, distinct: bool) -> Box<dyn Accumulator> {
    match function {
        AggregateFunction::Count => Box::new(CountAccumulator::new(distinct)),
        AggregateFunction::Sum => Box::new(SumAccumulator::with_distinct(distinct)),
        AggregateFunction::Total => Box::new(TotalAccumulator::new()),
        AggregateFunction::Avg => Box::new(AvgAccumulator::with_distinct(distinct)),
        AggregateFunction::Min => Box::new(MinMaxAccumulator::with_distinct(true, distinct)),
        AggregateFunction::Max => Box::new(MinMaxAccumulator::with_distinct(false, distinct)),
        AggregateFunction::GroupConcat { separator } => {
            let sep = separator.clone().unwrap_or_else(|| ",".to_string());
            Box::new(GroupConcatAccumulator::with_distinct(sep, distinct))
        }
        AggregateFunction::StringAgg { separator } => {
            let sep = separator.clone().unwrap_or_else(|| ",".to_string());
            Box::new(StringAggAccumulator::with_distinct(sep, distinct))
        }
        AggregateFunction::PercentileDisc { fraction } => {
            Box::new(PercentileDiscAccumulator::new(*fraction, Vec::new()))
        }
    }
}

fn aggregate_sort_specs(aggregate: &AggregateExpr) -> Vec<(bool, bool)> {
    aggregate
        .order_by
        .iter()
        .map(|sort| (sort.asc, sort.nulls_first))
        .collect()
}

/// Create an accumulator using the aggregate expression's resolved result
/// type and its aggregate-local ordering (issue #148).
pub fn create_accumulator_for_aggregate(aggregate: &AggregateExpr) -> Box<dyn Accumulator> {
    match &aggregate.function {
        AggregateFunction::Sum => Box::new(SumAccumulator::with_distinct_for_type(
            aggregate.distinct,
            aggregate.result_type.clone(),
        )),
        AggregateFunction::GroupConcat { separator } if !aggregate.order_by.is_empty() => {
            let sep = separator.clone().unwrap_or_else(|| ",".to_string());
            Box::new(GroupConcatAccumulator::with_order(
                sep,
                aggregate.distinct,
                aggregate_sort_specs(aggregate),
            ))
        }
        AggregateFunction::StringAgg { separator } if !aggregate.order_by.is_empty() => {
            let sep = separator.clone().unwrap_or_else(|| ",".to_string());
            Box::new(StringAggAccumulator::with_order(
                sep,
                aggregate.distinct,
                aggregate_sort_specs(aggregate),
            ))
        }
        AggregateFunction::PercentileDisc { fraction } => Box::new(PercentileDiscAccumulator::new(
            *fraction,
            aggregate_sort_specs(aggregate),
        )),
        _ => create_accumulator(&aggregate.function, aggregate.distinct),
    }
}

/// Returns whether an aggregate's partial state can be merged without
/// changing the current local SQL result. Floating-point, DISTINCT, and
/// order-sensitive aggregates must instead be replayed from ordered inputs.
/// A FILTER predicate does not affect this proof: it applies per input row
/// before the accumulator, so it commutes with Partial/Final splitting —
/// but the distributed catalog still classifies filtered aggregates as
/// local-only before any state reaches this kernel.
pub fn exact_partial_aggregate_is_proven(aggregate: &AggregateExpr) -> bool {
    !aggregate.distinct
        && aggregate.order_by.is_empty()
        && matches!(
            aggregate.function,
            AggregateFunction::Count | AggregateFunction::Min | AggregateFunction::Max
        )
}

/// Merge only aggregate states whose merge rule is proven to preserve the
/// current local SQL result. This is the coordinator-side kernel used by the
/// distributed result assembler after every worker has acknowledged cleanup.
pub fn merge_exact_aggregate_states(
    aggregates: &[AggregateExpr],
    partial_rows: impl IntoIterator<Item = Vec<Vec<SqlValue>>>,
) -> Result<Vec<SqlValue>> {
    if let Some(aggregate) = aggregates
        .iter()
        .find(|aggregate| !exact_partial_aggregate_is_proven(aggregate))
    {
        return Err(ExecutorError::InvalidOperation {
            operation: "distributed aggregate merge".into(),
            reason: format!(
                "{:?} requires ordered input replay rather than an exact partial merge",
                aggregate.function
            ),
        });
    }

    let mut accumulators = aggregates
        .iter()
        .map(create_accumulator_for_aggregate)
        .collect::<Vec<_>>();
    for states in partial_rows {
        if states.len() != accumulators.len() {
            return Err(ExecutorError::InvalidOperation {
                operation: "distributed aggregate merge".into(),
                reason: format!(
                    "partial state has {} aggregate(s), expected {}",
                    states.len(),
                    accumulators.len()
                ),
            });
        }
        for (accumulator, state) in accumulators.iter_mut().zip(states) {
            accumulator.merge(&state)?;
        }
    }
    accumulators
        .iter()
        .map(|accumulator| accumulator.finalize())
        .collect()
}

const DEFAULT_GROUP_LIMIT: usize = 1_000_000;
const AGGREGATE_ACCUMULATOR_OVERHEAD_BYTES: u64 = 32;

/// Aggregate execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateMode {
    /// Consume raw input rows and output aggregate partial state rows.
    Partial,
    /// Consume partial state rows and output final aggregate values.
    Final,
    /// Consume raw input rows and output final aggregate values in one pass.
    Single,
}

struct AggregateGroup {
    key_values: Vec<SqlValue>,
    accumulators: Vec<Box<dyn Accumulator>>,
    /// Grouping-set mask emitted as the trailing `__grouping_id` output
    /// column; `None` outside grouping-sets mode (issue #149).
    grouping_id: Option<i64>,
}

/// Iterator that performs hash-based aggregation over input rows.
pub struct AggregateIterator<'a> {
    input: Box<dyn RowIterator + 'a>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<TypedExpr>,
    mode: AggregateMode,
    hash_table: Option<HashMap<GroupKeyBytes, AggregateGroup>>,
    result_rows: Vec<Row>,
    index: usize,
    schema: Vec<ColumnMetadata>,
    group_limit: usize,
    memory_tracker: Option<MemoryTracker>,
    shared_group_counter: Option<Arc<AtomicUsize>>,
    /// Expanded grouping-set masks over `group_keys` (issue #149, D9).
    /// Key 0 owns the most significant of the low `group_keys.len()` bits;
    /// a 1 bit excludes the key from the set (NULL placeholder output).
    grouping_sets: Option<Vec<u64>>,
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
            mode: AggregateMode::Single,
            hash_table: None,
            result_rows: Vec::new(),
            index: 0,
            schema,
            group_limit: DEFAULT_GROUP_LIMIT,
            memory_tracker: None,
            shared_group_counter: None,
            grouping_sets: None,
        }
    }

    /// Override the maximum number of groups allowed during aggregation.
    pub fn with_group_limit(mut self, limit: usize) -> Self {
        self.group_limit = limit;
        self
    }

    /// Enable single-pass GROUPING SETS aggregation (issue #149, D9).
    ///
    /// Each input row accumulates once per set under a set-id-prefixed key;
    /// the output rows gain a trailing `__grouping_id` BIGINT value and the
    /// group limit applies to the group total across every set (D6). Only
    /// `AggregateMode::Single` supports grouping sets — the planner keeps
    /// parallel/spill/streaming execution on the `None` path.
    pub fn with_grouping_sets(mut self, grouping_sets: Option<Vec<u64>>) -> Self {
        self.grouping_sets = grouping_sets;
        self
    }

    /// Set the aggregate execution mode.
    pub fn with_mode(mut self, mode: AggregateMode) -> Self {
        self.mode = mode;
        self
    }

    /// Attach a memory policy for enforcing in-flight aggregation limits.
    pub fn with_memory_policy(mut self, policy: Option<MemoryPolicy>) -> Self {
        self.memory_tracker = policy.map(MemoryTracker::new);
        self
    }

    /// Attach a shared group counter used by parallel Partial aggregation.
    pub fn with_shared_group_counter(mut self, counter: Option<Arc<AtomicUsize>>) -> Self {
        self.shared_group_counter = counter;
        self
    }

    fn build_hash_table(&mut self) -> Result<()> {
        let mut table: HashMap<GroupKeyBytes, AggregateGroup> = HashMap::new();
        let mut next_row_id = 0u64;

        while let Some(result) = self.input.next_row() {
            let row = result?;
            let (key_values, key_bytes) = match self.mode {
                AggregateMode::Final => {
                    let key_values = row
                        .values
                        .get(..self.group_keys.len())
                        .ok_or_else(|| {
                            invalid_aggregate_state(
                                "aggregate",
                                "partial state row is missing group key values",
                            )
                        })?
                        .to_vec();
                    let key_bytes = encode_group_key(&key_values)?;
                    (key_values, key_bytes)
                }
                AggregateMode::Partial | AggregateMode::Single => {
                    let ctx = EvalContext::new(&row.values);
                    let mut key_values = Vec::with_capacity(self.group_keys.len());
                    for expr in &self.group_keys {
                        key_values.push(crate::executor::evaluator::evaluate(expr, &ctx)?);
                    }
                    let key_bytes = encode_group_key(&key_values)?;
                    (key_values, key_bytes)
                }
            };

            // Grouping-sets mode accumulates every row once per set under a
            // set-id-prefixed key (issue #149, D3/D7): the prefix keeps
            // duplicate sets and per-set real-NULL groups distinct while the
            // masked key values become the NULL placeholder outputs.
            let variants: Vec<(Vec<SqlValue>, GroupKeyBytes, Option<i64>)> =
                if let Some(sets) = &self.grouping_sets {
                    let key_count = self.group_keys.len();
                    let mut variants = Vec::with_capacity(sets.len());
                    for (set_id, mask) in sets.iter().enumerate() {
                        let mut masked = key_values.clone();
                        for (position, value) in masked.iter_mut().enumerate() {
                            if (mask >> (key_count - 1 - position)) & 1 == 1 {
                                *value = SqlValue::Null;
                            }
                        }
                        let mut bytes = Vec::with_capacity(4 + key_bytes.len());
                        bytes.extend_from_slice(&(set_id as u32).to_le_bytes());
                        bytes.extend_from_slice(&encode_group_key(&masked)?);
                        variants.push((masked, bytes, Some(*mask as i64)));
                    }
                    variants
                } else {
                    vec![(key_values, key_bytes, None)]
                };

            for (key_values, key_bytes, grouping_id) in variants {
                if !table.contains_key(&key_bytes) {
                    self.reserve_group_slot(table.len())?;
                    if let Some(tracker) = &mut self.memory_tracker {
                        tracker
                            .add_values(&key_values)
                            .map_err(map_core_memory_error)?;
                        tracker
                            .add_bytes(
                                self.aggregates.len() as u64 * AGGREGATE_ACCUMULATOR_OVERHEAD_BYTES,
                            )
                            .map_err(map_core_memory_error)?;
                    }
                    let accumulators = self
                        .aggregates
                        .iter()
                        .map(|agg| {
                            let mut aggregate = agg.clone();
                            aggregate.distinct =
                                matches!(self.mode, AggregateMode::Single) && agg.distinct;
                            create_accumulator_for_aggregate(&aggregate)
                        })
                        .collect::<Vec<_>>();
                    table.insert(
                        key_bytes.clone(),
                        AggregateGroup {
                            key_values: key_values.clone(),
                            accumulators,
                            grouping_id,
                        },
                    );
                }

                if let Some(group) = table.get_mut(&key_bytes) {
                    match self.mode {
                        AggregateMode::Final => {
                            let mut offset = self.group_keys.len();
                            for (idx, agg) in self.aggregates.iter().enumerate() {
                                let arity = aggregate_state_types(agg).len();
                                let state = row.values.get(offset..offset + arity).ok_or_else(|| {
                                invalid_aggregate_state(
                                    "aggregate",
                                    format!(
                                        "partial state row is missing state values for aggregate {idx}"
                                    ),
                                )
                            })?;
                                group.accumulators[idx].merge(state)?;
                                offset += arity;
                            }
                            if offset != row.values.len() {
                                return Err(invalid_aggregate_state(
                                    "aggregate",
                                    format!(
                                        "partial state row has {} trailing value(s)",
                                        row.values.len() - offset
                                    ),
                                ));
                            }
                        }
                        AggregateMode::Partial | AggregateMode::Single => {
                            let ctx = EvalContext::new(&row.values);
                            for (idx, agg) in self.aggregates.iter().enumerate() {
                                // FILTER applies per input row before the
                                // accumulator (and before DISTINCT), in Partial
                                // and Single mode alike (issue #148, D1).
                                if !aggregate_filter_accepts(agg, &ctx)? {
                                    continue;
                                }
                                let value = match &agg.arg {
                                    None => None,
                                    Some(expr) => {
                                        Some(crate::executor::evaluator::evaluate(expr, &ctx)?)
                                    }
                                };
                                if let Some(tracker) = &mut self.memory_tracker
                                    && matches!(
                                        agg.function,
                                        AggregateFunction::GroupConcat { .. }
                                            | AggregateFunction::StringAgg { .. }
                                            | AggregateFunction::PercentileDisc { .. }
                                    )
                                    && let Some(value_ref) = value.as_ref()
                                {
                                    tracker
                                        .add_value(value_ref)
                                        .map_err(map_core_memory_error)?;
                                }
                                if agg.order_by.is_empty() {
                                    group.accumulators[idx].update(value)?;
                                } else {
                                    let keys = evaluate_sort_keys(agg, &ctx)?;
                                    group.accumulators[idx].update_ordered(value, &keys)?;
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(sets) = self.grouping_sets.clone() {
            // Sets that group over no key (their mask covers every key)
            // still emit exactly one row when the input is empty, matching
            // the global-aggregation contract below.
            let key_count = self.group_keys.len();
            let full_mask = if key_count == 0 {
                0
            } else {
                (1u64 << key_count) - 1
            };
            if table.is_empty() {
                for (set_id, mask) in sets.iter().enumerate() {
                    if *mask != full_mask {
                        continue;
                    }
                    if let Some(tracker) = &mut self.memory_tracker {
                        tracker
                            .add_bytes(
                                self.aggregates.len() as u64 * AGGREGATE_ACCUMULATOR_OVERHEAD_BYTES,
                            )
                            .map_err(map_core_memory_error)?;
                    }
                    let accumulators = self
                        .aggregates
                        .iter()
                        .map(|agg| {
                            let mut aggregate = agg.clone();
                            aggregate.distinct =
                                matches!(self.mode, AggregateMode::Single) && agg.distinct;
                            create_accumulator_for_aggregate(&aggregate)
                        })
                        .collect::<Vec<_>>();
                    let key_values = vec![SqlValue::Null; key_count];
                    let mut bytes = Vec::with_capacity(4);
                    bytes.extend_from_slice(&(set_id as u32).to_le_bytes());
                    bytes.extend_from_slice(&encode_group_key(&key_values)?);
                    table.insert(
                        bytes,
                        AggregateGroup {
                            key_values,
                            accumulators,
                            grouping_id: Some(*mask as i64),
                        },
                    );
                }
            }
        } else if table.is_empty() && self.group_keys.is_empty() {
            if let Some(tracker) = &mut self.memory_tracker {
                tracker
                    .add_bytes(self.aggregates.len() as u64 * AGGREGATE_ACCUMULATOR_OVERHEAD_BYTES)
                    .map_err(map_core_memory_error)?;
            }
            let accumulators = self
                .aggregates
                .iter()
                .map(|agg| {
                    let mut aggregate = agg.clone();
                    aggregate.distinct = matches!(self.mode, AggregateMode::Single) && agg.distinct;
                    create_accumulator_for_aggregate(&aggregate)
                })
                .collect::<Vec<_>>();
            table.insert(
                Vec::new(),
                AggregateGroup {
                    key_values: Vec::new(),
                    accumulators,
                    grouping_id: None,
                },
            );
        }

        let mut rows = Vec::with_capacity(table.len());
        for group in table.values() {
            let mut values = Vec::with_capacity(self.group_keys.len() + self.aggregates.len() + 1);
            values.extend(group.key_values.iter().cloned());
            for acc in &group.accumulators {
                match self.mode {
                    AggregateMode::Partial => values.extend(acc.state()?),
                    AggregateMode::Final | AggregateMode::Single => values.push(acc.finalize()?),
                }
            }
            // The hidden __grouping_id column joins the row before HAVING so
            // GROUPING() predicates evaluate against it (issue #149, D11).
            if let Some(grouping_id) = group.grouping_id {
                values.push(SqlValue::BigInt(grouping_id));
            }
            let row = Row::new(next_row_id, values);
            next_row_id += 1;
            if let Some(tracker) = &mut self.memory_tracker {
                tracker
                    .add_row(&row.values)
                    .map_err(map_core_memory_error)?;
            }

            if self.mode != AggregateMode::Partial
                && let Some(having) = &self.having
            {
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

    fn reserve_group_slot(&self, local_group_count: usize) -> Result<()> {
        let next_count = if let Some(counter) = &self.shared_group_counter {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
        } else {
            local_group_count + 1
        };
        if next_count > self.group_limit {
            return Err(ExecutorError::ResourceExhausted {
                message: format!(
                    "GROUP BY result exceeds memory limit (max groups: {})",
                    self.group_limit
                ),
            });
        }
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
            .map(create_accumulator_for_aggregate)
            .collect()
    }

    fn update_accumulators(&mut self, ctx: &EvalContext<'_>) -> Result<()> {
        for (idx, agg) in self.aggregates.iter().enumerate() {
            if !aggregate_filter_accepts(agg, ctx)? {
                continue;
            }
            let value = match &agg.arg {
                None => None,
                Some(expr) => Some(crate::executor::evaluator::evaluate(expr, ctx)?),
            };
            if agg.order_by.is_empty() {
                self.accumulators[idx].update(value)?;
            } else {
                let keys = evaluate_sort_keys(agg, ctx)?;
                self.accumulators[idx].update_ordered(value, &keys)?;
            }
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

fn aggregate_state_types(agg: &AggregateExpr) -> Vec<ResolvedType> {
    match &agg.function {
        AggregateFunction::Count => vec![ResolvedType::BigInt],
        AggregateFunction::Sum => vec![agg.result_type.clone()],
        AggregateFunction::Total => vec![ResolvedType::Double],
        AggregateFunction::Avg => vec![ResolvedType::Double, ResolvedType::BigInt],
        AggregateFunction::Min | AggregateFunction::Max => vec![agg.result_type.clone()],
        AggregateFunction::GroupConcat { .. } | AggregateFunction::StringAgg { .. } => {
            vec![ResolvedType::Text, ResolvedType::Text]
        }
        // Ordered-set aggregation never runs in Partial mode (D11); the
        // accumulator rejects state()/merge() with invalid_aggregate_state.
        AggregateFunction::PercentileDisc { .. } => vec![agg.result_type.clone()],
    }
}

/// Evaluate a FILTER (WHERE ...) predicate for one input row. Only TRUE
/// admits the row; FALSE and NULL (UNKNOWN) skip it (issue #148, D1).
fn aggregate_filter_accepts(agg: &AggregateExpr, ctx: &EvalContext<'_>) -> Result<bool> {
    let Some(filter) = &agg.filter else {
        return Ok(true);
    };
    match crate::executor::evaluator::evaluate(filter, ctx)? {
        SqlValue::Boolean(true) => Ok(true),
        SqlValue::Boolean(false) | SqlValue::Null => Ok(false),
        other => Err(ExecutorError::Evaluation(
            crate::executor::EvaluationError::TypeMismatch {
                expected: "Boolean".into(),
                actual: other.type_name().into(),
            },
        )),
    }
}

/// Evaluate the aggregate-local sort keys of one input row.
fn evaluate_sort_keys(agg: &AggregateExpr, ctx: &EvalContext<'_>) -> Result<Vec<SqlValue>> {
    agg.order_by
        .iter()
        .map(|sort| crate::executor::evaluator::evaluate(&sort.expr, ctx))
        .collect()
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
            AggregateFunction::PercentileDisc { .. } => format!("percentile_disc_{idx}"),
        };
        schema.push(ColumnMetadata::new(name, agg.result_type.clone()));
    }
    schema
}

/// Build internal partial aggregate schema.
pub fn build_partial_aggregate_schema(
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
    for (agg_idx, agg) in aggregates.iter().enumerate() {
        for (state_idx, state_type) in aggregate_state_types(agg).into_iter().enumerate() {
            schema.push(ColumnMetadata::new(
                format!("__agg{agg_idx}_state{state_idx}"),
                state_type,
            ));
        }
    }
    schema
}

/// Return true when aggregate execution must remain Single for correctness.
/// FILTER is applied per input row and therefore commutes with Partial/Final
/// splitting; ordered aggregates (aggregate-local ORDER BY and ordered-set
/// aggregates such as PERCENTILE_DISC) buffer whole groups and stay Single
/// (issue #148, D11).
pub fn should_use_single_for_parallel(parallelism: usize, aggregates: &[AggregateExpr]) -> bool {
    parallelism <= 1
        || aggregates.iter().any(|agg| {
            agg.distinct
                || !agg.order_by.is_empty()
                || matches!(agg.function, AggregateFunction::PercentileDisc { .. })
        })
}

fn collect_iterator_rows(iter: &mut dyn RowIterator) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    while let Some(result) = iter.next_row() {
        rows.push(result?);
    }
    Ok(rows)
}

struct ChainRowIterator<'a> {
    prefix: std::vec::IntoIter<Row>,
    tail: Box<dyn RowIterator + 'a>,
    schema: Vec<ColumnMetadata>,
}

impl<'a> ChainRowIterator<'a> {
    fn new(prefix: Vec<Row>, tail: Box<dyn RowIterator + 'a>, schema: Vec<ColumnMetadata>) -> Self {
        Self {
            prefix: prefix.into_iter(),
            tail,
            schema,
        }
    }
}

impl RowIterator for ChainRowIterator<'_> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        if let Some(row) = self.prefix.next() {
            return Some(Ok(row));
        }
        self.tail.next_row()
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

fn estimate_row_bytes(row: &Row) -> u64 {
    row.values.iter().map(ByteSized::estimated_bytes).sum()
}

fn split_contiguous_partitions(rows: Vec<Row>, parallelism: usize) -> Vec<Vec<Row>> {
    let requested = parallelism.max(1);
    if rows.is_empty() {
        return (0..requested).map(|_| Vec::new()).collect();
    }
    let partitions = requested.min(rows.len());
    let total = rows.len();
    let mut tail = rows;
    let mut output = Vec::with_capacity(partitions);
    for partition in (0..partitions).rev() {
        let start = partition * total / partitions;
        output.push(tail.split_off(start));
    }
    output.reverse();
    output
}

#[allow(clippy::too_many_arguments)]
fn execute_partial_partition(
    partition_index: usize,
    rows: Vec<Row>,
    input_schema: Vec<ColumnMetadata>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    partial_schema: Vec<ColumnMetadata>,
    group_limit: usize,
    shared_group_counter: Arc<AtomicUsize>,
    shared_memory_counter: Arc<AtomicU64>,
    memory_limit: Option<u64>,
) -> Result<(usize, Vec<Row>)> {
    let input = VecIterator::new(rows, input_schema);
    let mut iter = AggregateIterator::new(
        Box::new(input),
        group_keys,
        aggregates,
        None,
        partial_schema,
    )
    .with_mode(AggregateMode::Partial)
    .with_group_limit(group_limit)
    .with_shared_group_counter(Some(shared_group_counter));
    let rows = collect_iterator_rows(&mut iter)?;
    for row in &rows {
        let used = shared_memory_counter
            .fetch_add(estimate_row_bytes(row), std::sync::atomic::Ordering::SeqCst)
            .saturating_add(estimate_row_bytes(row));
        if let Some(limit) = memory_limit
            && used > limit
        {
            return Err(ExecutorError::ResourceExhausted {
                message: format!(
                    "parallel aggregate memory limit exceeded: {used} bytes (limit {limit})"
                ),
            });
        }
    }
    Ok((partition_index, rows))
}

fn recv_partition_results(
    receiver: std::sync::mpsc::Receiver<Result<(usize, Vec<Row>)>>,
    expected: usize,
) -> Result<Vec<(usize, Vec<Row>)>> {
    let mut outputs = Vec::with_capacity(expected);
    for _ in 0..expected {
        let result = receiver
            .recv()
            .map_err(|err| ExecutorError::InvalidOperation {
                operation: "parallel aggregate".into(),
                reason: format!("partition worker failed to report result: {err}"),
            })?;
        outputs.push(result?);
    }
    outputs.sort_by_key(|(idx, _)| *idx);
    Ok(outputs)
}

#[cfg(feature = "tokio")]
#[allow(clippy::too_many_arguments)]
fn run_partial_partitions(
    partitions: Vec<Vec<Row>>,
    input_schema: Vec<ColumnMetadata>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    partial_schema: Vec<ColumnMetadata>,
    group_limit: usize,
    shared_group_counter: Arc<AtomicUsize>,
    shared_memory_counter: Arc<AtomicU64>,
    memory_limit: Option<u64>,
) -> Result<Vec<(usize, Vec<Row>)>> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let expected = partitions.len();
        let (sender, receiver) = std::sync::mpsc::channel();
        for (partition_index, rows) in partitions.into_iter().enumerate() {
            let sender = sender.clone();
            let input_schema = input_schema.clone();
            let group_keys = group_keys.clone();
            let aggregates = aggregates.clone();
            let partial_schema = partial_schema.clone();
            let shared_group_counter = Arc::clone(&shared_group_counter);
            let shared_memory_counter = Arc::clone(&shared_memory_counter);
            handle.spawn_blocking(move || {
                let result = execute_partial_partition(
                    partition_index,
                    rows,
                    input_schema,
                    group_keys,
                    aggregates,
                    partial_schema,
                    group_limit,
                    shared_group_counter,
                    shared_memory_counter,
                    memory_limit,
                );
                let _ = sender.send(result);
            });
        }
        drop(sender);
        return recv_partition_results(receiver, expected);
    }

    run_partial_partitions_on_threads(
        partitions,
        input_schema,
        group_keys,
        aggregates,
        partial_schema,
        group_limit,
        shared_group_counter,
        shared_memory_counter,
        memory_limit,
    )
}

#[cfg(not(feature = "tokio"))]
#[allow(clippy::too_many_arguments)]
fn run_partial_partitions(
    partitions: Vec<Vec<Row>>,
    input_schema: Vec<ColumnMetadata>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    partial_schema: Vec<ColumnMetadata>,
    group_limit: usize,
    shared_group_counter: Arc<AtomicUsize>,
    shared_memory_counter: Arc<AtomicU64>,
    memory_limit: Option<u64>,
) -> Result<Vec<(usize, Vec<Row>)>> {
    run_partial_partitions_on_threads(
        partitions,
        input_schema,
        group_keys,
        aggregates,
        partial_schema,
        group_limit,
        shared_group_counter,
        shared_memory_counter,
        memory_limit,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_partial_partitions_on_threads(
    partitions: Vec<Vec<Row>>,
    input_schema: Vec<ColumnMetadata>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    partial_schema: Vec<ColumnMetadata>,
    group_limit: usize,
    shared_group_counter: Arc<AtomicUsize>,
    shared_memory_counter: Arc<AtomicU64>,
    memory_limit: Option<u64>,
) -> Result<Vec<(usize, Vec<Row>)>> {
    let expected = partitions.len();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for (partition_index, rows) in partitions.into_iter().enumerate() {
            let sender = sender.clone();
            let input_schema = input_schema.clone();
            let group_keys = group_keys.clone();
            let aggregates = aggregates.clone();
            let partial_schema = partial_schema.clone();
            let shared_group_counter = Arc::clone(&shared_group_counter);
            let shared_memory_counter = Arc::clone(&shared_memory_counter);
            scope.spawn(move || {
                let result = execute_partial_partition(
                    partition_index,
                    rows,
                    input_schema,
                    group_keys,
                    aggregates,
                    partial_schema,
                    group_limit,
                    shared_group_counter,
                    shared_memory_counter,
                    memory_limit,
                );
                let _ = sender.send(result);
            });
        }
    });
    drop(sender);
    recv_partition_results(receiver, expected)
}

/// Execute a deterministic single-process parallel partial-to-final aggregate.
pub fn execute_parallel_aggregate_rows<'a>(
    input: Box<dyn RowIterator + 'a>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<TypedExpr>,
    final_schema: Vec<ColumnMetadata>,
    parallelism: usize,
) -> Result<Vec<Row>> {
    execute_parallel_aggregate_rows_with_policy(
        input,
        group_keys,
        aggregates,
        having,
        final_schema,
        parallelism,
        None,
        DEFAULT_GROUP_LIMIT,
    )
}

/// Execute a deterministic parallel aggregate with memory fallback controls.
#[allow(clippy::too_many_arguments)]
pub fn execute_parallel_aggregate_rows_with_policy<'a>(
    mut input: Box<dyn RowIterator + 'a>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<TypedExpr>,
    final_schema: Vec<ColumnMetadata>,
    parallelism: usize,
    memory: Option<MemoryPolicy>,
    group_limit: usize,
) -> Result<Vec<Row>> {
    if parallelism <= 1 {
        return execute_single_aggregate_rows(
            input,
            group_keys,
            aggregates,
            having,
            final_schema,
            memory,
            group_limit,
        );
    }

    let input_schema = input.schema().to_vec();
    let mut input_rows = Vec::new();
    let mut materialized_bytes = 0u64;
    let materialize_threshold = memory
        .as_ref()
        .and_then(MemoryPolicy::limit_bytes)
        .map(|limit| (limit / 2).max(1));

    while let Some(result) = input.next_row() {
        let row = result?;
        let row_bytes = materialize_threshold.map(|_| estimate_row_bytes(&row));
        if let (Some(threshold), Some(row_bytes)) = (materialize_threshold, row_bytes)
            && materialized_bytes.saturating_add(row_bytes) > threshold
        {
            input_rows.push(row);
            let chained = ChainRowIterator::new(input_rows, input, input_schema.clone());
            return execute_single_aggregate_rows(
                Box::new(chained),
                group_keys,
                aggregates,
                having,
                final_schema,
                memory,
                group_limit,
            );
        }
        if let Some(row_bytes) = row_bytes {
            materialized_bytes = materialized_bytes.saturating_add(row_bytes);
        }
        input_rows.push(row);
    }

    let fallback_rows = if memory.is_some() || group_limit < input_rows.len() {
        Some(input_rows.clone())
    } else {
        None
    };
    let result = execute_parallel_aggregate_rows_from_materialized(
        input_rows,
        input_schema.clone(),
        group_keys.clone(),
        aggregates.clone(),
        having.clone(),
        final_schema.clone(),
        parallelism,
        group_limit,
        materialized_bytes,
        memory.as_ref().and_then(MemoryPolicy::limit_bytes),
    );
    match result {
        Ok(rows) => Ok(rows),
        Err(ExecutorError::ResourceExhausted { .. }) => {
            if let Some(fallback_rows) = fallback_rows {
                execute_single_aggregate_rows(
                    Box::new(VecIterator::new(fallback_rows, input_schema)),
                    group_keys,
                    aggregates,
                    having,
                    final_schema,
                    memory,
                    group_limit,
                )
            } else {
                Err(ExecutorError::ResourceExhausted {
                    message: format!(
                        "parallel aggregate exceeded group limit {group_limit}; no fallback rows retained"
                    ),
                })
            }
        }
        Err(err) => Err(err),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_parallel_aggregate_rows_from_materialized(
    input_rows: Vec<Row>,
    input_schema: Vec<ColumnMetadata>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<TypedExpr>,
    final_schema: Vec<ColumnMetadata>,
    parallelism: usize,
    group_limit: usize,
    materialized_bytes: u64,
    memory_limit: Option<u64>,
) -> Result<Vec<Row>> {
    let partial_schema = build_partial_aggregate_schema(&group_keys, &aggregates);
    let partitions = split_contiguous_partitions(input_rows, parallelism);
    let shared_group_counter = Arc::new(AtomicUsize::new(0));
    let shared_memory_counter = Arc::new(AtomicU64::new(materialized_bytes));
    let partial_results = run_partial_partitions(
        partitions,
        input_schema,
        group_keys.clone(),
        aggregates.clone(),
        partial_schema.clone(),
        group_limit,
        shared_group_counter,
        shared_memory_counter,
        memory_limit,
    )?;

    let partial_rows = partial_results
        .into_iter()
        .flat_map(|(_, rows)| rows)
        .collect::<Vec<_>>();
    let final_input = VecIterator::new(partial_rows, partial_schema);
    let mut final_iter = AggregateIterator::new(
        Box::new(final_input),
        group_keys,
        aggregates,
        having,
        final_schema,
    )
    .with_mode(AggregateMode::Final)
    .with_group_limit(group_limit);
    collect_iterator_rows(&mut final_iter)
}

fn execute_single_aggregate_rows<'a>(
    input: Box<dyn RowIterator + 'a>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<TypedExpr>,
    final_schema: Vec<ColumnMetadata>,
    memory: Option<MemoryPolicy>,
    group_limit: usize,
) -> Result<Vec<Row>> {
    let mut iter = AggregateIterator::new(input, group_keys, aggregates, having, final_schema)
        .with_group_limit(group_limit)
        .with_memory_policy(memory);
    collect_iterator_rows(&mut iter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::span::Span;
    use crate::executor::memory::SpillPolicy;
    use crate::planner::typed_expr::TypedExprKind;

    fn apply_values(acc: &mut dyn Accumulator, values: &[Option<SqlValue>]) {
        for value in values {
            acc.update(value.clone()).unwrap();
        }
    }

    fn single_result(
        make_accumulator: impl Fn() -> Box<dyn Accumulator>,
        partitions: &[Vec<Option<SqlValue>>],
    ) -> SqlValue {
        let mut acc = make_accumulator();
        for partition in partitions {
            apply_values(acc.as_mut(), partition);
        }
        acc.finalize().unwrap()
    }

    fn merged_result(
        make_partial: impl Fn() -> Box<dyn Accumulator>,
        make_final: impl Fn() -> Box<dyn Accumulator>,
        partitions: &[Vec<Option<SqlValue>>],
        merge_order: &[usize],
    ) -> SqlValue {
        let states = partitions
            .iter()
            .map(|partition| {
                let mut acc = make_partial();
                apply_values(acc.as_mut(), partition);
                acc.state().unwrap()
            })
            .collect::<Vec<_>>();

        let mut final_acc = make_final();
        for idx in merge_order {
            final_acc.merge(&states[*idx]).unwrap();
        }
        final_acc.finalize().unwrap()
    }

    fn assert_single_equals_merged(
        make_accumulator: impl Fn() -> Box<dyn Accumulator> + Copy,
        partitions: Vec<Vec<Option<SqlValue>>>,
    ) {
        let merge_order = (0..partitions.len()).collect::<Vec<_>>();
        let single = single_result(make_accumulator, &partitions);
        let merged = merged_result(
            make_accumulator,
            make_accumulator,
            &partitions,
            &merge_order,
        );
        assert_eq!(single, merged);
    }

    fn assert_merge_order_invariant(
        make_accumulator: impl Fn() -> Box<dyn Accumulator> + Copy,
        partitions: Vec<Vec<Option<SqlValue>>>,
        merge_orders: &[Vec<usize>],
    ) {
        let single = single_result(make_accumulator, &partitions);
        for order in merge_orders {
            let merged = merged_result(make_accumulator, make_accumulator, &partitions, order);
            assert_eq!(single, merged, "merge order {order:?}");
        }
    }

    fn column_expr(index: usize, name: &str, resolved_type: ResolvedType) -> TypedExpr {
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "t".into(),
                column: name.into(),
                column_index: index,
            },
            resolved_type,
            span: Span::default(),
        }
    }

    fn sample_aggregate_schema() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata::new("category", ResolvedType::Text),
            ColumnMetadata::new("price", ResolvedType::Double),
            ColumnMetadata::new("label", ResolvedType::Text),
        ]
    }

    fn sample_aggregate_rows() -> Vec<Row> {
        vec![
            Row::new(
                0,
                vec![
                    SqlValue::Text("book".into()),
                    SqlValue::Double(10.0),
                    SqlValue::Text("a".into()),
                ],
            ),
            Row::new(
                1,
                vec![
                    SqlValue::Text("book".into()),
                    SqlValue::Double(15.0),
                    SqlValue::Text("b".into()),
                ],
            ),
            Row::new(
                2,
                vec![
                    SqlValue::Text("game".into()),
                    SqlValue::Double(20.0),
                    SqlValue::Text("c".into()),
                ],
            ),
            Row::new(
                3,
                vec![
                    SqlValue::Text("book".into()),
                    SqlValue::Null,
                    SqlValue::Text("a".into()),
                ],
            ),
            Row::new(
                4,
                vec![
                    SqlValue::Text("toy".into()),
                    SqlValue::Double(3.0),
                    SqlValue::Text("d".into()),
                ],
            ),
        ]
    }

    fn sample_aggregates() -> Vec<AggregateExpr> {
        let price = column_expr(1, "price", ResolvedType::Double);
        let label = column_expr(2, "label", ResolvedType::Text);
        vec![
            AggregateExpr::count_star(),
            AggregateExpr::sum(price.clone()),
            AggregateExpr::avg(price),
            AggregateExpr {
                function: AggregateFunction::GroupConcat {
                    separator: Some("|".into()),
                },
                arg: Some(label),
                distinct: false,
                result_type: ResolvedType::Text,
                filter: None,
                order_by: Vec::new(),
            },
        ]
    }

    fn collect_single_aggregate(
        group_keys: Vec<TypedExpr>,
        aggregates: Vec<AggregateExpr>,
    ) -> Vec<Vec<SqlValue>> {
        let input = VecIterator::new(sample_aggregate_rows(), sample_aggregate_schema());
        let schema = build_aggregate_schema(&group_keys, &aggregates);
        let mut iter =
            AggregateIterator::new(Box::new(input), group_keys, aggregates, None, schema);
        collect_iterator_rows(&mut iter)
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect()
    }

    fn collect_parallel_aggregate(
        group_keys: Vec<TypedExpr>,
        aggregates: Vec<AggregateExpr>,
        parallelism: usize,
    ) -> Vec<Vec<SqlValue>> {
        let input = VecIterator::new(sample_aggregate_rows(), sample_aggregate_schema());
        let schema = build_aggregate_schema(&group_keys, &aggregates);
        execute_parallel_aggregate_rows(
            Box::new(input),
            group_keys,
            aggregates,
            None,
            schema,
            parallelism,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.values)
        .collect()
    }

    fn sort_rows(mut rows: Vec<Vec<SqlValue>>) -> Vec<Vec<SqlValue>> {
        rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        rows
    }

    #[test]
    fn grouping_sets_accumulate_each_row_once_per_set() {
        let category = column_expr(0, "category", ResolvedType::Text);
        let group_keys = vec![category];
        let aggregates = vec![AggregateExpr::count_star()];
        let input = VecIterator::new(sample_aggregate_rows(), sample_aggregate_schema());
        let mut schema = build_aggregate_schema(&group_keys, &aggregates);
        schema.push(ColumnMetadata::new("__grouping_id", ResolvedType::BigInt));
        // Mask 0b0 keeps the category key; mask 0b1 is the grand total.
        let mut iter =
            AggregateIterator::new(Box::new(input), group_keys, aggregates, None, schema)
                .with_grouping_sets(Some(vec![0b0, 0b1]));
        let rows = sort_rows(
            collect_iterator_rows(&mut iter)
                .unwrap()
                .into_iter()
                .map(|row| row.values)
                .collect(),
        );

        assert_eq!(
            rows,
            sort_rows(vec![
                vec![
                    SqlValue::Text("book".into()),
                    SqlValue::BigInt(3),
                    SqlValue::BigInt(0),
                ],
                vec![
                    SqlValue::Text("game".into()),
                    SqlValue::BigInt(1),
                    SqlValue::BigInt(0),
                ],
                vec![
                    SqlValue::Text("toy".into()),
                    SqlValue::BigInt(1),
                    SqlValue::BigInt(0),
                ],
                vec![SqlValue::Null, SqlValue::BigInt(5), SqlValue::BigInt(1)],
            ])
        );
    }

    #[test]
    fn grouping_sets_group_limit_applies_across_all_sets() {
        let category = column_expr(0, "category", ResolvedType::Text);
        let group_keys = vec![category];
        let aggregates = vec![AggregateExpr::count_star()];
        let input = VecIterator::new(sample_aggregate_rows(), sample_aggregate_schema());
        let mut schema = build_aggregate_schema(&group_keys, &aggregates);
        schema.push(ColumnMetadata::new("__grouping_id", ResolvedType::BigInt));
        // Three category groups plus the grand total = 4 groups; a limit of
        // 3 must fail even though each single set stays within the limit
        // (issue #149, D6).
        let mut iter =
            AggregateIterator::new(Box::new(input), group_keys, aggregates, None, schema)
                .with_grouping_sets(Some(vec![0b0, 0b1]))
                .with_group_limit(3);
        let error = collect_iterator_rows(&mut iter).unwrap_err();
        assert!(matches!(error, ExecutorError::ResourceExhausted { .. }));
    }

    #[test]
    fn partial_schema_uses_group_keys_and_state_columns() {
        let category = column_expr(0, "category", ResolvedType::Text);
        let price = column_expr(1, "price", ResolvedType::Double);
        let aggregates = vec![AggregateExpr::count_star(), AggregateExpr::avg(price)];

        let schema = build_partial_aggregate_schema(&[category], &aggregates);
        let names = schema
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "category",
                "__agg0_state0",
                "__agg1_state0",
                "__agg1_state1"
            ]
        );
        assert_eq!(schema[1].data_type, ResolvedType::BigInt);
        assert_eq!(schema[2].data_type, ResolvedType::Double);
        assert_eq!(schema[3].data_type, ResolvedType::BigInt);
    }

    #[test]
    fn parallel_aggregate_matches_single_with_group_by() {
        let group_keys = vec![column_expr(0, "category", ResolvedType::Text)];
        let aggregates = sample_aggregates();

        let single = sort_rows(collect_single_aggregate(
            group_keys.clone(),
            aggregates.clone(),
        ));
        let parallel = sort_rows(collect_parallel_aggregate(group_keys, aggregates, 3));

        assert_eq!(parallel, single);
    }

    #[test]
    fn parallel_aggregate_matches_single_without_group_by() {
        let aggregates = sample_aggregates();

        let single = collect_single_aggregate(Vec::new(), aggregates.clone());
        let parallel = collect_parallel_aggregate(Vec::new(), aggregates, 4);

        assert_eq!(parallel, single);
    }

    #[test]
    fn distinct_aggregates_force_single_parallel_mode() {
        let price = column_expr(1, "price", ResolvedType::Double);
        let aggregates = vec![AggregateExpr {
            distinct: true,
            ..AggregateExpr::sum(price)
        }];

        assert!(should_use_single_for_parallel(4, &aggregates));
        assert!(should_use_single_for_parallel(1, &sample_aggregates()));
        assert!(!should_use_single_for_parallel(2, &sample_aggregates()));
    }

    #[test]
    fn parallel_group_counter_exhaustion_falls_back_to_single() {
        let schema = vec![ColumnMetadata::new("category", ResolvedType::Text)];
        let rows = vec![
            Row::new(0, vec![SqlValue::Text("a".into())]),
            Row::new(1, vec![SqlValue::Text("b".into())]),
            Row::new(2, vec![SqlValue::Text("a".into())]),
            Row::new(3, vec![SqlValue::Text("b".into())]),
        ];
        let group_keys = vec![column_expr(0, "category", ResolvedType::Text)];
        let aggregates = vec![AggregateExpr::count_star()];
        let final_schema = build_aggregate_schema(&group_keys, &aggregates);

        let single_values = {
            let input = VecIterator::new(rows.clone(), schema.clone());
            execute_single_aggregate_rows(
                Box::new(input),
                group_keys.clone(),
                aggregates.clone(),
                None,
                final_schema.clone(),
                None,
                2,
            )
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>()
        };

        let parallel_values = execute_parallel_aggregate_rows_with_policy(
            Box::new(VecIterator::new(rows, schema)),
            group_keys,
            aggregates,
            None,
            final_schema,
            2,
            None,
            2,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.values)
        .collect::<Vec<_>>();

        assert_eq!(sort_rows(parallel_values), sort_rows(single_values));
    }

    #[test]
    fn materialize_limit_exhaustion_falls_back_to_streaming_single() {
        let schema = vec![ColumnMetadata::new("payload", ResolvedType::Text)];
        let rows = (0..4)
            .map(|idx| Row::new(idx, vec![SqlValue::Text("x".repeat(40))]))
            .collect::<Vec<_>>();
        let aggregates = vec![AggregateExpr::count_star()];
        let final_schema = build_aggregate_schema(&[], &aggregates);
        let policy = MemoryPolicy::new(Some(100), SpillPolicy::FailFast);

        let result = execute_parallel_aggregate_rows_with_policy(
            Box::new(VecIterator::new(rows, schema)),
            Vec::new(),
            aggregates,
            None,
            final_schema,
            4,
            Some(policy),
            DEFAULT_GROUP_LIMIT,
        )
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values, vec![SqlValue::BigInt(4)]);
    }

    #[test]
    fn streaming_aggregate_respects_distinct_accumulators() {
        let schema = sample_aggregate_schema();
        let rows = vec![
            Row::new(
                0,
                vec![
                    SqlValue::Text("book".into()),
                    SqlValue::Double(10.0),
                    SqlValue::Text("a".into()),
                ],
            ),
            Row::new(
                1,
                vec![
                    SqlValue::Text("book".into()),
                    SqlValue::Double(10.0),
                    SqlValue::Text("a".into()),
                ],
            ),
            Row::new(
                2,
                vec![
                    SqlValue::Text("book".into()),
                    SqlValue::Double(15.0),
                    SqlValue::Text("b".into()),
                ],
            ),
        ];
        let group_keys = vec![column_expr(0, "category", ResolvedType::Text)];
        let price = column_expr(1, "price", ResolvedType::Double);
        let label = column_expr(2, "label", ResolvedType::Text);
        let aggregates = vec![
            AggregateExpr {
                distinct: true,
                ..AggregateExpr::sum(price)
            },
            AggregateExpr {
                function: AggregateFunction::GroupConcat {
                    separator: Some("|".into()),
                },
                arg: Some(label),
                distinct: true,
                result_type: ResolvedType::Text,
                filter: None,
                order_by: Vec::new(),
            },
        ];
        let output_schema = build_aggregate_schema(&group_keys, &aggregates);
        let input = VecIterator::new(rows, schema);
        let mut iter = StreamingAggregateIterator::new(
            Box::new(input),
            group_keys,
            aggregates,
            None,
            output_schema,
        );
        let rows = collect_iterator_rows(&mut iter).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[1], SqlValue::Double(25.0));
        assert_eq!(rows[0].values[2], SqlValue::Text("a|b".into()));
    }

    #[test]
    fn partial_state_matches_single_for_count_sum_total_avg_min_max() {
        assert_single_equals_merged(
            || Box::new(CountAccumulator::new(false)),
            vec![
                vec![
                    Some(SqlValue::Integer(1)),
                    Some(SqlValue::BigInt(2)),
                    Some(SqlValue::Text("x".into())),
                    Some(SqlValue::Null),
                ],
                vec![Some(SqlValue::Integer(3))],
            ],
        );
        assert_single_equals_merged(
            || Box::new(SumAccumulator::new()),
            vec![
                vec![
                    Some(SqlValue::Integer(1)),
                    Some(SqlValue::BigInt(2)),
                    Some(SqlValue::Float(3.5)),
                ],
                vec![Some(SqlValue::Double(4.5)), Some(SqlValue::Null)],
            ],
        );
        assert_single_equals_merged(
            || Box::new(TotalAccumulator::new()),
            vec![
                vec![Some(SqlValue::Integer(1)), Some(SqlValue::Null)],
                vec![Some(SqlValue::Double(2.5))],
            ],
        );
        assert_single_equals_merged(
            || Box::new(AvgAccumulator::new()),
            vec![
                vec![Some(SqlValue::Integer(2)), Some(SqlValue::Double(4.0))],
                vec![Some(SqlValue::Null), Some(SqlValue::Double(6.0))],
            ],
        );
        assert_single_equals_merged(
            || Box::new(MinMaxAccumulator::new(true)),
            vec![
                vec![Some(SqlValue::Integer(3)), Some(SqlValue::Integer(1))],
                vec![Some(SqlValue::Integer(2)), Some(SqlValue::Null)],
            ],
        );
        assert_single_equals_merged(
            || Box::new(MinMaxAccumulator::new(false)),
            vec![
                vec![
                    Some(SqlValue::Text("b".into())),
                    Some(SqlValue::Text("a".into())),
                ],
                vec![Some(SqlValue::Text("c".into())), Some(SqlValue::Null)],
            ],
        );
    }

    #[test]
    fn partial_state_matches_single_for_ordered_string_aggregates() {
        assert_single_equals_merged(
            || Box::new(GroupConcatAccumulator::new("|".into())),
            vec![
                vec![Some(SqlValue::Text("a".into())), Some(SqlValue::Null)],
                vec![
                    Some(SqlValue::Text("b".into())),
                    Some(SqlValue::Text("c".into())),
                ],
            ],
        );
        assert_single_equals_merged(
            || Box::new(StringAggAccumulator::new("::".into())),
            vec![
                vec![Some(SqlValue::Text("a".into()))],
                vec![Some(SqlValue::Text("b".into())), Some(SqlValue::Null)],
            ],
        );
    }

    #[test]
    fn partial_state_handles_empty_all_null_single_and_mixed_boundaries() {
        assert_single_equals_merged(
            || Box::new(CountAccumulator::new(false)),
            vec![vec![], vec![]],
        );
        assert_single_equals_merged(|| Box::new(SumAccumulator::new()), vec![vec![], vec![]]);
        assert_single_equals_merged(|| Box::new(TotalAccumulator::new()), vec![vec![], vec![]]);
        assert_single_equals_merged(|| Box::new(AvgAccumulator::new()), vec![vec![], vec![]]);
        assert_single_equals_merged(
            || Box::new(MinMaxAccumulator::new(true)),
            vec![vec![], vec![]],
        );
        assert_single_equals_merged(
            || Box::new(MinMaxAccumulator::new(false)),
            vec![vec![], vec![]],
        );
        assert_single_equals_merged(
            || Box::new(GroupConcatAccumulator::new(",".into())),
            vec![vec![], vec![]],
        );
        assert_single_equals_merged(
            || Box::new(StringAggAccumulator::new(",".into())),
            vec![vec![], vec![]],
        );
        assert_single_equals_merged(
            || Box::new(CountAccumulator::new(false)),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(SumAccumulator::new()),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(TotalAccumulator::new()),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(AvgAccumulator::new()),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(MinMaxAccumulator::new(true)),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(MinMaxAccumulator::new(false)),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(GroupConcatAccumulator::new(",".into())),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(StringAggAccumulator::new(",".into())),
            vec![vec![Some(SqlValue::Null)], vec![Some(SqlValue::Null)]],
        );
        assert_single_equals_merged(
            || Box::new(SumAccumulator::new()),
            vec![vec![Some(SqlValue::Integer(7))]],
        );
        assert_single_equals_merged(
            || Box::new(AvgAccumulator::new()),
            vec![
                vec![Some(SqlValue::Null)],
                vec![Some(SqlValue::Double(8.0))],
            ],
        );
    }

    #[test]
    fn commutative_accumulators_are_merge_order_invariant() {
        let orders = vec![
            vec![1, 3, 0, 2],
            vec![3, 2, 1, 0],
            vec![0, 1, 2, 3],
            vec![2, 0, 3, 1],
        ];
        assert_merge_order_invariant(
            || Box::new(CountAccumulator::new(false)),
            vec![vec![Some(SqlValue::Integer(1))]],
            &[vec![0]],
        );
        assert_merge_order_invariant(
            || Box::new(SumAccumulator::new()),
            vec![
                vec![Some(SqlValue::Integer(1))],
                vec![Some(SqlValue::Integer(2))],
            ],
            &[vec![0, 1], vec![1, 0]],
        );
        assert_merge_order_invariant(
            || Box::new(AvgAccumulator::new()),
            vec![
                vec![Some(SqlValue::Integer(1))],
                vec![Some(SqlValue::Integer(2))],
                vec![Some(SqlValue::Integer(3))],
            ],
            &[vec![0, 1, 2], vec![2, 1, 0]],
        );
        let numeric_partitions = vec![
            vec![Some(SqlValue::Integer(1)), Some(SqlValue::Null)],
            vec![Some(SqlValue::BigInt(2))],
            vec![],
            vec![Some(SqlValue::Double(3.0))],
        ];
        assert_merge_order_invariant(
            || Box::new(CountAccumulator::new(false)),
            numeric_partitions.clone(),
            &orders,
        );
        assert_merge_order_invariant(
            || Box::new(SumAccumulator::new()),
            numeric_partitions.clone(),
            &orders,
        );
        assert_merge_order_invariant(
            || Box::new(TotalAccumulator::new()),
            numeric_partitions.clone(),
            &orders,
        );
        assert_merge_order_invariant(
            || Box::new(AvgAccumulator::new()),
            numeric_partitions.clone(),
            &orders,
        );
        let integer_partitions = vec![
            vec![Some(SqlValue::Integer(3)), Some(SqlValue::Null)],
            vec![Some(SqlValue::Integer(1))],
            vec![],
            vec![Some(SqlValue::Integer(2))],
        ];
        assert_merge_order_invariant(
            || Box::new(MinMaxAccumulator::new(true)),
            integer_partitions.clone(),
            &orders,
        );
        assert_merge_order_invariant(
            || Box::new(MinMaxAccumulator::new(false)),
            integer_partitions,
            &orders,
        );
    }

    #[test]
    fn avg_partial_state_uses_sum_count_and_never_divides_by_zero_during_merge() {
        let empty = {
            let acc = AvgAccumulator::new();
            acc.state().unwrap()
        };
        assert_eq!(empty, vec![SqlValue::Double(0.0), SqlValue::BigInt(0)]);

        let mut partial = AvgAccumulator::new();
        partial.update(Some(SqlValue::Integer(2))).unwrap();
        partial.update(Some(SqlValue::Double(4.0))).unwrap();
        assert_eq!(
            partial.state().unwrap(),
            vec![SqlValue::Double(6.0), SqlValue::BigInt(2)]
        );

        let mut final_acc = AvgAccumulator::new();
        final_acc.merge(&empty).unwrap();
        assert_eq!(final_acc.finalize().unwrap(), SqlValue::Null);
        final_acc.merge(&partial.state().unwrap()).unwrap();
        assert_eq!(final_acc.finalize().unwrap(), SqlValue::Double(3.0));
    }

    #[test]
    fn merge_rejects_invalid_state_contracts_without_panicking() {
        let mut count = CountAccumulator::new(false);
        assert!(count.merge(&[]).is_err());
        assert!(count.merge(&[SqlValue::Text("bad".into())]).is_err());

        let mut avg = AvgAccumulator::new();
        assert!(avg.merge(&[SqlValue::Double(1.0)]).is_err());
        assert!(
            avg.merge(&[SqlValue::Double(1.0), SqlValue::Text("bad".into())])
                .is_err()
        );

        let mut concat = GroupConcatAccumulator::new("|".into());
        assert!(
            concat
                .merge(&[SqlValue::Text("a".into()), SqlValue::Text(",".into())])
                .is_err()
        );
    }

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
    fn count_distinct_uses_group_key_equality_boundaries() {
        let mut acc = CountAccumulator::new(true);
        let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan_b = f64::from_bits(0x7ff8_0000_0000_0002);
        for value in [
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Double(1.0),
            SqlValue::Double(-0.0),
            SqlValue::Double(0.0),
            SqlValue::Double(nan_a),
            SqlValue::Double(nan_a),
            SqlValue::Double(nan_b),
            SqlValue::Text("same".into()),
            SqlValue::Text("same".into()),
            SqlValue::Blob(vec![1, 2]),
            SqlValue::Blob(vec![1, 2]),
            SqlValue::Blob(vec![1, 3]),
        ] {
            acc.update(Some(value)).unwrap();
        }
        assert_eq!(acc.finalize().unwrap(), SqlValue::BigInt(9));
    }

    #[test]
    fn distinct_non_count_accumulators_deduplicate_non_null_values() {
        let mut sum = SumAccumulator::with_distinct(true);
        for value in [
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Double(1.0),
            SqlValue::Integer(2),
            SqlValue::Null,
        ] {
            sum.update(Some(value)).unwrap();
        }
        assert_eq!(sum.finalize().unwrap(), SqlValue::Double(4.0));

        let mut avg = AvgAccumulator::with_distinct(true);
        for value in [
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(3),
            SqlValue::Null,
        ] {
            avg.update(Some(value)).unwrap();
        }
        assert_eq!(avg.finalize().unwrap(), SqlValue::Double(2.0));

        let mut min = MinMaxAccumulator::with_distinct(true, true);
        let mut max = MinMaxAccumulator::with_distinct(false, true);
        for value in [
            SqlValue::Text("b".into()),
            SqlValue::Text("a".into()),
            SqlValue::Text("a".into()),
            SqlValue::Text("c".into()),
        ] {
            min.update(Some(value.clone())).unwrap();
            max.update(Some(value)).unwrap();
        }
        assert_eq!(min.finalize().unwrap(), SqlValue::Text("a".into()));
        assert_eq!(max.finalize().unwrap(), SqlValue::Text("c".into()));

        let mut group_concat = GroupConcatAccumulator::with_distinct("|".into(), true);
        let mut string_agg = StringAggAccumulator::with_distinct(";".into(), true);
        for value in [
            SqlValue::Text("a".into()),
            SqlValue::Text("a".into()),
            SqlValue::Null,
            SqlValue::Text("b".into()),
        ] {
            group_concat.update(Some(value.clone())).unwrap();
            string_agg.update(Some(value)).unwrap();
        }
        assert_eq!(
            group_concat.finalize().unwrap(),
            SqlValue::Text("a|b".into())
        );
        assert_eq!(string_agg.finalize().unwrap(), SqlValue::Text("a;b".into()));
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

    #[test]
    fn percentile_disc_accumulator_follows_postgres_selection_rule() {
        // values [1, 2, 2, 3]; index = max(ceil(f * n) - 1, 0)
        let values = [1i64, 2, 2, 3];
        for (fraction, expected) in [(0.0, 1i64), (0.25, 1), (0.5, 2), (0.75, 2), (1.0, 3)] {
            let mut acc = PercentileDiscAccumulator::new(fraction, vec![(true, false)]);
            for value in values {
                let value = SqlValue::Integer(value as i32);
                acc.update_ordered(Some(value.clone()), std::slice::from_ref(&value))
                    .unwrap();
            }
            // NULL sort values are excluded.
            acc.update_ordered(Some(SqlValue::Null), &[SqlValue::Null])
                .unwrap();
            assert_eq!(
                acc.finalize().unwrap(),
                SqlValue::Integer(expected as i32),
                "fraction {fraction}"
            );
        }

        let empty = PercentileDiscAccumulator::new(0.5, vec![(true, false)]);
        assert_eq!(empty.finalize().unwrap(), SqlValue::Null);

        let acc = PercentileDiscAccumulator::new(0.5, vec![(true, false)]);
        assert!(acc.state().is_err(), "ordered-set partial state is invalid");
    }

    #[test]
    fn ordered_string_accumulators_sort_stably_and_reject_partial_state() {
        let mut acc = StringAggAccumulator::with_order(",".into(), false, vec![(false, false)]);
        for (key, value) in [(2, "b"), (1, "a"), (3, "c"), (2, "d")] {
            acc.update_ordered(
                Some(SqlValue::Text(value.into())),
                &[SqlValue::Integer(key)],
            )
            .unwrap();
        }
        // DESC by key; the two key=2 entries keep arrival order (stable sort).
        assert_eq!(acc.finalize().unwrap(), SqlValue::Text("c,b,d,a".into()));
        assert!(acc.state().is_err());
        assert!(acc.merge(&[]).is_err());

        let mut concat = GroupConcatAccumulator::with_order("|".into(), true, vec![(true, false)]);
        for (key, value) in [(2, "x"), (1, "y"), (3, "x")] {
            concat
                .update_ordered(
                    Some(SqlValue::Text(value.into())),
                    &[SqlValue::Integer(key)],
                )
                .unwrap();
        }
        // DISTINCT drops the second "x"; ASC by key -> y,x.
        assert_eq!(concat.finalize().unwrap(), SqlValue::Text("y|x".into()));
    }
}
