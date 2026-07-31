use chrono::NaiveDateTime;

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::planner::types::ResolvedType;
use crate::storage::SqlValue;

use super::{EvalContext, evaluate};

/// Evaluate a typed coercion used by column assignment and INSERT planning.
pub(crate) fn evaluate_cast(
    expr: &TypedExpr,
    target_type: &ResolvedType,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let value = evaluate(expr, ctx)?;
    match target_type {
        ResolvedType::Timestamp => coerce_timestamp(value),
        other => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedExpression(format!("cast to {other}")),
        )),
    }
}

/// Coerce the documented TIMESTAMP input forms into epoch microseconds.
///
/// Alopex v0.8.2 accepts the space-separated UTC literal form with optional
/// fractional seconds, or a numeric value that is exactly an i64 microsecond
/// count. Time-zone suffixes and offsets are intentionally rejected because
/// the dialect has no time-zone type or conversion rules.
pub(crate) fn coerce_timestamp(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Timestamp(value) => Ok(SqlValue::Timestamp(value)),
        SqlValue::Text(value) => parse_timestamp(&value).map(SqlValue::Timestamp),
        SqlValue::Integer(value) => Ok(SqlValue::Timestamp(i64::from(value))),
        SqlValue::BigInt(value) => Ok(SqlValue::Timestamp(value)),
        SqlValue::Float(value) => {
            epoch_micros_from_float(f64::from(value)).map(SqlValue::Timestamp)
        }
        SqlValue::Double(value) => epoch_micros_from_float(value).map(SqlValue::Timestamp),
        other => timestamp_type_mismatch(other.type_name()),
    }
}

fn parse_timestamp(value: &str) -> Result<i64> {
    match NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        Ok(timestamp) => Ok(timestamp.and_utc().timestamp_micros()),
        Err(_) => timestamp_type_mismatch("TEXT"),
    }
}

fn epoch_micros_from_float(value: f64) -> Result<i64> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value < -(i64::MIN as f64)
    {
        return Ok(value as i64);
    }
    timestamp_type_mismatch("non-integral or out-of-range numeric value")
}

fn timestamp_type_mismatch<T>(actual: &str) -> Result<T> {
    Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
        expected: "Timestamp".into(),
        actual: actual.into(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_timestamp_text_becomes_epoch_micros() {
        assert_eq!(
            coerce_timestamp(SqlValue::Text("2025-01-15 10:30:00.123456".into())).unwrap(),
            SqlValue::Timestamp(1_736_937_000_123_456)
        );
    }

    #[test]
    fn timestamp_rejects_time_zone_suffixes_and_fractional_epoch_micros() {
        assert!(coerce_timestamp(SqlValue::Text("2025-01-15T10:30:00Z".into())).is_err());
        assert!(coerce_timestamp(SqlValue::Double(1.5)).is_err());
    }
}
