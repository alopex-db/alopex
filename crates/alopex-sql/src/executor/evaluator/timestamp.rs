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
        ResolvedType::Integer | ResolvedType::BigInt => coerce_integer(value, target_type),
        ResolvedType::Float | ResolvedType::Double => coerce_double(value, target_type),
        ResolvedType::Text => coerce_text(value),
        ResolvedType::Boolean => coerce_boolean(value),
        other => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedExpression(format!("cast to {other}")),
        )),
    }
}

fn cast_error(value: &SqlValue, target: &ResolvedType) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::TypeMismatch {
        expected: target.to_string(),
        actual: format!("{value:?}"),
    })
}

/// Cast to INTEGER/BIGINT. Floating point values truncate toward zero, matching
/// SQLite and PostgreSQL's `CAST(... AS INTEGER)`. Text is parsed as an integer.
fn coerce_integer(value: SqlValue, target: &ResolvedType) -> Result<SqlValue> {
    let wide: i64 = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(v) => i64::from(v),
        SqlValue::BigInt(v) | SqlValue::Timestamp(v) => v,
        SqlValue::Boolean(b) => i64::from(b),
        SqlValue::Float(v) => {
            let t = f64::from(v).trunc();
            if !t.is_finite() || t < i64::MIN as f64 || t > i64::MAX as f64 {
                return Err(cast_error(&SqlValue::Float(v), target));
            }
            t as i64
        }
        SqlValue::Double(v) => {
            let t = v.trunc();
            if !t.is_finite() || t < i64::MIN as f64 || t > i64::MAX as f64 {
                return Err(cast_error(&SqlValue::Double(v), target));
            }
            t as i64
        }
        SqlValue::Text(ref s) => match s.trim().parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Err(cast_error(&value, target)),
        },
        other => return Err(cast_error(&other, target)),
    };

    if matches!(target, ResolvedType::Integer) {
        i32::try_from(wide)
            .map(SqlValue::Integer)
            .map_err(|_| cast_error(&SqlValue::BigInt(wide), target))
    } else {
        Ok(SqlValue::BigInt(wide))
    }
}

/// Cast to FLOAT/DOUBLE. Text is parsed as a floating point literal.
fn coerce_double(value: SqlValue, target: &ResolvedType) -> Result<SqlValue> {
    let wide: f64 = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Double(v) => v,
        SqlValue::Float(v) => f64::from(v),
        SqlValue::Integer(v) => f64::from(v),
        SqlValue::BigInt(v) | SqlValue::Timestamp(v) => v as f64,
        SqlValue::Boolean(b) => f64::from(b),
        SqlValue::Text(ref s) => match s.trim().parse::<f64>() {
            Ok(v) => v,
            Err(_) => return Err(cast_error(&value, target)),
        },
        other => return Err(cast_error(&other, target)),
    };

    if matches!(target, ResolvedType::Float) {
        Ok(SqlValue::Float(wide as f32))
    } else {
        Ok(SqlValue::Double(wide))
    }
}

/// Cast to TEXT using the same rendering the dialect uses for query output.
fn coerce_text(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Text(s) => Ok(SqlValue::Text(s)),
        SqlValue::Integer(v) => Ok(SqlValue::Text(v.to_string())),
        SqlValue::BigInt(v) | SqlValue::Timestamp(v) => Ok(SqlValue::Text(v.to_string())),
        SqlValue::Float(v) => Ok(SqlValue::Text(v.to_string())),
        SqlValue::Double(v) => Ok(SqlValue::Text(v.to_string())),
        SqlValue::Boolean(b) => Ok(SqlValue::Text(if b { "true" } else { "false" }.to_string())),
        other => Err(cast_error(&other, &ResolvedType::Text)),
    }
}

/// Cast to BOOLEAN. Zero is false, any other number is true.
fn coerce_boolean(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Boolean(b) => Ok(SqlValue::Boolean(b)),
        SqlValue::Integer(v) => Ok(SqlValue::Boolean(v != 0)),
        SqlValue::BigInt(v) | SqlValue::Timestamp(v) => Ok(SqlValue::Boolean(v != 0)),
        SqlValue::Float(v) => Ok(SqlValue::Boolean(v != 0.0)),
        SqlValue::Double(v) => Ok(SqlValue::Boolean(v != 0.0)),
        SqlValue::Text(ref s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => Ok(SqlValue::Boolean(true)),
            "false" | "f" | "no" | "n" | "0" => Ok(SqlValue::Boolean(false)),
            _ => Err(cast_error(&value, &ResolvedType::Boolean)),
        },
        other => Err(cast_error(&other, &ResolvedType::Boolean)),
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
