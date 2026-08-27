use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::planner::types::ResolvedType;
use crate::storage::{DecimalValue, JsonValue, SqlValue};

use super::{EvalContext, evaluate};

/// Evaluate a typed coercion used by column assignment and INSERT planning.
pub(crate) fn evaluate_cast(
    expr: &TypedExpr,
    target_type: &ResolvedType,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    coerce_value(evaluate(expr, ctx)?, target_type)
}

/// Evaluate a TRY_CAST without hiding failures from the source expression.
pub(crate) fn evaluate_try_cast(
    expr: &TypedExpr,
    target_type: &ResolvedType,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let value = evaluate(expr, ctx)?;
    try_coerce_value(value, target_type)
}

pub(crate) fn try_coerce_value(value: SqlValue, target_type: &ResolvedType) -> Result<SqlValue> {
    match coerce_value(value, target_type) {
        Ok(value) => Ok(value),
        Err(ExecutorError::Evaluation(EvaluationError::CastFailed { .. })) => Ok(SqlValue::Null),
        Err(error) => Err(error),
    }
}

/// Normalize a value to the representation required by a typed assignment.
///
/// INSERT ... SELECT does not retain the source expressions after its query
/// has executed, so the DML boundary uses this function to enforce the same
/// storage representation as an expression-level CAST.
pub(crate) fn coerce_value(value: SqlValue, target_type: &ResolvedType) -> Result<SqlValue> {
    coerce_value_depth(value, target_type, 0)
}

fn coerce_value_depth(
    value: SqlValue,
    target_type: &ResolvedType,
    depth: usize,
) -> Result<SqlValue> {
    if depth > 16 {
        return cast_failure(
            value.type_name(),
            target_type,
            "nested value exceeds depth 16",
        );
    }
    match target_type {
        ResolvedType::Timestamp => coerce_timestamp(value),
        ResolvedType::Date => coerce_date(value),
        ResolvedType::Time => coerce_time(value),
        ResolvedType::Interval => coerce_interval(value),
        ResolvedType::Decimal { precision, scale } => {
            coerce_decimal(value, target_type, *precision, *scale)
        }
        ResolvedType::Json => coerce_json(value),
        ResolvedType::Integer | ResolvedType::BigInt => coerce_integer(value, target_type),
        ResolvedType::Float | ResolvedType::Double => coerce_double(value, target_type),
        ResolvedType::Text => coerce_text(value),
        ResolvedType::Blob => coerce_blob(value),
        ResolvedType::Boolean => coerce_boolean(value),
        ResolvedType::Vector { dimension, .. } => coerce_vector(value, target_type, *dimension),
        ResolvedType::Array(element_type) => match value {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Array(values) if values.len() <= 100_000 => values
                .into_iter()
                .map(|value| coerce_value_depth(value, element_type, depth + 1))
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Array),
            SqlValue::Array(_) => {
                cast_failure("Array", target_type, "array exceeds 100000 elements")
            }
            value => cast_failure(value.type_name(), target_type, "expected ARRAY"),
        },
        ResolvedType::Map {
            key: key_type,
            value: value_type,
        } => match value {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Map(values) if values.len() <= 100_000 => values
                .into_iter()
                .map(|(key, value)| {
                    if key.is_null() {
                        return cast_failure("Null", target_type, "map keys must not be NULL");
                    }
                    Ok((
                        coerce_value_depth(key, key_type, depth + 1)?,
                        coerce_value_depth(value, value_type, depth + 1)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Map),
            SqlValue::Map(_) => cast_failure("Map", target_type, "map exceeds 100000 entries"),
            value => cast_failure(value.type_name(), target_type, "expected MAP"),
        },
        ResolvedType::Struct(fields) => match value {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Struct(values) if values.len() == fields.len() => values
                .into_iter()
                .zip(fields)
                .map(|((name, value), (expected_name, expected_type))| {
                    if name != *expected_name {
                        return cast_failure("Struct", target_type, "struct field name mismatch");
                    }
                    Ok((name, coerce_value_depth(value, expected_type, depth + 1)?))
                })
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Struct),
            SqlValue::Struct(_) => {
                cast_failure("Struct", target_type, "struct field count mismatch")
            }
            value => cast_failure(value.type_name(), target_type, "expected STRUCT"),
        },
        ResolvedType::Null => {
            cast_failure(value.type_name(), target_type, "NULL is not a cast target")
        }
    }
}

fn coerce_decimal(
    value: SqlValue,
    target: &ResolvedType,
    precision: u8,
    scale: u8,
) -> Result<SqlValue> {
    let source = value.type_name();
    let decimal = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Decimal(value) => value,
        SqlValue::Integer(value) => DecimalValue::new(i128::from(value), 0),
        SqlValue::BigInt(value) => DecimalValue::new(i128::from(value), 0),
        SqlValue::Float(value) if value.is_finite() => DecimalValue::parse(&value.to_string())
            .ok_or_else(|| cast_error_from(source, target, "invalid decimal value"))?,
        SqlValue::Double(value) if value.is_finite() => DecimalValue::parse(&value.to_string())
            .ok_or_else(|| cast_error_from(source, target, "invalid decimal value"))?,
        SqlValue::Text(value) => DecimalValue::parse(&value)
            .ok_or_else(|| cast_error_from(source, target, "invalid decimal text"))?,
        other => return Err(cast_error(&other, target, "conversion is not supported")),
    };
    let decimal = decimal
        .rescale(scale)
        .ok_or_else(|| cast_error_from(source, target, "decimal overflow"))?;
    if !decimal.fits_precision(precision) {
        return Err(cast_error_from(
            source,
            target,
            "decimal precision overflow",
        ));
    }
    Ok(SqlValue::Decimal(decimal))
}

fn coerce_json(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        value @ SqlValue::Json(_) => Ok(value),
        SqlValue::Text(value) => JsonValue::parse(&value)
            .map(SqlValue::Json)
            .map_err(|error| cast_error_from("Text", &ResolvedType::Json, &error.to_string())),
        other => Err(cast_error(
            &other,
            &ResolvedType::Json,
            "conversion is not supported",
        )),
    }
}

fn cast_error(value: &SqlValue, target: &ResolvedType, reason: &str) -> ExecutorError {
    cast_error_from(value.type_name(), target, reason)
}

fn cast_error_from(source: &str, target: &ResolvedType, reason: &str) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::CastFailed {
        source_type: source.to_string(),
        target: target.to_string(),
        reason: reason.to_string(),
    })
}

fn cast_failure<T>(source: &str, target: &ResolvedType, reason: &str) -> Result<T> {
    Err(cast_error_from(source, target, reason))
}

/// Cast to INTEGER/BIGINT. Alopex floating-point conversion truncates toward
/// zero; text is parsed as a base-10 integer.
fn coerce_integer(value: SqlValue, target: &ResolvedType) -> Result<SqlValue> {
    let wide: i64 = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Integer(v) => i64::from(v),
        SqlValue::BigInt(v) | SqlValue::Timestamp(v) => v,
        SqlValue::Decimal(v) => {
            let divisor = crate::storage::value::decimal_power(v.scale).ok_or_else(|| {
                cast_error_from("Decimal", target, "decimal scale is out of range")
            })?;
            i64::try_from(v.coefficient / divisor)
                .map_err(|_| cast_error_from("Decimal", target, "value is out of range"))?
        }
        SqlValue::Boolean(b) => i64::from(b),
        SqlValue::Float(v) => {
            let t = f64::from(v).trunc();
            if !t.is_finite() || t < i64::MIN as f64 || t >= -(i64::MIN as f64) {
                return Err(cast_error(
                    &SqlValue::Float(v),
                    target,
                    "non-finite or out-of-range numeric value",
                ));
            }
            t as i64
        }
        SqlValue::Double(v) => {
            let t = v.trunc();
            if !t.is_finite() || t < i64::MIN as f64 || t >= -(i64::MIN as f64) {
                return Err(cast_error(
                    &SqlValue::Double(v),
                    target,
                    "non-finite or out-of-range numeric value",
                ));
            }
            t as i64
        }
        SqlValue::Text(ref s) => match s.trim().parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Err(cast_error(&value, target, "invalid integer text")),
        },
        other => return Err(cast_error(&other, target, "conversion is not supported")),
    };

    if matches!(target, ResolvedType::Integer) {
        i32::try_from(wide)
            .map(SqlValue::Integer)
            .map_err(|_| cast_error(&SqlValue::BigInt(wide), target, "value is out of range"))
    } else {
        Ok(SqlValue::BigInt(wide))
    }
}

/// Cast to FLOAT/DOUBLE. Text is parsed as a floating point literal.
fn coerce_double(value: SqlValue, target: &ResolvedType) -> Result<SqlValue> {
    let source = value.type_name();
    let wide: f64 = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Double(v) => v,
        SqlValue::Float(v) => f64::from(v),
        SqlValue::Integer(v) => f64::from(v),
        SqlValue::BigInt(v) | SqlValue::Timestamp(v) => v as f64,
        SqlValue::Decimal(v) => v
            .to_string()
            .parse::<f64>()
            .map_err(|_| cast_error_from("Decimal", target, "value is out of range"))?,
        SqlValue::Boolean(b) => f64::from(b),
        SqlValue::Text(ref s) => match s.trim().parse::<f64>() {
            Ok(v) => v,
            Err(_) => return Err(cast_error(&value, target, "invalid floating-point text")),
        },
        other => return Err(cast_error(&other, target, "conversion is not supported")),
    };

    if !wide.is_finite() {
        return cast_failure(source, target, "non-finite numeric value");
    }

    if matches!(target, ResolvedType::Float) {
        let narrowed = wide as f32;
        if !narrowed.is_finite() {
            return cast_failure(source, target, "value is out of range");
        }
        Ok(SqlValue::Float(narrowed))
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
        SqlValue::Decimal(v) => Ok(SqlValue::Text(v.to_string())),
        SqlValue::Json(v) => Ok(SqlValue::Text(v.to_string())),
        SqlValue::Boolean(b) => Ok(SqlValue::Text(if b { "true" } else { "false" }.to_string())),
        SqlValue::Date(days) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
            let date = epoch
                .checked_add_signed(chrono::Duration::days(i64::from(days)))
                .ok_or_else(|| {
                    cast_error_from("Date", &ResolvedType::Text, "date is out of range")
                })?;
            Ok(SqlValue::Text(date.format("%Y-%m-%d").to_string()))
        }
        SqlValue::Time(micros) => {
            if !(0..86_400 * MICROS_PER_SECOND).contains(&micros) {
                return cast_failure("Time", &ResolvedType::Text, "time is out of range");
            }
            let seconds = u32::try_from(micros / MICROS_PER_SECOND).expect("time seconds fit u32");
            let nanos =
                u32::try_from(micros % MICROS_PER_SECOND).expect("time micros fit u32") * 1_000;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos)
                .expect("validated time components");
            Ok(SqlValue::Text(time.format("%H:%M:%S%.6f").to_string()))
        }
        SqlValue::Interval {
            months,
            days,
            micros,
        } => Ok(SqlValue::Text(format!(
            "{months} months {days} days {micros} microseconds"
        ))),
        SqlValue::Blob(bytes) => String::from_utf8(bytes)
            .map(SqlValue::Text)
            .map_err(|_| cast_error_from("Blob", &ResolvedType::Text, "invalid UTF-8")),
        other => Err(cast_error(
            &other,
            &ResolvedType::Text,
            "conversion is not supported",
        )),
    }
}

fn coerce_blob(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Blob(bytes) => Ok(SqlValue::Blob(bytes)),
        SqlValue::Text(text) => Ok(SqlValue::Blob(text.into_bytes())),
        other => Err(cast_error(
            &other,
            &ResolvedType::Blob,
            "conversion is not supported",
        )),
    }
}

fn coerce_vector(value: SqlValue, target: &ResolvedType, dimension: u32) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Vector(values)
            if values.len() == dimension as usize
                && values.iter().all(|value| value.is_finite()) =>
        {
            Ok(SqlValue::Vector(values))
        }
        SqlValue::Vector(values) if values.len() != dimension as usize => Err(cast_error(
            &SqlValue::Vector(values),
            target,
            "vector dimension does not match",
        )),
        SqlValue::Vector(values) => Err(cast_error(
            &SqlValue::Vector(values),
            target,
            "vector contains a non-finite value",
        )),
        other => Err(cast_error(&other, target, "conversion is not supported")),
    }
}

/// Cast to BOOLEAN. Zero is false, any other number is true.
fn coerce_boolean(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Boolean(b) => Ok(SqlValue::Boolean(b)),
        SqlValue::Integer(v) => Ok(SqlValue::Boolean(v != 0)),
        SqlValue::BigInt(v) | SqlValue::Timestamp(v) => Ok(SqlValue::Boolean(v != 0)),
        SqlValue::Float(v) if v.is_finite() => Ok(SqlValue::Boolean(v != 0.0)),
        SqlValue::Double(v) if v.is_finite() => Ok(SqlValue::Boolean(v != 0.0)),
        SqlValue::Float(v) => Err(cast_error(
            &SqlValue::Float(v),
            &ResolvedType::Boolean,
            "non-finite numeric value",
        )),
        SqlValue::Double(v) => Err(cast_error(
            &SqlValue::Double(v),
            &ResolvedType::Boolean,
            "non-finite numeric value",
        )),
        SqlValue::Text(ref s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => Ok(SqlValue::Boolean(true)),
            "false" | "f" | "no" | "n" | "0" => Ok(SqlValue::Boolean(false)),
            _ => Err(cast_error(
                &value,
                &ResolvedType::Boolean,
                "invalid boolean text",
            )),
        },
        other => Err(cast_error(
            &other,
            &ResolvedType::Boolean,
            "conversion is not supported",
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
            epoch_micros_from_float(f64::from(value), "Float").map(SqlValue::Timestamp)
        }
        SqlValue::Double(value) => {
            epoch_micros_from_float(value, "Double").map(SqlValue::Timestamp)
        }
        other => cast_failure(
            other.type_name(),
            &ResolvedType::Timestamp,
            "conversion is not supported",
        ),
    }
}

fn parse_timestamp(value: &str) -> Result<i64> {
    match NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        Ok(timestamp) => Ok(timestamp.and_utc().timestamp_micros()),
        Err(_) => cast_failure("Text", &ResolvedType::Timestamp, "invalid timestamp text"),
    }
}

const MICROS_PER_SECOND: i64 = 1_000_000;

pub(crate) fn coerce_date(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Date(days) => Ok(SqlValue::Date(days)),
        SqlValue::Timestamp(micros) => Ok(SqlValue::Date(
            i32::try_from(micros.div_euclid(86_400 * MICROS_PER_SECOND)).map_err(|_| {
                cast_error_from("Timestamp", &ResolvedType::Date, "date is out of range")
            })?,
        )),
        SqlValue::Text(text) => parse_date(&text).map(SqlValue::Date),
        other => cast_failure(
            other.type_name(),
            &ResolvedType::Date,
            "conversion is not supported",
        ),
    }
}

pub(crate) fn coerce_time(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Time(micros) => Ok(SqlValue::Time(micros)),
        SqlValue::Timestamp(micros) => Ok(SqlValue::Time(
            micros.rem_euclid(86_400 * MICROS_PER_SECOND),
        )),
        SqlValue::Text(text) => parse_time(&text).map(SqlValue::Time),
        other => cast_failure(
            other.type_name(),
            &ResolvedType::Time,
            "conversion is not supported",
        ),
    }
}

pub(crate) fn coerce_interval(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        value @ SqlValue::Interval { .. } => Ok(value),
        SqlValue::Text(text) => parse_interval(&text),
        other => cast_failure(
            other.type_name(),
            &ResolvedType::Interval,
            "conversion is not supported",
        ),
    }
}

fn parse_date(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d")
        .map_err(|_| cast_error_from("Text", &ResolvedType::Date, "invalid date text"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| cast_error_from("Text", &ResolvedType::Date, "date is out of range"))
}

fn parse_time(value: &str) -> Result<i64> {
    let value = value.trim();
    if value
        .split_once('.')
        .is_some_and(|(_, fraction)| fraction.len() > 6)
    {
        return cast_failure(
            "Text",
            &ResolvedType::Time,
            "time precision exceeds microseconds",
        );
    }
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
        .map_err(|_| cast_error_from("Text", &ResolvedType::Time, "invalid time text"))?;
    Ok(
        i64::from(time.num_seconds_from_midnight()) * MICROS_PER_SECOND
            + i64::from(time.nanosecond() / 1_000),
    )
}

pub(crate) fn parse_interval(value: &str) -> Result<SqlValue> {
    let mut months = 0_i32;
    let mut days = 0_i32;
    let mut micros = 0_i64;
    let parts = value.split_whitespace().collect::<Vec<_>>();
    let mut index = 0;
    while index < parts.len() {
        if parts[index].contains(':') {
            let negative = parts[index].starts_with('-');
            let value = parse_time(parts[index].trim_start_matches(['+', '-']))?;
            micros = micros
                .checked_add(if negative { -value } else { value })
                .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
            index += 1;
            continue;
        }
        if index + 1 >= parts.len() {
            return cast_failure("Text", &ResolvedType::Interval, "invalid interval text");
        }
        let unit = parts[index + 1]
            .trim_end_matches('s')
            .to_ascii_lowercase()
            .to_string();
        if unit == "second" {
            let seconds = parts[index].parse::<f64>().map_err(|_| {
                cast_error_from("Text", &ResolvedType::Interval, "invalid interval text")
            })?;
            let delta = seconds * MICROS_PER_SECOND as f64;
            if !delta.is_finite() || delta < i64::MIN as f64 || delta >= -(i64::MIN as f64) {
                return cast_failure("Text", &ResolvedType::Interval, "interval is out of range");
            }
            micros = micros
                .checked_add(delta.round() as i64)
                .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
            index += 2;
            continue;
        }
        let amount = parts[index].parse::<i64>().map_err(|_| {
            cast_error_from("Text", &ResolvedType::Interval, "invalid interval text")
        })?;
        match unit.as_str() {
            "year" => {
                months = months
                    .checked_add(
                        i32::try_from(
                            amount
                                .checked_mul(12)
                                .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                        )
                        .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    )
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            "month" => {
                months = months
                    .checked_add(
                        i32::try_from(amount)
                            .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    )
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            "week" => {
                days = days
                    .checked_add(
                        i32::try_from(
                            amount
                                .checked_mul(7)
                                .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                        )
                        .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    )
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            "day" => {
                days = days
                    .checked_add(
                        i32::try_from(amount)
                            .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    )
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            "hour" => {
                micros = micros
                    .checked_add(
                        amount
                            .checked_mul(3_600 * MICROS_PER_SECOND)
                            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    )
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            "minute" => {
                micros = micros
                    .checked_add(
                        amount
                            .checked_mul(60 * MICROS_PER_SECOND)
                            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    )
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            "millisecond" => {
                micros = micros
                    .checked_add(
                        amount
                            .checked_mul(1_000)
                            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                    )
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            "microsecond" => {
                micros = micros
                    .checked_add(amount)
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
            }
            _ => return cast_failure("Text", &ResolvedType::Interval, "invalid interval unit"),
        }
        index += 2;
    }
    Ok(SqlValue::Interval {
        months,
        days,
        micros,
    })
}

fn epoch_micros_from_float(value: f64, source: &str) -> Result<i64> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value < -(i64::MIN as f64)
    {
        return Ok(value as i64);
    }
    cast_failure(
        source,
        &ResolvedType::Timestamp,
        "non-integral or out-of-range numeric value",
    )
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

    #[test]
    fn numeric_casts_reject_non_finite_values_and_float_overflow() {
        for value in ["NaN", "Infinity", "-Infinity"] {
            assert!(coerce_value(SqlValue::Text(value.into()), &ResolvedType::Double).is_err());
        }
        assert!(coerce_value(SqlValue::Text("3.5e38".into()), &ResolvedType::Float).is_err());
        assert!(coerce_value(SqlValue::Float(f32::NAN), &ResolvedType::Boolean).is_err());
        assert!(coerce_value(SqlValue::Double(f64::INFINITY), &ResolvedType::Boolean).is_err());
    }

    #[test]
    fn numeric_casts_reject_the_exclusive_i64_upper_bound() {
        let exclusive_upper_bound = -(i64::MIN as f64);
        assert!(
            coerce_value(
                SqlValue::Double(exclusive_upper_bound),
                &ResolvedType::BigInt,
            )
            .is_err()
        );
        assert!(coerce_timestamp(SqlValue::Double(exclusive_upper_bound)).is_err());
    }

    #[test]
    fn blob_text_and_vector_casts_enforce_encoding_and_dimension() {
        assert_eq!(
            coerce_value(SqlValue::Text("hello".into()), &ResolvedType::Blob).unwrap(),
            SqlValue::Blob(b"hello".to_vec())
        );
        assert!(coerce_value(SqlValue::Blob(vec![0xff]), &ResolvedType::Text).is_err());

        let vector_two = ResolvedType::Vector {
            dimension: 2,
            metric: crate::ast::ddl::VectorMetric::Cosine,
        };
        assert_eq!(
            coerce_value(SqlValue::Vector(vec![1.0, 2.0]), &vector_two).unwrap(),
            SqlValue::Vector(vec![1.0, 2.0])
        );
        assert!(coerce_value(SqlValue::Vector(vec![1.0]), &vector_two).is_err());
        assert!(coerce_value(SqlValue::Vector(vec![f32::NAN, 1.0]), &vector_two).is_err());
        assert!(coerce_value(SqlValue::Vector(vec![f32::INFINITY, 1.0]), &vector_two).is_err());

        let vector_l2 = ResolvedType::Vector {
            dimension: 2,
            metric: crate::ast::ddl::VectorMetric::L2,
        };
        let rendered = coerce_value(SqlValue::Vector(vec![1.0]), &vector_l2)
            .expect_err("dimension mismatch")
            .to_string();
        assert!(rendered.contains("VECTOR(2, L2)"), "{rendered}");
    }
}
