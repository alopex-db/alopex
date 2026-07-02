//! Type casting primitives for DataFrame values.

use std::fmt;
use std::num::ParseFloatError;

use crate::Error;
use crate::Result;

/// Stable cast failure code for values outside the target type range.
pub const CAST_REASON_OVERFLOW: &str = "overflow";
/// Stable cast failure code for conversions that would discard information.
pub const CAST_REASON_PRECISION_LOSS: &str = "precision_loss";
/// Stable cast failure code for malformed string input.
pub const CAST_REASON_INVALID_FORMAT: &str = "invalid_format";
/// Stable cast failure code for NaN or infinite floating-point input.
pub const CAST_REASON_NON_FINITE: &str = "non_finite";
/// Stable cast failure code for unsupported type pairs.
pub const CAST_REASON_UNSUPPORTED: &str = "unsupported";

const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// DataFrame scalar types supported by the core cast matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    /// Absence of a value.
    Null,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 32-bit floating point.
    Float32,
    /// 64-bit floating point.
    Float64,
    /// UTF-8 string.
    Utf8,
    /// Timestamp stored as microseconds since the Unix epoch.
    TimestampMicros,
}

impl DataType {
    /// Return the stable display name used in cast errors.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Null => "Null",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Utf8 => "Utf8",
            Self::TimestampMicros => "TimestampMicros",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// DataFrame scalar value used by the core cast primitives.
#[derive(Clone, Debug, PartialEq)]
pub enum DataValue {
    /// Absence of a value.
    Null,
    /// 32-bit signed integer.
    Int32(i32),
    /// 64-bit signed integer.
    Int64(i64),
    /// 32-bit floating point.
    Float32(f32),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// Timestamp stored as microseconds since the Unix epoch.
    TimestampMicros(i64),
}

impl DataValue {
    /// Return this value's DataFrame type.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::TimestampMicros(_) => DataType::TimestampMicros,
        }
    }
}

/// Return whether the preferred cast matrix has a path between two types.
pub fn can_cast(from: DataType, to: DataType) -> bool {
    if from == to || from == DataType::Null {
        return true;
    }

    matches!(
        (from, to),
        (_, DataType::Utf8)
            | (
                DataType::Utf8,
                DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::TimestampMicros,
            )
            | (
                DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64,
                DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::TimestampMicros,
            )
            | (
                DataType::TimestampMicros,
                DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64,
            )
    )
}

/// Cast a DataFrame scalar value to the requested target type.
pub fn cast_value(value: &DataValue, to_type: DataType) -> Result<DataValue> {
    let from_type = value.data_type();
    if !can_cast(from_type, to_type) {
        return Err(cast_failed(from_type, to_type, CAST_REASON_UNSUPPORTED));
    }

    if matches!(value, DataValue::Null) {
        return Ok(DataValue::Null);
    }

    if value.data_type() == to_type {
        return Ok(value.clone());
    }

    match to_type {
        DataType::Null => Ok(DataValue::Null),
        DataType::Int32 => cast_to_int32(value),
        DataType::Int64 => cast_to_int64(value),
        DataType::Float32 => cast_to_float32(value),
        DataType::Float64 => cast_to_float64(value),
        DataType::Utf8 => Ok(DataValue::Utf8(value_to_string(value))),
        DataType::TimestampMicros => cast_to_timestamp_micros(value),
    }
}

fn cast_to_int32(value: &DataValue) -> Result<DataValue> {
    let from_type = value.data_type();
    let parsed = match value {
        DataValue::Int64(v) => i32::try_from(*v)
            .map_err(|_| cast_failed(from_type, DataType::Int32, CAST_REASON_OVERFLOW))?,
        DataValue::Float32(v) => f64_to_i32(f64::from(*v), from_type)?,
        DataValue::Float64(v) => f64_to_i32(*v, from_type)?,
        DataValue::Utf8(v) => v
            .trim()
            .parse::<i32>()
            .map_err(|_| cast_failed(from_type, DataType::Int32, CAST_REASON_INVALID_FORMAT))?,
        DataValue::TimestampMicros(v) => {
            micros_to_i64_epoch_seconds(*v, from_type).and_then(|seconds| {
                i32::try_from(seconds)
                    .map_err(|_| cast_failed(from_type, DataType::Int32, CAST_REASON_OVERFLOW))
            })?
        }
        DataValue::Int32(v) => *v,
        DataValue::Null => unreachable!("null is handled before typed casts"),
    };
    Ok(DataValue::Int32(parsed))
}

fn cast_to_int64(value: &DataValue) -> Result<DataValue> {
    let from_type = value.data_type();
    let parsed = match value {
        DataValue::Int32(v) => i64::from(*v),
        DataValue::Float32(v) => f64_to_i64(f64::from(*v), from_type)?,
        DataValue::Float64(v) => f64_to_i64(*v, from_type)?,
        DataValue::Utf8(v) => v
            .trim()
            .parse::<i64>()
            .map_err(|_| cast_failed(from_type, DataType::Int64, CAST_REASON_INVALID_FORMAT))?,
        DataValue::TimestampMicros(v) => micros_to_i64_epoch_seconds(*v, from_type)?,
        DataValue::Int64(v) => *v,
        DataValue::Null => unreachable!("null is handled before typed casts"),
    };
    Ok(DataValue::Int64(parsed))
}

fn cast_to_float32(value: &DataValue) -> Result<DataValue> {
    let from_type = value.data_type();
    let parsed = match value {
        DataValue::Int32(v) => i64_to_f32(i64::from(*v), from_type)?,
        DataValue::Int64(v) => i64_to_f32(*v, from_type)?,
        DataValue::Float64(v) => f64_to_f32(*v, from_type)?,
        DataValue::Utf8(v) => parse_f32(v.trim(), from_type)?,
        DataValue::TimestampMicros(v) => {
            f64_to_f32(micros_to_f64_epoch_seconds(*v, from_type)?, from_type)?
        }
        DataValue::Float32(v) => *v,
        DataValue::Null => unreachable!("null is handled before typed casts"),
    };
    Ok(DataValue::Float32(parsed))
}

fn cast_to_float64(value: &DataValue) -> Result<DataValue> {
    let from_type = value.data_type();
    let parsed = match value {
        DataValue::Int32(v) => f64::from(*v),
        DataValue::Int64(v) => i64_to_f64(*v, from_type)?,
        DataValue::Float32(v) => {
            if v.is_finite() {
                f64::from(*v)
            } else {
                return Err(cast_failed(
                    from_type,
                    DataType::Float64,
                    CAST_REASON_NON_FINITE,
                ));
            }
        }
        DataValue::Utf8(v) => parse_f64(v.trim(), from_type)?,
        DataValue::TimestampMicros(v) => micros_to_f64_epoch_seconds(*v, from_type)?,
        DataValue::Float64(v) => *v,
        DataValue::Null => unreachable!("null is handled before typed casts"),
    };
    Ok(DataValue::Float64(parsed))
}

fn cast_to_timestamp_micros(value: &DataValue) -> Result<DataValue> {
    let from_type = value.data_type();
    let micros = match value {
        DataValue::Int32(v) => epoch_seconds_to_micros(i64::from(*v), from_type)?,
        DataValue::Int64(v) => epoch_seconds_to_micros(*v, from_type)?,
        DataValue::Float32(v) => f64_epoch_seconds_to_micros(f64::from(*v), from_type)?,
        DataValue::Float64(v) => f64_epoch_seconds_to_micros(*v, from_type)?,
        DataValue::Utf8(v) => parse_timestamp_micros(v.trim(), from_type)?,
        DataValue::TimestampMicros(v) => *v,
        DataValue::Null => unreachable!("null is handled before typed casts"),
    };
    Ok(DataValue::TimestampMicros(micros))
}

fn value_to_string(value: &DataValue) -> String {
    match value {
        DataValue::Null => "NULL".to_string(),
        DataValue::Int32(v) => v.to_string(),
        DataValue::Int64(v) => v.to_string(),
        DataValue::Float32(v) => v.to_string(),
        DataValue::Float64(v) => v.to_string(),
        DataValue::Utf8(v) => v.clone(),
        DataValue::TimestampMicros(v) => format_timestamp_micros(*v),
    }
}

fn parse_f32(input: &str, from_type: DataType) -> Result<f32> {
    let parsed = input
        .parse::<f32>()
        .map_err(|err| parse_float_error(from_type, DataType::Float32, err))?;
    if parsed.is_finite() {
        Ok(parsed)
    } else {
        Err(cast_failed(
            from_type,
            DataType::Float32,
            CAST_REASON_NON_FINITE,
        ))
    }
}

fn parse_f64(input: &str, from_type: DataType) -> Result<f64> {
    let parsed = input
        .parse::<f64>()
        .map_err(|err| parse_float_error(from_type, DataType::Float64, err))?;
    if parsed.is_finite() {
        Ok(parsed)
    } else {
        Err(cast_failed(
            from_type,
            DataType::Float64,
            CAST_REASON_NON_FINITE,
        ))
    }
}

fn parse_float_error(from_type: DataType, to_type: DataType, _err: ParseFloatError) -> Error {
    cast_failed(from_type, to_type, CAST_REASON_INVALID_FORMAT)
}

fn f64_to_i32(value: f64, from_type: DataType) -> Result<i32> {
    ensure_finite(value, from_type, DataType::Int32)?;
    if value.fract() != 0.0 {
        return Err(cast_failed(
            from_type,
            DataType::Int32,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    if value < f64::from(i32::MIN) || value > f64::from(i32::MAX) {
        return Err(cast_failed(
            from_type,
            DataType::Int32,
            CAST_REASON_OVERFLOW,
        ));
    }
    Ok(value as i32)
}

fn f64_to_i64(value: f64, from_type: DataType) -> Result<i64> {
    ensure_finite(value, from_type, DataType::Int64)?;
    if value.fract() != 0.0 {
        return Err(cast_failed(
            from_type,
            DataType::Int64,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    if value < i64::MIN as f64 || value >= 9_223_372_036_854_775_808.0 {
        return Err(cast_failed(
            from_type,
            DataType::Int64,
            CAST_REASON_OVERFLOW,
        ));
    }
    let casted = value as i64;
    if casted as f64 != value {
        return Err(cast_failed(
            from_type,
            DataType::Int64,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    Ok(casted)
}

fn f64_to_f32(value: f64, from_type: DataType) -> Result<f32> {
    ensure_finite(value, from_type, DataType::Float32)?;
    let casted = value as f32;
    if !casted.is_finite() {
        return Err(cast_failed(
            from_type,
            DataType::Float32,
            CAST_REASON_OVERFLOW,
        ));
    }
    if f64::from(casted) != value {
        return Err(cast_failed(
            from_type,
            DataType::Float32,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    Ok(casted)
}

fn i64_to_f32(value: i64, from_type: DataType) -> Result<f32> {
    if !integer_exact_in_float(value.unsigned_abs(), 24) {
        return Err(cast_failed(
            from_type,
            DataType::Float32,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    Ok(value as f32)
}

fn i64_to_f64(value: i64, from_type: DataType) -> Result<f64> {
    if !integer_exact_in_float(value.unsigned_abs(), 53) {
        return Err(cast_failed(
            from_type,
            DataType::Float64,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    Ok(value as f64)
}

fn integer_exact_in_float(abs_value: u64, precision_bits: u32) -> bool {
    if abs_value == 0 {
        return true;
    }
    let bit_width = u64::BITS - abs_value.leading_zeros();
    bit_width <= precision_bits || abs_value.trailing_zeros() >= bit_width - precision_bits
}

fn ensure_finite(value: f64, from_type: DataType, to_type: DataType) -> Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(cast_failed(from_type, to_type, CAST_REASON_NON_FINITE))
    }
}

fn epoch_seconds_to_micros(seconds: i64, from_type: DataType) -> Result<i64> {
    seconds
        .checked_mul(MICROS_PER_SECOND)
        .ok_or_else(|| cast_failed(from_type, DataType::TimestampMicros, CAST_REASON_OVERFLOW))
}

fn f64_epoch_seconds_to_micros(seconds: f64, from_type: DataType) -> Result<i64> {
    ensure_finite(seconds, from_type, DataType::TimestampMicros)?;
    let micros = seconds * MICROS_PER_SECOND as f64;
    ensure_finite(micros, from_type, DataType::TimestampMicros)?;
    if micros.fract() != 0.0 {
        return Err(cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    if micros < i64::MIN as f64 || micros >= 9_223_372_036_854_775_808.0 {
        return Err(cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_OVERFLOW,
        ));
    }
    Ok(micros as i64)
}

fn micros_to_i64_epoch_seconds(micros: i64, from_type: DataType) -> Result<i64> {
    if micros % MICROS_PER_SECOND != 0 {
        return Err(cast_failed(
            from_type,
            DataType::Int64,
            CAST_REASON_PRECISION_LOSS,
        ));
    }
    Ok(micros / MICROS_PER_SECOND)
}

fn micros_to_f64_epoch_seconds(micros: i64, from_type: DataType) -> Result<f64> {
    let micros = i64_to_f64(micros, from_type)?;
    Ok(micros / MICROS_PER_SECOND as f64)
}

fn parse_timestamp_micros(input: &str, from_type: DataType) -> Result<i64> {
    let input = input.strip_suffix('Z').unwrap_or(input);
    let (date, time) = input
        .split_once('T')
        .or_else(|| input.split_once(' '))
        .ok_or_else(|| {
            cast_failed(
                from_type,
                DataType::TimestampMicros,
                CAST_REASON_INVALID_FORMAT,
            )
        })?;

    let mut date_parts = date.split('-');
    let year = parse_date_part::<i32>(date_parts.next(), from_type)?;
    let month = parse_date_part::<u32>(date_parts.next(), from_type)?;
    let day = parse_date_part::<u32>(date_parts.next(), from_type)?;
    if date_parts.next().is_some() || !valid_date(year, month, day) {
        return Err(cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        ));
    }

    let mut time_parts = time.split(':');
    let hour = parse_date_part::<u32>(time_parts.next(), from_type)?;
    let minute = parse_date_part::<u32>(time_parts.next(), from_type)?;
    let second_text = time_parts.next().ok_or_else(|| {
        cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        )
    })?;
    if time_parts.next().is_some() {
        return Err(cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        ));
    }

    let (second, micros) = parse_second_micros(second_text, from_type)?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        ));
    }

    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(SECONDS_PER_DAY)
        .and_then(|v| v.checked_add(i64::from(hour) * 3_600))
        .and_then(|v| v.checked_add(i64::from(minute) * 60))
        .and_then(|v| v.checked_add(i64::from(second)))
        .ok_or_else(|| cast_failed(from_type, DataType::TimestampMicros, CAST_REASON_OVERFLOW))?;
    seconds
        .checked_mul(MICROS_PER_SECOND)
        .and_then(|v| v.checked_add(i64::from(micros)))
        .ok_or_else(|| cast_failed(from_type, DataType::TimestampMicros, CAST_REASON_OVERFLOW))
}

fn parse_date_part<T>(part: Option<&str>, from_type: DataType) -> Result<T>
where
    T: std::str::FromStr,
{
    part.and_then(|text| text.parse::<T>().ok()).ok_or_else(|| {
        cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        )
    })
}

fn parse_second_micros(input: &str, from_type: DataType) -> Result<(u32, u32)> {
    let (second, fraction) = input.split_once('.').unwrap_or((input, ""));
    let second = second.parse::<u32>().map_err(|_| {
        cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        )
    })?;
    if fraction.is_empty() {
        return Ok((second, 0));
    }
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        ));
    }
    if fraction.len() > 6 {
        return Err(cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_PRECISION_LOSS,
        ));
    }

    let mut micros = fraction.parse::<u32>().map_err(|_| {
        cast_failed(
            from_type,
            DataType::TimestampMicros,
            CAST_REASON_INVALID_FORMAT,
        )
    })?;
    for _ in fraction.len()..6 {
        micros *= 10;
    }
    Ok((second, micros))
}

fn format_timestamp_micros(micros: i64) -> String {
    let seconds = micros.div_euclid(MICROS_PER_SECOND);
    let micros_remainder = micros.rem_euclid(MICROS_PER_SECOND);
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3_600;
    let minute = (second_of_day % 3_600) / 60;
    let second = second_of_day % 60;

    if micros_remainder == 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    } else {
        let mut fraction = format!("{micros_remainder:06}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{fraction}Z")
    }
}

fn valid_date(year: i32, month: u32, day: u32) -> bool {
    (1..=12).contains(&month) && (1..=days_in_month(year, month)).contains(&day)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn cast_failed(from_type: DataType, to_type: DataType, reason: &str) -> Error {
    Error::CastFailed {
        from_type: from_type.as_str().to_string(),
        to_type: to_type.as_str().to_string(),
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{can_cast, cast_value, DataType, DataValue};
    use crate::Error;

    fn assert_cast(input: DataValue, target: DataType, expected: DataValue) {
        assert_eq!(cast_value(&input, target).unwrap(), expected);
    }

    fn assert_cast_error(input: DataValue, target: DataType, reason: &str) {
        let err = cast_value(&input, target).unwrap_err();
        match err {
            Error::CastFailed {
                from_type,
                to_type,
                reason: actual,
            } => {
                assert_eq!(from_type, input.data_type().as_str());
                assert_eq!(to_type, target.as_str());
                assert_eq!(actual, reason);
            }
            other => panic!("expected CastFailed, got {other:?}"),
        }
    }

    #[test]
    fn cast_matrix_marks_supported_major_paths() {
        assert!(can_cast(DataType::Int32, DataType::Int64));
        assert!(can_cast(DataType::Int32, DataType::Utf8));
        assert!(can_cast(DataType::Utf8, DataType::Float64));
        assert!(can_cast(DataType::Utf8, DataType::TimestampMicros));
        assert!(can_cast(DataType::TimestampMicros, DataType::Int64));
        assert!(can_cast(DataType::Float64, DataType::TimestampMicros));
        assert!(can_cast(DataType::Null, DataType::TimestampMicros));
    }

    #[test]
    fn numeric_casts_cover_widening_and_checked_narrowing() {
        assert_cast(DataValue::Int32(42), DataType::Int64, DataValue::Int64(42));
        assert_cast(
            DataValue::Int64(16_777_216),
            DataType::Float32,
            DataValue::Float32(16_777_216.0),
        );
        assert_cast(
            DataValue::Float64(42.0),
            DataType::Int32,
            DataValue::Int32(42),
        );
        assert_cast(
            DataValue::Float32(1.25),
            DataType::Float64,
            DataValue::Float64(1.25),
        );
    }

    #[test]
    fn numeric_casts_report_stable_error_codes() {
        assert_cast_error(
            DataValue::Int64(i64::from(i32::MAX) + 1),
            DataType::Int32,
            "overflow",
        );
        assert_cast_error(DataValue::Float64(42.5), DataType::Int64, "precision_loss");
        assert_cast_error(DataValue::Float64(f64::NAN), DataType::Int64, "non_finite");
        assert_cast_error(
            DataValue::Int64(16_777_217),
            DataType::Float32,
            "precision_loss",
        );
    }

    #[test]
    fn string_numeric_casts_parse_and_stringify() {
        assert_cast(
            DataValue::Utf8(" -12 ".into()),
            DataType::Int32,
            DataValue::Int32(-12),
        );
        assert_cast(
            DataValue::Utf8("3.5".into()),
            DataType::Float64,
            DataValue::Float64(3.5),
        );
        assert_cast(
            DataValue::Int64(99),
            DataType::Utf8,
            DataValue::Utf8("99".into()),
        );
        assert_cast_error(
            DataValue::Utf8("nan".into()),
            DataType::Int64,
            "invalid_format",
        );
    }

    #[test]
    fn timestamp_string_and_epoch_second_casts_are_stable() {
        assert_cast(
            DataValue::Utf8("1970-01-01T00:00:01Z".into()),
            DataType::TimestampMicros,
            DataValue::TimestampMicros(1_000_000),
        );
        assert_cast(
            DataValue::Utf8("1970-01-01 00:00:01.250000".into()),
            DataType::TimestampMicros,
            DataValue::TimestampMicros(1_250_000),
        );
        assert_cast(
            DataValue::TimestampMicros(1_250_000),
            DataType::Float64,
            DataValue::Float64(1.25),
        );
        assert_cast(
            DataValue::Float64(1.25),
            DataType::TimestampMicros,
            DataValue::TimestampMicros(1_250_000),
        );
        assert_cast(
            DataValue::TimestampMicros(1_000_000),
            DataType::Utf8,
            DataValue::Utf8("1970-01-01T00:00:01Z".into()),
        );
        assert_cast_error(
            DataValue::Utf8("1970-13-01T00:00:00Z".into()),
            DataType::TimestampMicros,
            "invalid_format",
        );
    }

    #[test]
    fn timestamp_micros_to_float64_rejects_precision_loss() {
        assert_cast_error(
            DataValue::TimestampMicros((1_i64 << 53) + 1),
            DataType::Float64,
            "precision_loss",
        );
    }

    #[test]
    fn null_casts_to_any_supported_type_without_changing_value() {
        assert_cast(DataValue::Null, DataType::Int64, DataValue::Null);
        assert_cast(DataValue::Null, DataType::Utf8, DataValue::Null);
        assert_cast(DataValue::Null, DataType::TimestampMicros, DataValue::Null);
    }
}
