use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::context::current_statement_timestamp;
use super::{EvalContext, evaluate};

/// Evaluate NOW() using the UTC timestamp fixed when the statement began.
pub(crate) fn eval_now_values(_values: &[SqlValue]) -> Result<SqlValue> {
    Ok(SqlValue::Timestamp(current_statement_timestamp()))
}

/// Lazy dispatch keeps NOW() tied to the context that evaluates the expression.
pub(crate) fn eval_now_lazy(args: &[TypedExpr], ctx: &EvalContext<'_>) -> Result<SqlValue> {
    let precision = match args.first() {
        None => 6,
        Some(arg) => match evaluate(arg, ctx)? {
            SqlValue::Integer(value) => value,
            SqlValue::BigInt(value) => i32::try_from(value)
                .map_err(|_| invalid("current_timestamp", "precision must be between 0 and 6"))?,
            SqlValue::Null => return Ok(SqlValue::Null),
            value => {
                return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "Integer".into(),
                    actual: value.type_name().into(),
                }));
            }
        },
    };
    if !(0..=6).contains(&precision) {
        return Err(invalid(
            "current_timestamp",
            "precision must be between 0 and 6",
        ));
    }
    let factor = 10_i64.pow((6 - precision) as u32);
    Ok(SqlValue::Timestamp(
        ctx.statement_timestamp().div_euclid(factor) * factor,
    ))
}

macro_rules! wrappers {
    ($($fn_name:ident => $name:literal),+ $(,)?) => {
        $(fn $fn_name(values: &[SqlValue]) -> Result<SqlValue> { eval_named($name, values) })+
        pub fn eval_for(name: &str) -> Option<fn(&[SqlValue]) -> Result<SqlValue>> {
            match name { $( $name => Some($fn_name), )+ _ => None }
        }
    };
}

wrappers!(
    eval_current_date => "current_date",
    eval_current_time => "current_time",
    eval_make_date => "make_date",
    eval_make_time => "make_time",
    eval_make_timestamp => "make_timestamp",
    eval_make_interval => "make_interval",
    eval_date => "date",
    eval_time => "time",
    eval_datetime => "datetime",
    eval_to_date => "to_date",
    eval_age => "age",
    eval_date_add => "date_add",
    eval_date_sub => "date_sub",
    eval_extract => "extract",
    eval_date_part => "date_part",
    eval_date_trunc => "date_trunc",
    eval_to_char => "to_char",
    eval_to_timestamp => "to_timestamp",
    eval_strftime => "strftime",
    eval_julianday => "julianday",
    eval_unixepoch => "unixepoch",
);

fn eval_named(name: &str, values: &[SqlValue]) -> Result<SqlValue> {
    if values.iter().any(SqlValue::is_null) {
        return Ok(SqlValue::Null);
    }
    match name {
        "current_date" => Ok(SqlValue::Date(
            i32::try_from(current_statement_timestamp().div_euclid(86_400_000_000))
                .map_err(|_| invalid(name, "date is out of range"))?,
        )),
        "current_time" => Ok(SqlValue::Time(
            current_statement_timestamp().rem_euclid(86_400_000_000),
        )),
        "make_date" => make_date(values),
        "make_time" => make_time(values),
        "make_timestamp" => make_timestamp(values),
        "make_interval" => make_interval(values),
        "date" => super::timestamp::coerce_date(values[0].clone()),
        "time" => super::timestamp::coerce_time(values[0].clone()),
        "datetime" => sqlite_datetime(values),
        "to_date" => to_date(values),
        "age" => age(values),
        "date_add" => super::binary_op::eval_binary_values(
            &crate::ast::expr::BinaryOp::Add,
            values[0].clone(),
            values[1].clone(),
        ),
        "date_sub" => super::binary_op::eval_binary_values(
            &crate::ast::expr::BinaryOp::Sub,
            values[0].clone(),
            values[1].clone(),
        ),
        "extract" | "date_part" => {
            let unit = text(values.first(), name)?;
            let micros = timestamp(values.get(1), name)?;
            Ok(SqlValue::Double(extract_part(unit, micros, name)?))
        }
        "date_trunc" => {
            let unit = text(values.first(), name)?;
            let micros = timestamp(values.get(1), name)?;
            Ok(SqlValue::Timestamp(truncate(unit, micros)?))
        }
        "to_char" => {
            let micros = timestamp(values.first(), name)?;
            let format = postgres_format(text(values.get(1), name)?);
            Ok(SqlValue::Text(format_datetime(micros, &format, name)?))
        }
        "to_timestamp" => to_timestamp(values),
        "strftime" => {
            let format = text(values.first(), name)?;
            let micros = timestamp(values.get(1), name)?;
            Ok(SqlValue::Text(format_datetime(micros, format, name)?))
        }
        "julianday" => Ok(SqlValue::Double(
            timestamp(values.first(), name)? as f64 / 86_400_000_000.0 + 2_440_587.5,
        )),
        "unixepoch" => Ok(SqlValue::BigInt(
            timestamp(values.first(), name)?.div_euclid(1_000_000),
        )),
        _ => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(name.into()),
        )),
    }
}

fn integer(value: &SqlValue, function: &str) -> Result<i64> {
    match value {
        SqlValue::Integer(value) => Ok(i64::from(*value)),
        SqlValue::BigInt(value) => Ok(*value),
        value => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Integer".into(),
            actual: format!("{} in {function}", value.type_name()),
        })),
    }
}

fn number(value: &SqlValue, function: &str) -> Result<f64> {
    match value {
        SqlValue::Integer(value) => Ok(f64::from(*value)),
        SqlValue::BigInt(value) => Ok(*value as f64),
        SqlValue::Float(value) => Ok(f64::from(*value)),
        SqlValue::Double(value) => Ok(*value),
        value => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Numeric".into(),
            actual: format!("{} in {function}", value.type_name()),
        })),
    }
}

fn make_date(values: &[SqlValue]) -> Result<SqlValue> {
    let date = NaiveDate::from_ymd_opt(
        i32::try_from(integer(&values[0], "make_date")?)
            .map_err(|_| invalid("make_date", "year is out of range"))?,
        u32::try_from(integer(&values[1], "make_date")?)
            .map_err(|_| invalid("make_date", "month is out of range"))?,
        u32::try_from(integer(&values[2], "make_date")?)
            .map_err(|_| invalid("make_date", "day is out of range"))?,
    )
    .ok_or_else(|| invalid("make_date", "invalid calendar date"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    Ok(SqlValue::Date(
        i32::try_from(date.signed_duration_since(epoch).num_days())
            .map_err(|_| invalid("make_date", "date is out of range"))?,
    ))
}

fn make_time(values: &[SqlValue]) -> Result<SqlValue> {
    let hour = u32::try_from(integer(&values[0], "make_time")?)
        .map_err(|_| invalid("make_time", "hour is out of range"))?;
    let minute = u32::try_from(integer(&values[1], "make_time")?)
        .map_err(|_| invalid("make_time", "minute is out of range"))?;
    let seconds = number(&values[2], "make_time")?;
    if !(0.0..60.0).contains(&seconds) {
        return Err(invalid("make_time", "second is out of range"));
    }
    let whole = seconds.trunc() as u32;
    let micros = ((seconds.fract() * 1_000_000.0).round() as u32).min(999_999);
    let time = chrono::NaiveTime::from_hms_micro_opt(hour, minute, whole, micros)
        .ok_or_else(|| invalid("make_time", "invalid time"))?;
    Ok(SqlValue::Time(
        i64::from(time.num_seconds_from_midnight()) * 1_000_000
            + i64::from(time.nanosecond() / 1_000),
    ))
}

fn make_timestamp(values: &[SqlValue]) -> Result<SqlValue> {
    let SqlValue::Date(days) = make_date(&values[..3])? else {
        unreachable!()
    };
    let SqlValue::Time(micros) = make_time(&values[3..])? else {
        unreachable!()
    };
    Ok(SqlValue::Timestamp(
        i64::from(days) * 86_400_000_000 + micros,
    ))
}

fn make_interval(values: &[SqlValue]) -> Result<SqlValue> {
    let arg = |index: usize| {
        values
            .get(index)
            .map(|value| integer(value, "make_interval"))
            .transpose()
            .map(|value| value.unwrap_or(0))
    };
    let years = arg(0)?;
    let months = years
        .checked_mul(12)
        .and_then(|value| value.checked_add(arg(1).ok()?))
        .ok_or_else(|| invalid("make_interval", "months overflow"))?;
    let days = arg(2)?
        .checked_mul(7)
        .and_then(|value| value.checked_add(arg(3).ok()?))
        .ok_or_else(|| invalid("make_interval", "days overflow"))?;
    let hours = arg(4)?;
    let minutes = arg(5)?;
    let seconds = values
        .get(6)
        .map(|value| number(value, "make_interval"))
        .transpose()?
        .unwrap_or(0.0);
    let second_micros = seconds * 1_000_000.0;
    if !second_micros.is_finite()
        || second_micros < i64::MIN as f64
        || second_micros >= -(i64::MIN as f64)
    {
        return Err(invalid("make_interval", "seconds are out of range"));
    }
    let micros = hours
        .checked_mul(3_600_000_000)
        .and_then(|value| value.checked_add(minutes.checked_mul(60_000_000)?))
        .and_then(|value| value.checked_add(second_micros.round() as i64))
        .ok_or_else(|| invalid("make_interval", "time overflow"))?;
    Ok(SqlValue::Interval {
        months: i32::try_from(months).map_err(|_| invalid("make_interval", "months overflow"))?,
        days: i32::try_from(days).map_err(|_| invalid("make_interval", "days overflow"))?,
        micros,
    })
}

fn sqlite_datetime(values: &[SqlValue]) -> Result<SqlValue> {
    let mut value = match &values[0] {
        SqlValue::Timestamp(value) => SqlValue::Timestamp(*value),
        SqlValue::Date(days) => SqlValue::Timestamp(i64::from(*days) * 86_400_000_000),
        SqlValue::Text(text) => super::timestamp::coerce_timestamp(SqlValue::Text(text.clone()))?,
        value => {
            return Err(invalid(
                "datetime",
                format!("unsupported {} input", value.type_name()),
            ));
        }
    };
    for modifier in &values[1..] {
        let modifier = text(Some(modifier), "datetime")?
            .trim()
            .to_ascii_lowercase();
        if modifier == "start of day" {
            let SqlValue::Timestamp(micros) = value else {
                unreachable!()
            };
            value = SqlValue::Timestamp(micros.div_euclid(86_400_000_000) * 86_400_000_000);
        } else {
            let interval = super::timestamp::parse_interval(&modifier)?;
            value = super::binary_op::eval_binary_values(
                &crate::ast::expr::BinaryOp::Add,
                value,
                interval,
            )?;
        }
    }
    Ok(value)
}

fn to_date(values: &[SqlValue]) -> Result<SqlValue> {
    let input = text(values.first(), "to_date")?;
    let format = postgres_format(text(values.get(1), "to_date")?);
    let date = NaiveDate::parse_from_str(input, &format)
        .map_err(|error| invalid("to_date", error.to_string()))?;
    make_date(&[
        SqlValue::Integer(date.year()),
        SqlValue::Integer(date.month() as i32),
        SqlValue::Integer(date.day() as i32),
    ])
}

fn age(values: &[SqlValue]) -> Result<SqlValue> {
    let right = values.get(1).cloned().unwrap_or_else(|| match values[0] {
        SqlValue::Date(_) => SqlValue::Date(
            i32::try_from(current_statement_timestamp().div_euclid(86_400_000_000))
                .expect("current date fits i32"),
        ),
        _ => SqlValue::Timestamp(current_statement_timestamp()),
    });
    super::binary_op::eval_binary_values(&crate::ast::expr::BinaryOp::Sub, values[0].clone(), right)
}

fn invalid(function: &str, reason: impl Into<String>) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::InvalidArgument {
        function: function.into(),
        reason: reason.into(),
    })
}

fn text<'a>(value: Option<&'a SqlValue>, function: &str) -> Result<&'a str> {
    match value {
        Some(SqlValue::Text(value)) => Ok(value),
        Some(value) => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Text".into(),
            actual: value.type_name().into(),
        })),
        None => Err(invalid(function, "missing argument")),
    }
}

fn timestamp(value: Option<&SqlValue>, function: &str) -> Result<i64> {
    match value {
        Some(SqlValue::Timestamp(value)) => Ok(*value),
        Some(value) => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Timestamp".into(),
            actual: value.type_name().into(),
        })),
        None => Err(invalid(function, "missing argument")),
    }
}

fn datetime(micros: i64, function: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| invalid(function, "timestamp is out of range"))
}

fn extract_part(unit: &str, micros: i64, function: &str) -> Result<f64> {
    let value = datetime(micros, function)?;
    let seconds = f64::from(value.second()) + f64::from(value.nanosecond()) / 1_000_000_000.0;
    match unit.to_ascii_lowercase().as_str() {
        "microsecond" | "microseconds" => Ok(seconds * 1_000_000.0),
        "millisecond" | "milliseconds" => Ok(seconds * 1_000.0),
        "second" | "seconds" => Ok(seconds),
        "minute" | "minutes" => Ok(f64::from(value.minute())),
        "hour" | "hours" => Ok(f64::from(value.hour())),
        "day" | "days" => Ok(f64::from(value.day())),
        "dow" => Ok(f64::from(value.weekday().num_days_from_sunday())),
        "isodow" => Ok(f64::from(value.weekday().number_from_monday())),
        "doy" => Ok(f64::from(value.ordinal())),
        "week" => Ok(f64::from(value.iso_week().week())),
        "month" | "months" => Ok(f64::from(value.month())),
        "quarter" => Ok(f64::from((value.month() - 1) / 3 + 1)),
        "year" | "years" => Ok(f64::from(value.year())),
        "epoch" => Ok(micros as f64 / 1_000_000.0),
        _ => Err(invalid(function, format!("unsupported date part '{unit}'"))),
    }
}

fn truncate(unit: &str, micros: i64) -> Result<i64> {
    let value = datetime(micros, "date_trunc")?;
    let date = value.date_naive();
    let result = match unit.to_ascii_lowercase().as_str() {
        "microsecond" | "microseconds" => return Ok(micros),
        "millisecond" | "milliseconds" => return Ok(micros.div_euclid(1_000) * 1_000),
        "second" | "seconds" => return Ok(micros.div_euclid(1_000_000) * 1_000_000),
        "minute" | "minutes" => date.and_hms_opt(value.hour(), value.minute(), 0),
        "hour" | "hours" => date.and_hms_opt(value.hour(), 0, 0),
        "day" | "days" => date.and_hms_opt(0, 0, 0),
        "week" | "weeks" => (date
            - Duration::days(i64::from(value.weekday().num_days_from_monday())))
        .and_hms_opt(0, 0, 0),
        "month" | "months" => NaiveDate::from_ymd_opt(value.year(), value.month(), 1)
            .and_then(|date| date.and_hms_opt(0, 0, 0)),
        "quarter" | "quarters" => {
            let month = (value.month() - 1) / 3 * 3 + 1;
            NaiveDate::from_ymd_opt(value.year(), month, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
        }
        "year" | "years" => {
            NaiveDate::from_ymd_opt(value.year(), 1, 1).and_then(|date| date.and_hms_opt(0, 0, 0))
        }
        _ => return Err(invalid("date_trunc", format!("unsupported unit '{unit}'"))),
    }
    .ok_or_else(|| invalid("date_trunc", "timestamp is out of range"))?;
    Ok(result.and_utc().timestamp_micros())
}

fn postgres_format(format: &str) -> String {
    format
        .replace("HH24", "%H")
        .replace("YYYY", "%Y")
        .replace("MI", "%M")
        .replace("SS", "%S")
        .replace("US", "%6f")
        .replace("MM", "%m")
        .replace("DD", "%d")
}

fn validate_format(format: &str, function: &str) -> Result<()> {
    if StrftimeItems::new(format).any(|item| matches!(item, Item::Error)) {
        Err(invalid(function, "invalid format string"))
    } else {
        Ok(())
    }
}

fn format_datetime(micros: i64, format: &str, function: &str) -> Result<String> {
    validate_format(format, function)?;
    Ok(datetime(micros, function)?.format(format).to_string())
}

fn to_timestamp(values: &[SqlValue]) -> Result<SqlValue> {
    if values.len() == 2 {
        let input = text(values.first(), "to_timestamp")?;
        let format = postgres_format(text(values.get(1), "to_timestamp")?);
        validate_format(&format, "to_timestamp")?;
        let parsed = NaiveDateTime::parse_from_str(input, &format)
            .map_err(|error| invalid("to_timestamp", error.to_string()))?;
        return Ok(SqlValue::Timestamp(parsed.and_utc().timestamp_micros()));
    }
    match values.first() {
        Some(SqlValue::Text(value)) => {
            super::timestamp::coerce_timestamp(SqlValue::Text(value.clone()))
        }
        Some(SqlValue::Integer(value)) => seconds_to_micros(i64::from(*value)),
        Some(SqlValue::BigInt(value)) => seconds_to_micros(*value),
        Some(SqlValue::Float(value)) => fractional_seconds_to_micros(f64::from(*value)),
        Some(SqlValue::Double(value)) => fractional_seconds_to_micros(*value),
        Some(value) => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Numeric or Text".into(),
            actual: value.type_name().into(),
        })),
        None => Err(invalid("to_timestamp", "missing argument")),
    }
}

fn seconds_to_micros(seconds: i64) -> Result<SqlValue> {
    seconds
        .checked_mul(1_000_000)
        .map(SqlValue::Timestamp)
        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))
}

fn fractional_seconds_to_micros(seconds: f64) -> Result<SqlValue> {
    let micros = seconds * 1_000_000.0;
    if !micros.is_finite() || micros < i64::MIN as f64 || micros >= -(i64::MIN as f64) {
        return Err(ExecutorError::Evaluation(EvaluationError::Overflow));
    }
    Ok(SqlValue::Timestamp(micros.round() as i64))
}
