use crate::ast::expr::BinaryOp;
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;
use chrono::{Duration, Months, NaiveDate};

use super::evaluate;

pub fn eval_binary_op(
    op: &BinaryOp,
    left: &TypedExpr,
    right: &TypedExpr,
    ctx: &super::EvalContext<'_>,
) -> Result<SqlValue> {
    let l = evaluate(left, ctx)?;
    let r = evaluate(right, ctx)?;
    eval_binary_values(op, l, r)
}

pub(crate) fn eval_binary_values(op: &BinaryOp, l: SqlValue, r: SqlValue) -> Result<SqlValue> {
    match op {
        BinaryOp::Add => add(l, r),
        BinaryOp::Sub => sub(l, r),
        BinaryOp::Mul => mul(l, r),
        BinaryOp::Div => div(l, r),
        BinaryOp::Mod => r#mod(l, r),
        BinaryOp::Eq => compare(l, r, OrderingKind::Eq),
        BinaryOp::Neq => compare(l, r, OrderingKind::Neq),
        BinaryOp::Lt => compare(l, r, OrderingKind::Lt),
        BinaryOp::Gt => compare(l, r, OrderingKind::Gt),
        BinaryOp::LtEq => compare(l, r, OrderingKind::Le),
        BinaryOp::GtEq => compare(l, r, OrderingKind::Ge),
        BinaryOp::And => logical_and(l, r),
        BinaryOp::Or => logical_or(l, r),
        BinaryOp::StringConcat => string_concat(l, r),
        BinaryOp::BitAnd => bitwise(l, r, |a, b| a & b),
        BinaryOp::BitOr => bitwise(l, r, |a, b| a | b),
        BinaryOp::BitXor => bitwise(l, r, |a, b| a ^ b),
        BinaryOp::ShiftLeft => shift(l, r, true),
        BinaryOp::ShiftRight => shift(l, r, false),
    }
}

fn bitwise(left: SqlValue, right: SqlValue, op: fn(i64, i64) -> i64) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }
    match (&left, &right) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => {
            Ok(SqlValue::Integer(op(i64::from(*a), i64::from(*b)) as i32))
        }
        (SqlValue::Integer(a), SqlValue::BigInt(b)) => Ok(SqlValue::BigInt(op(i64::from(*a), *b))),
        (SqlValue::BigInt(a), SqlValue::Integer(b)) => Ok(SqlValue::BigInt(op(*a, i64::from(*b)))),
        (SqlValue::BigInt(a), SqlValue::BigInt(b)) => Ok(SqlValue::BigInt(op(*a, *b))),
        _ => type_mismatch("Integer/BigInt", &left, &right),
    }
}

fn shift(left: SqlValue, right: SqlValue, left_shift: bool) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }
    let (amount, right_wide) = match right {
        SqlValue::Integer(value) => (i64::from(value), false),
        SqlValue::BigInt(value) => (value, true),
        other => return type_mismatch("Integer/BigInt", &left, &other),
    };
    let wide = matches!(left, SqlValue::BigInt(_)) || right_wide;
    let width = if wide { 64 } else { 32 };
    if !(0..width).contains(&amount) {
        return Err(ExecutorError::Evaluation(EvaluationError::Overflow));
    }
    let amount = amount as u32;
    match (left, wide, left_shift) {
        (SqlValue::Integer(value), false, true) => {
            let shifted = i128::from(value) * (1_i128 << amount);
            i32::try_from(shifted)
                .map(SqlValue::Integer)
                .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))
        }
        (SqlValue::Integer(value), true, true) => {
            let shifted = i128::from(value) * (1_i128 << amount);
            i64::try_from(shifted)
                .map(SqlValue::BigInt)
                .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))
        }
        (SqlValue::BigInt(value), _, true) => {
            let shifted = i128::from(value) * (1_i128 << amount);
            i64::try_from(shifted)
                .map(SqlValue::BigInt)
                .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))
        }
        (SqlValue::Integer(value), false, false) => Ok(SqlValue::Integer(value >> amount)),
        (SqlValue::Integer(value), true, false) => Ok(SqlValue::BigInt(i64::from(value) >> amount)),
        (SqlValue::BigInt(value), _, false) => Ok(SqlValue::BigInt(value >> amount)),
        (other, _, _) => type_mismatch("Integer/BigInt", &other, &SqlValue::Integer(amount as i32)),
    }
}

fn add(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }
    if let Some(value) = temporal_add(&left, &right, false)? {
        return Ok(value);
    }
    match numeric_operands(&left, &right) {
        Some(NumericOperands::Integer(a, b)) => a
            .checked_add(b)
            .map(SqlValue::Integer)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::BigInt(a, b)) => a
            .checked_add(b)
            .map(SqlValue::BigInt)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::Float(a, b)) => Ok(SqlValue::Float(a + b)),
        Some(NumericOperands::Double(a, b)) => Ok(SqlValue::Double(a + b)),
        None => type_mismatch("Numeric", &left, &right),
    }
}

fn sub(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }
    if let Some(value) = temporal_add(&left, &right, true)? {
        return Ok(value);
    }
    match numeric_operands(&left, &right) {
        Some(NumericOperands::Integer(a, b)) => a
            .checked_sub(b)
            .map(SqlValue::Integer)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::BigInt(a, b)) => a
            .checked_sub(b)
            .map(SqlValue::BigInt)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::Float(a, b)) => Ok(SqlValue::Float(a - b)),
        Some(NumericOperands::Double(a, b)) => Ok(SqlValue::Double(a - b)),
        None => type_mismatch("Numeric", &left, &right),
    }
}

fn temporal_add(left: &SqlValue, right: &SqlValue, subtract: bool) -> Result<Option<SqlValue>> {
    let negate = |months: i32, days: i32, micros: i64| {
        if subtract {
            months
                .checked_neg()
                .zip(days.checked_neg())
                .zip(micros.checked_neg())
                .map(|((m, d), u)| (m, d, u))
        } else {
            Some((months, days, micros))
        }
    };
    let interval = match right {
        SqlValue::Interval {
            months,
            days,
            micros,
        } => negate(*months, *days, *micros),
        _ => None,
    };
    if let Some((months, days, micros)) = interval {
        return match left {
            SqlValue::Date(value) => add_date_interval(*value, months, days, micros).map(Some),
            SqlValue::Timestamp(value) => {
                add_timestamp_interval(*value, months, days, micros).map(Some)
            }
            SqlValue::Time(value) if months == 0 => {
                add_time_interval(*value, days, micros).map(Some)
            }
            SqlValue::Interval {
                months: lm,
                days: ld,
                micros: lu,
            } => Ok(Some(SqlValue::Interval {
                months: lm
                    .checked_add(months)
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                days: ld
                    .checked_add(days)
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
                micros: lu
                    .checked_add(micros)
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
            })),
            _ => Ok(None),
        };
    }
    if subtract {
        return match (left, right) {
            (SqlValue::Date(a), SqlValue::Date(b)) => Ok(Some(SqlValue::Interval {
                months: 0,
                days: a - b,
                micros: 0,
            })),
            (SqlValue::Timestamp(a), SqlValue::Timestamp(b))
            | (SqlValue::Time(a), SqlValue::Time(b)) => Ok(Some(SqlValue::Interval {
                months: 0,
                days: 0,
                micros: a
                    .checked_sub(*b)
                    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?,
            })),
            _ => Ok(None),
        };
    }
    if let SqlValue::Interval {
        months,
        days,
        micros,
    } = left
    {
        return match right {
            SqlValue::Date(value) => add_date_interval(*value, *months, *days, *micros).map(Some),
            SqlValue::Timestamp(value) => {
                add_timestamp_interval(*value, *months, *days, *micros).map(Some)
            }
            SqlValue::Time(value) if *months == 0 => {
                add_time_interval(*value, *days, *micros).map(Some)
            }
            _ => Ok(None),
        };
    }
    Ok(None)
}

fn add_time_interval(value: i64, days: i32, micros: i64) -> Result<SqlValue> {
    let delta = i64::from(days)
        .checked_mul(86_400_000_000)
        .and_then(|days| days.checked_add(micros))
        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
    value
        .checked_add(delta)
        .map(|value| SqlValue::Time(value.rem_euclid(86_400_000_000)))
        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))
}

fn add_date_interval(value: i32, months: i32, days: i32, micros: i64) -> Result<SqlValue> {
    if micros % 86_400_000_000 != 0 {
        return type_mismatch(
            "DATE with whole-day INTERVAL",
            &SqlValue::Date(value),
            &SqlValue::Interval {
                months,
                days,
                micros,
            },
        );
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    let date = epoch
        .checked_add_signed(Duration::days(i64::from(value)))
        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
    let date = if months >= 0 {
        date.checked_add_months(Months::new(months as u32))
    } else {
        date.checked_sub_months(Months::new(months.unsigned_abs()))
    }
    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
    let total_days = i64::from(days) + micros / 86_400_000_000;
    let date = date
        .checked_add_signed(Duration::days(total_days))
        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
    Ok(SqlValue::Date(
        i32::try_from(date.signed_duration_since(epoch).num_days())
            .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))?,
    ))
}

fn add_timestamp_interval(value: i64, months: i32, days: i32, micros: i64) -> Result<SqlValue> {
    let datetime = chrono::DateTime::from_timestamp_micros(value)
        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?
        .naive_utc();
    let datetime = if months >= 0 {
        datetime.checked_add_months(Months::new(months as u32))
    } else {
        datetime.checked_sub_months(Months::new(months.unsigned_abs()))
    }
    .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
    let datetime = datetime
        .checked_add_signed(Duration::days(i64::from(days)) + Duration::microseconds(micros))
        .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
    Ok(SqlValue::Timestamp(datetime.and_utc().timestamp_micros()))
}

fn mul(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }
    match numeric_operands(&left, &right) {
        Some(NumericOperands::Integer(a, b)) => a
            .checked_mul(b)
            .map(SqlValue::Integer)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::BigInt(a, b)) => a
            .checked_mul(b)
            .map(SqlValue::BigInt)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::Float(a, b)) => Ok(SqlValue::Float(a * b)),
        Some(NumericOperands::Double(a, b)) => Ok(SqlValue::Double(a * b)),
        None => type_mismatch("Numeric", &left, &right),
    }
}

fn div(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }
    match numeric_operands(&left, &right) {
        Some(NumericOperands::Integer(_, 0)) | Some(NumericOperands::BigInt(_, 0)) => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        Some(NumericOperands::Float(_, 0.0)) => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        Some(NumericOperands::Double(_, 0.0)) => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        Some(NumericOperands::Integer(a, b)) => a
            .checked_div(b)
            .map(SqlValue::Integer)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::BigInt(a, b)) => a
            .checked_div(b)
            .map(SqlValue::BigInt)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::Float(a, b)) => Ok(SqlValue::Float(a / b)),
        Some(NumericOperands::Double(a, b)) => Ok(SqlValue::Double(a / b)),
        None => type_mismatch("Numeric", &left, &right),
    }
}

fn r#mod(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }
    match numeric_operands(&left, &right) {
        Some(NumericOperands::Integer(_, 0)) | Some(NumericOperands::BigInt(_, 0)) => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        Some(NumericOperands::Integer(a, b)) => a
            .checked_rem(b)
            .map(SqlValue::Integer)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::BigInt(a, b)) => a
            .checked_rem(b)
            .map(SqlValue::BigInt)
            .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow)),
        Some(NumericOperands::Float(..) | NumericOperands::Double(..)) | None => {
            type_mismatch("Integer/BigInt", &left, &right)
        }
    }
}

/// Runtime operands promoted with the same numeric hierarchy used by the planner.
///
/// INTEGER and BIGINT retain integral arithmetic where possible; FLOAT with
/// BIGINT promotes to DOUBLE because an i64 cannot be represented exactly by f32.
enum NumericOperands {
    Integer(i32, i32),
    BigInt(i64, i64),
    Float(f32, f32),
    Double(f64, f64),
}

fn numeric_operands(left: &SqlValue, right: &SqlValue) -> Option<NumericOperands> {
    use SqlValue::*;

    match (left, right) {
        (Integer(a), Integer(b)) => Some(NumericOperands::Integer(*a, *b)),
        (Integer(a), BigInt(b)) => Some(NumericOperands::BigInt(i64::from(*a), *b)),
        (BigInt(a), Integer(b)) => Some(NumericOperands::BigInt(*a, i64::from(*b))),
        (BigInt(a), BigInt(b)) => Some(NumericOperands::BigInt(*a, *b)),
        // f32 has 24 bits of mantissa and cannot hold the whole i32 range, so an
        // INTEGER mixed with FLOAT widens to DOUBLE instead of losing magnitude.
        (Integer(a), Float(b)) => Some(NumericOperands::Double(f64::from(*a), f64::from(*b))),
        (Float(a), Integer(b)) => Some(NumericOperands::Double(f64::from(*a), f64::from(*b))),
        (Float(a), Float(b)) => Some(NumericOperands::Float(*a, *b)),
        (BigInt(a), Float(b)) => Some(NumericOperands::Double(*a as f64, f64::from(*b))),
        (Float(a), BigInt(b)) => Some(NumericOperands::Double(f64::from(*a), *b as f64)),
        (Integer(a), Double(b)) => Some(NumericOperands::Double(f64::from(*a), *b)),
        (Double(a), Integer(b)) => Some(NumericOperands::Double(*a, f64::from(*b))),
        (BigInt(a), Double(b)) => Some(NumericOperands::Double(*a as f64, *b)),
        (Double(a), BigInt(b)) => Some(NumericOperands::Double(*a, *b as f64)),
        (Float(a), Double(b)) => Some(NumericOperands::Double(f64::from(*a), *b)),
        (Double(a), Float(b)) => Some(NumericOperands::Double(*a, f64::from(*b))),
        (Double(a), Double(b)) => Some(NumericOperands::Double(*a, *b)),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum OrderingKind {
    Eq,
    Neq,
    Lt,
    Gt,
    Le,
    Ge,
}

fn compare(left: SqlValue, right: SqlValue, kind: OrderingKind) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
    }

    use OrderingKind::*;
    use std::cmp::Ordering;
    if let (Some(lhs), Some(rhs)) = (numeric_as_f64(&left), numeric_as_f64(&right)) {
        let cmp = lhs.partial_cmp(&rhs).ok_or(ExecutorError::Evaluation(
            EvaluationError::TypeMismatch {
                expected: "Comparable".into(),
                actual: format!("{:?} vs {:?}", left.type_name(), right.type_name()),
            },
        ))?;
        let result = match kind {
            Eq => cmp == Ordering::Equal,
            Neq => cmp != Ordering::Equal,
            Lt => cmp == Ordering::Less,
            Gt => cmp == Ordering::Greater,
            Le => cmp != Ordering::Greater,
            Ge => cmp != Ordering::Less,
        };
        return Ok(SqlValue::Boolean(result));
    }
    let cmp = left.partial_cmp(&right).ok_or(ExecutorError::Evaluation(
        EvaluationError::TypeMismatch {
            expected: "Comparable".into(),
            actual: format!("{:?} vs {:?}", left.type_name(), right.type_name()),
        },
    ))?;

    let result = match kind {
        Eq => cmp == Ordering::Equal,
        Neq => cmp != Ordering::Equal,
        Lt => cmp == Ordering::Less,
        Gt => cmp == Ordering::Greater,
        Le => cmp != Ordering::Greater,
        Ge => cmp != Ordering::Less,
    };
    Ok(SqlValue::Boolean(result))
}

fn numeric_as_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Integer(v) => Some(*v as f64),
        SqlValue::BigInt(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v as f64),
        SqlValue::Double(v) => Some(*v),
        _ => None,
    }
}

fn logical_and(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    match (left, right) {
        (SqlValue::Boolean(false), _) => Ok(SqlValue::Boolean(false)),
        (SqlValue::Boolean(true), SqlValue::Boolean(rb)) => Ok(SqlValue::Boolean(rb)),
        (SqlValue::Boolean(true), SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Null, SqlValue::Boolean(false)) => Ok(SqlValue::Boolean(false)),
        (SqlValue::Null, SqlValue::Boolean(true)) => Ok(SqlValue::Null),
        (SqlValue::Null, SqlValue::Null) => Ok(SqlValue::Null),
        (l, r) => type_mismatch("Boolean", &l, &r),
    }
}

fn logical_or(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    match (left, right) {
        (SqlValue::Boolean(true), _) => Ok(SqlValue::Boolean(true)),
        (SqlValue::Boolean(false), SqlValue::Boolean(rb)) => Ok(SqlValue::Boolean(rb)),
        (SqlValue::Boolean(false), SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Null, SqlValue::Boolean(true)) => Ok(SqlValue::Boolean(true)),
        (SqlValue::Null, SqlValue::Boolean(false)) => Ok(SqlValue::Null),
        (SqlValue::Null, SqlValue::Null) => Ok(SqlValue::Null),
        (l, r) => type_mismatch("Boolean", &l, &r),
    }
}

fn string_concat(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Text(a), SqlValue::Text(b)) => Ok(SqlValue::Text(format!("{a}{b}"))),
        (l, r) => type_mismatch("Text", &l, &r),
    }
}

fn type_mismatch<T>(expected: &str, left: &SqlValue, right: &SqlValue) -> Result<T> {
    Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
        expected: expected.into(),
        actual: format!("{} vs {}", left.type_name(), right.type_name()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_null_propagates() {
        assert_eq!(
            add(SqlValue::Null, SqlValue::Integer(1)).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn logical_and_null_truth_table() {
        assert_eq!(
            logical_and(SqlValue::Null, SqlValue::Boolean(false)).unwrap(),
            SqlValue::Boolean(false)
        );
        assert_eq!(
            logical_and(SqlValue::Null, SqlValue::Boolean(true)).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn shifts_reject_invalid_counts_and_left_overflow() {
        for amount in [-1, 32] {
            assert!(matches!(
                shift(SqlValue::Integer(1), SqlValue::Integer(amount), true),
                Err(ExecutorError::Evaluation(EvaluationError::Overflow))
            ));
        }
        assert!(matches!(
            shift(SqlValue::Integer(i32::MAX), SqlValue::Integer(1), true),
            Err(ExecutorError::Evaluation(EvaluationError::Overflow))
        ));
    }
}
