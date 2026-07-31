use crate::ast::expr::BinaryOp;
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

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
    }
}

fn add(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if left.is_null() || right.is_null() {
        return Ok(SqlValue::Null);
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
        (Integer(a), Float(b)) => Some(NumericOperands::Float(*a as f32, *b)),
        (Float(a), Integer(b)) => Some(NumericOperands::Float(*a, *b as f32)),
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
}
