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
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a
            .checked_add(b)
            .map(SqlValue::Integer)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        (SqlValue::BigInt(a), SqlValue::BigInt(b)) => a
            .checked_add(b)
            .map(SqlValue::BigInt)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        (SqlValue::Float(a), SqlValue::Float(b)) => Ok(SqlValue::Float(a + b)),
        (SqlValue::Double(a), SqlValue::Double(b)) => Ok(SqlValue::Double(a + b)),
        (l, r) => type_mismatch("Numeric", &l, &r),
    }
}

fn sub(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a
            .checked_sub(b)
            .map(SqlValue::Integer)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        (SqlValue::BigInt(a), SqlValue::BigInt(b)) => a
            .checked_sub(b)
            .map(SqlValue::BigInt)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        (SqlValue::Float(a), SqlValue::Float(b)) => Ok(SqlValue::Float(a - b)),
        (SqlValue::Double(a), SqlValue::Double(b)) => Ok(SqlValue::Double(a - b)),
        (l, r) => type_mismatch("Numeric", &l, &r),
    }
}

fn mul(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a
            .checked_mul(b)
            .map(SqlValue::Integer)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        (SqlValue::BigInt(a), SqlValue::BigInt(b)) => a
            .checked_mul(b)
            .map(SqlValue::BigInt)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        (SqlValue::Float(a), SqlValue::Float(b)) => Ok(SqlValue::Float(a * b)),
        (SqlValue::Double(a), SqlValue::Double(b)) => Ok(SqlValue::Double(a * b)),
        (l, r) => type_mismatch("Numeric", &l, &r),
    }
}

fn div(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (_, SqlValue::Integer(0)) => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        (_, SqlValue::BigInt(0)) => Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero)),
        (_, SqlValue::Float(f)) if f == 0.0 => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        (_, SqlValue::Double(d)) if d == 0.0 => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(SqlValue::Integer(a / b)),
        (SqlValue::BigInt(a), SqlValue::BigInt(b)) => Ok(SqlValue::BigInt(a / b)),
        (SqlValue::Float(a), SqlValue::Float(b)) => Ok(SqlValue::Float(a / b)),
        (SqlValue::Double(a), SqlValue::Double(b)) => Ok(SqlValue::Double(a / b)),
        (l, r) => type_mismatch("Numeric", &l, &r),
    }
}

fn r#mod(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (_, SqlValue::Integer(0) | SqlValue::BigInt(0)) => {
            Err(ExecutorError::Evaluation(EvaluationError::DivisionByZero))
        }
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(SqlValue::Integer(a % b)),
        (SqlValue::BigInt(a), SqlValue::BigInt(b)) => Ok(SqlValue::BigInt(a % b)),
        (l, r) => type_mismatch("Integer/BigInt", &l, &r),
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
    let cmp = left.partial_cmp(&right).ok_or_else(|| {
        ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Comparable".into(),
            actual: format!("{:?} vs {:?}", left.type_name(), right.type_name()),
        })
    })?;

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
