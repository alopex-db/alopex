use crate::ast::expr::UnaryOp;
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::evaluate;

pub fn eval_unary_op(
    op: &UnaryOp,
    operand: &TypedExpr,
    ctx: &super::EvalContext<'_>,
) -> Result<SqlValue> {
    let value = evaluate(operand, ctx)?;
    match op {
        UnaryOp::Not => eval_not(value),
        UnaryOp::Minus => eval_minus(value),
    }
}

fn eval_not(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Boolean(b) => Ok(SqlValue::Boolean(!b)),
        other => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Boolean".into(),
            actual: other.type_name().into(),
        })),
    }
}

fn eval_minus(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(v) => v
            .checked_neg()
            .map(SqlValue::Integer)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        SqlValue::BigInt(v) => v
            .checked_neg()
            .map(SqlValue::BigInt)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::Overflow)),
        SqlValue::Float(v) => Ok(SqlValue::Float(-v)),
        SqlValue::Double(v) => Ok(SqlValue::Double(-v)),
        other => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Numeric".into(),
            actual: other.type_name().into(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_null_passthrough() {
        assert_eq!(eval_not(SqlValue::Null).unwrap(), SqlValue::Null);
    }

    #[test]
    fn minus_overflow() {
        let err = eval_minus(SqlValue::Integer(i32::MIN)).unwrap_err();
        assert!(matches!(
            err,
            ExecutorError::Evaluation(EvaluationError::Overflow)
        ));
    }
}
