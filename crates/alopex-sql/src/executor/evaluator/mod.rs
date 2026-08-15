//! Expression evaluator for typed expressions.
//!
//! Provides a lightweight, zero-allocation evaluator over typed expressions
//! emitted by the planner. The evaluator operates on a borrowed row slice
//! via [`EvalContext`] and returns [`SqlValue`] results or [`ExecutorError`].

pub(crate) mod binary_op;
mod case_expr;
mod column_ref;
pub(crate) mod conditional;
mod context;
pub(crate) mod datetime;
mod function_call;
pub(crate) mod hash;
mod is_null;
mod literal;
pub(crate) mod numeric;
pub(crate) mod pattern;
pub mod registry;
pub(crate) mod string;
mod timestamp;
pub(crate) mod type_fn;
mod unary_op;
pub mod vector_ops;

pub use vector_ops::{VectorError, VectorMetric, vector_distance, vector_similarity};

pub use context::EvalContext;
pub(crate) use context::begin_statement;
pub(crate) use timestamp::coerce_value;

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::planner::typed_expr::TypedExprKind;
use crate::storage::SqlValue;

/// Evaluate a typed expression against the provided evaluation context.
pub fn evaluate(expr: &TypedExpr, ctx: &EvalContext<'_>) -> Result<SqlValue> {
    match &expr.kind {
        TypedExprKind::Literal(lit) => literal::eval_literal(lit, &expr.resolved_type),
        TypedExprKind::ColumnRef { column_index, .. } => {
            column_ref::eval_column_ref(*column_index, ctx)
        }
        TypedExprKind::BinaryOp { left, op, right } => {
            binary_op::eval_binary_op(op, left, right, ctx)
        }
        TypedExprKind::UnaryOp { op, operand } => unary_op::eval_unary_op(op, operand, ctx),
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => case_expr::evaluate_case(operand.as_deref(), branches, else_expr.as_deref(), ctx),
        TypedExprKind::IsNull { expr, negated } => is_null::eval_is_null(expr, *negated, ctx),
        TypedExprKind::VectorLiteral(values) => {
            Ok(SqlValue::Vector(values.iter().map(|v| *v as f32).collect()))
        }
        TypedExprKind::FunctionCall {
            name,
            args,
            distinct,
            star,
        } => function_call::evaluate_function_call(name, args, *distinct, *star, ctx),
        TypedExprKind::Cast { expr, target_type } => {
            timestamp::evaluate_cast(expr, target_type, ctx)
        }
        TypedExprKind::Like {
            expr,
            pattern,
            escape,
            negated,
            kind,
        } => pattern::evaluate_pattern(expr, pattern, escape.as_deref(), *negated, *kind, ctx),
        TypedExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => evaluate_between(expr, low, high, *negated, ctx),
        TypedExprKind::InList {
            expr,
            list,
            negated,
        } => evaluate_in_list(expr, list, *negated, ctx),
        // Unsupported expressions return a clear error message.
        other => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedExpression(format!("{other:?}")),
        )),
    }
}

fn evaluate_between(
    expr: &TypedExpr,
    low: &TypedExpr,
    high: &TypedExpr,
    negated: bool,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let value = evaluate(expr, ctx)?;
    let lower = binary_op::eval_binary_values(
        &crate::ast::expr::BinaryOp::GtEq,
        value.clone(),
        evaluate(low, ctx)?,
    )?;
    let upper = binary_op::eval_binary_values(
        &crate::ast::expr::BinaryOp::LtEq,
        value,
        evaluate(high, ctx)?,
    )?;
    let result = binary_op::eval_binary_values(&crate::ast::expr::BinaryOp::And, lower, upper)?;
    negate_predicate(result, negated)
}

fn evaluate_in_list(
    expr: &TypedExpr,
    list: &[TypedExpr],
    negated: bool,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let value = evaluate(expr, ctx)?;
    let mut unknown = false;

    for item in list {
        match binary_op::eval_binary_values(
            &crate::ast::expr::BinaryOp::Eq,
            value.clone(),
            evaluate(item, ctx)?,
        )? {
            SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(!negated)),
            SqlValue::Boolean(false) => {}
            SqlValue::Null => unknown = true,
            other => {
                return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "Boolean".into(),
                    actual: other.type_name().into(),
                }));
            }
        }
    }

    if unknown {
        Ok(SqlValue::Null)
    } else {
        Ok(SqlValue::Boolean(negated))
    }
}

fn negate_predicate(value: SqlValue, negated: bool) -> Result<SqlValue> {
    if !negated {
        return Ok(value);
    }
    match value {
        SqlValue::Boolean(value) => Ok(SqlValue::Boolean(!value)),
        SqlValue::Null => Ok(SqlValue::Null),
        other => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Boolean".into(),
            actual: other.type_name().into(),
        })),
    }
}
