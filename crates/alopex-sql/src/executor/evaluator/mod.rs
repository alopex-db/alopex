//! Expression evaluator for typed expressions.
//!
//! Provides a lightweight, zero-allocation evaluator over typed expressions
//! emitted by the planner. The evaluator operates on a borrowed row slice
//! via [`EvalContext`] and returns [`SqlValue`] results or [`ExecutorError`].

mod binary_op;
mod column_ref;
mod context;
mod is_null;
mod literal;
mod unary_op;

pub use context::EvalContext;

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
        TypedExprKind::IsNull { expr, negated } => is_null::eval_is_null(expr, *negated, ctx),
        TypedExprKind::VectorLiteral(values) => {
            Ok(SqlValue::Vector(values.iter().map(|v| *v as f32).collect()))
        }
        // Unsupported expressions return a clear error message.
        other => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedExpression(format!("{other:?}")),
        )),
    }
}
