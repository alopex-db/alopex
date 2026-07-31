//! Expression evaluator for typed expressions.
//!
//! Provides a lightweight, zero-allocation evaluator over typed expressions
//! emitted by the planner. The evaluator operates on a borrowed row slice
//! via [`EvalContext`] and returns [`SqlValue`] results or [`ExecutorError`].

pub(crate) mod binary_op;
mod column_ref;
pub(crate) mod conditional;
mod context;
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
        // Unsupported expressions return a clear error message.
        other => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedExpression(format!("{other:?}")),
        )),
    }
}
