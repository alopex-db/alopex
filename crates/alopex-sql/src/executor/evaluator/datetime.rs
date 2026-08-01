use crate::executor::Result;
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::EvalContext;
use super::context::current_statement_timestamp;

/// Evaluate NOW() using the UTC timestamp fixed when the statement began.
pub(crate) fn eval_now_values(_values: &[SqlValue]) -> Result<SqlValue> {
    Ok(SqlValue::Timestamp(current_statement_timestamp()))
}

/// Lazy dispatch keeps NOW() tied to the context that evaluates the expression.
pub(crate) fn eval_now_lazy(_args: &[TypedExpr], ctx: &EvalContext<'_>) -> Result<SqlValue> {
    Ok(SqlValue::Timestamp(ctx.statement_timestamp()))
}
