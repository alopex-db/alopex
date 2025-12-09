use crate::executor::Result;
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::evaluate;

pub fn eval_is_null(
    expr: &TypedExpr,
    negated: bool,
    ctx: &super::EvalContext<'_>,
) -> Result<SqlValue> {
    let value = evaluate(expr, ctx)?;
    let is_null = value.is_null();
    Ok(SqlValue::Boolean(if negated { !is_null } else { is_null }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;
    use crate::ast::expr::Literal;
    use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
    use crate::planner::types::ResolvedType;

    fn ctx() -> super::super::EvalContext<'static> {
        super::super::EvalContext::new(&[])
    }

    #[test]
    fn is_null_true() {
        let expr = TypedExpr {
            kind: TypedExprKind::Literal(Literal::Null),
            resolved_type: ResolvedType::Null,
            span: Span::default(),
        };
        let v = eval_is_null(&expr, false, &ctx()).unwrap();
        assert_eq!(v, SqlValue::Boolean(true));
    }

    #[test]
    fn is_not_null_false() {
        let expr = TypedExpr {
            kind: TypedExprKind::Literal(Literal::Boolean(true)),
            resolved_type: ResolvedType::Boolean,
            span: Span::default(),
        };
        let v = eval_is_null(&expr, true, &ctx()).unwrap();
        assert_eq!(v, SqlValue::Boolean(true));
    }
}
