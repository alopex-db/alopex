//! Lazy SQL CASE expression evaluation.

use crate::ast::BinaryOp;
use crate::executor::Result;
use crate::planner::typed_expr::{TypedCaseWhen, TypedExpr};
use crate::storage::SqlValue;

use super::{EvalContext, binary_op, evaluate};

pub fn evaluate_case(
    operand: Option<&TypedExpr>,
    branches: &[TypedCaseWhen],
    else_expr: Option<&TypedExpr>,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let operand = operand.map(|expr| evaluate(expr, ctx)).transpose()?;
    for branch in branches {
        let selected = match &operand {
            Some(operand) => matches!(
                binary_op::eval_binary_values(
                    &BinaryOp::Eq,
                    operand.clone(),
                    evaluate(&branch.when, ctx)?
                )?,
                SqlValue::Boolean(true)
            ),
            None => matches!(evaluate(&branch.when, ctx)?, SqlValue::Boolean(true)),
        };
        if selected {
            return evaluate(&branch.then, ctx);
        }
    }
    else_expr
        .map(|expr| evaluate(expr, ctx))
        .transpose()
        .map(|value| value.unwrap_or(SqlValue::Null))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Literal, Span};
    use crate::planner::ResolvedType;
    use crate::planner::typed_expr::TypedExprKind;

    fn expr(kind: TypedExprKind, resolved_type: ResolvedType) -> TypedExpr {
        TypedExpr::new(kind, resolved_type, Span::default())
    }

    fn integer(value: i32) -> TypedExpr {
        expr(
            TypedExprKind::Literal(Literal::Number(value.to_string())),
            ResolvedType::Integer,
        )
    }

    fn boolean(value: bool) -> TypedExpr {
        expr(
            TypedExprKind::Literal(Literal::Boolean(value)),
            ResolvedType::Boolean,
        )
    }

    fn division_by_zero() -> TypedExpr {
        expr(
            TypedExprKind::BinaryOp {
                left: Box::new(integer(1)),
                op: BinaryOp::Div,
                right: Box::new(integer(0)),
            },
            ResolvedType::Integer,
        )
    }

    #[test]
    fn searched_case_evaluates_only_the_selected_branch() {
        let case = expr(
            TypedExprKind::Case {
                operand: None,
                branches: vec![
                    TypedCaseWhen {
                        when: boolean(false),
                        then: division_by_zero(),
                    },
                    TypedCaseWhen {
                        when: boolean(true),
                        then: integer(7),
                    },
                ],
                else_expr: Some(Box::new(division_by_zero())),
            },
            ResolvedType::Integer,
        );

        assert_eq!(
            crate::executor::evaluator::evaluate(&case, &EvalContext::new(&[])).unwrap(),
            SqlValue::Integer(7)
        );
    }

    #[test]
    fn simple_case_keeps_sql_null_comparison_semantics() {
        let null = expr(TypedExprKind::Literal(Literal::Null), ResolvedType::Null);
        let case = expr(
            TypedExprKind::Case {
                operand: Some(Box::new(null.clone())),
                branches: vec![TypedCaseWhen {
                    when: null,
                    then: integer(1),
                }],
                else_expr: Some(Box::new(integer(9))),
            },
            ResolvedType::Integer,
        );

        assert_eq!(
            crate::executor::evaluator::evaluate(&case, &EvalContext::new(&[])).unwrap(),
            SqlValue::Integer(9)
        );
    }

    #[test]
    fn case_without_else_returns_null() {
        let case = expr(
            TypedExprKind::Case {
                operand: None,
                branches: vec![TypedCaseWhen {
                    when: boolean(false),
                    then: integer(1),
                }],
                else_expr: None,
            },
            ResolvedType::Null,
        );

        assert_eq!(
            crate::executor::evaluator::evaluate(&case, &EvalContext::new(&[])).unwrap(),
            SqlValue::Null
        );
    }
}
