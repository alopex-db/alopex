use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::{TypedCaseWhen, TypedExpr};
use crate::storage::SqlValue;

use super::{EvalContext, evaluate};

pub(super) fn evaluate_case(
    operand: Option<&TypedExpr>,
    branches: &[TypedCaseWhen],
    else_expr: Option<&TypedExpr>,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let operand = operand.map(|expr| evaluate(expr, ctx)).transpose()?;

    for branch in branches {
        let matched = if let Some(operand) = &operand {
            super::binary_op::eval_binary_values(
                &crate::ast::expr::BinaryOp::Eq,
                operand.clone(),
                evaluate(&branch.when, ctx)?,
            )?
        } else {
            evaluate(&branch.when, ctx)?
        };

        match matched {
            SqlValue::Boolean(true) => return evaluate(&branch.then, ctx),
            SqlValue::Boolean(false) | SqlValue::Null => {}
            other => {
                return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "Boolean".into(),
                    actual: other.type_name().into(),
                }));
            }
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
    use crate::ast::{BinaryOp, Literal, Span};
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
            evaluate(&case, &EvalContext::new(&[])).unwrap(),
            SqlValue::Integer(7)
        );
    }

    #[test]
    fn simple_case_null_does_not_equal_null() {
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
            evaluate(&case, &EvalContext::new(&[])).unwrap(),
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
            evaluate(&case, &EvalContext::new(&[])).unwrap(),
            SqlValue::Null
        );
    }
}
