use crate::ast::expr::Literal;
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::types::ResolvedType;
use crate::storage::SqlValue;

pub fn eval_literal(lit: &Literal, ty: &ResolvedType) -> Result<SqlValue> {
    match (lit, ty) {
        (Literal::Null, _) => Ok(SqlValue::Null),
        (Literal::Boolean(v), ResolvedType::Boolean) => Ok(SqlValue::Boolean(*v)),
        (Literal::String(s), ResolvedType::Text) => Ok(SqlValue::Text(s.clone())),
        (Literal::Number(n), ResolvedType::Integer) => {
            let parsed = n.parse::<i32>().map_err(|_| {
                ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "Integer".into(),
                    actual: n.clone(),
                })
            })?;
            Ok(SqlValue::Integer(parsed))
        }
        (Literal::Number(n), ResolvedType::BigInt) => {
            let parsed = n.parse::<i64>().map_err(|_| {
                ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "BigInt".into(),
                    actual: n.clone(),
                })
            })?;
            Ok(SqlValue::BigInt(parsed))
        }
        (Literal::Number(n), ResolvedType::Float) => {
            let parsed = n.parse::<f32>().map_err(|_| {
                ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "Float".into(),
                    actual: n.clone(),
                })
            })?;
            Ok(SqlValue::Float(parsed))
        }
        (Literal::Number(n), ResolvedType::Double) => {
            let parsed = n.parse::<f64>().map_err(|_| {
                ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "Double".into(),
                    actual: n.clone(),
                })
            })?;
            Ok(SqlValue::Double(parsed))
        }
        // Typed vector literal is represented as VectorLiteral TypedExprKind, not here.
        (lit, ty) => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: ty.to_string(),
            actual: format!("{lit:?}"),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_to_integer() {
        let v = eval_literal(&Literal::Number("42".into()), &ResolvedType::Integer).unwrap();
        assert_eq!(v, SqlValue::Integer(42));
    }

    #[test]
    fn string_to_text() {
        let v = eval_literal(&Literal::String("hi".into()), &ResolvedType::Text).unwrap();
        assert_eq!(v, SqlValue::Text("hi".into()));
    }

    #[test]
    fn mismatched_type_errors() {
        let err = eval_literal(&Literal::Boolean(true), &ResolvedType::Integer).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::TypeMismatch { .. }) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}
