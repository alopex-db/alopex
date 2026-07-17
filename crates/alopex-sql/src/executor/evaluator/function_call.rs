use crate::executor::evaluator::vector_ops::{
    VectorError, VectorMetric, vector_dims, vector_distance, vector_norm, vector_similarity,
};
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::{EvalContext, evaluate, registry::scalar_registry};

pub fn evaluate_function_call(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    star: bool,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    if distinct || star {
        return Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(format!("{name} with modifiers")),
        ));
    }
    let Some(function) = scalar_registry().get(name) else {
        return Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(name.to_string()),
        ));
    };
    if let Some(eval_lazy) = function.eval_lazy {
        return eval_lazy(args, ctx);
    }
    let values = args
        .iter()
        .map(|arg| evaluate(arg, ctx))
        .collect::<Result<Vec<_>>>()?;
    (function.eval)(&values)
}

pub(crate) fn eval_vector_similarity_values(values: &[SqlValue]) -> Result<SqlValue> {
    eval_vector_values(values, VectorFn::Similarity)
}

pub(crate) fn eval_vector_distance_values(values: &[SqlValue]) -> Result<SqlValue> {
    eval_vector_values(values, VectorFn::Distance)
}

pub(crate) fn eval_vector_dims_values(values: &[SqlValue]) -> Result<SqlValue> {
    if values.len() != 1 {
        return Err(ExecutorError::Evaluation(EvaluationError::Vector(
            VectorError::ArgumentCountMismatch {
                actual: values.len(),
            },
        )));
    }
    match &values[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Vector(v) => Ok(SqlValue::Integer(vector_dims(v) as i32)),
        _other => Err(ExecutorError::Evaluation(EvaluationError::Vector(
            VectorError::TypeMismatch,
        ))),
    }
}

pub(crate) fn eval_vector_norm_values(values: &[SqlValue]) -> Result<SqlValue> {
    if values.len() != 1 {
        return Err(ExecutorError::Evaluation(EvaluationError::Vector(
            VectorError::ArgumentCountMismatch {
                actual: values.len(),
            },
        )));
    }
    match &values[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Vector(v) => Ok(SqlValue::Double(vector_norm(v))),
        _ => Err(ExecutorError::Evaluation(EvaluationError::Vector(
            VectorError::TypeMismatch,
        ))),
    }
}

fn eval_vector_values(values: &[SqlValue], kind: VectorFn) -> Result<SqlValue> {
    if values.len() != 3 {
        return Err(ExecutorError::Evaluation(EvaluationError::Vector(
            VectorError::ArgumentCountMismatch {
                actual: values.len(),
            },
        )));
    }
    let column = match &values[0] {
        SqlValue::Vector(v) => v,
        _ => {
            return Err(ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::TypeMismatch,
            )));
        }
    };
    let query = match &values[1] {
        SqlValue::Vector(v) if !v.is_empty() => v,
        SqlValue::Vector(_) => {
            return Err(ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::InvalidVectorLiteral {
                    reason: "empty vector literal not allowed".into(),
                },
            )));
        }
        _ => {
            return Err(ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::InvalidVectorLiteral {
                    reason: "second argument must be vector literal".into(),
                },
            )));
        }
    };
    let metric = match &values[2] {
        SqlValue::Text(value) => value
            .parse::<VectorMetric>()
            .map_err(|error| ExecutorError::Evaluation(EvaluationError::Vector(error)))?,
        other => {
            return Err(ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::InvalidMetric {
                    metric: other.type_name().into(),
                    reason: "third argument must be string".into(),
                },
            )));
        }
    };
    let result = match kind {
        VectorFn::Similarity => vector_similarity(column, query, metric),
        VectorFn::Distance => vector_distance(column, query, metric),
    }
    .map_err(|error| ExecutorError::Evaluation(EvaluationError::Vector(error)))?;
    Ok(SqlValue::Double(result))
}

#[derive(Clone, Copy)]
enum VectorFn {
    Similarity,
    Distance,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ddl::VectorMetric as AstVectorMetric;
    use crate::ast::expr::Literal;
    use crate::ast::span::Span;
    use crate::executor::evaluator::vector_ops::VectorError;
    use crate::planner::typed_expr::TypedExpr;
    use crate::planner::types::ResolvedType;

    fn make_metric_expr(metric: &str) -> TypedExpr {
        TypedExpr::literal(
            Literal::String(metric.to_string()),
            ResolvedType::Text,
            Span::empty(),
        )
    }

    fn make_vector_literal(values: Vec<f64>) -> TypedExpr {
        let dimension = values.len() as u32;
        TypedExpr::vector_literal(values, dimension, Span::empty())
    }

    fn make_vector_column(index: usize, dimension: u32) -> TypedExpr {
        TypedExpr::column_ref(
            "t".to_string(),
            "v".to_string(),
            index,
            ResolvedType::Vector {
                dimension,
                metric: AstVectorMetric::Cosine,
            },
            Span::empty(),
        )
    }

    #[test]
    fn evaluate_vector_similarity_success() {
        let args = vec![
            make_vector_column(0, 2),
            make_vector_literal(vec![0.0, 1.0]),
            make_metric_expr("cosine"),
        ];
        let row = vec![SqlValue::Vector(vec![1.0, 0.0])];
        let ctx = EvalContext::new(&row);

        let result =
            evaluate_function_call("vector_similarity", &args, false, false, &ctx).unwrap();
        match result {
            SqlValue::Double(v) => assert!((v - 0.0).abs() < 1e-6),
            other => panic!("unexpected value {other:?}"),
        }
    }

    #[test]
    fn evaluate_vector_distance_success() {
        let args = vec![
            make_vector_column(0, 3),
            make_vector_literal(vec![4.0, 5.0, 6.0]),
            make_metric_expr("inner"),
        ];
        let row = vec![SqlValue::Vector(vec![1.0, 2.0, 3.0])];
        let ctx = EvalContext::new(&row);

        let result = evaluate_function_call("vector_distance", &args, false, false, &ctx).unwrap();
        match result {
            SqlValue::Double(v) => assert!((v - 32.0).abs() < 1e-6),
            other => panic!("unexpected value {other:?}"),
        }
    }

    #[test]
    fn evaluate_vector_dims_success() {
        let args = vec![make_vector_column(0, 3)];
        let row = vec![SqlValue::Vector(vec![1.0, 2.0, 3.0])];
        let ctx = EvalContext::new(&row);

        let result = evaluate_function_call("vector_dims", &args, false, false, &ctx).unwrap();
        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn evaluate_vector_norm_success() {
        let args = vec![make_vector_column(0, 2)];
        let row = vec![SqlValue::Vector(vec![3.0, 4.0])];
        let ctx = EvalContext::new(&row);

        let result = evaluate_function_call("vector_norm", &args, false, false, &ctx).unwrap();
        match result {
            SqlValue::Double(v) => assert!((v - 5.0).abs() < 1e-6),
            other => panic!("unexpected value {other:?}"),
        }
    }

    #[test]
    fn evaluate_function_argument_count_error() {
        let args = vec![
            make_vector_column(0, 2),
            make_vector_literal(vec![1.0, 2.0]),
        ];
        let row = vec![SqlValue::Vector(vec![1.0, 0.0])];
        let ctx = EvalContext::new(&row);

        let err =
            evaluate_function_call("vector_similarity", &args, false, false, &ctx).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::ArgumentCountMismatch { actual },
            )) => assert_eq!(actual, 2),
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn evaluate_function_metric_type_error() {
        let bad_metric = TypedExpr::literal(
            Literal::Number("1".into()),
            ResolvedType::Integer,
            Span::empty(),
        );
        let args = vec![
            make_vector_column(0, 2),
            make_vector_literal(vec![1.0, 2.0]),
            bad_metric,
        ];
        let row = vec![SqlValue::Vector(vec![1.0, 0.0])];
        let ctx = EvalContext::new(&row);

        let err =
            evaluate_function_call("vector_similarity", &args, false, false, &ctx).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::Vector(VectorError::InvalidMetric {
                ..
            })) => {}
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn evaluate_function_type_mismatch_first_argument() {
        let col = TypedExpr::literal(Literal::Null, ResolvedType::Null, Span::empty());
        let args = vec![
            col,
            make_vector_literal(vec![1.0, 2.0]),
            make_metric_expr("cosine"),
        ];
        let row = vec![SqlValue::Null];
        let ctx = EvalContext::new(&row);

        let err =
            evaluate_function_call("vector_similarity", &args, false, false, &ctx).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::Vector(VectorError::TypeMismatch)) => {}
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn evaluate_function_rejects_empty_vector_literal() {
        let args = vec![
            make_vector_column(0, 0),
            make_vector_literal(vec![]),
            make_metric_expr("cosine"),
        ];
        let row = vec![SqlValue::Vector(vec![])];
        let ctx = EvalContext::new(&row);

        let err =
            evaluate_function_call("vector_similarity", &args, false, false, &ctx).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::InvalidVectorLiteral { reason },
            )) => assert!(reason.contains("empty")),
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn evaluate_function_rejects_empty_metric_string() {
        let args = vec![
            make_vector_column(0, 2),
            make_vector_literal(vec![1.0, 2.0]),
            make_metric_expr(""),
        ];
        let row = vec![SqlValue::Vector(vec![1.0, 0.0])];
        let ctx = EvalContext::new(&row);

        let err =
            evaluate_function_call("vector_similarity", &args, false, false, &ctx).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::Vector(VectorError::InvalidMetric {
                reason,
                ..
            })) => assert!(reason.contains("empty")),
            other => panic!("unexpected error {other:?}"),
        }
    }
}
