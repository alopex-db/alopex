use crate::executor::evaluator::vector_ops::{
    VectorError, VectorMetric, vector_distance, vector_similarity,
};
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::{EvalContext, evaluate};

pub fn evaluate_function_call(
    name: &str,
    args: &[TypedExpr],
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let name_lower = name.to_lowercase();
    match name_lower.as_str() {
        "vector_similarity" => evaluate_vector_function(args, ctx, VectorFn::Similarity),
        "vector_distance" => evaluate_vector_function(args, ctx, VectorFn::Distance),
        _ => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(name.to_string()),
        )),
    }
}

#[derive(Clone, Copy)]
enum VectorFn {
    Similarity,
    Distance,
}

fn evaluate_vector_function(
    args: &[TypedExpr],
    ctx: &EvalContext<'_>,
    kind: VectorFn,
) -> Result<SqlValue> {
    if args.len() != 3 {
        return Err(ExecutorError::Evaluation(EvaluationError::Vector(
            VectorError::ArgumentCountMismatch { actual: args.len() },
        )));
    }

    let column_value = evaluate(&args[0], ctx)?;
    let column_vec = match column_value {
        SqlValue::Vector(v) => v,
        _ => {
            return Err(ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::TypeMismatch,
            )));
        }
    };

    let query_value = evaluate(&args[1], ctx)?;
    let query_vec = match query_value {
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

    let metric_value = evaluate(&args[2], ctx)?;
    let metric_str = match metric_value {
        SqlValue::Text(s) => s,
        other => {
            return Err(ExecutorError::Evaluation(EvaluationError::Vector(
                VectorError::InvalidMetric {
                    metric: other.type_name().to_string(),
                    reason: "third argument must be string".into(),
                },
            )));
        }
    };

    let metric: VectorMetric = metric_str
        .parse()
        .map_err(|e| ExecutorError::Evaluation(EvaluationError::Vector(e)))?;

    let score = match kind {
        VectorFn::Similarity => vector_similarity(&column_vec, &query_vec, metric),
        VectorFn::Distance => vector_distance(&column_vec, &query_vec, metric),
    }
    .map_err(|e| ExecutorError::Evaluation(EvaluationError::Vector(e)))?;

    Ok(SqlValue::Double(score))
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

        let result = evaluate_function_call("vector_similarity", &args, &ctx).unwrap();
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

        let result = evaluate_function_call("vector_distance", &args, &ctx).unwrap();
        match result {
            SqlValue::Double(v) => assert!((v - 32.0).abs() < 1e-6),
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

        let err = evaluate_function_call("vector_similarity", &args, &ctx).unwrap_err();
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

        let err = evaluate_function_call("vector_similarity", &args, &ctx).unwrap_err();
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

        let err = evaluate_function_call("vector_similarity", &args, &ctx).unwrap_err();
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

        let err = evaluate_function_call("vector_similarity", &args, &ctx).unwrap_err();
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

        let err = evaluate_function_call("vector_similarity", &args, &ctx).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::Vector(VectorError::InvalidMetric {
                reason,
                ..
            })) => assert!(reason.contains("empty")),
            other => panic!("unexpected error {other:?}"),
        }
    }
}
