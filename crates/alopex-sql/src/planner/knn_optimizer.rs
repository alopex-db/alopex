use crate::ast::expr::Literal;
use crate::executor::evaluator::vector_ops::VectorMetric;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::{Projection, SortExpr, TypedExprKind};

/// ORDER BY + LIMIT から抽出した KNN 最適化パターン。
#[derive(Debug, Clone, PartialEq)]
pub struct KnnPattern {
    pub table: String,
    pub column: String,
    pub query_vector: Vec<f32>,
    pub metric: VectorMetric,
    pub k: u64,
    pub sort_direction: SortDirection,
}

/// ソート方向（ASC / DESC）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// ORDER BY vector_similarity/vector_distance + LIMIT K の形になっているか検出する。
///
/// - LIMIT が存在し、OFFSET が無い（または 0）の場合のみ最適化対象。
/// - ORDER BY は単一のベクトル関数呼び出しであること。
/// - メトリクスとソート方向の整合性を満たす場合のみ Some を返す。
pub fn detect_knn_pattern(plan: &LogicalPlan) -> Option<KnnPattern> {
    let (sort_plan, k) = extract_limit(plan)?;
    let (order_expr, input_after_sort) = extract_sort(sort_plan)?;
    let sort_direction = if order_expr.asc {
        SortDirection::Asc
    } else {
        SortDirection::Desc
    };

    let (table, _projection, _filter) = extract_scan_context(input_after_sort)?;

    let (func_name, args) = match &order_expr.expr.kind {
        TypedExprKind::FunctionCall { name, args } => (name.to_ascii_lowercase(), args),
        _ => return None,
    };

    if func_name != "vector_similarity" && func_name != "vector_distance" {
        return None;
    }

    if args.len() != 3 {
        return None;
    }

    let column_name = extract_column_name(&args[0], &table)?;
    let query_vector = extract_query_vector(&args[1])?;
    let metric = extract_metric(&args[2])?;

    if !is_valid_knn_direction(metric, sort_direction) {
        return None;
    }

    Some(KnnPattern {
        table,
        column: column_name,
        query_vector,
        metric,
        k,
        sort_direction,
    })
}

fn extract_limit(plan: &LogicalPlan) -> Option<(&LogicalPlan, u64)> {
    match plan {
        LogicalPlan::Limit {
            input,
            limit: Some(k),
            offset,
        } if offset.unwrap_or(0) == 0 => Some((input.as_ref(), *k)),
        _ => None,
    }
}

fn extract_sort(plan: &LogicalPlan) -> Option<(&SortExpr, &LogicalPlan)> {
    if let LogicalPlan::Sort { input, order_by } = plan
        && order_by.len() == 1
    {
        return Some((&order_by[0], input.as_ref()));
    }
    None
}

fn extract_scan_context(
    plan: &LogicalPlan,
) -> Option<(
    String,
    Projection,
    Option<crate::planner::typed_expr::TypedExpr>,
)> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::Scan { table, projection } = input.as_ref() {
                return Some((table.clone(), projection.clone(), Some(predicate.clone())));
            }
            None
        }
        LogicalPlan::Scan { table, projection } => Some((table.clone(), projection.clone(), None)),
        _ => None,
    }
}

fn extract_column_name(
    expr: &crate::planner::typed_expr::TypedExpr,
    table: &str,
) -> Option<String> {
    match &expr.kind {
        TypedExprKind::ColumnRef {
            table: tbl, column, ..
        } if tbl == table => Some(column.clone()),
        _ => None,
    }
}

fn extract_query_vector(expr: &crate::planner::typed_expr::TypedExpr) -> Option<Vec<f32>> {
    match &expr.kind {
        TypedExprKind::VectorLiteral(values) if !values.is_empty() => {
            Some(values.iter().map(|v| *v as f32).collect())
        }
        _ => None,
    }
}

fn extract_metric(expr: &crate::planner::typed_expr::TypedExpr) -> Option<VectorMetric> {
    match &expr.kind {
        TypedExprKind::Literal(Literal::String(s)) => s.parse().ok(),
        _ => None,
    }
}

fn is_valid_knn_direction(metric: VectorMetric, dir: SortDirection) -> bool {
    matches!(
        (metric, dir),
        (VectorMetric::Cosine, SortDirection::Desc)
            | (VectorMetric::Inner, SortDirection::Desc)
            | (VectorMetric::L2, SortDirection::Asc)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ddl::VectorMetric as AstVectorMetric;
    use crate::ast::span::Span;
    use crate::planner::logical_plan::LogicalPlan;
    use crate::planner::typed_expr::{Projection, SortExpr, TypedExpr};
    use crate::planner::types::ResolvedType;

    fn base_vector_type() -> ResolvedType {
        ResolvedType::Vector {
            dimension: 2,
            metric: AstVectorMetric::Cosine,
        }
    }

    fn build_plan(order_asc: bool, metric_literal: &str, offset: Option<u64>) -> LogicalPlan {
        let span = Span::empty();
        let vector_expr = TypedExpr::function_call(
            "vector_similarity".to_string(),
            vec![
                TypedExpr::column_ref(
                    "items".to_string(),
                    "embedding".to_string(),
                    0,
                    base_vector_type(),
                    span,
                ),
                TypedExpr::vector_literal(vec![1.0, 0.0], 2, span),
                TypedExpr::literal(
                    Literal::String(metric_literal.to_string()),
                    ResolvedType::Text,
                    span,
                ),
            ],
            ResolvedType::Double,
            span,
        );
        let sort = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                table: "items".to_string(),
                projection: Projection::All(vec!["embedding".to_string()]),
            }),
            order_by: vec![SortExpr::new(vector_expr, order_asc, false)],
        };

        LogicalPlan::Limit {
            input: Box::new(sort),
            limit: Some(2),
            offset,
        }
    }

    #[test]
    fn detect_knn_pattern_cosine_desc() {
        let plan = build_plan(false, "cosine", None);
        let pattern = detect_knn_pattern(&plan).expect("should detect pattern");
        assert_eq!(pattern.table, "items");
        assert_eq!(pattern.column, "embedding");
        assert_eq!(pattern.k, 2);
        assert_eq!(pattern.metric, VectorMetric::Cosine);
        assert_eq!(pattern.sort_direction, SortDirection::Desc);
        assert_eq!(pattern.query_vector, vec![1.0, 0.0]);
    }

    #[test]
    fn reject_invalid_direction() {
        let plan = build_plan(true, "cosine", None);
        assert!(detect_knn_pattern(&plan).is_none());
    }

    #[test]
    fn reject_missing_limit_or_offset() {
        let plan_no_limit = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                table: "items".to_string(),
                projection: Projection::All(vec!["embedding".to_string()]),
            }),
            order_by: vec![],
        };
        assert!(detect_knn_pattern(&plan_no_limit).is_none());

        let plan_with_offset = build_plan(false, "cosine", Some(1));
        assert!(detect_knn_pattern(&plan_with_offset).is_none());
    }

    #[test]
    fn reject_unknown_metric() {
        let plan = build_plan(false, "unknown", None);
        assert!(detect_knn_pattern(&plan).is_none());
    }
}
