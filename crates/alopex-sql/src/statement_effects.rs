//! Conservative write classification for query expressions. This is independent
//! of name resolution so callers can enforce write restrictions before planning.

use crate::ast::*;

pub(crate) fn select_requires_write(select: &Select) -> bool {
    with_requires_write(select.with.as_ref())
        || select.distinct_on.iter().any(expr_requires_write)
        || select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => expr_requires_write(expr),
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
        })
        || select.from.iter().any(from_requires_write)
        || select.selection.as_ref().is_some_and(expr_requires_write)
        || select
            .group_by
            .iter()
            .flatten()
            .any(|item| item.exprs().any(expr_requires_write))
        || select.having.as_ref().is_some_and(expr_requires_write)
        || select
            .windows
            .iter()
            .any(|window| window_requires_write(&window.spec))
        || select.qualify.as_ref().is_some_and(expr_requires_write)
        || select
            .set_operations
            .iter()
            .any(|op| body_requires_write(&op.right))
        || order_requires_write(&select.order_by)
        || select.limit.as_ref().is_some_and(expr_requires_write)
        || select.offset.as_ref().is_some_and(expr_requires_write)
        || select
            .knn_options
            .iter()
            .any(|option| expr_requires_write(&option.value))
}

pub(crate) fn values_requires_write(values: &Values) -> bool {
    with_requires_write(values.with.as_ref())
        || values.rows.iter().flatten().any(expr_requires_write)
        || values
            .set_operations
            .iter()
            .any(|op| body_requires_write(&op.right))
        || order_requires_write(&values.order_by)
        || values.limit.as_ref().is_some_and(expr_requires_write)
        || values.offset.as_ref().is_some_and(expr_requires_write)
}

fn body_requires_write(body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(select) => select_requires_write(select),
        QueryBody::Values(values) => values_requires_write(values),
    }
}

fn with_requires_write(with: Option<&WithClause>) -> bool {
    with.is_some_and(|with| with.ctes.iter().any(|cte| body_requires_write(&cte.query)))
}

fn from_requires_write(from: &FromItem) -> bool {
    match from {
        FromItem::Table { .. } => false,
        FromItem::Join {
            left,
            right,
            condition,
            ..
        } => {
            from_requires_write(left)
                || from_requires_write(right)
                || condition.as_ref().is_some_and(expr_requires_write)
        }
        FromItem::Derived { subquery, .. } => body_requires_write(subquery),
        FromItem::Function { args, .. } => args.iter().any(expr_requires_write),
    }
}

fn order_requires_write(order: &[OrderByExpr]) -> bool {
    order.iter().any(|order| expr_requires_write(&order.expr))
}

fn window_requires_write(window: &WindowSpec) -> bool {
    window.partition_by.iter().any(expr_requires_write) || order_requires_write(&window.order_by)
}

fn expr_requires_write(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Literal { .. }
        | ExprKind::ColumnRef { .. }
        | ExprKind::VectorLiteral { .. }
        | ExprKind::Parameter { .. } => false,
        ExprKind::BinaryOp { left, right, .. } | ExprKind::IsDistinctFrom { left, right, .. } => {
            expr_requires_write(left) || expr_requires_write(right)
        }
        ExprKind::UnaryOp { operand: expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::TryCast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::TruthPredicate { expr, .. } => expr_requires_write(expr),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_requires_write)
                || branches.iter().any(|branch| {
                    expr_requires_write(&branch.when) || expr_requires_write(&branch.then)
                })
                || else_expr.as_deref().is_some_and(expr_requires_write)
        }
        ExprKind::FunctionCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            name.eq_ignore_ascii_case("nextval")
                || args.iter().any(expr_requires_write)
                || order_requires_write(order_by)
                || order_requires_write(within_group)
                || filter.as_deref().is_some_and(expr_requires_write)
                || over.as_ref().is_some_and(window_requires_write)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => expr_requires_write(expr) || expr_requires_write(low) || expr_requires_write(high),
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_requires_write(expr)
                || expr_requires_write(pattern)
                || escape.as_deref().is_some_and(expr_requires_write)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_requires_write(expr) || list.iter().any(expr_requires_write)
        }
        ExprKind::Row { items } => items.iter().any(expr_requires_write),
        ExprKind::ScalarSubquery { subquery } | ExprKind::Exists { subquery, .. } => {
            subquery.kind.requires_write()
        }
        ExprKind::InSubquery { expr, subquery, .. }
        | ExprKind::Quantified { expr, subquery, .. } => {
            expr_requires_write(expr) || subquery.kind.requires_write()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AlopexDialect, Parser};
    fn kind(sql: &str) -> StatementKind {
        Parser::parse_sql(&AlopexDialect, sql)
            .unwrap()
            .remove(0)
            .kind
    }
    #[test]
    fn sequence_write_requirement_covers_nested_queries_without_matching_text() {
        for sql in [
            "SELECT nextval('counter')",
            "SELECT NEXTVAL('counter') AS value",
            "VALUES (nextval('counter'))",
            "SELECT coalesce(nextval('counter'), 0)",
            "SELECT CASE WHEN true THEN nextval('counter') ELSE 0 END",
            "SELECT (SELECT nextval('counter'))",
            "SELECT * FROM (SELECT nextval('counter')) AS nested",
            "WITH nested AS (SELECT nextval('counter')) SELECT * FROM nested",
            "SELECT 1 UNION ALL SELECT nextval('counter')",
            "SELECT 1 WHERE nextval('counter') > 0",
            "SELECT 1 ORDER BY nextval('counter')",
            "SELECT sum(1) OVER (ORDER BY nextval('counter'))",
            "EXPLAIN ANALYZE SELECT nextval('counter')",
        ] {
            assert!(kind(sql).requires_write(), "{sql}");
        }
        for sql in [
            "SELECT currval('counter')",
            "SELECT 'nextval(counter)' AS nextval",
            "SELECT 1 /* nextval('counter') */",
            "VALUES (1)",
            "EXPLAIN SELECT nextval('counter')",
        ] {
            assert!(!kind(sql).requires_write(), "{sql}");
        }
    }
}
