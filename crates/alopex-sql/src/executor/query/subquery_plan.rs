//! Statement-local subquery registration and conservative dependency analysis.

use crate::ast::dml::LITERAL_TABLE;
use crate::catalog::Catalog;
use crate::planner::aggregate_expr::AggregateExpr;
use crate::planner::logical_plan::{LogicalPlan, ValueWindowFunction, WindowExpr, WindowFunction};
use crate::planner::typed_expr::{Projection, TypedExpr, TypedExprKind};

fn expr_children(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        TypedExprKind::BinaryOp { left, right, .. } => vec![left, right],
        TypedExprKind::UnaryOp { operand, .. } => vec![operand],
        TypedExprKind::Cast { expr, .. }
        | TypedExprKind::TryCast { expr, .. }
        | TypedExprKind::IsNull { expr, .. } => vec![expr],
        TypedExprKind::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        TypedExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            let mut result = vec![expr.as_ref(), pattern.as_ref()];
            result.extend(escape.as_deref());
            result
        }
        TypedExprKind::InList { expr, list, .. } => {
            std::iter::once(expr.as_ref()).chain(list).collect()
        }
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => operand
            .iter()
            .map(Box::as_ref)
            .chain(
                branches
                    .iter()
                    .flat_map(|branch| [&branch.when, &branch.then]),
            )
            .chain(else_expr.iter().map(Box::as_ref))
            .collect(),
        TypedExprKind::FunctionCall {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            let mut result: Vec<_> = args
                .iter()
                .chain(filter.iter().map(Box::as_ref))
                .chain(order_by.iter().map(|key| &key.expr))
                .collect();
            if let Some(window) = over {
                result.extend(&window.partition_by);
                result.extend(window.order_by.iter().map(|key| &key.expr));
            }
            result
        }
        TypedExprKind::InSubquery { expr, .. } | TypedExprKind::Quantified { expr, .. } => {
            vec![expr]
        }
        TypedExprKind::Literal(_)
        | TypedExprKind::ColumnRef { .. }
        | TypedExprKind::VectorLiteral(_)
        | TypedExprKind::ScalarSubquery(_)
        | TypedExprKind::Exists { .. } => Vec::new(),
    }
}

fn nested_plan(expr: &TypedExpr) -> Option<&LogicalPlan> {
    match &expr.kind {
        TypedExprKind::ScalarSubquery(plan)
        | TypedExprKind::Exists { subquery: plan, .. }
        | TypedExprKind::InSubquery { subquery: plan, .. }
        | TypedExprKind::Quantified { subquery: plan, .. } => Some(plan),
        _ => None,
    }
}

fn projection_exprs(projection: &Projection) -> Vec<&TypedExpr> {
    match projection {
        Projection::All(_) => Vec::new(),
        Projection::Columns(columns) => columns.iter().map(|column| &column.expr).collect(),
    }
}

fn aggregate_exprs(aggregate: &AggregateExpr) -> Vec<&TypedExpr> {
    aggregate
        .arg
        .iter()
        .chain(&aggregate.extra_args)
        .chain(aggregate.filter.iter())
        .chain(aggregate.order_by.iter().map(|key| &key.expr))
        .collect()
}

fn window_exprs(window: &WindowExpr) -> Vec<&TypedExpr> {
    let mut result: Vec<_> = window
        .partition_by
        .iter()
        .chain(window.order_by.iter().map(|key| &key.expr))
        .collect();
    match &window.function {
        WindowFunction::Ntile(expr) => result.push(expr),
        WindowFunction::Aggregate(aggregate) => result.extend(aggregate_exprs(aggregate)),
        WindowFunction::Value(
            ValueWindowFunction::FirstValue(expr) | ValueWindowFunction::LastValue(expr),
        ) => result.push(expr),
        WindowFunction::Value(ValueWindowFunction::NthValue { value, nth }) => {
            result.extend([value, nth])
        }
        WindowFunction::Lag(offset) | WindowFunction::Lead(offset) => {
            result.push(&offset.value);
            result.extend(offset.offset.iter());
            result.extend(offset.default.iter());
        }
        WindowFunction::RowNumber
        | WindowFunction::Rank
        | WindowFunction::DenseRank
        | WindowFunction::PercentRank
        | WindowFunction::CumeDist => {}
    }
    result
}

fn plan_exprs(plan: &LogicalPlan) -> Vec<&TypedExpr> {
    match plan {
        LogicalPlan::Scan { projection, .. } | LogicalPlan::Project { projection, .. } => {
            projection_exprs(projection)
        }
        LogicalPlan::Values { rows, .. } => rows.iter().flatten().collect(),
        LogicalPlan::TableFunction { args, .. } => args.iter().collect(),
        LogicalPlan::Filter { predicate, .. } => vec![predicate],
        LogicalPlan::Join { condition, .. } | LogicalPlan::LateralJoin { condition, .. } => {
            condition.iter().collect()
        }
        LogicalPlan::Aggregate {
            group_keys,
            aggregates,
            having,
            projection,
            ..
        } => group_keys
            .iter()
            .chain(aggregates.iter().flat_map(aggregate_exprs))
            .chain(having.iter())
            .chain(projection_exprs(projection))
            .collect(),
        LogicalPlan::Window { windows, .. } => windows.iter().flat_map(window_exprs).collect(),
        LogicalPlan::Sort { order_by, .. } | LogicalPlan::DistinctOn { order_by, .. } => {
            order_by.iter().map(|key| &key.expr).collect()
        }
        LogicalPlan::Limit { ties, .. } => ties.iter().flatten().map(|key| &key.expr).collect(),
        _ => Vec::new(),
    }
}

/// Visits each boxed subquery in a stable structural order, including children.
/// The caller keeps the owning expression alive; no address is dereferenced later.
pub(super) fn visit_expr(expr: &TypedExpr, visit: &mut impl FnMut(&LogicalPlan)) {
    if let Some(plan) = nested_plan(expr) {
        visit(plan);
        visit_plan(plan, visit);
    }
    for child in expr_children(expr) {
        visit_expr(child, visit);
    }
}

pub(super) fn visit_plan(plan: &LogicalPlan, visit: &mut impl FnMut(&LogicalPlan)) {
    for expr in plan_exprs(plan) {
        visit_expr(expr, visit);
    }
    for child in plan.explain_children() {
        visit_plan(child, visit);
    }
}

pub(super) fn independent(plan: &LogicalPlan, catalog: &(impl Catalog + ?Sized)) -> bool {
    widths(plan, 0, catalog).is_some()
}

fn projection_len(projection: &Projection) -> usize {
    match projection {
        Projection::All(columns) => columns.len(),
        Projection::Columns(columns) => columns.len(),
    }
}

fn all_local<'a>(
    expressions: impl IntoIterator<Item = &'a TypedExpr>,
    width: usize,
    catalog: &(impl Catalog + ?Sized),
) -> bool {
    expressions
        .into_iter()
        .all(|expr| local(expr, width, catalog))
}

// Aggregate consumes physical input rows and discards their deferred projection.
// The planner leaves the original aggregate call in Scan.projection; treating
// that unevaluated call as a scalar function would reject every MAX/MIN query.
fn aggregate_input_width(
    plan: &LogicalPlan,
    outer: usize,
    catalog: &(impl Catalog + ?Sized),
) -> Option<usize> {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            if table == LITERAL_TABLE {
                Some(0)
            } else {
                Some(catalog.get_table(table)?.columns.len())
            }
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::Limit { input, .. } => {
            let width = aggregate_input_width(input, outer, catalog)?;
            all_local(plan_exprs(plan), width + outer, catalog).then_some(width)
        }
        _ => widths(plan, outer, catalog).map(|(physical, _)| physical),
    }
}

// Physical row width and final projected width differ in the existing pipeline.
fn widths(
    plan: &LogicalPlan,
    outer: usize,
    catalog: &(impl Catalog + ?Sized),
) -> Option<(usize, usize)> {
    let (physical, projected, expression_width) = match plan {
        LogicalPlan::Scan { table, projection } => {
            let width = if table == LITERAL_TABLE {
                0
            } else {
                catalog.get_table(table)?.columns.len()
            };
            (width, projection_len(projection), width + outer)
        }
        LogicalPlan::Values { schema, .. } => (schema.len(), schema.len(), outer),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::Limit { input, .. } => {
            let (physical, projected) = widths(input, outer, catalog)?;
            (physical, projected, physical + outer)
        }
        LogicalPlan::Project { input, projection } => {
            let (_, input) = widths(input, outer, catalog)?;
            (
                projection_len(projection),
                projection_len(projection),
                input + outer,
            )
        }
        LogicalPlan::Join { left, right, .. } => {
            let width = widths(left, outer, catalog)?.0 + widths(right, outer, catalog)?.0;
            (width, width, width + outer)
        }
        LogicalPlan::LateralJoin { left, right, .. } => {
            let left = widths(left, outer, catalog)?.0;
            let right = widths(right, left + outer, catalog)?.1;
            (left + right, left + right, left + right + outer)
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
            projection,
            grouping_sets,
        } => {
            let input = aggregate_input_width(input, outer, catalog)? + outer;
            let output = group_keys.len() + aggregates.len() + usize::from(grouping_sets.is_some());
            return (all_local(
                group_keys
                    .iter()
                    .chain(aggregates.iter().flat_map(aggregate_exprs)),
                input,
                catalog,
            ) && all_local(
                having.iter().chain(projection_exprs(projection)),
                output + outer,
                catalog,
            ))
            .then_some((output, projection_len(projection)));
        }
        LogicalPlan::Window { input, windows } => {
            let input = widths(input, outer, catalog)?.0;
            (input + windows.len(), input + windows.len(), input + outer)
        }
        LogicalPlan::SetOperation { left, right, .. } => {
            let output = widths(left, outer, catalog)?.1;
            widths(right, outer, catalog)?;
            (output, output, output + outer)
        }
        // External table functions and recursive working tables do not have a
        // proven stable read contract here. Never infer independence for them.
        _ => return None,
    };
    all_local(plan_exprs(plan), expression_width, catalog).then_some((physical, projected))
}

fn local(expr: &TypedExpr, width: usize, catalog: &(impl Catalog + ?Sized)) -> bool {
    if let TypedExprKind::ColumnRef { column_index, .. } = &expr.kind {
        return *column_index < width;
    }
    if let TypedExprKind::FunctionCall { name, .. } = &expr.kind
        && !crate::scalar::signature(name)
            .is_some_and(|signature| !signature.meta.volatile && !signature.meta.side_effecting)
    {
        return false;
    }
    if let Some(plan) = nested_plan(expr)
        && widths(plan, width, catalog).is_none()
    {
        return false;
    }
    all_local(expr_children(expr), width, catalog)
}
