#![allow(dead_code)]

use crate::ast::dml::{OrderByExpr, Select, SelectItem};
use crate::ast::expr::{Expr, ExprKind, Literal};
use crate::catalog::{Catalog, ColumnMetadata, TableMetadata};
use crate::planner::aggregate_expr::{AggregateExpr, AggregateFunction};
use crate::planner::typed_expr::{Projection, SortExpr, TypedExpr};
use crate::planner::{LogicalPlan, Planner, PlannerError, ResolvedType};

impl<'a, C: Catalog> Planner<'a, C> {
    /// Check if SELECT contains aggregate functions in the projection.
    pub(crate) fn has_aggregates(&self, select: &Select) -> bool {
        select.projection.iter().any(select_item_has_aggregate)
    }

    /// Extract aggregate expressions from SELECT projection.
    pub(crate) fn extract_aggregates(
        &self,
        projection: &[SelectItem],
        table: &TableMetadata,
    ) -> Result<Vec<AggregateExpr>, PlannerError> {
        let mut aggregates = Vec::new();

        for item in projection {
            let SelectItem::Expr { expr, .. } = item else {
                continue;
            };

            let ExprKind::FunctionCall {
                name,
                args,
                distinct,
            } = &expr.kind
            else {
                continue;
            };

            let function = match aggregate_function(name) {
                Some(func) => func,
                None => continue,
            };

            match function {
                AggregateFunction::Count => {
                    if args.is_empty() {
                        return Err(PlannerError::unsupported_feature(
                            "COUNT() requires an argument",
                            "v0.3.0+",
                            expr.span,
                        ));
                    }
                    if args.len() > 1 {
                        return Err(PlannerError::unsupported_feature(
                            "COUNT() with multiple arguments",
                            "v0.3.0+",
                            expr.span,
                        ));
                    }

                    let arg_expr = &args[0];
                    if is_count_star_arg(arg_expr) {
                        if *distinct {
                            return Err(PlannerError::unsupported_feature(
                                "COUNT(DISTINCT *) is not supported",
                                "v0.3.0+",
                                expr.span,
                            ));
                        }
                        aggregates.push(AggregateExpr::count_star());
                    } else {
                        let typed_arg = self.type_checker.infer_type(arg_expr, table)?;
                        aggregates.push(AggregateExpr::count(typed_arg, *distinct));
                    }
                }
                AggregateFunction::Sum
                | AggregateFunction::Avg
                | AggregateFunction::Min
                | AggregateFunction::Max => {
                    if *distinct {
                        return Err(PlannerError::unsupported_feature(
                            "DISTINCT in aggregate function",
                            "v0.3.0+",
                            expr.span,
                        ));
                    }
                    if args.len() != 1 {
                        return Err(PlannerError::unsupported_feature(
                            "aggregate function requires exactly one argument",
                            "v0.3.0+",
                            expr.span,
                        ));
                    }
                    let typed_arg = self.type_checker.infer_type(&args[0], table)?;
                    let aggregate = match function {
                        AggregateFunction::Sum => AggregateExpr::sum(typed_arg),
                        AggregateFunction::Avg => AggregateExpr::avg(typed_arg),
                        AggregateFunction::Min => AggregateExpr::min(typed_arg),
                        AggregateFunction::Max => AggregateExpr::max(typed_arg),
                        AggregateFunction::Count => unreachable!(),
                    };
                    aggregates.push(aggregate);
                }
            }
        }

        Ok(aggregates)
    }

    /// Validate GROUP BY semantics for aggregate queries.
    pub(crate) fn validate_group_by_semantics(
        &self,
        projection: &[SelectItem],
        group_by: &[Expr],
        having: &Option<Expr>,
        selection: &Option<Expr>,
    ) -> Result<(), PlannerError> {
        let has_aggregate = projection.iter().any(select_item_has_aggregate);

        if let Some(selection_expr) = selection
            && let Some((function, span)) = find_aggregate_call(selection_expr)
        {
            return Err(PlannerError::aggregate_in_where(function, span));
        }

        if let Some(having_expr) = having
            && group_by.is_empty()
            && !has_aggregate
        {
            return Err(PlannerError::invalid_having(having_expr.span));
        }

        for item in projection {
            match item {
                SelectItem::Wildcard { span } => {
                    if has_aggregate || !group_by.is_empty() {
                        return Err(PlannerError::invalid_group_by("*", *span));
                    }
                }
                SelectItem::Expr { expr, .. } => {
                    if expr_contains_aggregate(expr) {
                        let mut columns = Vec::new();
                        collect_column_refs_outside_aggregate(expr, group_by, false, &mut columns);
                        if let Some((column, span)) = columns.first() {
                            return Err(PlannerError::invalid_group_by(column.clone(), *span));
                        }
                        continue;
                    }

                    if group_by.is_empty() {
                        if has_aggregate {
                            let (column, span) = first_column_ref(expr)
                                .unwrap_or_else(|| ("expression".to_string(), expr.span));
                            return Err(PlannerError::invalid_group_by(column, span));
                        }
                        continue;
                    }

                    let matches_group_by = group_by
                        .iter()
                        .any(|group_expr| expr_equivalent(expr, group_expr));
                    if !matches_group_by {
                        let (column, span) = first_column_ref(expr)
                            .unwrap_or_else(|| ("expression".to_string(), expr.span));
                        return Err(PlannerError::invalid_group_by(column, span));
                    }
                }
            }
        }

        if let Some(having_expr) = having {
            let mut columns = Vec::new();
            collect_column_refs_outside_aggregate(having_expr, group_by, false, &mut columns);
            if let Some((column, span)) = columns.first() {
                return Err(PlannerError::invalid_group_by(column.clone(), *span));
            }
        }

        Ok(())
    }

    /// Validate aggregate argument types.
    pub(crate) fn validate_aggregate_types(
        &self,
        aggregates: &[AggregateExpr],
    ) -> Result<(), PlannerError> {
        for aggregate in aggregates {
            let Some(arg) = aggregate.arg.as_ref() else {
                continue;
            };

            let arg_type = &arg.resolved_type;
            let function_name = aggregate.function.to_string();
            match aggregate.function {
                AggregateFunction::Count => {}
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    if !is_numeric_type(arg_type) {
                        return Err(PlannerError::type_mismatch_in_function(
                            function_name,
                            "numeric (Integer, Float, Double)",
                            arg_type.type_name(),
                            arg.span,
                        ));
                    }
                }
                AggregateFunction::Min | AggregateFunction::Max => {
                    if !is_comparable_type(arg_type) {
                        return Err(PlannerError::type_mismatch_in_function(
                            function_name,
                            "comparable (numeric, Text, Boolean)",
                            arg_type.type_name(),
                            arg.span,
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn plan_aggregate_select(&self, stmt: &Select) -> Result<LogicalPlan, PlannerError> {
        let table = self
            .name_resolver
            .resolve_table(&stmt.from.name, stmt.from.span)?;

        let group_keys = stmt
            .group_by
            .iter()
            .map(|expr| self.type_checker.infer_type(expr, table))
            .collect::<Result<Vec<_>, _>>()?;

        let aggregates = self.extract_aggregates(&stmt.projection, table)?;
        self.validate_aggregate_types(&aggregates)?;

        let aggregate_sources = collect_aggregate_sources(&stmt.projection);
        if aggregate_sources.len() != aggregates.len() {
            return Err(PlannerError::unsupported_feature(
                "aggregate expressions in projection",
                "v0.3.0+",
                stmt.span,
            ));
        }

        let output_names =
            build_aggregate_output_names(&stmt.projection, &stmt.group_by, &aggregate_sources)?;
        let output_table = build_aggregate_output_table(&output_names, &group_keys, &aggregates);

        let projection =
            Projection::All(table.column_names().into_iter().map(String::from).collect());
        let mut plan = LogicalPlan::Scan {
            table: table.name.clone(),
            projection,
        };

        if let Some(ref selection) = stmt.selection {
            let predicate = self.type_checker.infer_type(selection, table)?;
            if predicate.resolved_type != ResolvedType::Boolean {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    predicate.resolved_type.to_string(),
                    selection.span,
                ));
            }
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        let having = if let Some(having_expr) = stmt.having.as_ref() {
            let rewritten = rewrite_expr_for_aggregate_output(
                having_expr,
                &stmt.group_by,
                &aggregate_sources,
                &output_names,
            )?;
            let typed = self.type_checker.infer_type(&rewritten, &output_table)?;
            if typed.resolved_type != ResolvedType::Boolean {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    typed.resolved_type.to_string(),
                    having_expr.span,
                ));
            }
            Some(typed)
        } else {
            None
        };

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys,
            aggregates,
            having,
        };

        if !stmt.order_by.is_empty() {
            let order_by = self.resolve_order_by_for_aggregate(
                &stmt.order_by,
                &stmt.group_by,
                &aggregate_sources,
                &output_names,
                &output_table,
            )?;
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = self.extract_limit_value(&stmt.limit, stmt.span)?;
            let offset = self.extract_limit_value(&stmt.offset, stmt.span)?;
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }

        Ok(plan)
    }

    fn resolve_order_by_for_aggregate(
        &self,
        order_by: &[OrderByExpr],
        group_by: &[Expr],
        aggregate_sources: &[Expr],
        output_names: &[String],
        output_table: &TableMetadata,
    ) -> Result<Vec<SortExpr>, PlannerError> {
        let mut sort_exprs = Vec::new();

        for order_expr in order_by {
            let rewritten = rewrite_expr_for_aggregate_output(
                &order_expr.expr,
                group_by,
                aggregate_sources,
                output_names,
            )?;
            let typed_expr = self.type_checker.infer_type(&rewritten, output_table)?;

            let asc = order_expr.asc.unwrap_or(true);
            let nulls_first = order_expr.nulls_first.unwrap_or(false);

            sort_exprs.push(SortExpr::new(typed_expr, asc, nulls_first));
        }

        Ok(sort_exprs)
    }
}

fn select_item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::Wildcard { .. } => false,
        SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
    }
}

fn aggregate_function(name: &str) -> Option<AggregateFunction> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggregateFunction::Count),
        "SUM" => Some(AggregateFunction::Sum),
        "AVG" => Some(AggregateFunction::Avg),
        "MIN" => Some(AggregateFunction::Min),
        "MAX" => Some(AggregateFunction::Max),
        _ => None,
    }
}

fn is_direct_aggregate_call(expr: &Expr) -> bool {
    matches!(
        &expr.kind,
        ExprKind::FunctionCall { name, .. } if aggregate_function(name).is_some()
    )
}

fn collect_aggregate_sources(projection: &[SelectItem]) -> Vec<Expr> {
    let mut sources = Vec::new();
    for item in projection {
        let SelectItem::Expr { expr, .. } = item else {
            continue;
        };
        if is_direct_aggregate_call(expr) {
            sources.push(expr.clone());
        }
    }
    sources
}

fn build_aggregate_output_names(
    projection: &[SelectItem],
    group_by: &[Expr],
    aggregate_sources: &[Expr],
) -> Result<Vec<String>, PlannerError> {
    let mut output_names = Vec::with_capacity(group_by.len() + aggregate_sources.len());
    for (idx, expr) in group_by.iter().enumerate() {
        output_names.push(default_group_key_name(expr, idx));
    }
    for (idx, expr) in aggregate_sources.iter().enumerate() {
        output_names.push(default_aggregate_name(expr, idx));
    }

    let mut aggregate_index = 0usize;
    for item in projection {
        let SelectItem::Expr { expr, alias, .. } = item else {
            continue;
        };
        if is_direct_aggregate_call(expr) {
            if let Some(alias) = alias
                && let Some(slot) = output_names.get_mut(group_by.len() + aggregate_index)
            {
                *slot = alias.clone();
            }
            aggregate_index = aggregate_index.saturating_add(1);
            continue;
        }

        let Some(group_index) = group_by
            .iter()
            .position(|group_expr| expr_equivalent(expr, group_expr))
        else {
            let (column, span) =
                first_column_ref(expr).unwrap_or_else(|| ("expression".to_string(), expr.span));
            return Err(PlannerError::invalid_group_by(column, span));
        };
        if let Some(alias) = alias {
            output_names[group_index] = alias.clone();
        }
    }

    Ok(output_names)
}

fn default_group_key_name(expr: &Expr, index: usize) -> String {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => column.clone(),
        _ => format!("group_key_{index}"),
    }
}

fn default_aggregate_name(expr: &Expr, index: usize) -> String {
    match &expr.kind {
        ExprKind::FunctionCall { name, .. } => {
            format!("{}_{}", name.to_ascii_lowercase(), index)
        }
        _ => format!("agg_{index}"),
    }
}

fn build_aggregate_output_table(
    output_names: &[String],
    group_keys: &[TypedExpr],
    aggregates: &[AggregateExpr],
) -> TableMetadata {
    let mut columns = Vec::with_capacity(group_keys.len() + aggregates.len());
    for (idx, key) in group_keys.iter().enumerate() {
        let name = output_names
            .get(idx)
            .cloned()
            .unwrap_or_else(|| format!("group_key_{idx}"));
        columns.push(ColumnMetadata::new(name, key.resolved_type.clone()));
    }
    for (idx, aggregate) in aggregates.iter().enumerate() {
        let output_idx = group_keys.len() + idx;
        let name = output_names
            .get(output_idx)
            .cloned()
            .unwrap_or_else(|| format!("agg_{idx}"));
        columns.push(ColumnMetadata::new(name, aggregate.result_type.clone()));
    }
    TableMetadata::new("aggregate", columns)
}

fn rewrite_expr_for_aggregate_output(
    expr: &Expr,
    group_by: &[Expr],
    aggregate_sources: &[Expr],
    output_names: &[String],
) -> Result<Expr, PlannerError> {
    if let Some(group_index) = group_by
        .iter()
        .position(|group_expr| expr_equivalent(expr, group_expr))
    {
        return Ok(output_column_ref(
            output_names.get(group_index).ok_or_else(|| {
                PlannerError::unsupported_feature("aggregate output column", "v0.3.0+", expr.span)
            })?,
            expr.span,
        ));
    }

    if let ExprKind::FunctionCall { name, .. } = &expr.kind
        && aggregate_function(name).is_some()
    {
        let Some(aggregate_index) = aggregate_sources
            .iter()
            .position(|agg_expr| expr_equivalent(expr, agg_expr))
        else {
            return Err(PlannerError::unsupported_feature(
                "aggregate in HAVING/ORDER BY must appear in SELECT list",
                "v0.3.0+",
                expr.span,
            ));
        };
        let output_index = group_by.len() + aggregate_index;
        return Ok(output_column_ref(
            output_names.get(output_index).ok_or_else(|| {
                PlannerError::unsupported_feature("aggregate output column", "v0.3.0+", expr.span)
            })?,
            expr.span,
        ));
    }

    let kind = match &expr.kind {
        ExprKind::Literal(literal) => ExprKind::Literal(literal.clone()),
        ExprKind::ColumnRef { table, column } => ExprKind::ColumnRef {
            table: table.clone(),
            column: column.clone(),
        },
        ExprKind::BinaryOp { left, op, right } => ExprKind::BinaryOp {
            left: Box::new(rewrite_expr_for_aggregate_output(
                left,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            op: *op,
            right: Box::new(rewrite_expr_for_aggregate_output(
                right,
                group_by,
                aggregate_sources,
                output_names,
            )?),
        },
        ExprKind::UnaryOp { op, operand } => ExprKind::UnaryOp {
            op: *op,
            operand: Box::new(rewrite_expr_for_aggregate_output(
                operand,
                group_by,
                aggregate_sources,
                output_names,
            )?),
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| {
                    rewrite_expr_for_aggregate_output(
                        arg,
                        group_by,
                        aggregate_sources,
                        output_names,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            distinct: *distinct,
        },
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: Box::new(rewrite_expr_for_aggregate_output(
                expr,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            low: Box::new(rewrite_expr_for_aggregate_output(
                low,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            high: Box::new(rewrite_expr_for_aggregate_output(
                high,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            negated: *negated,
        },
        ExprKind::Like {
            expr,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: Box::new(rewrite_expr_for_aggregate_output(
                expr,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            pattern: Box::new(rewrite_expr_for_aggregate_output(
                pattern,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            escape: match escape {
                Some(escape_expr) => Some(Box::new(rewrite_expr_for_aggregate_output(
                    escape_expr,
                    group_by,
                    aggregate_sources,
                    output_names,
                )?)),
                None => None,
            },
            negated: *negated,
        },
        ExprKind::InList {
            expr,
            list,
            negated,
        } => ExprKind::InList {
            expr: Box::new(rewrite_expr_for_aggregate_output(
                expr,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            list: list
                .iter()
                .map(|item| {
                    rewrite_expr_for_aggregate_output(
                        item,
                        group_by,
                        aggregate_sources,
                        output_names,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        ExprKind::IsNull { expr, negated } => ExprKind::IsNull {
            expr: Box::new(rewrite_expr_for_aggregate_output(
                expr,
                group_by,
                aggregate_sources,
                output_names,
            )?),
            negated: *negated,
        },
        ExprKind::VectorLiteral(values) => ExprKind::VectorLiteral(values.clone()),
    };

    Ok(Expr {
        kind,
        span: expr.span,
    })
}

fn output_column_ref(name: &str, span: crate::ast::Span) -> Expr {
    Expr {
        kind: ExprKind::ColumnRef {
            table: None,
            column: name.to_string(),
        },
        span,
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    find_aggregate_call(expr).is_some()
}

fn find_aggregate_call(expr: &Expr) -> Option<(String, crate::ast::Span)> {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            if aggregate_function(name).is_some() {
                return Some((name.clone(), expr.span));
            }
            for arg in args {
                if let Some(found) = find_aggregate_call(arg) {
                    return Some(found);
                }
            }
            None
        }
        ExprKind::BinaryOp { left, right, .. } => {
            find_aggregate_call(left).or_else(|| find_aggregate_call(right))
        }
        ExprKind::UnaryOp { operand, .. } => find_aggregate_call(operand),
        ExprKind::Between {
            expr, low, high, ..
        } => find_aggregate_call(expr)
            .or_else(|| find_aggregate_call(low))
            .or_else(|| find_aggregate_call(high)),
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => find_aggregate_call(expr)
            .or_else(|| find_aggregate_call(pattern))
            .or_else(|| escape.as_ref().and_then(|expr| find_aggregate_call(expr))),
        ExprKind::InList { expr, list, .. } => {
            if let Some(found) = find_aggregate_call(expr) {
                return Some(found);
            }
            list.iter().find_map(find_aggregate_call)
        }
        ExprKind::IsNull { expr, .. } => find_aggregate_call(expr),
        ExprKind::Literal(_) | ExprKind::ColumnRef { .. } | ExprKind::VectorLiteral(_) => None,
    }
}

fn collect_column_refs_outside_aggregate(
    expr: &Expr,
    group_by: &[Expr],
    in_aggregate: bool,
    columns: &mut Vec<(String, crate::ast::Span)>,
) {
    if !in_aggregate
        && group_by
            .iter()
            .any(|group_expr| expr_equivalent(expr, group_expr))
    {
        return;
    }

    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => {
            if !in_aggregate {
                columns.push((column.clone(), expr.span));
            }
        }
        ExprKind::FunctionCall { name, args, .. } => {
            let is_aggregate = aggregate_function(name).is_some();
            let next_in_aggregate = in_aggregate || is_aggregate;
            for arg in args {
                collect_column_refs_outside_aggregate(arg, group_by, next_in_aggregate, columns);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_column_refs_outside_aggregate(left, group_by, in_aggregate, columns);
            collect_column_refs_outside_aggregate(right, group_by, in_aggregate, columns);
        }
        ExprKind::UnaryOp { operand, .. } => {
            collect_column_refs_outside_aggregate(operand, group_by, in_aggregate, columns);
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_column_refs_outside_aggregate(expr, group_by, in_aggregate, columns);
            collect_column_refs_outside_aggregate(low, group_by, in_aggregate, columns);
            collect_column_refs_outside_aggregate(high, group_by, in_aggregate, columns);
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_column_refs_outside_aggregate(expr, group_by, in_aggregate, columns);
            collect_column_refs_outside_aggregate(pattern, group_by, in_aggregate, columns);
            if let Some(escape) = escape {
                collect_column_refs_outside_aggregate(escape, group_by, in_aggregate, columns);
            }
        }
        ExprKind::InList { expr, list, .. } => {
            collect_column_refs_outside_aggregate(expr, group_by, in_aggregate, columns);
            for item in list {
                collect_column_refs_outside_aggregate(item, group_by, in_aggregate, columns);
            }
        }
        ExprKind::IsNull { expr, .. } => {
            collect_column_refs_outside_aggregate(expr, group_by, in_aggregate, columns);
        }
        ExprKind::Literal(_) | ExprKind::VectorLiteral(_) => {}
    }
}

fn expr_equivalent(lhs: &Expr, rhs: &Expr) -> bool {
    match (&lhs.kind, &rhs.kind) {
        (ExprKind::Literal(left), ExprKind::Literal(right)) => match (left, right) {
            (Literal::Number(a), Literal::Number(b)) => a == b,
            (Literal::String(a), Literal::String(b)) => a == b,
            (Literal::Boolean(a), Literal::Boolean(b)) => a == b,
            (Literal::Null, Literal::Null) => true,
            _ => false,
        },
        (ExprKind::ColumnRef { column: left, .. }, ExprKind::ColumnRef { column: right, .. }) => {
            left == right
        }
        (
            ExprKind::BinaryOp {
                left: l,
                op: lop,
                right: r,
            },
            ExprKind::BinaryOp {
                left: l2,
                op: rop,
                right: r2,
            },
        ) => lop == rop && expr_equivalent(l, l2) && expr_equivalent(r, r2),
        (
            ExprKind::UnaryOp { op, operand },
            ExprKind::UnaryOp {
                op: op2,
                operand: operand2,
            },
        ) => op == op2 && expr_equivalent(operand, operand2),
        (
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
            },
            ExprKind::FunctionCall {
                name: name2,
                args: args2,
                distinct: distinct2,
            },
        ) => {
            if !name.eq_ignore_ascii_case(name2) || distinct != distinct2 {
                return false;
            }
            if args.len() != args2.len() {
                return false;
            }
            args.iter()
                .zip(args2.iter())
                .all(|(a, b)| expr_equivalent(a, b))
        }
        (
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            },
            ExprKind::Between {
                expr: expr2,
                low: low2,
                high: high2,
                negated: negated2,
            },
        ) => {
            negated == negated2
                && expr_equivalent(expr, expr2)
                && expr_equivalent(low, low2)
                && expr_equivalent(high, high2)
        }
        (
            ExprKind::Like {
                expr,
                pattern,
                escape,
                negated,
            },
            ExprKind::Like {
                expr: expr2,
                pattern: pattern2,
                escape: escape2,
                negated: negated2,
            },
        ) => {
            if negated != negated2 {
                return false;
            }
            if !expr_equivalent(expr, expr2) || !expr_equivalent(pattern, pattern2) {
                return false;
            }
            match (escape, escape2) {
                (Some(left), Some(right)) => expr_equivalent(left, right),
                (None, None) => true,
                _ => false,
            }
        }
        (
            ExprKind::InList {
                expr,
                list,
                negated,
            },
            ExprKind::InList {
                expr: expr2,
                list: list2,
                negated: negated2,
            },
        ) => {
            if negated != negated2 || list.len() != list2.len() {
                return false;
            }
            if !expr_equivalent(expr, expr2) {
                return false;
            }
            list.iter()
                .zip(list2.iter())
                .all(|(a, b)| expr_equivalent(a, b))
        }
        (
            ExprKind::IsNull { expr, negated },
            ExprKind::IsNull {
                expr: expr2,
                negated: negated2,
            },
        ) => negated == negated2 && expr_equivalent(expr, expr2),
        (ExprKind::VectorLiteral(left), ExprKind::VectorLiteral(right)) => left == right,
        _ => false,
    }
}

fn first_column_ref(expr: &Expr) -> Option<(String, crate::ast::Span)> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some((column.clone(), expr.span)),
        ExprKind::BinaryOp { left, right, .. } => {
            first_column_ref(left).or_else(|| first_column_ref(right))
        }
        ExprKind::UnaryOp { operand, .. } => first_column_ref(operand),
        ExprKind::FunctionCall { args, .. } => args.iter().find_map(first_column_ref),
        ExprKind::Between {
            expr, low, high, ..
        } => first_column_ref(expr)
            .or_else(|| first_column_ref(low))
            .or_else(|| first_column_ref(high)),
        ExprKind::Like { expr, pattern, .. } => {
            first_column_ref(expr).or_else(|| first_column_ref(pattern))
        }
        ExprKind::InList { expr, list, .. } => {
            first_column_ref(expr).or_else(|| list.iter().find_map(first_column_ref))
        }
        ExprKind::IsNull { expr, .. } => first_column_ref(expr),
        ExprKind::Literal(_) | ExprKind::VectorLiteral(_) => None,
    }
}

fn is_numeric_type(resolved_type: &ResolvedType) -> bool {
    matches!(
        resolved_type,
        ResolvedType::Integer | ResolvedType::Float | ResolvedType::Double
    )
}

fn is_comparable_type(resolved_type: &ResolvedType) -> bool {
    matches!(
        resolved_type,
        ResolvedType::Integer
            | ResolvedType::Float
            | ResolvedType::Double
            | ResolvedType::Text
            | ResolvedType::Boolean
    )
}

fn is_count_star_arg(expr: &Expr) -> bool {
    matches!(
        &expr.kind,
        ExprKind::ColumnRef {
            table: None,
            column,
        } if column == "*"
    )
}

impl std::fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
        };
        write!(f, "{name}")
    }
}
