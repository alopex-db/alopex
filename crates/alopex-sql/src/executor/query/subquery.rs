use alopex_core::kv::KVStore;
use alopex_core::sql::subquery::{materialize_cache, nested_scan, semi_join_probe};

use crate::catalog::Catalog;
use crate::executor::evaluator::EvalContext;
use crate::executor::{EvaluationError, ExecutorError, Result, Row};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::{Quantifier, TypedExpr, TypedExprKind};
use crate::storage::{SqlTxn, SqlValue};

/// Execute scalar subquery.
pub fn execute_scalar_subquery<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    subquery: &LogicalPlan,
) -> Result<SqlValue> {
    execute_scalar_subquery_with_outer(txn, catalog, subquery, None)
}

pub(crate) fn execute_scalar_subquery_with_outer<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    subquery: &LogicalPlan,
    outer: Option<&Row>,
) -> Result<SqlValue> {
    let rows = execute_subquery_rows_with_outer(txn, catalog, subquery, outer)?;
    if rows.len() > 1 {
        return Err(ExecutorError::InvalidOperation {
            operation: "execute_scalar_subquery".into(),
            reason: "scalar subquery returned multiple rows".into(),
        });
    }
    let Some(row) = rows.first() else {
        return Ok(SqlValue::Null);
    };
    if row.len() != 1 {
        return Err(ExecutorError::InvalidOperation {
            operation: "execute_scalar_subquery".into(),
            reason: format!("scalar subquery returned {} columns", row.len()),
        });
    }
    Ok(row[0].clone())
}

/// Execute IN subquery.
///
/// Implements SQL three-valued logic: when no row matches but the subquery
/// result contains NULL (or the probe value itself is NULL and the result is
/// non-empty), the comparison is UNKNOWN and `SqlValue::Null` is returned.
/// Consequently `NOT IN` over a NULL-containing result never yields TRUE.
pub fn execute_in_subquery<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    value: &SqlValue,
    subquery: &LogicalPlan,
    negated: bool,
) -> Result<SqlValue> {
    execute_in_subquery_with_outer(txn, catalog, value, subquery, negated, None)
}

pub(crate) fn execute_in_subquery_with_outer<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    value: &SqlValue,
    subquery: &LogicalPlan,
    negated: bool,
    outer: Option<&Row>,
) -> Result<SqlValue> {
    let rows = execute_subquery_rows_with_outer(txn, catalog, subquery, outer)?;
    let mut unknown = false;
    let matched = semi_join_probe(&rows, |row| {
        let Some(candidate) = row.first() else {
            return Ok(false);
        };
        if matches!(candidate, SqlValue::Null) || matches!(value, SqlValue::Null) {
            unknown = true;
            return Ok(false);
        }
        compare_values(
            crate::ast::expr::BinaryOp::Eq,
            value.clone(),
            candidate.clone(),
        )
    })?;
    if matched {
        return Ok(SqlValue::Boolean(!negated));
    }
    if unknown {
        // UNKNOWN stays UNKNOWN under NOT.
        return Ok(SqlValue::Null);
    }
    Ok(SqlValue::Boolean(negated))
}

/// Execute EXISTS subquery.
pub fn execute_exists<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    subquery: &LogicalPlan,
) -> Result<bool> {
    execute_exists_with_outer(txn, catalog, subquery, false, None)
}

pub(crate) fn execute_exists_with_outer<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    subquery: &LogicalPlan,
    negated: bool,
    outer: Option<&Row>,
) -> Result<bool> {
    let rows = execute_subquery_rows_with_outer(txn, catalog, subquery, outer)?;
    let exists = semi_join_probe(&rows, |_| Ok::<bool, ExecutorError>(true))?;
    Ok(if negated { !exists } else { exists })
}

pub(crate) fn evaluate_expr_with_subqueries<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    expr: &TypedExpr,
    row: &Row,
) -> Result<SqlValue> {
    match &expr.kind {
        TypedExprKind::ScalarSubquery(subquery) => {
            execute_scalar_subquery_with_outer(txn, catalog, subquery, Some(row))
        }
        TypedExprKind::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let value = evaluate_expr_with_subqueries(txn, catalog, expr, row)?;
            execute_in_subquery_with_outer(txn, catalog, &value, subquery, *negated, Some(row))
        }
        TypedExprKind::Exists { subquery, negated } => {
            execute_exists_with_outer(txn, catalog, subquery, *negated, Some(row))
                .map(SqlValue::Boolean)
        }
        TypedExprKind::Quantified {
            expr,
            op,
            quantifier,
            subquery,
        } => {
            let value = evaluate_expr_with_subqueries(txn, catalog, expr, row)?;
            execute_quantified_with_outer(
                txn,
                catalog,
                value,
                *op,
                *quantifier,
                subquery,
                Some(row),
            )
            .map(SqlValue::Boolean)
        }
        TypedExprKind::BinaryOp { left, op, right } if contains_subquery(expr) => {
            let left = evaluate_expr_with_subqueries(txn, catalog, left, row)?;
            let right = evaluate_expr_with_subqueries(txn, catalog, right, row)?;
            crate::executor::evaluator::binary_op::eval_binary_values(op, left, right)
        }
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } if contains_subquery(expr) => {
            let operand = operand
                .as_deref()
                .map(|operand| evaluate_expr_with_subqueries(txn, catalog, operand, row))
                .transpose()?;
            for branch in branches {
                let matched = if let Some(operand) = &operand {
                    let condition = evaluate_expr_with_subqueries(txn, catalog, &branch.when, row)?;
                    crate::executor::evaluator::binary_op::eval_binary_values(
                        &crate::ast::expr::BinaryOp::Eq,
                        operand.clone(),
                        condition,
                    )?
                } else {
                    evaluate_expr_with_subqueries(txn, catalog, &branch.when, row)?
                };
                if matches!(matched, SqlValue::Boolean(true)) {
                    return evaluate_expr_with_subqueries(txn, catalog, &branch.then, row);
                }
            }
            if let Some(else_expr) = else_expr {
                evaluate_expr_with_subqueries(txn, catalog, else_expr, row)
            } else {
                Ok(SqlValue::Null)
            }
        }
        TypedExprKind::Cast {
            expr: inner,
            target_type,
        } if contains_subquery(expr) => {
            let value = evaluate_expr_with_subqueries(txn, catalog, inner, row)?;
            crate::executor::evaluator::coerce_value(value, target_type)
        }
        TypedExprKind::TryCast {
            expr: inner,
            target_type,
        } if contains_subquery(expr) => {
            let value = evaluate_expr_with_subqueries(txn, catalog, inner, row)?;
            crate::executor::evaluator::try_coerce_value(value, target_type)
        }
        _ => {
            let ctx = EvalContext::new(&row.values);
            crate::executor::evaluator::evaluate(expr, &ctx)
        }
    }
}

pub(crate) fn contains_subquery(expr: &TypedExpr) -> bool {
    match &expr.kind {
        TypedExprKind::ScalarSubquery(_)
        | TypedExprKind::InSubquery { .. }
        | TypedExprKind::Exists { .. }
        | TypedExprKind::Quantified { .. } => true,
        TypedExprKind::BinaryOp { left, right, .. } => {
            contains_subquery(left) || contains_subquery(right)
        }
        TypedExprKind::UnaryOp { operand, .. } => contains_subquery(operand),
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(contains_subquery)
                || branches.iter().any(|branch| {
                    contains_subquery(&branch.when) || contains_subquery(&branch.then)
                })
                || else_expr.as_deref().is_some_and(contains_subquery)
        }
        TypedExprKind::FunctionCall { args, .. } => args.iter().any(contains_subquery),
        TypedExprKind::Cast { expr, .. }
        | TypedExprKind::TryCast { expr, .. }
        | TypedExprKind::IsNull { expr, .. } => contains_subquery(expr),
        TypedExprKind::Between {
            expr, low, high, ..
        } => contains_subquery(expr) || contains_subquery(low) || contains_subquery(high),
        TypedExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            contains_subquery(expr)
                || contains_subquery(pattern)
                || escape.as_deref().is_some_and(contains_subquery)
        }
        TypedExprKind::InList { expr, list, .. } => {
            contains_subquery(expr) || list.iter().any(contains_subquery)
        }
        TypedExprKind::Literal(_)
        | TypedExprKind::ColumnRef { .. }
        | TypedExprKind::VectorLiteral(_) => false,
    }
}

/// Returns true if any expression in the query plan contains a subquery.
///
/// The streaming pipeline cannot evaluate subqueries because subquery
/// execution needs transaction access, which the streaming iterators borrow
/// exclusively. Plans containing subqueries must therefore be routed to the
/// materializing execution path (see `build_streaming_pipeline_with_policy`).
pub(crate) fn plan_contains_subquery(plan: &LogicalPlan) -> bool {
    fn projection_contains_subquery(projection: &crate::planner::typed_expr::Projection) -> bool {
        match projection {
            crate::planner::typed_expr::Projection::All(_) => false,
            crate::planner::typed_expr::Projection::Columns(cols) => {
                cols.iter().any(|col| contains_subquery(&col.expr))
            }
        }
    }

    match plan {
        LogicalPlan::Scan { projection, .. } => projection_contains_subquery(projection),
        LogicalPlan::Values { rows, .. } => rows.iter().flatten().any(contains_subquery),
        LogicalPlan::Filter { input, predicate } => {
            contains_subquery(predicate) || plan_contains_subquery(input)
        }
        LogicalPlan::Project { input, projection } => {
            projection_contains_subquery(projection) || plan_contains_subquery(input)
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            condition.as_ref().is_some_and(contains_subquery)
                || plan_contains_subquery(left)
                || plan_contains_subquery(right)
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
            projection,
            grouping_sets: _,
        } => {
            group_keys.iter().any(contains_subquery)
                || aggregates
                    .iter()
                    .any(|agg| agg.arg.as_ref().is_some_and(contains_subquery))
                || having.as_ref().is_some_and(contains_subquery)
                || projection_contains_subquery(projection)
                || plan_contains_subquery(input)
        }
        LogicalPlan::SetOperation { left, right, .. } => {
            plan_contains_subquery(left) || plan_contains_subquery(right)
        }
        LogicalPlan::RecursiveCte {
            anchor,
            recursive_term,
            ..
        } => plan_contains_subquery(anchor) || plan_contains_subquery(recursive_term),
        LogicalPlan::RecursiveReference { .. } => false,
        LogicalPlan::Sort { input, order_by } => {
            order_by.iter().any(|sort| contains_subquery(&sort.expr))
                || plan_contains_subquery(input)
        }
        LogicalPlan::DistinctOn {
            input, order_by, ..
        } => {
            order_by.iter().any(|sort| contains_subquery(&sort.expr))
                || plan_contains_subquery(input)
        }
        LogicalPlan::Limit { input, .. } => plan_contains_subquery(input),
        // DML/DDL plans are never executed through the query pipelines.
        _ => false,
    }
}

fn execute_quantified_with_outer<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    value: SqlValue,
    op: crate::ast::expr::BinaryOp,
    quantifier: Quantifier,
    subquery: &LogicalPlan,
    outer: Option<&Row>,
) -> Result<bool> {
    let rows = execute_subquery_rows_with_outer(txn, catalog, subquery, outer)?;
    if rows.is_empty() {
        return Ok(matches!(quantifier, Quantifier::All));
    }
    Ok(match quantifier {
        Quantifier::Any => semi_join_probe(&rows, |row| {
            let Some(candidate) = row.first() else {
                return Ok::<bool, ExecutorError>(false);
            };
            compare_values(op, value.clone(), candidate.clone())
        })?,
        Quantifier::All => {
            let has_non_match = semi_join_probe(&rows, |row| {
                let Some(candidate) = row.first() else {
                    return Ok::<bool, ExecutorError>(false);
                };
                Ok(!compare_values(op, value.clone(), candidate.clone())?)
            })?;
            !has_non_match
        }
    })
}

fn execute_subquery_rows_with_outer<
    'txn,
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
>(
    txn: &mut T,
    catalog: &C,
    subquery: &LogicalPlan,
    outer: Option<&Row>,
) -> Result<Vec<Vec<SqlValue>>> {
    if outer.is_none() {
        let mut cache = materialize_cache();
        return cache.get_or_try_insert_with((), || {
            nested_scan(|| {
                super::execute_query_result_with_outer(txn, catalog, subquery.clone(), outer)
                    .map(|result| result.rows)
            })
        });
    }

    nested_scan(|| {
        super::execute_query_result_with_outer(txn, catalog, subquery.clone(), outer)
            .map(|result| result.rows)
    })
}

fn compare_values(op: crate::ast::expr::BinaryOp, left: SqlValue, right: SqlValue) -> Result<bool> {
    match crate::executor::evaluator::binary_op::eval_binary_values(&op, left, right)? {
        SqlValue::Boolean(value) => Ok(value),
        other => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Boolean".into(),
            actual: other.type_name().into(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::plan_contains_subquery;
    use crate::catalog::{Catalog, ColumnMetadata, MemoryCatalog, TableMetadata};
    use crate::dialect::AlopexDialect;
    use crate::parser::Parser;
    use crate::planner::Planner;
    use crate::planner::logical_plan::LogicalPlan;
    use crate::planner::types::ResolvedType;

    fn plan_select(sql: &str) -> LogicalPlan {
        let mut catalog = MemoryCatalog::new();
        catalog
            .create_table(TableMetadata::new(
                "users",
                vec![
                    ColumnMetadata::new("id", ResolvedType::Integer),
                    ColumnMetadata::new("name", ResolvedType::Text),
                ],
            ))
            .unwrap();
        catalog
            .create_table(TableMetadata::new(
                "orders",
                vec![
                    ColumnMetadata::new("id", ResolvedType::Integer),
                    ColumnMetadata::new("user_id", ResolvedType::Integer),
                    ColumnMetadata::new("total", ResolvedType::Integer),
                ],
            ))
            .unwrap();
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse sql");
        assert_eq!(statements.len(), 1, "expected single statement");
        Planner::new(&catalog)
            .plan(&statements[0])
            .expect("plan sql")
    }

    #[test]
    fn detects_subquery_in_join_on_condition() {
        let plan = plan_select(
            "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id AND orders.user_id IN (SELECT orders.user_id FROM orders)",
        );
        assert!(plan_contains_subquery(&plan));
    }

    #[test]
    fn detects_subquery_in_having() {
        let plan = plan_select(
            "SELECT orders.user_id, COUNT(*) FROM orders GROUP BY orders.user_id HAVING COUNT(*) > (SELECT MIN(orders.total) FROM orders)",
        );
        assert!(plan_contains_subquery(&plan));
    }

    #[test]
    fn detects_subquery_in_order_by() {
        let plan = plan_select(
            "SELECT users.name FROM users ORDER BY (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id)",
        );
        assert!(plan_contains_subquery(&plan));
    }

    #[test]
    fn plan_without_subquery_is_not_detected() {
        let plan =
            plan_select("SELECT users.name FROM users WHERE users.id = 1 ORDER BY users.name");
        assert!(!plan_contains_subquery(&plan));
    }
}
