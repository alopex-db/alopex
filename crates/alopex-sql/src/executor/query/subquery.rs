use alopex_core::kv::KVStore;

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
    let result = super::execute_query_result_with_outer(txn, catalog, subquery.clone(), outer)?;
    if result.rows.len() > 1 {
        return Err(ExecutorError::InvalidOperation {
            operation: "execute_scalar_subquery".into(),
            reason: "scalar subquery returned multiple rows".into(),
        });
    }
    let Some(row) = result.rows.first() else {
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
pub fn execute_in_subquery<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
    txn: &mut T,
    catalog: &C,
    value: &SqlValue,
    subquery: &LogicalPlan,
    negated: bool,
) -> Result<bool> {
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
) -> Result<bool> {
    let result = super::execute_query_result_with_outer(txn, catalog, subquery.clone(), outer)?;
    let mut matched = false;
    for row in result.rows {
        let Some(candidate) = row.first() else {
            continue;
        };
        if compare_values(
            crate::ast::expr::BinaryOp::Eq,
            value.clone(),
            candidate.clone(),
        )? {
            matched = true;
            break;
        }
    }
    Ok(if negated { !matched } else { matched })
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
    let result = super::execute_query_result_with_outer(txn, catalog, subquery.clone(), outer)?;
    let exists = !result.rows.is_empty();
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
                .map(SqlValue::Boolean)
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
        TypedExprKind::FunctionCall { args, .. } => args.iter().any(contains_subquery),
        TypedExprKind::Cast { expr, .. } | TypedExprKind::IsNull { expr, .. } => {
            contains_subquery(expr)
        }
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
    let result = super::execute_query_result_with_outer(txn, catalog, subquery.clone(), outer)?;
    if result.rows.is_empty() {
        return Ok(matches!(quantifier, Quantifier::All));
    }
    let mut any_true = false;
    for row in result.rows {
        let Some(candidate) = row.first() else {
            continue;
        };
        let comparison = compare_values(op, value.clone(), candidate.clone())?;
        match quantifier {
            Quantifier::Any if comparison => return Ok(true),
            Quantifier::All if !comparison => return Ok(false),
            Quantifier::Any => any_true = any_true || comparison,
            Quantifier::All => {}
        }
    }
    Ok(match quantifier {
        Quantifier::Any => any_true,
        Quantifier::All => true,
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
