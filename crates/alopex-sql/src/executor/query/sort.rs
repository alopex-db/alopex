use std::cmp::Ordering;

use crate::executor::Result;
use crate::storage::SqlValue;

use super::{Row, eval_expr};

/// Sort rows according to order_by expressions.
pub fn execute_sort(
    mut rows: Vec<Row>,
    order_by: &[crate::planner::typed_expr::SortExpr],
) -> Result<Vec<Row>> {
    if order_by.is_empty() {
        return Ok(rows);
    }

    // TODO: Key evaluation/materialization holds all rows in memory; consider external/streaming sort for very large datasets if requirements expand.
    // Precompute sort keys to avoid repeated evaluation during comparisons.
    let mut keyed: Vec<(Row, Vec<SqlValue>)> = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let mut keys = Vec::with_capacity(order_by.len());
        for expr in order_by {
            keys.push(eval_expr(&expr.expr, &row)?);
        }
        keyed.push((row, keys));
    }

    keyed.sort_by(|a, b| compare_keys(a, b, order_by));

    Ok(keyed.into_iter().map(|(row, _)| row).collect())
}

fn compare_keys(
    a: &(Row, Vec<SqlValue>),
    b: &(Row, Vec<SqlValue>),
    order_by: &[crate::planner::typed_expr::SortExpr],
) -> Ordering {
    for (i, sort_expr) in order_by.iter().enumerate() {
        let left = &a.1[i];
        let right = &b.1[i];
        let cmp = compare_single(left, right, sort_expr.asc, sort_expr.nulls_first);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn compare_single(left: &SqlValue, right: &SqlValue, asc: bool, nulls_first: bool) -> Ordering {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => match left.partial_cmp(right).unwrap_or(Ordering::Equal) {
            Ordering::Equal => Ordering::Equal,
            ord if asc => ord,
            ord => ord.reverse(),
        },
    }
}
