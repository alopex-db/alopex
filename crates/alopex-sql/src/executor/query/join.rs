use std::collections::{HashMap, HashSet};

use crate::executor::evaluator::EvalContext;
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::logical_plan::JoinType;
use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
use crate::storage::SqlValue;

/// Execute JOIN operation.
pub fn execute_join(
    left_rows: Vec<Row>,
    right_rows: Vec<Row>,
    join_type: JoinType,
    condition: Option<&TypedExpr>,
) -> Result<Vec<Row>> {
    let left_width = left_rows.first().map_or(0, Row::len);
    let right_width = right_rows.first().map_or(0, Row::len);
    execute_join_with_widths(
        left_rows,
        right_rows,
        join_type,
        condition,
        left_width,
        right_width,
    )
}

pub(crate) fn execute_join_with_widths(
    left_rows: Vec<Row>,
    right_rows: Vec<Row>,
    join_type: JoinType,
    condition: Option<&TypedExpr>,
    left_width: usize,
    right_width: usize,
) -> Result<Vec<Row>> {
    if matches!(join_type, JoinType::Cross) || condition.is_none() {
        return nested_loop_join_with_widths(
            &left_rows,
            &right_rows,
            condition,
            join_type,
            left_width,
            right_width,
        );
    }

    if let Some((left_key, right_key)) = condition.and_then(|expr| equi_join_keys(expr, left_width))
    {
        return hash_join_with_widths(
            &left_rows,
            &right_rows,
            left_key,
            right_key,
            join_type,
            left_width,
            right_width,
        );
    }

    nested_loop_join_with_widths(
        &left_rows,
        &right_rows,
        condition,
        join_type,
        left_width,
        right_width,
    )
}

/// Nested loop join implementation.
pub fn nested_loop_join(
    left: &[Row],
    right: &[Row],
    condition: &TypedExpr,
    join_type: JoinType,
) -> Result<Vec<Row>> {
    let left_width = left.first().map_or(0, Row::len);
    let right_width = right.first().map_or(0, Row::len);
    nested_loop_join_with_widths(
        left,
        right,
        Some(condition),
        join_type,
        left_width,
        right_width,
    )
}

fn nested_loop_join_with_widths(
    left: &[Row],
    right: &[Row],
    condition: Option<&TypedExpr>,
    join_type: JoinType,
    left_width: usize,
    right_width: usize,
) -> Result<Vec<Row>> {
    let mut output = Vec::new();
    let mut matched_right = HashSet::new();

    for left_row in left {
        let mut matched_left = false;
        for (right_idx, right_row) in right.iter().enumerate() {
            let joined = combine_rows(left_row, right_row, output.len() as u64);
            if condition_matches(condition, &joined)? {
                matched_left = true;
                matched_right.insert(right_idx);
                output.push(joined);
            }
        }
        if !matched_left && matches!(join_type, JoinType::Left | JoinType::Full) {
            output.push(pad_right(left_row, right_width, output.len() as u64));
        }
    }

    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for (right_idx, right_row) in right.iter().enumerate() {
            if !matched_right.contains(&right_idx) {
                output.push(pad_left(right_row, left_width, output.len() as u64));
            }
        }
    }

    Ok(output)
}

/// Hash join implementation for equi-joins.
pub fn hash_join(
    left: &[Row],
    right: &[Row],
    left_key: usize,
    right_key: usize,
    join_type: JoinType,
) -> Result<Vec<Row>> {
    let left_width = left.first().map_or(0, Row::len);
    let right_width = right.first().map_or(0, Row::len);
    hash_join_with_widths(
        left,
        right,
        left_key,
        right_key,
        join_type,
        left_width,
        right_width,
    )
}

fn hash_join_with_widths(
    left: &[Row],
    right: &[Row],
    left_key: usize,
    right_key: usize,
    join_type: JoinType,
    left_width: usize,
    right_width: usize,
) -> Result<Vec<Row>> {
    let mut buckets: HashMap<String, Vec<(usize, &Row)>> = HashMap::new();
    for (idx, right_row) in right.iter().enumerate() {
        let key = right_row
            .get(right_key)
            .map(hash_key)
            .ok_or(ExecutorError::Evaluation(
                crate::executor::EvaluationError::InvalidColumnRef { index: right_key },
            ))?;
        buckets.entry(key).or_default().push((idx, right_row));
    }

    let mut output = Vec::new();
    let mut matched_right = HashSet::new();
    for left_row in left {
        let key = left_row
            .get(left_key)
            .map(hash_key)
            .ok_or(ExecutorError::Evaluation(
                crate::executor::EvaluationError::InvalidColumnRef { index: left_key },
            ))?;
        let mut matched_left = false;
        if let Some(matches) = buckets.get(&key) {
            for (right_idx, right_row) in matches {
                matched_left = true;
                matched_right.insert(*right_idx);
                output.push(combine_rows(left_row, right_row, output.len() as u64));
            }
        }
        if !matched_left && matches!(join_type, JoinType::Left | JoinType::Full) {
            output.push(pad_right(left_row, right_width, output.len() as u64));
        }
    }

    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for (right_idx, right_row) in right.iter().enumerate() {
            if !matched_right.contains(&right_idx) {
                output.push(pad_left(right_row, left_width, output.len() as u64));
            }
        }
    }

    Ok(output)
}

fn condition_matches(condition: Option<&TypedExpr>, row: &Row) -> Result<bool> {
    let Some(condition) = condition else {
        return Ok(true);
    };
    let ctx = EvalContext::new(&row.values);
    match crate::executor::evaluator::evaluate(condition, &ctx)? {
        SqlValue::Boolean(true) => Ok(true),
        _ => Ok(false),
    }
}

fn combine_rows(left: &Row, right: &Row, row_id: u64) -> Row {
    let mut values = Vec::with_capacity(left.len() + right.len());
    values.extend(left.values.clone());
    values.extend(right.values.clone());
    Row::new(row_id, values)
}

fn pad_right(left: &Row, right_width: usize, row_id: u64) -> Row {
    let mut values = Vec::with_capacity(left.len() + right_width);
    values.extend(left.values.clone());
    values.extend(std::iter::repeat_n(SqlValue::Null, right_width));
    Row::new(row_id, values)
}

fn pad_left(right: &Row, left_width: usize, row_id: u64) -> Row {
    let mut values = Vec::with_capacity(left_width + right.len());
    values.extend(std::iter::repeat_n(SqlValue::Null, left_width));
    values.extend(right.values.clone());
    Row::new(row_id, values)
}

fn equi_join_keys(condition: &TypedExpr, left_width: usize) -> Option<(usize, usize)> {
    let TypedExprKind::BinaryOp { left, op, right } = &condition.kind else {
        return None;
    };
    if !matches!(op, crate::ast::expr::BinaryOp::Eq) {
        return None;
    }
    let (
        TypedExprKind::ColumnRef {
            column_index: l, ..
        },
        TypedExprKind::ColumnRef {
            column_index: r, ..
        },
    ) = (&left.kind, &right.kind)
    else {
        return None;
    };
    match (*l < left_width, *r < left_width) {
        (true, false) => Some((*l, *r - left_width)),
        (false, true) => Some((*r, *l - left_width)),
        _ => None,
    }
}

fn hash_key(value: &SqlValue) -> String {
    format!("{value:?}")
}
