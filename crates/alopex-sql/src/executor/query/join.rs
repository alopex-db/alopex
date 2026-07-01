use crate::executor::evaluator::EvalContext;
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::logical_plan::JoinType;
use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
use crate::storage::SqlValue;
use alopex_core::sql::join::{
    RowLike, combine_rows as core_combine_rows, hash_join as core_hash_join,
    nested_loop_join as core_nested_loop_join, pad_left as core_pad_left,
    pad_right as core_pad_right,
};

impl RowLike for Row {
    type Value = SqlValue;

    fn values(&self) -> &[Self::Value] {
        &self.values
    }
}

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
    let pairs = core_nested_loop_join(
        left,
        right,
        matches!(join_type, JoinType::Left | JoinType::Full),
        matches!(join_type, JoinType::Right | JoinType::Full),
        |left_row, right_row| {
            let joined = core_combine_rows(left_row, right_row, 0, Row::new);
            condition_matches(condition, &joined)
        },
    )?;
    Ok(materialize_join_pairs(pairs, left_width, right_width))
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
    let pairs = core_hash_join(
        left,
        right,
        matches!(join_type, JoinType::Left | JoinType::Full),
        matches!(join_type, JoinType::Right | JoinType::Full),
        |row| {
            row.get(left_key).map(hash_key).ok_or({
                ExecutorError::Evaluation(crate::executor::EvaluationError::InvalidColumnRef {
                    index: left_key,
                })
            })
        },
        |row| {
            row.get(right_key).map(hash_key).ok_or({
                ExecutorError::Evaluation(crate::executor::EvaluationError::InvalidColumnRef {
                    index: right_key,
                })
            })
        },
    )?;
    Ok(materialize_join_pairs(pairs, left_width, right_width))
}

fn materialize_join_pairs(
    pairs: Vec<(Option<Row>, Option<Row>)>,
    left_width: usize,
    right_width: usize,
) -> Vec<Row> {
    pairs
        .into_iter()
        .enumerate()
        .map(|(row_id, (left, right))| match (left, right) {
            (Some(left), Some(right)) => core_combine_rows(&left, &right, row_id as u64, Row::new),
            (Some(left), None) => core_pad_right(
                &left,
                right_width,
                row_id as u64,
                || SqlValue::Null,
                Row::new,
            ),
            (None, Some(right)) => core_pad_left(
                &right,
                left_width,
                row_id as u64,
                || SqlValue::Null,
                Row::new,
            ),
            (None, None) => unreachable!("core join never emits an empty join pair"),
        })
        .collect()
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
