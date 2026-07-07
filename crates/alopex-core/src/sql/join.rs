//! Generic JOIN execution primitives.
//!
//! This module contains type-independent JOIN control flow. Callers provide
//! concrete row storage, predicate evaluation, key extraction, row construction,
//! and null value construction.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// A generic joined row pair emitted by JOIN control-flow primitives.
pub type JoinPair<T> = (Option<T>, Option<T>);

/// Generic joined row pairs emitted by JOIN control-flow primitives.
pub type JoinPairs<T> = Vec<JoinPair<T>>;

/// Minimal row interface required by generic JOIN row-combination helpers.
pub trait RowLike {
    /// Cell value type stored by the row.
    type Value: Clone;

    /// Return the row values in column order.
    fn values(&self) -> &[Self::Value];

    /// Return the number of columns in the row.
    fn arity(&self) -> usize {
        self.values().len()
    }
}

/// Combine left and right row values into a caller-owned output row.
pub fn combine_rows<R, O>(
    left: &R,
    right: &R,
    row_id: u64,
    build_row: impl FnOnce(u64, Vec<R::Value>) -> O,
) -> O
where
    R: RowLike,
{
    let mut values = Vec::with_capacity(left.arity() + right.arity());
    values.extend(left.values().iter().cloned());
    values.extend(right.values().iter().cloned());
    build_row(row_id, values)
}

/// Combine a left row with right-side null padding.
pub fn pad_right<R, O, N>(
    left: &R,
    right_width: usize,
    row_id: u64,
    mut null_value: N,
    build_row: impl FnOnce(u64, Vec<R::Value>) -> O,
) -> O
where
    R: RowLike,
    N: FnMut() -> R::Value,
{
    let mut values = Vec::with_capacity(left.arity() + right_width);
    values.extend(left.values().iter().cloned());
    values.extend((0..right_width).map(|_| null_value()));
    build_row(row_id, values)
}

/// Combine a right row with left-side null padding.
pub fn pad_left<R, O, N>(
    right: &R,
    left_width: usize,
    row_id: u64,
    mut null_value: N,
    build_row: impl FnOnce(u64, Vec<R::Value>) -> O,
) -> O
where
    R: RowLike,
    N: FnMut() -> R::Value,
{
    let mut values = Vec::with_capacity(left_width + right.arity());
    values.extend((0..left_width).map(|_| null_value()));
    values.extend(right.values().iter().cloned());
    build_row(row_id, values)
}

/// Execute a generic nested-loop join.
///
/// `include_unmatched_left` and `include_unmatched_right` model LEFT/RIGHT/FULL
/// outer-row emission while leaving concrete join-type dispatch to callers.
pub fn nested_loop_join<T, E, P>(
    left: &[T],
    right: &[T],
    include_unmatched_left: bool,
    include_unmatched_right: bool,
    mut matches: P,
) -> Result<JoinPairs<T>, E>
where
    T: Clone,
    P: FnMut(&T, &T) -> Result<bool, E>,
{
    let mut output = Vec::new();
    let mut matched_right = HashSet::new();

    for left_row in left {
        let mut matched_left = false;
        for (right_idx, right_row) in right.iter().enumerate() {
            if matches(left_row, right_row)? {
                matched_left = true;
                matched_right.insert(right_idx);
                output.push((Some(left_row.clone()), Some(right_row.clone())));
            }
        }
        if !matched_left && include_unmatched_left {
            output.push((Some(left_row.clone()), None));
        }
    }

    if include_unmatched_right {
        for (right_idx, right_row) in right.iter().enumerate() {
            if !matched_right.contains(&right_idx) {
                output.push((None, Some(right_row.clone())));
            }
        }
    }

    Ok(output)
}

/// Execute a generic hash join for equi-joins.
///
/// Callers provide key extraction so SQL-specific value and error types remain
/// outside `alopex-core`.
pub fn hash_join<T, K, E, L, R>(
    left: &[T],
    right: &[T],
    include_unmatched_left: bool,
    include_unmatched_right: bool,
    mut left_key: L,
    mut right_key: R,
) -> Result<JoinPairs<T>, E>
where
    T: Clone,
    K: Eq + Hash,
    L: FnMut(&T) -> Result<K, E>,
    R: FnMut(&T) -> Result<K, E>,
{
    let mut buckets: HashMap<K, Vec<(usize, &T)>> = HashMap::new();
    for (idx, right_row) in right.iter().enumerate() {
        let key = right_key(right_row)?;
        buckets.entry(key).or_default().push((idx, right_row));
    }

    let mut output = Vec::new();
    let mut matched_right = HashSet::new();
    for left_row in left {
        let key = left_key(left_row)?;
        let mut matched_left = false;
        if let Some(matches) = buckets.get(&key) {
            for (right_idx, right_row) in matches {
                matched_left = true;
                matched_right.insert(*right_idx);
                output.push((Some(left_row.clone()), Some((*right_row).clone())));
            }
        }
        if !matched_left && include_unmatched_left {
            output.push((Some(left_row.clone()), None));
        }
    }

    if include_unmatched_right {
        for (right_idx, right_row) in right.iter().enumerate() {
            if !matched_right.contains(&right_idx) {
                output.push((None, Some(right_row.clone())));
            }
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{combine_rows, hash_join, nested_loop_join, pad_left, pad_right, RowLike};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestRow {
        row_id: u64,
        values: Vec<Option<i64>>,
    }

    impl TestRow {
        fn new(row_id: u64, values: Vec<Option<i64>>) -> Self {
            Self { row_id, values }
        }
    }

    impl RowLike for TestRow {
        type Value = Option<i64>;

        fn values(&self) -> &[Self::Value] {
            &self.values
        }
    }

    fn rows(values: &[&[i64]]) -> Vec<TestRow> {
        values
            .iter()
            .enumerate()
            .map(|(idx, row)| TestRow::new(idx as u64, row.iter().copied().map(Some).collect()))
            .collect()
    }

    #[test]
    fn combine_and_padding_build_join_rows() {
        let left = TestRow::new(10, vec![Some(1), Some(2)]);
        let right = TestRow::new(20, vec![Some(3)]);

        assert_eq!(
            combine_rows(&left, &right, 0, TestRow::new),
            TestRow::new(0, vec![Some(1), Some(2), Some(3)])
        );
        assert_eq!(
            pad_right(&left, 2, 1, || None, TestRow::new),
            TestRow::new(1, vec![Some(1), Some(2), None, None])
        );
        assert_eq!(
            pad_left(&right, 2, 2, || None, TestRow::new),
            TestRow::new(2, vec![None, None, Some(3)])
        );
    }

    #[test]
    fn nested_loop_join_emits_inner_matches() {
        let left = rows(&[&[1, 10], &[2, 20]]);
        let right = rows(&[&[1, 100], &[3, 300]]);

        let output = nested_loop_join(&left, &right, false, false, |left, right| {
            Ok::<_, ()>(left.values[0] == right.values[0])
        })
        .unwrap();

        assert_eq!(
            output,
            vec![(Some(left[0].clone()), Some(right[0].clone()))]
        );
    }

    #[test]
    fn nested_loop_join_emits_left_right_and_full_rows() {
        let left = rows(&[&[1], &[2]]);
        let right = rows(&[&[1], &[3]]);

        let left_output = nested_loop_join(&left, &right, true, false, |left, right| {
            Ok::<_, ()>(left.values[0] == right.values[0])
        })
        .unwrap();
        assert_eq!(
            left_output,
            vec![
                (Some(left[0].clone()), Some(right[0].clone())),
                (Some(left[1].clone()), None),
            ]
        );

        let right_output = nested_loop_join(&left, &right, false, true, |left, right| {
            Ok::<_, ()>(left.values[0] == right.values[0])
        })
        .unwrap();
        assert_eq!(
            right_output,
            vec![
                (Some(left[0].clone()), Some(right[0].clone())),
                (None, Some(right[1].clone())),
            ]
        );

        let full_output = nested_loop_join(&left, &right, true, true, |left, right| {
            Ok::<_, ()>(left.values[0] == right.values[0])
        })
        .unwrap();
        assert_eq!(
            full_output,
            vec![
                (Some(left[0].clone()), Some(right[0].clone())),
                (Some(left[1].clone()), None),
                (None, Some(right[1].clone())),
            ]
        );
    }

    #[test]
    fn hash_join_emits_matches_and_outer_rows() {
        let left = rows(&[&[1, 10], &[2, 20]]);
        let right = rows(&[&[1, 100], &[3, 300]]);

        let output = hash_join(
            &left,
            &right,
            true,
            true,
            |row| Ok::<_, ()>(row.values[0]),
            |row| Ok::<_, ()>(row.values[0]),
        )
        .unwrap();

        assert_eq!(
            output,
            vec![
                (Some(left[0].clone()), Some(right[0].clone())),
                (Some(left[1].clone()), None),
                (None, Some(right[1].clone())),
            ]
        );
    }
}
