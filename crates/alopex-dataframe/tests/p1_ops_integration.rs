use std::sync::Arc;

use alopex_dataframe::{DataFrame, FillNullStrategy, JoinType, Series, SortOptions};
use arrow::array::{ArrayRef, Int64Array, StringArray};

fn s_i64(name: &str, values: Vec<Option<i64>>) -> Series {
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

fn s_str(name: &str, values: Vec<Option<&str>>) -> Series {
    let array: ArrayRef = Arc::new(StringArray::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

fn left_df() -> DataFrame {
    DataFrame::new(vec![
        s_i64("id", vec![Some(1), Some(2), Some(3), Some(4)]),
        s_str("group", vec![None, Some("g1"), Some("g1"), Some("g2")]),
        s_i64("value", vec![Some(10), None, Some(30), Some(40)]),
    ])
    .unwrap()
}

fn right_df() -> DataFrame {
    DataFrame::new(vec![
        s_i64("id", vec![Some(1), Some(2), Some(3)]),
        s_i64("score", vec![Some(50), Some(100), Some(200)]),
    ])
    .unwrap()
}

#[test]
fn p1_ops_pipeline_eager_matches_lazy() {
    let left = left_df();
    let right = right_df();

    let eager = left
        .join(&right, vec!["id".to_string()], JoinType::Left)
        .unwrap()
        .sort(vec!["id".to_string()], vec![false])
        .unwrap()
        .unique(Some(vec!["id".to_string()]))
        .unwrap()
        .fill_null(FillNullStrategy::Forward)
        .unwrap()
        .drop_nulls(Some(vec!["group".to_string()]))
        .unwrap();

    let options = SortOptions {
        by: vec!["id".to_string()],
        descending: vec![false],
        nulls_last: true,
        stable: true,
    };

    let lazy = left
        .lazy()
        .join(right.lazy(), vec!["id".to_string()], JoinType::Left)
        .sort(options)
        .unique(Some(vec!["id".to_string()]))
        .fill_null(FillNullStrategy::Forward)
        .drop_nulls(Some(vec!["group".to_string()]))
        .collect()
        .unwrap();

    assert_eq!(eager.to_arrow(), lazy.to_arrow());
}

#[test]
fn p1_null_count_matches_lazy() {
    let left = left_df();
    let right = right_df();

    let eager = left
        .join(&right, vec!["id".to_string()], JoinType::Left)
        .unwrap()
        .null_count()
        .unwrap();

    let lazy = left
        .lazy()
        .join(right.lazy(), vec!["id".to_string()], JoinType::Left)
        .null_count()
        .collect()
        .unwrap();

    assert_eq!(eager.to_arrow(), lazy.to_arrow());
}
