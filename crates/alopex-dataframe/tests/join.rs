use std::sync::Arc;

use alopex_dataframe::ops::JoinType;
use alopex_dataframe::{DataFrame, DataFrameError, Series};
use arrow::array::{ArrayRef, Int64Array, StringArray};

fn s_i64(name: &str, values: Vec<i64>) -> Series {
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

fn s_str(name: &str, values: Vec<&str>) -> Series {
    let array: ArrayRef = Arc::new(StringArray::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

fn left_df() -> DataFrame {
    DataFrame::new(vec![
        s_i64("id", vec![1, 2, 3]),
        s_i64("value", vec![10, 20, 30]),
    ])
    .unwrap()
}

fn right_df() -> DataFrame {
    DataFrame::new(vec![
        s_i64("id", vec![2, 3, 4]),
        s_i64("value", vec![200, 300, 400]),
    ])
    .unwrap()
}

#[test]
fn join_inner_suffixes_right_columns() {
    let left = left_df();
    let right = right_df();

    let out = left
        .join(&right, vec!["id".to_string()], JoinType::Inner)
        .unwrap();

    let names: Vec<_> = out.schema().fields().iter().map(|f| f.name()).collect();
    assert_eq!(names, vec!["id", "value", "value_right"]);
    assert_eq!(out.height(), 2);
}

#[test]
fn join_left_keeps_unmatched_rows() {
    let left = left_df();
    let right = right_df();

    let out = left
        .join(&right, vec!["id".to_string()], JoinType::Left)
        .unwrap();

    assert_eq!(out.height(), 3);
    let right_vals = out.column("value_right").unwrap().to_arrow();
    let array = right_vals[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(array.is_null(0));
}

#[test]
fn join_semi_and_anti_return_left_columns_only() {
    let left = left_df();
    let right = right_df();

    let semi = left
        .join(&right, vec!["id".to_string()], JoinType::Semi)
        .unwrap();
    let anti = left
        .join(&right, vec!["id".to_string()], JoinType::Anti)
        .unwrap();

    let semi_names: Vec<_> = semi.schema().fields().iter().map(|f| f.name()).collect();
    let anti_names: Vec<_> = anti.schema().fields().iter().map(|f| f.name()).collect();
    assert_eq!(semi_names, vec!["id", "value"]);
    assert_eq!(anti_names, vec!["id", "value"]);
    assert_eq!(semi.height(), 2);
    assert_eq!(anti.height(), 1);
}

#[test]
fn join_left_right_keys_include_right_key_column() {
    let left = DataFrame::new(vec![
        s_i64("id", vec![1, 2, 3]),
        s_i64("value", vec![10, 20, 30]),
    ])
    .unwrap();
    let right = DataFrame::new(vec![
        s_i64("rid", vec![2, 3, 4]),
        s_i64("value", vec![200, 300, 400]),
    ])
    .unwrap();

    let out = left
        .join(
            &right,
            (vec!["id".to_string()], vec!["rid".to_string()]),
            JoinType::Inner,
        )
        .unwrap();

    let names: Vec<_> = out.schema().fields().iter().map(|f| f.name()).collect();
    assert_eq!(names, vec!["id", "value", "rid", "value_right"]);
}

#[test]
fn join_rejects_mismatched_key_types() {
    let left = DataFrame::new(vec![s_i64("id", vec![1, 2])]).unwrap();
    let right = DataFrame::new(vec![s_str("id", vec!["1", "2"])]).unwrap();

    let err = left
        .join(&right, vec!["id".to_string()], JoinType::Inner)
        .unwrap_err();
    assert!(matches!(err, DataFrameError::TypeMismatch { .. }));
}
