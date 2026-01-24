use std::sync::Arc;

use alopex_dataframe::{DataFrame, DataFrameError, Series};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};

fn s_i64(name: &str, values: Vec<Option<i64>>) -> Series {
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

fn s_str(name: &str, values: Vec<&str>) -> Series {
    let array: ArrayRef = Arc::new(StringArray::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

#[test]
fn sort_is_stable_and_nulls_last() {
    let df = DataFrame::new(vec![
        s_i64("a", vec![Some(2), None, Some(1), Some(1)]),
        s_i64("b", vec![Some(10), Some(40), Some(20), Some(30)]),
    ])
    .unwrap();

    let out = df.sort(vec!["a".to_string()], vec![false]).unwrap();
    let a = out.column("a").unwrap().to_arrow();
    let b = out.column("b").unwrap().to_arrow();

    let a = a[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let b = b[0].as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(a.value(0), 1);
    assert_eq!(a.value(1), 1);
    assert_eq!(a.value(2), 2);
    assert!(a.is_null(3));

    assert_eq!(b.value(0), 20);
    assert_eq!(b.value(1), 30);
    assert_eq!(b.value(2), 10);
    assert_eq!(b.value(3), 40);
}

#[test]
fn sort_multiple_columns_with_descending() {
    let df = DataFrame::new(vec![
        s_str("category", vec!["a", "a", "b", "b"]),
        s_i64("score", vec![Some(1), Some(2), Some(1), Some(2)]),
    ])
    .unwrap();

    let out = df
        .sort(
            vec!["category".to_string(), "score".to_string()],
            vec![false, true],
        )
        .unwrap();
    let category = out.column("category").unwrap().to_arrow();
    let score = out.column("score").unwrap().to_arrow();

    let category = category[0].as_any().downcast_ref::<StringArray>().unwrap();
    let score = score[0].as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(category.value(0), "a");
    assert_eq!(score.value(0), 2);
    assert_eq!(category.value(1), "a");
    assert_eq!(score.value(1), 1);
    assert_eq!(category.value(2), "b");
    assert_eq!(score.value(2), 2);
    assert_eq!(category.value(3), "b");
    assert_eq!(score.value(3), 1);
}

#[test]
fn sort_errors_on_missing_column() {
    let df = DataFrame::new(vec![s_i64("a", vec![Some(1)])]).unwrap();
    let err = df
        .sort(vec!["missing".to_string()], vec![false])
        .unwrap_err();
    assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
}

#[test]
fn sort_errors_on_descending_length_mismatch() {
    let df = DataFrame::new(vec![s_i64("a", vec![Some(1)]), s_i64("b", vec![Some(2)])]).unwrap();
    let err = df
        .sort(vec!["a".to_string(), "b".to_string()], vec![true])
        .unwrap_err();
    assert!(matches!(err, DataFrameError::InvalidOperation { .. }));
}

#[test]
fn head_tail_handle_boundaries() {
    let df = DataFrame::new(vec![s_i64("a", vec![Some(1), Some(2)])]).unwrap();
    let head = df.head(0).unwrap();
    assert_eq!(head.height(), 0);

    let tail = df.tail(5).unwrap();
    assert_eq!(tail.height(), 2);
}
