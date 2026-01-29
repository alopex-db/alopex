use std::sync::Arc;

use alopex_dataframe::{DataFrame, DataFrameError, Series};
use arrow::array::{ArrayRef, Int64Array};

fn s_i64(name: &str, values: Vec<i64>) -> Series {
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

#[test]
fn unique_keeps_first_occurrence() {
    let df = DataFrame::new(vec![
        s_i64("id", vec![1, 1, 2, 2]),
        s_i64("val", vec![10, 10, 20, 30]),
    ])
    .unwrap();

    let out = df.unique(None).unwrap();
    assert_eq!(out.height(), 3);

    let ids = out.column("id").unwrap().to_arrow();
    let ids = ids[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 2);
}

#[test]
fn unique_subset_keeps_first_per_key() {
    let df = DataFrame::new(vec![
        s_i64("id", vec![1, 1, 2, 2]),
        s_i64("val", vec![10, 11, 20, 30]),
    ])
    .unwrap();

    let out = df.unique(Some(vec!["id".to_string()])).unwrap();
    assert_eq!(out.height(), 2);

    let vals = out.column("val").unwrap().to_arrow();
    let vals = vals[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(vals.value(0), 10);
    assert_eq!(vals.value(1), 20);
}

#[test]
fn unique_errors_on_missing_subset_column() {
    let df = DataFrame::new(vec![s_i64("id", vec![1])]).unwrap();
    let err = df.unique(Some(vec!["missing".to_string()])).unwrap_err();
    assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
}
