use std::sync::Arc;

use alopex_dataframe::{DataFrame, DataFrameError, FillNullStrategy, Series};
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};

fn s_i64(name: &str, values: Vec<Option<i64>>) -> Series {
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

fn s_f64(name: &str, values: Vec<Option<f64>>) -> Series {
    let array: ArrayRef = Arc::new(Float64Array::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

fn s_str(name: &str, values: Vec<Option<&str>>) -> Series {
    let array: ArrayRef = Arc::new(StringArray::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
}

#[test]
fn fill_null_with_scalar_value() {
    let df = DataFrame::new(vec![s_i64("a", vec![Some(1), None, Some(3)])]).unwrap();
    let out = df.fill_null(0_i64).unwrap();
    let a = out.column("a").unwrap().to_arrow();
    let a = a[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(a.value(1), 0);
}

#[test]
fn fill_null_forward_strategy() {
    let df = DataFrame::new(vec![s_i64("a", vec![Some(1), None, Some(3), None])]).unwrap();
    let out = df.fill_null(FillNullStrategy::Forward).unwrap();
    let a = out.column("a").unwrap().to_arrow();
    let a = a[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(a.value(1), 1);
    assert_eq!(a.value(3), 3);
}

#[test]
fn fill_null_mean_strategy_on_float() {
    let df = DataFrame::new(vec![s_f64("a", vec![Some(1.0), None, Some(3.0)])]).unwrap();
    let out = df.fill_null(FillNullStrategy::Mean).unwrap();
    let a = out.column("a").unwrap().to_arrow();
    let a = a[0].as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(a.value(1), 2.0);
}

#[test]
fn drop_nulls_subset_only_drops_target_columns() {
    let df = DataFrame::new(vec![
        s_i64("a", vec![Some(1), None, Some(3)]),
        s_str("b", vec![Some("x"), None, Some("z")]),
    ])
    .unwrap();

    let out = df.drop_nulls(Some(vec!["a".to_string()])).unwrap();
    assert_eq!(out.height(), 2);
}

#[test]
fn null_count_returns_single_row() {
    let df = DataFrame::new(vec![
        s_i64("a", vec![Some(1), None, Some(3)]),
        s_str("b", vec![None, None, Some("z")]),
    ])
    .unwrap();

    let out = df.null_count().unwrap();
    assert_eq!(out.height(), 1);
    let a = out.column("a").unwrap().to_arrow();
    let b = out.column("b").unwrap().to_arrow();
    let a = a[0].as_any().downcast_ref::<UInt64Array>().unwrap();
    let b = b[0].as_any().downcast_ref::<UInt64Array>().unwrap();
    assert_eq!(a.value(0), 1);
    assert_eq!(b.value(0), 2);
}

#[test]
fn fill_null_type_mismatch_errors() {
    let df = DataFrame::new(vec![s_i64("a", vec![Some(1), None])]).unwrap();
    let err = df.fill_null("x").unwrap_err();
    assert!(matches!(err, DataFrameError::TypeMismatch { .. }));
}
