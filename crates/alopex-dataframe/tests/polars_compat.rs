use std::sync::Arc;

use alopex_dataframe::expr::{col, lit};
use alopex_dataframe::{DataFrame, DataFrameError, Series};
use arrow::array::{ArrayRef, Int64Array};

fn df() -> DataFrame {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
    DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
    ])
    .unwrap()
}

#[test]
fn column_resolution_is_case_sensitive() {
    let df = df();
    let err = df.column("A").unwrap_err();
    assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
}

#[test]
fn select_preserves_input_expression_order() {
    let df = df();
    let out = df.select(vec![col("b"), col("a")]).unwrap();
    let names: Vec<_> = out
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["b", "a"]);
}

#[test]
fn filter_has_expected_semantics() {
    let df = df();
    let out = df.filter(col("a").gt(lit(1_i64))).unwrap();
    assert_eq!(out.height(), 2);
    assert_eq!(out.schema().fields()[0].name(), "a");
    assert_eq!(out.schema().fields()[1].name(), "b");
}
