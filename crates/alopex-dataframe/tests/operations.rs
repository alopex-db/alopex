use std::sync::Arc;

use alopex_dataframe::expr::{col, lit};
use alopex_dataframe::{DataFrame, Series};
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
fn select_supports_column_reorder_and_alias() {
    let df = df();
    let out = df.select(vec![col("b"), col("a").alias("aa")]).unwrap();
    assert_eq!(out.schema().fields()[0].name(), "b");
    assert_eq!(out.schema().fields()[1].name(), "aa");
    assert_eq!(out.height(), 3);
}

#[test]
fn filter_filters_rows() {
    let df = df();
    let out = df.filter(col("a").gt(lit(1_i64))).unwrap();
    assert_eq!(out.height(), 2);
    let a = out.column("a").unwrap();
    assert_eq!(a.len(), 2);
}

#[test]
fn with_columns_adds_and_overwrites() {
    let df = df();
    let out = df
        .with_columns(vec![
            col("a").add(col("b")).alias("c"),
            col("a").add(lit(1_i64)).alias("a"),
        ])
        .unwrap();

    assert_eq!(out.schema().fields()[0].name(), "a");
    assert_eq!(out.schema().fields()[1].name(), "b");
    assert_eq!(out.schema().fields()[2].name(), "c");
    assert_eq!(out.height(), 3);
}

#[test]
fn lazy_and_eager_operations_match() {
    let df = df();
    let eager = df
        .filter(col("a").gt(lit(1_i64)))
        .unwrap()
        .select(vec![col("b").alias("bb")])
        .unwrap();
    let lazy = df
        .lazy()
        .filter(col("a").gt(lit(1_i64)))
        .select(vec![col("b").alias("bb")])
        .collect()
        .unwrap();

    assert_eq!(eager.to_arrow(), lazy.to_arrow());
}
