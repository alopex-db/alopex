use std::collections::HashMap;
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
fn eager_select_matches_lazy() {
    let df = df();
    let eager = df.select(vec![col("b").alias("bb")]).unwrap();
    let lazy = df
        .lazy()
        .select(vec![col("b").alias("bb")])
        .collect()
        .unwrap();
    assert_eq!(eager.to_arrow(), lazy.to_arrow());
}

#[test]
fn eager_filter_matches_lazy() {
    let df = df();
    let eager = df.filter(col("a").gt(lit(1_i64))).unwrap();
    let lazy = df.lazy().filter(col("a").gt(lit(1_i64))).collect().unwrap();
    assert_eq!(eager.to_arrow(), lazy.to_arrow());
}

#[test]
fn eager_with_columns_matches_lazy() {
    let df = df();
    let eager = df
        .with_columns(vec![col("a").add(col("b")).alias("c")])
        .unwrap();
    let lazy = df
        .lazy()
        .with_columns(vec![col("a").add(col("b")).alias("c")])
        .collect()
        .unwrap();
    assert_eq!(eager.to_arrow(), lazy.to_arrow());
}

#[test]
fn eager_group_by_agg_matches_lazy_semantically() {
    let df = df();
    let eager = df
        .group_by(vec![col("a")])
        .agg(vec![col("b").sum().alias("sum_b")])
        .unwrap();
    let lazy = df
        .lazy()
        .group_by(vec![col("a")])
        .agg(vec![col("b").sum().alias("sum_b")])
        .collect()
        .unwrap();

    let eager_map = group_sum_map(&eager);
    let lazy_map = group_sum_map(&lazy);
    assert_eq!(eager_map, lazy_map);
}

fn group_sum_map(df: &DataFrame) -> HashMap<i64, i64> {
    let keys = df.column("a").unwrap().to_arrow();
    let sums = df.column("sum_b").unwrap().to_arrow();
    assert_eq!(keys.len(), 1);
    assert_eq!(sums.len(), 1);

    let keys = keys[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let sums = sums[0].as_any().downcast_ref::<Int64Array>().unwrap();

    let mut out = HashMap::new();
    for i in 0..keys.len() {
        out.insert(keys.value(i), sums.value(i));
    }
    out
}
