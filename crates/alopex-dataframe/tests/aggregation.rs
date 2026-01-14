use std::collections::HashMap;
use std::sync::Arc;

use alopex_dataframe::expr::col;
use alopex_dataframe::{DataFrame, DataFrameError, Series};
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;

fn df() -> DataFrame {
    let g: ArrayRef = Arc::new(StringArray::from(vec![
        Some("x"),
        Some("x"),
        Some("y"),
        Some("y"),
        Some("y"),
    ]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(10_i64),
        None,
        Some(3),
        Some(7),
        None,
    ]));

    DataFrame::new(vec![
        Series::from_arrow("g", vec![g]).unwrap(),
        Series::from_arrow("v", vec![v]).unwrap(),
    ])
    .unwrap()
}

#[test]
fn group_by_agg_semantics_and_column_order() {
    let out = df()
        .lazy()
        .group_by(vec![col("g")])
        .agg(vec![
            col("v").sum().alias("sum_v"),
            col("v").mean().alias("mean_v"),
            col("v").min().alias("min_v"),
            col("v").max().alias("max_v"),
            col("v").count().alias("cnt_v"),
        ])
        .collect()
        .unwrap();

    let names: Vec<_> = out
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        vec!["g", "sum_v", "mean_v", "min_v", "max_v", "cnt_v"]
    );

    assert_eq!(out.column("mean_v").unwrap().dtype(), DataType::Float64);
    assert_eq!(out.column("cnt_v").unwrap().dtype(), DataType::Int64);

    let sums = group_i64_map(&out, "sum_v");
    let mins = group_i64_map(&out, "min_v");
    let maxs = group_i64_map(&out, "max_v");
    let cnts = group_i64_map(&out, "cnt_v");
    let means = group_f64_map(&out, "mean_v");

    assert_eq!(sums.get("x").copied(), Some(10));
    assert_eq!(mins.get("x").copied(), Some(10));
    assert_eq!(maxs.get("x").copied(), Some(10));
    assert_eq!(cnts.get("x").copied(), Some(1));
    assert_eq!(means.get("x").copied(), Some(10.0));

    assert_eq!(sums.get("y").copied(), Some(10));
    assert_eq!(mins.get("y").copied(), Some(3));
    assert_eq!(maxs.get("y").copied(), Some(7));
    assert_eq!(cnts.get("y").copied(), Some(2));
    assert_eq!(means.get("y").copied(), Some(5.0));
}

#[test]
fn non_numeric_aggregation_is_type_mismatch() {
    let g: ArrayRef = Arc::new(StringArray::from(vec!["x", "x"]));
    let s: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let df = DataFrame::new(vec![
        Series::from_arrow("g", vec![g]).unwrap(),
        Series::from_arrow("s", vec![s]).unwrap(),
    ])
    .unwrap();

    let err = df
        .lazy()
        .group_by(vec![col("g")])
        .agg(vec![col("s").sum().alias("sum_s")])
        .collect()
        .unwrap_err();
    assert!(matches!(err, DataFrameError::TypeMismatch { .. }));
    assert!(err.to_string().contains("expected"));
}

fn group_i64_map(df: &DataFrame, value_col: &str) -> HashMap<String, i64> {
    let keys = df.column("g").unwrap().to_arrow();
    let vals = df.column(value_col).unwrap().to_arrow();
    assert_eq!(keys.len(), 1);
    assert_eq!(vals.len(), 1);

    let keys = keys[0].as_any().downcast_ref::<StringArray>().unwrap();
    let vals = vals[0].as_any().downcast_ref::<Int64Array>().unwrap();

    let mut out = HashMap::new();
    for i in 0..keys.len() {
        out.insert(keys.value(i).to_string(), vals.value(i));
    }
    out
}

fn group_f64_map(df: &DataFrame, value_col: &str) -> HashMap<String, f64> {
    let keys = df.column("g").unwrap().to_arrow();
    let vals = df.column(value_col).unwrap().to_arrow();
    assert_eq!(keys.len(), 1);
    assert_eq!(vals.len(), 1);

    let keys = keys[0].as_any().downcast_ref::<StringArray>().unwrap();
    let vals = vals[0].as_any().downcast_ref::<Float64Array>().unwrap();

    let mut out = HashMap::new();
    for i in 0..keys.len() {
        out.insert(keys.value(i).to_string(), vals.value(i));
    }
    out
}
