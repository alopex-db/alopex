use std::sync::Arc;

use alopex_dataframe::expr::col;
use alopex_dataframe::io::{read_csv_with_options, CsvReadOptions};
use alopex_dataframe::{DataFrame, DataFrameError, Series};
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[test]
fn column_not_found_is_reported() {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    let df = DataFrame::new(vec![Series::from_arrow("a", vec![a]).unwrap()]).unwrap();
    let err = df.column("missing").unwrap_err();
    assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
    assert!(err.to_string().contains("missing"));
}

#[test]
fn schema_mismatch_duplicate_column_name() {
    let a1: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    let a2: ArrayRef = Arc::new(Int64Array::from(vec![2_i64]));
    let err = DataFrame::new(vec![
        Series::from_arrow("a", vec![a1]).unwrap(),
        Series::from_arrow("a", vec![a2]).unwrap(),
    ])
    .unwrap_err();
    assert!(matches!(err, DataFrameError::SchemaMismatch { .. }));
    assert!(err.to_string().contains("duplicate"));
}

#[test]
fn schema_mismatch_length_mismatch() {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10_i64]));
    let err = DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
    ])
    .unwrap_err();
    assert!(matches!(err, DataFrameError::SchemaMismatch { .. }));
    assert!(err.to_string().contains("length"));
}

#[test]
fn from_batches_schema_mismatch() {
    let s1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let s2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
    let b1 = RecordBatch::try_new(
        s1,
        vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
    )
    .unwrap();
    let b2 = RecordBatch::try_new(
        s2,
        vec![Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef],
    )
    .unwrap();
    let err = DataFrame::from_batches(vec![b1, b2]).unwrap_err();
    assert!(matches!(err, DataFrameError::SchemaMismatch { .. }));
    let msg = err.to_string();
    assert!(msg.contains("schema mismatch"));
    assert!(msg.contains("batch 0"));
    assert!(msg.contains("batch 1"));
}

#[test]
fn type_mismatch_for_mixed_series_dtypes() {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
    let err = Series::from_arrow("x", vec![a, b]).unwrap_err();
    assert!(matches!(err, DataFrameError::TypeMismatch { .. }));
    assert!(err.to_string().contains("expected"));
}

#[test]
fn type_mismatch_for_non_numeric_agg() {
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
    let msg = err.to_string();
    assert!(msg.contains("expected"));
    assert!(msg.contains("numeric"));
    assert!(msg.contains("Utf8"));
}

#[test]
fn configuration_error_for_invalid_csv_delimiter() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.csv");
    std::fs::write(&path, "a,b\n1,2\n").unwrap();

    let opts = CsvReadOptions::default().with_delimiter(b'\0');
    let err = read_csv_with_options(&path, &opts).unwrap_err();
    assert!(matches!(err, DataFrameError::Configuration { .. }));
    assert!(err.to_string().contains("delimiter"));
}
