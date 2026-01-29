use std::sync::Arc;

use alopex_dataframe::expr::{col, lit};
use alopex_dataframe::io::{
    read_csv_with_options, read_parquet_with_options, CsvReadOptions, ParquetReadOptions,
};
use alopex_dataframe::{read_csv, read_parquet, write_csv, write_parquet, DataFrame, Series};
use arrow::array::{ArrayRef, Int64Array, StringArray};

fn df_no_nulls() -> DataFrame {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "z"]));
    DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
    ])
    .unwrap()
}

#[test]
fn csv_write_then_read_roundtrip() {
    let df = df_no_nulls();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.csv");

    write_csv(&path, &df).unwrap();
    let df2 = read_csv(&path).unwrap();

    assert_eq!(df.schema().as_ref(), df2.schema().as_ref());
    assert_eq!(df.to_arrow(), df2.to_arrow());
}

#[test]
fn csv_read_options_header_and_delimiter() {
    let dir = tempfile::tempdir().unwrap();

    let path_no_header = dir.path().join("no_header.csv");
    std::fs::write(&path_no_header, "1,10\n2,20\n").unwrap();
    let df = read_csv_with_options(
        &path_no_header,
        &CsvReadOptions::default().with_has_header(false),
    )
    .unwrap();
    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 2);

    let path_delim = dir.path().join("semi.csv");
    std::fs::write(&path_delim, "a;b\n1;2\n").unwrap();
    let df_ok = read_csv_with_options(&path_delim, &CsvReadOptions::default().with_delimiter(b';'))
        .unwrap();
    assert_eq!(df_ok.width(), 2);

    let df_wrong = read_csv_with_options(&path_delim, &CsvReadOptions::default()).unwrap();
    assert_eq!(df_wrong.width(), 1);
}

#[test]
fn parquet_write_then_read_roundtrip() {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
    let b: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("z")]));
    let df = DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
    ])
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.parquet");

    write_parquet(&path, &df).unwrap();
    let df2 = read_parquet(&path).unwrap();

    assert_eq!(df.schema().as_ref(), df2.schema().as_ref());
    assert_eq!(df.to_arrow(), df2.to_arrow());
}

#[test]
fn parquet_read_options_columns_and_predicate() {
    let df = df_no_nulls();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.parquet");
    write_parquet(&path, &df).unwrap();

    let opts = ParquetReadOptions::default()
        .with_columns(["b"])
        .with_predicate(col("a").gt(lit(1_i64)));
    let out = read_parquet_with_options(&path, &opts).unwrap();
    assert_eq!(out.width(), 1);
    assert_eq!(out.column("b").unwrap().len(), 2);
}
