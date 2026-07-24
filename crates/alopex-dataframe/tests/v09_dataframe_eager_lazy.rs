use std::collections::BTreeMap;
use std::sync::Arc;

use alopex_dataframe::expr::{col, lit};
use alopex_dataframe::{
    read_csv, read_parquet, scan_csv, scan_parquet, write_csv, write_parquet, DataFrame, Series,
};
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};

const IO_REGISTER: [&str; 6] = [
    "read_csv",
    "read_parquet",
    "write_csv",
    "write_parquet",
    "scan_csv",
    "scan_parquet",
];

const CORE_REGISTER: [&str; 10] = [
    "DataFrame.select",
    "LazyFrame.select",
    "DataFrame.filter",
    "LazyFrame.filter",
    "DataFrame.with_columns",
    "LazyFrame.with_columns",
    "group_by",
    "agg.sum",
    "agg.mean",
    "agg.count",
];

const AGGREGATE_REGISTER: [&str; 5] = ["agg.sum", "agg.mean", "agg.count", "agg.min", "agg.max"];

#[derive(Debug)]
struct StatusRow {
    operation: &'static str,
    passed: bool,
}

fn dataframe() -> DataFrame {
    let group: ArrayRef = Arc::new(StringArray::from(vec!["x", "x", "y", "y"]));
    let value: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 3, 10, 20]));
    DataFrame::new(vec![
        Series::from_arrow("group", vec![group]).unwrap(),
        Series::from_arrow("value", vec![value]).unwrap(),
    ])
    .unwrap()
}

fn assert_success_register(expected: &[&str], rows: &[StatusRow]) {
    let actual: Vec<_> = rows.iter().map(|row| row.operation).collect();
    assert_eq!(actual, expected, "the v0.9 I-08 operation register drifted");
    for row in rows {
        assert!(
            row.passed,
            "{} must be supported and successful",
            row.operation
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn i08_io_register_has_one_successful_status_row_per_named_operation() {
    let input = dataframe();
    let directory = tempfile::tempdir().unwrap();
    let csv_path = directory.path().join("v09.csv");
    let parquet_path = directory.path().join("v09.parquet");

    write_csv(&csv_path, &input).unwrap();
    write_parquet(&parquet_path, &input).unwrap();

    let csv = read_csv(&csv_path).unwrap();
    let parquet = read_parquet(&parquet_path).unwrap();
    let csv_scan = scan_csv(&csv_path).unwrap().collect().unwrap();
    let parquet_scan = scan_parquet(&parquet_path).unwrap().collect().unwrap();

    let rows = [
        StatusRow {
            operation: "read_csv",
            passed: csv.to_arrow() == input.to_arrow(),
        },
        StatusRow {
            operation: "read_parquet",
            passed: parquet.to_arrow() == input.to_arrow(),
        },
        StatusRow {
            operation: "write_csv",
            passed: csv_path.is_file(),
        },
        StatusRow {
            operation: "write_parquet",
            passed: parquet_path.is_file(),
        },
        StatusRow {
            operation: "scan_csv",
            passed: csv_scan.to_arrow() == input.to_arrow(),
        },
        StatusRow {
            operation: "scan_parquet",
            passed: parquet_scan.to_arrow() == input.to_arrow(),
        },
    ];
    assert_success_register(&IO_REGISTER, &rows);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn i08_eager_lazy_core_and_aggregate_registers_have_status_rows() {
    let input = dataframe();

    let eager_select = input.select(vec![col("value").alias("selected")]).unwrap();
    let lazy_select = input
        .lazy()
        .select(vec![col("value").alias("selected")])
        .collect()
        .unwrap();
    let eager_filter = input.filter(col("value").gt(lit(2_i64))).unwrap();
    let lazy_filter = input
        .lazy()
        .filter(col("value").gt(lit(2_i64)))
        .collect()
        .unwrap();
    let eager_columns = input
        .with_columns(vec![col("value").add(lit(1_i64)).alias("next")])
        .unwrap();
    let lazy_columns = input
        .lazy()
        .with_columns(vec![col("value").add(lit(1_i64)).alias("next")])
        .collect()
        .unwrap();
    let grouped = input
        .lazy()
        .group_by(vec![col("group")])
        .agg(vec![
            col("value").sum().alias("sum"),
            col("value").mean().alias("mean"),
            col("value").count().alias("count"),
            col("value").min().alias("min"),
            col("value").max().alias("max"),
        ])
        .collect()
        .unwrap();

    let grouped_i64 = group_i64_columns(&grouped, &["sum", "count", "min", "max"]);
    let grouped_mean = group_f64_column(&grouped, "mean");
    let core_rows = [
        StatusRow {
            operation: "DataFrame.select",
            passed: eager_select.to_arrow() == lazy_select.to_arrow(),
        },
        StatusRow {
            operation: "LazyFrame.select",
            passed: eager_select.width() == 1 && eager_select.height() == input.height(),
        },
        StatusRow {
            operation: "DataFrame.filter",
            passed: eager_filter.to_arrow() == lazy_filter.to_arrow(),
        },
        StatusRow {
            operation: "LazyFrame.filter",
            passed: eager_filter.height() == 3,
        },
        StatusRow {
            operation: "DataFrame.with_columns",
            passed: eager_columns.to_arrow() == lazy_columns.to_arrow(),
        },
        StatusRow {
            operation: "LazyFrame.with_columns",
            passed: eager_columns.column("next").is_ok(),
        },
        StatusRow {
            operation: "group_by",
            passed: grouped.height() == 2 && grouped.width() == 6,
        },
        StatusRow {
            operation: "agg.sum",
            passed: grouped_i64["sum"]
                == BTreeMap::from([("x".to_owned(), 4), ("y".to_owned(), 30)]),
        },
        StatusRow {
            operation: "agg.mean",
            passed: grouped_mean == BTreeMap::from([("x".to_owned(), 2.0), ("y".to_owned(), 15.0)]),
        },
        StatusRow {
            operation: "agg.count",
            passed: grouped_i64["count"]
                == BTreeMap::from([("x".to_owned(), 2), ("y".to_owned(), 2)]),
        },
    ];
    assert_success_register(&CORE_REGISTER, &core_rows);

    let aggregate_rows = [
        StatusRow {
            operation: "agg.sum",
            passed: grouped_i64["sum"]
                == BTreeMap::from([("x".to_owned(), 4), ("y".to_owned(), 30)]),
        },
        StatusRow {
            operation: "agg.mean",
            passed: grouped_mean == BTreeMap::from([("x".to_owned(), 2.0), ("y".to_owned(), 15.0)]),
        },
        StatusRow {
            operation: "agg.count",
            passed: grouped_i64["count"]
                == BTreeMap::from([("x".to_owned(), 2), ("y".to_owned(), 2)]),
        },
        StatusRow {
            operation: "agg.min",
            passed: grouped_i64["min"]
                == BTreeMap::from([("x".to_owned(), 1), ("y".to_owned(), 10)]),
        },
        StatusRow {
            operation: "agg.max",
            passed: grouped_i64["max"]
                == BTreeMap::from([("x".to_owned(), 3), ("y".to_owned(), 20)]),
        },
    ];
    assert_success_register(&AGGREGATE_REGISTER, &aggregate_rows);
}

fn group_i64_columns(df: &DataFrame, columns: &[&str]) -> BTreeMap<String, BTreeMap<String, i64>> {
    let groups = df.column("group").unwrap().to_arrow();
    let groups = groups[0].as_any().downcast_ref::<StringArray>().unwrap();
    columns
        .iter()
        .map(|column| {
            let values = df.column(column).unwrap().to_arrow();
            let values = values[0].as_any().downcast_ref::<Int64Array>().unwrap();
            let rows = (0..groups.len())
                .map(|index| (groups.value(index).to_owned(), values.value(index)))
                .collect();
            ((*column).to_owned(), rows)
        })
        .collect()
}

fn group_f64_column(df: &DataFrame, column: &str) -> BTreeMap<String, f64> {
    let groups = df.column("group").unwrap().to_arrow();
    let groups = groups[0].as_any().downcast_ref::<StringArray>().unwrap();
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
    (0..groups.len())
        .map(|index| (groups.value(index).to_owned(), values.value(index)))
        .collect()
}
