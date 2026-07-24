use std::sync::Arc;

use alopex_dataframe::{col, lit, DataFrame, LazyFrame, Series, StreamOptions};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListBuilder, StringArray, StringBuilder,
    TimestampMicrosecondArray, UInt64Array,
};

const I10B_REGISTER: [&str; 23] = [
    "str.to_lowercase",
    "str.to_uppercase",
    "str.contains",
    "str.replace",
    "str.strip_chars",
    "str.split",
    "str.len_chars",
    "str.extract",
    "dt.year",
    "dt.month",
    "dt.day",
    "dt.weekday",
    "dt.to_string",
    "dt.convert_time_zone",
    "list.join",
    "list.len",
    "list.contains",
    "collect",
    "collect_with_options",
    "collect_streaming",
    "explain",
    "cse",
    "concat",
];

#[derive(Debug)]
struct StatusRow {
    operation: &'static str,
    passed: bool,
}

fn string_values(df: &DataFrame, column: &str) -> Vec<String> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<StringArray>().unwrap();
    (0..values.len())
        .map(|index| values.value(index).to_owned())
        .collect()
}

fn bool_values(df: &DataFrame, column: &str) -> Vec<bool> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<BooleanArray>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn u64_values(df: &DataFrame, column: &str) -> Vec<u64> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<UInt64Array>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn stream_options() -> StreamOptions {
    StreamOptions::default()
}

fn string_dataframe() -> DataFrame {
    DataFrame::new(vec![Series::from_arrow(
        "name",
        vec![Arc::new(StringArray::from(vec![" A1 ", "beta"])) as ArrayRef],
    )
    .unwrap()])
    .unwrap()
}

fn timestamp_dataframe() -> DataFrame {
    DataFrame::new(vec![Series::from_arrow(
        "ts",
        vec![Arc::new(TimestampMicrosecondArray::from(vec![
            0_i64,
            1_704_067_200_123_000,
        ])) as ArrayRef],
    )
    .unwrap()])
    .unwrap()
}

fn list_dataframe() -> DataFrame {
    let mut lists = ListBuilder::new(StringBuilder::new());
    for values in [vec!["db", "rust"], vec!["db", ""]] {
        for value in values {
            lists.values().append_value(value);
        }
        lists.append(true);
    }
    DataFrame::new(vec![Series::from_arrow(
        "tags",
        vec![Arc::new(lists.finish()) as ArrayRef],
    )
    .unwrap()])
    .unwrap()
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn i10b_every_namespace_and_terminal_has_a_status_row() {
    let strings = string_dataframe()
        .select(vec![
            col("name").str().to_lowercase().alias("lower"),
            col("name").str().to_uppercase().alias("upper"),
            col("name").str().contains(r"\d").alias("contains"),
            col("name").str().replace(r"\d", "#").alias("replace"),
            col("name").str().strip_chars(None::<String>).alias("strip"),
            col("name").str().split("a").alias("split"),
            col("name").str().len_chars().alias("length"),
            col("name")
                .str()
                .extract(r"([A-Za-z]+)", 1)
                .alias("extract"),
        ])
        .unwrap();
    let datetimes = timestamp_dataframe()
        .select(vec![
            col("ts").dt().year().alias("year"),
            col("ts").dt().month().alias("month"),
            col("ts").dt().day().alias("day"),
            col("ts").dt().weekday().alias("weekday"),
            col("ts").dt().to_string().alias("text"),
            col("ts")
                .dt()
                .convert_time_zone("Z", "+09:00")
                .alias("tokyo"),
        ])
        .unwrap();
    let lists = list_dataframe()
        .select(vec![
            col("tags").list().join("|", Some("NULL")).alias("join"),
            col("tags").list().len().alias("length"),
            col("tags").list().contains("db").alias("contains"),
        ])
        .unwrap();

    let directory = tempfile::tempdir().unwrap();
    let csv = directory.path().join("stream.csv");
    std::fs::write(&csv, "a\n1\n2\n").unwrap();
    let lazy = LazyFrame::scan_csv(&csv).unwrap();
    let collected = lazy.clone().collect().unwrap();
    let bounded = lazy.clone().collect_with_options(stream_options()).unwrap();
    let mut stream = lazy.clone().collect_streaming(stream_options()).unwrap();
    let mut streamed_rows = 0;
    while let Some(batch) = stream.next_batch().unwrap() {
        streamed_rows += batch.height();
    }
    let explanation = lazy.explain(true);

    let cse_input = DataFrame::new(vec![Series::from_arrow(
        "a",
        vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
    )
    .unwrap()])
    .unwrap();
    let repeated = col("a").add(lit(1_i64));
    let cse = cse_input
        .lazy()
        .select(vec![
            repeated.clone().alias("once"),
            repeated.alias("twice"),
        ])
        .collect()
        .unwrap();
    let concatenated = DataFrame::concat(vec![cse_input.clone(), cse_input.clone()]).unwrap();
    let converted = datetimes.column("tokyo").unwrap().to_arrow();
    let converted = converted[0]
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();

    let rows = [
        StatusRow {
            operation: "str.to_lowercase",
            passed: string_values(&strings, "lower") == [" a1 ", "beta"],
        },
        StatusRow {
            operation: "str.to_uppercase",
            passed: string_values(&strings, "upper") == [" A1 ", "BETA"],
        },
        StatusRow {
            operation: "str.contains",
            passed: bool_values(&strings, "contains") == [true, false],
        },
        StatusRow {
            operation: "str.replace",
            passed: string_values(&strings, "replace") == [" A# ", "beta"],
        },
        StatusRow {
            operation: "str.strip_chars",
            passed: string_values(&strings, "strip") == ["A1", "beta"],
        },
        StatusRow {
            operation: "str.split",
            passed: strings.column("split").is_ok(),
        },
        StatusRow {
            operation: "str.len_chars",
            passed: u64_values(&strings, "length") == [4, 4],
        },
        StatusRow {
            operation: "str.extract",
            passed: string_values(&strings, "extract") == ["A", "beta"],
        },
        StatusRow {
            operation: "dt.year",
            passed: i32_values(&datetimes, "year") == [1970, 2024],
        },
        StatusRow {
            operation: "dt.month",
            passed: u64_values(&datetimes, "month") == [1, 1],
        },
        StatusRow {
            operation: "dt.day",
            passed: u64_values(&datetimes, "day") == [1, 1],
        },
        StatusRow {
            operation: "dt.weekday",
            passed: u64_values(&datetimes, "weekday") == [4, 1],
        },
        StatusRow {
            operation: "dt.to_string",
            passed: string_values(&datetimes, "text")
                == ["1970-01-01T00:00:00Z", "2024-01-01T00:00:00.123Z"],
        },
        StatusRow {
            operation: "dt.convert_time_zone",
            passed: converted.value(0) == 32_400_000_000,
        },
        StatusRow {
            operation: "list.join",
            passed: string_values(&lists, "join") == ["db|rust", "db|"],
        },
        StatusRow {
            operation: "list.len",
            passed: u64_values(&lists, "length") == [2, 2],
        },
        StatusRow {
            operation: "list.contains",
            passed: bool_values(&lists, "contains") == [true, true],
        },
        StatusRow {
            operation: "collect",
            passed: collected.height() == 2,
        },
        StatusRow {
            operation: "collect_with_options",
            passed: bounded.height() == 2,
        },
        StatusRow {
            operation: "collect_streaming",
            passed: streamed_rows == 2,
        },
        StatusRow {
            operation: "explain",
            passed: explanation.contains("scan[csv"),
        },
        StatusRow {
            operation: "cse",
            passed: int_values(&cse, "once") == int_values(&cse, "twice"),
        },
        StatusRow {
            operation: "concat",
            passed: concatenated.height() == 4 && concatenated.width() == cse_input.width(),
        },
    ];
    let names: Vec<_> = rows.iter().map(|row| row.operation).collect();
    assert_eq!(names, I10B_REGISTER, "the I-10b operation register drifted");
    for row in rows {
        assert!(
            row.passed,
            "{} must retain its verified behavior",
            row.operation
        );
    }
}

fn int_values(df: &DataFrame, column: &str) -> Vec<i64> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<Int64Array>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn i32_values(df: &DataFrame, column: &str) -> Vec<i32> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<Int32Array>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}
