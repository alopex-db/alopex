use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListBuilder, StringArray, StringBuilder,
    TimestampMicrosecondArray, UInt64Array,
};

use alopex_dataframe::{col, DataFrame, Series};

fn utf8_series(name: &str, values: Vec<Option<&str>>) -> Series {
    Series::from_arrow(name, vec![Arc::new(StringArray::from(values)) as ArrayRef]).unwrap()
}

fn timestamp_series(name: &str, values: Vec<Option<i64>>) -> Series {
    Series::from_arrow(
        name,
        vec![Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef],
    )
    .unwrap()
}

fn strings(array: &ArrayRef) -> Vec<Option<String>> {
    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
    (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx).to_string())
            }
        })
        .collect()
}

fn bools(array: &ArrayRef) -> Vec<Option<bool>> {
    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect()
}

fn i32s(array: &ArrayRef) -> Vec<Option<i32>> {
    let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
    (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect()
}

fn u64s(array: &ArrayRef) -> Vec<Option<u64>> {
    let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect()
}

fn list_utf8(values: Vec<Option<Vec<Option<&str>>>>) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for list in values {
        match list {
            Some(items) => {
                for item in items {
                    match item {
                        Some(value) => builder.values().append_value(value),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

#[test]
fn string_namespace_works_for_eager_and_lazy() {
    let df = DataFrame::new(vec![utf8_series(
        "name",
        vec![Some(" Alopex "), None, Some("Straße42")],
    )])
    .unwrap();

    let exprs = vec![
        col("name").str().to_lowercase().alias("lower"),
        col("name").str().contains(r"\d+$").alias("has_digits"),
        col("name").str().replace(r"\d+", "#").alias("masked"),
        col("name").str().len_chars().alias("chars"),
    ];
    let eager = df.select(exprs.clone()).unwrap();
    let lazy = df.lazy().select(exprs).collect().unwrap();

    assert_eq!(
        strings(&eager.column("lower").unwrap().to_arrow()[0]),
        vec![
            Some(" alopex ".to_string()),
            None,
            Some("straße42".to_string())
        ]
    );
    assert_eq!(
        strings(&lazy.column("masked").unwrap().to_arrow()[0]),
        vec![
            Some(" Alopex ".to_string()),
            None,
            Some("Straße#".to_string())
        ]
    );
    assert_eq!(
        bools(&eager.column("has_digits").unwrap().to_arrow()[0]),
        vec![Some(false), None, Some(true)]
    );
    assert_eq!(
        u64s(&lazy.column("chars").unwrap().to_arrow()[0]),
        vec![Some(8), None, Some(8)]
    );
}

#[test]
fn datetime_namespace_works_for_timestamp_micros() {
    let df = DataFrame::new(vec![timestamp_series(
        "ts",
        vec![Some(0), Some(1_704_067_200_123_000), None],
    )])
    .unwrap();

    let out = df
        .lazy()
        .select(vec![
            col("ts").dt().year().alias("year"),
            col("ts").dt().weekday().alias("weekday"),
            col("ts").dt().to_string().alias("text"),
            col("ts")
                .dt()
                .convert_time_zone("Z", "+09:00")
                .alias("tokyo"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        i32s(&out.column("year").unwrap().to_arrow()[0]),
        vec![Some(1970), Some(2024), None]
    );
    assert_eq!(
        u64s(&out.column("weekday").unwrap().to_arrow()[0]),
        vec![Some(4), Some(1), None]
    );
    assert_eq!(
        strings(&out.column("text").unwrap().to_arrow()[0]),
        vec![
            Some("1970-01-01T00:00:00Z".to_string()),
            Some("2024-01-01T00:00:00.123Z".to_string()),
            None
        ]
    );
    let tokyo = out.column("tokyo").unwrap().to_arrow()[0]
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .clone();
    assert_eq!(tokyo.value(0), 32_400_000_000);
}

#[test]
fn list_namespace_accepts_string_split_output() {
    let df = DataFrame::new(vec![utf8_series(
        "tags",
        vec![Some("db,rust"), None, Some("db,")],
    )])
    .unwrap();

    let out = df
        .lazy()
        .with_columns(vec![col("tags").str().split(",").alias("parts")])
        .select(vec![
            col("parts").list().join("|", Some("NULL")).alias("joined"),
            col("parts").list().len().alias("len"),
            col("parts").list().contains("db").alias("has_db"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        strings(&out.column("joined").unwrap().to_arrow()[0]),
        vec![Some("db|rust".to_string()), None, Some("db|".to_string())]
    );
    assert_eq!(
        u64s(&out.column("len").unwrap().to_arrow()[0]),
        vec![Some(2), None, Some(2)]
    );
    assert_eq!(
        bools(&out.column("has_db").unwrap().to_arrow()[0]),
        vec![Some(true), None, Some(true)]
    );
}

#[test]
fn implode_and_explode_are_deterministic_for_eager_and_lazy() {
    let df = DataFrame::new(vec![utf8_series("word", vec![Some("a"), None, Some("c")])]).unwrap();

    let eager = df.implode().unwrap().explode("word").unwrap();
    let lazy = df.lazy().implode().explode("word").collect().unwrap();

    assert_eq!(
        strings(&eager.column("word").unwrap().to_arrow()[0]),
        vec![Some("a".to_string()), None, Some("c".to_string())]
    );
    assert_eq!(
        strings(&lazy.column("word").unwrap().to_arrow()[0]),
        vec![Some("a".to_string()), None, Some("c".to_string())]
    );

    let list_df = DataFrame::new(vec![
        utf8_series("id", vec![Some("x"), Some("y"), Some("z")]),
        Series::from_arrow(
            "items",
            vec![list_utf8(vec![
                Some(vec![Some("a"), Some("b")]),
                Some(Vec::new()),
                None,
            ])],
        )
        .unwrap(),
    ])
    .unwrap();
    let exploded = list_df.explode("items").unwrap();
    assert_eq!(
        strings(&exploded.column("id").unwrap().to_arrow()[0]),
        vec![
            Some("x".to_string()),
            Some("x".to_string()),
            Some("y".to_string()),
            Some("z".to_string())
        ]
    );
    assert_eq!(
        strings(&exploded.column("items").unwrap().to_arrow()[0]),
        vec![Some("a".to_string()), Some("b".to_string()), None, None]
    );
}
