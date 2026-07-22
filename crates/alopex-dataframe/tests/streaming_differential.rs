use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use alopex_dataframe::{
    col, concat_str, lit, write_parquet, ConcatStrNullBehavior, DataFrame, DataFrameStream,
    LazyFrame, Series, StreamOptions,
};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};

fn stream_options(batch_rows: usize) -> StreamOptions {
    StreamOptions::new(
        1024 * 1024,
        NonZeroUsize::new(4).unwrap(),
        NonZeroUsize::new(batch_rows).unwrap(),
    )
}

fn pipeline(path: &Path, parquet: bool) -> LazyFrame {
    let scan = if parquet {
        LazyFrame::scan_parquet(path).unwrap()
    } else {
        LazyFrame::scan_csv(path).unwrap()
    };
    scan.filter(col("a").gt(lit(1_i64))).select(vec![
        col("a").add(lit(1_i64)).alias("a_plus_one"),
        concat_str(
            vec![col("left"), col("right")],
            "-",
            ConcatStrNullBehavior::Propagate,
        )
        .unwrap()
        .alias("label"),
    ])
}

fn drain(mut stream: DataFrameStream) -> DataFrame {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch().unwrap() {
        batches.extend(batch.to_arrow());
    }
    assert!(stream.next_batch().unwrap().is_none());
    assert_eq!(stream.budget().usage().reserved_bytes, 0);
    assert_eq!(stream.budget().usage().reserved_batches, 0);
    DataFrame::from_batches(batches).unwrap()
}

fn int_values(frame: &DataFrame, name: &str) -> Vec<Option<i64>> {
    frame
        .column(name)
        .unwrap()
        .to_arrow()
        .iter()
        .flat_map(|chunk| {
            let values = chunk.as_any().downcast_ref::<Int64Array>().unwrap();
            (0..values.len())
                .map(|index| (!values.is_null(index)).then(|| values.value(index)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn text_values(frame: &DataFrame, name: &str) -> Vec<Option<String>> {
    frame
        .column(name)
        .unwrap()
        .to_arrow()
        .iter()
        .flat_map(|chunk| {
            let values = chunk.as_any().downcast_ref::<StringArray>().unwrap();
            (0..values.len())
                .map(|index| (!values.is_null(index)).then(|| values.value(index).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn assert_pipeline_output(frame: &DataFrame) {
    assert_eq!(frame.schema().fields()[0].name(), "a_plus_one");
    assert_eq!(frame.schema().fields()[1].name(), "label");
    assert_eq!(int_values(frame, "a_plus_one"), vec![Some(3), Some(4)]);
    assert_eq!(
        text_values(frame, "label"),
        vec![
            Some("second-two".to_owned()),
            Some("third-three".to_owned())
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn csv_normal_bounded_and_incremental_modes_match_for_filter_projection_and_concat_str() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.csv");
    std::fs::write(
        &path,
        "a,left,right\n1,first,one\n2,second,two\n3,third,three\n",
    )
    .unwrap();

    let normal = pipeline(&path, false).collect().unwrap();
    let bounded = pipeline(&path, false)
        .collect_with_options(stream_options(1))
        .unwrap();
    let incremental = drain(
        pipeline(&path, false)
            .collect_streaming(stream_options(1))
            .unwrap(),
    );

    assert_eq!(normal.schema().as_ref(), bounded.schema().as_ref());
    assert_eq!(normal.schema().as_ref(), incremental.schema().as_ref());
    assert_pipeline_output(&normal);
    assert_pipeline_output(&bounded);
    assert_pipeline_output(&incremental);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn parquet_normal_bounded_and_incremental_modes_match_for_the_same_expression_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.parquet");
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let left: ArrayRef = Arc::new(StringArray::from(vec!["first", "second", "third"]));
    let right: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
    let frame = DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("left", vec![left]).unwrap(),
        Series::from_arrow("right", vec![right]).unwrap(),
    ])
    .unwrap();
    write_parquet(&path, &frame).unwrap();

    let normal = pipeline(&path, true).collect().unwrap();
    let bounded = pipeline(&path, true)
        .collect_with_options(stream_options(1))
        .unwrap();
    let incremental = drain(
        pipeline(&path, true)
            .collect_streaming(stream_options(1))
            .unwrap(),
    );

    assert_eq!(normal.schema().as_ref(), bounded.schema().as_ref());
    assert_eq!(normal.schema().as_ref(), incremental.schema().as_ref());
    assert_pipeline_output(&normal);
    assert_pipeline_output(&bounded);
    assert_pipeline_output(&incremental);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn deferred_csv_concat_preserves_declared_input_order_in_bounded_and_incremental_modes() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.csv");
    let second = directory.path().join("second.csv");
    std::fs::write(&first, "a\n1\n2\n").unwrap();
    std::fs::write(&second, "a\n3\n4\n").unwrap();

    let normal = LazyFrame::concat(vec![
        LazyFrame::scan_csv(&first).unwrap(),
        LazyFrame::scan_csv(&second).unwrap(),
    ])
    .unwrap()
    .collect()
    .unwrap();
    let bounded = LazyFrame::concat(vec![
        LazyFrame::scan_csv(&first).unwrap(),
        LazyFrame::scan_csv(&second).unwrap(),
    ])
    .unwrap()
    .collect_with_options(stream_options(1))
    .unwrap();
    let incremental = drain(
        LazyFrame::concat(vec![
            LazyFrame::scan_csv(&first).unwrap(),
            LazyFrame::scan_csv(&second).unwrap(),
        ])
        .unwrap()
        .collect_streaming(stream_options(1))
        .unwrap(),
    );

    assert_eq!(normal.schema().as_ref(), bounded.schema().as_ref());
    assert_eq!(normal.schema().as_ref(), incremental.schema().as_ref());
    assert_eq!(
        int_values(&normal, "a"),
        vec![Some(1), Some(2), Some(3), Some(4)]
    );
    assert_eq!(
        int_values(&bounded, "a"),
        vec![Some(1), Some(2), Some(3), Some(4)]
    );
    assert_eq!(
        int_values(&incremental, "a"),
        vec![Some(1), Some(2), Some(3), Some(4)]
    );
}
