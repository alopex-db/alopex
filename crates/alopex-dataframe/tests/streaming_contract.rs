use std::num::NonZeroUsize;
use std::sync::Arc;

use alopex_dataframe::physical::budget::StreamTerminal;
use alopex_dataframe::{DataFrame, DataFrameError, LazyFrame, Series, StreamOptions};
use arrow::array::{ArrayRef, Int64Array};

fn options() -> StreamOptions {
    StreamOptions::new(
        1024 * 1024,
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    )
}

fn csv_stream(path: &std::path::Path) -> alopex_dataframe::DataFrameStream {
    LazyFrame::scan_csv(path)
        .unwrap()
        .collect_streaming(options())
        .unwrap()
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn csv_stream_terminal_states_are_repeatable_and_release_all_budget_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.csv");
    std::fs::write(&path, "a\n1\n2\n").unwrap();

    let mut exhausted = csv_stream(&path);
    assert!(exhausted.next_batch().unwrap().is_some());
    assert!(exhausted.next_batch().unwrap().is_some());
    assert!(exhausted.next_batch().unwrap().is_none());
    assert!(exhausted.next_batch().unwrap().is_none());
    assert_eq!(exhausted.status(), StreamTerminal::Exhausted);
    assert_eq!(exhausted.budget().usage().reserved_bytes, 0);
    assert_eq!(exhausted.budget().usage().reserved_batches, 0);

    let mut closed = csv_stream(&path);
    closed.close().unwrap();
    closed.close().unwrap();
    assert_eq!(closed.status(), StreamTerminal::Closed);
    assert!(matches!(
        closed.next_batch(),
        Err(DataFrameError::StreamClosed)
    ));
    assert_eq!(closed.budget().usage().reserved_bytes, 0);
    assert_eq!(closed.budget().usage().reserved_batches, 0);

    let mut cancelled = csv_stream(&path);
    cancelled.cancel().unwrap();
    cancelled.cancel().unwrap();
    assert_eq!(cancelled.status(), StreamTerminal::Cancelled);
    assert!(matches!(
        cancelled.next_batch(),
        Err(DataFrameError::StreamCancelled)
    ));
    assert_eq!(cancelled.budget().usage().reserved_bytes, 0);
    assert_eq!(cancelled.budget().usage().reserved_batches, 0);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn unsupported_materialized_source_and_deferred_concat_schema_mismatch_fail_before_stream_output() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    let in_memory = DataFrame::new(vec![Series::from_arrow("a", vec![values]).unwrap()]).unwrap();
    let unsupported = match in_memory.lazy().collect_streaming(options()) {
        Ok(_) => panic!("in-memory dataframe scans must not return a streaming source"),
        Err(error) => error,
    };
    assert!(matches!(
        unsupported,
        DataFrameError::StreamingUnsupported { ref subject, ref reason }
            if subject == "dataframe_scan" && reason == "legacy_materialized_source"
    ));

    let directory = tempfile::tempdir().unwrap();
    let numeric = directory.path().join("numeric.csv");
    let text = directory.path().join("text.csv");
    std::fs::write(&numeric, "a\n1\n").unwrap();
    std::fs::write(&text, "a\nnot-a-number\n").unwrap();
    let result = LazyFrame::concat(vec![
        LazyFrame::scan_csv(&numeric).unwrap(),
        LazyFrame::scan_csv(&text).unwrap(),
    ])
    .unwrap()
    .collect_streaming(options());
    let error = match result {
        Ok(_) => panic!("mismatched deferred concat must fail before returning a stream"),
        Err(error) => error,
    };
    assert!(matches!(error, DataFrameError::SchemaMismatch { .. }));
    assert!(error.to_string().contains("concat_schema_mismatch"));
}
