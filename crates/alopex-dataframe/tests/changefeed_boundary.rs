use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use alopex_dataframe::changefeed_boundary::{
    dataframe_changefeed_requests, preflight_dataframe_changefeed_request,
    DataFrameChangefeedClassification, DATAFRAME_CHANGEFEED_LIFECYCLES,
    DATAFRAME_CHANGEFEED_TARGETS,
};
use alopex_dataframe::expr::{col, lit};
use alopex_dataframe::{DataFrame, LazyFrame, Series, StreamOptions};
use arrow::array::{ArrayRef, Int64Array};

#[test]
fn every_dataframe_changefeed_lifecycle_and_source_is_rejected_before_execution() {
    let requests: Vec<_> = dataframe_changefeed_requests().collect();
    assert_eq!(
        requests.len(),
        DATAFRAME_CHANGEFEED_LIFECYCLES.len() * DATAFRAME_CHANGEFEED_TARGETS.len()
    );

    let unique: BTreeSet<_> = requests.iter().map(|request| request.id()).collect();
    assert_eq!(unique.len(), requests.len());

    let expected_rows: BTreeSet<_> = DATAFRAME_CHANGEFEED_TARGETS
        .iter()
        .flat_map(|target| {
            DATAFRAME_CHANGEFEED_LIFECYCLES
                .iter()
                .map(move |lifecycle| (target.id(), lifecycle.id()))
        })
        .collect();
    assert_eq!(
        unique, expected_rows,
        "no source or lifecycle may be omitted"
    );

    for request in requests {
        let rejection = preflight_dataframe_changefeed_request(request).unwrap_err();
        assert_eq!(rejection.boundary_version, "v0.9");
        assert_eq!(
            rejection.classification,
            DataFrameChangefeedClassification::PreExecutionUnsupported
        );
        assert_eq!(rejection.code, "dataframe_changefeed_unsupported");
        assert_eq!(
            rejection.reason_code,
            "dataframe_changefeed_surface_unsupported"
        );
        assert_eq!(rejection.canonical_routing_kind, "unsupported");
        assert_eq!(rejection.canonical_failure_class, "invalid_request");
        assert_eq!(rejection.surface_error_code, "changefeed_unsupported");
        assert!(!rejection.retryable);
        assert!(!rejection.execution_started);
    }
}

#[test]
fn local_eager_lazy_and_streaming_behavior_remain_available() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let dataframe =
        DataFrame::new(vec![Series::from_arrow("value", vec![values]).unwrap()]).unwrap();

    let eager = dataframe
        .select(vec![col("value").add(lit(1_i64)).alias("next")])
        .unwrap();
    let lazy = dataframe
        .lazy()
        .select(vec![col("value").add(lit(1_i64)).alias("next")])
        .collect()
        .unwrap();
    assert_eq!(eager.to_arrow(), lazy.to_arrow());

    let directory = tempfile::tempdir().unwrap();
    let csv = directory.path().join("local.csv");
    std::fs::write(&csv, "value\n1\n2\n").unwrap();
    let options = StreamOptions::new(
        1024 * 1024,
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut stream = LazyFrame::scan_csv(&csv)
        .unwrap()
        .collect_streaming(options)
        .unwrap();
    let mut batches = 0;
    while stream.next_batch().unwrap().is_some() {
        batches += 1;
    }
    assert!(batches > 0);
}
