use std::collections::{BTreeMap, BTreeSet};

use alopex_sql::changefeed_boundary::{
    ChangefeedSqlChangeKind, ChangefeedSqlClassification, ChangefeedSqlRequest,
    SQL_CHANGEFEED_UNSUPPORTED_REQUESTS, preflight_changefeed_sql_request,
};

#[test]
fn every_sql_lifecycle_and_unsupported_change_kind_stops_before_execution() {
    let expected_reasons = BTreeMap::from([
        ("ddl_schema", "schema_unsupported"),
        ("copy_bulk", "bulk_changefeed_unsupported"),
        ("vector_hnsw_columnar", "vector_changefeed_unsupported"),
        ("crdt_update", "crdt_changefeed_unsupported"),
        (
            "distributed_transaction",
            "distributed_transaction_changefeed_unsupported",
        ),
    ]);
    let requests = SQL_CHANGEFEED_UNSUPPORTED_REQUESTS;
    let unique_ids: BTreeSet<_> = requests.iter().map(|request| request.id()).collect();

    assert_eq!(
        requests.len(),
        13,
        "eight lifecycle and five change-kind rows"
    );
    assert_eq!(
        unique_ids.len(),
        requests.len(),
        "the register has no implicit row"
    );

    for request in requests.iter().copied() {
        let rejection = preflight_changefeed_sql_request(request).unwrap_err();
        assert_eq!(
            rejection.classification,
            ChangefeedSqlClassification::PreExecutionUnsupported
        );
        assert_eq!(rejection.boundary_version, "v0.9");
        assert_eq!(rejection.code, "changefeed_sql_unsupported");
        assert_eq!(rejection.canonical_routing_kind, "unsupported");
        assert_eq!(rejection.canonical_failure_class, "invalid_request");
        assert_eq!(rejection.surface_error_code, "changefeed_unsupported");
        assert!(!rejection.retryable);
        assert!(
            !rejection.execution_started,
            "{} must not create a plan, transport, checkpoint, or implicit feed",
            request.id()
        );

        match request {
            ChangefeedSqlRequest::Lifecycle(_) => {
                assert_eq!(rejection.reason_code, "sql_changefeed_surface_unsupported")
            }
            ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::DdlSchema)
            | ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::CopyBulk)
            | ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::VectorHnswColumnar)
            | ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::CrdtUpdate)
            | ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::DistributedTransaction) => {
                assert_eq!(
                    rejection.reason_code,
                    expected_reasons[request.id()],
                    "{} must retain its distinct unsupported reason",
                    request.id()
                )
            }
        }
    }
}
