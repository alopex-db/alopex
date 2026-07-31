use alopex_cluster::changefeed::{ChangefeedResult, JournalAdapterError, JournalEventAdapter};
use alopex_cluster::{
    ChangefeedOutcome, FailureClass, FeedIdentity, IdempotencyResult, OperationState,
    OrderingScope, Placement, PlacementReadiness, PlacementRole, RangeIdentity, RetentionWindow,
    RoutingOutcome, RoutingOutcomeKind,
};

fn unsupported_outcome(reason_code: &str) -> ChangefeedOutcome {
    let feed = FeedIdentity::new(
        "unsupported-feed",
        RangeIdentity::new("cluster-a", 7, "range-a", None, None, 4, 9),
        3,
        Placement::new(
            "node-a",
            vec![],
            PlacementRole::Owner,
            PlacementReadiness::Ready,
            11,
        ),
        OrderingScope::Range,
        RetentionWindow {
            deadline_epoch: Some(90),
            retained_through_position: Some(1),
        },
        OperationState::Committed,
    )
    .unwrap();
    let range = feed.range.clone();

    ChangefeedOutcome::new(
        feed,
        "unsupported-operation",
        "unsupported-request",
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        Some(reason_code.to_owned()),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(range),
            12,
            "pre_execution_unsupported",
        ),
        false,
        IdempotencyResult {
            operation_id: "unsupported-operation".into(),
            request_id: "unsupported-request".into(),
            first_outcome: "changefeed_unsupported".into(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
        ChangefeedResult::Feed,
    )
    .unwrap()
}

#[test]
fn unsupported_schema_cannot_be_adapted_or_advertised_by_any_surface() {
    let adapter_error = JournalEventAdapter.reject_schema();
    assert!(matches!(
        adapter_error,
        JournalAdapterError::UnsupportedChangeKind {
            reason_code: "schema_unsupported"
        }
    ));
    assert_eq!(adapter_error.failure_class(), FailureClass::InvalidRequest);
    assert_eq!(adapter_error.reason_code(), "schema_unsupported");

    let outcome = unsupported_outcome(adapter_error.reason_code());
    assert_eq!(outcome.operation_state, OperationState::Rejected);
    assert_eq!(outcome.failure_class, Some(FailureClass::InvalidRequest));
    assert_eq!(outcome.reason_code.as_deref(), Some("schema_unsupported"));
    assert!(!outcome.retryable);
    assert!(matches!(outcome.result, ChangefeedResult::Feed));
    assert_eq!(outcome.idempotency.duplicate_count, 0);

    let status = outcome.surface_status();
    assert_eq!(
        (
            status.http_status,
            status.grpc_code,
            status.cli_exit_code,
            status.python_error_code,
        ),
        (501, "UNIMPLEMENTED", 5, Some("changefeed_unsupported"))
    );
}
