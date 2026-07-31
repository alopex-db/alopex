use alopex_cluster::{
    ChangefeedResult, FailureClass, FeedIdentity, OperationState, OrderingScope, Placement,
    PlacementReadiness, PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome,
    RoutingOutcomeKind,
    changefeed::{DurableProfileAdapter, FeedCoordinator, FeedRequest},
};
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/changefeed_surface_parity.json"
    ))
    .expect("valid parity fixture")
}

#[test]
fn embedded_durable_preflight_is_the_canonical_cross_surface_oracle() {
    let fixture = fixture();
    let range = RangeIdentity::new(
        fixture["cluster_id"].as_str().unwrap(),
        fixture["table_id"].as_u64().unwrap() as u32,
        fixture["range_id"].as_str().unwrap(),
        None,
        None,
        fixture["schema_version"].as_u64().unwrap(),
        fixture["data_epoch"].as_u64().unwrap(),
    );
    let feed = FeedIdentity::new(
        fixture["feed_id"].as_str().unwrap(),
        range.clone(),
        fixture["generation"].as_u64().unwrap(),
        Placement::new(
            "node-a",
            vec![],
            PlacementRole::Owner,
            PlacementReadiness::Ready,
            11,
        ),
        OrderingScope::Range,
        RetentionWindow::unbounded(),
        OperationState::Accepted,
    )
    .expect("fixture feed");
    let routing = RoutingOutcome::new(
        RoutingOutcomeKind::SingleRange,
        Some(range),
        11,
        "parity_fixture",
    );
    let request = FeedRequest::new(
        "changefeed-create-parity-request",
        fixture["request_id"].as_str().unwrap(),
    )
    .expect("fixture request");

    let mut coordinator = FeedCoordinator::new(DurableProfileAdapter::compiled().preflight());
    let outcome = coordinator
        .create(feed, routing, request)
        .expect("Durable preflight must return a canonical outcome");

    assert_eq!(outcome.feed.feed_id, fixture["feed_id"]);
    assert_eq!(outcome.request_id.as_str(), fixture["request_id"]);
    assert_eq!(outcome.operation_state, OperationState::TerminalFailure);
    assert_eq!(
        outcome.failure_class,
        Some(FailureClass::PrerequisiteMissing)
    );
    assert!(
        outcome
            .reason_code
            .as_deref()
            .is_some_and(|code| code.starts_with(fixture["reason_prefix"].as_str().unwrap()))
    );
    assert!(!outcome.retryable);
    assert!(matches!(outcome.result, ChangefeedResult::Feed));
    assert_eq!(
        outcome.idempotency.request_id.as_str(),
        fixture["request_id"]
    );

    let status = outcome.surface_status();
    assert_eq!(
        status.http_status,
        fixture["http_status"].as_u64().unwrap() as u16
    );
    assert_eq!(status.grpc_code, fixture["grpc_code"]);
    assert_eq!(
        status.cli_exit_code,
        fixture["cli_exit_code"].as_i64().unwrap() as i32
    );
    assert_eq!(
        status.python_error_code,
        fixture["python_error_code"].as_str()
    );
}
