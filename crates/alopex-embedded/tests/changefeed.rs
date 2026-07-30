use std::collections::BTreeSet;

use alopex_cluster::{
    changefeed::{
        ChangefeedAuthorization, ChangefeedResult, ChangefeedScope, CheckpointCursor,
        DurableCapabilityVersion, DurableProfileAdapter, DurableProfileEvidence, FeedRequest,
    },
    AuthenticatedSubject, Checkpoint, FailureClass, OperationState, OrderingScope, Placement,
    PlacementReadiness, PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome,
    RoutingOutcomeKind,
};
use alopex_embedded::Database;

fn feed() -> alopex_cluster::FeedIdentity {
    alopex_cluster::FeedIdentity::new(
        "feed-a",
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
        OperationState::Accepted,
    )
    .unwrap()
}

fn routing() -> RoutingOutcome {
    RoutingOutcome::new(
        RoutingOutcomeKind::SingleRange,
        Some(feed().range),
        12,
        "placement_ready",
    )
}

fn authorization(scopes: &[ChangefeedScope]) -> ChangefeedAuthorization {
    ChangefeedAuthorization {
        subject: AuthenticatedSubject::new("embedded-app"),
        tenant: "tenant-a".to_string(),
        allowed_ranges: BTreeSet::from(["range-a".into()]),
        allowed_scopes: scopes.iter().copied().collect(),
    }
}

fn request(name: &str) -> FeedRequest {
    FeedRequest::new(format!("operation-{name}"), format!("request-{name}")).unwrap()
}

fn ready_adapter() -> DurableProfileAdapter {
    DurableProfileAdapter::new(DurableProfileEvidence::complete(
        DurableCapabilityVersion::new(0, 7, 0),
    ))
}

fn checkpoint() -> String {
    CheckpointCursor::new(Checkpoint::new("feed-a", "range-a", 3, 0, 0, 9, Some(90)).unwrap())
        .unwrap()
        .encode()
        .unwrap()
}

#[test]
fn embedded_lifecycle_uses_the_coordinator_after_authorized_preflight() {
    let database = Database::new();
    let created = database
        .create_changefeed(
            ready_adapter(),
            authorization(&[ChangefeedScope::Read, ChangefeedScope::Ack]),
            "tenant-a",
            feed(),
            routing(),
            request("create"),
        )
        .unwrap();
    assert_eq!(created.outcome.operation_state, OperationState::Accepted);
    let handle = created
        .changefeed
        .expect("authorized ready create returns handle");

    assert_eq!(
        handle
            .subscribe(3, 9, request("subscribe"))
            .unwrap()
            .operation_state,
        OperationState::Running
    );
    let poll = handle.poll(10, request("poll")).unwrap();
    assert_eq!(poll.outcome.operation_state, OperationState::Running);
    assert!(poll.events.is_empty());
    let ack = handle.ack("ack-a", &checkpoint(), request("ack")).unwrap();
    assert!(matches!(ack.result, ChangefeedResult::Ack(_)));
    assert_eq!(
        handle.cancel(request("cancel")).unwrap().operation_state,
        OperationState::Cancelled
    );
    assert_eq!(
        handle.close(request("close")).unwrap().operation_state,
        OperationState::Cancelled
    );
}

#[test]
fn embedded_scope_denial_never_mutates_or_exposes_ack_payload() {
    let database = Database::new();
    let created = database
        .create_changefeed(
            ready_adapter(),
            authorization(&[ChangefeedScope::Read]),
            "tenant-a",
            feed(),
            routing(),
            request("create"),
        )
        .unwrap();
    let handle = created.changefeed.unwrap();
    let denied = handle
        .ack("ack-a", "not-a-valid-checkpoint", request("ack"))
        .unwrap();
    assert_eq!(
        denied.failure_class,
        Some(alopex_cluster::FailureClass::Unauthorized)
    );
    assert!(matches!(denied.result, ChangefeedResult::Feed));
    assert_eq!(denied.surface_status().http_status, 401);
}

#[test]
fn embedded_ack_rejects_a_checkpoint_for_another_feed_before_acceptance() {
    let database = Database::new();
    let created = database
        .create_changefeed(
            ready_adapter(),
            authorization(&[ChangefeedScope::Read, ChangefeedScope::Ack]),
            "tenant-a",
            feed(),
            routing(),
            request("create"),
        )
        .unwrap();
    let handle = created.changefeed.unwrap();

    let rejected = handle
        .ack("ack-a", "not-a-valid-checkpoint", request("ack"))
        .unwrap();
    assert_eq!(rejected.failure_class, Some(FailureClass::InvalidRequest));
    assert_eq!(rejected.reason_code.as_deref(), Some("invalid_checkpoint"));
    assert!(matches!(rejected.result, ChangefeedResult::Feed));
}

#[test]
fn unavailable_durable_profile_returns_outcome_without_embedded_handle() {
    let database = Database::new();
    let created = database
        .create_changefeed(
            DurableProfileAdapter::compiled(),
            authorization(&[ChangefeedScope::Read]),
            "tenant-a",
            feed(),
            routing(),
            request("create"),
        )
        .unwrap();
    assert_eq!(
        created.outcome.failure_class,
        Some(alopex_cluster::FailureClass::PrerequisiteMissing)
    );
    assert!(created.changefeed.is_none());
}
