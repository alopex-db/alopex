use alopex_cluster::{
    ChangeEventEnvelope, ChangeOperationType, ChangePayload, ChangefeedResult, Checkpoint,
    FailureClass, FeedIdentity, IdempotencyResult, OperationState, OrderingScope, Placement,
    PlacementReadiness, PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome,
    RoutingOutcomeKind,
    changefeed::{
        CoordinatorError, DurableCapabilityVersion, DurableProfileAdapter, DurableProfileEvidence,
        FeedCoordinator, FeedPreflight, FeedRequest,
    },
};

fn feed() -> FeedIdentity {
    FeedIdentity::new(
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

fn request(operation: &str) -> FeedRequest {
    FeedRequest::new(operation, format!("request-{operation}")).unwrap()
}

fn event(ordinal: u32, event_id: &str) -> ChangeEventEnvelope {
    let operation_id = format!("replay-{ordinal}");
    ChangeEventEnvelope::new(
        event_id,
        "feed-a",
        feed().range,
        3,
        operation_id.clone(),
        operation_id.clone(),
        9,
        ordinal,
        ChangeOperationType::Delete,
        format!("key-{ordinal}"),
        ChangePayload::available(format!("tombstone-{ordinal}").into_bytes()),
        Checkpoint::new("feed-a", "range-a", 3, 9, ordinal, 9, Some(90)).unwrap(),
        OperationState::Committed,
        None,
        None,
        routing(),
        false,
        IdempotencyResult {
            operation_id,
            request_id: format!("replay-{ordinal}").into(),
            first_outcome: "committed".to_string(),
            state: OperationState::Committed,
            duplicate_count: 0,
        },
    )
    .unwrap()
}

fn ready_coordinator() -> FeedCoordinator {
    let preflight = DurableProfileAdapter::new(DurableProfileEvidence::complete(
        DurableCapabilityVersion::new(0, 7, 0),
    ))
    .preflight();
    let mut coordinator = FeedCoordinator::new(preflight);
    coordinator
        .create(feed(), routing(), request("create"))
        .unwrap();
    coordinator
        .subscribe("feed-a", 3, 9, request("subscribe"))
        .unwrap();
    coordinator
}

#[test]
fn rejected_preflight_never_creates_a_supported_feed() {
    let mut coordinator = FeedCoordinator::new(FeedPreflight::rejected(
        FailureClass::PrerequisiteMissing,
        "durable_unavailable",
        false,
    ));
    let outcome = coordinator
        .create(feed(), routing(), request("create"))
        .unwrap();
    assert_eq!(outcome.operation_state, OperationState::TerminalFailure);
    assert_eq!(
        outcome.failure_class,
        Some(FailureClass::PrerequisiteMissing)
    );
    assert_eq!(outcome.reason_code.as_deref(), Some("durable_unavailable"));
}

#[test]
fn lifecycle_preserves_range_order_and_exposes_at_least_once_duplicates() {
    let mut coordinator = ready_coordinator();
    coordinator.publish(event(0, "event-0")).unwrap();
    coordinator.publish(event(0, "event-0")).unwrap();
    coordinator.publish(event(1, "event-1")).unwrap();

    let delivery = coordinator.poll("feed-a", 10, request("poll")).unwrap();
    assert_eq!(delivery.outcome.operation_state, OperationState::Running);
    assert_eq!(delivery.events.len(), 3);
    assert_eq!(delivery.events[0].event_id, delivery.events[1].event_id);
    assert_eq!(delivery.events[2].checkpoint.payload_ordinal, 1);

    assert!(matches!(
        coordinator.publish(event(0, "different-event-at-old-position")),
        Err(CoordinatorError::RangeOrderViolation)
    ));
    let ack = coordinator.ack("feed-a", "ack-a", request("ack")).unwrap();
    let ChangefeedResult::Ack(ack) = ack.result else {
        panic!("ack result required");
    };
    assert_eq!(ack.ack_state, alopex_cluster::AckState::Accepted);
    assert!(ack.committed_checkpoint.is_none());
}

#[test]
fn resume_is_strictly_after_checkpoint_and_gap_is_never_an_empty_success() {
    let mut coordinator = ready_coordinator();
    coordinator.publish(event(0, "event-0")).unwrap();
    coordinator.publish(event(1, "event-1")).unwrap();
    let cursor = alopex_cluster::changefeed::CheckpointCursor::new(
        Checkpoint::new("feed-a", "range-a", 3, 9, 0, 9, Some(90)).unwrap(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let resumed = coordinator
        .resume("feed-a", &cursor, request("resume"))
        .unwrap();
    assert_eq!(
        resumed.outcome.operation_state,
        OperationState::RecoveryPending
    );
    assert_eq!(resumed.events.len(), 1);
    assert_eq!(resumed.events[0].checkpoint.payload_ordinal, 1);

    coordinator
        .mark_continuity_failure("feed-a", FailureClass::Gap, "range_order_gap", true)
        .unwrap();
    let gap = coordinator.poll("feed-a", 10, request("poll-gap")).unwrap();
    assert!(gap.events.is_empty());
    assert_eq!(
        gap.outcome.operation_state,
        OperationState::RetryableFailure
    );
    assert_eq!(gap.outcome.failure_class, Some(FailureClass::Gap));
    assert_eq!(gap.outcome.reason_code.as_deref(), Some("range_order_gap"));
}

#[test]
fn cancel_and_close_are_idempotent_terminal_lifecycle_operations() {
    let mut coordinator = ready_coordinator();
    let first = coordinator.close("feed-a", request("close")).unwrap();
    let repeated = coordinator.cancel("feed-a", request("cancel")).unwrap();
    assert_eq!(first.operation_state, OperationState::Cancelled);
    assert_eq!(repeated.operation_state, OperationState::Cancelled);
    let poll = coordinator
        .poll("feed-a", 10, request("poll-after-close"))
        .unwrap();
    assert_eq!(poll.outcome.operation_state, OperationState::Cancelled);
    assert!(poll.events.is_empty());
}
