use alopex_cluster::{
    AckState, ChangeEventEnvelope, ChangeOperationType, ChangePayload, ChangefeedResult,
    FailureClass, FeedIdentity, IdempotencyResult, OperationState, OrderingScope, Placement,
    PlacementReadiness, PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome,
    RoutingOutcomeKind,
    changefeed::{
        AckProcessor, AckRequest, Checkpoint, CheckpointCursor, CheckpointStore,
        DurableCapabilityVersion, DurableProfileAdapter, DurableProfileEvidence, FeedCoordinator,
        FeedRequest,
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

fn request(operation: &str, request_id: &str) -> FeedRequest {
    FeedRequest::new(operation, request_id).unwrap()
}

fn checkpoint(position: u64) -> Checkpoint {
    Checkpoint::new("feed-a", "range-a", 3, position, 0, 9, Some(90)).unwrap()
}

fn event(position: u64) -> ChangeEventEnvelope {
    let operation_id = format!("replay-{position}");
    ChangeEventEnvelope::new(
        format!("event-{position}"),
        "feed-a",
        feed().range,
        3,
        operation_id.clone(),
        operation_id.clone(),
        position,
        0,
        ChangeOperationType::Delete,
        format!("key-{position}"),
        ChangePayload::available(format!("tombstone-{position}").into_bytes()),
        checkpoint(position),
        OperationState::Committed,
        None,
        None,
        routing(),
        false,
        IdempotencyResult {
            operation_id,
            request_id: format!("replay-{position}").into(),
            first_outcome: "committed".to_string(),
            state: OperationState::Committed,
            duplicate_count: 0,
        },
    )
    .unwrap()
}

fn coordinator() -> FeedCoordinator {
    let preflight = DurableProfileAdapter::new(DurableProfileEvidence::complete(
        DurableCapabilityVersion::new(0, 7, 0),
    ))
    .preflight();
    let mut coordinator = FeedCoordinator::new(preflight);
    coordinator
        .create(feed(), routing(), request("create", "create-request"))
        .unwrap();
    coordinator
        .subscribe("feed-a", 3, 9, request("subscribe", "subscribe-request"))
        .unwrap();
    coordinator
}

#[test]
fn ack_replays_first_and_recovered_third_attempt_without_a_second_commit() {
    let request = AckRequest::new(
        "ack-operation",
        "ack-request",
        "ack-a",
        "replay-4",
        checkpoint(4),
    )
    .unwrap();
    let mut processor = AckProcessor::new(CheckpointStore::new(feed()).unwrap());
    let first = processor.accept(request.clone());
    processor.commit_after_durable_write("ack-request").unwrap();
    let second = processor.accept(request.clone());
    let mut recovered = AckProcessor::new(processor.into_store());
    let third = recovered.accept(request);

    assert_eq!(first.ack_state, AckState::Accepted);
    assert_eq!(second.ack_state, AckState::Committed);
    assert_eq!(third.ack_state, AckState::Committed);
    assert_eq!(second.committed_checkpoint, Some(checkpoint(4)));
    assert_eq!(third.committed_checkpoint, Some(checkpoint(4)));
    assert_eq!(second.idempotency.duplicate_count, 1);
    assert_eq!(third.idempotency.duplicate_count, 2);
    assert_eq!(
        second.idempotency.first_outcome,
        third.idempotency.first_outcome
    );
}

#[test]
fn resume_replays_the_first_batch_on_second_and_reconnect_third_attempt() {
    let mut coordinator = coordinator();
    coordinator.publish(event(4)).unwrap();
    coordinator.publish(event(5)).unwrap();
    let cursor = CheckpointCursor::new(checkpoint(4))
        .unwrap()
        .encode()
        .unwrap();
    let resume_request = request("resume", "resume-request");

    let first = coordinator
        .resume("feed-a", &cursor, resume_request.clone())
        .unwrap();
    coordinator.publish(event(6)).unwrap();
    let second = coordinator
        .resume("feed-a", &cursor, resume_request.clone())
        .unwrap();
    let third = coordinator
        .resume("feed-a", &cursor, resume_request)
        .unwrap();

    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].event_id, "event-5");
    assert_eq!(second.events, first.events);
    assert_eq!(third.events, first.events);
    assert_eq!(first.outcome.idempotency.duplicate_count, 0);
    assert_eq!(second.outcome.idempotency.duplicate_count, 1);
    assert_eq!(third.outcome.idempotency.duplicate_count, 2);
    assert_eq!(
        first.outcome.idempotency.first_outcome,
        second.outcome.idempotency.first_outcome
    );
    assert_eq!(
        second.outcome.idempotency.first_outcome,
        third.outcome.idempotency.first_outcome
    );
}

#[test]
fn cancel_replays_its_terminal_outcome_without_mutating_the_subscription_again() {
    let mut coordinator = coordinator();
    let cancel_request = request("cancel", "cancel-request");

    let first = coordinator
        .cancel("feed-a", cancel_request.clone())
        .unwrap();
    let second = coordinator
        .cancel("feed-a", cancel_request.clone())
        .unwrap();
    let third = coordinator.cancel("feed-a", cancel_request).unwrap();

    for outcome in [&first, &second, &third] {
        assert_eq!(outcome.operation_state, OperationState::Cancelled);
        assert!(matches!(outcome.result, ChangefeedResult::Feed));
    }
    assert_eq!(first.idempotency.duplicate_count, 0);
    assert_eq!(second.idempotency.duplicate_count, 1);
    assert_eq!(third.idempotency.duplicate_count, 2);
    assert_eq!(
        first.idempotency.first_outcome,
        second.idempotency.first_outcome
    );
    assert_eq!(
        second.idempotency.first_outcome,
        third.idempotency.first_outcome
    );

    let poll = coordinator
        .poll("feed-a", 10, request("poll", "poll-after-cancel"))
        .unwrap();
    assert_eq!(poll.outcome.operation_state, OperationState::Cancelled);
    assert!(poll.events.is_empty());
}

#[test]
fn same_resume_request_id_with_a_different_cursor_is_an_explicit_conflict() {
    let mut coordinator = coordinator();
    coordinator.publish(event(4)).unwrap();
    coordinator.publish(event(5)).unwrap();
    let first_cursor = CheckpointCursor::new(checkpoint(4))
        .unwrap()
        .encode()
        .unwrap();
    let second_cursor = CheckpointCursor::new(checkpoint(5))
        .unwrap()
        .encode()
        .unwrap();
    let resume_request = request("resume", "resume-request");
    coordinator
        .resume("feed-a", &first_cursor, resume_request.clone())
        .unwrap();

    let conflict = coordinator
        .resume("feed-a", &second_cursor, resume_request)
        .unwrap();
    assert!(conflict.events.is_empty());
    assert_eq!(
        conflict.outcome.operation_state,
        OperationState::TerminalFailure
    );
    assert_eq!(conflict.outcome.failure_class, Some(FailureClass::Conflict));
    assert_eq!(
        conflict.outcome.reason_code.as_deref(),
        Some("request_idempotency_conflict")
    );
}
