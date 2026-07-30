use alopex_cluster::{
    ChangeEventEnvelope, ChangeOperationType, ChangePayload, FailureClass, FeedIdentity,
    IdempotencyResult, NodeId, OperationState, OrderingScope, Placement, PlacementReadiness,
    PlacementRole, RangeIdentity, RangeTransferCoordinator, RangeTransferPhase, RequestId,
    RetentionWindow, RoutingOutcome, RoutingOutcomeKind,
    changefeed::{
        AckProcessor, AckRequest, Checkpoint, CheckpointCursor, CheckpointStore, CoordinatorError,
        DurableCapabilityVersion, DurableProfileAdapter, DurableProfileEvidence, FeedCoordinator,
        FeedRequest, ResumePlanner, ResumeSourceStatus,
    },
};

fn feed(owner: &str, generation: u64, data_epoch: u64) -> FeedIdentity {
    FeedIdentity::new(
        "feed-a",
        RangeIdentity::new("cluster-a", 7, "range-a", None, None, 4, data_epoch),
        generation,
        Placement::new(
            owner,
            vec![],
            PlacementRole::Owner,
            PlacementReadiness::Ready,
            data_epoch + 2,
        ),
        OrderingScope::Range,
        RetentionWindow {
            deadline_epoch: Some(90),
            retained_through_position: Some(100),
        },
        OperationState::Accepted,
    )
    .unwrap()
}

fn routing(feed: &FeedIdentity) -> RoutingOutcome {
    RoutingOutcome::new(
        RoutingOutcomeKind::SingleRange,
        Some(feed.range.clone()),
        feed.range.data_epoch + 3,
        "placement_ready",
    )
}

fn request(operation: &str) -> FeedRequest {
    FeedRequest::new(operation, format!("request-{operation}")).unwrap()
}

fn coordinator(feed: FeedIdentity) -> FeedCoordinator {
    let preflight = DurableProfileAdapter::new(DurableProfileEvidence::complete(
        DurableCapabilityVersion::new(0, 7, 0),
    ))
    .preflight();
    let mut coordinator = FeedCoordinator::new(preflight);
    coordinator
        .create(feed.clone(), routing(&feed), request("create"))
        .unwrap();
    coordinator
        .subscribe(
            &feed.feed_id,
            feed.generation,
            feed.range.data_epoch,
            request("subscribe"),
        )
        .unwrap();
    coordinator
}

fn event(feed: &FeedIdentity, commit_position: u64, event_id: &str) -> ChangeEventEnvelope {
    let operation_id = format!("replay-{event_id}");
    ChangeEventEnvelope::new(
        event_id,
        &feed.feed_id,
        feed.range.clone(),
        feed.generation,
        operation_id.clone(),
        operation_id.clone(),
        commit_position,
        0,
        ChangeOperationType::Delete,
        format!("key-{commit_position}"),
        ChangePayload::available(format!("tombstone-{commit_position}").into_bytes()),
        Checkpoint::new(
            &feed.feed_id,
            feed.range.range_id.clone(),
            feed.generation,
            commit_position,
            0,
            feed.range.data_epoch,
            Some(90),
        )
        .unwrap(),
        OperationState::Committed,
        None,
        None,
        routing(feed),
        false,
        IdempotencyResult {
            operation_id,
            request_id: format!("replay-{event_id}").into(),
            first_outcome: "committed".to_string(),
            state: OperationState::Committed,
            duplicate_count: 0,
        },
    )
    .unwrap()
}

fn committed_ack(feed: &FeedIdentity, checkpoint: Checkpoint) -> alopex_cluster::AckResult {
    let mut processor = AckProcessor::new(CheckpointStore::new(feed.clone()).unwrap());
    processor.accept(
        AckRequest::new(
            "ack-operation",
            "ack-request",
            "ack-a",
            "replay-event-100",
            checkpoint,
        )
        .unwrap(),
    );
    processor.commit_after_durable_write("ack-request").unwrap()
}

#[test]
fn range_transfer_catchup_and_reconnect_fence_a_stale_cursor() {
    let source_feed = feed("node-a", 3, 9);
    let mut source = coordinator(source_feed.clone());
    let first = event(&source_feed, 100, "event-100");
    source.publish(first.clone()).unwrap();
    let cursor = CheckpointCursor::new(first.checkpoint.clone())
        .unwrap()
        .encode()
        .unwrap();

    let unavailable = ResumePlanner::new(source_feed.clone(), Some(first.checkpoint.clone()))
        .unwrap()
        .plan(
            &committed_ack(&source_feed, first.checkpoint.clone()),
            ResumeSourceStatus::NodeUnavailable,
        );
    assert_eq!(
        unavailable.operation_state,
        OperationState::RetryableFailure
    );
    assert_eq!(
        unavailable.failure_class,
        Some(FailureClass::NodeUnavailable)
    );
    assert_eq!(
        unavailable.reason_code.as_deref(),
        Some("source_unavailable")
    );
    assert!(!unavailable.can_resume());

    let transfer_request = RequestId::new("transfer-range-a-to-b");
    let mut transfer = RangeTransferCoordinator::default();
    let prepared = transfer
        .prepare(
            transfer_request.clone(),
            "range-a-to-node-b",
            "node-a",
            "node-b",
        )
        .unwrap();
    assert_eq!(prepared.phase, RangeTransferPhase::Prepared);
    assert_eq!(prepared.serving_owner, NodeId::new("node-a"));
    transfer.copy_chunk(&transfer_request).unwrap();
    let caught_up = transfer.copy_chunk(&transfer_request).unwrap();
    assert_eq!(caught_up.copied_chunks, 2);
    let verified = transfer.verify(&transfer_request, 10).unwrap();
    assert_eq!(verified.verified_epoch, Some(10));
    let published = transfer.publish(&transfer_request).unwrap();
    assert_eq!(published.phase, RangeTransferPhase::Published);
    assert_eq!(published.serving_owner, NodeId::new("node-b"));

    let target_feed = feed("node-b", 4, 10);
    let mut reconnected = coordinator(target_feed.clone());
    let stale_subscribe = reconnected
        .subscribe("feed-a", 3, 9, request("subscribe-stale"))
        .unwrap();
    assert_eq!(
        stale_subscribe.operation_state,
        OperationState::RetryableFailure
    );
    assert_eq!(
        stale_subscribe.failure_class,
        Some(FailureClass::EpochMismatch)
    );

    let stale_resume = reconnected
        .resume("feed-a", &cursor, request("resume-stale"))
        .unwrap();
    assert!(stale_resume.events.is_empty());
    assert_eq!(
        stale_resume.outcome.operation_state,
        OperationState::RetryableFailure
    );
    assert_eq!(
        stale_resume.outcome.failure_class,
        Some(FailureClass::EpochMismatch)
    );
    assert_eq!(
        stale_resume.outcome.reason_code.as_deref(),
        Some("range_order_gap")
    );
}

#[test]
fn duplicate_replay_stays_identifiable_and_reordered_delivery_is_rejected() {
    let feed = feed("node-a", 3, 9);
    let mut coordinator = coordinator(feed.clone());
    let first = event(&feed, 100, "event-100");
    coordinator.publish(first.clone()).unwrap();
    coordinator.publish(first.clone()).unwrap();
    coordinator.publish(event(&feed, 102, "event-102")).unwrap();

    let delivery = coordinator.poll("feed-a", 10, request("poll")).unwrap();
    assert_eq!(delivery.events.len(), 3);
    assert_eq!(delivery.events[0].event_id, delivery.events[1].event_id);
    assert_eq!(delivery.events[2].checkpoint.commit_position, 102);
    assert!(matches!(
        coordinator.publish(event(&feed, 101, "event-101")),
        Err(CoordinatorError::RangeOrderViolation)
    ));

    let cursor = CheckpointCursor::new(first.checkpoint)
        .unwrap()
        .encode()
        .unwrap();
    let resumed = coordinator
        .resume("feed-a", &cursor, request("resume-after-duplicate"))
        .unwrap();
    assert_eq!(
        resumed.outcome.operation_state,
        OperationState::RecoveryPending
    );
    assert_eq!(resumed.events.len(), 1);
    assert_eq!(resumed.events[0].event_id, "event-102");
}

#[test]
fn unprovable_replica_catchup_returns_gap_instead_of_empty_success() {
    let feed = feed("node-a", 3, 9);
    let mut coordinator = coordinator(feed.clone());
    let first = event(&feed, 100, "event-100");
    coordinator.publish(first.clone()).unwrap();
    let cursor = CheckpointCursor::new(first.checkpoint)
        .unwrap()
        .encode()
        .unwrap();
    coordinator
        .mark_continuity_failure("feed-a", FailureClass::Gap, "range_order_gap", true)
        .unwrap();

    let resumed = coordinator
        .resume("feed-a", &cursor, request("resume-gap"))
        .unwrap();
    assert!(resumed.events.is_empty());
    assert_eq!(
        resumed.outcome.operation_state,
        OperationState::RetryableFailure
    );
    assert_eq!(resumed.outcome.failure_class, Some(FailureClass::Gap));
    assert_eq!(
        resumed.outcome.reason_code.as_deref(),
        Some("range_order_gap")
    );
}
