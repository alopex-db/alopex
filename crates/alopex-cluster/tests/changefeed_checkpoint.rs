use alopex_cluster::{
    AckState, FailureClass, OperationState, OrderingScope, Placement, PlacementReadiness,
    PlacementRole, RangeIdentity, RetentionWindow,
    changefeed::{
        AckProcessor, AckRequest, Checkpoint, CheckpointStore, ResumePlanner, ResumeSourceStatus,
    },
};

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

fn checkpoint(position: u64) -> Checkpoint {
    Checkpoint::new("feed-a", "range-a", 3, position, 0, 9, Some(90)).unwrap()
}

fn request(request_id: &str, position: u64) -> AckRequest {
    AckRequest::new(
        format!("operation-{request_id}"),
        request_id,
        format!("ack-{request_id}"),
        format!("replay-{request_id}"),
        checkpoint(position),
    )
    .unwrap()
}

fn processor() -> AckProcessor {
    AckProcessor::new(CheckpointStore::new(feed()).unwrap())
}

#[test]
fn accepted_and_pending_acks_cannot_resume_until_durable_commit() {
    let mut processor = processor();
    let accepted = processor.accept(request("request-a", 4));
    assert_eq!(accepted.ack_state, AckState::Accepted);
    assert!(accepted.committed_checkpoint.is_none());

    let planner = ResumePlanner::new(feed(), Some(checkpoint(1))).unwrap();
    let not_durable = planner.plan(&accepted, ResumeSourceStatus::Ready);
    assert_eq!(
        not_durable.failure_class,
        Some(FailureClass::PrerequisiteMissing)
    );
    assert_eq!(
        not_durable.reason_code.as_deref(),
        Some("durable_checkpoint_uncommitted")
    );
    assert!(!not_durable.can_resume());

    let pending = processor.mark_pending("request-a").unwrap();
    assert_eq!(pending.ack_state, AckState::Pending);
    assert!(pending.committed_checkpoint.is_none());

    let committed = processor.commit_after_durable_write("request-a").unwrap();
    assert_eq!(committed.ack_state, AckState::Committed);
    assert_eq!(committed.committed_checkpoint, Some(checkpoint(4)));
    assert_eq!(
        processor.into_store().latest_committed(),
        Some(&checkpoint(4))
    );
}

#[test]
fn duplicate_ack_replays_after_recovery_without_second_checkpoint_write() {
    let mut processor = processor();
    let request = request("request-a", 4);
    let first = processor.accept(request.clone());
    assert_eq!(first.idempotency.duplicate_count, 0);
    processor.mark_pending("request-a").unwrap();
    processor.commit_after_durable_write("request-a").unwrap();

    let second = processor.accept(request.clone());
    assert_eq!(second.ack_state, AckState::Committed);
    assert_eq!(second.idempotency.duplicate_count, 1);
    assert_eq!(second.committed_checkpoint, Some(checkpoint(4)));

    let recovered = processor.into_store();
    let mut restarted = AckProcessor::new(recovered);
    let third = restarted.accept(request);
    assert_eq!(third.ack_state, AckState::Committed);
    assert_eq!(third.idempotency.duplicate_count, 2);
    assert_eq!(third.committed_checkpoint, Some(checkpoint(4)));
}

#[test]
fn conflicting_or_non_monotonic_acknowledgements_are_explicitly_rejected() {
    let mut processor = processor();
    let first = request("request-a", 4);
    processor.accept(first.clone());
    processor.commit_after_durable_write("request-a").unwrap();

    let conflicting = processor.accept(
        AckRequest::new(
            "operation-request-a",
            "request-a",
            "ack-request-a",
            "replay-request-a",
            checkpoint(5),
        )
        .unwrap(),
    );
    assert_eq!(conflicting.ack_state, AckState::Rejected);
    assert_eq!(conflicting.failure_class, Some(FailureClass::Conflict));
    assert_eq!(
        conflicting.reason_code.as_deref(),
        Some("ack_idempotency_conflict")
    );

    let non_monotonic = processor.accept(request("request-b", 3));
    assert_eq!(non_monotonic.ack_state, AckState::Rejected);
    assert_eq!(non_monotonic.failure_class, Some(FailureClass::Conflict));
    assert_eq!(
        non_monotonic.reason_code.as_deref(),
        Some("checkpoint_not_monotonic")
    );
}

#[test]
fn out_of_order_durable_commit_replays_its_original_conflict_after_restart() {
    let mut processor = processor();
    let lower = request("request-lower", 4);
    let higher = request("request-higher", 5);
    processor.accept(lower.clone());
    processor.accept(higher);
    processor
        .commit_after_durable_write("request-higher")
        .unwrap();

    let first_rejection = processor
        .commit_after_durable_write("request-lower")
        .unwrap();
    assert_eq!(first_rejection.ack_state, AckState::Rejected);
    assert_eq!(
        first_rejection.reason_code.as_deref(),
        Some("checkpoint_not_monotonic")
    );

    let mut recovered = AckProcessor::new(processor.into_store());
    let replay = recovered.accept(lower);
    assert_eq!(replay.ack_state, AckState::Rejected);
    assert_eq!(replay.failure_class, Some(FailureClass::Conflict));
    assert_eq!(
        replay.reason_code.as_deref(),
        Some("checkpoint_not_monotonic")
    );
    assert_eq!(replay.idempotency.duplicate_count, 1);
}

#[test]
fn resume_planner_classifies_retention_and_source_failures_without_empty_success() {
    let mut processor = processor();
    processor.accept(request("request-a", 4));
    let committed = processor.commit_after_durable_write("request-a").unwrap();

    let planner = ResumePlanner::new(feed(), Some(checkpoint(3))).unwrap();
    let ready = planner.plan(&committed, ResumeSourceStatus::Ready);
    assert!(ready.can_resume());
    assert_eq!(ready.operation_state, OperationState::RecoveryPending);
    assert_eq!(ready.checkpoint, Some(checkpoint(4)));

    for (source, failure_class, reason_code) in [
        (
            ResumeSourceStatus::MetadataStale,
            FailureClass::StaleMetadata,
            "metadata_refresh_required",
        ),
        (
            ResumeSourceStatus::Gap,
            FailureClass::Gap,
            "range_order_gap",
        ),
        (
            ResumeSourceStatus::EpochMismatch,
            FailureClass::EpochMismatch,
            "range_order_gap",
        ),
        (
            ResumeSourceStatus::DurableUnavailable,
            FailureClass::PrerequisiteMissing,
            "durable_unavailable",
        ),
        (
            ResumeSourceStatus::NodeUnavailable,
            FailureClass::NodeUnavailable,
            "source_unavailable",
        ),
    ] {
        let rejected = planner.plan(&committed, source);
        assert_eq!(rejected.failure_class, Some(failure_class));
        assert_eq!(rejected.reason_code.as_deref(), Some(reason_code));
        assert!(!rejected.can_resume());
    }

    let expired = ResumePlanner::new(feed(), Some(checkpoint(5)))
        .unwrap()
        .plan(&committed, ResumeSourceStatus::Ready);
    assert_eq!(expired.failure_class, Some(FailureClass::StaleMetadata));
    assert_eq!(expired.reason_code.as_deref(), Some("retention_expired"));
    assert_eq!(expired.operation_state, OperationState::TerminalFailure);
}
