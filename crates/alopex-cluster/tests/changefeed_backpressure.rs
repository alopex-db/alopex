use alopex_cluster::{
    AckState, FailureClass, OperationState, OrderingScope, Placement, PlacementReadiness,
    PlacementRole, RangeIdentity, RetentionWindow,
    changefeed::{
        AckProcessor, AckRequest, Checkpoint, CheckpointStore, DeliveryBudget, DeliveryOutcome,
        DeliveryTransition, DeliveryUsage, RetentionPolicy,
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
            retained_through_position: Some(4),
        },
        OperationState::Accepted,
    )
    .unwrap()
}

fn checkpoint(position: u64) -> Checkpoint {
    Checkpoint::new("feed-a", "range-a", 3, position, 0, 9, Some(90)).unwrap()
}

fn committed_checkpoint(position: u64) -> Checkpoint {
    let mut processor = AckProcessor::new(CheckpointStore::new(feed()).unwrap());
    let accepted = processor.accept(
        AckRequest::new(
            format!("ack-operation-{position}"),
            format!("ack-request-{position}"),
            format!("ack-{position}"),
            format!("replay-{position}"),
            checkpoint(position),
        )
        .unwrap(),
    );
    assert_eq!(accepted.ack_state, AckState::Accepted);
    let committed = processor
        .commit_after_durable_write(&format!("ack-request-{position}"))
        .unwrap();
    assert_eq!(committed.ack_state, AckState::Committed);
    committed.committed_checkpoint.unwrap()
}

fn assert_preserves_checkpoint(outcome: &DeliveryOutcome, committed: &Checkpoint) {
    assert_eq!(outcome.last_committed_checkpoint.as_ref(), Some(committed));
    assert_eq!(outcome.next_resume_position.as_ref(), Some(committed));
}

#[test]
fn retention_expiry_is_terminal_and_preserves_the_durable_checkpoint_without_rewind() {
    let committed = committed_checkpoint(4);
    let policy = RetentionPolicy::new(feed(), Some(checkpoint(5)), Some(100)).unwrap();
    let expired = policy.classify_resume(&committed, 89, Some(committed.clone()));

    assert_eq!(expired.operation_state, OperationState::TerminalFailure);
    assert_eq!(expired.failure_class, Some(FailureClass::StaleMetadata));
    assert_eq!(expired.reason_code.as_deref(), Some("retention_expired"));
    assert!(!expired.retryable);
    assert!(!expired.may_deliver());
    assert_preserves_checkpoint(&expired, &committed);
}

#[test]
fn lag_and_every_buffer_limit_are_retryable_without_dropping_the_durable_checkpoint() {
    let committed = committed_checkpoint(6);
    let budget = DeliveryBudget::new(2, 64, 3).unwrap();

    for usage in [
        DeliveryUsage {
            buffered_events: 3,
            buffered_bytes: 1,
            consumer_lag: 1,
            storage_limit_reached: false,
        },
        DeliveryUsage {
            buffered_events: 1,
            buffered_bytes: 65,
            consumer_lag: 1,
            storage_limit_reached: false,
        },
        DeliveryUsage {
            buffered_events: 1,
            buffered_bytes: 1,
            consumer_lag: 4,
            storage_limit_reached: false,
        },
    ] {
        let backpressure = budget.evaluate(usage, Some(committed.clone()));
        assert_eq!(
            backpressure.operation_state,
            OperationState::RetryableFailure
        );
        assert_eq!(backpressure.failure_class, Some(FailureClass::Timeout));
        assert_eq!(backpressure.reason_code.as_deref(), Some("backpressure"));
        assert!(backpressure.retryable);
        assert!(!backpressure.may_deliver());
        assert_preserves_checkpoint(&backpressure, &committed);
    }

    let resource_limit = budget.evaluate(
        DeliveryUsage {
            buffered_events: 0,
            buffered_bytes: 0,
            consumer_lag: 0,
            storage_limit_reached: true,
        },
        Some(committed.clone()),
    );
    assert_eq!(
        resource_limit.operation_state,
        OperationState::TerminalFailure
    );
    assert_eq!(
        resource_limit.failure_class,
        Some(FailureClass::InvalidRequest)
    );
    assert_eq!(
        resource_limit.reason_code.as_deref(),
        Some("resource_limit")
    );
    assert!(!resource_limit.retryable);
    assert!(!resource_limit.may_deliver());
    assert_preserves_checkpoint(&resource_limit, &committed);
}

#[test]
fn close_timeout_cancel_and_reconnect_keep_one_explicit_resume_position() {
    let committed = committed_checkpoint(7);

    let early_close =
        DeliveryOutcome::transition(DeliveryTransition::EarlyClose, Some(committed.clone()));
    assert_eq!(early_close.operation_state, OperationState::Cancelled);
    assert_eq!(early_close.reason_code.as_deref(), Some("early_close"));
    assert!(!early_close.may_deliver());
    assert_preserves_checkpoint(&early_close, &committed);

    let timeout = DeliveryOutcome::transition(DeliveryTransition::Timeout, Some(committed.clone()));
    assert_eq!(timeout.operation_state, OperationState::RetryableFailure);
    assert_eq!(timeout.failure_class, Some(FailureClass::Timeout));
    assert_eq!(timeout.reason_code.as_deref(), Some("delivery_timeout"));
    assert!(timeout.retryable);
    assert!(!timeout.may_deliver());
    assert_preserves_checkpoint(&timeout, &committed);

    let cancelled =
        DeliveryOutcome::transition(DeliveryTransition::Cancel, Some(committed.clone()));
    assert_eq!(cancelled.operation_state, OperationState::Cancelled);
    assert_eq!(cancelled.reason_code.as_deref(), Some("cancelled"));
    assert!(!cancelled.may_deliver());
    assert_preserves_checkpoint(&cancelled, &committed);

    let reconnected =
        DeliveryOutcome::transition(DeliveryTransition::Reconnect, Some(committed.clone()));
    assert_eq!(reconnected.operation_state, OperationState::RecoveryPending);
    assert_eq!(reconnected.reason_code.as_deref(), Some("reconnect"));
    assert!(reconnected.retryable);
    assert!(reconnected.may_deliver());
    assert_preserves_checkpoint(&reconnected, &committed);
}
