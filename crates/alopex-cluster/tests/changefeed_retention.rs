use alopex_cluster::{
    FailureClass, OperationState, OrderingScope, Placement, PlacementReadiness, PlacementRole,
    RangeIdentity, RetentionWindow,
    changefeed::{
        Checkpoint, DeliveryBudget, DeliveryOutcome, DeliveryTransition, DeliveryUsage,
        RetentionPolicy,
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
            retained_through_position: Some(3),
        },
        OperationState::Accepted,
    )
    .unwrap()
}

fn checkpoint(position: u64) -> Checkpoint {
    Checkpoint::new("feed-a", "range-a", 3, position, 0, 9, Some(90)).unwrap()
}

fn assert_resume_position(outcome: &DeliveryOutcome, position: u64) {
    assert_eq!(
        outcome.last_committed_checkpoint,
        Some(checkpoint(position))
    );
    assert_eq!(outcome.next_resume_position, Some(checkpoint(position)));
}

#[test]
fn retained_boundary_and_deadline_are_terminal_without_implicit_restart() {
    let policy = RetentionPolicy::new(feed(), Some(checkpoint(3)), Some(90)).unwrap();

    let old = policy.classify_resume(&checkpoint(2), 89, Some(checkpoint(2)));
    assert_eq!(old.operation_state, OperationState::TerminalFailure);
    assert_eq!(old.failure_class, Some(FailureClass::StaleMetadata));
    assert_eq!(old.reason_code.as_deref(), Some("retention_expired"));
    assert!(!old.may_deliver());
    assert_resume_position(&old, 2);

    let deadline = policy.classify_resume(&checkpoint(3), 90, Some(checkpoint(3)));
    assert_eq!(deadline.failure_class, Some(FailureClass::StaleMetadata));
    assert_eq!(deadline.reason_code.as_deref(), Some("retention_expired"));
    assert_resume_position(&deadline, 3);
}

#[test]
fn budget_exhaustion_is_retryable_backpressure_or_explicit_resource_limit() {
    let budget = DeliveryBudget::new(2, 64, 3).unwrap();
    let backpressure = budget.evaluate(
        DeliveryUsage {
            buffered_events: 3,
            buffered_bytes: 63,
            consumer_lag: 2,
            storage_limit_reached: false,
        },
        Some(checkpoint(4)),
    );
    assert_eq!(
        backpressure.operation_state,
        OperationState::RetryableFailure
    );
    assert_eq!(backpressure.failure_class, Some(FailureClass::Timeout));
    assert_eq!(backpressure.reason_code.as_deref(), Some("backpressure"));
    assert!(backpressure.retryable);
    assert_resume_position(&backpressure, 4);

    let resource = budget.evaluate(
        DeliveryUsage {
            buffered_events: 0,
            buffered_bytes: 0,
            consumer_lag: 0,
            storage_limit_reached: true,
        },
        Some(checkpoint(4)),
    );
    assert_eq!(resource.operation_state, OperationState::TerminalFailure);
    assert_eq!(resource.failure_class, Some(FailureClass::InvalidRequest));
    assert_eq!(resource.reason_code.as_deref(), Some("resource_limit"));
    assert_resume_position(&resource, 4);
}

#[test]
fn lifecycle_transitions_preserve_checkpoint_and_never_become_empty_success() {
    let checkpoint = Some(checkpoint(4));
    let early_close =
        DeliveryOutcome::transition(DeliveryTransition::EarlyClose, checkpoint.clone());
    assert_eq!(early_close.operation_state, OperationState::Cancelled);
    assert_eq!(early_close.reason_code.as_deref(), Some("early_close"));
    assert_resume_position(&early_close, 4);

    let timeout = DeliveryOutcome::transition(DeliveryTransition::Timeout, checkpoint.clone());
    assert_eq!(timeout.operation_state, OperationState::RetryableFailure);
    assert_eq!(timeout.failure_class, Some(FailureClass::Timeout));
    assert_eq!(timeout.reason_code.as_deref(), Some("delivery_timeout"));
    assert_resume_position(&timeout, 4);

    let cancel = DeliveryOutcome::transition(DeliveryTransition::Cancel, checkpoint.clone());
    assert_eq!(cancel.operation_state, OperationState::Cancelled);
    assert_eq!(cancel.reason_code.as_deref(), Some("cancelled"));
    assert_resume_position(&cancel, 4);

    let reconnect = DeliveryOutcome::transition(DeliveryTransition::Reconnect, checkpoint);
    assert_eq!(reconnect.operation_state, OperationState::RecoveryPending);
    assert_eq!(reconnect.reason_code.as_deref(), Some("reconnect"));
    assert!(reconnect.may_deliver());
    assert_resume_position(&reconnect, 4);
}

#[test]
fn in_budget_and_retained_resume_are_the_only_delivery_ready_results() {
    let policy = RetentionPolicy::new(feed(), Some(checkpoint(3)), Some(100)).unwrap();
    let ready = policy.classify_resume(&checkpoint(4), 89, Some(checkpoint(4)));
    assert_eq!(ready.operation_state, OperationState::Accepted);
    assert!(ready.may_deliver());

    let budget = DeliveryBudget::new(2, 64, 3).unwrap();
    let in_budget = budget.evaluate(
        DeliveryUsage {
            buffered_events: 2,
            buffered_bytes: 64,
            consumer_lag: 3,
            storage_limit_reached: false,
        },
        Some(checkpoint(4)),
    );
    assert!(in_budget.may_deliver());
    assert_resume_position(&in_budget, 4);
}
