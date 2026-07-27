use alopex_cluster::{
    ClusterBootstrapOutcome, CrdtConvergenceCoordinator, CrdtCoordinatorConfig, CrdtReadinessGate,
    CrdtReplicaObservation, FailureClass, NodeId, OperationState, RangeIdentity,
    RoutingOutcomeKind, accepted_digest_counts,
};

fn gate(bootstrap: ClusterBootstrapOutcome) -> CrdtReadinessGate {
    CrdtReadinessGate {
        phase_one_approved: true,
        phase_one_evidence_complete: true,
        range_lease_valid: true,
        placement_ready: true,
        recovery_ready: true,
        bootstrap,
        range: RangeIdentity::new("cluster-a", 7, "range-a", None, None, 1, 9),
        metadata_version: 4,
    }
}

fn coordinator(
    bootstrap: ClusterBootstrapOutcome,
    configured_replicas: usize,
    quorum: usize,
    local_durable: bool,
) -> CrdtConvergenceCoordinator {
    CrdtConvergenceCoordinator::new(CrdtCoordinatorConfig {
        gate: gate(bootstrap),
        local_node: NodeId::new("node-a"),
        configured_replicas,
        quorum,
        local_durable,
    })
}

fn observation(
    node_id: &str,
    digest: &str,
    data_epoch: u64,
    reachable: bool,
    durable_owner: bool,
) -> CrdtReplicaObservation {
    CrdtReplicaObservation {
        node_id: node_id.into(),
        data_epoch,
        accepted_operation_digest: digest.to_string(),
        ready: true,
        reachable,
        durable_owner,
    }
}

#[test]
fn phase_one_gate_blocks_before_any_local_durability() {
    let mut readiness = gate(ClusterBootstrapOutcome::ReadyForClusterControl);
    readiness.phase_one_evidence_complete = false;
    let coordinator = CrdtConvergenceCoordinator::new(CrdtCoordinatorConfig {
        gate: readiness,
        local_node: "node-a".into(),
        configured_replicas: 2,
        quorum: 2,
        local_durable: true,
    });

    let outcome = coordinator.evaluate(&[]);
    assert_eq!(outcome.state, OperationState::Rejected);
    assert_eq!(
        outcome.failure_class,
        Some(FailureClass::PrerequisiteMissing)
    );
    assert_eq!(outcome.routing.kind, RoutingOutcomeKind::Blocked);
    assert!(!outcome.permit_local_durability);
}

#[test]
fn single_node_and_two_replica_quorum_commit_only_when_the_digest_converges() {
    let single = coordinator(ClusterBootstrapOutcome::SingleNode, 1, 1, true).evaluate(&[]);
    assert_eq!(single.state, OperationState::Committed);
    assert_eq!(single.routing.kind, RoutingOutcomeKind::LocalOnly);
    assert!(single.permit_local_durability);

    let cluster = coordinator(ClusterBootstrapOutcome::ReadyForClusterControl, 2, 2, true);
    let committed = cluster.evaluate(&[
        observation("node-a", "digest-1", 9, true, true),
        observation("node-b", "digest-1", 9, true, false),
    ]);
    assert_eq!(committed.state, OperationState::Committed);
    assert_eq!(committed.routing.reason_code, "replica_converged");
    assert!(committed.permit_local_durability);
}

#[test]
fn durable_partition_is_pending_then_reconnect_reconciles_without_a_second_apply() {
    let cluster = coordinator(ClusterBootstrapOutcome::ReadyForClusterControl, 2, 2, true);
    let partitioned = cluster.evaluate(&[
        observation("node-a", "digest-1", 9, true, true),
        observation("node-b", "digest-1", 9, false, false),
    ]);
    assert_eq!(partitioned.state, OperationState::RecoveryPending);
    assert_eq!(
        partitioned.failure_class,
        Some(FailureClass::NodeUnavailable)
    );
    assert!(partitioned.permit_local_durability);
    assert!(partitioned.retryable);

    let reconciled = cluster.evaluate(&[
        observation("node-a", "digest-1", 9, true, true),
        observation("node-b", "digest-1", 9, true, false),
    ]);
    assert_eq!(reconciled.state, OperationState::Committed);

    let counts = accepted_digest_counts(&[
        observation("node-a", "digest-1", 9, true, true),
        observation("node-b", "digest-1", 9, true, false),
    ]);
    assert_eq!(counts.get("digest-1"), Some(&2));
}

#[test]
fn non_durable_partition_retries_and_epoch_mismatch_never_enters_the_ledger() {
    let retry =
        coordinator(ClusterBootstrapOutcome::ReadyForClusterControl, 2, 2, false).evaluate(&[
            observation("node-a", "digest-1", 9, true, true),
            observation("node-b", "digest-2", 9, false, false),
        ]);
    assert_eq!(retry.state, OperationState::RetryableFailure);
    assert_eq!(retry.failure_class, Some(FailureClass::NodeUnavailable));
    assert!(!retry.permit_local_durability);

    let epoch_mismatch = coordinator(ClusterBootstrapOutcome::ReadyForClusterControl, 2, 2, true)
        .evaluate(&[
            observation("node-a", "digest-1", 9, true, true),
            observation("node-b", "digest-1", 8, true, false),
        ]);
    assert_eq!(epoch_mismatch.state, OperationState::RetryableFailure);
    assert_eq!(
        epoch_mismatch.failure_class,
        Some(FailureClass::EpochMismatch)
    );
    assert!(!epoch_mismatch.permit_local_durability);
}
