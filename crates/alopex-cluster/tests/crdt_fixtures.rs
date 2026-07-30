use alopex_cluster::{
    ClusterBootstrapOutcome, CrdtConvergenceCoordinator, CrdtCoordinatorConfig, CrdtReadinessGate,
    CrdtReplicaObservation, FailureClass, NodeId, OperationState, RangeIdentity,
};

fn fixture_coordinator(local_durable: bool) -> CrdtConvergenceCoordinator {
    CrdtConvergenceCoordinator::new(CrdtCoordinatorConfig {
        gate: CrdtReadinessGate {
            phase_one_approved: true,
            phase_one_evidence_complete: true,
            range_lease_valid: true,
            placement_ready: true,
            recovery_ready: true,
            bootstrap: ClusterBootstrapOutcome::ReadyForClusterControl,
            range: RangeIdentity::new("fixture", 7, "range-fixture", None, None, 1, 9),
            metadata_version: 4,
        },
        local_node: NodeId::new("node-a"),
        configured_replicas: 2,
        quorum: 2,
        local_durable,
    })
}

fn replica(
    node: &str,
    digest: &str,
    epoch: u64,
    reachable: bool,
    durable: bool,
) -> CrdtReplicaObservation {
    CrdtReplicaObservation {
        node_id: node.into(),
        data_epoch: epoch,
        accepted_operation_digest: digest.into(),
        ready: true,
        reachable,
        durable_owner: durable,
    }
}

#[test]
fn seeded_offline_quorum_partition_reconnect_and_range_move_fixtures_match_oracles() {
    let coordinator = fixture_coordinator(true);
    let committed = coordinator.evaluate(&[
        replica("node-a", "seeded-digest", 9, true, true),
        replica("node-b", "seeded-digest", 9, true, false),
    ]);
    assert_eq!(committed.state, OperationState::Committed);
    assert_eq!(committed.routing.reason_code, "replica_converged");

    let partitioned = coordinator.evaluate(&[
        replica("node-a", "seeded-digest", 9, true, true),
        replica("node-b", "seeded-digest", 9, false, false),
    ]);
    assert_eq!(partitioned.state, OperationState::RecoveryPending);
    assert_eq!(
        partitioned.failure_class,
        Some(FailureClass::NodeUnavailable)
    );
    assert!(partitioned.permit_local_durability);

    let reconnect = coordinator.evaluate(&[
        replica("node-b", "seeded-digest", 9, true, false),
        replica("node-a", "seeded-digest", 9, true, true),
    ]);
    assert_eq!(reconnect.state, OperationState::Committed);
    assert_eq!(reconnect.routing.reason_code, "replica_converged");

    let range_move = coordinator.evaluate(&[
        replica("node-a", "seeded-digest", 9, true, true),
        replica("node-b", "seeded-digest", 10, true, false),
    ]);
    assert_eq!(range_move.state, OperationState::RetryableFailure);
    assert_eq!(range_move.failure_class, Some(FailureClass::EpochMismatch));
    assert!(!range_move.permit_local_durability);

    let non_durable = fixture_coordinator(false).evaluate(&[
        replica("node-a", "seeded-digest", 9, true, true),
        replica("node-b", "other-digest", 9, false, false),
    ]);
    assert_eq!(non_durable.state, OperationState::RetryableFailure);
    assert_eq!(
        non_durable.failure_class,
        Some(FailureClass::NodeUnavailable)
    );
    assert!(!non_durable.permit_local_durability);
}
