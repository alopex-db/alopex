use std::collections::BTreeSet;

use alopex_cluster::{
    ClusterMetricsSource, NodeId, NodeRole, RoutingDecisionKind, SimulatedClusterFixture,
    SimulatedClusterHarness, SimulatedSubRequestState, SimulatedTargetBehavior,
    SimulatedTargetOutcome, StableDiagnosticCode,
};

#[test]
fn fixed_three_node_fixture_produces_expected_scatter_gather_diagnostics() {
    let fixture = SimulatedClusterFixture::fixed_three_node_scatter_gather();
    let run = SimulatedClusterHarness::new().run(&fixture);

    run.validate_expected(&fixture).unwrap();
    assert_eq!(
        run.diagnostics.decision,
        RoutingDecisionKind::ScatterGatherSimulated
    );
    assert_eq!(
        run.diagnostics.reason,
        StableDiagnosticCode::ScatterGatherSimulated
    );
    assert!(run.diagnostics.roles.contains(&NodeRole::Gateway));
    assert!(run.diagnostics.roles.contains(&NodeRole::Worker));
    assert_eq!(run.diagnostics.targets.len(), 2);
    assert_eq!(
        run.metrics_summary.source,
        ClusterMetricsSource::SimulatedHarness
    );
    assert!(
        run.metrics_summary
            .members
            .iter()
            .all(
                |member| member.source == ClusterMetricsSource::SimulatedHarness
                    && member.latency_ms.is_none()
                    && member.load.is_none()
            )
    );
}

#[test]
fn simulated_retry_policy_is_bounded_and_records_cancellation_state() {
    let fixture = SimulatedClusterFixture::fixed_three_node_scatter_gather();
    let run = SimulatedClusterHarness::new().run(&fixture);
    let retry_summary = run
        .diagnostics
        .retry_summary
        .as_ref()
        .expect("retry summary");

    assert_eq!(retry_summary.max_attempts, 3);
    assert_eq!(retry_summary.max_backoff_ms, 1_000);
    assert_eq!(
        retry_summary.cancellation_state.as_deref(),
        Some("cancelled_after_2_attempts")
    );
    assert!(
        run.diagnostic_codes
            .contains(&StableDiagnosticCode::RetryScheduled)
    );
    assert!(
        run.diagnostic_codes
            .contains(&StableDiagnosticCode::SubRequestCancelled)
    );
    assert!(
        run.sub_requests
            .iter()
            .all(|request| request.backoff_ms <= retry_summary.max_backoff_ms)
    );
    assert!(
        run.sub_requests
            .iter()
            .any(|request| request.state == SimulatedSubRequestState::Cancelled)
    );
}

#[test]
fn simulated_sub_request_idempotency_key_is_stable_across_retries() {
    let fixture = SimulatedClusterFixture::fixed_three_node_scatter_gather();
    let run = SimulatedClusterHarness::new().run(&fixture);
    let cancelled_node = NodeId::new("node-c");
    let retry_keys = run
        .sub_requests
        .iter()
        .filter(|request| request.target.node_id == cancelled_node)
        .map(|request| request.idempotency_key.clone())
        .collect::<BTreeSet<_>>();
    let request_ids = run
        .sub_requests
        .iter()
        .filter(|request| request.target.node_id == cancelled_node)
        .map(|request| request.request_id.clone())
        .collect::<BTreeSet<_>>();

    assert_eq!(retry_keys.len(), 1);
    assert_eq!(request_ids.len(), 2);
    assert!(
        run.sub_requests
            .iter()
            .any(|request| request.attempt > 1
                && request.state == SimulatedSubRequestState::Cancelled)
    );
}

#[test]
fn simulated_retry_exhaustion_does_not_complete_the_sub_request() {
    let fixture = SimulatedClusterFixture::fixed_three_node_scatter_gather().with_target_outcome(
        SimulatedTargetOutcome::new(
            "node-b",
            SimulatedTargetBehavior::RetryThenSucceed {
                failed_attempts: 99,
            },
        ),
    );
    let run = SimulatedClusterHarness::new().run(&fixture);

    assert!(
        run.diagnostic_codes
            .contains(&StableDiagnosticCode::RetryExhausted)
    );
    assert!(
        run.sub_requests
            .iter()
            .any(|request| request.target.node_id == NodeId::new("node-b")
                && request.state == SimulatedSubRequestState::RetryExhausted)
    );
    assert!(
        !run.sub_requests
            .iter()
            .any(|request| request.target.node_id == NodeId::new("node-b")
                && request.state == SimulatedSubRequestState::Completed)
    );
}

#[test]
fn simulated_harness_json_roundtrip_preserves_release_gate_diagnostics() {
    let fixture = SimulatedClusterFixture::fixed_three_node_scatter_gather();
    let run = SimulatedClusterHarness::new().run(&fixture);

    let encoded = serde_json::to_string(&run).unwrap();
    assert!(encoded.contains("scatter_gather_simulated"));
    assert!(encoded.contains("retry_scheduled"));
    assert!(encoded.contains("sub_request_cancelled"));
    assert!(encoded.contains("cancelled_after_2_attempts"));

    let decoded: alopex_cluster::SimulatedClusterRun = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, run);
}

#[test]
fn simulated_shard_range_fixture_is_deterministic() {
    let fixture = SimulatedClusterFixture::fixed_three_node_shard_range();
    let run = SimulatedClusterHarness::new().run(&fixture);

    run.validate_expected(&fixture).unwrap();
    assert_eq!(
        run.diagnostics.decision,
        RoutingDecisionKind::ScatterGatherSimulated
    );
    assert_eq!(run.diagnostics.targets.len(), 2);
    assert_eq!(run.diagnostics.targets[0].node_id, NodeId::new("node-b"));
    assert!(run.diagnostics.targets[0].range_id.is_some());
    assert_eq!(run.diagnostics.targets[1].node_id, NodeId::new("node-c"));
    assert!(run.diagnostics.targets[1].shard_id.is_some());
}
