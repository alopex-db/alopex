use std::collections::{BTreeMap, BTreeSet};

use alopex_cluster::{
    ClusterId, ClusterIdentity, ClusterManager, ClusterManagerConfig, ClusterReadPointAuthority,
    CommittedMetadata, CommittedMetadataProjection, CommittedMetadataProjector, Endpoint, NodeRole,
    NodeState, RangeDirectory, RangeId, RangeRoutingDefinition, RangeTransferCoordinator,
    RangeTransferPhase, ReadConsistencyMode, ReadPointFailure, ReadPointRequest, RequestId,
    RoutingOutcomeKind, SUPPORTED_UPGRADE_SOURCE_VERSION, TableRef, UpgradeCheckpoint,
    UpgradeInput, UpgradeOutcome, UpgradePlanner, UpgradeSourceKind,
};
use alopex_core::CanonicalRowKey;

const I12_REGISTER: [&str; 8] = [
    "status.membership",
    "metadata.routing.unavailable",
    "metadata.routing.local_only",
    "range.split_merge",
    "range.gap_reject",
    "transfer.recovery",
    "read_point.fail_closed",
    "schema.upgrade",
];

#[derive(Debug)]
struct StatusRow {
    operation: &'static str,
    passed: bool,
}

fn definition(id: &str, lower: Option<u64>, upper: Option<u64>) -> RangeRoutingDefinition {
    RangeRoutingDefinition {
        range_id: RangeId::new(id),
        table_ref: TableRef::new("default.public.v09"),
        table_id: 7,
        lower_inclusive: lower.map(|value| CanonicalRowKey::new(7, value).encode()),
        upper_exclusive: upper.map(|value| CanonicalRowKey::new(7, value).encode()),
        generation: 1,
    }
}

#[test]
fn i12_cluster_status_and_failure_boundaries_have_fixed_status_rows() {
    let mut identity = ClusterIdentity::new("node-a", NodeRole::Gateway, NodeState::Joining);
    identity.cluster_id = Some(ClusterId::new("cluster-v09"));
    identity.advertised_endpoint = Some(Endpoint::new("127.0.0.1:7001"));
    let mut manager = ClusterManager::new(ClusterManagerConfig::cluster_aware_chirps_unavailable(
        identity,
    ))
    .unwrap();
    let joined = manager.join().unwrap();
    let left = manager.leave().unwrap();

    let unavailable = CommittedMetadataProjection::unavailable().routing_outcome(7);
    let metadata = CommittedMetadata::new(ClusterId::new("cluster-v09"));
    let local_only = CommittedMetadataProjector
        .project(&metadata, &BTreeMap::new())
        .routing_outcome(7);
    let schema_initially_unowned = metadata.schema().owner.is_none();

    let mut directory =
        RangeDirectory::from_definitions(10, [definition("all", None, None)]).unwrap();
    let split = directory
        .split(
            &RangeId::new("all"),
            CanonicalRowKey::new(7, 50).encode(),
            RangeId::new("left"),
            RangeId::new("right"),
            10,
        )
        .unwrap();
    let merged = directory
        .merge(
            &RangeId::new("left"),
            &RangeId::new("right"),
            RangeId::new("merged"),
            11,
        )
        .unwrap();
    let gap = RangeDirectory::from_definitions(
        1,
        [
            definition("before", None, Some(40)),
            definition("after", Some(50), None),
        ],
    )
    .unwrap_err();

    let mut transfer = RangeTransferCoordinator::default();
    let request = RequestId::new("v09-transfer");
    transfer
        .prepare(request.clone(), "transfer", "node-a", "node-b")
        .unwrap();
    transfer.copy_chunk(&request).unwrap();
    transfer.verify(&request, 42).unwrap();
    let published = transfer.publish(&request).unwrap();
    let recovered = transfer.publish(&request).unwrap();

    let read_point = ClusterReadPointAuthority.issue(
        &metadata,
        &ReadPointRequest {
            ranges: BTreeSet::new(),
            consistency: ReadConsistencyMode::Strong,
        },
        &[],
        0,
    );

    let planner = UpgradePlanner;
    let input = UpgradeInput {
        source_version: SUPPORTED_UPGRADE_SOURCE_VERSION.to_owned(),
        source_kind: UpgradeSourceKind::SingleNode,
        source_hash: "source-hash".to_owned(),
        legacy_metadata_hash: None,
    };
    let mut upgrade = planner.plan("v09-upgrade", input.clone()).unwrap();
    planner
        .advance(
            &mut upgrade,
            UpgradeCheckpoint::CompatibilityValidated,
            None,
        )
        .unwrap();
    let upgrade_outcome = planner.resume(&mut upgrade, &input);

    let rows = [
        StatusRow {
            operation: "status.membership",
            passed: joined.degraded
                && joined.identity.lifecycle_state == NodeState::Active
                && left.identity.lifecycle_state == NodeState::Leaving,
        },
        StatusRow {
            operation: "metadata.routing.unavailable",
            passed: unavailable.kind == RoutingOutcomeKind::Unavailable
                && unavailable.reason_code == "metadata_unavailable",
        },
        StatusRow {
            operation: "metadata.routing.local_only",
            passed: local_only.kind == RoutingOutcomeKind::LocalOnly
                && local_only.reason_code == "range_not_configured",
        },
        StatusRow {
            operation: "range.split_merge",
            passed: split.predecessors == [RangeId::new("all")]
                && merged.successors == [RangeId::new("merged")]
                && directory.ranges().contains_key(&RangeId::new("merged")),
        },
        StatusRow {
            operation: "range.gap_reject",
            passed: gap.to_string().contains("gap"),
        },
        StatusRow {
            operation: "transfer.recovery",
            passed: published.phase == RangeTransferPhase::Published
                && published.serving_owner.as_str() == "node-b"
                && recovered == published,
        },
        StatusRow {
            operation: "read_point.fail_closed",
            passed: matches!(read_point, Err(ReadPointFailure::EmptyRangeSet)),
        },
        StatusRow {
            operation: "schema.upgrade",
            passed: schema_initially_unowned
                && upgrade.checkpoint == UpgradeCheckpoint::CompatibilityValidated
                && upgrade_outcome == UpgradeOutcome::ResumeRequired,
        },
    ];
    let names: Vec<_> = rows.iter().map(|row| row.operation).collect();
    assert_eq!(names, I12_REGISTER, "the I-12 cluster register drifted");
    for row in rows {
        assert!(
            row.passed,
            "{} must retain its status contract",
            row.operation
        );
    }
}
