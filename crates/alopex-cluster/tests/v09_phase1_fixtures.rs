use alopex_cluster::{
    NodeId, RangeDirectory, RangeId, RangeRoutingDefinition, RangeTransferCoordinator,
    RangeTransferPhase, RequestId, TableRef,
};
use alopex_core::CanonicalRowKey;

fn definition(id: &str, lower: Option<u64>, upper: Option<u64>) -> RangeRoutingDefinition {
    RangeRoutingDefinition {
        range_id: RangeId::new(id),
        table_ref: TableRef::new("default.public.fixture"),
        table_id: 7,
        lower_inclusive: lower.map(|value| CanonicalRowKey::new(7, value).encode()),
        upper_exclusive: upper.map(|value| CanonicalRowKey::new(7, value).encode()),
        generation: 1,
    }
}

#[test]
fn range_split_merge_fixture_is_deterministic_and_version_fenced() {
    let mut directory = RangeDirectory::from_definitions(10, [definition("range-a", None, None)])
        .expect("complete fixture coverage");
    let split_key = CanonicalRowKey::new(7, 50).encode();
    let split = directory
        .split(
            &RangeId::new("range-a"),
            split_key,
            RangeId::new("range-left"),
            RangeId::new("range-right"),
            10,
        )
        .expect("split");
    assert_eq!(directory.metadata_version(), 11);
    assert_eq!(split.predecessors, vec![RangeId::new("range-a")]);
    let merge = directory
        .merge(
            &RangeId::new("range-left"),
            &RangeId::new("range-right"),
            RangeId::new("range-merged"),
            11,
        )
        .expect("merge");
    assert_eq!(directory.metadata_version(), 12);
    assert_eq!(merge.successors, vec![RangeId::new("range-merged")]);
    assert!(
        directory
            .ranges()
            .contains_key(&RangeId::new("range-merged"))
    );
}

#[test]
fn range_move_fixture_preserves_owner_until_verified_publish_and_is_idempotent() {
    let mut coordinator = RangeTransferCoordinator::default();
    let request_id = RequestId::new("fixture-move-1");
    let prepared = coordinator
        .prepare(
            request_id.clone(),
            "transfer-1",
            NodeId::new("node-a"),
            NodeId::new("node-b"),
        )
        .expect("prepare");
    assert_eq!(prepared.phase, RangeTransferPhase::Prepared);
    assert_eq!(prepared.serving_owner, NodeId::new("node-a"));
    assert_eq!(
        coordinator.prepare(request_id.clone(), "transfer-1", "node-a", "node-b"),
        Ok(prepared.clone())
    );
    coordinator.copy_chunk(&request_id).expect("copy");
    coordinator.verify(&request_id, 42).expect("verify");
    let published = coordinator.publish(&request_id).expect("publish");
    assert_eq!(published.phase, RangeTransferPhase::Published);
    assert_eq!(published.serving_owner, NodeId::new("node-b"));
    assert_eq!(
        coordinator
            .publish(&request_id)
            .expect("idempotent publish"),
        published
    );
}

#[test]
fn range_gap_and_overlap_fixtures_are_rejected_before_commit() {
    let gap = RangeDirectory::from_definitions(
        3,
        [
            definition("range-a", None, Some(40)),
            definition("range-b", Some(50), None),
        ],
    )
    .expect_err("gap must be rejected");
    assert!(gap.to_string().contains("gap"));

    let overlap = RangeDirectory::from_definitions(
        3,
        [
            definition("range-a", None, Some(60)),
            definition("range-b", Some(50), None),
        ],
    )
    .expect_err("overlap must be rejected");
    assert!(overlap.to_string().contains("overlap"));
}
