use alopex_cluster::{
    Checkpoint, ClusterId, FailureClass, RangeId,
    changefeed::{CheckpointCursor, CursorError, EventIdentity},
};

fn checkpoint(
    generation: u64,
    epoch: u64,
    commit_position: u64,
    payload_ordinal: u32,
) -> Checkpoint {
    Checkpoint::new(
        "feed-a",
        "range-a",
        generation,
        commit_position,
        payload_ordinal,
        epoch,
        Some(99),
    )
    .expect("valid checkpoint")
}

#[test]
fn duplicate_source_position_has_one_stable_event_id() {
    let first = EventIdentity::new("cluster-a", "range-a", 4, 9, "replay-a", 2)
        .expect("valid source identity");
    let duplicate = EventIdentity::new(
        ClusterId::from("cluster-a"),
        RangeId::from("range-a"),
        4,
        9,
        "replay-a",
        2,
    )
    .expect("same source identity");
    let next_payload = EventIdentity::new("cluster-a", "range-a", 4, 9, "replay-a", 3)
        .expect("next payload identity");

    assert_eq!(
        first.canonical_bytes().unwrap(),
        duplicate.canonical_bytes().unwrap()
    );
    assert_eq!(first.event_id().unwrap(), duplicate.event_id().unwrap());
    assert_ne!(first.event_id().unwrap(), next_payload.event_id().unwrap());
}

#[test]
fn cursor_round_trip_preserves_half_open_range_order() {
    let base_checkpoint = checkpoint(4, 9, 23, 2);
    let encoded = CheckpointCursor::new(base_checkpoint.clone())
        .unwrap()
        .encode()
        .unwrap();
    assert!(!encoded.contains('='));
    let decoded = CheckpointCursor::decode(&encoded).expect("valid cursor");
    assert_eq!(decoded.checkpoint(), &base_checkpoint);

    assert!(
        !CheckpointCursor::new(checkpoint(4, 9, 23, 2))
            .unwrap()
            .is_strictly_after(&base_checkpoint)
            .unwrap()
    );
    assert!(
        CheckpointCursor::new(checkpoint(4, 9, 23, 3))
            .unwrap()
            .is_strictly_after(&base_checkpoint)
            .unwrap()
    );
    assert!(
        CheckpointCursor::new(checkpoint(5, 1, 0, 0))
            .unwrap()
            .is_strictly_after(&base_checkpoint)
            .unwrap()
    );
}

#[test]
fn malformed_versioned_or_wrong_scope_cursor_is_invalid_checkpoint() {
    let encoded = CheckpointCursor::new(checkpoint(4, 9, 23, 2))
        .unwrap()
        .encode()
        .unwrap();
    let other_range = RangeId::from("range-b");
    let version_two = format!("C{}", &encoded[1..]);

    for invalid in ["", "=", "A", "***", "A===", &version_two] {
        let error = CheckpointCursor::decode(invalid).expect_err("cursor must be rejected");
        assert_eq!(error.failure_class(), FailureClass::InvalidRequest);
        assert_eq!(error.reason_code(), "invalid_checkpoint");
    }
    assert_eq!(
        CheckpointCursor::decode_for(&encoded, "feed-a", &other_range).unwrap_err(),
        CursorError::RangeMismatch
    );
    assert_eq!(
        CheckpointCursor::decode_for(&encoded, "feed-b", &RangeId::from("range-a")).unwrap_err(),
        CursorError::FeedMismatch
    );
}
