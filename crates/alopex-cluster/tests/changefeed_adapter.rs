use alopex_cluster::{
    ChangeOperationType, FailureClass, FeedIdentity, OperationState, OrderingScope, Placement,
    PlacementReadiness, PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome,
    RoutingOutcomeKind,
    changefeed::{JournalAdapterError, JournalEventAdapter},
};
use alopex_core::kv::{RangeChangePayload, RangeChangeRecord};

fn feed(epoch: u64) -> FeedIdentity {
    FeedIdentity::new(
        "feed-a",
        RangeIdentity::new("cluster-a", 7, "range-a", None, None, 4, epoch),
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
        OperationState::Committed,
    )
    .unwrap()
}

fn routing() -> RoutingOutcome {
    RoutingOutcome::new(
        RoutingOutcomeKind::SingleRange,
        Some(feed(9).range),
        12,
        "placement_ready",
    )
}

fn record(payload: Vec<RangeChangePayload>) -> RangeChangeRecord {
    RangeChangeRecord {
        range_id: "range-a".to_string(),
        generation: 3,
        epoch: 9,
        predecessor_epoch: Some(8),
        replay_id: "replay-9".to_string(),
        payload,
    }
}

#[test]
fn delete_and_index_tombstone_become_stable_ordinal_events() {
    let source = record(vec![
        RangeChangePayload::DeleteRow {
            row_key: b"row-a".to_vec(),
            tombstone: b"prior-row".to_vec(),
        },
        RangeChangePayload::DeleteIndex {
            index_id: 7,
            index_key: b"index-a".to_vec(),
            row_key: b"row-a".to_vec(),
        },
    ]);
    let adapter = JournalEventAdapter;
    let first = adapter.adapt(&feed(9), &routing(), &source).unwrap();
    let replay = adapter.adapt(&feed(9), &routing(), &source).unwrap();

    assert_eq!(first, replay);
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].operation_type, ChangeOperationType::Delete);
    assert_eq!(first[0].checkpoint.payload_ordinal, 0);
    assert_eq!(first[0].payload.payload, Some(b"prior-row".to_vec()));
    assert_eq!(first[1].operation_type, ChangeOperationType::Tombstone);
    assert_eq!(first[1].checkpoint.payload_ordinal, 1);
    assert_eq!(first[0].operation_id, "replay-9");
    assert_eq!(first[0].request_id.as_str(), "replay-9");
    let auxiliary = String::from_utf8(first[1].payload.payload.clone().unwrap()).unwrap();
    assert!(auxiliary.contains("index_tombstone"));
    assert!(auxiliary.contains("\"index_id\":7"));
}

#[test]
fn unclassified_upsert_and_schema_are_rejected_before_delivery() {
    let source = record(vec![RangeChangePayload::UpsertRow {
        row_key: b"row-a".to_vec(),
        encoded_row: b"post-image-only".to_vec(),
    }]);
    let adapter = JournalEventAdapter;
    let error = adapter.adapt(&feed(9), &routing(), &source).unwrap_err();
    assert!(matches!(
        error,
        JournalAdapterError::PayloadUnavailable {
            payload_ordinal: 0,
            reason_code: "operation_type_unattributable"
        }
    ));
    assert_eq!(error.failure_class(), FailureClass::InvalidRequest);
    assert_eq!(error.reason_code(), "operation_type_unattributable");

    let schema = adapter.reject_schema();
    assert!(matches!(
        schema,
        JournalAdapterError::UnsupportedChangeKind {
            reason_code: "schema_unsupported"
        }
    ));
    assert_eq!(schema.failure_class(), FailureClass::InvalidRequest);
}

#[test]
fn source_range_generation_or_epoch_mismatch_cannot_emit_event() {
    let source = record(vec![RangeChangePayload::DeleteRow {
        row_key: b"row-a".to_vec(),
        tombstone: b"prior-row".to_vec(),
    }]);
    let error = JournalEventAdapter
        .adapt(&feed(10), &routing(), &source)
        .unwrap_err();
    assert!(matches!(
        error,
        JournalAdapterError::SourceMismatch {
            field: "data_epoch"
        }
    ));
    assert_eq!(error.failure_class(), FailureClass::StaleMetadata);
    assert_eq!(error.reason_code(), "journal_source_mismatch");
}
