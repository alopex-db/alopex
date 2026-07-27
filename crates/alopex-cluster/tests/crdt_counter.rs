use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use alopex_cluster::{
    CrdtCounterProjection, CrdtOperationEnvelope, CrdtOperationKind, CrdtOperationLedger,
    CrdtPayload, OperationState, RangeIdentity,
};
use alopex_core::MemoryKV;

fn wal_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "alopex-crdt-counter-{}-{nonce}.wal",
        std::process::id()
    ))
}

fn remove_wal(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sst"));
}

fn envelope(
    object_id: &str,
    operation_id: &str,
    operation: CrdtOperationKind,
    payload: CrdtPayload,
) -> CrdtOperationEnvelope {
    CrdtOperationEnvelope::new(
        object_id,
        RangeIdentity::new("cluster-a", 7, "range-a", None, None, 1, 9),
        "node-a",
        format!("request-{operation_id}"),
        operation_id,
        12,
        operation,
        payload,
    )
    .expect("valid counter envelope")
}

fn create(object_id: &str, operation_id: &str, initial_value: i64) -> CrdtOperationEnvelope {
    envelope(
        object_id,
        operation_id,
        CrdtOperationKind::CounterCreate,
        CrdtPayload::Counter {
            initial_value: Some(initial_value),
            delta: None,
        },
    )
}

fn delta(
    object_id: &str,
    operation_id: &str,
    operation: CrdtOperationKind,
    value: i64,
) -> CrdtOperationEnvelope {
    envelope(
        object_id,
        operation_id,
        operation,
        CrdtPayload::Counter {
            initial_value: None,
            delta: Some(value),
        },
    )
}

#[test]
fn counter_uses_unique_signed_deltas_and_replays_after_wal_restart() {
    let path = wal_path();
    remove_wal(&path);

    {
        let counter = CrdtCounterProjection::new(MemoryKV::open(&path).expect("open WAL"));
        assert_eq!(
            counter
                .apply(&create("counter-a", "create", -2), 30)
                .unwrap()
                .value
                .value,
            -2
        );
        assert_eq!(
            counter
                .apply(
                    &delta(
                        "counter-a",
                        "increment",
                        CrdtOperationKind::CounterIncrement,
                        5
                    ),
                    30
                )
                .unwrap()
                .value
                .value,
            3
        );
        let duplicate = counter
            .apply(
                &delta(
                    "counter-a",
                    "increment",
                    CrdtOperationKind::CounterIncrement,
                    5,
                ),
                30,
            )
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.value.value, 3);
        assert_eq!(duplicate.ledger.duplicate_count, 1);
        assert_eq!(
            counter
                .apply(
                    &delta(
                        "counter-a",
                        "decrement",
                        CrdtOperationKind::CounterDecrement,
                        3
                    ),
                    30
                )
                .unwrap()
                .value
                .value,
            0
        );
    }

    {
        let counter = CrdtCounterProjection::new(MemoryKV::open(&path).expect("reopen WAL"));
        let read = envelope(
            "counter-a",
            "read",
            CrdtOperationKind::CounterRead,
            CrdtPayload::None,
        );
        let value = counter.read(&read).expect("read recovered counter");
        assert_eq!(value.initial_value, -2);
        assert_eq!(value.accepted_delta_total, 2);
        assert_eq!(value.value, 0);
        assert_eq!(value.accepted_operation_versions.len(), 3);
    }

    remove_wal(&path);
}

#[test]
fn overflow_and_underflow_leave_projection_and_ledger_unchanged() {
    let counter = CrdtCounterProjection::new(MemoryKV::new());
    counter
        .apply(&create("counter-max", "create-max", i64::MAX), 30)
        .unwrap();
    assert!(
        counter
            .apply(
                &delta(
                    "counter-max",
                    "overflow",
                    CrdtOperationKind::CounterIncrement,
                    1
                ),
                30,
            )
            .is_err()
    );
    let max_read = envelope(
        "counter-max",
        "read-max",
        CrdtOperationKind::CounterRead,
        CrdtPayload::None,
    );
    assert_eq!(counter.read(&max_read).unwrap().value, i64::MAX);

    counter
        .apply(&create("counter-min", "create-min", i64::MIN), 30)
        .unwrap();
    assert!(
        counter
            .apply(
                &delta(
                    "counter-min",
                    "underflow",
                    CrdtOperationKind::CounterDecrement,
                    1
                ),
                30,
            )
            .is_err()
    );
    let min_read = envelope(
        "counter-min",
        "read-min",
        CrdtOperationKind::CounterRead,
        CrdtPayload::None,
    );
    assert_eq!(counter.read(&min_read).unwrap().value, i64::MIN);

    let ledger = CrdtOperationLedger::new(counter.into_store());
    assert!(ledger.read("overflow").unwrap().is_none());
    assert!(ledger.read("underflow").unwrap().is_none());
    let original = ledger
        .read("create-max")
        .unwrap()
        .expect("first operation exists");
    assert_eq!(original.first_state, OperationState::Committed);
}
