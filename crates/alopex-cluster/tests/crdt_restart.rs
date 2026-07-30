use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use alopex_cluster::{
    CrdtCounterProjection, CrdtOperationEnvelope, CrdtOperationKind, CrdtPayload, RangeIdentity,
};
use alopex_core::MemoryKV;

fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "alopex-v09-f2-restart-{}-{}.wal",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn operation(
    id: &str,
    kind: CrdtOperationKind,
    initial: Option<i64>,
    delta: Option<i64>,
) -> CrdtOperationEnvelope {
    CrdtOperationEnvelope::new(
        "fixture-counter",
        RangeIdentity::new("fixture", 7, "range-fixture", None, None, 1, 9),
        "node-a",
        format!("request-{id}"),
        id,
        1,
        kind,
        if kind == CrdtOperationKind::CounterRead {
            CrdtPayload::None
        } else {
            CrdtPayload::Counter {
                initial_value: initial,
                delta,
            }
        },
    )
    .expect("envelope")
}

#[test]
fn seeded_duplicate_reorder_and_restart_fixture_retains_one_converged_counter() {
    let path = path();
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(path.with_extension("sst"));
    let create = operation("create", CrdtOperationKind::CounterCreate, Some(-4), None);
    let increment = operation(
        "increment",
        CrdtOperationKind::CounterIncrement,
        None,
        Some(7),
    );
    let decrement = operation(
        "decrement",
        CrdtOperationKind::CounterDecrement,
        None,
        Some(2),
    );
    {
        let projection = CrdtCounterProjection::new(MemoryKV::open(&path).expect("WAL"));
        projection.apply(&create, 30).expect("create");
        // The seeded order intentionally differs from operation-id order.
        projection.apply(&decrement, 30).expect("decrement");
        let first = projection.apply(&increment, 30).expect("increment");
        assert_eq!(first.value.value, 1);
        let duplicate = projection.apply(&increment, 30).expect("duplicate");
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.ledger.duplicate_count, 1);
    }
    {
        let projection = CrdtCounterProjection::new(MemoryKV::open(&path).expect("restart WAL"));
        let read = operation("read", CrdtOperationKind::CounterRead, None, None);
        let value = projection.read(&read).expect("restart read");
        assert_eq!(value.value, 1);
        assert_eq!(value.accepted_delta_total, 5);
        assert_eq!(value.accepted_operation_versions.len(), 3);
    }
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(path.with_extension("sst"));
}
