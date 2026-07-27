use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use alopex_cluster::{
    CrdtLedgerAdmission, CrdtOperationEnvelope, CrdtOperationKind, CrdtOperationLedger,
    CrdtPayload, OperationState, RangeIdentity,
};
use alopex_core::MemoryKV;

fn wal_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "alopex-crdt-ledger-{}-{nonce}.wal",
        std::process::id()
    ))
}

fn remove_wal(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sst"));
}

fn envelope(initial_value: i64, range_id: &str) -> CrdtOperationEnvelope {
    CrdtOperationEnvelope::new(
        "counter-a",
        RangeIdentity::new("cluster-a", 7, range_id, None, None, 1, 9),
        "node-a",
        "request-a",
        "operation-a",
        12,
        CrdtOperationKind::CounterCreate,
        CrdtPayload::Counter {
            initial_value: Some(initial_value),
            delta: None,
        },
    )
    .expect("valid envelope")
}

#[test]
fn retained_first_outcome_duplicate_count_and_tombstone_survive_wal_restart() {
    let path = wal_path();
    remove_wal(&path);

    {
        let ledger = CrdtOperationLedger::new(MemoryKV::open(&path).expect("open durable WAL"));
        let first = ledger
            .admit(
                &envelope(4, "range-a"),
                "committed:4",
                OperationState::Committed,
                None,
                20,
            )
            .expect("record first operation");
        assert!(!first.is_duplicate());
        assert_eq!(first.record().first_outcome, "committed:4");

        let duplicate = ledger
            .admit(
                &envelope(4, "range-a"),
                "must-not-replace-first-outcome",
                OperationState::TerminalFailure,
                None,
                1,
            )
            .expect("replay retained operation");
        assert!(matches!(duplicate, CrdtLedgerAdmission::Duplicate(_)));
        assert_eq!(duplicate.record().duplicate_count, 1);
        assert_eq!(duplicate.record().first_outcome, "committed:4");
        ledger
            .tombstone("operation-a", 30)
            .expect("retain tombstone");
    }

    {
        let ledger = CrdtOperationLedger::new(MemoryKV::open(&path).expect("recover durable WAL"));
        let recovered = ledger
            .read("operation-a")
            .expect("read recovered record")
            .expect("record exists");
        assert!(recovered.tombstoned);
        assert_eq!(recovered.first_outcome, "committed:4");
        assert_eq!(recovered.duplicate_count, 1);
        assert_eq!(recovered.retention_until_epoch, 30);

        let replay = ledger
            .admit(
                &envelope(4, "range-a"),
                "must-not-replace-first-outcome",
                OperationState::TerminalFailure,
                None,
                1,
            )
            .expect("replay after restart");
        assert!(replay.is_duplicate());
        assert_eq!(replay.record().duplicate_count, 2);
        assert!(replay.record().tombstoned);

        let changed_payload = ledger.admit(
            &envelope(5, "range-a"),
            "must-not-apply",
            OperationState::Committed,
            None,
            30,
        );
        assert!(
            changed_payload
                .expect_err("changed payload conflicts")
                .is_conflict()
        );
        assert_eq!(
            ledger.read("operation-a").unwrap().unwrap().duplicate_count,
            2
        );

        let changed_scope = ledger.admit(
            &envelope(4, "range-b"),
            "must-not-apply",
            OperationState::Committed,
            None,
            30,
        );
        assert!(
            changed_scope
                .expect_err("changed range conflicts")
                .is_conflict()
        );
    }

    remove_wal(&path);
}
