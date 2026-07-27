use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use alopex_cluster::{
    CrdtOperationEnvelope, CrdtOperationKind, CrdtOperationLedger, CrdtPayload, CrdtSetProjection,
    RangeIdentity, SetProjectionLimits,
};
use alopex_core::MemoryKV;

const CREATE_ID: &str = "00000000-0000-0000-0000-000000000001";
const ADD_V7_ID: &str = "00000000-0000-0000-0000-000000000002";
const REMOVE_V7_ID: &str = "00000000-0000-0000-0000-000000000003";
const ADD_V8_ID: &str = "00000000-0000-0000-0000-000000000004";

fn wal_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "alopex-crdt-set-{}-{nonce}.wal",
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
    update_version: u64,
    data_epoch: u64,
    operation: CrdtOperationKind,
    payload: CrdtPayload,
) -> CrdtOperationEnvelope {
    CrdtOperationEnvelope::new(
        object_id,
        RangeIdentity::new("cluster-a", 7, "range-a", None, None, 1, data_epoch),
        "node-a",
        format!("request-{operation_id}"),
        operation_id,
        update_version,
        operation,
        payload,
    )
    .expect("valid Set envelope")
}

fn create(object_id: &str) -> CrdtOperationEnvelope {
    envelope(
        object_id,
        CREATE_ID,
        1,
        9,
        CrdtOperationKind::SetCreate,
        CrdtPayload::None,
    )
}

fn member(
    object_id: &str,
    operation_id: &str,
    update_version: u64,
    data_epoch: u64,
    operation: CrdtOperationKind,
    member: &str,
) -> CrdtOperationEnvelope {
    envelope(
        object_id,
        operation_id,
        update_version,
        data_epoch,
        operation,
        CrdtPayload::Set {
            member: Some(member.to_string()),
        },
    )
}

fn read(object_id: &str) -> CrdtOperationEnvelope {
    envelope(
        object_id,
        "read-id",
        0,
        9,
        CrdtOperationKind::SetRead,
        CrdtPayload::None,
    )
}

#[test]
fn add_remove_readd_is_total_ordered_and_independent_of_arrival_order() {
    let forward = CrdtSetProjection::new(MemoryKV::new());
    forward.apply(&create("set-a"), 30).unwrap();
    forward
        .apply(
            &member("set-a", ADD_V7_ID, 7, 9, CrdtOperationKind::SetAdd, "é"),
            30,
        )
        .unwrap();
    forward
        .apply(
            &member(
                "set-a",
                REMOVE_V7_ID,
                7,
                9,
                CrdtOperationKind::SetRemove,
                "é",
            ),
            30,
        )
        .unwrap();
    let forward_result = forward
        .apply(
            &member("set-a", ADD_V8_ID, 8, 9, CrdtOperationKind::SetAdd, "é"),
            30,
        )
        .unwrap()
        .value;

    let reverse = CrdtSetProjection::new(MemoryKV::new());
    reverse.apply(&create("set-a"), 30).unwrap();
    reverse
        .apply(
            &member("set-a", ADD_V8_ID, 8, 9, CrdtOperationKind::SetAdd, "é"),
            30,
        )
        .unwrap();
    reverse
        .apply(
            &member(
                "set-a",
                REMOVE_V7_ID,
                7,
                9,
                CrdtOperationKind::SetRemove,
                "é",
            ),
            30,
        )
        .unwrap();
    let reverse_result = reverse
        .apply(
            &member("set-a", ADD_V7_ID, 7, 9, CrdtOperationKind::SetAdd, "é"),
            30,
        )
        .unwrap()
        .value;

    assert_eq!(forward_result, reverse_result);
    assert_eq!(forward_result.members, vec!["é"]);
    let winner = forward_result.member_versions.get("é").unwrap();
    assert!(winner.present);
    assert_eq!(winner.update_version, 8);
    assert_eq!(winner.operation_id, ADD_V8_ID);
}

#[test]
fn non_nfc_size_and_epoch_fail_before_ledger_mutation() {
    let projection = CrdtSetProjection::with_limits(
        MemoryKV::new(),
        SetProjectionLimits {
            max_member_bytes: 4,
            max_object_bytes: 6,
        },
    );
    projection.apply(&create("set-b"), 30).unwrap();

    let non_nfc_id = "00000000-0000-0000-0000-000000000010";
    assert!(
        projection
            .apply(
                &member(
                    "set-b",
                    non_nfc_id,
                    2,
                    9,
                    CrdtOperationKind::SetAdd,
                    "e\u{301}",
                ),
                30,
            )
            .is_err()
    );
    let too_large_id = "00000000-0000-0000-0000-000000000011";
    assert!(
        projection
            .apply(
                &member(
                    "set-b",
                    too_large_id,
                    3,
                    9,
                    CrdtOperationKind::SetAdd,
                    "large",
                ),
                30,
            )
            .is_err()
    );
    let stale_epoch_id = "00000000-0000-0000-0000-000000000012";
    assert!(
        projection
            .apply(
                &member(
                    "set-b",
                    stale_epoch_id,
                    4,
                    10,
                    CrdtOperationKind::SetAdd,
                    "ok",
                ),
                30,
            )
            .is_err()
    );
    assert!(projection.read(&read("set-b")).unwrap().members.is_empty());

    let ledger = CrdtOperationLedger::new(projection.into_store());
    assert!(ledger.read(non_nfc_id).unwrap().is_none());
    assert!(ledger.read(too_large_id).unwrap().is_none());
    assert!(ledger.read(stale_epoch_id).unwrap().is_none());
}

#[test]
fn set_winner_survives_wal_restart_and_duplicate_replay() {
    let path = wal_path();
    remove_wal(&path);
    {
        let projection = CrdtSetProjection::new(MemoryKV::open(&path).expect("open WAL"));
        projection.apply(&create("set-c"), 30).unwrap();
        assert!(projection.apply(&create("set-c"), 30).unwrap().duplicate);
        projection
            .apply(
                &member(
                    "set-c",
                    ADD_V7_ID,
                    7,
                    9,
                    CrdtOperationKind::SetAdd,
                    "member",
                ),
                30,
            )
            .unwrap();
    }
    {
        let projection = CrdtSetProjection::new(MemoryKV::open(&path).expect("recover WAL"));
        let duplicate = projection
            .apply(
                &member(
                    "set-c",
                    ADD_V7_ID,
                    7,
                    9,
                    CrdtOperationKind::SetAdd,
                    "member",
                ),
                30,
            )
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.ledger.duplicate_count, 1);
        assert_eq!(
            projection.read(&read("set-c")).unwrap().members,
            vec!["member"]
        );
    }
    remove_wal(&path);
}
