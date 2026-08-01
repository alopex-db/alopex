use alopex_core::{KVStore, KVTransaction, MemoryKV, ReadAtCapability, ReadAtPoint, TxnMode};

#[test]
fn raw_kv_preserves_bytes_order_rollback_and_rejects_cluster_read_substitution() {
    let store = MemoryKV::new();

    assert!(matches!(
        store.read_at_capability(),
        ReadAtCapability::Unavailable { .. }
    ));
    assert!(
        store.begin_read_at(&ReadAtPoint::new(9, 3, 2, 1)).is_err(),
        "a local MemoryKV snapshot must not be substituted for a cluster-fenced read"
    );

    let mut write = store.begin(TxnMode::ReadWrite).expect("local write begin");
    write
        .put(b"compat:\x00a".to_vec(), b"\x00value-a\xff".to_vec())
        .expect("first byte-preserving put");
    write
        .put(b"compat:\x00b".to_vec(), b"\xffvalue-b\x00".to_vec())
        .expect("second byte-preserving put");
    write.commit_self().expect("local write commit");

    let mut snapshot = store.begin(TxnMode::ReadOnly).expect("local read begin");
    let rows: Vec<_> = snapshot
        .scan_prefix(b"compat:\x00")
        .expect("prefix scan")
        .collect();
    assert_eq!(
        rows,
        vec![
            (b"compat:\x00a".to_vec(), b"\x00value-a\xff".to_vec()),
            (b"compat:\x00b".to_vec(), b"\xffvalue-b\x00".to_vec()),
        ],
        "raw byte keys and values retain their lexicographic local ordering"
    );
    snapshot.rollback_self().expect("read rollback");

    let mut aborted = store.begin(TxnMode::ReadWrite).expect("rollback begin");
    aborted
        .delete(b"compat:\x00a".to_vec())
        .expect("staged delete");
    aborted.rollback_self().expect("rollback discards delete");

    let mut verify = store.begin(TxnMode::ReadOnly).expect("verification begin");
    assert_eq!(
        verify.get(&b"compat:\x00a".to_vec()).expect("read value"),
        Some(b"\x00value-a\xff".to_vec()),
        "a rolled-back raw KV mutation is never visible"
    );
    verify.rollback_self().expect("verification rollback");
}
