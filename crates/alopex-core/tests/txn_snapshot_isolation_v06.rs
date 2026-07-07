use std::sync::{mpsc, Arc};
use std::thread;

use alopex_core::{Error, KVStore, KVTransaction, MemoryKV, TxnMode};

fn key(name: &str) -> Vec<u8> {
    format!("kv:{name}").into_bytes()
}

fn value(name: &str) -> Vec<u8> {
    format!("v:{name}").into_bytes()
}

fn conflict_code(err: &Error) -> &'static str {
    match err {
        Error::TxnConflict => "ALOPEX-CORE-TXN-CONFLICT",
        _ => "ALOPEX-CORE-UNKNOWN",
    }
}

fn is_retryable(err: &Error) -> bool {
    matches!(err, Error::TxnConflict)
}

#[test]
fn snapshot_read_does_not_see_post_start_inserts() {
    let store = MemoryKV::new();

    let mut seed = store.begin(TxnMode::ReadWrite).expect("begin seed");
    seed.put(key("item-0"), value("base")).expect("seed put");
    seed.commit_self().expect("seed commit");

    let mut snapshot = store.begin(TxnMode::ReadOnly).expect("begin snapshot");
    let mut writer = store.begin(TxnMode::ReadWrite).expect("begin writer");
    writer.put(key("item-1"), value("new")).expect("writer put");
    writer.commit_self().expect("writer commit");

    let mut rows = snapshot
        .scan_prefix(b"kv:item-")
        .expect("snapshot scan")
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, key("item-0"));
    assert_eq!(rows[0].1, value("base"));

    let mut fresh = store.begin(TxnMode::ReadOnly).expect("begin fresh reader");
    assert_eq!(
        fresh.get(&key("item-1")).expect("fresh read"),
        Some(value("new"))
    );
}

#[test]
fn read_write_conflict_returns_retryable_txn_conflict() {
    let store = MemoryKV::new();

    let mut seed = store.begin(TxnMode::ReadWrite).expect("begin seed");
    seed.put(key("k"), value("v0")).expect("seed put");
    seed.commit_self().expect("seed commit");

    let mut stale_writer = store.begin(TxnMode::ReadWrite).expect("begin stale");
    assert_eq!(stale_writer.get(&key("k")).unwrap(), Some(value("v0")));

    let mut winner = store.begin(TxnMode::ReadWrite).expect("begin winner");
    winner.put(key("k"), value("v1")).expect("winner put");
    winner.commit_self().expect("winner commit");

    stale_writer
        .put(key("k"), value("stale"))
        .expect("stale put");
    let err = stale_writer
        .commit_self()
        .expect_err("stale writer must conflict");
    assert!(matches!(err, Error::TxnConflict));
    assert_eq!(conflict_code(&err), "ALOPEX-CORE-TXN-CONFLICT");
    assert!(
        is_retryable(&err),
        "TxnConflict should be treated as retryable"
    );
}

#[test]
fn write_write_conflict_is_deterministic_and_preserves_winner() {
    let store = Arc::new(MemoryKV::new());
    let (tx_ready, rx_ready) = mpsc::channel();
    let (tx_go, rx_go) = mpsc::channel();

    let s1 = Arc::clone(&store);
    let t1 = thread::spawn(move || {
        let mut txn = s1.begin(TxnMode::ReadWrite).expect("t1 begin");
        txn.put(key("ww"), value("from-t1")).expect("t1 put");
        tx_ready.send(()).expect("signal ready");
        rx_go.recv().expect("wait go");
        txn.commit_self()
    });

    let s2 = Arc::clone(&store);
    let t2 = thread::spawn(move || {
        rx_ready.recv().expect("wait t1 ready");
        let mut txn = s2.begin(TxnMode::ReadWrite).expect("t2 begin");
        txn.put(key("ww"), value("from-t2")).expect("t2 put");
        let result = txn.commit_self();
        tx_go.send(()).expect("allow t1 commit");
        result
    });

    let r2 = t2.join().expect("t2 join");
    let r1 = t1.join().expect("t1 join");

    assert!(r2.is_ok(), "first committer should succeed");
    let err = r1.expect_err("second committer should conflict");
    assert!(matches!(err, Error::TxnConflict));
    assert_eq!(conflict_code(&err), "ALOPEX-CORE-TXN-CONFLICT");
    assert!(is_retryable(&err));

    let mut ro = store.begin(TxnMode::ReadOnly).expect("final read begin");
    assert_eq!(
        ro.get(&key("ww")).expect("final read"),
        Some(value("from-t2")),
        "winner value should persist"
    );
}
