//! 受け入れ基準を網羅するコア側の統合テスト。

use alopex_core::{
    Error, KVStore, KVTransaction, LargeValueKind, LargeValueMeta, LargeValueReader,
    LargeValueWriter, MemoryKV, TxnManager, TxnMode,
};
use std::sync::{mpsc, Arc};
use std::thread;
use tempfile::tempdir;

fn key(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn value(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[test]
fn txn_consistency_covers_readonly_error_and_rollback() {
    // ReadOnly は書き込みできず、rollback した変更は永続化されないことを確認する。
    let store = MemoryKV::new();
    let manager = store.txn_manager();

    let mut seed = manager.begin(TxnMode::ReadWrite).unwrap();
    seed.put(key("k1"), value("v1")).unwrap();
    manager.commit(seed).unwrap();

    let mut ro = manager.begin(TxnMode::ReadOnly).unwrap();
    assert!(matches!(
        ro.put(key("k1"), value("forbidden")),
        Err(Error::TxnReadOnly)
    ));

    let mut rollback_txn = manager.begin(TxnMode::ReadWrite).unwrap();
    rollback_txn.put(key("k1"), value("temp")).unwrap();
    manager.rollback(rollback_txn).unwrap();

    let mut verify = manager.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(verify.get(&key("k1")).unwrap(), Some(value("v1")));
}

#[test]
fn flush_and_wal_recovery_overlay_sstable() {
    // flush 後の SST を基盤に WAL の後勝ち適用が再オープンで効くことを検証する。
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("acceptance.wal");

    {
        let store = MemoryKV::open(&wal_path).unwrap();
        let manager = store.txn_manager();

        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("persist"), value("v1")).unwrap();
        manager.commit(txn).unwrap();
        store.flush().unwrap();

        let mut overlay = manager.begin(TxnMode::ReadWrite).unwrap();
        overlay.put(key("persist"), value("v2")).unwrap();
        overlay.put(key("post-flush"), value("v3")).unwrap();
        manager.commit(overlay).unwrap();
    }

    let reopened = MemoryKV::open(&wal_path).unwrap();
    let manager = reopened.txn_manager();
    let mut ro = manager.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(ro.get(&key("persist")).unwrap(), Some(value("v2")));
    assert_eq!(ro.get(&key("post-flush")).unwrap(), Some(value("v3")));
}

#[test]
fn concurrent_writes_detect_conflict_and_keep_latest() {
    // 並行書き込みは OCC で競合し、最新コミットのみが反映される。
    let store = Arc::new(MemoryKV::new());
    let (tx_ready, rx_ready) = mpsc::channel();
    let (tx_commit, rx_commit) = mpsc::channel();

    let s1 = store.clone();
    let t1 = thread::spawn(move || {
        let manager = s1.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("k"), value("v1")).unwrap();
        tx_ready.send(()).unwrap();
        rx_commit.recv().unwrap();
        manager.commit(txn)
    });

    let s2 = store.clone();
    let t2 = thread::spawn(move || {
        rx_ready.recv().unwrap();
        let manager = s2.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("k"), value("v2")).unwrap();
        let res = manager.commit(txn);
        tx_commit.send(()).unwrap();
        res.unwrap();
    });

    let res1 = t1.join().unwrap();
    t2.join().unwrap();
    assert!(matches!(res1, Err(Error::TxnConflict)));

    let manager = store.txn_manager();
    let mut ro = manager.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(ro.get(&key("k")).unwrap(), Some(value("v2")));
}

#[test]
fn large_value_streams_blob_and_typed_in_chunks() {
    // Blob/Typed の両方でチャンク順序と O(chunk) ストリーミングを確認する。
    let dir = tempdir().unwrap();
    let blob_path = dir.path().join("blob.lv");
    let typed_path = dir.path().join("typed.lv");

    {
        let mut writer = LargeValueWriter::create(
            &blob_path,
            LargeValueMeta {
                kind: LargeValueKind::Blob,
                total_len: 6,
                chunk_size: 4,
            },
        )
        .unwrap();
        writer.write_chunk(b"abcd").unwrap();
        writer.write_chunk(b"ef").unwrap();
        writer.finish().unwrap();
    }

    {
        let mut reader = LargeValueReader::open(&blob_path).unwrap();
        let mut sizes = Vec::new();
        while let Some((info, chunk)) = reader.next_chunk().unwrap() {
            sizes.push(chunk.len());
            assert_eq!(info.index as usize, sizes.len() - 1);
        }
        assert_eq!(sizes, vec![4, 2]);
    }

    {
        let mut writer = LargeValueWriter::create(
            &typed_path,
            LargeValueMeta {
                kind: LargeValueKind::Typed(7),
                total_len: 5,
                chunk_size: 3,
            },
        )
        .unwrap();
        writer.write_chunk(b"hel").unwrap();
        writer.write_chunk(b"lo").unwrap();
        writer.finish().unwrap();
    }

    let mut reader = LargeValueReader::open(&typed_path).unwrap();
    assert!(matches!(reader.meta().kind, LargeValueKind::Typed(7)));
    let mut collected = Vec::new();
    while let Some((_info, chunk)) = reader.next_chunk().unwrap() {
        collected.extend_from_slice(&chunk);
    }
    assert_eq!(collected, b"hello");
}

#[test]
fn large_value_cancel_removes_partial_and_allows_restart() {
    // cancel で部分ファイルを消し、同一路径で再書き込みできることを確認する。
    let dir = tempdir().unwrap();
    let path = dir.path().join("cancel.lv");

    {
        let mut writer = LargeValueWriter::create(
            &path,
            LargeValueMeta {
                kind: LargeValueKind::Blob,
                total_len: 4,
                chunk_size: 4,
            },
        )
        .unwrap();
        writer.write_chunk(b"test").unwrap();
        writer.cancel().unwrap();
    }
    assert!(!path.exists());

    let mut writer = LargeValueWriter::create(
        &path,
        LargeValueMeta {
            kind: LargeValueKind::Blob,
            total_len: 2,
            chunk_size: 2,
        },
    )
    .unwrap();
    writer.write_chunk(b"ok").unwrap();
    writer.finish().unwrap();

    let mut reader = LargeValueReader::open(&path).unwrap();
    let first = reader.next_chunk().unwrap().unwrap();
    assert_eq!(first.1, b"ok");
    assert!(first.0.is_last);
    assert!(reader.next_chunk().unwrap().is_none());
}
