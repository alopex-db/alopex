//! Embedded API から受け入れ基準をカバーする統合テスト。

use alopex_core::{Error as CoreError, LargeValueKind, LargeValueReader, LargeValueWriter};
use alopex_embedded::{Database, Error, TxnMode};
use std::sync::{mpsc, Arc};
use std::thread;
use tempfile::tempdir;

#[test]
fn embedded_flush_and_reopen_overlays_wal() {
    // flush + WAL リカバリで最新値が残ることを確認する。
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("embedded.db");

    {
        let db = Database::open(&db_path).unwrap();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k1", b"v1").unwrap();
        txn.commit().unwrap();
        db.flush().unwrap();

        let mut overlay = db.begin(TxnMode::ReadWrite).unwrap();
        overlay.put(b"k1", b"v2").unwrap();
        overlay.put(b"k2", b"v3").unwrap();
        overlay.commit().unwrap();
    }

    let db = Database::open(&db_path).unwrap();
    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(ro.get(b"k1").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(ro.get(b"k2").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn embedded_readonly_and_rollback_behaviors() {
    // ReadOnly の書き込みエラーと rollback 後に変更が残らないことを確認する。
    let db = Database::new();

    let mut seed = db.begin(TxnMode::ReadWrite).unwrap();
    seed.put(b"k", b"v1").unwrap();
    seed.commit().unwrap();

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    assert!(matches!(
        ro.put(b"k", b"forbidden"),
        Err(Error::Core(CoreError::TxnReadOnly))
    ));

    let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
    tx.put(b"k", b"temp").unwrap();
    tx.rollback().unwrap();

    let mut check = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(check.get(b"k").unwrap(), Some(b"v1".to_vec()));
}

#[test]
fn embedded_concurrent_writes_surface_conflict() {
    // 並行 put は一方が TxnConflict となり、勝者のみが反映される。
    let db = Arc::new(Database::new());
    let (tx_ready, rx_ready) = mpsc::channel();
    let (tx_commit, rx_commit) = mpsc::channel();

    let d1 = db.clone();
    let t1 = thread::spawn(move || {
        let mut txn = d1.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k", b"v1").unwrap();
        tx_ready.send(()).unwrap();
        rx_commit.recv().unwrap();
        txn.commit()
    });

    let d2 = db.clone();
    let t2 = thread::spawn(move || {
        rx_ready.recv().unwrap();
        let mut txn = d2.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k", b"v2").unwrap();
        let res = txn.commit();
        tx_commit.send(()).unwrap();
        res.unwrap();
    });

    let res1 = t1.join().unwrap();
    t2.join().unwrap();
    assert!(matches!(res1, Err(Error::Core(CoreError::TxnConflict))));

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(ro.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn embedded_large_value_streaming_and_cancel() {
    // Blob/Typed のストリーミングと cancel 後の再書き込みを一通り確認する。
    let dir = tempdir().unwrap();
    let blob_path = dir.path().join("blob.lv");
    let typed_path = dir.path().join("typed.lv");
    let cancel_path = dir.path().join("cancel.lv");

    {
        let db = Database::new();
        let mut blob_writer = db.create_blob_writer(&blob_path, 6, Some(3)).unwrap();
        blob_writer.write_chunk(b"abc").unwrap();
        blob_writer.write_chunk(b"def").unwrap();
        blob_writer.finish().unwrap();

        let mut typed_writer = db.create_typed_writer(&typed_path, 42, 5, Some(4)).unwrap();
        typed_writer.write_chunk(b"hel").unwrap();
        typed_writer.write_chunk(b"lo").unwrap();
        typed_writer.finish().unwrap();

        let mut cancel_writer = db.create_blob_writer(&cancel_path, 4, Some(4)).unwrap();
        cancel_writer.write_chunk(b"dead").unwrap();
        cancel_writer.cancel().unwrap();
    }

    let db = Database::new();
    let mut blob_reader = db.open_large_value(&blob_path).unwrap();
    let mut blob_chunks = Vec::new();
    while let Some((info, chunk)) = blob_reader.next_chunk().unwrap() {
        blob_chunks.push((info.index, chunk));
    }
    assert_eq!(blob_chunks.len(), 2);

    let mut typed_reader = db.open_large_value(&typed_path).unwrap();
    assert!(matches!(
        typed_reader.meta().kind,
        LargeValueKind::Typed(42)
    ));
    let mut typed = Vec::new();
    while let Some((_info, chunk)) = typed_reader.next_chunk().unwrap() {
        typed.extend_from_slice(&chunk);
    }
    assert_eq!(typed, b"hello");

    assert!(!cancel_path.exists());
    let mut rewrite = LargeValueWriter::create(
        &cancel_path,
        alopex_core::LargeValueMeta {
            kind: LargeValueKind::Blob,
            total_len: 2,
            chunk_size: 2,
        },
    )
    .unwrap();
    rewrite.write_chunk(b"ok").unwrap();
    rewrite.finish().unwrap();

    let mut reread = LargeValueReader::open(&cancel_path).unwrap();
    let first = reread.next_chunk().unwrap().unwrap();
    assert_eq!(first.1, b"ok");
    assert!(first.0.is_last);
}
