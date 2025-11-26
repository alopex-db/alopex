use alopex_core::storage::large_value::{
    LargeValueKind, LargeValueMeta, LargeValueReader, LargeValueWriter,
};
use alopex_core::{KVStore, KVTransaction, MemoryKV, TxnManager, TxnMode};
use std::io::{Read, Seek, Write};
use tempfile::tempdir;

fn key(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn value(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[test]
fn sst_flush_reopen_roundtrip_and_checksum() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("kv.log");
    let sst_path = wal_path.with_extension("sst");

    {
        let store = MemoryKV::open(&wal_path).unwrap();
        let mgr = store.txn_manager();
        let mut txn = mgr.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("a"), value("1")).unwrap();
        txn.put(key("b"), value("2")).unwrap();
        mgr.commit(txn).unwrap();
        store.flush().unwrap();
    }

    // Corrupt SST checksum to ensure validation fails.
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sst_path)
            .unwrap();
        let len = file.metadata().unwrap().len();
        // Flip one byte in the footer checksum area (last byte).
        file.seek(std::io::SeekFrom::Start(len - 1)).unwrap();
        let mut b = [0u8; 1];
        file.read_exact(&mut b).unwrap();
        file.seek(std::io::SeekFrom::Current(-1)).unwrap();
        file.write_all(&[b[0] ^ 0xFF]).unwrap();
        file.sync_all().unwrap();
    }

    // Reopen should error due to checksum mismatch.
    let reopen_err = MemoryKV::open(&wal_path);
    assert!(reopen_err.is_err());
}

#[test]
fn sst_reopen_reads_values_and_overlays_wal() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("kv.log");
    {
        let store = MemoryKV::open(&wal_path).unwrap();
        let mgr = store.txn_manager();
        let mut txn = mgr.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("k1"), value("v1")).unwrap();
        mgr.commit(txn).unwrap();
        store.flush().unwrap();

        // Write a WAL-only update after flush.
        let mut txn2 = mgr.begin(TxnMode::ReadWrite).unwrap();
        txn2.put(key("k1"), value("v2")).unwrap();
        mgr.commit(txn2).unwrap();
    }

    let reopened = MemoryKV::open(&wal_path).unwrap();
    let mgr = reopened.txn_manager();
    let mut txn = mgr.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(txn.get(&key("k1")).unwrap(), Some(value("v2")));
}

#[test]
fn large_value_typed_streaming_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("typed.lv");
    let payload = b"abcde12345";

    {
        let meta = LargeValueMeta {
            kind: LargeValueKind::Typed(42),
            total_len: payload.len() as u64,
            chunk_size: 4,
        };
        let mut writer = LargeValueWriter::create(&path, meta).unwrap();
        writer.write_chunk(&payload[..4]).unwrap();
        writer.write_chunk(&payload[4..8]).unwrap();
        writer.write_chunk(&payload[8..]).unwrap();
        writer.finish().unwrap();
    }

    let mut reader = LargeValueReader::open(&path).unwrap();
    assert_eq!(reader.meta().kind, LargeValueKind::Typed(42));
    let mut buf = Vec::new();
    while let Some((info, chunk)) = reader.next_chunk().unwrap() {
        buf.extend_from_slice(&chunk);
        if info.is_last {
            assert_eq!(info.index, 2);
        }
    }
    assert_eq!(buf, payload);
}

#[test]
fn large_value_cancel_removes_partial_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("blob.lv");
    let meta = LargeValueMeta {
        kind: LargeValueKind::Blob,
        total_len: 3,
        chunk_size: 4,
    };

    {
        let writer = LargeValueWriter::create(&path, meta).unwrap();
        writer.cancel().unwrap();
    }
    assert!(!path.exists());
}
