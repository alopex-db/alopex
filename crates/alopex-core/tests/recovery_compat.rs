#![cfg(not(target_arch = "wasm32"))]

use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::lsm::checkpoint::load_checkpoint_meta;
use alopex_core::lsm::wal::{
    detect_wal_format_version, WalConfig, WalEntry, WalSegmentHeader, WalWriter,
    WAL_FORMAT_VERSION, WAL_FORMAT_VERSION_V04, WAL_MAGIC, WAL_SECTION_HEADER_SIZE,
    WAL_SEGMENT_HEADER_SIZE,
};
use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_core::types::TxnMode;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use tempfile::tempdir;

fn compute_segment_crc(version: u16, segment_id: u64, first_lsn: u64) -> u32 {
    let mut buf = [0u8; WAL_SEGMENT_HEADER_SIZE - 6];
    buf[0..4].copy_from_slice(&WAL_MAGIC);
    buf[4..6].copy_from_slice(&version.to_le_bytes());
    buf[6..14].copy_from_slice(&segment_id.to_le_bytes());
    buf[14..22].copy_from_slice(&first_lsn.to_le_bytes());
    crc32fast::hash(&buf)
}

fn rewrite_segment_headers_as_legacy(path: &Path, config: &WalConfig) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open wal file");
    let segment_size = config.segment_size as u64;
    for segment_index in 0..(config.max_segments as u64) {
        let offset = (WAL_SECTION_HEADER_SIZE as u64) + (segment_index * segment_size);
        file.seek(SeekFrom::Start(offset))
            .expect("seek segment header");
        let mut bytes = [0u8; WAL_SEGMENT_HEADER_SIZE];
        file.read_exact(&mut bytes).expect("read segment header");
        let header = WalSegmentHeader::from_bytes(&bytes).expect("parse segment header");
        let crc = compute_segment_crc(WAL_FORMAT_VERSION_V04, header.segment_id, header.first_lsn);

        let mut legacy = [0u8; WAL_SEGMENT_HEADER_SIZE];
        legacy[0..4].copy_from_slice(&WAL_MAGIC);
        legacy[4..6].copy_from_slice(&WAL_FORMAT_VERSION_V04.to_le_bytes());
        legacy[6..14].copy_from_slice(&header.segment_id.to_le_bytes());
        legacy[14..22].copy_from_slice(&header.first_lsn.to_le_bytes());
        legacy[22..26].copy_from_slice(&crc.to_le_bytes());
        legacy[26..28].copy_from_slice(&0u16.to_le_bytes());

        file.seek(SeekFrom::Start(offset))
            .expect("seek segment header write");
        file.write_all(&legacy).expect("write legacy header");
    }
    file.sync_data().expect("sync legacy headers");
}

#[test]
fn migrates_legacy_wal_and_preserves_data() {
    let dir = tempdir().expect("tempdir");
    let wal_path = dir.path().join("lsm.wal");

    let mut config = LsmKVConfig::default();
    config.wal.segment_size = 4096;
    config.wal.max_segments = 2;

    let mut writer = WalWriter::create(&wal_path, config.wal.clone(), 1, 1).expect("create wal");
    let entry1 = WalEntry::put(1, b"key1".to_vec(), b"value1".to_vec());
    let entry2 = WalEntry::put(2, b"key2".to_vec(), b"value2".to_vec());
    writer.append(&entry1).expect("append entry1");
    writer.append(&entry2).expect("append entry2");
    writer.force_sync().expect("sync wal");
    drop(writer);

    rewrite_segment_headers_as_legacy(&wal_path, &config.wal);

    let (store, _recovery) = LsmKV::open_with_config(dir.path(), config).expect("open with config");

    let backup_path = wal_path.with_extension("wal.bak");
    assert!(backup_path.exists(), "legacy WAL backup missing");

    let detected = detect_wal_format_version(&wal_path, &store.config.wal).expect("detect wal");
    assert_eq!(detected, WAL_FORMAT_VERSION);

    let checkpoint_path = dir.path().join("checkpoint.meta");
    assert!(
        load_checkpoint_meta(&checkpoint_path)
            .expect("load checkpoint meta")
            .is_some(),
        "checkpoint meta missing"
    );

    let mut txn = store.begin(TxnMode::ReadOnly).expect("begin txn");
    assert_eq!(
        txn.get(&b"key1".to_vec()).expect("get key1"),
        Some(b"value1".to_vec())
    );
    assert_eq!(
        txn.get(&b"key2".to_vec()).expect("get key2"),
        Some(b"value2".to_vec())
    );
}
