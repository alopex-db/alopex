use super::*;
use proptest::prelude::*;

fn entry_len() -> usize {
    WalEntry::batch(
        1,
        vec![WalBatchOp {
            op_type: WalOpType::Put,
            key: vec![0],
            value: Some(vec![0]),
        }],
    )
    .encoded_len()
}

fn config(entries: usize) -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: wal::WAL_SEGMENT_HEADER_SIZE + entry_len() * entries,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: false,
            ..Default::default()
        },
        prune_sidecar_on_drop: false,
        ..Default::default()
    }
}

fn put(store: &LsmKV, key: u8, value: u8) -> Result<()> {
    let mut tx = store.begin(TxnMode::ReadWrite)?;
    tx.put(vec![key], vec![value])?;
    tx.commit_self()
}

fn get(store: &LsmKV, key: u8) -> Option<Value> {
    store
        .begin(TxnMode::ReadOnly)
        .unwrap()
        .get(&vec![key])
        .unwrap()
}

#[test]
fn pressure_reclaims_a_completely_full_ring_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(2);
    let (store, _) = LsmKV::open_with_config(dir.path(), cfg.clone()).unwrap();
    put(&store, 0, 10).unwrap();
    put(&store, 1, 11).unwrap();
    assert_eq!(
        store.wal_checkpoint_stats().used_bytes,
        (2 * entry_len()) as u64
    );
    put(&store, 2, 12).unwrap();
    let stats = store.wal_checkpoint_stats();
    assert_eq!(stats.checkpoint_count, 1);
    assert_eq!(stats.reclaimed_bytes, (2 * entry_len()) as u64);
    assert_eq!(stats.used_bytes, entry_len() as u64);
    // Reopen a crash-like copy, so Drop/converge cannot repair the fixture.
    let copy = tempfile::tempdir().unwrap();
    copy_store(&store, copy.path());
    let (reopened, recovery) = LsmKV::open_with_config(copy.path(), cfg).unwrap();
    assert_eq!(recovery.entries_recovered, 1);
    for key in 0..3 {
        assert_eq!(get(&reopened, key), Some(vec![key + 10]));
    }
}

fn copy_store(store: &LsmKV, path: &Path) {
    fs::create_dir_all(path.join("sst")).unwrap();
    for entry in fs::read_dir(&store.data_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() && entry.file_name() != LOCK_FILE_NAME {
            fs::copy(entry.path(), path.join(entry.file_name())).unwrap();
        }
    }
    for entry in fs::read_dir(&store.sst_dir).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), path.join("sst").join(entry.file_name())).unwrap();
    }
}

#[test]
fn sidecar_pressure_progresses_without_user_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tiny.alopex.d");
    let cfg = config(1);
    {
        let (store, _) = LsmKV::open_with_config(&path, cfg.clone()).unwrap();
        assert!(store.container_path.is_some());
        for key in 0..3 {
            put(&store, key, key + 1).unwrap();
        }
        assert_eq!(store.wal_checkpoint_stats().checkpoint_count, 2);
    }
    let (store, _) = LsmKV::open_with_config(&path, cfg).unwrap();
    for key in 0..3 {
        assert_eq!(get(&store, key), Some(vec![key + 1]));
    }
}

#[test]
fn oversized_entry_does_not_checkpoint_or_publish() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config(1)).unwrap();
    put(&store, 0, 1).unwrap();
    let before = store.wal_checkpoint_stats();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.put(vec![1], vec![2, 3]).unwrap();
    let err = tx.commit_self().unwrap_err();
    assert!(matches!(&err, Error::Io(e) if e.kind() == std::io::ErrorKind::StorageFull));
    assert!(err.to_string().contains("exceeds ring capacity"));
    assert_eq!(store.wal_checkpoint_stats(), before);
    assert_eq!(get(&store, 1), None);
}

#[test]
fn proactive_threshold_is_connected_to_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(3);
    cfg.checkpoint = CheckpointConfig {
        auto_checkpoint: true,
        wal_size_threshold: 1,
        min_interval_ms: 0,
    };
    let (store, _) = LsmKV::open_with_config(dir.path(), cfg).unwrap();
    put(&store, 0, 1).unwrap();
    assert_eq!(store.wal_checkpoint_stats().checkpoint_count, 0);
    put(&store, 1, 2).unwrap();
    assert_eq!(store.wal_checkpoint_stats().checkpoint_count, 1);
}

#[test]
fn checkpoint_failures_preserve_wal_and_previous_commits() {
    for stage in [1, 2] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(1);
        let (store, _) = LsmKV::open_with_config(dir.path(), cfg.clone()).unwrap();
        put(&store, 0, 1).unwrap();
        let before = store.wal_checkpoint_stats();
        checkpoint::FAIL_CHECKPOINT_STAGE.set(stage);
        let result = put(&store, 1, 2);
        checkpoint::FAIL_CHECKPOINT_STAGE.set(0);
        assert!(matches!(result, Err(Error::Io(_))));
        assert_eq!(store.wal_checkpoint_stats(), before);
        assert_eq!(get(&store, 0), Some(vec![1]));
        assert_eq!(get(&store, 1), None);
        let copy = tempfile::tempdir().unwrap();
        copy_store(&store, copy.path());
        let (reopened, _) = LsmKV::open_with_config(copy.path(), cfg).unwrap();
        assert_eq!(get(&reopened, 0), Some(vec![1]));
        assert_eq!(get(&reopened, 1), None);
        put(&store, 1, 2).unwrap();
    }
}

#[test]
fn failed_sstable_creation_does_not_reclaim_wal() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config(1)).unwrap();
    put(&store, 0, 1).unwrap();
    let before = store.wal_checkpoint_stats();
    fs::remove_dir(&store.sst_dir).unwrap();
    fs::write(&store.sst_dir, b"blocked").unwrap();
    assert!(matches!(put(&store, 1, 2), Err(Error::Io(_))));
    assert_eq!(store.wal_checkpoint_stats(), before);
    assert_eq!(get(&store, 0), Some(vec![1]));
    fs::remove_file(&store.sst_dir).unwrap();
    fs::create_dir(&store.sst_dir).unwrap();
    put(&store, 1, 2).unwrap();
}

#[test]
fn old_snapshot_survives_versions_and_tombstones_being_checkpointed() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config(3)).unwrap();
    put(&store, 0, 1).unwrap();
    let mut old = store.begin(TxnMode::ReadOnly).unwrap();
    put(&store, 0, 2).unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.delete(vec![0]).unwrap();
    tx.commit_self().unwrap();
    put(&store, 1, 3).unwrap();
    assert_eq!(store.wal_checkpoint_stats().checkpoint_count, 1);
    assert_eq!(old.get(&vec![0]).unwrap(), Some(vec![1]));
    assert_eq!(get(&store, 0), None);
}

#[test]
fn cursor_gate_releases_before_capacity_checkpoint_takes_locks() {
    use crate::kv::owned::OwnedKVStore;
    use crate::kv::AnyKV;

    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config(1)).unwrap();
    put(&store, 0, 1).unwrap();
    let store = Arc::new(AnyKV::Lsm(Box::new(store)));
    let mut reader = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadOnly)
        .unwrap();
    let mut cursor = reader.scan_prefix(b"").unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let writer = Arc::clone(&store);
    let thread = std::thread::spawn(move || {
        let AnyKV::Lsm(lsm) = writer.as_ref() else {
            panic!("expected LSM")
        };
        send.send(put(lsm, 1, 2)).unwrap();
    });
    assert!(recv
        .recv_timeout(std::time::Duration::from_millis(20))
        .is_err());
    assert_eq!(cursor.next_entry().unwrap(), Some((vec![0], vec![1])));
    cursor.close().unwrap();
    recv.recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .unwrap();
    thread.join().unwrap();
    let AnyKV::Lsm(lsm) = store.as_ref() else {
        panic!("expected LSM")
    };
    assert_eq!(lsm.wal_checkpoint_stats().checkpoint_count, 1);
}

#[test]
fn partial_reclaim_of_a_full_ring_preserves_the_remaining_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let first = WalEntry::put(1, vec![0], vec![1]);
    let second = WalEntry::put(2, vec![1], vec![2]);
    let len = first.encoded_len();
    let cfg = WalConfig {
        segment_size: wal::WAL_SEGMENT_HEADER_SIZE + 2 * len,
        max_segments: 1,
        sync_mode: SyncMode::EveryWrite,
    };
    let mut writer = WalWriter::create(&path, cfg.clone(), 1, 1).unwrap();
    writer.append(&first).unwrap();
    writer.append(&second).unwrap();
    writer.advance_start(len as u64).unwrap();
    assert_eq!(writer.used_bytes(), len as u64);
    writer.force_sync().unwrap();
    assert_eq!(
        WalReader::open(&path, cfg)
            .unwrap()
            .replay()
            .unwrap()
            .entries,
        vec![second]
    );
}

#[test]
fn empty_checkpoint_remains_empty_and_accepts_the_next_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config(1)).unwrap();
    assert_eq!(store.checkpoint().unwrap().wal_bytes_reclaimed, 0);
    assert_eq!(store.wal_checkpoint_stats().used_bytes, 0);
    put(&store, 0, 1).unwrap();
    assert_eq!(get(&store, 0), Some(vec![1]));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn small_commit_history_matches_reference(values in prop::collection::vec(any::<u8>(), 1..7)) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(2);
        let (store, _) = LsmKV::open_with_config(dir.path(), cfg.clone()).unwrap();
        for (key, value) in values.iter().enumerate() {
            put(&store, key as u8, *value).unwrap();
        }
        let copy = tempfile::tempdir().unwrap();
        copy_store(&store, copy.path());
        let (reopened, _) = LsmKV::open_with_config(copy.path(), cfg).unwrap();
        for (key, value) in values.iter().enumerate() {
            prop_assert_eq!(get(&reopened, key as u8), Some(vec![*value]));
        }
    }
}
