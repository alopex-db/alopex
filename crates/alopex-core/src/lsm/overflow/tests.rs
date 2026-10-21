use super::*;
use crate::storage::format::{FileHeader, HEADER_SIZE};
use proptest::prelude::*;

fn config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: wal::WAL_SEGMENT_HEADER_SIZE + 64,
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

fn limits() -> LsmOverflowConfig {
    LsmOverflowConfig::new(
        SpoolLimits::new(512, 4096, 7).unwrap(),
        OperationLimits::new(512, 8, 64).unwrap(),
        16384,
        7,
        4096,
    )
    .with_inline_bytes(1024)
    .with_activation_bytes(4096)
    .with_container_fence_bytes(4096)
}

fn write(store: &LsmKV, values: &[(u8, Option<Vec<u8>>)]) -> Result<()> {
    let mut tx = store.begin(TxnMode::ReadWrite)?;
    for (key, value) in values {
        match value {
            Some(value) => tx.put(vec![*key], value.clone())?,
            None => tx.delete(vec![*key])?,
        }
    }
    tx.commit_self()
}

fn value(store: &LsmKV, key: u8) -> Option<Value> {
    store
        .begin(TxnMode::ReadOnly)
        .unwrap()
        .get(&vec![key])
        .unwrap()
}

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == LOCK_FILE_NAME {
            continue;
        }
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn header(path: &Path) -> FileHeader {
    let bytes = fs::read(path).unwrap();
    FileHeader::from_bytes(bytes[..HEADER_SIZE].try_into().unwrap()).unwrap()
}

#[test]
fn batch_larger_than_ring_replays_values_empty_put_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    let values = [(0, Some(vec![9; 40])), (1, Some(Vec::new())), (2, None)];
    write(&store, &values).unwrap();
    assert_eq!(
        store.wal_checkpoint_stats().used_bytes,
        REFERENCE_BYTES as u64
    );
    let copy = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), copy.path());
    assert!(LsmKV::open_with_config(copy.path(), config()).is_err());
    let (reopened, recovery) = LsmKV::open_with_overflow(copy.path(), config(), limits()).unwrap();
    assert_eq!(recovery.entries_recovered, 1);
    for (key, expected) in values {
        assert_eq!(value(&reopened, key), expected);
    }
}

#[test]
fn container_fence_survives_converge_and_container_only_rehydrate() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("db.alopex.d");
    let container = dir.path().join("db.alopex");
    {
        let (store, _) = LsmKV::open_with_config(&data, config()).unwrap();
        write(&store, &[(0, Some(vec![1]))]).unwrap();
        store.converge().unwrap();
    }
    assert_eq!(header(&container).version, FileVersion::CURRENT);
    let legacy_bytes = fs::read(&container).unwrap();
    let (store, _) = LsmKV::open_with_overflow(&data, config(), limits()).unwrap();
    assert_eq!(&fs::read(&container).unwrap()[10..], &legacy_bytes[10..]);
    assert!(header(&container)
        .check_compatibility(&FileVersion::CURRENT)
        .is_err());
    assert_eq!(value(&store, 0), Some(vec![1]));
    write(&store, &[(1, Some(vec![2; 40]))]).unwrap();
    store.converge().unwrap();
    assert_eq!(header(&container).version, FileVersion::WAL_OVERFLOW);
    drop(store);
    // Test the single-file backup contract without the live sidecar.
    let copy = tempfile::tempdir().unwrap();
    fs::copy(&container, copy.path().join("db.alopex")).unwrap();
    let restored = copy.path().join("db.alopex.d");
    assert!(LsmKV::open_with_config(&restored, config()).is_err());
    assert!(!restored.join(container::SIDECAR_WAL_FILE).exists());
    let (store, _) = LsmKV::open_with_overflow(&restored, config(), limits()).unwrap();
    assert_eq!(
        disk::detect_version(&store.wal_path, &config().wal).unwrap(),
        3
    );
    assert_eq!(value(&store, 0), Some(vec![1]));
    assert_eq!(value(&store, 1), Some(vec![2; 40]));
    write(&store, &[(0, None)]).unwrap();
    assert_eq!(value(&store, 0), None);
}

#[test]
fn fence_failures_never_activate_wal_and_retry_preserves_data() {
    for stage in [1, 2, 3, 4] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("db.alopex.d");
        {
            let (store, _) = LsmKV::open_with_config(&data, config()).unwrap();
            write(&store, &[(0, Some(vec![5]))]).unwrap();
            store.converge().unwrap();
        }
        FAIL_STAGE.set(stage);
        let result = LsmKV::open_with_overflow(&data, config(), limits());
        FAIL_STAGE.set(0);
        assert!(result.is_err(), "stage {stage}");
        assert_eq!(
            detect_wal_format_version(&data.join(container::SIDECAR_WAL_FILE), &config().wal)
                .unwrap(),
            2
        );
        let (store, _) = LsmKV::open_with_overflow(&data, config(), limits()).unwrap();
        assert_eq!(value(&store, 0), Some(vec![5]));
    }
}

#[test]
fn activation_budget_failure_leaves_a_fenced_legacy_wal() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("db.alopex.d");
    let mut small = limits();
    small.activation_bytes = 1;
    assert!(LsmKV::open_with_overflow(&data, config(), small).is_err());
    assert_eq!(
        header(&dir.path().join("db.alopex")).version,
        FileVersion::WAL_OVERFLOW
    );
    assert_eq!(
        detect_wal_format_version(&data.join(container::SIDECAR_WAL_FILE), &config().wal).unwrap(),
        2
    );
    assert!(LsmKV::open_with_config(&data, config()).is_err());
    LsmKV::open_with_overflow(&data, config(), limits()).unwrap();
}

#[test]
fn rejected_payload_does_not_checkpoint_or_publish() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![1]))]).unwrap();
    let before = store.wal_checkpoint_stats();
    assert!(write(&store, &[(1, Some(vec![2; 65]))]).is_err());
    assert_eq!(store.wal_checkpoint_stats(), before);
    assert_eq!(value(&store, 0), Some(vec![1]));
    assert_eq!(value(&store, 1), None);
}

#[test]
fn private_replay_budget_failure_cannot_publish_or_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![1])), (1, Some(vec![2]))]).unwrap();
    let copy = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), copy.path());
    let wal = copy.path().join(container::SIDECAR_WAL_FILE);
    let before = fs::read(&wal).unwrap();
    let mut small = limits();
    small.replay_bytes = 130;
    let error = LsmKV::open_with_overflow(copy.path(), config(), small).unwrap_err();
    assert!(error.to_string().contains("replay budget"));
    assert_eq!(fs::read(wal).unwrap(), before);
    assert!(!copy
        .path()
        .join(container::SIDECAR_CHECKPOINT_FILE)
        .exists());
    let (reopened, _) = LsmKV::open_with_overflow(copy.path(), config(), limits()).unwrap();
    assert_eq!(value(&reopened, 0), Some(vec![1]));
    assert_eq!(value(&reopened, 1), Some(vec![2]));
}

#[test]
fn missing_referenced_spool_is_a_hard_open_error() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![1]))]).unwrap();
    let copy = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), copy.path());
    for entry in fs::read_dir(copy.path().join(SPOOL_DIRECTORY)).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    assert!(LsmKV::open_with_overflow(copy.path(), config(), limits()).is_err());
}

#[test]
fn uncertain_reference_blocks_checkpoint_and_drop_convergence() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("db.alopex.d");
    let (store, _) = LsmKV::open_with_overflow(&data, config(), limits()).unwrap();
    disk::FAIL_STAGE.set(5);
    let result = write(&store, &[(0, Some(vec![7; 40]))]);
    disk::FAIL_STAGE.set(0);
    assert!(result.is_err());
    assert_eq!(value(&store, 0), None);
    assert!(store.checkpoint().is_err());
    assert!(store.converge().is_err());
    drop(store);
    assert!(!data.join(container::SIDECAR_CHECKPOINT_FILE).exists());
    let (store, recovery) = LsmKV::open_with_overflow(&data, config(), limits()).unwrap();
    assert_eq!(recovery.entries_recovered, 1);
    assert_eq!(value(&store, 0), Some(vec![7; 40]));
}

#[test]
fn replay_budget_checkpoints_only_when_sum_would_exceed_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.wal.segment_size = wal::WAL_SEGMENT_HEADER_SIZE + 4 * REFERENCE_BYTES;
    let mut options = limits();
    options.replay_bytes = 260; // 2 * (key 1 + value 1 + charge 128)
    let (store, _) = LsmKV::open_with_overflow(dir.path(), cfg.clone(), options).unwrap();
    write(&store, &[(0, Some(vec![1]))]).unwrap();
    write(&store, &[(1, Some(vec![2]))]).unwrap();
    assert_eq!(store.wal_checkpoint_stats().checkpoint_count, 0);
    let copy = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), copy.path());
    let (reopened, _) = LsmKV::open_with_overflow(copy.path(), cfg, options).unwrap();
    write(&reopened, &[(2, Some(vec![3]))]).unwrap();
    assert_eq!(reopened.wal_checkpoint_stats().checkpoint_count, 1);
    for key in 0..3 {
        assert_eq!(value(&reopened, key), Some(vec![key + 1]));
    }
    // A single batch at the exact budget is valid; one byte over must fail.
    reopened.checkpoint().unwrap();
    write(&reopened, &[(3, Some(vec![4])), (4, Some(vec![5]))]).unwrap();
    let before = reopened.wal_checkpoint_stats();
    assert!(write(&reopened, &[(5, Some(vec![6])), (6, Some(vec![7, 8]))]).is_err());
    assert_eq!(reopened.wal_checkpoint_stats(), before);
}

#[test]
fn existing_container_copy_respects_exact_temporary_budget() {
    for extra in [0, 1] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("db.alopex.d");
        let path = dir.path().join("db.alopex");
        {
            let (store, _) = LsmKV::open_with_config(&data, config()).unwrap();
            store.converge().unwrap();
        }
        let before = fs::read(&path).unwrap();
        let mut options = limits();
        options.container_fence_bytes = before.len() as u64 - extra;
        let result = LsmKV::open_with_overflow(&data, config(), options);
        if extra == 0 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
            assert_eq!(fs::read(path).unwrap(), before);
        }
    }
}

#[test]
fn migration_preserves_inline_records_and_replays_them_with_spools() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.wal.segment_size = wal::WAL_SEGMENT_HEADER_SIZE + 128;
    let (legacy, _) = LsmKV::open_with_config(dir.path(), cfg.clone()).unwrap();
    write(&legacy, &[(0, Some(vec![1]))]).unwrap();
    let copy = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), copy.path());
    let (migrated, initial) =
        LsmKV::open_with_overflow(copy.path(), cfg.clone(), limits()).unwrap();
    assert_eq!(initial.entries_recovered, 1);
    assert_eq!(value(&migrated, 0), Some(vec![1]));
    write(&migrated, &[(1, Some(vec![2; 40]))]).unwrap();
    let crash = tempfile::tempdir().unwrap();
    copy_tree(copy.path(), crash.path());
    let (reopened, recovery) = LsmKV::open_with_overflow(crash.path(), cfg, limits()).unwrap();
    assert_eq!(recovery.entries_recovered, 2);
    assert_eq!(value(&reopened, 0), Some(vec![1]));
    assert_eq!(value(&reopened, 1), Some(vec![2; 40]));
}

#[test]
fn legacy_live_sidecar_wins_but_activation_refuses_an_invalid_container() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("db.alopex.d");
    let (store, _) = LsmKV::open_with_config(&data, config()).unwrap();
    write(&store, &[(0, Some(vec![4]))]).unwrap();
    let invalid = vec![b'x'; 128];
    fs::write(dir.path().join("db.alopex"), &invalid).unwrap();
    let copy = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), copy.path());
    let copy_data = copy.path().join("db.alopex.d");
    assert!(LsmKV::open_with_overflow(&copy_data, config(), limits()).is_err());
    assert_eq!(fs::read(copy.path().join("db.alopex")).unwrap(), invalid);
    let (reopened, _) = LsmKV::open_with_config(&copy_data, config()).unwrap();
    assert_eq!(value(&reopened, 0), Some(vec![4]));
}

#[test]
fn nested_new_directories_are_published_up_to_the_existing_parent() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("new").join("nested");
    let data = parent.join("db.alopex.d");
    assert_eq!(
        publication_directories(&data, None).unwrap(),
        vec![
            parent.clone(),
            dir.path().join("new"),
            dir.path().to_path_buf()
        ]
    );
    let (store, _) = LsmKV::open_with_overflow(&data, config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![1; 40]))]).unwrap();
    assert_eq!(value(&store, 0), Some(vec![1; 40]));
    assert_eq!(
        header(&parent.join("db.alopex")).version,
        FileVersion::WAL_OVERFLOW
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn rehydrate_preserves_raw_wal_version_before_any_lsm_activation(lsn in 1u64..5) {
        use crate::storage::format::{AlopexFileWriter, FileFlags};
        // Independent empty-container fixtures, with both reader requirements.
        for (version, expected_wal) in [(FileVersion::CURRENT, 2), (FileVersion::WAL_OVERFLOW, 3)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("fixture.alopex");
            let mut writer = AlopexFileWriter::new(path.clone(), version, FileFlags(0)).unwrap();
            writer.set_wal_sequence_number(lsn);
            writer.finalize().unwrap();
            let data = dir.path().join("restored");
            container::rehydrate(&path, &data, &config().wal).unwrap();
            let wal_path = data.join(container::SIDECAR_WAL_FILE);
            let bytes = fs::read(&wal_path).unwrap();
            let offset = wal::WAL_SECTION_HEADER_SIZE;
            prop_assert_eq!(u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]), expected_wal);
            prop_assert_eq!(disk::detect_version(&wal_path, &config().wal).unwrap(), expected_wal);
            prop_assert_eq!(load_checkpoint_meta(&data.join(container::SIDECAR_CHECKPOINT_FILE)).unwrap().unwrap().checkpoint_lsn, lsn);
            prop_assert_eq!(WalReader::open(&wal_path, config().wal).is_ok(), expected_wal == 2);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn small_history_survives_pressure_checkpoints(values in prop::collection::vec(any::<u8>(), 1..5)) {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
        for (key, value) in values.iter().enumerate() { write(&store, &[(key as u8, Some(vec![*value; 40]))]).unwrap(); }
        let copy = tempfile::tempdir().unwrap(); copy_tree(dir.path(), copy.path());
        let (reopened, _) = LsmKV::open_with_overflow(copy.path(), config(), limits()).unwrap();
        for (key, value) in values.iter().enumerate() { prop_assert_eq!(self::value(&reopened, key as u8), Some(vec![*value; 40])); }
    }
}

#[test]
fn checkpoint_gc_preserves_unknown_and_future_files_and_removes_abandoned_temps() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![1; 40]))]).unwrap();
    let spool = dir.path().join(SPOOL_DIRECTORY);
    let future = format!("{}-ffffffffffffffff.spool", "01".repeat(16));
    let temporary = format!("{}-ffffffffffffffff.tmp", "02".repeat(16));
    fs::write(spool.join(&future), b"future").unwrap();
    fs::write(spool.join(&temporary), b"temp").unwrap();
    fs::write(spool.join("foreign.tmp"), b"unknown").unwrap();
    store.checkpoint().unwrap();
    assert!(!spool.join(temporary).exists());
    assert!(spool.join(future).exists());
    let stats = store.overflow_stats().unwrap().unwrap();
    assert_eq!(stats.directory_bytes, 13);
    assert_eq!(stats.replay_bytes, 0);
    assert_eq!(stats.gc_failures, 0);
    drop(store);
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    assert_eq!(value(&store, 0), Some(vec![1; 40]));
}

#[test]
fn failed_checkpoint_does_not_collect_committed_spool() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![1; 40]))]).unwrap();
    let before = store.overflow_stats().unwrap().unwrap().directory_bytes;
    super::super::checkpoint::FAIL_CHECKPOINT_STAGE.set(1);
    let result = store.checkpoint();
    super::super::checkpoint::FAIL_CHECKPOINT_STAGE.set(0);
    assert!(result.is_err());
    assert_eq!(
        store.overflow_stats().unwrap().unwrap().directory_bytes,
        before
    );
    store.checkpoint().unwrap();
    assert_eq!(store.overflow_stats().unwrap().unwrap().directory_bytes, 0);
}

#[test]
fn gc_failure_is_observable_and_retained_bytes_are_not_discounted() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = limits();
    options.directory_bytes = 300;
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), options).unwrap();
    write(&store, &[(0, Some(vec![1; 40]))]).unwrap();
    let before = store.overflow_stats().unwrap().unwrap().directory_bytes;
    FAIL_STAGE.set(5);
    store.checkpoint().unwrap();
    assert!(write(&store, &[(1, Some(vec![2; 40]))]).is_err());
    FAIL_STAGE.set(0);
    let stats = store.overflow_stats().unwrap().unwrap();
    assert_eq!(stats.directory_bytes, before);
    assert_eq!(stats.gc_failures, 2);
    assert_eq!(value(&store, 0), Some(vec![1; 40]));
    assert_eq!(value(&store, 1), None);
    // Retry collects the old file before reserving another spool.
    write(&store, &[(1, Some(vec![2; 40]))]).unwrap();
    assert_eq!(
        store.overflow_stats().unwrap().unwrap().directory_bytes,
        before
    );
}

#[test]
fn converge_collects_spools_without_losing_container_only_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.alopex.d");
    let (store, _) = LsmKV::open_with_overflow(&path, config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![7; 40]))]).unwrap();
    store.converge().unwrap();
    assert_eq!(store.overflow_stats().unwrap().unwrap().directory_bytes, 0);
    let copy = tempfile::tempdir().unwrap();
    fs::copy(dir.path().join("db.alopex"), copy.path().join("db.alopex")).unwrap();
    let (restored, _) =
        LsmKV::open_with_overflow(copy.path().join("db.alopex.d"), config(), limits()).unwrap();
    assert_eq!(value(&restored, 0), Some(vec![7; 40]));
}

#[test]
fn oversized_put_is_rejected_before_retention_for_borrowed_and_owned_transactions() {
    use crate::kv::OwnedKVStore;
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    assert!(tx.put(vec![0], vec![1; 65]).is_err());
    tx.put(vec![0], vec![2]).unwrap();
    tx.commit_self().unwrap();
    let store = Arc::new(crate::kv::AnyKV::Lsm(Box::new(store)));
    let mut tx = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadWrite)
        .unwrap();
    assert!(tx.put(vec![0], vec![3; 65]).is_err());
    assert!(tx.delete(vec![0; 9]).is_err());
    assert_eq!(tx.get(&vec![0]).unwrap(), Some(vec![2]));
}

#[test]
fn single_spool_too_large_is_rejected_before_checkpointing_previous_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = limits();
    options.directory_bytes = 300;
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), options).unwrap();
    write(&store, &[(0, Some(vec![1; 40]))]).unwrap();
    let before = store.overflow_stats().unwrap().unwrap();
    let wal_before = store.wal_checkpoint_stats().used_bytes;
    assert!(write(&store, &[(1, Some(vec![2; 64]))]).is_err());
    assert_eq!(store.overflow_stats().unwrap().unwrap(), before);
    assert_eq!(store.wal_checkpoint_stats().used_bytes, wal_before);
    assert_eq!(value(&store, 0), Some(vec![1; 40]));
    assert_eq!(value(&store, 1), None);
}

#[test]
fn gc_directory_sync_failure_is_reported_without_undoing_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    write(&store, &[(0, Some(vec![4; 40]))]).unwrap();
    FAIL_STAGE.set(6);
    store.checkpoint().unwrap();
    FAIL_STAGE.set(0);
    assert_eq!(store.overflow_stats().unwrap().unwrap().gc_failures, 1);
    assert_eq!(store.overflow_stats().unwrap().unwrap().directory_bytes, 0);
    drop(store);
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    assert_eq!(value(&store, 0), Some(vec![4; 40]));
}

#[test]
fn single_operation_payload_limit_is_checked_at_put_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = limits();
    options.operations = OperationLimits::new(40, 8, 64).unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), options).unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    assert!(tx.put(vec![0], vec![1; 7]).is_err());
    assert_eq!(store.overflow_stats().unwrap().unwrap().directory_bytes, 0);
    tx.put(vec![0], vec![2; 6]).unwrap();
    tx.commit_self().unwrap();
    assert_eq!(value(&store, 0), Some(vec![2; 6]));
}
