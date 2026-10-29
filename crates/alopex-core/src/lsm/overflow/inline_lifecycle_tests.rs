//! Small inline/reference integration contracts on both native platforms.
//!
//! Both native platforms use the public opt-in. These cases do not prove
//! device power-loss safety.
use super::*;

fn open(path: &Path) -> Result<(LsmKV, RecoveryResult)> {
    LsmKV::open_with_overflow(path, inline_config(), inline_options())
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

pub(super) fn copy_tree(from: &Path, to: &Path) {
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

pub(super) fn inline_options() -> LsmOverflowConfig {
    LsmOverflowConfig::new(
        SpoolLimits::new(16384, 32768, 256).unwrap(),
        OperationLimits::new(16384, 16, 4096).unwrap(),
        131072,
        256,
        32768,
    )
    .with_inline_bytes(1024)
    .with_activation_bytes(8192)
}

pub(super) fn inline_config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: wal::WAL_SEGMENT_HEADER_SIZE + 4096,
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

#[test]
fn small_inline_replays_empty_values_and_deletes_without_spools() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = open(dir.path()).unwrap();
    write(
        &store,
        &[(0, Some(vec![1; 16])), (1, Some(vec![])), (2, None)],
    )
    .unwrap();
    assert_eq!(store.overflow_stats().unwrap().unwrap().directory_bytes, 0);
    assert_eq!(
        store.overflow_stats().unwrap().unwrap().replay_bytes,
        3 * 129 + 16
    );
    // The source owner is idle and held open: Drop would checkpoint the WAL.
    // This disk-image replay check does not simulate process death or power loss.
    let crash = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), crash.path());
    assert!(LsmKV::open_with_config(crash.path(), inline_config()).is_err());
    let (reopened, recovery) = open(crash.path()).unwrap();
    assert_eq!(recovery.entries_recovered, 1);
    assert_eq!(value(&reopened, 0), Some(vec![1; 16]));
    assert_eq!(value(&reopened, 1), Some(vec![]));
    assert_eq!(value(&reopened, 2), None);
    assert_eq!(
        reopened.overflow_stats().unwrap().unwrap().replay_bytes,
        3 * 129 + 16
    );
}

#[test]
fn small_inline_mixes_with_large_spools_and_checkpoint_gc() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = open(dir.path()).unwrap();
    write(
        &store,
        &[(0, Some(vec![3; 2048])), (1, Some(vec![4; 2048]))],
    )
    .unwrap();
    let spool_bytes = store.overflow_stats().unwrap().unwrap().directory_bytes;
    assert!(spool_bytes > 4096);
    write(&store, &[(0, None), (2, Some(vec![]))]).unwrap();
    assert_eq!(
        store.overflow_stats().unwrap().unwrap().directory_bytes,
        spool_bytes
    );
    // The source owner is idle and held open: Drop would checkpoint the WAL.
    // This disk-image replay check does not simulate process death or power loss.
    let crash = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), crash.path());
    let (reopened, recovery) = open(crash.path()).unwrap();
    assert_eq!(recovery.entries_recovered, 2);
    assert_eq!(value(&reopened, 0), None);
    assert_eq!(value(&reopened, 1), Some(vec![4; 2048]));
    assert_eq!(value(&reopened, 2), Some(vec![]));
    reopened.checkpoint().unwrap();
    let stats = reopened.overflow_stats().unwrap().unwrap();
    assert_eq!(stats.directory_bytes, 0);
    assert_eq!(stats.replay_bytes, 0);
    drop(reopened);
    // Verify the checkpoint/SST publication, then a second clean reopen.
    for _ in 0..2 {
        let (reopened, _) = open(crash.path()).unwrap();
        assert_eq!(value(&reopened, 0), None);
        assert_eq!(value(&reopened, 1), Some(vec![4; 2048]));
        assert_eq!(value(&reopened, 2), Some(vec![]));
        assert_eq!(
            reopened.overflow_stats().unwrap().unwrap().directory_bytes,
            0
        );
    }
}
