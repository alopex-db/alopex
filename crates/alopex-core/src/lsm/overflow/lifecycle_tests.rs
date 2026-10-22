//! Internal Windows pipeline qualification; public API guard stays enabled.
use super::*;

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

fn open(path: &Path) -> LsmKV {
    LsmKV::open_inner(path, config(), Some(limits())).unwrap().0
}

#[test]
fn internal_activation_checkpoint_and_reopen_preserve_one_record() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("new/parent/db.alopex.d");
    let store = open(&data);
    write(&store, &[(1, Some(vec![7; 40]))]).unwrap();
    assert!(store.overflow_stats().unwrap().unwrap().directory_bytes > 0);
    assert_eq!(
        store.wal_checkpoint_stats().used_bytes,
        REFERENCE_BYTES as u64
    );
    store.checkpoint().unwrap();
    assert_eq!(store.overflow_stats().unwrap().unwrap().directory_bytes, 0);
    drop(store);
    assert_eq!(value(&open(&data), 1), Some(vec![7; 40]));
}

#[test]
fn recovery_reclaims_retired_and_unreferenced_files_but_preserves_live_reference() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("source");
    let store = open(&data);
    write(&store, &[(1, Some(vec![7; 40]))]).unwrap();
    let bytes = store.overflow_stats().unwrap().unwrap().directory_bytes;
    // Copy the tiny on-disk crash image while the exclusive source owner is idle.
    // Dropping it first would checkpoint/converge and no longer exercise replay.
    let image = dir.path().join("image");
    fs::create_dir(&image).unwrap();
    for entry in fs::read_dir(&data).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == LOCK_FILE_NAME {
            continue;
        }
        let to = image.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            fs::create_dir(&to).unwrap();
            for child in fs::read_dir(entry.path()).unwrap() {
                let child = child.unwrap();
                fs::copy(child.path(), to.join(child.file_name())).unwrap();
            }
        } else {
            fs::copy(entry.path(), to).unwrap();
        }
    }
    let spool = image.join(SPOOL_DIRECTORY);
    let id = "02020202020202020202020202020202-0000000000000002";
    fs::write(spool.join(format!("{id}.spool")), b"a").unwrap();
    fs::write(spool.join(format!("{id}.tmp")), b"b").unwrap();
    fs::write(spool.join(format!(".gc-0000000000000003-{id}.spool")), b"").unwrap();
    fs::write(spool.join("foreign.tmp"), b"?").unwrap();
    let (reopened, recovery) = LsmKV::open_inner(&image, config(), Some(limits())).unwrap();
    assert_eq!(recovery.entries_recovered, 1);
    assert_eq!(value(&reopened, 1), Some(vec![7; 40]));
    assert_eq!(
        reopened.overflow_stats().unwrap().unwrap().directory_bytes,
        bytes + 1
    );
    assert_eq!(fs::read_dir(&spool).unwrap().count(), 2);
    assert_eq!(fs::read(spool.join("foreign.tmp")).unwrap(), b"?");
}

#[test]
fn failed_checkpoint_preserves_spool_until_successful_retry() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    write(&store, &[(1, Some(vec![7; 40]))]).unwrap();
    let before = store.overflow_stats().unwrap().unwrap().directory_bytes;
    assert!(before > 0);
    super::super::checkpoint::FAIL_CHECKPOINT_STAGE.set(1);
    let result = store.checkpoint();
    super::super::checkpoint::FAIL_CHECKPOINT_STAGE.set(0);
    assert!(result.is_err());
    assert_eq!(
        store.overflow_stats().unwrap().unwrap().directory_bytes,
        before
    );
    assert_eq!(value(&store, 1), Some(vec![7; 40]));
    store.checkpoint().unwrap();
    assert_eq!(store.overflow_stats().unwrap().unwrap().directory_bytes, 0);
}

#[test]
fn internal_reopen_reclaims_redundant_activation_copy() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    write(&store, &[(1, Some(vec![7; 40]))]).unwrap();
    store.checkpoint().unwrap();
    drop(store);
    let wal = dir.path().join(container::SIDECAR_WAL_FILE);
    let temporary = wal.with_extension("overflow-upgrade");
    fs::write(&temporary, &fs::read(&wal).unwrap()[..3]).unwrap();
    let reopened = open(dir.path());
    assert!(!temporary.exists());
    assert_eq!(value(&reopened, 1), Some(vec![7; 40]));
}

#[cfg(not(windows))]
#[test]
fn public_overflow_open_preserves_unknown_staging_then_recovers_redundant_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    write(&store, &[(1, Some(vec![7; 40]))]).unwrap();
    store.checkpoint().unwrap();
    drop(store);
    let wal = dir.path().join(container::SIDECAR_WAL_FILE);
    let temporary = wal.with_extension("overflow-upgrade");
    let original = fs::read(&wal).unwrap();
    fs::write(&temporary, b"unknown").unwrap();
    assert!(LsmKV::open_with_overflow(dir.path(), config(), limits()).is_err());
    assert_eq!(fs::read(&temporary).unwrap(), b"unknown");
    assert_eq!(fs::read(&wal).unwrap(), original);
    fs::write(&temporary, &original[..3]).unwrap();
    let (reopened, _) = LsmKV::open_with_overflow(dir.path(), config(), limits()).unwrap();
    assert_eq!(value(&reopened, 1), Some(vec![7; 40]));
    assert!(!temporary.exists());
}
