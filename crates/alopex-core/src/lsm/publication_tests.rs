//! Tiny native contracts for the files published before WAL reclamation.
use super::*;
use crate::storage::format::{
    AlopexFileReader, AlopexFileWriter, FileFlags, FileReader, FileSource, FileVersion, SectionType,
};

fn config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: false,
            ..Default::default()
        },
        converge: ConvergePolicy::Never,
        prune_sidecar_on_drop: false,
        ..Default::default()
    }
}

fn write_container(
    path: &Path,
    bytes: &[u8],
) -> std::result::Result<(), crate::storage::format::FormatError> {
    let mut writer = AlopexFileWriter::new(path.to_path_buf(), FileVersion::CURRENT, FileFlags(0))?;
    writer.add_section(SectionType::Metadata, bytes, false)?;
    writer.finalize()
}

#[test]
fn checkpoint_prepublication_failure_keeps_previous_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint.meta");
    let old = CheckpointMeta::new(1, 2);
    let new = CheckpointMeta::new(3, 4);
    save_checkpoint_meta(&path, &old).unwrap();
    checkpoint::FAIL_CHECKPOINT_STAGE.set(2);
    let failed = save_checkpoint_meta(&path, &new);
    checkpoint::FAIL_CHECKPOINT_STAGE.set(0);
    assert!(failed.is_err());
    assert_eq!(load_checkpoint_meta(&path).unwrap(), Some(old));
    save_checkpoint_meta(&path, &new).unwrap();
    assert_eq!(load_checkpoint_meta(&path).unwrap(), Some(new));
    assert!(!path.with_extension("tmp").exists());
}

#[test]
fn checkpoint_publishes_sst_before_reopen_without_wal_records() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config();
    let (store, _) = LsmKV::open_with_config(dir.path(), cfg.clone()).unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
    tx.commit_self().unwrap();
    store.checkpoint().unwrap();
    assert_eq!(fs::read_dir(dir.path().join("sst")).unwrap().count(), 1);
    drop(store);
    // Reopen must obtain the row from the published SST, not a WAL replay.
    let (store, recovery) = LsmKV::open_with_config(dir.path(), cfg).unwrap();
    assert_eq!(recovery.entries_recovered, 0);
    let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

#[test]
fn container_replacement_reopens_complete_new_contents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("状態.alopex");
    write_container(&path, b"old").unwrap();
    write_container(&path, b"new").unwrap();
    assert!(!path.with_extension("alopex.tmp").exists());
    let reader = AlopexFileReader::open(FileSource::Path(path)).unwrap();
    reader.validate_section(0).unwrap();
    assert_eq!(reader.read_section(0).unwrap(), b"new");
}

#[test]
fn container_rehydrate_publishes_sst_before_wal_marker() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.alopex.d");
    let cfg = LsmKVConfig {
        converge: ConvergePolicy::SidecarOnly,
        ..config()
    };
    let (store, _) = LsmKV::open_with_config(&source, cfg.clone()).unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
    tx.commit_self().unwrap();
    store.converge().unwrap();
    drop(store);
    let path = dir.path().join("source.alopex");
    let restored = dir.path().join("restored");
    let outcome = container::rehydrate(&path, &restored, &cfg.wal).unwrap();
    assert_eq!(outcome.tables_restored, 1);
    assert_eq!(fs::read_dir(restored.join("sst")).unwrap().count(), 1);
    let (store, recovery) = LsmKV::open_with_config(&restored, cfg).unwrap();
    assert_eq!(recovery.entries_recovered, 0);
    let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

#[cfg(windows)]
#[test]
fn windows_checkpoint_sharing_failure_preserves_old_metadata() {
    use std::os::windows::fs::OpenOptionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint.meta");
    let old = CheckpointMeta::new(1, 2);
    let new = CheckpointMeta::new(3, 4);
    save_checkpoint_meta(&path, &old).unwrap();
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path)
        .unwrap();
    assert!(save_checkpoint_meta(&path, &new).is_err());
    drop(held);
    assert_eq!(load_checkpoint_meta(&path).unwrap(), Some(old));
    save_checkpoint_meta(&path, &new).unwrap();
    assert_eq!(load_checkpoint_meta(&path).unwrap(), Some(new));
}

#[cfg(windows)]
#[test]
fn windows_container_sharing_failure_preserves_old_bytes() {
    use std::os::windows::fs::OpenOptionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.alopex");
    write_container(&path, b"old").unwrap();
    let old = fs::read(&path).unwrap();
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path)
        .unwrap();
    assert!(write_container(&path, b"new").is_err());
    drop(held);
    assert_eq!(fs::read(&path).unwrap(), old);
    write_container(&path, b"new").unwrap();
    let reader = AlopexFileReader::open(FileSource::Path(path)).unwrap();
    assert_eq!(reader.read_section(0).unwrap(), b"new");
}

#[cfg(windows)]
#[test]
fn windows_public_lsm_overflow_rejects_before_touching_storage() {
    use wal::{operations::OperationLimits, spool::SpoolLimits};
    let dir = tempfile::tempdir().unwrap();
    let options = overflow::LsmOverflowConfig::new(
        SpoolLimits::new(512, 4096, 7).unwrap(),
        OperationLimits::new(512, 8, 64).unwrap(),
        16384,
        7,
        4096,
    );
    let missing = dir.path().join("missing/child");
    assert!(
        matches!(LsmKV::open_with_overflow(&missing, config(), options),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported)
    );
    assert!(!dir.path().join("missing").exists());
    let existing = dir.path().join("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("lsm.wal"), b"old").unwrap();
    assert!(
        matches!(LsmKV::open_with_overflow(&existing, config(), options),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported)
    );
    assert_eq!(fs::read(existing.join("lsm.wal")).unwrap(), b"old");
    assert_eq!(fs::read_dir(existing).unwrap().count(), 1);
}
