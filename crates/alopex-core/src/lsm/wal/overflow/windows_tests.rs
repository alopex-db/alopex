use super::*;

fn config() -> WalConfig {
    WalConfig {
        segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
        max_segments: 1,
        sync_mode: SyncMode::NoSync,
    }
}

#[test]
fn windows_stale_create_temp_is_preserved_and_blocks_creation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let temporary = path.with_extension("overflow-create");
    fs::write(&temporary, b"owned-by-prior-attempt").unwrap();
    assert!(WalWriter::create_overflow(&path, config(), 1, 1).is_err());
    assert!(!path.exists());
    assert_eq!(fs::read(temporary).unwrap(), b"owned-by-prior-attempt");
}

#[test]
fn windows_publication_preserves_existing_files_and_supports_unicode_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("一時");
    let target = dir.path().join("確定");
    fs::write(&source, b"new").unwrap();
    fs::write(&target, b"old").unwrap();
    OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .sync_all()
        .unwrap();
    assert!(windows_publication::publish(&source, &target, false).is_err());
    assert_eq!(fs::read(&source).unwrap(), b"new");
    assert_eq!(fs::read(&target).unwrap(), b"old");
    windows_publication::publish(&source, &target, true).unwrap();
    assert!(!source.exists());
    assert_eq!(fs::read(&target).unwrap(), b"new");
}

#[test]
fn windows_invalid_publication_paths_leave_source_intact() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("temp");
    fs::write(&source, b"new").unwrap();
    for target in [
        dir.path().join("missing/wal"),
        dir.path().join("wal\0suffix"),
    ] {
        assert!(windows_publication::publish(&source, &target, false).is_err());
        assert_eq!(fs::read(&source).unwrap(), b"new");
    }
    assert!(!dir.path().join("wal").exists());
}

#[test]
fn windows_sharing_violation_preserves_old_file_until_handles_close() {
    use std::os::windows::fs::OpenOptionsExt;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("temp");
    let target = dir.path().join("wal");
    fs::write(&source, b"new").unwrap();
    fs::write(&target, b"old").unwrap();
    let held = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&target)
        .unwrap();
    assert!(windows_publication::publish(&source, &target, true).is_err());
    assert_eq!(fs::read(&source).unwrap(), b"new");
    drop(held);
    assert_eq!(fs::read(&target).unwrap(), b"old");
    OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .sync_all()
        .unwrap();
    windows_publication::publish(&source, &target, true).unwrap();
    assert_eq!(fs::read(target).unwrap(), b"new");
}

#[test]
fn windows_writer_round_trips_a_long_unicode_path() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir
        .path()
        .join("a".repeat(100))
        .join("b".repeat(100))
        .join("c".repeat(80));
    fs::create_dir_all(&parent).unwrap();
    let path = parent.join("復旧.wal");
    let spools = parent.join("spools");
    fs::create_dir(&spools).unwrap();
    let limits = OverflowLimits::new(SpoolLimits::new(3, 128, 2).unwrap(), 128, 64).unwrap();
    let mut writer = WalWriter::create_overflow(&path, config(), 1, 1).unwrap();
    writer
        .append_spool(
            &mut &b"abc"[..],
            &spools,
            SpoolIdentity::new([1; 16], 1).unwrap(),
            3,
            2,
            limits,
        )
        .unwrap();
    drop(writer);
    let mut reader = WalReader::open_overflow(&path, config()).unwrap();
    let mut payload = Vec::new();
    let result = reader
        .recover_overflow(&spools, 0, limits, |event| {
            if let RecoveryEvent::Fragment(bytes) = event {
                payload.extend_from_slice(bytes);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(result.records, 1);
    assert_eq!(payload, b"abc");
}
