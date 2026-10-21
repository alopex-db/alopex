use super::*;

fn unsupported<T>(result: Result<T>) {
    assert!(matches!(result, Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::Unsupported));
}
fn config() -> WalConfig {
    WalConfig {
        segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
        max_segments: 1,
        sync_mode: SyncMode::NoSync,
    }
}

#[test]
fn windows_activation_refuses_to_claim_directory_durability() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    unsupported(WalWriter::create_overflow(&path, config(), 1, 1));
    assert!(!path.exists());
    drop(WalWriter::create(&path, config(), 1, 1).unwrap());
    let before = fs::read(&path).unwrap();
    unsupported(activate(&path, config(), 1024));
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(!path.with_extension("overflow-upgrade").exists());
}

#[test]
fn windows_allows_v3_reading_but_refuses_writing_before_io() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let spools = dir.path().join("spools");
    fs::create_dir(&spools).unwrap();
    // Private test fixture: production v3 writer construction is refused above.
    let mut writer = WalWriter::create_version(&path, config(), 1, 1, FORMAT_VERSION).unwrap();
    let before = fs::read(&path).unwrap();
    let limits = OverflowLimits::new(SpoolLimits::new(3, 128, 2).unwrap(), 128, 64).unwrap();
    unsupported(writer.append_spool(
        &mut &b"abc"[..],
        &spools,
        SpoolIdentity::new([1; 16], 1).unwrap(),
        3,
        2,
        limits,
    ));
    assert_eq!(directory_usage(&spools).unwrap(), 0);
    assert_eq!(fs::read(&path).unwrap(), before);
    drop(writer);
    unsupported(WalWriter::open_overflow(&path, config()));
    let mut reader = WalReader::open_overflow(&path, config()).unwrap();
    assert_eq!(
        reader
            .recover_overflow(&spools, 0, limits, |_| panic!("empty WAL"))
            .unwrap()
            .records,
        0
    );
}
