//! Run the same tiny read-only v3 fixtures on Windows, macOS and Linux. Fixture
//! construction deliberately bypasses production activation; it proves reader
//! portability, not crash durability of a Windows writer.
use super::*;

fn fixture(root: &Path) -> (PathBuf, PathBuf, PathBuf, OverflowLimits) {
    let directory = root.join("復旧");
    fs::create_dir(&directory).unwrap();
    let path = root.join("wal");
    let limits = OverflowLimits::new(SpoolLimits::new(3, 128, 2).unwrap(), 128, 64).unwrap();
    let identity = SpoolIdentity::new([1; 16], 1).unwrap();
    let spool = spool_path(&directory, identity);
    let mut output = File::create(&spool).unwrap();
    let descriptor =
        write_spool(&mut &b"abc"[..], &mut output, identity, 3, 2, limits.spool).unwrap();
    output.sync_all().unwrap();
    drop(output);
    let mut writer = WalWriter::create_version(&path, config(), 1, 1, FORMAT_VERSION).unwrap();
    writer.append_encoded(&reference(descriptor), 1).unwrap();
    writer.force_sync().unwrap();
    drop(writer);
    (path, directory, spool, limits)
}

fn config() -> WalConfig {
    WalConfig {
        segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
        max_segments: 1,
        sync_mode: SyncMode::EveryWrite,
    }
}

#[test]
fn portable_v3_reader_replays_two_fragments_without_mutating_files() {
    let dir = tempfile::tempdir().unwrap();
    let (path, directory, spool, limits) = fixture(dir.path());
    let before_wal = fs::read(&path).unwrap();
    let before_spool = fs::read(&spool).unwrap();
    let mut reader = WalReader::open_overflow(&path, config()).unwrap();
    let mut payload = Vec::new();
    let mut fragments = 0;
    let mut ends = 0;
    let outcome = reader
        .recover_overflow(&directory, 0, limits, |event| {
            match event {
                RecoveryEvent::Fragment(bytes) => {
                    fragments += 1;
                    payload.extend_from_slice(bytes);
                }
                RecoveryEvent::End => ends += 1,
                RecoveryEvent::Begin(d) => assert_eq!(d.identity().lsn(), 1),
                RecoveryEvent::Inline(_) => panic!("expected reference"),
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(payload, b"abc");
    assert_eq!((fragments, ends, outcome.records), (2, 1, 1));
    assert_eq!(fs::read(path).unwrap(), before_wal);
    assert_eq!(fs::read(spool).unwrap(), before_spool);
}

#[test]
fn portable_v3_reader_rejects_missing_reference_without_repairing_wal() {
    let dir = tempfile::tempdir().unwrap();
    let (path, directory, spool, limits) = fixture(dir.path());
    fs::remove_file(spool).unwrap();
    let before = fs::read(&path).unwrap();
    let mut reader = WalReader::open_overflow(&path, config()).unwrap();
    assert!(reader
        .recover_overflow(&directory, 0, limits, |_| panic!(
            "invalid reference must not be delivered"
        ))
        .is_err());
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn portable_v3_reader_rejects_truncated_spool_without_delivering_a_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let (path, directory, spool, limits) = fixture(dir.path());
    OpenOptions::new()
        .write(true)
        .open(&spool)
        .unwrap()
        .set_len(70)
        .unwrap();
    let before = fs::read(&spool).unwrap();
    let mut reader = WalReader::open_overflow(&path, config()).unwrap();
    assert!(reader
        .recover_overflow(&directory, 0, limits, |_| panic!(
            "invalid spool must not be delivered"
        ))
        .is_err());
    assert_eq!(fs::read(spool).unwrap(), before);
}
