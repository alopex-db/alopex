use super::*;

fn fixture() -> (tempfile::TempDir, PathBuf, WalConfig, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lsm.wal");
    let config = WalConfig {
        segment_size: 1024,
        max_segments: 2,
        sync_mode: SyncMode::EveryWrite,
    };
    drop(WalWriter::create(&path, config.clone(), 1, 1).unwrap());
    let original = fs::read(&path).unwrap();
    (dir, path, config, original)
}

#[test]
fn partial_activation_copy_is_reclaimed_without_changing_v2() {
    let (_dir, path, config, original) = fixture();
    let temp = path.with_extension("overflow-upgrade");
    fs::write(&temp, &original[..3]).unwrap();
    reconcile_activation_staging(&path, &config, original.len() as u64).unwrap();
    assert!(!temp.exists());
    assert_eq!(fs::read(path).unwrap(), original);
}

#[test]
fn mixed_header_versions_across_buffer_boundary_are_redundant() {
    let (_dir, path, config, original) = fixture();
    let mut changed = original.clone();
    let start = segment_header_offset(1024, 1) as usize;
    let mut header = WalSegmentHeader::from_bytes(
        original[start..start + WAL_SEGMENT_HEADER_SIZE]
            .try_into()
            .unwrap(),
    )
    .unwrap();
    header.version = FORMAT_VERSION;
    header.crc32 = header.compute_crc();
    changed[start..start + WAL_SEGMENT_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    let temp = path.with_extension("overflow-upgrade");
    fs::write(&temp, changed).unwrap();
    reconcile_activation_staging(&path, &config, original.len() as u64).unwrap();
    assert!(!temp.exists());
    assert_eq!(fs::read(path).unwrap(), original);
}

#[test]
fn old_copy_after_v3_publication_is_reclaimed() {
    let (_dir, path, config, original) = fixture();
    drop(activate(&path, config.clone(), original.len() as u64).unwrap());
    let new = fs::read(&path).unwrap();
    let temp = path.with_extension("overflow-upgrade");
    fs::write(&temp, &original).unwrap();
    reconcile_activation_staging(&path, &config, original.len() as u64).unwrap();
    assert!(!temp.exists());
    assert_eq!(fs::read(path).unwrap(), new);
}

#[test]
fn different_payload_staging_is_preserved() {
    let (_dir, path, config, original) = fixture();
    let mut changed = original.clone();
    *changed.last_mut().unwrap() ^= 1;
    let temp = path.with_extension("overflow-upgrade");
    fs::write(&temp, &changed).unwrap();
    assert!(reconcile_activation_staging(&path, &config, original.len() as u64).is_err());
    assert_eq!(fs::read(path).unwrap(), original);
    assert_eq!(fs::read(temp).unwrap(), changed);
}
