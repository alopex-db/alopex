use super::*;
use proptest::prelude::*;

fn fixture(bytes: &[u8]) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let temp = dir.path().join("staging");
    fs::write(&source, bytes).unwrap();
    (dir, source, temp)
}

fn recover(source: &Path, temp: &Path, budget: u64) -> Result<()> {
    recover_prefix(source, temp, budget, |_, _| Ok(()))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn only_redundant_prefix_is_removed(prefix in 0usize..=3) {
        let (_dir, source, temp) = fixture(b"abc");
        fs::write(&temp, &b"abc"[..prefix]).unwrap();
        recover(&source, &temp, 3).unwrap();
        prop_assert!(!temp.exists());
        prop_assert_eq!(fs::read(source).unwrap(), b"abc");
    }
}

#[test]
fn partial_header_rewrite_is_redundant_without_promoting_it() {
    let (_dir, source, temp) = fixture(b"abc");
    fs::write(&temp, b"Abc").unwrap();
    recover_prefix(&source, &temp, 3, |_, bytes| {
        bytes[..2].copy_from_slice(b"AB");
        Ok(())
    })
    .unwrap();
    assert!(!temp.exists());
    assert_eq!(fs::read(source).unwrap(), b"abc");
}

#[test]
fn foreign_payload_is_not_truncated() {
    let (_dir, source, temp) = fixture(b"abc");
    fs::write(&temp, b"abd").unwrap();
    assert!(recover(&source, &temp, 3).is_err());
    assert_eq!(fs::read(temp).unwrap(), b"abd");
    assert_eq!(fs::read(source).unwrap(), b"abc");
}

#[test]
fn one_byte_short_budget_preserves_both_files() {
    let (_dir, source, temp) = fixture(b"abc");
    fs::write(&temp, b"a").unwrap();
    assert!(matches!(
        recover(&source, &temp, 2),
        Err(Error::WalCapacityExceeded { .. })
    ));
    assert_eq!(fs::read(temp).unwrap(), b"a");
    assert_eq!(fs::read(source).unwrap(), b"abc");
}

#[test]
fn staging_larger_than_source_is_preserved() {
    let (_dir, source, temp) = fixture(b"ab");
    fs::write(&temp, b"abc").unwrap();
    assert!(matches!(
        recover(&source, &temp, 3),
        Err(Error::InvalidFormat(_))
    ));
    assert_eq!(fs::read(temp).unwrap(), b"abc");
}

#[test]
fn staging_over_budget_returns_capacity_error() {
    let (_dir, source, temp) = fixture(b"ab");
    fs::write(&temp, b"abc").unwrap();
    assert!(matches!(
        recover(&source, &temp, 2),
        Err(Error::WalCapacityExceeded { .. })
    ));
    assert_eq!(fs::read(temp).unwrap(), b"abc");
}

#[test]
fn mismatch_after_buffer_boundary_preserves_entire_staging() {
    let bytes = [7u8; 1025]; // First byte in the second comparison buffer.
    let (_dir, source, temp) = fixture(&bytes);
    let mut changed = bytes;
    changed[1024] = 8;
    fs::write(&temp, changed).unwrap();
    assert!(recover(&source, &temp, 1025).is_err());
    assert_eq!(fs::read(temp).unwrap(), changed);
    assert_eq!(fs::read(source).unwrap(), bytes);
}

#[test]
fn hard_link_never_truncates_the_authoritative_source() {
    let (_dir, source, temp) = fixture(b"abc");
    fs::hard_link(&source, &temp).unwrap();
    assert!(recover(&source, &temp, 3).is_err());
    assert_eq!(fs::read(source).unwrap(), b"abc");
    assert_eq!(fs::read(temp).unwrap(), b"abc");
}

#[test]
fn transform_validation_failure_preserves_staging() {
    let (_dir, source, temp) = fixture(b"abc");
    fs::write(&temp, b"a").unwrap();
    assert!(
        recover_prefix(&source, &temp, 3, |_, _| Err(Error::InvalidFormat(
            "header".into()
        )))
        .is_err()
    );
    assert_eq!(fs::read(temp).unwrap(), b"a");
}

#[test]
fn absent_staging_does_not_require_source_or_create_files() {
    let dir = tempfile::tempdir().unwrap();
    recover(&dir.path().join("missing"), &dir.path().join("absent"), 0).unwrap();
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn directory_is_not_a_staging_file() {
    let (_dir, source, temp) = fixture(b"abc");
    fs::create_dir(&temp).unwrap();
    assert!(matches!(
        recover(&source, &temp, 3),
        Err(Error::InvalidFormat(_))
    ));
    assert!(temp.is_dir());
}

#[test]
fn directory_is_not_an_authoritative_file() {
    let (_dir, source, temp) = fixture(b"abc");
    fs::remove_file(&source).unwrap();
    fs::create_dir(&source).unwrap();
    fs::write(&temp, b"a").unwrap();
    assert!(matches!(
        recover(&source, &temp, 3),
        Err(Error::InvalidFormat(_))
    ));
    assert!(source.is_dir());
    assert_eq!(fs::read(temp).unwrap(), b"a");
}

#[cfg(unix)]
#[test]
fn metadata_failure_is_not_treated_as_absent_staging() {
    let (_dir, source, temp) = fixture(b"abc");
    std::os::unix::fs::symlink("staging", &temp).unwrap();
    assert!(recover(&source, &temp.join("file"), 3).is_err());
    assert_eq!(fs::read(source).unwrap(), b"abc");
}

#[cfg(unix)]
#[test]
fn symlink_is_preserved_without_following_it() {
    let (_dir, source, temp) = fixture(b"abc");
    std::os::unix::fs::symlink(&source, &temp).unwrap();
    assert!(matches!(
        recover(&source, &temp, 3),
        Err(Error::InvalidFormat(_))
    ));
    assert!(fs::symlink_metadata(&temp)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(fs::read(source).unwrap(), b"abc");
}

#[cfg(windows)]
#[test]
fn sharing_failure_preserves_staging_until_retry() {
    use std::os::windows::fs::OpenOptionsExt;
    let (_dir, source, temp) = fixture(b"abc");
    fs::write(&temp, b"ab").unwrap();
    let held = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&temp)
        .unwrap();
    assert!(recover(&source, &temp, 3).is_err());
    drop(held);
    assert_eq!(fs::read(&temp).unwrap(), b"ab");
    recover(&source, &temp, 3).unwrap();
    assert!(!temp.exists());
}
