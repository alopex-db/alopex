//! Small file-publication contracts; these do not enable Windows LSM overflow.
use super::*;
use crate::storage::format::{AlopexFileWriter, FileFlags, SectionType};

fn fixture() -> (tempfile::TempDir, PathBuf, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("状態.alopex");
    let mut writer =
        AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).unwrap();
    writer
        .add_section(SectionType::Metadata, b"old", false)
        .unwrap();
    writer.finalize().unwrap();
    let original = fs::read(&path).unwrap();
    (dir, path, original)
}

fn assert_fenced(path: &Path) {
    let reader = AlopexFileReader::open(FileSource::Path(path.to_path_buf())).unwrap();
    assert_eq!(reader.header().version, FileVersion::WAL_OVERFLOW);
    reader.validate_section(0).unwrap();
    assert_eq!(reader.read_section(0).unwrap(), b"old");
    assert!(!path.with_extension("overflow-fence").exists());
}

#[test]
fn fence_accepts_exact_copy_budget_and_preserves_payload() {
    let (dir, path, original) = fixture();
    fence_container(&path, dir.path(), 0, original.len() as u64).unwrap();
    assert_fenced(&path);
}

#[test]
fn fence_rejects_one_byte_short_budget_before_creating_temp() {
    let (dir, path, original) = fixture();
    assert!(matches!(
        fence_container(&path, dir.path(), 0, original.len() as u64 - 1),
        Err(Error::WalCapacityExceeded { .. })
    ));
    assert_eq!(fs::read(&path).unwrap(), original);
    assert!(!path.with_extension("overflow-fence").exists());
}

#[test]
fn failure_before_fence_publication_preserves_old_file_and_allows_retry() {
    let (dir, path, original) = fixture();
    FAIL_STAGE.set(1);
    let result = fence_container(&path, dir.path(), 0, original.len() as u64);
    FAIL_STAGE.set(0);
    assert!(result.is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    assert!(!path.with_extension("overflow-fence").exists());
    fence_container(&path, dir.path(), 0, original.len() as u64).unwrap();
    assert_fenced(&path);
}

#[test]
fn failure_after_fence_publication_retries_complete_new_file() {
    let (dir, path, original) = fixture();
    FAIL_STAGE.set(2);
    let result = fence_container(&path, dir.path(), 0, original.len() as u64);
    FAIL_STAGE.set(0);
    assert!(result.is_err());
    assert_fenced(&path);
    let fenced = fs::read(&path).unwrap();
    fence_container(&path, dir.path(), 0, original.len() as u64).unwrap();
    assert_eq!(fs::read(&path).unwrap(), fenced);
    assert_fenced(&path);
}

#[test]
fn stale_fence_temp_is_not_overwritten_or_removed() {
    let (dir, path, original) = fixture();
    let temporary = path.with_extension("overflow-fence");
    fs::write(&temporary, b"old").unwrap();
    assert!(fence_container(&path, dir.path(), 0, original.len() as u64).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    assert_eq!(fs::read(temporary).unwrap(), b"old");
}

#[cfg(windows)]
#[test]
fn windows_fenced_retry_requires_publication_and_keeps_bytes_on_sharing_failure() {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ;
    let (dir, path, original) = fixture();
    fence_container(&path, dir.path(), 0, original.len() as u64).unwrap();
    let fenced = fs::read(&path).unwrap();
    let held = OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ)
        .open(&path)
        .unwrap();
    assert!(fence_container(&path, dir.path(), 0, original.len() as u64).is_err());
    assert_eq!(fs::read(&path).unwrap(), fenced);
    assert!(!path.with_extension("overflow-fence").exists());
    drop(held);
    fence_container(&path, dir.path(), 0, original.len() as u64).unwrap();
    assert_fenced(&path);
}

#[cfg(windows)]
#[test]
fn windows_fenced_retry_respects_copy_budget() {
    let (dir, path, original) = fixture();
    fence_container(&path, dir.path(), 0, original.len() as u64).unwrap();
    let fenced = fs::read(&path).unwrap();
    assert!(matches!(
        fence_container(&path, dir.path(), 0, original.len() as u64 - 1),
        Err(Error::WalCapacityExceeded { .. })
    ));
    assert_eq!(fs::read(&path).unwrap(), fenced);
    assert!(!path.with_extension("overflow-fence").exists());
}
