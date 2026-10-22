use super::*;
use proptest::prelude::*;
use std::{fs, io};

const NAME: &str = "01010101010101010101010101010101-0000000000000001.spool";

fn publish(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(windows)]
    return crate::storage::publication::windows::publish(from, to, false);
    #[cfg(not(windows))]
    {
        // Portable state-machine adapter; not evidence of Windows persistence.
        if to.try_exists()? {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        fs::rename(from, to)
    }
}

fn usage(path: &Path) -> u64 {
    super::super::directory_usage(path).unwrap()
}

fn interrupted_at(stop: Phase) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    fs::write(&path, b"abc").unwrap();
    assert!(remove_with(&path, &publish, &|phase| {
        if phase == stop {
            Err(io::Error::other("injected interruption"))
        } else {
            Ok(())
        }
    })
    .is_err());
    let expected = if stop == Phase::Released { 0 } else { 3 };
    assert_eq!(usage(dir.path()), expected);
    let staged = fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert!(!path.exists());
    assert_eq!(
        fs::metadata(&staged).unwrap().len(),
        if stop == Phase::Retired { 3 } else { 0 }
    );
    // A fresh invocation has no in-memory knowledge of the previous attempt.
    remove_with(&staged, &publish, &|_| Ok(())).unwrap();
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_eq!(usage(dir.path()), 0);
}

#[test]
fn retirement_interruption_retains_charge_and_retries() {
    interrupted_at(Phase::Retired);
}
#[test]
fn truncate_interruption_retains_charge_and_retries() {
    interrupted_at(Phase::Truncated);
}
#[test]
fn flush_interruption_retains_charge_and_retries() {
    interrupted_at(Phase::Synced);
}
#[test]
fn release_interruption_leaves_only_empty_garbage() {
    interrupted_at(Phase::Released);
}

#[test]
fn first_publication_failure_preserves_original() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    fs::write(&path, b"abc").unwrap();
    assert!(remove_with(&path, &|_, _| Err(io::Error::other("publish")), &|_| Ok(())).is_err());
    assert_eq!(fs::read(path).unwrap(), b"abc");
    assert_eq!(usage(dir.path()), 3);
}

#[test]
fn zero_publication_failure_keeps_reservation_after_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    fs::write(&path, b"abc").unwrap();
    assert!(remove_with(
        &path,
        &|from, to| {
            if to
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(".gc-0000000000000000-")
            {
                Err(io::Error::other("zero publish"))
            } else {
                publish(from, to)
            }
        },
        &|_| Ok(())
    )
    .is_err());
    assert_eq!(usage(dir.path()), 3);
    let staged = dir.path().join(format!(".gc-0000000000000003-{NAME}"));
    assert_eq!(fs::metadata(&staged).unwrap().len(), 0);
    remove_with(&staged, &publish, &|_| Ok(())).unwrap();
    assert_eq!(usage(dir.path()), 0);
}

fn visible_move_then_error(zero: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    fs::write(&path, b"abc").unwrap();
    assert!(remove_with(
        &path,
        &|from, to| {
            publish(from, to)?;
            let is_zero = to
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(".gc-0000000000000000-");
            if is_zero == zero {
                Err(io::Error::other("visible move with error"))
            } else {
                Ok(())
            }
        },
        &|_| Ok(())
    )
    .is_err());
    assert!(!path.exists());
    assert_eq!(usage(dir.path()), if zero { 0 } else { 3 });
    let remaining = fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(
        fs::metadata(&remaining).unwrap().len(),
        if zero { 0 } else { 3 }
    );
    remove_with(&remaining, &publish, &|_| Ok(())).unwrap();
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn visible_retirement_error_leaves_charged_garbage_for_retry() {
    visible_move_then_error(false);
}

#[test]
fn visible_zero_publication_error_leaves_synced_empty_file_for_retry() {
    visible_move_then_error(true);
}

#[test]
fn existing_retirement_destination_is_not_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    let staged = dir.path().join(format!(".gc-0000000000000003-{NAME}"));
    fs::write(&path, b"abc").unwrap();
    fs::write(&staged, b"old").unwrap();
    assert!(remove_with(&path, &publish, &|_| Ok(())).is_err());
    assert_eq!(fs::read(path).unwrap(), b"abc");
    assert_eq!(fs::read(staged).unwrap(), b"old");
    assert_eq!(usage(dir.path()), 6);
}

#[test]
fn inconsistent_retirement_reservation_is_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!(".gc-0000000000000000-{NAME}"));
    fs::write(&path, b"abc").unwrap();
    assert!(remove_with(&path, &publish, &|_| Ok(())).is_err());
    assert_eq!(fs::read(path).unwrap(), b"abc");
    assert_eq!(usage(dir.path()), 3);
}

#[test]
fn prior_empty_retirement_does_not_block_reused_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    let old = dir.path().join(format!(".gc-0000000000000000-{NAME}"));
    fs::write(&old, b"").unwrap();
    fs::write(&path, b"abc").unwrap();
    remove_with(&path, &publish, &|_| Ok(())).unwrap();
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn occupied_zero_destination_is_preserved_with_current_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    let old = dir.path().join(format!(".gc-0000000000000000-{NAME}"));
    fs::write(&old, b"old").unwrap();
    fs::write(&path, b"abc").unwrap();
    assert!(remove_with(&path, &publish, &|_| Ok(())).is_err());
    assert_eq!(fs::read(&old).unwrap(), b"old");
    assert_eq!(usage(dir.path()), 6);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
}

#[test]
fn non_file_zero_destination_is_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    let old = dir.path().join(format!(".gc-0000000000000000-{NAME}"));
    fs::create_dir(&old).unwrap();
    fs::write(&path, b"abc").unwrap();
    assert!(remove_with(&path, &publish, &|_| Ok(())).is_err());
    assert!(old.is_dir());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
}

#[cfg(unix)]
#[test]
fn destination_metadata_errors_are_not_treated_as_absence() {
    let dir = tempfile::tempdir().unwrap();
    let link = dir.path().join("loop");
    std::os::unix::fs::symlink("loop", &link).unwrap();
    assert!(clear_empty_destination(&link.join(NAME)).is_err());
}

#[test]
fn unknown_names_and_non_files_are_preserved() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "other.tmp",
        ".gc-0000000000000003-other.tmp",
        ".gc-0000000000000003-00000000000000000000000000000000-0000000000000001.spool",
    ] {
        let path = dir.path().join(name);
        fs::write(&path, b"abc").unwrap();
        assert!(remove_with(&path, &publish, &|_| Ok(())).is_err());
        assert_eq!(fs::read(path).unwrap(), b"abc");
    }
    let path = dir.path().join(NAME);
    fs::create_dir(&path).unwrap();
    assert!(remove_with(&path, &publish, &|_| Ok(())).is_err());
    assert!(path.is_dir());
}

#[test]
fn reservation_parser_rejects_noncanonical_names() {
    for name in [
        format!(".gc-3-{NAME}"),
        format!(".gc-000000000000000G-{NAME}"),
        format!(".gc-000000000000000A-{NAME}"),
        format!(".gc-0000000000000003-{NAME}.other"),
        ".gc-0000000000000003-01010101010101010101010101010101-0000000000000000.spool".into(),
        format!(".gc-0000000000000003-.gc-0000000000000003-{NAME}"),
    ] {
        assert_eq!(retired(&name), None);
        assert_eq!(charge(Some(&name), 2), 2);
    }
    assert_eq!(charge(None, 2), 2);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn charge_is_maximum_of_actual_and_persistent_reservation(reserved in 0u64..8, actual in 0u64..8) {
        let name = format!(".gc-{reserved:016x}-{NAME}");
        prop_assert_eq!(retired(&name), Some((reserved, NAME)));
        prop_assert_eq!(charge(Some(&name), actual), if actual > reserved { actual } else { reserved });
    }
}

#[cfg(windows)]
#[test]
fn sharing_failure_keeps_retired_capacity_until_retry() {
    use std::os::windows::fs::OpenOptionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(NAME);
    fs::write(&path, b"abc").unwrap();
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path)
        .unwrap();
    assert!(remove(&path).is_err());
    assert_eq!(usage(dir.path()), 3);
    drop(held);
    remove(&path).unwrap();
    assert_eq!(usage(dir.path()), 0);
}
