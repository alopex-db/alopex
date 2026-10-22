//! Parent-first publication of empty directories; existing entries are retained.
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};

pub(super) fn create_dir_all_with(
    path: &Path,
    publish: &impl Fn(&Path, &Path) -> Result<()>,
) -> Result<()> {
    let mut missing = Vec::new();
    for directory in path.ancestors().filter(|p| !p.as_os_str().is_empty()) {
        match fs::metadata(directory) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "ancestor is not a directory",
                ))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => missing.push(directory),
            Err(error) => return Err(error),
        }
    }
    for directory in missing.into_iter().rev() {
        let temporary = staging_path(directory)?;
        // This process owns only a successfully created empty staging directory.
        // A stale staging name is never overwritten or removed on create failure.
        fs::create_dir(&temporary)?;
        if let Err(error) = publish(&temporary, directory) {
            // An interrupted/ambiguous move may already have published the parent.
            // Do not remove it or continue to children after an error.
            let _ = fs::remove_dir(&temporary);
            return Err(error);
        }
    }
    Ok(())
}

fn staging_path(path: &Path) -> Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "directory needs a name"))?;
    // Appending a suffix would reject otherwise valid near-limit components.
    // A hash collision only blocks creation: create_dir never takes ownership
    // of an existing staging directory, and no stale entry is reclaimed here.
    let mut hash = DefaultHasher::new();
    name.hash(&mut hash);
    Ok(path.with_file_name(format!(".alopex-directory-{:016x}.tmp", hash.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::cell::Cell;

    fn publish(source: &Path, target: &Path) -> Result<()> {
        #[cfg(windows)]
        return crate::storage::publication::windows::publish(source, target, false);
        // Portable protocol tests only: this is not a Windows durability proof.
        #[cfg(not(windows))]
        {
            if target.exists() {
                return Err(Error::from(ErrorKind::AlreadyExists));
            }
            fs::rename(source, target)
        }
    }

    #[test]
    fn nested_unicode_directories_preserve_existing_parent_contents() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("keep"), b"k").unwrap();
        let target = root.path().join("新規/child");
        create_dir_all_with(&target, &publish).unwrap();
        assert!(target.is_dir());
        assert_eq!(fs::read(root.path().join("keep")).unwrap(), b"k");
        assert_eq!(fs::read_dir(target.parent().unwrap()).unwrap().count(), 1);
        create_dir_all_with(&target, &|_, _| panic!("existing directory moved")).unwrap();
    }

    #[test]
    fn long_directory_component_is_not_extended_for_staging() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("n".repeat(240));
        create_dir_all_with(&target, &publish).unwrap();
        assert!(target.is_dir());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn file_ancestor_is_rejected_without_modification() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("file");
        fs::write(&file, b"k").unwrap();
        assert!(create_dir_all_with(&file.join("child"), &publish).is_err());
        assert_eq!(fs::read(file).unwrap(), b"k");
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn stale_staging_directory_is_not_owned_or_removed() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("new");
        let stale = staging_path(&target).unwrap();
        fs::create_dir(&stale).unwrap();
        fs::write(stale.join("keep"), b"k").unwrap();
        assert!(create_dir_all_with(&target, &publish).is_err());
        assert!(!target.exists());
        assert_eq!(fs::read(stale.join("keep")).unwrap(), b"k");
    }

    #[test]
    fn failure_before_parent_publication_cleans_owned_temp_and_can_retry() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("new/child");
        assert!(
            create_dir_all_with(&target, &|_, _| Err(ErrorKind::PermissionDenied.into())).is_err()
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
        create_dir_all_with(&target, &publish).unwrap();
        assert!(target.is_dir());
    }

    #[test]
    fn failure_after_parent_publication_stops_before_child_and_can_retry() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("new");
        let target = parent.join("child");
        let calls = Cell::new(0);
        assert!(create_dir_all_with(&target, &|source, target| {
            calls.set(calls.get() + 1);
            publish(source, target)?;
            Err(ErrorKind::Interrupted.into())
        })
        .is_err());
        assert_eq!(calls.get(), 1);
        assert!(parent.is_dir());
        assert!(!target.exists());
        assert_eq!(fs::read_dir(&parent).unwrap().count(), 0);
        create_dir_all_with(&target, &publish).unwrap();
        assert!(target.is_dir());
    }

    #[test]
    fn concurrent_destination_is_not_replaced_or_cleaned() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("new");
        assert!(create_dir_all_with(&target, &|source, target| {
            fs::create_dir(target)?;
            fs::write(target.join("keep"), b"k")?;
            publish(source, target)
        })
        .is_err());
        assert_eq!(fs::read(target.join("keep")).unwrap(), b"k");
        assert!(!staging_path(&target).unwrap().exists());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn existing_entry_type_and_contents_are_preserved(
            directory in any::<bool>(), bytes in proptest::collection::vec(any::<u8>(), 0..4),
        ) {
            let root = tempfile::tempdir().unwrap();
            let target = root.path().join("entry");
            let contents = if directory {
                fs::create_dir(&target).unwrap();
                target.join("keep")
            } else {
                target.clone()
            };
            fs::write(&contents, &bytes).unwrap();
            let result = create_dir_all_with(&target, &publish);
            prop_assert_eq!(result.is_ok(), directory);
            prop_assert_eq!(fs::read(contents).unwrap(), bytes);
            prop_assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
        }
    }

    #[cfg(unix)]
    #[test]
    fn metadata_error_is_preserved_without_treating_it_as_missing() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("loop");
        std::os::unix::fs::symlink("loop", &target).unwrap();
        let expected = fs::metadata(&target).unwrap_err();
        let actual = create_dir_all_with(&target, &publish).unwrap_err();
        assert_eq!(actual.kind(), expected.kind());
        assert_eq!(actual.raw_os_error(), expected.raw_os_error());
        assert_eq!(fs::read_link(&target).unwrap(), Path::new("loop"));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }
}
