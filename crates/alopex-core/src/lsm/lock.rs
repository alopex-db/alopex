//! Single-process enforcement for a data directory (issue #181).
//!
//! Alopex's storage engine is built for exactly one writer process. Nothing in
//! the on-disk format arbitrates between two of them:
//!
//! * [`crate::lsm::wal::WalWriter`] pre-allocates a fixed-length ring with
//!   `set_len(wal_section_size)` and seeks to the physical offset derived from
//!   its **in-memory** `logical_offset` before every write. Two processes each
//!   keep their own `logical_offset`, so they seek to the same physical bytes
//!   and the last writer wins.
//! * SSTable ids come from a process-local `AtomicU64`, so a second process
//!   re-uses ids that are already on disk and overwrites live tables.
//! * `container::prune_sidecar` / `discard_dead_sidecar` `remove_dir_all` the
//!   sidecar working directory, so a second opener can delete the first one's
//!   data outright.
//!
//! Rather than paper over any single one of these, opening a data directory
//! takes an OS-level exclusive lock and refuses to proceed if somebody already
//! holds it. Making concurrent multi-process writes actually work is issue
//! #183 (v2.0), not this module.
//!
//! # Why an OS lock and not a PID file
//!
//! The lock is [`std::fs::File::try_lock`], which is `flock(LOCK_EX|LOCK_NB)`
//! on Unix and `LockFileEx(LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY)`
//! on Windows. Both are released by the kernel when the owning process exits,
//! however it exits — including `SIGKILL`, a panic, or a power-cut reboot. That
//! satisfies "an abnormal exit must not leave a lock behind" with no staleness
//! heuristics at all. A PID file would need `kill(pid, 0)`, which misfires on
//! PID reuse, inside PID namespaces, and across users.
//!
//! The lock file's *contents* (pid, host, executable) are purely diagnostic:
//! they make the error message useful and are never consulted to decide whether
//! the lock is held.
//!
//! # Invariant: the lock file lives OUTSIDE the sidecar
//!
//! For the `X.alopex.d` sidecar shape the lock file is `X.alopex.lock`, a
//! sibling of the container — **never** a file inside `X.alopex.d/`. Moving it
//! inside would break two things at once:
//!
//! * On Windows `container::prune_sidecar`'s `fs::remove_dir_all` would fail on
//!   our own open handle, so #178's "a converged database is a single
//!   `X.alopex` file" would stop holding.
//! * On Unix the prune would `unlink` the lock file while we still hold it. A
//!   `flock` follows the inode, not the name, so the next process would create
//!   a brand-new file at the same path and lock it successfully — two live
//!   writers, which is exactly what this module exists to prevent.
//!
//! Plain directories (the server, `Database::open("./mydb")`) are never pruned
//! by the core, so their lock lives inside at [`LOCK_FILE_NAME`].

use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::lsm::container::{self, ConvergePolicy};

/// The lock file name used for a plain (non-sidecar) data directory.
pub const LOCK_FILE_NAME: &str = ".alopex.lock";

/// The suffix appended to a container path to form its lock file.
///
/// `mydb.alopex` locks through `mydb.alopex.lock`.
const LOCK_FILE_SUFFIX: &str = ".lock";

/// A held data-directory lock.
///
/// The lock lives for as long as this value does. Dropping it closes the file
/// descriptor, which is what releases the OS lock — there is no explicit
/// unlock, and the lock file itself is intentionally left on disk (裁定 D8):
/// deleting it would let `A unlink -> B creates a new inode and locks it -> C
/// locks the same new inode` slip two writers through.
#[derive(Debug)]
pub(crate) struct DirectoryLock {
    /// The lock file path observed by unit tests.
    #[cfg(test)]
    path: Option<PathBuf>,
    /// The locked handle.
    ///
    /// Never read — holding it *is* the point. The OS lock lives on the open
    /// file description, so the lock is released precisely when this field is
    /// dropped and the descriptor closes. That is also why an abnormal exit
    /// cannot leave a lock behind: the kernel closes it for us.
    #[cfg(not(target_arch = "wasm32"))]
    _file: Option<std::fs::File>,
    #[cfg(target_arch = "wasm32")]
    _wasm: (),
}

impl DirectoryLock {
    /// A lock that holds nothing, for in-memory stores, WASM, and unit tests
    /// that construct an `LsmKV` by hand.
    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn disabled() -> Self {
        Self {
            #[cfg(test)]
            path: None,
            #[cfg(not(target_arch = "wasm32"))]
            _file: None,
            #[cfg(target_arch = "wasm32")]
            _wasm: (),
        }
    }

    /// The lock file backing this lock, if one is held.
    #[cfg(test)]
    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

/// Resolve the lock file that guards `data_dir`.
///
/// | data directory      | policy                    | lock file            |
/// |---------------------|---------------------------|----------------------|
/// | `/t/mydb.alopex.d`  | `SidecarOnly` / `Never`   | `/t/mydb.alopex.lock`|
/// | `/t/plaindir`       | `SidecarOnly` / `Never`   | `/t/plaindir/.alopex.lock` |
/// | `/t/x.d.tmp`        | `Always { /t/x.alopex }`  | `/t/x.alopex.lock`   |
///
/// `Never` resolves the sidecar shape too (裁定 D4). A process that opens
/// `mydb.alopex.d` with `Never` and one that opens it with `SidecarOnly` are
/// writing to the same bytes, so they must contend for the same lock file;
/// deriving the path from the policy alone would let them both in.
pub(crate) fn lock_path_for(data_dir: &Path, policy: &ConvergePolicy) -> PathBuf {
    let container = match policy {
        // Converging into an explicit container means that container is the
        // real database; lock the destination, not the staging directory.
        ConvergePolicy::Always { container } => Some(container.clone()),
        ConvergePolicy::SidecarOnly | ConvergePolicy::Never => {
            container::container_path_for(data_dir, &ConvergePolicy::SidecarOnly)
        }
    };
    match container {
        Some(container) => append_lock_suffix(&container),
        None => data_dir.join(LOCK_FILE_NAME),
    }
}

/// Whether `path` names a data-directory lock file.
///
/// Both shapes end in `.alopex.lock` — the plain-directory lock *is*
/// [`LOCK_FILE_NAME`] and the sidecar lock is `<name>.alopex.lock` — so one
/// suffix test covers them.
///
/// Backup, restore, and S3 sync all use this to skip the lock: it is
/// host-local diagnostics, and copying it back over a live directory would
/// delete (Unix) or fail on (Windows) the file a running process holds
/// (裁定 D15).
pub fn is_lock_file(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.as_encoded_bytes().ends_with(LOCK_FILE_NAME.as_bytes()))
}

/// `mydb.alopex` -> `mydb.alopex.lock`.
///
/// Appends to the file name rather than using `with_extension`, which would
/// *replace* `.alopex` and collide across databases (`a.alopex` and `a.sqlite`
/// would both want `a.lock`).
fn append_lock_suffix(container: &Path) -> PathBuf {
    let mut name = container.as_os_str().to_os_string();
    name.push(LOCK_FILE_SUFFIX);
    PathBuf::from(name)
}

/// Acquire the data-directory lock, or report who holds it.
///
/// Returns [`crate::error::Error::AlreadyOpen`] when another handle — in this
/// process or any other — already owns the lock. I/O failures (a read-only
/// parent directory, a filesystem that rejects the lock call) surface as
/// [`crate::error::Error::Io`] so they are not mistaken for contention.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn acquire(data_dir: &Path, lock_path: &Path) -> Result<DirectoryLock> {
    use std::fs::{OpenOptions, TryLockError};

    use crate::error::Error;

    if let Some(parent) = lock_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    // No `truncate(true)`: the loser opens this file too, and on Unix a
    // truncating open would wipe the winner's diagnostics before we ever get to
    // the lock call. Only the winner rewrites the contents, below.
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)?;

    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(Error::AlreadyOpen {
                path: data_dir.to_path_buf(),
                lock_path: lock_path.to_path_buf(),
                holder: read_holder(lock_path),
            });
        }
        Err(TryLockError::Error(err)) => return Err(Error::Io(err)),
    }

    // Diagnostics are best-effort: a database that opened fine must not fail
    // because we could not describe ourselves in a text file.
    let _ = write_holder(&file);

    Ok(DirectoryLock {
        #[cfg(test)]
        path: Some(lock_path.to_path_buf()),
        _file: Some(file),
    })
}

/// WASM has no multi-process model and no `flock`, so locking is a no-op there,
/// exactly as `restore_from_container` and `Drop for LsmKV` already are.
#[cfg(target_arch = "wasm32")]
pub(crate) fn acquire(_data_dir: &Path, _lock_path: &Path) -> Result<DirectoryLock> {
    Ok(DirectoryLock::disabled())
}

/// Overwrite the lock file with a description of this process.
#[cfg(not(target_arch = "wasm32"))]
fn write_holder(file: &std::fs::File) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let line = holder_line();
    file.set_len(0)?;
    let mut handle = file;
    handle.seek(SeekFrom::Start(0))?;
    handle.write_all(line.as_bytes())?;
    handle.flush()?;
    Ok(())
}

/// A single human-readable line describing the current process.
#[cfg(not(target_arch = "wasm32"))]
fn holder_line() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let pid = std::process::id();
    let exe = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let started_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    format!("pid={pid} host={host} exe={exe} started_ms={started_ms}\n")
}

/// Best-effort read of the holder description.
///
/// On Windows this usually fails: `std`'s `LockFileEx` locks the whole byte
/// range mandatorily, so the loser's `ReadFile` returns `ERROR_LOCK_VIOLATION`
/// (裁定 D10). Degrading to `unknown` keeps the actionable half of the message
/// — the path and the single-process rule — on every platform.
#[cfg(not(target_arch = "wasm32"))]
fn read_holder(lock_path: &Path) -> String {
    match std::fs::read_to_string(lock_path) {
        Ok(text) => {
            let line = text.trim();
            if line.is_empty() {
                "unknown".to_string()
            } else {
                line.to_string()
            }
        }
        Err(_) => "unknown".to_string(),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::error::Error;
    use tempfile::tempdir;

    #[test]
    fn lock_path_table_pins_the_outside_the_sidecar_invariant() {
        assert_eq!(
            lock_path_for(Path::new("/t/mydb.alopex.d"), &ConvergePolicy::SidecarOnly),
            PathBuf::from("/t/mydb.alopex.lock"),
            "sidecar shape locks beside the container, never inside the sidecar"
        );
        // 裁定 D4: `Never` must not get its own private lock file.
        assert_eq!(
            lock_path_for(Path::new("/t/mydb.alopex.d"), &ConvergePolicy::Never),
            PathBuf::from("/t/mydb.alopex.lock"),
        );
        assert_eq!(
            lock_path_for(Path::new("/t/plaindir"), &ConvergePolicy::SidecarOnly),
            PathBuf::from("/t/plaindir/.alopex.lock"),
        );
        assert_eq!(
            lock_path_for(Path::new("/t/plaindir"), &ConvergePolicy::Never),
            PathBuf::from("/t/plaindir/.alopex.lock"),
        );
        assert_eq!(
            lock_path_for(
                Path::new("/t/x.alopex.d.tmp"),
                &ConvergePolicy::Always {
                    container: PathBuf::from("/t/x.alopex"),
                }
            ),
            PathBuf::from("/t/x.alopex.lock"),
        );
    }

    #[test]
    fn lock_files_are_recognized_for_exclusion() {
        assert!(is_lock_file(Path::new("/t/db/.alopex.lock")));
        assert!(is_lock_file(Path::new("/t/mydb.alopex.lock")));
        assert!(!is_lock_file(Path::new("/t/db/lsm.wal")));
        assert!(!is_lock_file(Path::new("/t/mydb.alopex")));
        assert!(!is_lock_file(Path::new("/t/db/sst/1.sst")));
    }

    #[cfg(unix)]
    #[test]
    fn lock_file_detection_does_not_require_a_utf8_database_name() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let mut name = vec![0xff];
        name.extend_from_slice(b".alopex.lock");
        assert!(is_lock_file(Path::new(&OsString::from_vec(name))));
    }

    #[test]
    fn lock_suffix_is_appended_not_substituted() {
        // `with_extension(".lock")` would turn both of these into `a.lock`.
        assert_eq!(
            append_lock_suffix(Path::new("/t/a.alopex")),
            PathBuf::from("/t/a.alopex.lock")
        );
        assert_eq!(
            append_lock_suffix(Path::new("/t/a.sqlite")),
            PathBuf::from("/t/a.sqlite.lock")
        );
    }

    #[test]
    fn second_acquire_reports_already_open() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let lock_path = data_dir.join(LOCK_FILE_NAME);

        let held = acquire(&data_dir, &lock_path).unwrap();
        assert_eq!(held.path(), Some(lock_path.as_path()));

        let err = acquire(&data_dir, &lock_path).unwrap_err();
        match &err {
            Error::AlreadyOpen {
                path,
                lock_path: reported,
                ..
            } => {
                assert_eq!(path, &data_dir);
                assert_eq!(reported, &lock_path);
            }
            other => panic!("expected AlreadyOpen, got {other:?}"),
        }
        assert!(err.to_string().contains("already open by another process"));
    }

    #[test]
    fn dropping_the_lock_releases_it() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let lock_path = data_dir.join(LOCK_FILE_NAME);

        let held = acquire(&data_dir, &lock_path).unwrap();
        drop(held);
        let again = acquire(&data_dir, &lock_path).unwrap();
        drop(again);
        assert!(
            lock_path.exists(),
            "the lock file is left behind on purpose (裁定 D8)"
        );
    }

    #[test]
    fn a_losing_open_does_not_truncate_the_holder_record() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let lock_path = data_dir.join(LOCK_FILE_NAME);

        let _held = acquire(&data_dir, &lock_path).unwrap();
        let _ = acquire(&data_dir, &lock_path).unwrap_err();

        let holder = read_holder(&lock_path);
        assert!(
            holder.contains(&format!("pid={}", std::process::id())),
            "the winner's diagnostics must survive the loser's open, got: {holder}"
        );
    }

    #[test]
    fn an_unlocked_leftover_lock_file_is_inert() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let lock_path = data_dir.join(LOCK_FILE_NAME);
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(&lock_path, "pid=999999 host=gone exe=/nope started_ms=0\n").unwrap();

        // A crash leaves the file but not the lock, so this must succeed.
        drop(acquire(&data_dir, &lock_path).unwrap());
    }
}
