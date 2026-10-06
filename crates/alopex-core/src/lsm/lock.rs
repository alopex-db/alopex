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
/// The lock lives for as long as this value does in the acquiring process.
/// Dropping it there explicitly unlocks before closing the descriptor: a forked
/// child can still hold the same open file description until exec, even with
/// CLOEXEC. A child's Drop only closes its inherited descriptor. The lock file
/// itself is intentionally left on disk (裁定 D8):
/// deleting it would let `A unlink -> B creates a new inode and locks it -> C
/// locks the same new inode` slip two writers through.
#[derive(Debug)]
pub(crate) struct DirectoryLock {
    /// The lock file path observed by unit tests.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    path: Option<PathBuf>,
    /// The locked handle.
    ///
    /// The OS lock lives on the open file description. Explicit unlock on owner
    /// drop prevents a forked child's temporary descriptor from extending it.
    /// Closing the last descriptor also releases it after an abnormal exit.
    #[cfg(not(target_arch = "wasm32"))]
    _file: Option<std::fs::File>,
    /// Only the acquiring process may explicitly unlock the shared description.
    #[cfg(not(target_arch = "wasm32"))]
    owner_pid: u32,
    #[cfg(target_arch = "wasm32")]
    _wasm: (),
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for DirectoryLock {
    fn drop(&mut self) {
        // A forked child shares the parent's open file description. Closing
        // its descriptor is safe; unlocking it would release the parent's lock.
        if self.owner_pid != std::process::id() {
            return;
        }
        if let Some(file) = &self._file {
            // Best effort in Drop; closing the file remains the fallback.
            let _ = file.unlock();
        }
    }
}

impl DirectoryLock {
    /// A lock that holds nothing, for in-memory stores, WASM, and unit tests
    /// that construct an `LsmKV` by hand.
    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn disabled() -> Self {
        Self {
            #[cfg(all(test, not(target_arch = "wasm32")))]
            path: None,
            #[cfg(not(target_arch = "wasm32"))]
            _file: None,
            #[cfg(not(target_arch = "wasm32"))]
            owner_pid: std::process::id(),
            #[cfg(target_arch = "wasm32")]
            _wasm: (),
        }
    }

    /// The lock file backing this lock, if one is held.
    #[cfg(all(test, not(target_arch = "wasm32")))]
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
        #[cfg(all(test, not(target_arch = "wasm32")))]
        path: Some(lock_path.to_path_buf()),
        _file: Some(file),
        owner_pid: std::process::id(),
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
        #[cfg(unix)]
        {
            assert!(err.to_string().contains("already open in this process"));
            assert!(err.to_string().contains("Database or Transaction handles"));
        }
        // Windows may reject reads of the held lock's diagnostic record.
        #[cfg(not(unix))]
        assert!(err.to_string().contains("already open"));
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

    #[cfg(unix)]
    #[test]
    #[ignore = "spawned by the forked lock drop regression"]
    fn child_checks_lock_after_forked_drop() {
        let data_dir = PathBuf::from(std::env::var_os("ALOPEX_FORK_LOCK_DIR").unwrap());
        let result = acquire(&data_dir, &data_dir.join(LOCK_FILE_NAME));
        if std::env::var_os("ALOPEX_FORK_LOCK_EXPECT_HELD").is_some() {
            assert!(matches!(result, Err(Error::AlreadyOpen { .. })));
        } else {
            assert!(
                result.is_ok(),
                "owner drop must release the lock: {result:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn forked_child_drop_preserves_the_parent_lock() {
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let lock_path = data_dir.join(LOCK_FILE_NAME);
        let mut held = acquire(&data_dir, &lock_path).unwrap();
        // Remove test-only heap storage before fork so the child's Drop only
        // calls the OS process/descriptor operations used in production.
        held.path = None;
        // SAFETY: the child only drops the descriptor-only lock and calls
        // _exit. It never returns to the multithreaded test runtime or runs
        // unrelated destructors, and the parent reaps it before assertions.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());
        if pid == 0 {
            drop(held);
            unsafe { libc::_exit(0) };
        }
        let mut status = 0;
        let deadline = Instant::now() + Duration::from_secs(10);
        let waited = loop {
            // SAFETY: pid is our child and status is valid writable storage.
            let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if waited == pid {
                break Ok(());
            }
            if waited < 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EINTR) {
                    break Err(error);
                }
            }
            if Instant::now() >= deadline {
                break Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        if let Err(error) = waited {
            // ECHILD means the PID is no longer ours; do not signal a reused PID.
            if error.raw_os_error() != Some(libc::ECHILD) {
                // SAFETY: this unreaped child still belongs to this test.
                unsafe { libc::kill(pid, libc::SIGKILL) };
                loop {
                    // SAFETY: status is writable and pid names our child.
                    let reaped = unsafe { libc::waitpid(pid, &mut status, 0) };
                    if reaped == pid
                        || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR)
                    {
                        break;
                    }
                }
            }
            panic!("forked lock-drop child did not complete: {error}");
        }
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);

        let check = |expect_held: bool| {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command.args([
                "lsm::lock::tests::child_checks_lock_after_forked_drop",
                "--exact",
                "--ignored",
            ]);
            command.env("ALOPEX_FORK_LOCK_DIR", &data_dir);
            command.env_remove("ALOPEX_FORK_LOCK_EXPECT_HELD");
            if expect_held {
                command.env("ALOPEX_FORK_LOCK_EXPECT_HELD", "1");
            }
            command.stdin(Stdio::null());
            let mut child = command.spawn().unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => break Ok(status),
                    Ok(None) => {}
                    Err(error) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        break Err(error);
                    }
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        let while_held = check(true);
        drop(held);
        let after_drop = check(false);
        assert!(
            while_held
                .expect("held-lock check child must complete")
                .success(),
            "another process must reject acquire after the forked child drops"
        );
        assert!(
            after_drop
                .expect("released-lock check child must complete")
                .success(),
            "owner drop must permit acquire"
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "spawned by the pre-exec lock lifetime regression"]
    fn child_after_lock_owner_drop() {}

    #[cfg(unix)]
    #[test]
    fn dropping_the_lock_releases_it_while_a_child_waits_before_exec() {
        use std::io::{Read, Write};
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let lock_path = data_dir.join(LOCK_FILE_NAME);
        let held = acquire(&data_dir, &lock_path).unwrap();
        let lock_fd = held._file.as_ref().unwrap().as_raw_fd();
        // CLOEXEC closes the inherited descriptor at exec, not at fork.
        // SAFETY: held owns this valid descriptor throughout this call.
        let flags = unsafe { libc::fcntl(lock_fd, libc::F_GETFD) };
        assert!(flags >= 0 && flags & libc::FD_CLOEXEC != 0);

        let (mut ready, child_ready) = UnixStream::pair().unwrap();
        let (mut resume, child_resume) = UnixStream::pair().unwrap();
        ready
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let child = std::thread::spawn(move || {
            let ready_fd = child_ready.as_raw_fd();
            let resume_fd = child_resume.as_raw_fd();
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "lsm::lock::tests::child_after_lock_owner_drop",
                    "--exact",
                    "--ignored",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            // Force and synchronize the fork-to-exec boundary. This does not
            // assume which spawn implementation an unmodified Command selects.
            // SAFETY: the child only uses async-signal-safe syscalls on inherited
            // descriptors, stack storage and non-allocating OS error values.
            unsafe {
                command.pre_exec(move || {
                    if libc::fcntl(lock_fd, libc::F_GETFD) < 0 {
                        return Err(std::io::Error::from_raw_os_error(libc::EBADF));
                    }
                    let mut byte = 1u8;
                    if libc::write(ready_fd, (&byte as *const u8).cast(), 1) != 1 {
                        return Err(std::io::Error::from_raw_os_error(libc::EIO));
                    }
                    let mut poll = libc::pollfd {
                        fd: resume_fd,
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    // Bound the child lifetime even if the parent assertion fails.
                    if libc::poll(&mut poll, 1, 10_000) != 1 {
                        return Err(std::io::Error::from_raw_os_error(libc::ETIMEDOUT));
                    }
                    if libc::read(resume_fd, (&mut byte as *mut u8).cast(), 1) != 1 {
                        return Err(std::io::Error::from_raw_os_error(libc::EIO));
                    }
                    Ok(())
                });
            }
            let mut child = command.spawn()?;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => break Ok(status),
                    Ok(None) => {}
                    Err(error) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        break Err(error);
                    }
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let synchronized = ready.read_exact(&mut [0]);
        let while_held = acquire(&data_dir, &lock_path);
        drop(held);
        let reopened = acquire(&data_dir, &lock_path);
        // Release/reap the child before asserting so RED leaves no child behind.
        let resumed = resume.write_all(&[1]);
        let child_status = child.join().unwrap();
        synchronized.expect("child reached pre-exec with the lock descriptor");
        resumed.expect("release child");
        assert!(child_status.unwrap().success());
        assert!(matches!(while_held, Err(Error::AlreadyOpen { .. })));
        assert!(
            reopened.is_ok(),
            "parent lock drop must allow reopen before the child execs: {reopened:?}"
        );
        assert!(
            matches!(
                acquire(&data_dir, &lock_path),
                Err(Error::AlreadyOpen { .. })
            ),
            "the new owner's lock must remain held after the child exits"
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
