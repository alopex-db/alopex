//! Issue #181 regression: one data directory, one process.
//!
//! The interesting cases here need a *real* second process, which the tests get
//! by re-executing this very test binary with `--exact <name> --ignored` plus an
//! environment variable naming the database.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use alopex_embedded::{Database, TxnMode};
use tempfile::tempdir;

/// Environment variable the re-executed child reads to find the database.
const CHILD_DIR_ENV: &str = "ALOPEX_LOCK_CHILD_DIR";
/// Stable substring every `AlreadyOpen` rendering must contain.
const LOCK_MESSAGE: &str = "already open by another process";

/// Re-run this test binary as a genuinely separate process.
fn respawn(test_name: &str, db_path: &Path) -> Command {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary path"));
    cmd.args([test_name, "--exact", "--ignored", "--nocapture"])
        .env(CHILD_DIR_ENV, db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

fn child_db_path() -> PathBuf {
    PathBuf::from(std::env::var(CHILD_DIR_ENV).expect("child needs ALOPEX_LOCK_CHILD_DIR"))
}

// ---------------------------------------------------------------------------
// Embedded vs embedded, same process
// ---------------------------------------------------------------------------

#[test]
fn a_second_open_in_the_same_process_is_rejected() {
    let dir = tempdir().unwrap();
    let container = dir.path().join("mydb.alopex");

    let first = Database::open(&container).expect("first open succeeds");
    let err = Database::open(&container)
        .err()
        .expect("second open must be rejected");
    let rendered = err.to_string();
    assert!(
        rendered.contains(LOCK_MESSAGE),
        "expected the stable lock message, got: {rendered}"
    );
    drop(first);
}

#[test]
fn a_plain_directory_is_guarded_too() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("plaindir");

    let first = Database::open(&data_dir).expect("first open succeeds");
    let err = Database::open(&data_dir)
        .err()
        .expect("second open must be rejected");
    assert!(err.to_string().contains(LOCK_MESSAGE));
    drop(first);
}

/// Guards against a lock leak: a cleanly released handle must hand the
/// directory back, or consecutive CLI invocations would start failing.
#[test]
fn close_then_drop_then_reopen_succeeds() {
    let dir = tempdir().unwrap();
    let container = dir.path().join("mydb.alopex");

    {
        let db = Database::open(&container).unwrap();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k".as_ref(), b"v".as_ref()).unwrap();
        txn.commit().unwrap();
        db.close().unwrap();
    }

    let reopened = Database::open(&container).expect("reopen after close+drop succeeds");
    let mut txn = reopened.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(txn.get(b"k".as_ref()).unwrap(), Some(b"v".to_vec()));
}

// ---------------------------------------------------------------------------
// In-memory databases are untouched
// ---------------------------------------------------------------------------

#[test]
fn in_memory_databases_never_lock() {
    let a = Database::open_in_memory().expect("first in-memory open");
    let b = Database::open_in_memory().expect("second in-memory open");
    let c = Database::new();
    let d = Database::new();
    drop((a, b, c, d));
}

// ---------------------------------------------------------------------------
// Embedded vs embedded, separate processes
// ---------------------------------------------------------------------------

#[test]
fn a_second_process_cannot_open_the_same_database() {
    let dir = tempdir().unwrap();
    let container = dir.path().join("mydb.alopex");

    let parent = Database::open(&container).expect("parent open succeeds");

    let output = respawn("child_open_must_fail", &container)
        .spawn()
        .expect("spawn child")
        .wait_with_output()
        .expect("child runs");

    assert!(
        output.status.success(),
        "child assertions failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    drop(parent);
}

/// Child half of [`a_second_process_cannot_open_the_same_database`].
#[test]
#[ignore = "spawned as a child process by a_second_process_cannot_open_the_same_database"]
fn child_open_must_fail() {
    let path = child_db_path();
    let err = Database::open(&path)
        .err()
        .expect("the parent holds the lock");
    let rendered = err.to_string();
    assert!(
        rendered.contains(LOCK_MESSAGE),
        "expected the stable lock message, got: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// An abnormal exit must not leave the lock behind (issue #181 checklist item 3)
// ---------------------------------------------------------------------------

#[test]
fn a_killed_process_does_not_leave_the_lock_held() {
    use std::io::{BufRead, BufReader};

    let dir = tempdir().unwrap();
    let container = dir.path().join("mydb.alopex");

    let mut child = respawn("child_open_and_wait", &container)
        .spawn()
        .expect("spawn child");

    // Wait until the child really holds the lock and has committed a row.
    let stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    let ready = loop {
        match lines.next() {
            Some(Ok(line)) if line.trim() == "OPEN" => break line,
            Some(Ok(_)) => continue, // libtest banner lines
            Some(Err(err)) => panic!("child stdout unreadable: {err}"),
            None => panic!("child exited before signalling OPEN"),
        }
    };
    assert_eq!(ready.trim(), "OPEN");

    // While the child lives, we must be locked out.
    let err = Database::open(&container)
        .err()
        .expect("the child holds the lock");
    assert!(
        err.to_string().contains(LOCK_MESSAGE),
        "expected the stable lock message, got: {err}"
    );

    // SIGKILL on Unix, TerminateProcess on Windows: no destructor runs, so
    // nothing in Alopex gets a chance to clean up. The kernel closing the file
    // descriptor is the only thing that releases the lock.
    child.kill().expect("kill the child");
    child.wait().expect("reap the child");

    let db = Database::open(&container).expect("a killed holder releases the lock");
    let mut txn = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        txn.get(b"lock-probe".as_ref()).unwrap(),
        Some(b"committed".to_vec()),
        "the child's committed write must survive its kill"
    );
}

/// Child half of [`a_killed_process_does_not_leave_the_lock_held`]: open, write,
/// announce, then park so the parent can kill us mid-flight.
#[test]
#[ignore = "spawned as a child process by a_killed_process_does_not_leave_the_lock_held"]
fn child_open_and_wait() {
    use std::io::{BufRead, Write};

    let path = child_db_path();
    let db = Database::open(&path).expect("child opens the database");
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.put(b"lock-probe".as_ref(), b"committed".as_ref())
        .unwrap();
    txn.commit().unwrap();
    db.flush().unwrap();

    println!("OPEN");
    std::io::stdout().flush().unwrap();

    // Deliberately never close or drop: the kill is what must free the lock.
    std::mem::forget(db);

    // Park on stdin, which the parent never writes to, then belt-and-braces
    // sleep so a closed pipe cannot turn this into a clean exit.
    let mut sink = String::new();
    let _ = std::io::stdin().lock().read_line(&mut sink);
    std::thread::sleep(std::time::Duration::from_secs(120));
}
