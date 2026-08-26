//! Issue #181 regression for the CLI: `--data-dir` pointing at a directory that
//! somebody else already owns must fail loudly instead of silently corrupting it.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::Server;
use tempfile::tempdir;

/// Stable substring every `AlreadyOpen` rendering must contain.
const LOCK_MESSAGE: &str = "already open by another process";

fn cli_put(data_dir: &Path, key: &str, value: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_alopex"))
        .args([
            "--data-dir",
            &data_dir.display().to_string(),
            "--batch",
            "kv",
            "put",
            key,
            value,
        ])
        .stdin(Stdio::null())
        .output()
        .expect("run alopex")
}

/// Block until `child` has the database open, or fail with its output.
///
/// `lsm.wal` is created while opening the store, right after the lock is taken,
/// so its appearance means the directory is genuinely held.
fn wait_until_open(child: &mut Child, data_dir: &Path) {
    let wal = data_dir.join("lsm.wal");
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if wal.exists() {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll the first alopex") {
            panic!("the first alopex exited early with {status} before opening {data_dir:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the first alopex never opened {data_dir:?}");
}

/// CLI ↔ server: `alopex --data-dir <the server's data_dir>` is the exact
/// footgun issue #181 opens with.
#[test]
fn the_cli_refuses_a_data_dir_a_running_server_owns() {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        query_timeout: Duration::from_secs(5),
        audit_log_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server starts");

    let output = cli_put(temp.path(), "k", "v");
    assert!(
        !output.status.success(),
        "the CLI must not succeed against a data_dir the server holds"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains(LOCK_MESSAGE),
        "expected the stable lock message, got:\n{combined}"
    );

    // Once the server releases the directory, the very same command works.
    drop(server);
    let output = cli_put(temp.path(), "k", "v");
    assert!(
        output.status.success(),
        "the CLI must work once the server is gone:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// CLI ↔ CLI. The first process is parked on stdin so the overlap is
/// deterministic rather than a race against process startup.
#[test]
fn two_cli_processes_cannot_share_one_data_dir() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("clidb");

    let mut first = Command::new(env!("CARGO_BIN_EXE_alopex"))
        .args([
            "--data-dir",
            &data_dir.display().to_string(),
            "--batch",
            "--output",
            "json",
            "sql",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the first alopex");

    // `alopex ... sql` with no query argument opens the database first and only
    // then reads stdin to EOF, so the process parks with the directory held for
    // as long as we keep its stdin open. Waiting for the WAL to appear is the
    // readiness signal: it is created while opening the store, after the lock.
    wait_until_open(&mut first, &data_dir);

    let output = cli_put(&data_dir, "k", "v");
    assert!(
        !output.status.success(),
        "the second CLI must not share the first one's data_dir"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains(LOCK_MESSAGE),
        "expected the stable lock message, got:\n{combined}"
    );

    // Close the first process's stdin so it reaches EOF and exits, then the
    // directory must come back. `wait_with_output` also drains the pipes, which
    // a plain `wait` could deadlock on.
    drop(first.stdin.take());
    first.wait_with_output().expect("first CLI exits");

    let output = cli_put(&data_dir, "k", "v");
    assert!(
        output.status.success(),
        "the data_dir must be reusable after the first CLI exits:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
