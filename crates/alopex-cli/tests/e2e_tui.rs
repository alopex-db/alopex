// TUI E2E tests are skipped on Windows due to extremely slow PTY performance
// causing CI timeouts. See: https://github.com/alopex-db/alopex/issues/TBD
#![cfg(not(target_os = "windows"))]

use portable_pty::CommandBuilder;
use ratatui_testlib::{
    events::{KeyCode, Modifiers},
    Result, TuiTestHarness,
};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn wait_for_contains(harness: &mut TuiTestHarness, needle: &str, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    let mut last_screen = String::new();
    while start.elapsed() < timeout {
        std::thread::sleep(Duration::from_millis(150));
        harness.update_state()?;
        let contents = harness.screen_contents();
        last_screen = contents.clone();
        if contents.contains(needle) {
            return Ok(());
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out waiting for '{needle}' to appear. Last screen:\n{last_screen}"),
    )
    .into())
}

fn wait_for_value<T, F>(harness: &mut TuiTestHarness, timeout: Duration, mut f: F) -> Result<T>
where
    F: FnMut(&str) -> Option<T>,
{
    let start = Instant::now();
    let mut last_screen = String::new();
    while start.elapsed() < timeout {
        std::thread::sleep(Duration::from_millis(150));
        harness.update_state()?;
        let contents = harness.screen_contents();
        last_screen = contents.clone();
        if let Some(value) = f(&contents) {
            return Ok(value);
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out waiting for value. Last screen:\n{last_screen}"),
    )
    .into())
}

fn wait_for_absent(harness: &mut TuiTestHarness, needle: &str, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        std::thread::sleep(Duration::from_millis(150));
        harness.update_state()?;
        if !harness.screen_contents().contains(needle) {
            return Ok(());
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out waiting for '{needle}' to disappear."),
    )
    .into())
}

fn visible_prefix(label: &str, max: usize) -> String {
    label.chars().take(max).collect()
}

fn ensure_guided_fields(harness: &mut TuiTestHarness) -> Result<()> {
    if wait_for_contains(
        harness,
        "Input: guided fields (Tab to move, e to edit, o to list)",
        Duration::from_secs(2),
    )
    .is_ok()
    {
        return Ok(());
    }
    harness.send_key(KeyCode::Char('r'))?;
    wait_for_contains(
        harness,
        "Input: guided fields (Tab to move, e to edit, o to list)",
        Duration::from_secs(5),
    )
}

fn ensure_active_field(harness: &mut TuiTestHarness, label: &str, attempts: usize) -> Result<()> {
    let marker = format!("> {label}");
    let marker_required = format!("> {label} *");
    if harness.screen_contents().contains(&marker)
        || harness.screen_contents().contains(&marker_required)
    {
        return Ok(());
    }
    for _ in 0..attempts {
        harness.send_key(KeyCode::Tab)?;
        if wait_for_contains(harness, &marker, Duration::from_secs(1)).is_ok()
            || wait_for_contains(harness, &marker_required, Duration::from_secs(1)).is_ok()
        {
            return Ok(());
        }
    }
    for _ in 0..attempts {
        harness.send_key_with_modifiers(KeyCode::Tab, Modifiers::SHIFT)?;
        if wait_for_contains(harness, &marker, Duration::from_secs(1)).is_ok()
            || wait_for_contains(harness, &marker_required, Duration::from_secs(1)).is_ok()
        {
            return Ok(());
        }
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::other(format!(
        "Failed to focus field '{label}'. Screen snapshot:\n{snapshot}"
    ))
    .into())
}

fn ensure_focus_table(harness: &mut TuiTestHarness) -> Result<()> {
    if wait_for_contains(harness, "Focus: Table", Duration::from_secs(5)).is_ok() {
        return Ok(());
    }
    harness.send_key(KeyCode::Char('h'))?;
    if wait_for_contains(harness, "Focus: Table", Duration::from_secs(5)).is_ok() {
        return Ok(());
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::other(format!(
        "Failed to find Focus: Table. Screen snapshot:\n{snapshot}"
    ))
    .into())
}

fn ensure_focus_status(harness: &mut TuiTestHarness) -> Result<()> {
    if wait_for_contains(harness, "Focus: Status", Duration::from_secs(1)).is_ok() {
        return Ok(());
    }
    for _ in 0..3 {
        harness.send_key(KeyCode::Char('l'))?;
        if wait_for_contains(harness, "Focus: Status", Duration::from_secs(2)).is_ok() {
            return Ok(());
        }
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::other(format!(
        "Failed to find Focus: Status. Screen snapshot:\n{snapshot}"
    ))
    .into())
}

fn open_selection_overlay_for(
    harness: &mut TuiTestHarness,
    label: &str,
    title: &str,
    attempts: usize,
) -> Result<()> {
    ensure_guided_fields(harness)?;
    ensure_active_field(harness, label, attempts)?;
    harness.send_text("o")?;
    if wait_for_contains(harness, title, Duration::from_secs(2)).is_ok() {
        return Ok(());
    }
    for _ in 0..5 {
        harness.send_key(KeyCode::Char('k'))?;
    }
    if wait_for_contains(harness, title, Duration::from_secs(5)).is_ok() {
        return Ok(());
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::other(format!(
        "Failed to open {title} overlay. Screen snapshot:\n{snapshot}"
    ))
    .into())
}

fn alopex_bin() -> PathBuf {
    let exe = std::env::var("CARGO_BIN_EXE_alopex")
        .ok()
        .map(PathBuf::from);
    let exe = exe.or_else(|| {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").ok()?;
        let path = PathBuf::from(manifest_dir).join("../../target/debug/alopex");
        Some(path)
    });
    exe.unwrap_or_else(|| {
        panic!(
            "Failed to locate alopex binary; set CARGO_BIN_EXE_alopex or build target/debug/alopex"
        );
    })
}

#[test]
fn server_binary_path_honors_absolute_cargo_target_dir() {
    let manifest_dir = Path::new("/workspace/crates/alopex-cli");
    let target_dir = Path::new("/tmp/alopex-test-target");

    assert_eq!(
        default_server_binary_path(manifest_dir, Some(target_dir)),
        target_dir
            .join("debug")
            .join(format!("alopex-server{}", std::env::consts::EXE_SUFFIX))
    );
}

#[test]
fn server_binary_path_resolves_relative_target_from_workspace_root() {
    let manifest_dir = Path::new("/workspace/crates/alopex-cli");
    let workspace_root = manifest_dir.join("../..");

    assert_eq!(
        default_server_binary_path(manifest_dir, Some(Path::new("target-e2e"))),
        workspace_root
            .join("target-e2e")
            .join("debug")
            .join(format!("alopex-server{}", std::env::consts::EXE_SUFFIX))
    );
}

#[test]
fn server_binary_path_defaults_to_workspace_target_dir() {
    let manifest_dir = Path::new("/workspace/crates/alopex-cli");

    assert_eq!(
        default_server_binary_path(manifest_dir, None),
        manifest_dir
            .join("../..")
            .join("target")
            .join("debug")
            .join(format!("alopex-server{}", std::env::consts::EXE_SUFFIX))
    );
}

#[test]
fn derived_server_binary_is_rebuilt_to_avoid_stale_e2e_execution() {
    assert!(server_binary_requires_build(
        ServerBinaryOrigin::Derived,
        false
    ));
    assert!(!server_binary_requires_build(
        ServerBinaryOrigin::Derived,
        true
    ));
    assert!(!server_binary_requires_build(
        ServerBinaryOrigin::Explicit,
        false
    ));
    assert!(!server_binary_requires_build(
        ServerBinaryOrigin::CargoManaged,
        false
    ));
}

#[test]
fn operation_completion_requires_the_completed_state() {
    assert!(is_completion_state_label("completed"));
    assert!(is_completion_state_label(" Completed "));
    assert!(!is_completion_state_label("accepted"));
    assert!(!is_completion_state_label("queued"));
    assert!(!is_completion_state_label("running"));
}

#[test]
fn typed_enter_uses_terminal_carriage_return() {
    let event = ratatui_testlib::events::KeyEvent::new(KeyCode::Enter);

    assert_eq!(event.to_bytes(), b"\r");
}

fn default_server_binary_path(manifest_dir: &Path, cargo_target_dir: Option<&Path>) -> PathBuf {
    let workspace_root = manifest_dir.join("../..");
    let target_dir = match cargo_target_dir {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => workspace_root.join(path),
        None => workspace_root.join("target"),
    };
    target_dir
        .join("debug")
        .join(format!("alopex-server{}", std::env::consts::EXE_SUFFIX))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServerBinaryOrigin {
    Explicit,
    CargoManaged,
    Derived,
}

fn server_binary_requires_build(origin: ServerBinaryOrigin, skip_build: bool) -> bool {
    origin == ServerBinaryOrigin::Derived && !skip_build
}

fn alopex_server_bin() -> PathBuf {
    static SERVER_BIN: OnceLock<PathBuf> = OnceLock::new();
    let path = SERVER_BIN.get_or_init(|| {
        if let Some(explicit) = std::env::var_os("ALOPEX_TEST_SERVER_BIN") {
            let path = PathBuf::from(explicit);
            if path.exists() {
                return path;
            }
        }
        if let Some(cargo_managed) = std::env::var_os("CARGO_BIN_EXE_alopex-server") {
            let path = PathBuf::from(cargo_managed);
            if path.exists() {
                return path;
            }
        }
        let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                panic!(
                    "Failed to locate alopex-server binary; set CARGO_BIN_EXE_alopex-server, ALOPEX_TEST_SERVER_BIN, or CARGO_MANIFEST_DIR"
                )
            });
        let target_dir = std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from);
        let path = default_server_binary_path(&manifest_dir, target_dir.as_deref());
        let skip_build = std::env::var_os("ALOPEX_TEST_SKIP_BUILD").is_some();
        if server_binary_requires_build(ServerBinaryOrigin::Derived, skip_build) {
            build_server_binary(&path);
        } else if !path.exists() {
            panic!(
                "alopex-server binary not found at {}; build it or unset ALOPEX_TEST_SKIP_BUILD",
                path.display()
            );
        }
        path
    });
    path.clone()
}

fn build_server_binary(expected: &Path) {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().expect("current dir"));
    let workspace_root = manifest_dir.join("../..");
    let mut cmd = Command::new("cargo");
    cmd.arg("build").arg("-p").arg("alopex-server");
    cmd.current_dir(workspace_root);
    if let Some(target_dir) = std::env::var_os("CARGO_TARGET_DIR") {
        cmd.env("CARGO_TARGET_DIR", target_dir);
    }
    let status = cmd.status().expect("failed to spawn cargo build");
    if !status.success() {
        panic!("failed to build alopex-server binary");
    }
    if !expected.exists() {
        panic!(
            "alopex-server binary still missing at {}",
            expected.display()
        );
    }
}

fn alopex_command(args: &[&str]) -> CommandBuilder {
    let mut cmd = CommandBuilder::new(alopex_bin());
    cmd.env("ALOPEX_TEST_TTY", "1");
    cmd.env("ALOPEX_MODE", "tui");
    cmd.env("TERM", "xterm-256color");
    if let Ok(home) = std::env::var("ALOPEX_TEST_SERVER_HOME") {
        cmd.env("HOME", home);
    }
    for arg in args {
        cmd.arg(arg);
    }
    cmd
}

fn alopex_batch(args: &[&str]) -> std::io::Result<()> {
    let status = Command::new(alopex_bin()).args(args).status()?;
    if !status.success() {
        return Err(std::io::Error::other("alopex batch command failed"));
    }
    Ok(())
}

fn alopex_batch_with_profile(profile: &str, args: &[&str]) -> std::io::Result<()> {
    // Lifecycle admin actions (archive/export/backup/restore) leave the shared
    // test server in maintenance mode for a short moment after the executing
    // test has already released `server_lock()`. Writes issued by the next
    // test's seeding step are then rejected with HTTP 409. Retry only that
    // specific transient rejection instead of failing the test.
    fn is_transient_maintenance_rejection(stderr: &str) -> bool {
        // Match the exact server error (status, code, and message) so that
        // unrelated failures are not hidden by the retry loop, e.g.:
        // `Server error: HTTP 409 - {"error":{"code":"CONFLICT","message":
        // "conflict: writes are blocked in maintenance mode",...}}`
        stderr.contains("HTTP 409")
            && stderr.contains("\"code\":\"CONFLICT\"")
            && stderr.contains("conflict: writes are blocked in maintenance mode")
    }

    // Backup/restore completion is reported before the server has necessarily left
    // maintenance mode. Loaded CI hosts have exceeded the former 10-second
    // allowance, so keep retrying this exact transient response for up to one
    // minute while still failing every other error immediately.
    const MAINTENANCE_RETRY_ATTEMPTS: usize = 120;
    const MAINTENANCE_RETRY_DELAY: Duration = Duration::from_millis(500);

    let mut last_stderr = String::new();
    for _ in 0..MAINTENANCE_RETRY_ATTEMPTS {
        let mut cmd = Command::new(alopex_bin());
        cmd.env("ALOPEX_TEST_TTY", "1");
        cmd.env("ALOPEX_MODE", "tui");
        if let Ok(home) = std::env::var("ALOPEX_TEST_SERVER_HOME") {
            cmd.env("HOME", home);
        }
        let output = cmd
            .arg("--profile")
            .arg(profile)
            .arg("--batch")
            .args(args)
            .output()?;
        if output.status.success() {
            return Ok(());
        }
        last_stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if !is_transient_maintenance_rejection(&last_stderr) {
            break;
        }
        std::thread::sleep(MAINTENANCE_RETRY_DELAY);
    }
    Err(std::io::Error::other(format!(
        "alopex batch command failed: {last_stderr}"
    )))
}

fn alopex_output(
    args: &[&str],
    envs: &[(&str, &str)],
    stdin_piped: bool,
) -> std::io::Result<std::process::Output> {
    let mut cmd = Command::new(alopex_bin());
    for arg in args {
        cmd.arg(arg);
    }
    for (key, value) in envs {
        cmd.env(key, value);
    }
    if stdin_piped {
        cmd.stdin(Stdio::piped());
    }
    cmd.output()
}

fn server_profile() -> Result<String> {
    if let Ok(profile) = std::env::var("ALOPEX_TEST_SERVER_PROFILE") {
        return Ok(profile);
    }
    let server = ensure_test_server()?;
    Ok(server.profile.clone())
}

fn server_data_dir() -> Result<PathBuf> {
    let server = ensure_test_server()?;
    Ok(server._data_dir.clone())
}

fn read_latest_marker(root: &Path) -> Result<PathBuf> {
    let marker = root.join("latest");
    if !marker.exists() {
        return Err(std::io::Error::other(format!(
            "latest marker missing at {}",
            marker.display()
        ))
        .into());
    }
    let path = fs::read_to_string(&marker)?;
    let path = PathBuf::from(path.trim());
    if !path.exists() {
        return Err(std::io::Error::other(format!(
            "latest path does not exist: {}",
            path.display()
        ))
        .into());
    }
    Ok(path)
}

fn unique_suffix() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let short = millis % 100_000;
    format!("{}{:05}", std::process::id(), short)
}

struct TestServer {
    _temp: TempDir,
    _data_dir: PathBuf,
    _config_path: PathBuf,
    _log_path: PathBuf,
    #[allow(dead_code)]
    home_dir: PathBuf,
    profile: String,
    #[allow(dead_code)]
    http_port: u16,
    child: std::process::Child,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

static TEST_SERVER: OnceLock<std::result::Result<TestServer, String>> = OnceLock::new();
static SERVER_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn ensure_test_server() -> Result<&'static TestServer> {
    let entry = TEST_SERVER.get_or_init(|| start_test_server().map_err(|err| err.to_string()));
    match entry {
        Ok(server) => Ok(server),
        Err(message) => Err(std::io::Error::other(message.clone()).into()),
    }
}

fn server_lock() -> MutexGuard<'static, ()> {
    SERVER_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn start_test_server() -> Result<TestServer> {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let home_dir = temp.path().join("home");
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&home_dir)?;

    let (http_port, admin_port, grpc_port) = pick_ports()?;
    let config_path = temp.path().join("alopex.toml");
    let log_path = temp.path().join("server.log");
    let config = format!(
        "http_bind = \"127.0.0.1:{http_port}\"\n\
grpc_bind = \"127.0.0.1:{grpc_port}\"\n\
admin_bind = \"127.0.0.1:{admin_port}\"\n\
data_dir = \"{}\"\n",
        data_dir.display()
    );
    fs::write(&config_path, config)?;

    let mut server_cmd = Command::new(alopex_server_bin());
    server_cmd
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::from(fs::File::create(&log_path)?))
        .stderr(Stdio::from(fs::File::create(&log_path)?));
    // The server is cached in a `static OnceLock`, so `TestServer::drop` never
    // runs for the shared instance, and a SIGKILL of the test binary (e.g. a
    // CI timeout) would bypass `Drop` anyway. Bind the server's lifetime to
    // the test process with PR_SET_PDEATHSIG so it can never outlive the
    // tests and keep inherited fds (such as the global cargo build lock)
    // open forever.
    //
    // This guard is Linux-only (PR_SET_PDEATHSIG); on other platforms
    // (e.g. macOS CI runners) a hard-killed test run can still leak the
    // server until the runner VM is recycled.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: prctl/getppid/_exit are async-signal-safe; no allocation
        // happens between fork and exec.
        unsafe {
            server_cmd.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // If the parent already died before prctl took effect, exit
                // immediately instead of running as an orphan.
                if libc::getppid() == 1 {
                    libc::_exit(1);
                }
                Ok(())
            });
        }
    }
    let child = spawn_process_lifetime_bound(server_cmd)?;

    wait_for_server(http_port)?;

    let profile = "e2e-server".to_string();
    let config_dir = home_dir.join(".alopex");
    fs::create_dir_all(&config_dir)?;
    let profile_path = config_dir.join("config");
    let contents = format!(
        "default_profile = \"{profile}\"\n\n[profiles.{profile}]\nconnection_type = \"server\"\n\n[profiles.{profile}.server]\nurl = \"http://127.0.0.1:{http_port}\"\ninsecure = true\n"
    );
    fs::write(&profile_path, contents)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&profile_path, fs::Permissions::from_mode(0o600))?;
    }

    std::env::set_var("ALOPEX_TEST_SERVER_PROFILE", &profile);
    std::env::set_var("ALOPEX_TEST_SERVER_HOME", &home_dir);

    Ok(TestServer {
        _temp: temp,
        _data_dir: data_dir,
        _config_path: config_path,
        _log_path: log_path,
        home_dir,
        profile,
        http_port,
        child,
    })
}

/// Spawn `cmd` from a thread that lives for the rest of the test process.
///
/// PR_SET_PDEATHSIG delivers the death signal when the *forking thread*
/// terminates, and libtest runs every test on its own short-lived thread.
/// Spawning from a parked keeper thread ties the child's lifetime to the
/// whole test process instead of the first test that touched the server.
fn spawn_process_lifetime_bound(mut cmd: Command) -> Result<std::process::Child> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("e2e-server-keeper".to_string())
        .spawn(move || {
            let _ = tx.send(cmd.spawn());
            // Keep the forking thread alive for the process lifetime so the
            // PDEATHSIG configured in pre_exec only fires when the whole
            // test process exits.
            loop {
                std::thread::park();
            }
        })?;
    let child = rx
        .recv()
        .map_err(|err| std::io::Error::other(format!("server spawn thread failed: {err}")))??;
    Ok(child)
}

fn pick_ports() -> Result<(u16, u16, u16)> {
    let http = TcpListener::bind("127.0.0.1:0")?;
    let admin = TcpListener::bind("127.0.0.1:0")?;
    let grpc = TcpListener::bind("127.0.0.1:0")?;
    let http_port = http.local_addr()?.port();
    let admin_port = admin.local_addr()?.port();
    let grpc_port = grpc.local_addr()?.port();
    drop(http);
    drop(admin);
    drop(grpc);
    Ok((http_port, admin_port, grpc_port))
}

fn wait_for_server(port: u16) -> Result<()> {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..80 {
        if let Ok(mut stream) = TcpStream::connect(&addr) {
            let request =
                b"GET /api/admin/health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(request);
            let mut response = String::new();
            let _ = stream.read_to_string(&mut response);
            if response.contains("200") {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(std::io::Error::other("server did not become ready").into())
}

fn seed_kv_entries(dir: &Path, entries: &[(&str, &str)]) -> Result<()> {
    for (key, value) in entries {
        alopex_batch(&[
            "--data-dir",
            dir.to_str().expect("data dir"),
            "--batch",
            "kv",
            "put",
            key,
            value,
        ])?;
    }
    Ok(())
}

fn seed_kv_entries_remote(profile: &str, entries: &[(&str, &str)]) -> Result<()> {
    for (key, value) in entries {
        alopex_batch_with_profile(profile, &["kv", "put", key, value])?;
    }
    Ok(())
}

fn seed_sql_table(dir: &Path, table: &str) -> Result<()> {
    alopex_batch(&[
        "--data-dir",
        dir.to_str().expect("data dir"),
        "--batch",
        "sql",
        &format!("CREATE TABLE IF NOT EXISTS {table} (id INTEGER, name TEXT);"),
    ])?;
    alopex_batch(&[
        "--data-dir",
        dir.to_str().expect("data dir"),
        "--batch",
        "sql",
        &format!("INSERT INTO {table} (id, name) VALUES (1, 'alice');"),
    ])?;
    Ok(())
}

fn seed_sql_table_remote(profile: &str, table: &str) -> Result<()> {
    alopex_batch_with_profile(
        profile,
        &[
            "sql",
            &format!("CREATE TABLE IF NOT EXISTS {table} (id INTEGER, name TEXT);"),
        ],
    )?;
    alopex_batch_with_profile(
        profile,
        &[
            "sql",
            &format!("INSERT INTO {table} (id, name) VALUES (1, 'alice');"),
        ],
    )?;
    Ok(())
}

fn seed_columnar_segment(dir: &Path, table: &str) -> Result<()> {
    let file_path = dir.join("e2e_columnar.csv");
    fs::write(&file_path, "id,value\n1,10\n")?;
    alopex_batch(&[
        "--data-dir",
        dir.to_str().expect("data dir"),
        "--batch",
        "columnar",
        "ingest",
        "--file",
        file_path.to_str().expect("csv path"),
        "--table",
        table,
    ])?;
    Ok(())
}

fn seed_columnar_segment_remote(profile: &str, table: &str, column: &str) -> Result<()> {
    let temp = TempDir::new().expect("tempdir");
    let file_path = temp.path().join("e2e_columnar.csv");
    let contents = format!("id,{column}\n1,10\n");
    fs::write(&file_path, contents)?;
    alopex_batch_with_profile(
        profile,
        &[
            "columnar",
            "ingest",
            "--file",
            file_path.to_str().expect("csv path"),
            "--table",
            table,
        ],
    )?;
    Ok(())
}

fn seed_hnsw_index(dir: &Path, name: &str, dim: usize) -> Result<()> {
    alopex_batch(&[
        "--data-dir",
        dir.to_str().expect("data dir"),
        "--batch",
        "hnsw",
        "create",
        name,
        "--dim",
        &dim.to_string(),
        "--metric",
        "l2",
    ])?;
    Ok(())
}

fn seed_hnsw_index_remote(profile: &str, name: &str, dim: usize) -> Result<()> {
    alopex_batch_with_profile(
        profile,
        &[
            "hnsw",
            "create",
            name,
            "--dim",
            &dim.to_string(),
            "--metric",
            "l2",
        ],
    )?;
    Ok(())
}

fn seed_vector_entry(dir: &Path, index: &str, key: &str, vector: &str) -> Result<()> {
    alopex_batch(&[
        "--data-dir",
        dir.to_str().expect("data dir"),
        "--batch",
        "vector",
        "upsert",
        "--index",
        index,
        "--key",
        key,
        "--vector",
        vector,
    ])?;
    Ok(())
}

fn seed_vector_entry_remote(profile: &str, index: &str, key: &str, vector: &str) -> Result<()> {
    alopex_batch_with_profile(
        profile,
        &[
            "vector", "upsert", "--index", index, "--key", key, "--vector", vector,
        ],
    )?;
    Ok(())
}

fn new_harness() -> Result<TuiTestHarness> {
    TuiTestHarness::builder()
        .with_size(120, 50)
        .with_timeout(Duration::from_secs(10))
        .with_poll_interval(Duration::from_millis(50))
        .build()
}

fn toggle_detail(harness: &mut TuiTestHarness) -> Result<()> {
    harness.send_text("\r")?;
    if wait_for_contains(harness, "Detail", Duration::from_secs(5)).is_ok() {
        return Ok(());
    }
    harness.send_key(KeyCode::Enter)?;
    if wait_for_contains(harness, "Detail", Duration::from_secs(5)).is_ok() {
        return Ok(());
    }
    harness.send_text("\n")?;
    wait_for_contains(harness, "Detail", Duration::from_secs(5))
}

fn confirm_selection_overlay(
    harness: &mut TuiTestHarness,
    title: &str,
    timeout: Duration,
) -> Result<()> {
    harness.send_key(KeyCode::Enter)?;
    if wait_for_absent(harness, title, timeout).is_ok() {
        return Ok(());
    }
    harness.send_text("\r")?;
    if wait_for_absent(harness, title, timeout).is_ok() {
        return Ok(());
    }
    harness.send_text("\n")?;
    wait_for_absent(harness, title, timeout)
}

fn execute_admin_action(harness: &mut TuiTestHarness) -> Result<()> {
    stabilize_action_selection(harness, 5)?;
    harness.send_key(KeyCode::Enter)?;
    Ok(())
}

fn execute_action(harness: &mut TuiTestHarness, label: &str, max_steps: usize) -> Result<()> {
    for _ in 0..3 {
        ensure_action_selected(harness, label, max_steps)?;
        harness.update_state()?;
        if action_selected_label(&harness.screen_contents())
            .map(|selected| selected != label)
            .unwrap_or(true)
        {
            ensure_action_selected(harness, label, max_steps)?;
        }
        wait_for_contains(harness, &format!("Action: {label}"), Duration::from_secs(2))?;
        if ensure_action_stable(harness, label, 20)? {
            return execute_admin_action(harness);
        }
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!(
            "Action selection '{label}' did not stabilize before execution. Screen snapshot:\n{snapshot}"
        ),
    )
    .into())
}

fn stabilize_action_selection(harness: &mut TuiTestHarness, attempts: usize) -> Result<()> {
    let mut last = None;
    for _ in 0..attempts {
        harness.update_state()?;
        let current = action_selected_label(&harness.screen_contents());
        if current.is_some() && current == last {
            return Ok(());
        }
        last = current;
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn activate_resource_selection(harness: &mut TuiTestHarness) -> Result<()> {
    harness.send_key(KeyCode::Enter)?;
    harness.send_text("\r")?;
    harness.send_text("\n")?;
    Ok(())
}

fn selection_contains_label(contents: &str, label: &str) -> bool {
    for line in contents.lines() {
        let mut parts = line.split('│');
        let _ = parts.next();
        let Some(resource_cell) = parts.next() else {
            continue;
        };
        let cell = resource_cell;
        if let Some(pos) = cell.find('>') {
            let after = cell[pos + 1..].trim_start();
            if after.starts_with(label) {
                return true;
            }
        }
    }
    false
}

fn move_selection_to(harness: &mut TuiTestHarness, label: &str, max_steps: usize) -> Result<()> {
    for _ in 0..max_steps {
        if selection_contains_label(&harness.screen_contents(), label) {
            return Ok(());
        }
        harness.send_text("j")?;
        std::thread::sleep(Duration::from_millis(100));
        harness.update_state()?;
    }
    for _ in 0..max_steps {
        if selection_contains_label(&harness.screen_contents(), label) {
            return Ok(());
        }
        harness.send_text("k")?;
        std::thread::sleep(Duration::from_millis(50));
        harness.update_state()?;
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out waiting for selection '{label}'. Screen snapshot:\n{snapshot}"),
    )
    .into())
}

fn ensure_action_selected(
    harness: &mut TuiTestHarness,
    label: &str,
    max_steps: usize,
) -> Result<()> {
    if let Some(shortcut) = action_shortcut(label) {
        harness.send_key(KeyCode::Char(shortcut))?;
        if wait_for_action_match(harness, label, 20)? {
            return Ok(());
        }
        let snapshot = harness.screen_contents();
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "Timed out waiting for action '{label}' after shortcut. Screen snapshot:\n{snapshot}"
            ),
        )
        .into());
    }
    if action_selected_label(&harness.screen_contents())
        .map(|selected| selected == label)
        .unwrap_or(false)
        && ensure_action_stable(harness, label, 6)?
    {
        return Ok(());
    }
    for _ in 0..max_steps {
        harness.send_key(KeyCode::Down)?;
        if wait_for_action_match(harness, label, 6)? {
            return Ok(());
        }
    }
    for _ in 0..max_steps {
        harness.send_key(KeyCode::Up)?;
        if wait_for_action_match(harness, label, 6)? {
            return Ok(());
        }
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out waiting for action '{label}'. Screen snapshot:\n{snapshot}"),
    )
    .into())
}

fn wait_for_action_match(
    harness: &mut TuiTestHarness,
    label: &str,
    attempts: usize,
) -> Result<bool> {
    for _ in 0..attempts {
        std::thread::sleep(Duration::from_millis(50));
        harness.update_state()?;
        if action_selected_label(&harness.screen_contents())
            .map(|selected| selected == label)
            .unwrap_or(false)
            && ensure_action_stable(harness, label, 4)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn action_selected_label(contents: &str) -> Option<String> {
    for line in contents.lines() {
        let parts: Vec<&str> = line.split('│').collect();
        if parts.len() < 4 {
            continue;
        }
        let action_cell = parts[3];
        if let Some(pos) = action_cell.find('>') {
            let after = action_cell[pos + 1..].trim_start();
            if !after.is_empty() {
                return Some(after.trim().to_string());
            }
        }
    }
    None
}

fn action_shortcut(label: &str) -> Option<char> {
    match label {
        "Read / List" => Some('1'),
        "Create" => Some('2'),
        "Update" => Some('3'),
        "Delete" => Some('4'),
        "Archive" => Some('5'),
        "Restore" => Some('6'),
        "Backup" => Some('7'),
        "Export" => Some('8'),
        _ => None,
    }
}

fn ensure_action_stable(
    harness: &mut TuiTestHarness,
    label: &str,
    attempts: usize,
) -> Result<bool> {
    let mut stable = 0usize;
    for _ in 0..attempts {
        std::thread::sleep(Duration::from_millis(50));
        harness.update_state()?;
        let matches = action_selected_label(&harness.screen_contents())
            .map(|selected| selected == label)
            .unwrap_or(false);
        if matches {
            stable += 1;
            if stable >= 2 {
                return Ok(true);
            }
        } else {
            stable = 0;
        }
    }
    Ok(false)
}

fn status_row_fields(contents: &str) -> Option<Vec<String>> {
    for line in contents.lines() {
        if !line.contains('|') {
            continue;
        }
        let trimmed = line.trim_matches(|ch| ch == '│' || ch == ' ' || ch == '╞' || ch == '╡');
        let parts: Vec<String> = trimmed
            .split('|')
            .map(|part| part.trim().to_string())
            .collect();
        if parts.len() >= 3 && parts.first().map(|part| part.as_str()) != Some("Status") {
            return Some(parts);
        }
    }
    None
}

fn find_handle_in_status(contents: &str) -> Option<String> {
    let fields = status_row_fields(contents)?;
    let handle = fields.get(1)?.trim();
    if handle.len() == 36 && handle.chars().all(|ch| ch.is_ascii_hexdigit() || ch == '-') {
        Some(handle.to_string())
    } else {
        None
    }
}

fn find_state_in_status(contents: &str) -> Option<String> {
    let fields = status_row_fields(contents)?;
    let state = fields.get(2)?.trim();
    if state.is_empty() {
        None
    } else {
        Some(state.to_string())
    }
}

fn is_failure_state_label(state: &str) -> bool {
    matches!(
        state.trim().to_ascii_lowercase().as_str(),
        "failed" | "error" | "failure" | "canceled" | "cancelled"
    )
}

fn is_completion_state_label(state: &str) -> bool {
    state.trim().eq_ignore_ascii_case("completed")
}

fn wait_for_backup_completion(
    harness: &mut TuiTestHarness,
    handle: &str,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        harness.update_state()?;
        if let Some(state) = find_state_in_status(&harness.screen_contents()) {
            let normalized = state.trim().to_ascii_lowercase();
            if is_completion_state_label(&normalized) {
                return Ok(());
            }
            if is_failure_state_label(&normalized) {
                let snapshot = harness.screen_contents();
                return Err(std::io::Error::other(format!(
                    "Backup failed with state '{state}'. Screen snapshot:\n{snapshot}"
                ))
                .into());
            }
        }

        ensure_active_field(harness, "Handle", 6)?;
        edit_guided_field(harness, handle)?;
        execute_admin_action(harness)?;

        if start.elapsed() > timeout {
            let snapshot = harness.screen_contents();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Timed out waiting for backup completion. Screen snapshot:\n{snapshot}"),
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn wait_for_restore_completion(
    harness: &mut TuiTestHarness,
    handle: &str,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        harness.update_state()?;
        if let Some(state) = find_state_in_status(&harness.screen_contents()) {
            let normalized = state.to_ascii_lowercase();
            if is_completion_state_label(&normalized) {
                return Ok(());
            }
            if is_failure_state_label(&normalized) {
                let snapshot = harness.screen_contents();
                return Err(std::io::Error::other(format!(
                    "Restore failed with state '{state}'. Screen snapshot:\n{snapshot}"
                ))
                .into());
            }
        }

        ensure_active_field(harness, "Handle", 6)?;
        edit_guided_field(harness, handle)?;
        execute_admin_action(harness)?;

        if start.elapsed() > timeout {
            let snapshot = harness.screen_contents();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Timed out waiting for restore completion. Screen snapshot:\n{snapshot}"),
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn select_resource_target(
    harness: &mut TuiTestHarness,
    target_text: &str,
    max_steps: usize,
) -> Result<()> {
    for _ in 0..max_steps {
        if harness.screen_contents().contains(target_text) {
            return Ok(());
        }
        harness.send_key(KeyCode::Enter)?;
        if wait_for_contains(harness, target_text, Duration::from_secs(2)).is_ok() {
            return Ok(());
        }
        harness.send_text("j")?;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out waiting for '{target_text}' to appear."),
    )
    .into())
}

fn ensure_resource_target(
    harness: &mut TuiTestHarness,
    resource_label: &str,
    target_text: &str,
    max_steps: usize,
    timeout: Duration,
) -> Result<()> {
    if wait_for_contains(harness, target_text, Duration::from_secs(2)).is_ok() {
        return Ok(());
    }
    for _ in 0..3 {
        harness.send_text("h")?;
        let _ = wait_for_contains(harness, "Focus: Table", Duration::from_secs(2));
        move_selection_to(harness, resource_label, max_steps)?;
        harness.send_key(KeyCode::Enter)?;
        if wait_for_contains(harness, target_text, timeout).is_ok() {
            return Ok(());
        }
    }
    let snapshot = harness.screen_contents();
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!(
            "Timed out waiting for '{target_text}' after selecting '{resource_label}'. Screen snapshot:\n{snapshot}"
        ),
    )
    .into())
}

fn edit_guided_field(harness: &mut TuiTestHarness, value: &str) -> Result<()> {
    harness.send_key(KeyCode::Char('e'))?;
    wait_for_contains(harness, "Mode: editing field", Duration::from_secs(5))?;
    for _ in 0..40 {
        harness.send_key(KeyCode::Backspace)?;
    }
    harness.send_keys(value)?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(
        harness,
        "Input: guided fields (Tab to move, e to edit, o to list)",
        Duration::from_secs(5),
    )
}

fn exit_editing_with_enter(harness: &mut TuiTestHarness, mode_text: &str) -> Result<()> {
    harness.send_key(KeyCode::Enter)?;
    if wait_for_absent(harness, mode_text, Duration::from_secs(2)).is_ok() {
        return Ok(());
    }
    harness.send_text("\r")?;
    if wait_for_absent(harness, mode_text, Duration::from_secs(2)).is_ok() {
        return Ok(());
    }
    harness.send_text("\n")?;
    wait_for_absent(harness, mode_text, Duration::from_secs(2))
}

fn row_visible(contents: &str, row: usize) -> bool {
    let needle = row.to_string();
    for line in contents.lines() {
        let trimmed = line.trim_start_matches(['│', '▌', ' ']);
        if let Some(rest) = trimmed.strip_prefix(&needle) {
            if rest
                .chars()
                .next()
                .map(|c| c.is_whitespace())
                .unwrap_or(true)
            {
                return true;
            }
        }
    }
    false
}

fn selected_row_number_from_contents(contents: &str) -> Option<usize> {
    for line in contents.lines() {
        if let Some(idx) = line.find('▌') {
            let start = idx + '▌'.len_utf8();
            let digits: String = line[start..]
                .chars()
                .skip_while(|ch| ch.is_whitespace())
                .take_while(|ch| ch.is_ascii_digit())
                .collect();
            if !digits.is_empty() {
                return digits.parse::<usize>().ok();
            }
        }
    }
    None
}

fn selected_row_number(harness: &TuiTestHarness) -> Option<usize> {
    selected_row_number_from_contents(&harness.screen_contents())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_to_admin_flow() -> Result<()> {
    let mut harness = new_harness()?;

    let cmd = alopex_command(&["--in-memory", "sql", "SELECT 1 AS col_0"]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "SQL: SELECT 1", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Connection: local", Duration::from_secs(10))?;

    harness.send_text("?")?;
    wait_for_contains(&mut harness, "Ops: ?: close", Duration::from_secs(5))?;
    harness.send_text("?")?;
    let start = Instant::now();
    harness.wait_for(|state| {
        if !state.contains("Ops: ?: close") {
            true
        } else {
            start.elapsed() >= Duration::from_secs(5)
        }
    })?;

    harness.send_text("/")?;
    harness.send_text("1")?;
    wait_for_contains(&mut harness, "/1", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "Ops: Enter: detail", Duration::from_secs(5))?;

    wait_for_contains(&mut harness, "Rows: 1", Duration::from_secs(5))?;

    harness.send_text("a")?;
    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Actions", Duration::from_secs(10))?;
    ensure_focus_table(&mut harness)?;

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    ensure_focus_status(&mut harness)?;
    harness.send_text("h")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;

    harness.send_text("a")?;
    wait_for_contains(&mut harness, "Rows: 1", Duration::from_secs(10))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_escape_exit() -> Result<()> {
    let mut harness = new_harness()?;

    let cmd = alopex_command(&["--in-memory", "sql", "SELECT 1"]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Connection: local", Duration::from_secs(10))?;
    harness.send_key(KeyCode::Esc)?;

    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_escape_exit() -> Result<()> {
    let mut harness = new_harness()?;
    let data_dir = TempDir::new().expect("tempdir");
    seed_kv_entries(data_dir.path(), &[("key-00", "value-00")])?;

    let cmd = alopex_command(&["--data-dir", data_dir.path().to_str().unwrap()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    harness.send_key(KeyCode::Esc)?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_server_subcommand_launch() -> Result<()> {
    let _guard = server_lock();
    let mut harness = new_harness()?;
    let profile = server_profile()?;

    let cmd = alopex_command(&["--profile", profile.as_str(), "server"]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_profile_launch() -> Result<()> {
    let mut harness = new_harness()?;
    let profile = match std::env::var("ALOPEX_TEST_PROFILE") {
        Ok(profile) => profile,
        Err(_) => return Ok(()),
    };

    let cmd = alopex_command(&["--profile", profile.as_str()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_remote_resources_and_actions() -> Result<()> {
    let _guard = server_lock();
    let profile = server_profile()?;
    let suffix = unique_suffix();
    let table = format!("e2e_remote_sql_{suffix}");
    let kv_key = format!("e2e-remote-kv-{suffix}");
    let columnar_table = format!("e2e_remote_columnar_{suffix}");
    let column_name = format!("col_{suffix}");

    seed_sql_table_remote(&profile, &table)?;
    seed_kv_entries_remote(&profile, &[(&kv_key, "value-00")])?;
    seed_columnar_segment_remote(&profile, &columnar_table, &column_name)?;

    let mut harness = new_harness()?;
    let cmd = alopex_command(&["--profile", profile.as_str()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(15))?;
    wait_for_contains(&mut harness, "Actions", Duration::from_secs(15))?;
    wait_for_contains(
        &mut harness,
        &visible_prefix(&table, 18),
        Duration::from_secs(15),
    )?;
    wait_for_contains(
        &mut harness,
        &visible_prefix(&kv_key, 18),
        Duration::from_secs(15),
    )?;
    wait_for_contains(
        &mut harness,
        &visible_prefix(&column_name, 18),
        Duration::from_secs(15),
    )?;

    ensure_focus_table(&mut harness)?;
    move_selection_to(&mut harness, "SQL Tables", 30)?;
    activate_resource_selection(&mut harness)?;
    let _ = wait_for_contains(&mut harness, "Target: SQL", Duration::from_secs(5));

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    ensure_guided_fields(&mut harness)?;
    ensure_active_field(&mut harness, "Query", 6)?;
    edit_guided_field(&mut harness, &format!("SELECT * FROM {table}"))?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Last Result", Duration::from_secs(10))?;

    harness.send_text("h")?;
    wait_for_contains(&mut harness, "Focus: Table", Duration::from_secs(5))?;
    move_selection_to(&mut harness, "KV Keys", 30)?;
    activate_resource_selection(&mut harness)?;
    wait_for_contains(&mut harness, "Target: KV", Duration::from_secs(5))?;

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    ensure_action_selected(&mut harness, "Create", 6)?;
    ensure_guided_fields(&mut harness)?;
    ensure_active_field(&mut harness, "Key", 6)?;
    edit_guided_field(&mut harness, &format!("{kv_key}-new"))?;
    harness.send_key(KeyCode::Tab)?;
    edit_guided_field(&mut harness, "value-remote")?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Last Result", Duration::from_secs(10))?;

    harness.send_text("h")?;
    wait_for_contains(&mut harness, "Focus: Table", Duration::from_secs(5))?;
    move_selection_to(&mut harness, "Columnar Segments", 30)?;
    activate_resource_selection(&mut harness)?;
    wait_for_contains(&mut harness, "Target: Columnar", Duration::from_secs(5))?;

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    ensure_action_selected(&mut harness, "Read / List", 6)?;
    ensure_guided_fields(&mut harness)?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Last Result", Duration::from_secs(10))?;

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_remote_lifecycle_actions() -> Result<()> {
    let _guard = server_lock();
    let profile = server_profile()?;
    let mut harness = new_harness()?;
    let cmd = alopex_command(&["--profile", profile.as_str()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(15))?;
    wait_for_contains(&mut harness, "Actions", Duration::from_secs(15))?;

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;

    execute_action(&mut harness, "Archive", 10)?;
    wait_for_contains(&mut harness, "Archived", Duration::from_secs(15))?;

    execute_action(&mut harness, "Export", 10)?;
    wait_for_contains(&mut harness, "Exported", Duration::from_secs(15))?;

    ensure_action_selected(&mut harness, "Backup", 10)?;
    ensure_guided_fields(&mut harness)?;
    ensure_active_field(&mut harness, "Handle", 6)?;
    edit_guided_field(&mut harness, "")?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Handle", Duration::from_secs(15))?;
    let backup_handle =
        wait_for_value(&mut harness, Duration::from_secs(5), find_handle_in_status)?;
    wait_for_backup_completion(&mut harness, &backup_handle, Duration::from_secs(20))?;
    let backup_location =
        read_latest_marker(&server_data_dir()?.join(".lifecycle").join("backup"))?
            .display()
            .to_string();

    ensure_action_selected(&mut harness, "Restore", 10)?;
    ensure_guided_fields(&mut harness)?;
    ensure_active_field(&mut harness, "Handle", 6)?;
    edit_guided_field(&mut harness, "")?;
    ensure_active_field(&mut harness, "Source", 6)?;
    edit_guided_field(&mut harness, &backup_location)?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Handle", Duration::from_secs(15))?;
    let handle = wait_for_value(&mut harness, Duration::from_secs(5), find_handle_in_status)?;
    wait_for_restore_completion(&mut harness, &handle, Duration::from_secs(20))?;

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_batch_mode_flag_disables_tui() -> Result<()> {
    let output = alopex_output(
        &["--batch", "sql", "SELECT 1 AS col_0"],
        &[("TERM", "xterm-256color")],
        false,
    )?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("col_0"));
    assert!(!stdout.contains("q/Esc: quit"));
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_output_flag_disables_tui() -> Result<()> {
    let output = alopex_output(
        &["--output", "json", "sql", "SELECT 1 AS col_0"],
        &[("TERM", "xterm-256color")],
        false,
    )?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("col_0"));
    assert!(!stdout.contains("q/Esc: quit"));
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_non_tty_fallback_to_batch() -> Result<()> {
    let output = alopex_output(
        &["sql", "SELECT 1 AS col_0"],
        &[("TERM", "xterm-256color")],
        true,
    )?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("Warning: no TTY detected") || stderr.contains("Warning: no TTY detected")
    );
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_help_and_detail_toggle() -> Result<()> {
    let mut harness = new_harness()?;

    let long_value = "x".repeat(240) + "TAIL_MARKER";
    let query = format!("SELECT '{long_value}' AS col_0");
    let cmd = alopex_command(&["--in-memory", "sql", query.as_str()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Connection: local", Duration::from_secs(10))?;
    harness.send_text("?")?;
    wait_for_contains(&mut harness, "Ops: ?: close", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "Ops: Enter: detail", Duration::from_secs(5))?;
    harness.send_text("?")?;
    wait_for_contains(&mut harness, "Ops: ?: close", Duration::from_secs(5))?;
    harness.send_text("?")?;
    wait_for_contains(&mut harness, "Ops: Enter: detail", Duration::from_secs(5))?;

    toggle_detail(&mut harness)?;
    wait_for_absent(&mut harness, "TAIL_MARKER", Duration::from_secs(2)).ok();
    harness.send_text("J")?;
    wait_for_contains(&mut harness, "TAIL_MARKER", Duration::from_secs(5))?;
    harness.send_text("K")?;
    harness.send_key(KeyCode::Enter)?;
    wait_for_absent(&mut harness, "Detail", Duration::from_secs(5)).ok();

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_select_key_overlay() -> Result<()> {
    let mut harness = new_harness()?;

    let data_dir = TempDir::new().expect("tempdir");
    seed_sql_table(data_dir.path(), "e2e_users")?;

    let cmd = alopex_command(&["--data-dir", data_dir.path().to_str().unwrap()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Actions", Duration::from_secs(10))?;
    ensure_focus_table(&mut harness)?;

    select_resource_target(&mut harness, "Target: SQL", 40)?;
    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;

    open_selection_overlay_for(&mut harness, "Table", "Select Table", 6)?;
    wait_for_contains(&mut harness, "e2e_users", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Char('/'))?;
    harness.send_text("e2e")?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "Select Table", Duration::from_secs(5))?;
    wait_for_contains(&mut harness, "e2e_users", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Esc)?;

    open_selection_overlay_for(&mut harness, "Table", "Select Table", 6)?;
    if harness.screen_contents().contains("No options available.") {
        let snapshot = harness.screen_contents();
        return Err(std::io::Error::other(format!(
            "Selection overlay is empty. Screen snapshot:\n{snapshot}"
        ))
        .into());
    }
    harness.send_key(KeyCode::Char('g'))?;
    harness.send_key(KeyCode::Char('G'))?;
    harness.send_key(KeyCode::Char('j'))?;
    harness.send_key(KeyCode::Char('k'))?;
    confirm_selection_overlay(&mut harness, "Select Table", Duration::from_secs(5))?;
    if wait_for_contains(&mut harness, "Table: e2e_users", Duration::from_secs(5)).is_err() {
        let snapshot = harness.screen_contents();
        return Err(std::io::Error::other(format!(
            "Failed to confirm table selection. Screen snapshot:\n{snapshot}"
        ))
        .into());
    }

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_sql_query_execution() -> Result<()> {
    let mut harness = new_harness()?;

    let data_dir = TempDir::new().expect("tempdir");
    seed_sql_table(data_dir.path(), "e2e_users")?;

    let cmd = alopex_command(&["--data-dir", data_dir.path().to_str().unwrap()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Actions", Duration::from_secs(10))?;
    ensure_focus_table(&mut harness)?;

    move_selection_to(&mut harness, "SQL Tables", 10)?;
    activate_resource_selection(&mut harness)?;
    let _ = wait_for_contains(&mut harness, "Target: SQL", Duration::from_secs(5));

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    wait_for_contains(&mut harness, "Action: Read / List", Duration::from_secs(5))?;
    ensure_guided_fields(&mut harness)?;

    open_selection_overlay_for(&mut harness, "Table", "Select Table", 6)?;
    wait_for_contains(&mut harness, "e2e_users", Duration::from_secs(5))?;
    confirm_selection_overlay(&mut harness, "Select Table", Duration::from_secs(5))?;

    open_selection_overlay_for(&mut harness, "Columns", "Select Columns", 6)?;
    wait_for_contains(&mut harness, "id", Duration::from_secs(5))?;
    confirm_selection_overlay(&mut harness, "Select Columns", Duration::from_secs(5))?;

    ensure_active_field(&mut harness, "Query", 6)?;
    edit_guided_field(&mut harness, "SELECT * FROM e2e_users")?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Last Result", Duration::from_secs(5))?;

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_admin_focus_edit_raw_and_exit() -> Result<()> {
    let mut harness = new_harness()?;

    let data_dir = TempDir::new().expect("tempdir");
    let kv_entries = (0..30)
        .map(|idx| {
            let key = format!("key-{idx:02}");
            let value = format!("value-{idx:02}");
            (key, value)
        })
        .collect::<Vec<_>>();
    let kv_refs = kv_entries
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect::<Vec<_>>();
    seed_kv_entries(data_dir.path(), &kv_refs)?;
    seed_sql_table(data_dir.path(), "e2e_users")?;
    seed_columnar_segment(data_dir.path(), "e2e_columnar")?;

    let cmd = alopex_command(&["--data-dir", data_dir.path().to_str().unwrap()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Actions", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Focus: Table", Duration::from_secs(10))?;
    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    wait_for_contains(&mut harness, "Action: Read / List", Duration::from_secs(5))?;
    harness.send_text("h")?;
    wait_for_contains(&mut harness, "Focus: Table", Duration::from_secs(5))?;

    harness.send_text("R")?;
    wait_for_contains(&mut harness, "SQL Tables", Duration::from_secs(5))?;

    harness.send_key_with_modifiers(KeyCode::Char('d'), Modifiers::CTRL)?;
    harness.send_key_with_modifiers(KeyCode::Char('u'), Modifiers::CTRL)?;
    harness.send_text("G")?;
    wait_for_contains(&mut harness, "key-29", Duration::from_secs(5))?;
    harness.send_text("g")?;
    wait_for_contains(&mut harness, "> SQL Tables", Duration::from_secs(5))?;

    move_selection_to(&mut harness, "SQL Tables", 10)?;
    activate_resource_selection(&mut harness)?;
    wait_for_contains(&mut harness, "Target: SQL", Duration::from_secs(5))?;
    harness.send_text("e")?;
    wait_for_contains(&mut harness, "Editing field", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    harness.send_text("e")?;
    wait_for_contains(&mut harness, "Editing field", Duration::from_secs(5))?;
    exit_editing_with_enter(&mut harness, "Editing field")?;
    harness.send_text("h")?;
    wait_for_contains(&mut harness, "Focus: Table", Duration::from_secs(5))?;
    harness.send_text("r")?;
    wait_for_contains(&mut harness, "Editing raw params", Duration::from_secs(5))?;
    exit_editing_with_enter(&mut harness, "Editing raw params")?;
    harness.send_text("e")?;
    wait_for_contains(&mut harness, "Editing raw params", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "raw parameters", Duration::from_secs(5))?;
    harness.send_text("r")?;
    wait_for_contains(&mut harness, "guided fields", Duration::from_secs(5))?;
    harness.send_text("h")?;
    wait_for_contains(&mut harness, "Focus: Table", Duration::from_secs(5))?;
    select_resource_target(&mut harness, "Target: Columnar", 30)?;
    move_selection_to(&mut harness, "KV Keys", 20)?;
    harness.send_key(KeyCode::Enter)?;
    ensure_resource_target(
        &mut harness,
        "KV Keys",
        "Target: KV",
        20,
        Duration::from_secs(5),
    )?;

    harness.send_text("/")?;
    harness.send_text("KV")?;
    wait_for_absent(&mut harness, "SQL Tables", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "SQL Tables", Duration::from_secs(5))?;
    harness.send_text("/")?;
    harness.send_text("KV")?;
    harness.send_key(KeyCode::Enter)?;
    wait_for_absent(&mut harness, "SQL Tables", Duration::from_secs(5))?;
    harness.send_text("/")?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "SQL Tables", Duration::from_secs(5))?;

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    wait_for_contains(&mut harness, "Action: Read / List", Duration::from_secs(5))?;
    harness.send_text("j")?;
    wait_for_contains(&mut harness, "Action: Create", Duration::from_secs(5))?;
    harness.send_text("k")?;
    wait_for_contains(&mut harness, "Action: Read / List", Duration::from_secs(5))?;

    harness.send_text("r")?;
    wait_for_contains(&mut harness, "editing raw params", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "raw parameters", Duration::from_secs(5))?;
    harness.send_text("r")?;
    wait_for_contains(&mut harness, "guided fields", Duration::from_secs(5))?;

    harness.send_text("e")?;
    wait_for_contains(&mut harness, "Editing field", Duration::from_secs(5))?;
    harness.send_text("SELECT 1")?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;

    harness.send_text("h")?;
    wait_for_contains(&mut harness, "Focus: Table", Duration::from_secs(5))?;
    harness.send_text("j")?;
    harness.send_text("j")?;
    harness.send_key(KeyCode::Enter)?;
    ensure_resource_target(
        &mut harness,
        "KV Keys",
        "Target: KV",
        20,
        Duration::from_secs(5),
    )?;

    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Tab)?;
    harness.send_key_with_modifiers(KeyCode::Tab, Modifiers::SHIFT)?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;

    if wait_for_contains(
        &mut harness,
        "Input: guided fields (Tab to move, e to edit, o to list)",
        Duration::from_secs(2),
    )
    .is_err()
    {
        harness.send_key(KeyCode::Char('r'))?;
        wait_for_contains(
            &mut harness,
            "Input: guided fields (Tab to move, e to edit, o to list)",
            Duration::from_secs(5),
        )?;
    }

    edit_guided_field(&mut harness, "key-29")?;
    harness.send_key(KeyCode::Tab)?;
    edit_guided_field(&mut harness, "value-29")?;

    execute_admin_action(&mut harness)?;
    harness.wait_for(|state| {
        let contents = state.contents();
        contents.contains("Missing: key, value") || contents.contains("key-29")
    })?;

    for _ in 0..10 {
        harness.send_text("k")?;
    }
    wait_for_contains(&mut harness, "Action: Read / List", Duration::from_secs(5))?;
    harness.send_text("j")?;
    wait_for_contains(&mut harness, "Action: Create", Duration::from_secs(5))?;
    edit_guided_field(&mut harness, "e2e-key")?;
    harness.send_key(KeyCode::Tab)?;
    edit_guided_field(&mut harness, "e2e-value")?;
    execute_admin_action(&mut harness)?;

    ensure_focus_status(&mut harness)?;
    harness.send_text("j")?;
    harness.send_text("k")?;
    harness.send_key_with_modifiers(KeyCode::Char('d'), Modifiers::CTRL)?;
    harness.send_text("G")?;
    harness.send_key_with_modifiers(KeyCode::Char('u'), Modifiers::CTRL)?;
    harness.send_text("g")?;
    harness.wait_for(|state| {
        let contents = state.contents();
        contents.contains("Missing: key, value") || contents.contains("key-29")
    })?;

    harness.send_text("?")?;
    wait_for_contains(&mut harness, "Help", Duration::from_secs(5))?;
    harness.send_text("?")?;

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_to_admin_vector_target() -> Result<()> {
    let mut harness = new_harness()?;
    let data_dir = TempDir::new().expect("tempdir");

    seed_hnsw_index(data_dir.path(), "e2e_vec", 2)?;
    seed_vector_entry(data_dir.path(), "e2e_vec", "vec1", "[1.0, 2.0]")?;

    let cmd = alopex_command(&[
        "--data-dir",
        data_dir.path().to_str().unwrap(),
        "vector",
        "search",
        "--index",
        "e2e_vec",
        "--query",
        "[1.0, 2.0]",
        "-k",
        "1",
    ]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Rows:", Duration::from_secs(10))?;
    harness.send_text("a")?;
    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Target: Vector", Duration::from_secs(5))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_to_admin_hnsw_target() -> Result<()> {
    let mut harness = new_harness()?;
    let data_dir = TempDir::new().expect("tempdir");

    seed_hnsw_index(data_dir.path(), "e2e_hnsw", 2)?;

    let cmd = alopex_command(&[
        "--data-dir",
        data_dir.path().to_str().unwrap(),
        "hnsw",
        "stats",
        "e2e_hnsw",
    ]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Rows:", Duration::from_secs(10))?;
    harness.send_text("a")?;
    wait_for_contains(&mut harness, "Resources", Duration::from_secs(10))?;
    wait_for_contains(&mut harness, "Target: HNSW", Duration::from_secs(5))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_to_admin_flow_remote() -> Result<()> {
    let _guard = server_lock();
    let profile = server_profile()?;
    let suffix = unique_suffix();
    let table = format!("e2e_remote_flow_{suffix}");
    seed_sql_table_remote(&profile, &table)?;

    let mut harness = new_harness()?;
    let query = format!("SELECT * FROM {table}");
    let cmd = alopex_command(&["--profile", profile.as_str(), "sql", query.as_str()]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Rows:", Duration::from_secs(15))?;
    harness.send_text("a")?;
    wait_for_contains(&mut harness, "Resources", Duration::from_secs(15))?;
    wait_for_contains(&mut harness, "Target: SQL", Duration::from_secs(5))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_to_admin_vector_target_remote() -> Result<()> {
    let _guard = server_lock();
    let profile = server_profile()?;
    let suffix = unique_suffix();
    let index = format!("e2e_remote_vec_{suffix}");
    seed_hnsw_index_remote(&profile, &index, 2)?;
    seed_vector_entry_remote(&profile, &index, "vec1", "[1.0, 2.0]")?;

    let mut harness = new_harness()?;
    let cmd = alopex_command(&[
        "--profile",
        profile.as_str(),
        "vector",
        "search",
        "--index",
        index.as_str(),
        "--query",
        "[1.0, 2.0]",
        "-k",
        "1",
    ]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Rows:", Duration::from_secs(15))?;
    harness.send_text("a")?;
    wait_for_contains(&mut harness, "Resources", Duration::from_secs(15))?;
    wait_for_contains(&mut harness, "Target: Vector", Duration::from_secs(5))?;
    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    ensure_action_selected(&mut harness, "Read / List", 6)?;
    ensure_guided_fields(&mut harness)?;
    ensure_active_field(&mut harness, "Index", 6)?;
    edit_guided_field(&mut harness, index.as_str())?;
    ensure_active_field(&mut harness, "Query", 6)?;
    edit_guided_field(&mut harness, "[1.0, 2.0]")?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Last Result", Duration::from_secs(10))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_to_admin_hnsw_target_remote() -> Result<()> {
    let _guard = server_lock();
    let profile = server_profile()?;
    let suffix = unique_suffix();
    let index = format!("e2e_remote_hnsw_{suffix}");
    seed_hnsw_index_remote(&profile, &index, 2)?;

    let mut harness = new_harness()?;
    let cmd = alopex_command(&[
        "--profile",
        profile.as_str(),
        "hnsw",
        "stats",
        index.as_str(),
    ]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Rows:", Duration::from_secs(15))?;
    harness.send_text("a")?;
    wait_for_contains(&mut harness, "Resources", Duration::from_secs(15))?;
    wait_for_contains(&mut harness, "Target: HNSW", Duration::from_secs(5))?;
    harness.send_text("l")?;
    wait_for_contains(&mut harness, "Focus: Detail", Duration::from_secs(5))?;
    ensure_action_selected(&mut harness, "Read / List", 6)?;
    ensure_guided_fields(&mut harness)?;
    ensure_active_field(&mut harness, "Index", 6)?;
    edit_guided_field(&mut harness, index.as_str())?;
    execute_admin_action(&mut harness)?;
    wait_for_contains(&mut harness, "Last Result", Duration::from_secs(10))?;
    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_search_confirm_and_navigation() -> Result<()> {
    let mut harness = new_harness()?;
    let data_dir = TempDir::new().expect("tempdir");
    seed_kv_entries(
        data_dir.path(),
        &[
            ("alpha", "v1"),
            ("beta", "v2"),
            ("alphabet", "v3"),
            ("gamma", "v4"),
        ],
    )?;

    let cmd = alopex_command(&[
        "--data-dir",
        data_dir.path().to_str().unwrap(),
        "kv",
        "list",
    ]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Rows: 4", Duration::from_secs(10))?;
    harness.send_key(KeyCode::Char('/'))?;
    harness.send_text("a")?;
    wait_for_contains(&mut harness, "/a", Duration::from_secs(5))?;
    harness.send_text("l")?;
    wait_for_contains(&mut harness, "/al", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Backspace)?;
    wait_for_contains(&mut harness, "/a", Duration::from_secs(5))?;
    harness.send_key(KeyCode::Enter)?;
    wait_for_contains(&mut harness, "/a", Duration::from_secs(5))?;

    harness.send_key(KeyCode::Char('n'))?;
    harness.send_key(KeyCode::Char('N'))?;

    harness.send_text("/")?;
    harness.send_key(KeyCode::Esc)?;
    wait_for_contains(&mut harness, "Ops: Enter: detail", Duration::from_secs(5))?;

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_navigation_and_paging() -> Result<()> {
    let mut harness = new_harness()?;
    let data_dir = TempDir::new().expect("tempdir");
    let entries = (0..25)
        .map(|idx| (format!("row-{idx:02}"), "v".to_string()))
        .collect::<Vec<_>>();
    let entry_refs = entries
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect::<Vec<_>>();
    seed_kv_entries(data_dir.path(), &entry_refs)?;

    let cmd = alopex_command(&[
        "--data-dir",
        data_dir.path().to_str().unwrap(),
        "kv",
        "list",
    ]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "Rows: 25", Duration::from_secs(10))?;
    harness.wait_for(|state| selected_row_number_from_contents(&state.contents()) == Some(1))?;
    harness.send_text("j")?;
    harness.wait_for(|state| selected_row_number_from_contents(&state.contents()) == Some(2))?;
    harness.send_text("k")?;
    harness.wait_for(|state| selected_row_number_from_contents(&state.contents()) == Some(1))?;
    assert_eq!(selected_row_number(&harness), Some(1));
    harness.send_text("G")?;
    harness.wait_for(|state| row_visible(&state.contents(), 25))?;
    harness.send_text("g")?;
    harness.wait_for(|state| row_visible(&state.contents(), 1))?;

    harness.send_key_with_modifiers(KeyCode::Char('d'), Modifiers::CTRL)?;
    harness.wait_for(|state| row_visible(&state.contents(), 11))?;
    harness.send_key_with_modifiers(KeyCode::Char('u'), Modifiers::CTRL)?;
    harness.wait_for(|state| row_visible(&state.contents(), 1))?;

    harness.send_key(KeyCode::PageDown)?;
    harness.wait_for(|state| row_visible(&state.contents(), 11))?;
    harness.send_key(KeyCode::PageUp)?;

    harness.send_text("q")?;
    Ok(())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn e2e_results_horizontal_scroll() -> Result<()> {
    let mut harness = new_harness()?;
    let cmd = alopex_command(&[
        "--in-memory",
        "sql",
        "SELECT 1 AS col_one, 2 AS col_two, 3 AS col_three, 4 AS col_four, 5 AS col_five, 6 AS col_six, 7 AS col_seven, 8 AS col_eight",
    ]);
    harness.spawn(cmd)?;

    wait_for_contains(&mut harness, "col_one", Duration::from_secs(10))?;
    harness.send_text("l")?;
    wait_for_contains(&mut harness, "col_two", Duration::from_secs(5))?;
    wait_for_absent(&mut harness, "col_one", Duration::from_secs(5)).ok();
    harness.send_text("h")?;
    wait_for_contains(&mut harness, "col_one", Duration::from_secs(5))?;

    harness.send_text("q")?;
    Ok(())
}
