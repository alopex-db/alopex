//! Issue #181 regression: the server and an embedded handle must not be able to
//! open the same `data_dir` at the same time, in either order.

use std::time::Duration;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::Server;
use tempfile::tempdir;

/// Stable substring every `AlreadyOpen` rendering must contain.
const LOCK_MESSAGE: &str = "already open by another process";

fn server_config(data_dir: &std::path::Path) -> ServerConfig {
    ServerConfig {
        data_dir: data_dir.to_path_buf(),
        auth_mode: AuthMode::None,
        query_timeout: Duration::from_secs(5),
        audit_log_enabled: false,
        ..ServerConfig::default()
    }
}

#[test]
fn an_embedded_handle_cannot_open_a_running_servers_data_dir() {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(server_config(temp.path())).expect("server starts");

    // `Database` is not `Debug`, so unwrap the error by hand.
    let err = alopex_embedded::Database::open(temp.path())
        .err()
        .expect("the server owns this data_dir");
    assert!(
        err.to_string().contains(LOCK_MESSAGE),
        "expected the stable lock message, got: {err}"
    );

    drop(server);
}

#[test]
fn a_server_cannot_start_on_a_data_dir_an_embedded_handle_holds() {
    let temp = tempdir().expect("tempdir");
    let db = alopex_embedded::Database::open(temp.path()).expect("embedded opens first");

    let err = Server::new(server_config(temp.path()))
        .err()
        .expect("the embedded handle owns this data_dir, so the server must refuse to start");
    assert!(
        err.to_string().contains(LOCK_MESSAGE),
        "expected the stable lock message, got: {err}"
    );

    drop(db);
}

#[test]
fn releasing_the_server_hands_the_data_dir_back() {
    let temp = tempdir().expect("tempdir");

    let server = Server::new(server_config(temp.path())).expect("server starts");
    assert!(alopex_embedded::Database::open(temp.path()).is_err());
    drop(server);

    // Every Arc<ServerState> is gone, so the store is dropped and the lock with it.
    let db = alopex_embedded::Database::open(temp.path())
        .expect("the data_dir is free once the server is released");
    drop(db);

    // ...and it can go back to the server afterwards.
    let server = Server::new(server_config(temp.path())).expect("server restarts");
    drop(server);
}

/// 裁定 D16: `RecoveryCoordinator::open_store` quarantines the WAL when the first
/// open fails, then retries. An `AlreadyOpen` failure must skip that path — the
/// WAL it would rename is another live process's, and moving it is exactly the
/// corruption this issue is about.
#[test]
fn a_locked_out_server_does_not_quarantine_the_live_wal() {
    let temp = tempdir().expect("tempdir");
    let db = alopex_embedded::Database::open(temp.path()).expect("embedded opens first");

    let err = Server::new(server_config(temp.path()))
        .err()
        .expect("locked out");
    assert!(err.to_string().contains(LOCK_MESSAGE));

    let wal = temp.path().join("lsm.wal");
    assert!(wal.exists(), "the live WAL must still be where it was");
    let quarantined: Vec<_> = std::fs::read_dir(temp.path())
        .expect("read data_dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("wal.bad"))
        .collect();
    assert!(
        quarantined.is_empty(),
        "a lock conflict must not quarantine anyone's WAL, found: {quarantined:?}"
    );

    drop(db);
}
