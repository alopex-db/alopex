//! Regression tests for issue #181: opening one data directory from more than
//! one place must be rejected instead of silently corrupting WAL and SSTables.

#![cfg(not(target_arch = "wasm32"))]

use std::fs;
use std::path::Path;

use alopex_core::error::Error;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::{ConvergePolicy, LsmKV, LsmKVConfig, LOCK_FILE_NAME};
use alopex_core::types::TxnMode;
use tempfile::tempdir;

fn test_config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::NoSync,
        },
        ..Default::default()
    }
}

fn open(path: &Path) -> alopex_core::error::Result<LsmKV> {
    LsmKV::open_with_config(path, test_config()).map(|(store, _)| store)
}

fn assert_already_open(err: Error, expected_data_dir: &Path) {
    match &err {
        Error::AlreadyOpen { path, .. } => {
            assert_eq!(
                path, expected_data_dir,
                "AlreadyOpen must name the data dir"
            );
        }
        other => panic!("expected Error::AlreadyOpen, got {other:?}"),
    }
    let rendered = err.to_string();
    assert!(
        rendered.contains("already open by another process"),
        "error message must carry the stable searchable string, got: {rendered}"
    );
}

/// The core symptom of #181: a second open of a plain data directory used to
/// succeed, giving two writers the same WAL ring and the same SSTable ids.
#[test]
fn second_open_of_a_plain_directory_is_rejected() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("plaindir");

    let first = open(&data_dir).expect("first open succeeds");

    let err = open(&data_dir).expect_err("second open must be rejected");
    assert_already_open(err, &data_dir);

    assert!(
        data_dir.join(LOCK_FILE_NAME).exists(),
        "a plain directory keeps its lock file inside itself"
    );
    drop(first);
}

/// Releasing the handle releases the lock, so sequential opens keep working.
#[test]
fn reopen_after_drop_succeeds() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("plaindir");

    let first = open(&data_dir).expect("first open succeeds");
    {
        let mut txn = first.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        txn.commit_self().unwrap();
    }
    drop(first);

    let second = open(&data_dir).expect("reopen after drop succeeds");
    let mut txn = second.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(txn.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

/// D3: for the `X.alopex.d` sidecar shape the lock lives *next to* the
/// container, never inside the sidecar that `prune_sidecar` deletes.
#[test]
fn sidecar_shape_locks_next_to_the_container() {
    let dir = tempdir().unwrap();
    let container = dir.path().join("mydb.alopex");
    let data_dir = dir.path().join("mydb.alopex.d");
    let lock_path = dir.path().join("mydb.alopex.lock");

    let store = open(&data_dir).expect("first open succeeds");
    {
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        txn.commit_self().unwrap();
    }
    assert!(lock_path.exists(), "lock file sits beside the container");
    assert!(
        !data_dir.join(LOCK_FILE_NAME).exists(),
        "the sidecar itself must stay free of lock files"
    );

    let err = open(&data_dir).expect_err("second open must be rejected");
    assert_already_open(err, &data_dir);

    store.close().unwrap();
    drop(store);

    // #178 convergence still holds: the sidecar is pruned, the container remains,
    // and the (now unlocked) lock file does not block the prune.
    assert!(!data_dir.exists(), "sidecar is pruned on drop");
    assert!(container.exists(), "container survives");

    // And the lock is genuinely released, so the container reopens.
    let reopened = open(&data_dir).expect("reopen after prune succeeds");
    let mut txn = reopened.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(txn.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

/// D4: `ConvergePolicy::Never` must resolve to the *same* lock path as
/// `SidecarOnly` for the sidecar shape, otherwise two processes using different
/// policies would each take a different lock and both get in.
#[test]
fn never_policy_shares_the_lock_with_sidecar_only() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("mydb.alopex.d");

    let mut never = test_config();
    never.converge = ConvergePolicy::Never;
    never.prune_sidecar_on_drop = false;

    let first = LsmKV::open_with_config(&data_dir, never).expect("first open succeeds");

    let err = open(&data_dir).expect_err("SidecarOnly open must hit the same lock");
    assert_already_open(err, &data_dir);
    drop(first);
}

/// D6: `close()` converges but leaves the store writable, so it must not hand
/// the directory to another process.
#[test]
fn close_does_not_release_the_lock() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("plaindir");

    let store = open(&data_dir).expect("first open succeeds");
    store.converge().unwrap();
    store.close().unwrap();

    let err = open(&data_dir).expect_err("close must not release the lock");
    assert_already_open(err, &data_dir);
    drop(store);
}

/// D5: the lock is taken before `restore_from_container`, so a losing opener
/// never reaches the destructive rehydrate path (`discard_dead_sidecar` +
/// `rehydrate`) that would wipe the winner's freshly rebuilt sidecar.
#[test]
fn container_only_state_is_protected_before_rehydrate() {
    let dir = tempdir().unwrap();
    let container = dir.path().join("mydb.alopex");
    let data_dir = dir.path().join("mydb.alopex.d");

    {
        let store = open(&data_dir).unwrap();
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        txn.commit_self().unwrap();
        store.close().unwrap();
    }
    assert!(container.exists() && !data_dir.exists());

    // First opener rehydrates the sidecar out of the container.
    let first = open(&data_dir).expect("rehydrating open succeeds");
    assert!(data_dir.exists(), "sidecar was rehydrated");

    // Second opener must be rejected *without* touching the sidecar.
    let err = open(&data_dir).expect_err("second open must be rejected");
    assert_already_open(err, &data_dir);
    assert!(
        data_dir.join("lsm.wal").exists(),
        "the loser must not have discarded the winner's sidecar"
    );

    let mut txn = first.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(txn.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

/// A stale lock file left behind by a crash carries no exclusion by itself.
#[test]
fn a_leftover_lock_file_alone_does_not_block_opening() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("plaindir");
    fs::create_dir_all(&data_dir).unwrap();
    fs::write(
        data_dir.join(LOCK_FILE_NAME),
        "pid=999999 exe=/nonexistent started_ms=0\n",
    )
    .unwrap();

    let store = open(&data_dir).expect("an unlocked leftover lock file is inert");
    drop(store);
}
