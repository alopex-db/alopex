use alopex_embedded::{Database, TxnMode};
use std::fs;
use tempfile::tempdir;

fn key(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn val(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[test]
fn crud_cycle_in_memory() {
    let db = Database::open_in_memory().expect("in-memory db");

    // Put and commit.
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.put(&key("k1"), &val("v1")).unwrap();
    txn.commit().unwrap();

    // Read verifies value.
    let mut read_txn = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read_txn.get(&key("k1")).unwrap(), Some(val("v1")));

    // Delete and commit.
    let mut del_txn = db.begin(TxnMode::ReadWrite).unwrap();
    del_txn.delete(&key("k1")).unwrap();
    del_txn.commit().unwrap();

    // Ensure removed.
    let mut read_txn2 = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read_txn2.get(&key("k1")).unwrap(), None);
}

#[test]
fn rollback_discards_changes() {
    let db = Database::open_in_memory().expect("in-memory db");

    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.put(&key("temp"), &val("value")).unwrap();
    txn.rollback().unwrap();

    let mut read_txn = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read_txn.get(&key("temp")).unwrap(), None);
}

#[test]
fn detects_conflict_between_transactions() {
    let db = Database::open_in_memory().expect("in-memory db");

    let mut t1 = db.begin(TxnMode::ReadWrite).unwrap();
    t1.get(&key("shared")).unwrap(); // track read version

    let mut t2 = db.begin(TxnMode::ReadWrite).unwrap();
    t2.put(&key("shared"), &val("second")).unwrap();
    t2.commit().unwrap();

    // t1 now conflicts because shared key was updated by t2.
    t1.put(&key("shared"), &val("first")).unwrap();
    let result = t1.commit();
    assert!(result.is_err(), "expected conflict when committing t1 after t2");
}

#[test]
fn persist_to_disk_is_atomic_and_reloadable() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    // Seed data in memory.
    let db = Database::open_in_memory().expect("in-memory db");
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.put(&key("hello"), &val("world")).unwrap();
    txn.commit().unwrap();

    db.persist_to_disk(&wal_path).expect("persist success");

    // All files exist, no temp leftovers.
    let sst = wal_path.with_extension("sst");
    let vec = wal_path.with_extension("vec");
    assert!(wal_path.exists());
    assert!(sst.exists());
    assert!(vec.exists());
    assert!(!sst.with_extension("sst.tmp").exists());
    assert!(!vec.with_extension("vec.tmp").exists());

    // Reload from disk path and verify data.
    let disk_db = Database::open(&wal_path).expect("open persisted db");
    let mut read_txn = disk_db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read_txn.get(&key("hello")).unwrap(), Some(val("world")));
}

#[test]
fn persist_to_disk_path_exists_error_reports_actual_path() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    // Pre-create WAL file to trigger PathExists.
    fs::write(&wal_path, b"already").unwrap();

    let db = Database::open_in_memory().expect("in-memory db");
    let err = db.persist_to_disk(&wal_path).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("path exists:") && msg.contains("wal.log"),
        "expected PathExists with wal path, got {msg}"
    );

    // No temp artifacts should exist.
    assert!(!wal_path.with_extension("sst").exists());
    assert!(!wal_path.with_extension("vec").exists());
    assert!(!wal_path.with_extension("sst.tmp").exists());
    assert!(!wal_path.with_extension("vec.tmp").exists());
}

#[test]
fn clone_to_memory_is_independent() {
    let db = Database::open_in_memory().expect("in-memory db");
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.put(&key("k1"), &val("v1")).unwrap();
    txn.commit().unwrap();

    let clone = db.clone_to_memory().expect("clone");

    // Change original after cloning.
    let mut txn_orig = db.begin(TxnMode::ReadWrite).unwrap();
    txn_orig.put(&key("k1"), &val("v2")).unwrap();
    txn_orig.commit().unwrap();

    // Clone should still see old value.
    let mut read_clone = clone.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read_clone.get(&key("k1")).unwrap(), Some(val("v1")));

    // Original sees new value.
    let mut read_orig = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read_orig.get(&key("k1")).unwrap(), Some(val("v2")));
}
