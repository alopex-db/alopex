use alopex_embedded::{Database, TxnMode};

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
