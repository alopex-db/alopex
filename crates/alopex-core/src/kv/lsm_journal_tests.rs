use super::*;
use crate::lsm::wal::{SyncMode, WalConfig};
use crate::lsm::LsmKVConfig;

fn store(path: &Path) -> Arc<AnyKV> {
    let config = LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::EveryWrite,
        },
        prune_sidecar_on_drop: false,
        ..Default::default()
    };
    Arc::new(AnyKV::Lsm(Box::new(
        LsmKV::open_with_config(path, config).unwrap().0,
    )))
}
fn seed(store: &Arc<AnyKV>) {
    let mut tx = OwnedLsmTransaction::new(store.clone(), TxnMode::ReadWrite).unwrap();
    tx.put(vec![0], vec![7]).unwrap();
    Box::new(tx).commit().unwrap();
}

#[test]
fn owned_journal_savepoints_preserve_originals_and_release_discarded_keys() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    seed(&store);
    let mut tx = OwnedLsmTransaction::new(store.clone(), TxnMode::ReadWrite).unwrap();
    assert_eq!(tx.journal_pending_writes(), Some(vec![]));
    assert!(tx.state.lock().unwrap().read_set.is_empty());
    let empty = tx.create_savepoint().unwrap();
    tx.put(vec![0], vec![9]).unwrap();
    let first = tx.create_savepoint().unwrap();
    tx.delete(vec![0]).unwrap();
    tx.put(vec![1], vec![10]).unwrap();
    tx.rollback_to_savepoint(first).unwrap();
    assert_eq!(
        tx.journal_pending_writes(),
        Some(vec![(vec![0], Some(vec![7]), Some(vec![9]))])
    );
    assert_eq!(tx.state.lock().unwrap().journal_before.len(), 1);
    tx.put(vec![0], vec![7]).unwrap();
    assert_eq!(tx.get(&vec![0]).unwrap(), Some(vec![7]));
    assert_eq!(
        tx.journal_pending_writes(),
        Some(vec![(vec![0], Some(vec![7]), Some(vec![7]))])
    );
    tx.release_savepoint(first).unwrap();
    tx.rollback_to_savepoint(empty).unwrap();
    assert!(tx.state.lock().unwrap().journal_before.is_empty());
    assert_eq!(tx.journal_pending_writes(), Some(vec![]));
    tx.put(vec![1], vec![10]).unwrap();
    tx.delete(vec![1]).unwrap();
    assert_eq!(
        tx.journal_pending_writes(),
        Some(vec![(vec![1], None, None)])
    );
    let state = tx.state.clone();
    Box::new(tx).rollback().unwrap();
    assert!(state.lock().unwrap().journal_before.is_empty());
}

#[test]
fn owned_journal_start_snapshot_conflict_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let database = store(dir.path());
    seed(&database);
    let mut older = OwnedLsmTransaction::new(database.clone(), TxnMode::ReadWrite).unwrap();
    let mut newer = OwnedLsmTransaction::new(database.clone(), TxnMode::ReadWrite).unwrap();
    newer.put(vec![0], vec![9]).unwrap();
    Box::new(newer).commit().unwrap();
    older.put(vec![0], vec![10]).unwrap();
    assert_eq!(
        older.journal_pending_writes(),
        Some(vec![(vec![0], Some(vec![7]), Some(vec![10]))])
    );
    assert!(matches!(Box::new(older).commit(), Err(Error::TxnConflict)));
    drop(database);
    let database = store(dir.path());
    let mut read = OwnedLsmTransaction::new(database, TxnMode::ReadOnly).unwrap();
    assert_eq!(read.get(&vec![0]).unwrap(), Some(vec![9]));
    assert!(read.put(vec![0], vec![1]).is_err());
    assert!(read.delete(vec![0]).is_err());
    assert_eq!(read.journal_pending_writes(), Some(vec![]));
}

#[test]
fn owned_journal_persisted_read_failure_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let database = store(dir.path());
    seed(&database);
    let AnyKV::Lsm(lsm) = database.as_ref() else {
        panic!("LSM required")
    };
    lsm.flush().unwrap();
    let file = std::fs::read_dir(dir.path().join("sst"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let bytes = std::fs::read(&file).unwrap();
    std::fs::remove_file(&file).unwrap();
    let mut tx = OwnedLsmTransaction::new(database, TxnMode::ReadWrite).unwrap();
    assert!(tx.put(vec![0], vec![9]).is_err());
    assert!(tx.delete(vec![0]).is_err());
    assert_eq!(tx.journal_pending_writes(), Some(vec![]));
    assert!(tx.state.lock().unwrap().journal_before.is_empty());
    std::fs::write(&file, bytes).unwrap();
    tx.put(vec![0], vec![9]).unwrap();
    assert_eq!(
        tx.journal_pending_writes(),
        Some(vec![(vec![0], Some(vec![7]), Some(vec![9]))])
    );
    Box::new(tx).commit().unwrap();
}
