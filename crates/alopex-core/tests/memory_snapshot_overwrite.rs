//! Minimal regression for versions replaced after a transaction begins.
use std::sync::Arc;

use alopex_core::kv::OwnedKVStore;
use alopex_core::{KVStore, KVTransaction, MemoryKV, TxnMode};

fn seeded() -> Arc<MemoryKV> {
    let store = Arc::new(MemoryKV::new());
    let mut seed = store.begin(TxnMode::ReadWrite).unwrap();
    seed.put(b"k".to_vec(), b"old".to_vec()).unwrap();
    seed.commit_self().unwrap();
    store
}

fn replace(store: &MemoryKV, delete: bool) {
    let mut writer = store.begin(TxnMode::ReadWrite).unwrap();
    if delete {
        writer.delete(b"k".to_vec()).unwrap();
    } else {
        writer.put(b"k".to_vec(), b"new".to_vec()).unwrap();
    }
    writer.commit_self().unwrap();
}

fn borrowed(delete: bool, scan: bool) {
    let store = seeded();
    let mut old = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    replace(&store, delete);
    if scan {
        assert_eq!(
            old.scan_prefix(b"k").unwrap().collect::<Vec<_>>(),
            vec![(b"k".to_vec(), b"old".to_vec())]
        );
    } else {
        assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    }
}

fn owned(delete: bool) {
    let store = seeded();
    let mut old = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadOnly)
        .unwrap();
    assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    replace(&store, delete);
    assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
}

#[test]
fn borrowed_get_preserves_overwritten_version() {
    borrowed(false, false);
}
#[test]
fn borrowed_get_preserves_deleted_version() {
    borrowed(true, false);
}
#[test]
fn borrowed_scan_preserves_overwritten_version() {
    borrowed(false, true);
}
#[test]
fn borrowed_scan_preserves_deleted_version() {
    borrowed(true, true);
}
#[test]
fn owned_get_preserves_overwritten_version() {
    owned(false);
}
#[test]
fn owned_get_preserves_deleted_version() {
    owned(true);
}
