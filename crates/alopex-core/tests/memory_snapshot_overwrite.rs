//! Minimal regression for versions replaced after a transaction begins.
#![cfg(not(target_arch = "wasm32"))]

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

#[test]
fn generations_are_retained_and_reclaimed_independently() {
    let store = seeded();
    let mut oldest = store.begin(TxnMode::ReadOnly).unwrap();
    replace(&store, false);
    let mut middle = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadOnly)
        .unwrap();
    replace(&store, true);
    assert_eq!(oldest.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    assert_eq!(middle.get(&b"k".to_vec()).unwrap(), Some(b"new".to_vec()));
    let both = store.memory_stats().kv_bytes;
    oldest.rollback_self().unwrap();
    assert!(store.memory_stats().kv_bytes < both);
    assert_eq!(middle.get(&b"k".to_vec()).unwrap(), Some(b"new".to_vec()));
    middle.rollback().unwrap();
    assert_eq!(store.memory_stats().kv_bytes, 0);
}

#[test]
fn snapshot_capacity_failure_is_atomic_and_own_version_does_not_double_charge() {
    let store = MemoryKV::new_with_limit(Some(4));
    let mut seed = store.begin(TxnMode::ReadWrite).unwrap();
    seed.put(b"k".to_vec(), b"old".to_vec()).unwrap();
    seed.commit_self().unwrap();
    // No other reader: replacing a value of equal size fits the existing limit.
    replace(&store, false);
    let mut old = store.begin(TxnMode::ReadOnly).unwrap();
    let mut writer = store.begin(TxnMode::ReadWrite).unwrap();
    writer.put(b"k".to_vec(), b"bad".to_vec()).unwrap();
    assert!(matches!(
        writer.commit_self(),
        Err(alopex_core::Error::MemoryLimitExceeded { .. })
    ));
    assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"new".to_vec()));
    assert_eq!(store.memory_stats().kv_bytes, 4);
    old.commit_self().unwrap();
    replace(&store, false);
}

#[test]
fn deleted_key_still_conflicts_with_borrowed_stale_writer() {
    let store = seeded();
    let mut stale = store.begin(TxnMode::ReadWrite).unwrap();
    stale.get(&b"k".to_vec()).unwrap();
    replace(&store, true);
    stale.put(b"k".to_vec(), b"bad".to_vec()).unwrap();
    assert!(matches!(
        stale.commit_self(),
        Err(alopex_core::Error::TxnConflict)
    ));
    assert_eq!(store.memory_stats().kv_bytes, 0);
}

#[test]
fn deleted_key_still_conflicts_with_owned_stale_writer() {
    let store = seeded();
    let mut stale = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadWrite)
        .unwrap();
    stale.get(&b"k".to_vec()).unwrap();
    replace(&store, true);
    stale.put(b"k".to_vec(), b"bad".to_vec()).unwrap();
    assert!(matches!(
        stale.commit(),
        Err(alopex_core::Error::TxnConflict)
    ));
    assert_eq!(store.memory_stats().kv_bytes, 0);
}

#[test]
fn owned_prefix_range_and_from_read_same_old_generation() {
    let store = seeded();
    let mut old = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadOnly)
        .unwrap();
    replace(&store, true);
    replace(&store, false);
    let mut writer = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadWrite)
        .unwrap();
    writer.put(b"later".to_vec(), b"x".to_vec()).unwrap();
    writer.commit().unwrap();
    for kind in 0..3 {
        let mut cursor = match kind {
            0 => old.scan_prefix(b"k").unwrap(),
            1 => old.scan_range(b"k", b"z").unwrap(),
            _ => old.scan_from(b"k").unwrap(),
        };
        assert_eq!(
            cursor.next_entry().unwrap(),
            Some((b"k".to_vec(), b"old".to_vec()))
        );
        assert_eq!(cursor.next_entry().unwrap(), None);
        cursor.close().unwrap();
    }
    old.commit().unwrap();
    assert_eq!(store.memory_stats().kv_bytes, 10); // k:new and later:x
}

#[test]
fn closed_owned_cursor_does_not_pin_rolled_back_transaction() {
    let store = seeded();
    let mut old = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadOnly)
        .unwrap();
    let mut cursor = old.scan_prefix(b"k").unwrap();
    cursor.close().unwrap();
    replace(&store, true);
    old.rollback().unwrap();
    assert_eq!(store.memory_stats().kv_bytes, 0);
    drop(cursor);
}

#[test]
fn deleted_sstable_value_is_not_resurrected_before_or_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.wal");
    {
        let store = MemoryKV::open(&path).unwrap();
        let mut seed = store.begin(TxnMode::ReadWrite).unwrap();
        seed.put(b"k".to_vec(), b"old".to_vec()).unwrap();
        seed.commit_self().unwrap();
        store.flush().unwrap();
        let mut old = store.begin(TxnMode::ReadOnly).unwrap();
        replace(&store, true);
        assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
        drop(old);
        assert_eq!(
            store
                .begin(TxnMode::ReadOnly)
                .unwrap()
                .get(&b"k".to_vec())
                .unwrap(),
            None
        );
    }
    let store = MemoryKV::open(&path).unwrap();
    assert_eq!(
        store
            .begin(TxnMode::ReadOnly)
            .unwrap()
            .get(&b"k".to_vec())
            .unwrap(),
        None
    );
}

#[test]
fn administrative_clear_closes_old_borrowed_transactions_and_restarts_versions() {
    let store = seeded();
    let mut old = store.begin(TxnMode::ReadWrite).unwrap();
    store.txn_manager().clear_all();
    assert!(matches!(
        old.get(&b"k".to_vec()),
        Err(alopex_core::Error::TxnClosed)
    ));
    assert!(matches!(
        old.scan_prefix(b"k"),
        Err(alopex_core::Error::TxnClosed)
    ));
    assert!(matches!(
        old.scan_range(b"k", b"z"),
        Err(alopex_core::Error::TxnClosed)
    ));
    assert!(matches!(
        old.scan_from(b"k"),
        Err(alopex_core::Error::TxnClosed)
    ));
    replace(&store, false);
    assert!(matches!(
        old.commit_self(),
        Err(alopex_core::Error::TxnClosed)
    ));
    assert_eq!(store.memory_stats().kv_bytes, 4);
    assert_eq!(
        store
            .begin(TxnMode::ReadOnly)
            .unwrap()
            .get(&b"k".to_vec())
            .unwrap(),
        Some(b"new".to_vec())
    );
}

#[test]
fn administrative_clear_closes_open_owned_cursor_without_waiting_for_it() {
    let store = seeded();
    let mut old = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadOnly)
        .unwrap();
    let mut cursor = old.scan_prefix(b"k").unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let writer_store = store.clone();
    let writer = std::thread::spawn(move || {
        writer_store.txn_manager().clear_all();
        replace(&writer_store, false);
        sender.send(()).unwrap();
    });
    let completed = receiver.recv_timeout(std::time::Duration::from_secs(5));
    if completed.is_ok() {
        assert!(matches!(
            cursor.next_entry(),
            Err(alopex_core::Error::TxnClosed)
        ));
    }
    // Always release the old gate before joining, so a regression fails within
    // the deadline instead of leaving an unbounded waiting worker.
    cursor.close().unwrap();
    drop(cursor);
    writer.join().unwrap();
    assert!(
        completed.is_ok(),
        "reset/new writer waited for an invalidated cursor"
    );
    assert!(matches!(
        old.get(&b"k".to_vec()),
        Err(alopex_core::Error::TxnClosed)
    ));
    drop(old);
    assert_eq!(store.memory_stats().kv_bytes, 4);
}

#[test]
fn administrative_clear_rejects_staged_owned_writes() {
    let store = seeded();
    let mut old = store
        .clone()
        .begin_owned_kv_transaction(TxnMode::ReadWrite)
        .unwrap();
    old.put(b"k".to_vec(), b"bad".to_vec()).unwrap();
    store.txn_manager().clear_all();
    replace(&store, false);
    assert!(matches!(old.commit(), Err(alopex_core::Error::TxnClosed)));
    assert_eq!(store.memory_stats().kv_bytes, 4);
    assert_eq!(
        store
            .begin(TxnMode::ReadOnly)
            .unwrap()
            .get(&b"k".to_vec())
            .unwrap(),
        Some(b"new".to_vec())
    );
}

// At most three keys, eight mutations, and three live generations per API.
// This is a reference-model correctness check, not a scale or load workload.
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn snapshots_match_small_reference_maps(
        operations in proptest::collection::vec((0u8..3, proptest::option::of(0u8..4)), 1..9)
    ) {
        use std::collections::BTreeMap;
        let store = Arc::new(MemoryKV::new());
        let mut model = BTreeMap::new();
        let mut readers = Vec::new();
        let mut owned_readers = Vec::new();
        for (i, (key, value)) in operations.into_iter().enumerate() {
            readers.push((store.begin(TxnMode::ReadOnly).unwrap(), model.clone()));
            owned_readers.push((store.clone().begin_owned_kv_transaction(TxnMode::ReadOnly).unwrap(), model.clone()));
            if readers.len() > 3 { readers.remove(0).0.rollback_self().unwrap(); }
            if owned_readers.len() > 3 { owned_readers.remove(0).0.rollback().unwrap(); }
            if i % 2 == 0 {
                let mut writer = store.begin(TxnMode::ReadWrite).unwrap();
                match value {
                    Some(value) => writer.put(vec![key], vec![value]).unwrap(),
                    None => writer.delete(vec![key]).unwrap(),
                }
                // Exercise the manager's consuming commit as well as commit_self.
                alopex_core::TxnManager::commit(&store.txn_manager(), writer).unwrap();
            } else {
                let mut writer = store.clone().begin_owned_kv_transaction(TxnMode::ReadWrite).unwrap();
                match value {
                    Some(value) => writer.put(vec![key], vec![value]).unwrap(),
                    None => writer.delete(vec![key]).unwrap(),
                }
                writer.commit().unwrap();
            }
            match value {
                Some(value) => { model.insert(vec![key], vec![value]); }
                None => { model.remove(&vec![key]); }
            }
            for (reader, expected) in &mut readers {
                for key in 0..3 { proptest::prop_assert_eq!(reader.get(&vec![key]).unwrap(), expected.get(&vec![key]).cloned()); }
                let expected_rows: Vec<_> = expected.iter().map(|(k,v)| (k.clone(),v.clone())).collect();
                proptest::prop_assert_eq!(reader.scan_prefix(b"").unwrap().collect::<Vec<_>>(), expected_rows.clone());
                proptest::prop_assert_eq!(reader.scan_range(&[0], &[3]).unwrap().collect::<Vec<_>>(), expected_rows.clone());
                proptest::prop_assert_eq!(reader.scan_from(&[0]).unwrap().collect::<Vec<_>>(), expected_rows);
            }
            for (reader, expected) in &mut owned_readers {
                for key in 0..3 { proptest::prop_assert_eq!(reader.get(&vec![key]).unwrap(), expected.get(&vec![key]).cloned()); }
                let mut cursor = reader.scan_prefix(b"").unwrap();
                let mut rows = Vec::new();
                while let Some(row) = cursor.next_entry().unwrap() { rows.push(row); }
                proptest::prop_assert_eq!(rows, expected.iter().map(|(k,v)| (k.clone(),v.clone())).collect::<Vec<_>>());
            }
        }
        drop(readers);
        drop(owned_readers);
        proptest::prop_assert_eq!(store.memory_stats().kv_bytes, model.iter().map(|(k,v)| k.len()+v.len()).sum::<usize>());
    }
}

#[test]
fn compaction_preserves_old_versions_and_pending_writes_overlay_them() {
    let store = seeded();
    let mut old = store.begin(TxnMode::ReadWrite).unwrap();
    replace(&store, false);
    assert!(store.txn_manager().compact_in_memory().unwrap());
    assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    old.put(b"k".to_vec(), b"own".to_vec()).unwrap();
    assert_eq!(old.get(&b"k".to_vec()).unwrap(), Some(b"own".to_vec()));
    assert_eq!(
        old.scan_prefix(b"k").unwrap().collect::<Vec<_>>(),
        vec![(b"k".to_vec(), b"own".to_vec())]
    );
    old.rollback_self().unwrap();
    assert_eq!(store.memory_stats().kv_bytes, 4);
}

#[test]
fn identical_reader_versions_remain_until_last_reader_ends() {
    let store = seeded();
    let mut a = store.begin(TxnMode::ReadOnly).unwrap();
    let mut b = store.begin(TxnMode::ReadOnly).unwrap();
    replace(&store, true);
    assert_eq!(a.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    let both = store.memory_stats().kv_bytes;
    drop(a);
    assert_eq!(store.memory_stats().kv_bytes, both);
    assert_eq!(b.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    drop(b);
    assert_eq!(store.memory_stats().kv_bytes, 0);
}

#[test]
fn capacity_includes_reclamation_index_key() {
    let store = MemoryKV::new_with_limit(Some(42));
    let key = vec![b'k'; 32];
    let mut seed = store.begin(TxnMode::ReadWrite).unwrap();
    seed.put(key.clone(), vec![1]).unwrap();
    seed.commit_self().unwrap();
    let mut old = store.begin(TxnMode::ReadOnly).unwrap();
    let mut writer = store.begin(TxnMode::ReadWrite).unwrap();
    writer.put(key.clone(), vec![2]).unwrap();
    assert!(matches!(
        writer.commit_self(),
        Err(alopex_core::Error::MemoryLimitExceeded { .. })
    ));
    assert_eq!(old.get(&key).unwrap(), Some(vec![1]));
    drop(old);
    assert_eq!(store.memory_stats().kv_bytes, 33);
}
