//! Minimal write-local before-image regressions; no load fixtures.
use super::*;
use proptest::prelude::*;

fn config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::EveryWrite,
        },
        prune_sidecar_on_drop: false,
        ..Default::default()
    }
}

fn seed(store: &LsmKV) {
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.put(vec![0], vec![7]).unwrap();
    tx.put(vec![1], vec![8]).unwrap();
    tx.commit_self().unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.delete(vec![1]).unwrap();
    tx.commit_self().unwrap();
}

#[test]
fn borrowed_journal_originals_survive_rewrites_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config()).unwrap();
    seed(&store);
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    assert_eq!(tx.journal_pending_writes(), Some(vec![]));
    assert!(tx.read_set.is_empty());
    tx.put(vec![0], vec![9]).unwrap();
    tx.delete(vec![0]).unwrap();
    tx.put(vec![0], vec![7]).unwrap();
    tx.put(vec![1], vec![3]).unwrap();
    tx.delete(vec![2]).unwrap();
    assert_eq!(tx.get(&vec![1]).unwrap(), Some(vec![3]));
    assert_eq!(
        tx.journal_pending_writes(),
        Some(vec![
            (vec![0], Some(vec![7]), Some(vec![7])),
            (vec![1], None, Some(vec![3])),
            (vec![2], None, None),
        ])
    );
    tx.rollback_in_place().unwrap();
    assert!(tx.journal_before.is_empty());
    assert_eq!(tx.journal_pending_writes(), Some(vec![]));
    tx.put(vec![0], vec![10]).unwrap();
    assert_eq!(
        tx.journal_pending_writes(),
        Some(vec![(vec![0], Some(vec![7]), Some(vec![10]))])
    );
    tx.rollback_self().unwrap();
    drop(store);
    let (store, _) = LsmKV::open_with_config(dir.path(), config()).unwrap();
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read.get(&vec![0]).unwrap(), Some(vec![7]));
    assert_eq!(read.get(&vec![1]).unwrap(), None);
    assert!(read.put(vec![0], vec![0]).is_err());
    assert!(read.delete(vec![0]).is_err());
    assert_eq!(read.journal_pending_writes(), Some(vec![]));
}

#[test]
fn borrowed_journal_uses_start_snapshot_and_preserves_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config()).unwrap();
    seed(&store);
    let mut older = store.begin(TxnMode::ReadWrite).unwrap();
    let mut newer = store.begin(TxnMode::ReadWrite).unwrap();
    newer.put(vec![0], vec![9]).unwrap();
    newer.commit_self().unwrap();
    older.put(vec![0], vec![10]).unwrap();
    assert_eq!(
        older.journal_pending_writes(),
        Some(vec![(vec![0], Some(vec![7]), Some(vec![10]))])
    );
    assert!(matches!(older.commit_self(), Err(Error::TxnConflict)));
    let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(reader.get(&vec![0]).unwrap(), Some(vec![9]));
}

#[test]
fn borrowed_journal_persisted_read_failure_does_not_stage_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = LsmKV::open_with_config(dir.path(), config()).unwrap();
    seed(&store);
    store.flush().unwrap();
    let file = fs::read_dir(&store.sst_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let bytes = fs::read(&file).unwrap();
    fs::remove_file(&file).unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    assert!(tx.put(vec![0], vec![9]).is_err());
    assert!(tx.delete(vec![0]).is_err());
    assert!(tx.write_set.is_empty() && tx.journal_before.is_empty());
    assert_eq!(tx.write_usage.encoded, 0);
    assert_eq!(tx.write_usage.replay, 0);
    fs::write(&file, bytes).unwrap();
    tx.put(vec![0], vec![9]).unwrap();
    assert_eq!(
        tx.journal_pending_writes(),
        Some(vec![(vec![0], Some(vec![7]), Some(vec![9]))])
    );
    tx.commit_self().unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn borrowed_journal_matches_first_snapshot_model(ops in prop::collection::vec((0u8..3, any::<bool>(), 0u8..10), 0..16)) {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = LsmKV::open_with_config(dir.path(), config()).unwrap();
        seed(&store);
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        let mut final_values = BTreeMap::new();
        for (key, put, value) in ops {
            let after = put.then_some(vec![value]);
            if put { tx.put(vec![key], vec![value]).unwrap(); } else { tx.delete(vec![key]).unwrap(); }
            final_values.insert(key, after.clone());
            prop_assert_eq!(tx.get(&vec![key]).unwrap(), after);
            let expected = final_values.iter().map(|(key, after)| (vec![*key], (*key == 0).then_some(vec![7]), after.clone())).collect::<Vec<_>>();
            prop_assert_eq!(tx.journal_pending_writes(), Some(expected));
        }
        tx.rollback_self().unwrap();
    }
}
