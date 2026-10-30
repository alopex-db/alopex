//! Small behavioral contracts for borrowed LSM prefix/range reconstruction.
use super::*;
use proptest::prelude::*;

fn store(dir: &tempfile::TempDir) -> LsmKV {
    let config = LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 8,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: false,
            ..Default::default()
        },
        ..Default::default()
    };
    LsmKV::open_with_config(dir.path(), config).unwrap().0
}

fn write(store: &LsmKV, entries: &[(&[u8], Option<&[u8]>)]) {
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    for (key, value) in entries {
        match value {
            Some(value) => tx.put(key.to_vec(), value.to_vec()).unwrap(),
            None => tx.delete(key.to_vec()).unwrap(),
        }
    }
    tx.commit_self().unwrap();
}

fn scan<'a>(
    tx: &'a mut LsmTransaction<'_>,
    prefix: bool,
) -> Box<dyn Iterator<Item = (Key, Value)> + 'a> {
    if prefix {
        tx.scan_prefix(b"p:").unwrap()
    } else {
        tx.scan_range(b"p:", b"q:").unwrap()
    }
}

#[test]
fn snapshot_overlay_and_tombstones_preserve_full_read_set() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    write(
        &store,
        &[
            (b"p:a", Some(b"old")),
            (b"p:b", Some(b"b")),
            (b"z", Some(b"outside")),
        ],
    );
    write(&store, &[(b"p:b", None)]);
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    write(&store, &[(b"p:a", Some(b"new")), (b"p:c", Some(b"future"))]);
    for prefix in [true, false] {
        assert_eq!(
            scan(&mut tx, prefix).collect::<Vec<_>>(),
            vec![(b"p:a".to_vec(), b"old".to_vec())]
        );
    }
    tx.put(b"p:a".to_vec(), b"overlay".to_vec()).unwrap();
    tx.put(b"p:b".to_vec(), b"resurrected".to_vec()).unwrap();
    tx.put(b"p:c".to_vec(), b"local".to_vec()).unwrap();
    tx.delete(b"p:d".to_vec()).unwrap();
    tx.delete(b"p:a".to_vec()).unwrap();
    for prefix in [true, false] {
        assert_eq!(
            scan(&mut tx, prefix).collect::<Vec<_>>(),
            vec![
                (b"p:b".to_vec(), b"resurrected".to_vec()),
                (b"p:c".to_vec(), b"local".to_vec()),
            ]
        );
    }
    assert_eq!(
        tx.read_set,
        [
            b"p:a".to_vec(),
            b"p:b".to_vec(),
            b"p:c".to_vec(),
            b"p:d".to_vec()
        ]
        .into_iter()
        .collect()
    );
    tx.rollback_self().unwrap();
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        scan(&mut read, false).collect::<Vec<_>>(),
        vec![
            (b"p:a".to_vec(), b"new".to_vec()),
            (b"p:c".to_vec(), b"future".to_vec()),
        ]
    );
}

fn conflict_after_early_drop(prefix: bool, consume: usize, tombstone: bool) {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    write(
        &store,
        &[
            (b"p:a", Some(b"a")),
            (b"p:z", if tombstone { None } else { Some(b"z") }),
        ],
    );
    let mut reader = store.begin(TxnMode::ReadWrite).unwrap();
    assert_eq!(scan(&mut reader, prefix).take(consume).count(), consume);
    // Even an unconsumed key or visible tombstone was eagerly observed before
    // the iterator was returned. Dropping it must not weaken conflict checks.
    write(&store, &[(b"p:z", Some(b"changed"))]);
    reader.put(b"q:write".to_vec(), b"value".to_vec()).unwrap();
    assert!(matches!(reader.commit_self(), Err(Error::TxnConflict)));
    assert_eq!(
        store
            .begin(TxnMode::ReadOnly)
            .unwrap()
            .get(&b"q:write".to_vec())
            .unwrap(),
        None
    );
}

#[test]
fn prefix_early_drop_keeps_unconsumed_and_tombstone_conflicts() {
    for consume in [0, 1] {
        for tombstone in [false, true] {
            conflict_after_early_drop(true, consume, tombstone);
        }
    }
}

#[test]
fn range_early_drop_keeps_unconsumed_and_tombstone_conflicts() {
    for consume in [0, 1] {
        for tombstone in [false, true] {
            conflict_after_early_drop(false, consume, tombstone);
        }
    }
}

#[test]
fn binary_prefix_and_half_open_range_keep_exact_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    write(
        &store,
        &[
            (b"", Some(b"empty")),
            (b"p:", Some(b"prefix")),
            (b"p:\0", Some(b"nul")),
            (b"p:\xff", Some(b"ff")),
            (b"q:", Some(b"end")),
            (b"\xff", Some(b"last")),
        ],
    );
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    let expected = vec![
        (b"p:".to_vec(), b"prefix".to_vec()),
        (b"p:\0".to_vec(), b"nul".to_vec()),
        (b"p:\xff".to_vec(), b"ff".to_vec()),
    ];
    assert_eq!(scan(&mut read, true).collect::<Vec<_>>(), expected);
    assert_eq!(scan(&mut read, false).collect::<Vec<_>>(), expected);
    assert!(read.scan_range(b"p:", b"p:").unwrap().next().is_none());
    assert_eq!(
        read.scan_prefix(b"\xff").unwrap().collect::<Vec<_>>(),
        vec![(vec![255], b"last".to_vec())]
    );
    assert_eq!(read.scan_prefix(b"").unwrap().count(), 6);
}

#[test]
fn active_immutable_and_sstable_merge_at_the_reader_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    write(&store, &[(b"p:a", Some(b"sst")), (b"p:b", Some(b"sst-b"))]);
    store.flush().unwrap();
    write(
        &store,
        &[
            (b"p:a", Some(b"immutable")),
            (b"p:b", None),
            (b"p:c", Some(b"immutable-c")),
        ],
    );
    let mut old = store.begin(TxnMode::ReadOnly).unwrap();
    let frozen = std::mem::take(&mut *store.active_memtable.write().unwrap()).freeze();
    store
        .immutable_memtables
        .write()
        .unwrap()
        .push_back(Arc::new(frozen));
    write(&store, &[(b"p:a", Some(b"active")), (b"p:c", None)]);
    for prefix in [true, false] {
        assert_eq!(
            scan(&mut old, prefix).collect::<Vec<_>>(),
            vec![
                (b"p:a".to_vec(), b"immutable".to_vec()),
                (b"p:c".to_vec(), b"immutable-c".to_vec()),
            ]
        );
        let mut current = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(
            scan(&mut current, prefix).collect::<Vec<_>>(),
            vec![(b"p:a".to_vec(), b"active".to_vec())]
        );
    }
}

#[test]
fn version_ties_preserve_sequence_tombstone_and_source_priority() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let immutable = MemTable::new();
    let active = store.active_memtable.read().unwrap();
    active.put(b"p:a".to_vec(), vec![1], 3, 2);
    immutable.put(b"p:a".to_vec(), vec![2], 3, 3);
    active.put(b"p:b".to_vec(), vec![3], 3, 2);
    immutable.put(b"p:b".to_vec(), vec![4], 2, 9);
    active.delete(b"p:c".to_vec(), 3, 3);
    immutable.put(b"p:c".to_vec(), vec![5], 3, 2);
    active.put(b"p:d".to_vec(), vec![6], 3, 2);
    immutable.put(b"p:d".to_vec(), vec![7], 3, 2);
    drop(active);
    store
        .immutable_memtables
        .write()
        .unwrap()
        .push_back(Arc::new(immutable.freeze()));
    for map in [
        store.scan_prefix_visible(b"p:", 3),
        store.scan_range_visible(b"p:", b"q:", 3),
    ] {
        assert_eq!(map.len(), 4);
        assert_eq!(map[b"p:a".as_slice()].value, Some(vec![2]));
        assert_eq!(map[b"p:b".as_slice()].value, Some(vec![3]));
        assert_eq!(map[b"p:c".as_slice()].value, None);
        assert_eq!(map[b"p:d".as_slice()].value, Some(vec![6]));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn visible_scan_matches_small_version_reference(
        versions in prop::collection::vec((0u8..6, 0u64..5, prop::option::of(any::<u8>()), any::<bool>()), 0..20),
        read_timestamp in 0u64..5,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let immutable = MemTable::new();
        let mut model: BTreeMap<Key, MemTableEntry> = BTreeMap::new();
        for (sequence, (key, timestamp, value, active)) in versions.into_iter().enumerate() {
            let sequence = sequence as u64;
            let key = vec![b'p', b':', key];
            let entry = MemTableEntry { value: value.map(|v| vec![v]), timestamp, sequence };
            let guard = store.active_memtable.read().unwrap();
            let table = if active { &*guard } else { &immutable };
            if let Some(value) = &entry.value { table.put(key.clone(), value.clone(), timestamp, sequence); }
            else { table.delete(key.clone(), timestamp, sequence); }
            // A unique sequence per input removes source-priority ambiguity.
            if timestamp <= read_timestamp {
                let replace = model.get(&key).is_none_or(|old| (timestamp, sequence) > (old.timestamp, old.sequence));
                if replace { model.insert(key, entry); }
            }
        }
        store.immutable_memtables.write().unwrap().push_back(Arc::new(immutable.freeze()));
        let prefix = store.scan_prefix_visible(b"p:", read_timestamp);
        let range = store.scan_range_visible(b"p:", b"q:", read_timestamp);
        prop_assert_eq!(&prefix, &range);
        prop_assert_eq!(prefix, model);
    }
}
