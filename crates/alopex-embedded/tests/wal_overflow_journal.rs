//! One row and one index entry exercise aggregate accounting of the real SQL
//! journal. The public operation encoder determines fixture budgets; no load.
#![cfg(not(windows))]
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{
    decode_range_change, journal_key, KVStore, KVTransaction, RangeChangeRecord,
};
use alopex_core::lsm::wal::operations::OperationStream;
use alopex_core::lsm::wal::WAL_SEGMENT_HEADER_SIZE;
use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_core::types::TxnMode;
use alopex_embedded::wal_overflow::{
    LsmOverflowConfig, OperationLimits, SpoolLimits, SyncMode, WalConfig,
};
use alopex_sql::storage::{KeyEncoder, LocalRangeChangeJournal, RangeChangeJournalScope};
use alopex_sql::SqlValue;
use std::collections::BTreeMap;

fn stage<'a>(tx: &mut impl KVTransaction<'a>) -> alopex_core::Result<Option<RangeChangeRecord>> {
    let journal =
        LocalRangeChangeJournal::capture(tx, RangeChangeJournalScope::local([(7, 1)].into()))?;
    tx.put(KeyEncoder::row_key(1, 9), b"row".to_vec())?;
    tx.put(
        KeyEncoder::index_key(7, &SqlValue::Integer(1), 9).unwrap(),
        Vec::new(),
    )?;
    journal.stage(tx)
}

fn payload_budget() -> u64 {
    let store = MemoryKV::new();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    stage(&mut tx).unwrap().unwrap();
    let writes: BTreeMap<_, _> = tx
        .scan_prefix(&[])
        .unwrap()
        .map(|(k, v)| (k, Some(v)))
        .collect();
    let limits = OperationLimits::new(4096, 1024, 4096).unwrap();
    let payload = OperationStream::new(&writes, limits)
        .unwrap()
        .payload_bytes();
    // The failing case must exceed only the aggregate, not a single operation.
    for (key, value) in &writes {
        let single = [(key.clone(), value.clone())].into();
        assert!(
            OperationStream::new(&single, limits)
                .unwrap()
                .payload_bytes()
                < payload - 1
        );
    }
    payload
}

fn options(payload: u64) -> LsmOverflowConfig {
    LsmOverflowConfig::new(
        SpoolLimits::new(4096, 8192, 64).unwrap(),
        OperationLimits::new(payload, 1024, 4096).unwrap(),
        16384,
        64,
        8192,
    )
    .with_activation_bytes(4096)
    .with_container_fence_bytes(4096)
}
fn config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
        prune_sidecar_on_drop: false,
        ..Default::default()
    }
}

#[test]
fn sql_journal_growth_is_rejected_before_commit_and_rollback_preserves_prior_data() {
    let dir = tempfile::tempdir().unwrap();
    let options = options(payload_budget() - 1);
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), options).unwrap();
    let mut previous = store.begin(TxnMode::ReadWrite).unwrap();
    previous.put(b"previous".to_vec(), b"ok".to_vec()).unwrap();
    previous.commit_self().unwrap();
    let before = store.overflow_stats().unwrap().unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    assert!(stage(&mut tx).is_err());
    tx.rollback_self().unwrap();
    assert_eq!(store.overflow_stats().unwrap().unwrap(), before);
    drop(store);
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), options).unwrap();
    let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        reader.get(&b"previous".to_vec()).unwrap(),
        Some(b"ok".to_vec())
    );
    assert!(reader.get(&KeyEncoder::row_key(1, 9)).unwrap().is_none());
    assert_eq!(
        reader.scan_prefix(&[]).unwrap().collect::<Vec<_>>(),
        vec![(b"previous".to_vec(), b"ok".to_vec())]
    );
}

#[test]
fn exact_aggregate_budget_replays_row_index_and_sql_journal() {
    let dir = tempfile::tempdir().unwrap();
    let options = options(payload_budget());
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), options).unwrap();
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    let record = stage(&mut tx).unwrap().unwrap();
    tx.commit_self().unwrap();
    drop(store);
    let (store, _) = LsmKV::open_with_overflow(dir.path(), config(), options).unwrap();
    let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        reader.get(&KeyEncoder::row_key(1, 9)).unwrap(),
        Some(b"row".to_vec())
    );
    assert_eq!(
        reader
            .get(&KeyEncoder::index_key(7, &SqlValue::Integer(1), 9).unwrap())
            .unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        decode_range_change(&reader.get(&journal_key(&record)).unwrap().unwrap()).unwrap(),
        record
    );
}
