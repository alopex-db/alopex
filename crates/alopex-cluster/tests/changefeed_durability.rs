//! Commit-to-journal durability oracle for the v0.9 changefeed boundary.
//!
//! This deliberately exercises `LocalRangeChangeJournal`, which is the SQL
//! storage bridge used at the local SQL commit boundary.  It does not turn a
//! WAL into a feed: the persisted record is read after recovery and only then
//! converted by `JournalEventAdapter`.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use alopex_cluster::{
    FeedIdentity, OperationState, OrderingScope, Placement, PlacementReadiness, PlacementRole,
    RangeIdentity, RetentionWindow, RoutingOutcome, RoutingOutcomeKind,
    changefeed::JournalEventAdapter,
};
use alopex_core::kv::{RangeChangeRecord, decode_range_change, journal_key};
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_core::{KVStore, KVTransaction, MemoryKV, TxnMode};
use alopex_sql::storage::{KeyEncoder, LocalRangeChangeJournal, RangeChangeJournalScope};

fn unique_path(suffix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "alopex-v09-changefeed-durability-{}-{nonce}-{suffix}",
        std::process::id()
    ))
}

fn journal_scope() -> RangeChangeJournalScope {
    RangeChangeJournalScope::local(Default::default())
}

/// Stage an insert first so the next commit is a fully classifiable delete
/// event.  Upsert post-images are intentionally not guessed as inserts or
/// updates by the approved v0.9 adapter.
fn committed_delete<S: KVStore>(store: &S) -> RangeChangeRecord {
    let row_key = KeyEncoder::row_key(7, 42);
    {
        let mut transaction = store.begin(TxnMode::ReadWrite).expect("insert transaction");
        let journal = LocalRangeChangeJournal::capture(&mut transaction, journal_scope())
            .expect("capture SQL bridge before insert");
        transaction
            .put(row_key.clone(), b"committed-row".to_vec())
            .expect("stage SQL row insert");
        assert!(
            journal
                .stage(&mut transaction)
                .expect("stage insert journal")
                .is_some()
        );
        transaction
            .commit_self()
            .expect("fsynced SQL insert commit");
    }

    let mut transaction = store.begin(TxnMode::ReadWrite).expect("delete transaction");
    let journal = LocalRangeChangeJournal::capture(&mut transaction, journal_scope())
        .expect("capture SQL bridge before delete");
    transaction.delete(row_key).expect("stage SQL row delete");
    let record = journal
        .stage(&mut transaction)
        .expect("stage delete journal")
        .expect("delete has one change record");
    transaction
        .commit_self()
        .expect("fsynced SQL delete commit");
    record
}

fn staged_but_uncommitted<S: KVStore>(store: &S) -> RangeChangeRecord {
    let mut transaction = store
        .begin(TxnMode::ReadWrite)
        .expect("uncommitted transaction");
    let journal = LocalRangeChangeJournal::capture(&mut transaction, journal_scope())
        .expect("capture SQL bridge before uncommitted write");
    transaction
        .put(KeyEncoder::row_key(7, 99), b"must-not-recover".to_vec())
        .expect("stage uncommitted SQL row");
    let record = journal
        .stage(&mut transaction)
        .expect("stage uncommitted journal")
        .expect("uncommitted write has a record before commit");
    drop(transaction);
    record
}

fn read_record<S: KVStore>(store: &S, expected: &RangeChangeRecord) -> RangeChangeRecord {
    let mut transaction = store.begin(TxnMode::ReadOnly).expect("read transaction");
    let value = transaction
        .get(&journal_key(expected))
        .expect("read journal key")
        .expect("committed journal record must survive");
    decode_range_change(&value).expect("decode recovered journal record")
}

fn assert_uncommitted_is_absent<S: KVStore>(store: &S, record: &RangeChangeRecord) {
    let mut transaction = store.begin(TxnMode::ReadOnly).expect("read transaction");
    assert!(
        transaction
            .get(&journal_key(record))
            .expect("read uncommitted journal key")
            .is_none(),
        "a transaction without commit_self must not publish a journal record"
    );
    assert!(
        transaction
            .get(&KeyEncoder::row_key(7, 99))
            .expect("read uncommitted SQL row")
            .is_none(),
        "a transaction without commit_self must not publish SQL row data"
    );
}

fn feed_and_routing(record: &RangeChangeRecord) -> (FeedIdentity, RoutingOutcome) {
    let range = RangeIdentity::new(
        "cluster-a",
        7,
        record.range_id.clone(),
        None,
        None,
        record.generation,
        record.epoch,
    );
    let feed = FeedIdentity::new(
        "feed-a",
        range.clone(),
        record.generation,
        Placement::new(
            "node-a",
            vec![],
            PlacementRole::Owner,
            PlacementReadiness::Ready,
            1,
        ),
        OrderingScope::Range,
        RetentionWindow::unbounded(),
        OperationState::Committed,
    )
    .expect("valid feed identity");
    let routing = RoutingOutcome::new(
        RoutingOutcomeKind::SingleRange,
        Some(range),
        1,
        "placement_ready",
    );
    (feed, routing)
}

fn recovered_event_id(record: &RangeChangeRecord) -> (String, u64, u32) {
    let (feed, routing) = feed_and_routing(record);
    let events = JournalEventAdapter
        .adapt(&feed, &routing, record)
        .expect("committed delete record adapts after recovery");
    assert_eq!(events.len(), 1, "one committed delete becomes one event");
    (
        events[0].event_id.clone(),
        events[0].range.data_epoch,
        events[0].checkpoint.payload_ordinal,
    )
}

fn append_torn_tail(path: &Path) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open WAL for torn-tail fixture");
    file.write_all(b"\x01torn-changefeed-tail")
        .expect("append incomplete WAL bytes");
    file.sync_data().expect("persist torn-tail fixture");
}

fn remove_memory_files(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sst"));
}

#[test]
fn memorykv_sql_journal_restart_repairs_torn_tail_without_changing_event_identity() {
    let path = unique_path("memory.wal");
    remove_memory_files(&path);

    let (record, before_restart) = {
        let store = MemoryKV::open(&path).expect("open persistent MemoryKV");
        let record = committed_delete(&store);
        let uncommitted = staged_but_uncommitted(&store);
        assert_uncommitted_is_absent(&store, &uncommitted);
        let event = recovered_event_id(&record);
        (record, event)
    };

    append_torn_tail(&path);

    let store = MemoryKV::open(&path).expect("reopen repairs MemoryKV torn tail");
    let recovered = read_record(&store, &record);
    assert_eq!(
        recovered, record,
        "only the fully committed journal replays"
    );
    assert_eq!(
        recovered_event_id(&recovered),
        before_restart,
        "restart keeps event identity, epoch, and ordinal stable"
    );
    remove_memory_files(&path);
}

#[test]
fn lsm_sql_journal_restart_keeps_fsynced_commit_and_excludes_uncommitted_record() {
    let path = unique_path("lsm");
    let config = LsmKVConfig {
        wal: WalConfig {
            sync_mode: SyncMode::EveryWrite,
            ..WalConfig::default()
        },
        ..LsmKVConfig::default()
    };

    let (record, before_restart) = {
        let (store, _) = LsmKV::open_with_config(&path, config.clone()).expect("open LSM");
        let record = committed_delete(&store);
        let uncommitted = staged_but_uncommitted(&store);
        assert_uncommitted_is_absent(&store, &uncommitted);
        let event = recovered_event_id(&record);
        (record, event)
    };

    let (store, recovery) = LsmKV::open_with_config(&path, config).expect("restart LSM");
    assert!(
        recovery.entries_recovered > 0,
        "restart must replay durable WAL entries"
    );
    let recovered = read_record(&store, &record);
    assert_eq!(recovered, record, "only the fsynced journal record replays");
    assert_eq!(
        recovered_event_id(&recovered),
        before_restart,
        "LSM restart keeps event identity, epoch, and ordinal stable"
    );
    drop(store);
    let _ = fs::remove_dir_all(&path);
}
