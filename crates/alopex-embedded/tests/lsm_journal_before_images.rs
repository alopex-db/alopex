//! Minimum journal contract for borrowed and Python's owned LSM route.
use alopex_core::kv::{
    AnyKV, JournalPendingWrite, KVStore, KVTransaction, OwnedKVTransactionAdapter,
    OwnedSessionFactory, RangeChangePayload,
};
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_core::types::{Key, TxnId, TxnMode, Value};
use alopex_core::Result;
use alopex_sql::storage::{KeyEncoder, LocalRangeChangeJournal, RangeChangeJournalScope};
use std::sync::Arc;

struct NoScan<T>(T);
impl<'a, T: KVTransaction<'a>> KVTransaction<'a> for NoScan<T> {
    fn id(&self) -> TxnId {
        self.0.id()
    }
    fn mode(&self) -> TxnMode {
        self.0.mode()
    }
    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.0.get(key)
    }
    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.0.put(key, value)
    }
    fn delete(&mut self, key: Key) -> Result<()> {
        self.0.delete(key)
    }
    fn scan_prefix(&mut self, _: &[u8]) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        panic!("journal must not scan")
    }
    fn scan_range(
        &mut self,
        _: &[u8],
        _: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        panic!("journal must not scan")
    }
    fn scan_from(&mut self, _: &[u8]) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        panic!("journal must not scan")
    }
    fn journal_pending_writes(&self) -> Option<Vec<JournalPendingWrite>> {
        self.0.journal_pending_writes()
    }
    fn commit_self(self) -> Result<()> {
        self.0.commit_self()
    }
    fn rollback_self(self) -> Result<()> {
        self.0.rollback_self()
    }
}

fn exercise<'a>(tx: &mut impl KVTransaction<'a>, restore: bool) {
    let row = KeyEncoder::row_key(1, 1);
    let scope = RangeChangeJournalScope::local(Default::default());
    let journal = LocalRangeChangeJournal::capture(tx, scope).unwrap();
    tx.put(row.clone(), b"new".to_vec()).unwrap();
    tx.delete(row.clone()).unwrap();
    tx.put(
        row.clone(),
        if restore {
            b"old".to_vec()
        } else {
            b"final".to_vec()
        },
    )
    .unwrap();
    let record = journal.stage(tx).unwrap();
    if restore {
        assert!(record.is_none());
    } else {
        assert_eq!(
            record.unwrap().payload,
            vec![RangeChangePayload::UpsertRow {
                row_key: row,
                encoded_row: b"final".to_vec()
            }]
        );
    }
}

#[test]
fn lsm_journal_borrowed_and_owned_never_scan_for_capture_or_stage() {
    for owned in [false, true] {
        for restore in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let config = LsmKVConfig {
                wal: WalConfig {
                    segment_size: 4096,
                    max_segments: 2,
                    sync_mode: SyncMode::EveryWrite,
                },
                prune_sidecar_on_drop: false,
                ..Default::default()
            };
            let store = Arc::new(AnyKV::Lsm(Box::new(
                LsmKV::open_with_config(dir.path(), config.clone())
                    .unwrap()
                    .0,
            )));
            let mut seed = store.begin(TxnMode::ReadWrite).unwrap();
            seed.put(KeyEncoder::row_key(1, 1), b"old".to_vec())
                .unwrap();
            seed.put(KeyEncoder::row_key(1, 2), b"untouched".to_vec())
                .unwrap();
            seed.commit_self().unwrap();
            if owned {
                let tx = store
                    .clone()
                    .begin_owned_transaction(TxnMode::ReadWrite)
                    .unwrap();
                tx.with_transaction(|tx| {
                    exercise(&mut NoScan(OwnedKVTransactionAdapter::new(tx)), restore);
                    Ok(())
                })
                .unwrap();
                tx.commit().unwrap();
            } else {
                let mut tx = NoScan(store.begin(TxnMode::ReadWrite).unwrap());
                exercise(&mut tx, restore);
                tx.commit_self().unwrap();
            }
            drop(store);
            let (store, _) = LsmKV::open_with_config(dir.path(), config).unwrap();
            let mut read = store.begin(TxnMode::ReadOnly).unwrap();
            assert_eq!(
                read.get(&KeyEncoder::row_key(1, 1)).unwrap(),
                Some(if restore {
                    b"old".to_vec()
                } else {
                    b"final".to_vec()
                })
            );
            assert_eq!(
                read.get(&KeyEncoder::row_key(1, 2)).unwrap(),
                Some(b"untouched".to_vec())
            );
        }
    }
}
