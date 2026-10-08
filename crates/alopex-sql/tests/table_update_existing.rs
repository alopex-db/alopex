//! Existing TableStorage get/put behavior, before and after existence-aware replacement.
//! These tests deliberately compile without the proposed KVTransaction method.
use std::sync::Arc;

use alopex_core::error::Error;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KVStore, KVTransaction, OwnedKVTransactionAdapter, OwnedSessionFactory};
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{CatalogOverlay, ColumnMetadata, TableMetadata};
use alopex_sql::planner::ResolvedType;
use alopex_sql::storage::{
    KeyEncoder, RowCodec, SqlTxn, SqlValue, StorageError, TableStorage, TxnBridge,
};

fn metadata() -> TableMetadata {
    TableMetadata::new(
        "replacement",
        vec![ColumnMetadata::new("v", ResolvedType::Integer)],
    )
    .with_table_id(42)
}

fn row(value: i32) -> Vec<SqlValue> {
    vec![SqlValue::Integer(value)]
}

fn key(id: u64) -> Vec<u8> {
    KeyEncoder::row_key(42, id)
}

fn encoded(value: i32) -> Vec<u8> {
    RowCodec::encode(&row(value))
}

fn seed(store: &MemoryKV) {
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    TableStorage::new(&mut txn, &metadata())
        .insert(1, &row(10))
        .unwrap();
    txn.commit_self().unwrap();
}

#[test]
fn existing_row_replaced_missing_row_not_inserted() {
    let store = MemoryKV::new();
    seed(&store);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut table = TableStorage::new(&mut txn, &metadata());
    table.update(1, &row(20)).unwrap();
    assert!(matches!(
        table.update(99, &row(20)),
        Err(StorageError::RowNotFound {
            table_id: 42,
            row_id: 99
        })
    ));
    assert_eq!(table.get(1).unwrap(), Some(row(20)));
    assert_eq!(table.get(99).unwrap(), None);
    assert_eq!(
        txn.journal_pending_writes(),
        Some(vec![(key(1), Some(encoded(10)), Some(encoded(20)))])
    );
    txn.commit_self().unwrap();
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        TableStorage::new(&mut read, &metadata()).get(1).unwrap(),
        Some(row(20))
    );
}

#[test]
fn concurrent_commit_before_or_after_replacement_conflicts() {
    for concurrent_first in [false, true] {
        let store = MemoryKV::new();
        seed(&store);
        let mut original = store.begin(TxnMode::ReadWrite).unwrap();
        if !concurrent_first {
            TableStorage::new(&mut original, &metadata())
                .update(1, &row(20))
                .unwrap();
        }
        let mut concurrent = store.begin(TxnMode::ReadWrite).unwrap();
        TableStorage::new(&mut concurrent, &metadata())
            .update(1, &row(30))
            .unwrap();
        concurrent.commit_self().unwrap();
        if concurrent_first {
            TableStorage::new(&mut original, &metadata())
                .update(1, &row(20))
                .unwrap();
        }
        assert!(matches!(original.commit_self(), Err(Error::TxnConflict)));
        let mut read = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(read.get(&key(1)).unwrap(), Some(encoded(30)));
    }
}

#[test]
fn repeated_update_delete_put_preserves_first_before_image() {
    let store = MemoryKV::new();
    seed(&store);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    for value in [20, 30] {
        TableStorage::new(&mut txn, &metadata())
            .update(1, &row(value))
            .unwrap();
    }
    txn.delete(key(1)).unwrap();
    txn.put(key(1), encoded(40)).unwrap();
    TableStorage::new(&mut txn, &metadata())
        .update(1, &row(50))
        .unwrap();
    assert_eq!(
        txn.journal_pending_writes(),
        Some(vec![(key(1), Some(encoded(10)), Some(encoded(50)))])
    );
    txn.rollback_self().unwrap();
}

#[test]
fn pending_insert_and_delete_keep_existence_and_original_absence() {
    let store = MemoryKV::new();
    seed(&store);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    txn.put(key(2), encoded(20)).unwrap();
    TableStorage::new(&mut txn, &metadata())
        .update(2, &row(30))
        .unwrap();
    txn.delete(key(1)).unwrap();
    assert!(matches!(
        TableStorage::new(&mut txn, &metadata()).update(1, &row(40)),
        Err(StorageError::RowNotFound {
            table_id: 42,
            row_id: 1
        })
    ));
    assert_eq!(
        txn.journal_pending_writes(),
        Some(vec![
            (key(1), Some(encoded(10)), None),
            (key(2), None, Some(encoded(30)))
        ])
    );
    txn.rollback_self().unwrap();
}

#[test]
fn borrowed_bridge_replacement_rolls_back_all_rows() {
    let store = MemoryKV::new();
    seed(&store);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    {
        let mut borrowed =
            TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let (mut sql, _) = borrowed.split_parts();
        // This real borrowed bridge monomorphizes TableStorage with MemoryTransaction.
        // The after-build bounded symbol-count check owns proof of override execution.
        let mut table = sql.table_storage(&metadata());
        table.update(1, &row(20)).unwrap();
        table.update(1, &row(30)).unwrap();
        table.insert(2, &row(40)).unwrap();
        table.update(2, &row(50)).unwrap();
    }
    txn.rollback_self().unwrap();
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read.get(&key(1)).unwrap(), Some(encoded(10)));
    assert_eq!(read.get(&key(2)).unwrap(), None);
}

#[test]
fn validation_precedes_missing_and_read_only_errors() {
    let store = MemoryKV::new();
    seed(&store);
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut table = TableStorage::new(&mut txn, &metadata());
    for id in [1, 99] {
        assert!(matches!(
            table.update(id, &[]),
            Err(StorageError::TypeMismatch { .. })
        ));
    }
    assert!(matches!(
        table.update(99, &row(20)),
        Err(StorageError::RowNotFound {
            table_id: 42,
            row_id: 99
        })
    ));
    assert!(matches!(
        table.update(1, &row(20)),
        Err(StorageError::TransactionReadOnly)
    ));
    assert_eq!(txn.journal_pending_writes(), Some(vec![]));
}

#[test]
fn flushed_sstable_replacement_retains_before_image() {
    // Standard-library temporary directory keeps this focused target dependency-minimal.
    let path =
        std::env::temp_dir().join(format!("v0816-570-existing-update-{}", std::process::id()));
    std::fs::create_dir(&path).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    let _cleanup = Cleanup(path.clone());
    let wal = path.join("wal.log");
    {
        let store = MemoryKV::open(&wal).unwrap();
        seed(&store);
        store.flush().unwrap();
    }
    assert!(wal.with_extension("sst").is_file());
    let store = MemoryKV::open(&wal).unwrap();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    TableStorage::new(&mut txn, &metadata())
        .update(1, &row(20))
        .unwrap();
    assert_eq!(
        txn.journal_pending_writes(),
        Some(vec![(key(1), Some(encoded(10)), Some(encoded(20)))])
    );
    txn.commit_self().unwrap();
    drop(store);
    let reopened = MemoryKV::open(&wal).unwrap();
    let mut read = reopened.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read.get(&key(1)).unwrap(), Some(encoded(20)));
}

#[test]
fn owned_default_fallback_preserves_journal_and_closed_session() {
    let store = Arc::new(MemoryKV::new());
    seed(&store);
    let session = store
        .clone()
        .begin_owned_transaction(TxnMode::ReadWrite)
        .unwrap();
    let lease = session.acquire_lease().unwrap();
    lease
        .with_transaction(|inner| {
            let mut txn = OwnedKVTransactionAdapter::new(inner);
            let mut table = TableStorage::new(&mut txn, &metadata());
            table.update(1, &row(20)).unwrap();
            table.update(1, &row(30)).unwrap();
            assert!(matches!(
                table.update(99, &row(40)),
                Err(StorageError::RowNotFound {
                    table_id: 42,
                    row_id: 99
                })
            ));
            assert_eq!(
                txn.journal_pending_writes(),
                Some(vec![(key(1), Some(encoded(10)), Some(encoded(30)))])
            );
            Ok(())
        })
        .unwrap();
    lease
        .finish(alopex_core::txn::OwnedLeaseOutcome::Exhausted)
        .unwrap();
    session.rollback().unwrap();
    assert!(matches!(session.acquire_lease(), Err(Error::TxnClosed)));
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(read.get(&key(1)).unwrap(), Some(encoded(10)));
}
