//! SQL ストレージ層の Disk モードテスト（LSM-Tree）。

use std::sync::Arc;

use alopex_core::lsm::LsmKVConfig;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::types::TxnMode;
use alopex_core::{StorageFactory, StorageMode};

use crate::SqlValue;
use crate::catalog::{ColumnMetadata, TableMetadata};
use crate::planner::types::ResolvedType;
use crate::storage::{IndexStorage, TableStorage, TxnBridge};

fn disk_store() -> Arc<alopex_core::kv::AnyKV> {
    let dir = tempfile::tempdir().unwrap().keep();
    let cfg = LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::NoSync,
        },
        ..Default::default()
    };
    Arc::new(
        StorageFactory::create(StorageMode::Disk {
            path: dir,
            config: Some(cfg),
        })
        .unwrap(),
    )
}

#[test]
fn table_storage_crud_and_scan_work_in_disk_mode() {
    let store = disk_store();
    let bridge = TxnBridge::new(store);

    let table = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
        ],
    )
    .with_table_id(1)
    .with_primary_key(vec!["id".into()]);

    let mut txn = bridge.begin_write().unwrap();
    {
        let mut t = TableStorage::new(txn.inner_mut(), &table);
        t.insert(1, &[SqlValue::Integer(1), SqlValue::Text("a".into())])
            .unwrap();
        t.insert(2, &[SqlValue::Integer(2), SqlValue::Text("b".into())])
            .unwrap();
        assert_eq!(
            t.get(1).unwrap(),
            Some(vec![SqlValue::Integer(1), SqlValue::Text("a".into())])
        );
        t.update(2, &[SqlValue::Integer(2), SqlValue::Text("bb".into())])
            .unwrap();
        t.delete(1).unwrap();

        let rows: Vec<_> = t.scan().unwrap().collect();
        assert_eq!(rows.len(), 1);
        let (row_id, row) = rows[0].as_ref().unwrap().clone();
        assert_eq!(row_id, 2);
        assert_eq!(row, vec![SqlValue::Integer(2), SqlValue::Text("bb".into())]);
    }
    txn.commit().unwrap();

    let mut ro = bridge.begin_read().unwrap();
    {
        let mut t = TableStorage::new(ro.inner_mut(), &table);
        assert!(t.get(1).unwrap().is_none());
        assert!(t.get(2).unwrap().is_some());
    }
    ro.commit().unwrap();
}

#[test]
fn index_storage_put_and_scan_work_in_disk_mode() {
    let store = disk_store();
    let bridge = TxnBridge::new(store);

    let mut txn = bridge.begin_write().unwrap();
    {
        // index_id=1, unique=true, column_indices=[0]
        let mut idx = IndexStorage::new(txn.inner_mut(), 1, true, vec![0]);
        idx.insert(&[SqlValue::Integer(10)], 100).unwrap();
        idx.insert(&[SqlValue::Integer(20)], 200).unwrap();
        assert_eq!(idx.lookup(&SqlValue::Integer(10)).unwrap(), vec![100]);
        assert_eq!(idx.lookup(&SqlValue::Integer(20)).unwrap(), vec![200]);
    }
    txn.commit().unwrap();
}

#[test]
fn can_use_txn_bridge_on_disk_store() {
    let store = disk_store();
    let bridge = TxnBridge::new(store);
    let tx = bridge.begin_write().unwrap();
    assert_eq!(tx.mode(), TxnMode::ReadWrite);
    tx.commit().unwrap();
}
