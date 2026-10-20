use std::sync::Arc;

use crate::ast::{IndexMethod, VectorMetric};
use crate::catalog::{CatalogOverlay, ColumnMetadata, IndexMetadata, TableMetadata};
use crate::executor::hnsw_bridge::HnswBridge;
use crate::planner::ResolvedType;
use crate::storage::{SqlTxn, SqlValue, TxnBridge};
use alopex_core::kv::{KVStore, memory::MemoryKV};
use alopex_core::{HnswIndex, TxnMode};

fn metadata(name: &str) -> (TableMetadata, IndexMetadata) {
    (
        TableMetadata::new(
            "items",
            vec![ColumnMetadata::new(
                "e",
                ResolvedType::Vector {
                    dimension: 2,
                    metric: VectorMetric::L2,
                },
            )],
        ),
        IndexMetadata::new(1, name, "items", vec!["e".into()])
            .with_column_indices(vec![0])
            .with_method(IndexMethod::Hnsw),
    )
}

fn staged_insert<'a>(txn: &mut impl SqlTxn<'a, MemoryKV>, name: &str) {
    let (table, index) = metadata(name);
    HnswBridge::on_insert_batch(
        txn,
        &table,
        &index,
        &[(1, vec![SqlValue::Vector(vec![1.0, 0.0])])],
    )
    .unwrap();
}

fn recreate<'a>(txn: &mut impl SqlTxn<'a, MemoryKV>) {
    let (table, index) = metadata("ih");
    HnswBridge::create_index(txn, &table, &index).unwrap();
    staged_insert(txn, "ih");
    txn.flush_hnsw().unwrap();
    assert_eq!(txn.hnsw_entry("ih").unwrap().stats().node_count, 1);
    // No table rows: rebuilding must discard the cached one-node graph.
    HnswBridge::create_index(txn, &table, &index).unwrap();
    assert_eq!(txn.hnsw_entry("ih").unwrap().stats().node_count, 0);
    staged_insert(txn, "ih");
    txn.flush_hnsw().unwrap();
    assert_eq!(
        HnswIndex::load("ih", txn.inner_mut())
            .unwrap()
            .stats()
            .node_count,
        1
    );
}

fn drop_preserves_other_staged_index<'a>(txn: &mut impl SqlTxn<'a, MemoryKV>) {
    for name in ["ih", "other"] {
        let (table, index) = metadata(name);
        HnswBridge::create_index(txn, &table, &index).unwrap();
        staged_insert(txn, name);
    }
    let (_, index) = metadata("ih");
    HnswBridge::drop_index(txn, &index, false).unwrap();
    assert!(
        txn.hnsw_entry("ih").is_err(),
        "dropped cache entry survived"
    );
    txn.flush_hnsw().unwrap();
    assert!(HnswIndex::load("ih", txn.inner_mut()).is_err());
    assert_eq!(
        HnswIndex::load("other", txn.inner_mut())
            .unwrap()
            .stats()
            .node_count,
        1,
        "DDL must flush unrelated staged writes before clearing its cache"
    );
}

#[test]
fn owned_recreate_invalidates_cached_graph() {
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    recreate(&mut bridge.begin_write().unwrap());
}

#[test]
fn owned_drop_invalidates_cache_and_preserves_other_writes() {
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    drop_preserves_other_staged_index(&mut bridge.begin_write().unwrap());
}

#[test]
fn drop_missing_index_obeys_if_exists() {
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let mut txn = bridge.begin_write().unwrap();
    let (_, index) = metadata("missing");
    HnswBridge::drop_index(&mut txn, &index, true).unwrap();
    assert!(matches!(
        HnswBridge::drop_index(&mut txn, &index, false),
        Err(crate::executor::ExecutorError::Core(
            alopex_core::Error::IndexNotFound { .. }
        ))
    ));
}

#[test]
fn borrowed_recreate_invalidates_cached_graph() {
    let store = MemoryKV::new();
    let mut raw = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut raw, TxnMode::ReadWrite, &mut overlay);
    recreate(&mut borrowed.split_parts().0);
}

#[test]
fn borrowed_drop_invalidates_cache_and_preserves_other_writes() {
    let store = MemoryKV::new();
    let mut raw = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut raw, TxnMode::ReadWrite, &mut overlay);
    drop_preserves_other_staged_index(&mut borrowed.split_parts().0);
}
