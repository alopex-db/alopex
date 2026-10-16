#![cfg(all(feature = "tokio", not(target_arch = "wasm32")))]

use std::sync::{Arc, RwLock};

use alopex_core::kv::async_adapter::AsyncKVStoreAdapter;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{AsyncKVStore, AsyncKVTransaction, KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::HnswIndex;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::{AsyncExecutor, ExecutionResult};
use alopex_sql::storage::async_storage::{AsyncSqlTransaction, AsyncTxnBridge};
use tokio::time::{Duration, timeout};

fn assert_only_committed_vector(store: &MemoryKV) {
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut read).unwrap();
    let (hits, _) = index.search(&[0.0, 1.0], 2, Some(8)).unwrap();
    assert_eq!(
        hits.into_iter().map(|hit| hit.key).collect::<Vec<_>>(),
        vec![1u64.to_be_bytes().to_vec()]
    );
    read.commit_self().unwrap();
}

#[tokio::test]
async fn worker_sql_hnsw_commit_persists_and_rollback_discards() {
    let store = Arc::new(MemoryKV::new());
    let adapter = AsyncKVStoreAdapter::from_arc(store.clone(), TxnMode::ReadWrite);
    let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> =
        Arc::new(RwLock::new(MemoryCatalog::new()));
    let txn = adapter.begin_async().await.unwrap();
    assert!(txn.supports_worker_tasks());
    let mut executor = AsyncExecutor::new(AsyncTxnBridge::with_catalog(
        txn,
        TxnMode::ReadWrite,
        catalog.clone(),
    ));
    for sql in [
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2))",
        "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW",
    ] {
        assert!(matches!(
            executor.execute_ddl_async(sql).await.unwrap(),
            ExecutionResult::Success
        ));
    }
    assert!(matches!(
        executor
            .execute_dml_async("INSERT INTO items VALUES (1, [1.0, 0.0])")
            .await
            .unwrap(),
        ExecutionResult::RowsAffected(1)
    ));
    executor.into_inner().async_commit().await.unwrap();
    assert_only_committed_vector(&store);

    let txn = adapter.begin_async().await.unwrap();
    let mut executor = AsyncExecutor::new(AsyncTxnBridge::with_catalog(
        txn,
        TxnMode::ReadWrite,
        catalog,
    ));
    assert!(matches!(
        executor
            .execute_dml_async("INSERT INTO items VALUES (2, [0.0, 1.0])")
            .await
            .unwrap(),
        ExecutionResult::RowsAffected(1)
    ));
    executor.into_inner().async_rollback().await.unwrap();
    assert_only_committed_vector(&store);
}

#[tokio::test]
async fn cancelled_worker_sql_future_restores_transaction_ownership() {
    let store = Arc::new(MemoryKV::new());
    let adapter = AsyncKVStoreAdapter::from_arc(store, TxnMode::ReadWrite);
    let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> =
        Arc::new(RwLock::new(MemoryCatalog::new()));
    let txn = adapter.begin_async().await.unwrap();
    assert!(txn.supports_worker_tasks());
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, catalog);

    let mut create = Box::pin(
        bridge.async_execute_multi("CREATE TABLE worker_cancellation (id INT PRIMARY KEY)"),
    );
    assert!(futures::poll!(create.as_mut()).is_pending());
    drop(create);

    timeout(Duration::from_secs(2), bridge.async_wait_for_idle())
        .await
        .expect("worker must finish after the caller drops its future")
        .expect("idle barrier");
    bridge
        .async_execute("INSERT INTO worker_cancellation VALUES (1)")
        .await
        .expect("transaction remains usable");
    bridge
        .async_commit()
        .await
        .expect("commit after cancellation");
}
