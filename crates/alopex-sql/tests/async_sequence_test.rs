#![cfg(feature = "tokio")]

use std::sync::{Arc, RwLock};

use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::storage::async_storage::{AsyncSqlTransaction, AsyncTxnBridge};

async fn transaction(
    store: &Arc<MemoryKV>,
    mode: TxnMode,
) -> AsyncTxnBridge<'static, AsyncKVTransactionAdapter> {
    let adapter = AsyncKVStoreAdapter::from_arc(Arc::clone(store), mode);
    AsyncTxnBridge::with_catalog(
        adapter.begin_async().await.unwrap(),
        mode,
        Arc::new(RwLock::new(MemoryCatalog::new())),
    )
}

#[tokio::test]
async fn missing_sequence_with_if_exists_succeeds() {
    let mut txn = transaction(&Arc::new(MemoryKV::new()), TxnMode::ReadWrite).await;
    assert!(matches!(
        txn.async_execute("DROP SEQUENCE IF EXISTS missing")
            .await
            .unwrap(),
        ExecutionResult::Success
    ));
}

#[tokio::test]
async fn missing_sequence_without_if_exists_has_sequence_error() {
    let mut txn = transaction(&Arc::new(MemoryKV::new()), TxnMode::ReadWrite).await;
    assert!(matches!(
        txn.async_execute("DROP SEQUENCE missing").await,
        Err(ExecutorError::InvalidOperation { operation, reason })
            if operation == "DROP SEQUENCE" && reason == "sequence not found: missing"
    ));
}

#[tokio::test]
async fn sequence_create_alter_and_drop_preserve_metadata_and_absence() {
    let store = Arc::new(MemoryKV::new());
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("CREATE SEQUENCE counter START WITH 7")
        .await
        .unwrap();
    txn.async_execute("ALTER SEQUENCE counter RESTART WITH 11")
        .await
        .unwrap();
    txn.async_commit().await.unwrap();
    let executor = Executor::new(
        Arc::clone(&store),
        Arc::new(RwLock::new(MemoryCatalog::new())),
    );
    let sequences = executor.list_sequences().unwrap();
    assert_eq!(sequences.len(), 1);
    assert_eq!(sequences[0].name, "counter");
    assert_eq!(sequences[0].next_value, 11);
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("DROP SEQUENCE counter").await.unwrap();
    txn.async_commit().await.unwrap();
    assert!(executor.list_sequences().unwrap().is_empty());
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    assert!(matches!(txn.async_execute("DROP SEQUENCE counter").await,
        Err(ExecutorError::InvalidOperation { operation, .. }) if operation == "DROP SEQUENCE"));
}

#[tokio::test]
async fn sequence_drop_rollback_preserves_committed_state() {
    let store = Arc::new(MemoryKV::new());
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("CREATE SEQUENCE counter START WITH 7")
        .await
        .unwrap();
    txn.async_commit().await.unwrap();
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("DROP SEQUENCE counter").await.unwrap();
    txn.async_rollback().await.unwrap();
    let executor = Executor::new(store, Arc::new(RwLock::new(MemoryCatalog::new())));
    let sequences = executor.list_sequences().unwrap();
    assert_eq!(sequences.len(), 1);
    assert_eq!(sequences[0].name, "counter");
    assert_eq!(sequences[0].next_value, 7);
}

#[tokio::test]
async fn sequence_ddl_requires_write_transaction() {
    let mut txn = transaction(&Arc::new(MemoryKV::new()), TxnMode::ReadOnly).await;
    for sql in [
        "CREATE SEQUENCE counter",
        "ALTER SEQUENCE counter RESTART WITH 2",
        "DROP SEQUENCE IF EXISTS counter",
    ] {
        assert!(
            matches!(
                txn.async_execute(sql).await,
                Err(ExecutorError::ReadOnlyTransaction { .. })
            ),
            "{sql}"
        );
    }
}
