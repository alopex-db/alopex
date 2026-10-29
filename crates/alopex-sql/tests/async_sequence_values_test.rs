#![cfg(feature = "tokio")]

use std::sync::{Arc, RwLock};

use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::executor::{ExecutionResult, ExecutorError};
use alopex_sql::planner::ResolvedType;
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::{AsyncSqlTransaction, AsyncTxnBridge};
use futures::StreamExt;

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

async fn value(txn: &mut AsyncTxnBridge<'static, AsyncKVTransactionAdapter>, sql: &str) -> i64 {
    let ExecutionResult::Query(result) = txn.async_execute(sql).await.unwrap() else {
        panic!("expected query")
    };
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].name, "value");
    assert_eq!(result.columns[0].data_type, ResolvedType::BigInt);
    assert_eq!(result.rows.len(), 1);
    let [SqlValue::BigInt(value)] = result.rows[0].as_slice() else {
        panic!("expected BigInt: {:?}", result.rows)
    };
    *value
}

#[tokio::test]
async fn sequence_values_advance_and_commit_with_bigint_alias() {
    let store = Arc::new(MemoryKV::new());
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("CREATE SEQUENCE counter START WITH 7 INCREMENT BY 2")
        .await
        .unwrap();
    assert_eq!(
        value(&mut txn, "SELECT NEXTVAL('counter') AS value").await,
        7
    );
    assert_eq!(
        value(&mut txn, "SELECT currval('counter') AS value").await,
        7
    );
    assert_eq!(
        value(&mut txn, "SELECT nextval('counter') AS value").await,
        9
    );
    txn.async_commit().await.unwrap();
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    assert_eq!(
        value(&mut txn, "SELECT currval('counter') AS value").await,
        9
    );
    assert_eq!(
        value(&mut txn, "SELECT nextval('counter') AS value").await,
        11
    );
}

#[tokio::test]
async fn sequence_value_rollback_restores_allocation() {
    let store = Arc::new(MemoryKV::new());
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("CREATE SEQUENCE counter START WITH 7")
        .await
        .unwrap();
    txn.async_commit().await.unwrap();
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    assert_eq!(
        value(&mut txn, "SELECT nextval('counter') AS value").await,
        7
    );
    txn.async_rollback().await.unwrap();
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    assert!(
        matches!(txn.async_execute("SELECT currval('counter')").await,
        Err(ExecutorError::InvalidOperation { operation, reason }) if operation == "currval" && reason.contains("has not been called"))
    );
    assert_eq!(
        value(&mut txn, "SELECT nextval('counter') AS value").await,
        7
    );
}

#[tokio::test]
async fn sequence_values_report_missing_and_unallocated_errors() {
    let mut txn = transaction(&Arc::new(MemoryKV::new()), TxnMode::ReadWrite).await;
    for function in ["nextval", "currval"] {
        assert!(
            matches!(txn.async_execute(&format!("SELECT {function}('missing')")).await,
            Err(ExecutorError::InvalidOperation { operation, reason }) if operation == function && reason == "sequence not found: missing")
        );
    }
    txn.async_execute("CREATE SEQUENCE counter").await.unwrap();
    assert!(
        matches!(txn.async_execute("SELECT currval('counter')").await,
        Err(ExecutorError::InvalidOperation { operation, reason }) if operation == "currval" && reason == "nextval has not been called for sequence: counter")
    );
}

#[tokio::test]
async fn sequence_read_only_allows_currval_but_rejects_nextval_in_both_apis() {
    let store = Arc::new(MemoryKV::new());
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("CREATE SEQUENCE counter START WITH 7")
        .await
        .unwrap();
    assert_eq!(
        value(&mut txn, "SELECT nextval('counter') AS value").await,
        7
    );
    txn.async_commit().await.unwrap();
    let mut txn = transaction(&store, TxnMode::ReadOnly).await;
    assert_eq!(
        value(&mut txn, "SELECT currval('counter') AS value").await,
        7
    );
    assert!(
        matches!(txn.async_execute("SELECT nextval('counter')").await,
        Err(ExecutorError::ReadOnlyTransaction { operation }) if operation == "nextval")
    );
    let rows: Vec<_> = txn.async_query("SELECT nextval('counter')").collect().await;
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(&rows[0], Err(ExecutorError::ReadOnlyTransaction { operation }) if operation == "nextval")
    );
    let rows: Vec<_> = txn.async_query("SELECT currval('counter')").collect().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].as_ref().unwrap().values, vec![SqlValue::BigInt(7)]);
    txn.async_rollback().await.unwrap();
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    assert_eq!(
        value(&mut txn, "SELECT nextval('counter') AS value").await,
        8
    );
}

#[tokio::test]
async fn streaming_sequence_values_share_transaction_state_and_errors() {
    let store = Arc::new(MemoryKV::new());
    let mut txn = transaction(&store, TxnMode::ReadWrite).await;
    txn.async_execute("CREATE SEQUENCE counter START WITH 7")
        .await
        .unwrap();
    let rows: Vec<_> = txn
        .async_query("SELECT nextval('counter') AS value")
        .collect()
        .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].as_ref().unwrap().values, vec![SqlValue::BigInt(7)]);
    assert_eq!(
        value(&mut txn, "SELECT currval('counter') AS value").await,
        7
    );
    let rows: Vec<_> = txn.async_query("SELECT nextval('missing')").collect().await;
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(&rows[0], Err(ExecutorError::InvalidOperation { operation, reason }) if operation == "nextval" && reason == "sequence not found: missing")
    );
    txn.async_rollback().await.unwrap();
}

#[tokio::test]
async fn explain_only_allocates_sequence_when_analyze_executes() {
    let mut txn = transaction(&Arc::new(MemoryKV::new()), TxnMode::ReadWrite).await;
    txn.async_execute("CREATE SEQUENCE counter START WITH 7")
        .await
        .unwrap();
    txn.async_execute("EXPLAIN SELECT nextval('counter')")
        .await
        .unwrap();
    assert_eq!(
        value(&mut txn, "SELECT nextval('counter') AS value").await,
        7
    );
    txn.async_execute("EXPLAIN ANALYZE SELECT nextval('counter')")
        .await
        .unwrap();
    assert_eq!(
        value(&mut txn, "SELECT currval('counter') AS value").await,
        8
    );
}
