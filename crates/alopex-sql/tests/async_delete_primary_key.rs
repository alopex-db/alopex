#![cfg(feature = "tokio")]

use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::storage::async_storage::{AsyncSqlTransaction, AsyncTxnBridge};
use alopex_sql::{ExecutionResult, SqlValue};
use std::sync::{Arc, RwLock};

type Transaction = AsyncTxnBridge<'static, AsyncKVTransactionAdapter>;
struct Fixture {
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<dyn Catalog + Send + Sync>>,
}

impl Fixture {
    async fn new() -> Self {
        let fixture = Self {
            store: Arc::new(MemoryKV::new()),
            catalog: Arc::new(RwLock::new(MemoryCatalog::new())),
        };
        let mut txn = fixture.begin().await;
        for sql in [
            "CREATE TABLE items(id INTEGER PRIMARY KEY,v INTEGER)",
            "CREATE INDEX by_v ON items(v)",
            "INSERT INTO items VALUES (42,10),(7,20),(99,30)",
        ] {
            txn.async_execute(sql).await.unwrap();
        }
        txn.async_commit().await.unwrap();
        fixture
    }

    async fn begin(&self) -> Transaction {
        let adapter = AsyncKVStoreAdapter::from_arc(self.store.clone(), TxnMode::ReadWrite);
        AsyncTxnBridge::with_catalog(
            adapter.begin_async().await.unwrap(),
            TxnMode::ReadWrite,
            self.catalog.clone(),
        )
    }
}

async fn rows(txn: &mut Transaction, sql: &str) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = txn.async_execute(sql).await.unwrap() else {
        panic!("expected rows from {sql}");
    };
    result.rows
}

fn values(rows: &[(i32, i32)]) -> Vec<Vec<SqlValue>> {
    rows.iter()
        .map(|&(id, v)| vec![SqlValue::Integer(id), SqlValue::Integer(v)])
        .collect()
}

#[tokio::test]
async fn async_delete_primary_key_join_requires_a_source_row() {
    let fixture = Fixture::new().await;
    let mut txn = fixture.begin().await;
    txn.async_execute("CREATE TABLE source(id INTEGER)")
        .await
        .unwrap();
    let delete = "DELETE FROM items USING source AS s WHERE items.id=7 RETURNING items.id,items.v";
    assert!(rows(&mut txn, delete).await.is_empty());
    txn.async_execute("INSERT INTO source VALUES (100)")
        .await
        .unwrap();
    assert_eq!(rows(&mut txn, delete).await, values(&[(7, 20)]));
    assert_eq!(
        rows(&mut txn, "SELECT id,v FROM items ORDER BY id").await,
        values(&[(42, 10), (99, 30)])
    );
    txn.async_rollback().await.unwrap();
}

#[tokio::test]
async fn async_delete_primary_key_source_predicate_is_not_target_lookup() {
    let fixture = Fixture::new().await;
    let mut txn = fixture.begin().await;
    // The source column has the same physical name, but refers to the joined
    // tuple. Every target row must match the one source row selected by s.id.
    let deleted = rows(
        &mut txn,
        "DELETE FROM items USING items AS s WHERE s.id=7 RETURNING items.id,items.v",
    )
    .await;
    assert_eq!(deleted, values(&[(42, 10), (7, 20), (99, 30)]));
    assert!(rows(&mut txn, "SELECT id,v FROM items").await.is_empty());
    txn.async_rollback().await.unwrap();
}

#[tokio::test]
async fn async_delete_primary_key_without_predicate_removes_all_rows() {
    let fixture = Fixture::new().await;
    let mut txn = fixture.begin().await;
    assert!(matches!(
        txn.async_execute("DELETE FROM items").await.unwrap(),
        ExecutionResult::RowsAffected(3)
    ));
    assert!(rows(&mut txn, "SELECT id,v FROM items").await.is_empty());
    txn.async_rollback().await.unwrap();
}

#[tokio::test]
async fn async_delete_primary_key_commit_and_key_reuse() {
    let fixture = Fixture::new().await;
    let mut txn = fixture.begin().await;
    assert_eq!(
        rows(&mut txn, "DELETE FROM items WHERE id=7 RETURNING id,v").await,
        values(&[(7, 20)])
    );
    txn.async_commit().await.unwrap();
    let mut txn = fixture.begin().await;
    assert!(
        rows(&mut txn, "SELECT id,v FROM items WHERE v=20")
            .await
            .is_empty()
    );
    txn.async_execute("INSERT INTO items VALUES (7,70)")
        .await
        .unwrap();
    txn.async_commit().await.unwrap();
    let mut txn = fixture.begin().await;
    assert_eq!(
        rows(&mut txn, "SELECT id,v FROM items ORDER BY id").await,
        values(&[(7, 70), (42, 10), (99, 30)])
    );
    txn.async_rollback().await.unwrap();
}

#[tokio::test]
async fn async_delete_primary_key_rollback_restores_row_and_indexes() {
    let fixture = Fixture::new().await;
    let mut txn = fixture.begin().await;
    assert_eq!(
        rows(&mut txn, "DELETE FROM items WHERE 7=id RETURNING id,v").await,
        values(&[(7, 20)])
    );
    txn.async_rollback().await.unwrap();
    let mut txn = fixture.begin().await;
    assert_eq!(
        rows(&mut txn, "SELECT id,v FROM items WHERE id=7").await,
        values(&[(7, 20)])
    );
    assert_eq!(
        rows(&mut txn, "SELECT id,v FROM items WHERE v=20").await,
        values(&[(7, 20)])
    );
    assert_eq!(
        rows(&mut txn, "SELECT id,v FROM items ORDER BY id").await,
        values(&[(7, 20), (42, 10), (99, 30)])
    );
    txn.async_rollback().await.unwrap();
}

#[tokio::test]
async fn async_delete_primary_key_foreign_key_failure_keeps_parent() {
    let fixture = Fixture::new().await;
    let mut txn = fixture.begin().await;
    txn.async_execute("CREATE TABLE children(id INTEGER, parent_id INTEGER REFERENCES items(id))")
        .await
        .unwrap();
    txn.async_execute("INSERT INTO children VALUES (1,7)")
        .await
        .unwrap();
    txn.async_commit().await.unwrap();
    let mut txn = fixture.begin().await;
    assert!(
        txn.async_execute("DELETE FROM items WHERE id=7")
            .await
            .is_err()
    );
    txn.async_commit().await.unwrap();
    let mut txn = fixture.begin().await;
    assert_eq!(
        rows(&mut txn, "SELECT id,v FROM items ORDER BY id").await,
        values(&[(7, 20), (42, 10), (99, 30)])
    );
    assert_eq!(
        rows(&mut txn, "SELECT id,parent_id FROM children").await,
        values(&[(1, 7)])
    );
    txn.async_rollback().await.unwrap();
}

#[tokio::test]
async fn async_delete_primary_key_cascade_preserves_other_children() {
    let fixture = Fixture::new().await;
    let mut txn = fixture.begin().await;
    txn.async_execute("CREATE TABLE children(id INTEGER, parent_id INTEGER REFERENCES items(id) ON DELETE CASCADE)").await.unwrap();
    txn.async_execute("INSERT INTO children VALUES (1,7),(2,42)")
        .await
        .unwrap();
    txn.async_commit().await.unwrap();
    let mut txn = fixture.begin().await;
    assert_eq!(
        rows(&mut txn, "DELETE FROM items WHERE id=7 RETURNING id,v").await,
        values(&[(7, 20)])
    );
    txn.async_commit().await.unwrap();
    let mut txn = fixture.begin().await;
    assert_eq!(
        rows(&mut txn, "SELECT id,v FROM items ORDER BY id").await,
        values(&[(42, 10), (99, 30)])
    );
    assert_eq!(
        rows(&mut txn, "SELECT id,parent_id FROM children").await,
        values(&[(2, 42)])
    );
    txn.async_rollback().await.unwrap();
}
