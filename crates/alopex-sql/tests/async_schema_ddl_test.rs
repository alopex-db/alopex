#![cfg(feature = "tokio")]

use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::{ExecutionResult, ExecutorError};
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::{AsyncSqlTransaction, AsyncTxnBridge};
use std::sync::{Arc, RwLock};

type CatalogHandle = Arc<RwLock<dyn Catalog + Send + Sync>>;
async fn transaction(
    store: &Arc<MemoryKV>,
    catalog: &CatalogHandle,
    mode: TxnMode,
) -> AsyncTxnBridge<'static, AsyncKVTransactionAdapter> {
    let adapter = AsyncKVStoreAdapter::from_arc(store.clone(), mode);
    AsyncTxnBridge::with_catalog(adapter.begin_async().await.unwrap(), mode, catalog.clone())
}
async fn rows(
    txn: &mut AsyncTxnBridge<'static, AsyncKVTransactionAdapter>,
    sql: &str,
) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = txn.async_execute(sql).await.unwrap() else {
        panic!("expected query")
    };
    result.rows
}

#[tokio::test]
async fn async_schema_alter_if_exists_distinguishes_missing_and_existing_tables() {
    let store = Arc::new(MemoryKV::new());
    let catalog: CatalogHandle = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
    txn.async_execute("ALTER TABLE IF EXISTS missing ADD COLUMN value TEXT")
        .await
        .unwrap();
    assert!(
        txn.async_execute("ALTER TABLE missing ADD COLUMN value TEXT")
            .await
            .is_err()
    );
    txn.async_execute("CREATE TABLE items (id INTEGER)")
        .await
        .unwrap();
    txn.async_execute("ALTER TABLE IF EXISTS items ADD COLUMN value TEXT")
        .await
        .unwrap();
    assert!(rows(&mut txn, "SELECT value FROM items").await.is_empty());
    assert_eq!(
        catalog
            .read()
            .unwrap()
            .get_table("items")
            .unwrap()
            .column_names(),
        vec!["id", "value"]
    );
}

#[tokio::test]
async fn async_schema_alter_preserves_rows_and_committed_metadata() {
    let store = Arc::new(MemoryKV::new());
    let catalog: CatalogHandle = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
    for sql in [
        "CREATE TABLE items (id INTEGER)",
        "INSERT INTO items VALUES (1)",
        "ALTER TABLE items ADD COLUMN value TEXT DEFAULT 'seed'",
        "ALTER TABLE items RENAME TO renamed",
    ] {
        txn.async_execute(sql).await.unwrap();
    }
    txn.async_commit().await.unwrap();
    let mut txn = transaction(&store, &catalog, TxnMode::ReadOnly).await;
    assert_eq!(
        rows(&mut txn, "SELECT id, value FROM renamed").await,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("seed".into())]]
    );
    assert!(!catalog.read().unwrap().table_exists("items"));
}

#[tokio::test]
async fn async_schema_view_uses_live_rows_and_drop_semantics() {
    let store = Arc::new(MemoryKV::new());
    let catalog: CatalogHandle = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
    for sql in [
        "CREATE TABLE items (id INTEGER)",
        "CREATE VIEW visible AS SELECT id FROM items",
        "INSERT INTO items VALUES (1)",
    ] {
        txn.async_execute(sql).await.unwrap();
    }
    assert_eq!(
        rows(&mut txn, "SELECT id FROM visible").await,
        vec![vec![SqlValue::Integer(1)]]
    );
    assert!(txn.async_execute("DROP TABLE items").await.is_err());
    txn.async_execute("DROP VIEW visible").await.unwrap();
    txn.async_execute("DROP VIEW IF EXISTS visible")
        .await
        .unwrap();
    assert!(txn.async_execute("DROP VIEW visible").await.is_err());
}

#[tokio::test]
async fn async_schema_truncate_rollback_restores_indexed_rows() {
    let store = Arc::new(MemoryKV::new());
    let catalog: CatalogHandle = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
    for sql in [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)",
        "CREATE INDEX value_idx ON items (value)",
        "INSERT INTO items VALUES (1, 7)",
    ] {
        txn.async_execute(sql).await.unwrap();
    }
    txn.async_commit().await.unwrap();
    let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
    txn.async_execute("TRUNCATE TABLE items").await.unwrap();
    assert!(
        rows(&mut txn, "SELECT id FROM items WHERE value = 7")
            .await
            .is_empty()
    );
    txn.async_rollback().await.unwrap();
    let mut txn = transaction(&store, &catalog, TxnMode::ReadOnly).await;
    assert_eq!(
        rows(&mut txn, "SELECT id FROM items WHERE value = 7").await,
        vec![vec![SqlValue::Integer(1)]]
    );
}

#[tokio::test]
async fn async_schema_ddl_requires_write_mode() {
    let store = Arc::new(MemoryKV::new());
    let catalog: CatalogHandle = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
    for sql in [
        "CREATE TABLE items (id INTEGER)",
        "CREATE VIEW visible AS SELECT id FROM items",
    ] {
        txn.async_execute(sql).await.unwrap();
    }
    txn.async_commit().await.unwrap();
    let mut txn = transaction(&store, &catalog, TxnMode::ReadOnly).await;
    for sql in [
        "ALTER TABLE items ADD COLUMN value TEXT",
        "TRUNCATE TABLE items",
        "CREATE VIEW another AS SELECT id FROM items",
        "DROP VIEW visible",
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

#[tokio::test]
async fn async_schema_truncate_preserves_hnsw_on_rollback_and_clears_on_commit() {
    use alopex_core::kv::{KVStore, KVTransaction};
    use alopex_core::vector::hnsw::HnswIndex;
    let store = Arc::new(MemoryKV::new());
    let catalog: CatalogHandle = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
    for sql in [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2,L2))",
        "CREATE INDEX vector_idx ON items (embedding) USING HNSW WITH (m = 4, ef_construction = 8)",
        "INSERT INTO items VALUES (1,[1,0]),(2,[5,0])",
    ] {
        txn.async_execute(sql).await.unwrap();
    }
    txn.async_commit().await.unwrap();
    for commit in [false, true] {
        let mut txn = transaction(&store, &catalog, TxnMode::ReadWrite).await;
        txn.async_execute("TRUNCATE TABLE items").await.unwrap();
        assert!(rows(&mut txn, "SELECT id FROM items").await.is_empty());
        if commit {
            txn.async_commit().await.unwrap();
        } else {
            txn.async_rollback().await.unwrap();
        }
        let mut raw = store.begin(TxnMode::ReadOnly).unwrap();
        let index = HnswIndex::load("vector_idx", &mut raw).unwrap();
        let (hits, _) = index.search(&[0.0, 0.0], 2, Some(8)).unwrap();
        assert_eq!(hits.len(), if commit { 0 } else { 2 });
        if !commit {
            assert_eq!(hits[0].key, 1u64.to_be_bytes());
        }
        raw.commit_self().unwrap();
    }
}
