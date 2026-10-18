#![cfg(all(feature = "tokio", not(target_arch = "wasm32")))]

use std::sync::{Arc, RwLock};

use alopex_core::Result as KvResult;
use alopex_core::async_runtime::{BoxFuture, BoxStream};
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{
    AsyncKVStore, AsyncKVTransaction, KeySearchCancellation, KeySearchPage, KeySearchRequest,
};
use alopex_core::types::{Key, TxnMode, Value};
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::{AsyncExecutor, ExecutionResult, ExecutorError};
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use futures::StreamExt;

struct AsyncOnlyTransaction(AsyncKVTransactionAdapter);

impl<'txn> AsyncKVTransaction<'txn> for AsyncOnlyTransaction {
    fn async_get<'a>(&'a self, key: &'a [u8]) -> BoxFuture<'a, KvResult<Option<Value>>> {
        self.0.async_get(key)
    }

    fn async_put<'a>(&'a mut self, key: &'a [u8], value: &'a [u8]) -> BoxFuture<'a, KvResult<()>> {
        self.0.async_put(key, value)
    }

    fn async_delete<'a>(&'a mut self, key: &'a [u8]) -> BoxFuture<'a, KvResult<()>> {
        self.0.async_delete(key)
    }

    fn async_scan_prefix<'a>(&'a self, prefix: &'a [u8]) -> BoxStream<'a, KvResult<(Key, Value)>> {
        self.0.async_scan_prefix(prefix)
    }

    fn async_search_keys<'a>(
        &'a self,
        request: KeySearchRequest,
    ) -> BoxFuture<'a, KvResult<KeySearchPage>> {
        self.0.async_search_keys(request)
    }

    fn async_search_keys_with_cancellation<'a>(
        &'a self,
        request: KeySearchRequest,
        cancellation: KeySearchCancellation,
    ) -> BoxFuture<'a, KvResult<KeySearchPage>> {
        self.0
            .async_search_keys_with_cancellation(request, cancellation)
    }

    fn async_commit(self) -> BoxFuture<'txn, KvResult<()>> {
        self.0.async_commit()
    }

    fn async_rollback(self) -> BoxFuture<'txn, KvResult<()>> {
        self.0.async_rollback()
    }
}

type TestExecutor = AsyncExecutor<'static, AsyncTxnBridge<'static, AsyncOnlyTransaction>>;

async fn executor(
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<dyn Catalog + Send + Sync>>,
    mode: TxnMode,
) -> TestExecutor {
    let adapter = AsyncKVStoreAdapter::from_arc(store, mode);
    let txn = AsyncOnlyTransaction(adapter.begin_async().await.unwrap());
    assert!(!txn.supports_worker_tasks());
    AsyncExecutor::new(AsyncTxnBridge::with_catalog(txn, mode, catalog))
}

fn returning_rows(result: ExecutionResult) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(query) = result else {
        panic!("expected RETURNING rows");
    };
    query.rows
}

async fn rows(executor: &mut TestExecutor) -> Vec<Vec<SqlValue>> {
    executor
        .execute_async("SELECT id,v FROM items ORDER BY id")
        .map(|row| row.unwrap().values)
        .collect()
        .await
}

#[tokio::test]
async fn async_only_backend_preserves_dml_commit_rollback_and_read_only() {
    let store = Arc::new(MemoryKV::new());
    let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> =
        Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut write = executor(store.clone(), catalog.clone(), TxnMode::ReadWrite).await;
    write
        .execute_ddl_async("CREATE TABLE items (id INTEGER PRIMARY KEY, v INTEGER)")
        .await
        .unwrap();
    assert!(matches!(
        write
            .execute_dml_async("INSERT INTO items VALUES (1,10),(2,20),(3,30)")
            .await
            .unwrap(),
        ExecutionResult::RowsAffected(3)
    ));
    assert_eq!(
        returning_rows(
            write
                .execute_dml_async("UPDATE items SET v=v+1 WHERE id=1 RETURNING id,v")
                .await
                .unwrap()
        ),
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(11)]]
    );
    assert_eq!(
        returning_rows(
            write
                .execute_dml_async("DELETE FROM items WHERE id=2 RETURNING id,v")
                .await
                .unwrap()
        ),
        vec![vec![SqlValue::Integer(2), SqlValue::Integer(20)]]
    );
    write.into_inner().async_commit().await.unwrap();

    let committed = vec![
        vec![SqlValue::Integer(1), SqlValue::Integer(11)],
        vec![SqlValue::Integer(3), SqlValue::Integer(30)],
    ];
    let mut write = executor(store.clone(), catalog.clone(), TxnMode::ReadWrite).await;
    assert_eq!(rows(&mut write).await, committed);
    for sql in [
        "UPDATE items SET v=99 WHERE id=1",
        "DELETE FROM items WHERE id=3",
        "INSERT INTO items VALUES (4,40)",
    ] {
        assert!(matches!(
            write.execute_dml_async(sql).await.unwrap(),
            ExecutionResult::RowsAffected(1)
        ));
    }
    assert_eq!(
        rows(&mut write).await,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(99)],
            vec![SqlValue::Integer(4), SqlValue::Integer(40)],
        ]
    );
    write.into_inner().async_rollback().await.unwrap();

    let mut read = executor(store, catalog, TxnMode::ReadOnly).await;
    for sql in [
        "INSERT INTO items VALUES (4,40)",
        "UPDATE items SET v=99 WHERE id=1",
        "DELETE FROM items WHERE id=3",
    ] {
        assert!(matches!(
            read.execute_dml_async(sql).await,
            Err(ExecutorError::ReadOnlyTransaction { .. })
        ));
        assert_eq!(rows(&mut read).await, committed);
    }
    read.into_inner().async_commit().await.unwrap();
}
