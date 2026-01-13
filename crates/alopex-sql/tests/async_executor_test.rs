#![cfg(feature = "tokio")]

use std::sync::{Arc, RwLock};
use std::time::Duration;

use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::{AsyncExecutor, ExecutionResult};
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use futures::StreamExt;

fn rss_check_enabled() -> bool {
    std::env::var("ALOPEX_ASYNC_RSS_CHECK")
        .ok()
        .map(|v| v != "0")
        .unwrap_or(cfg!(target_os = "linux"))
}

fn rss_limit_kb() -> usize {
    std::env::var("ALOPEX_ASYNC_RSS_LIMIT_KB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100_000)
}

#[cfg(target_os = "linux")]
fn read_rss_kb() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let rest = line.strip_prefix("VmRSS:")?;
        let mut parts = rest.split_whitespace();
        if let Some(value) = parts.next() {
            return value.parse::<usize>().ok();
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn read_rss_kb() -> Option<usize> {
    None
}

fn build_catalog() -> Arc<RwLock<dyn Catalog + Send + Sync>> {
    Arc::new(RwLock::new(MemoryCatalog::new()))
}

async fn build_executor() -> (
    AsyncExecutor<'static, AsyncTxnBridge<'static, AsyncKVTransactionAdapter>>,
    Arc<AsyncKVStoreAdapter<MemoryKV>>,
) {
    let store = Arc::new(MemoryKV::new());
    let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(
        store.clone(),
        TxnMode::ReadWrite,
    ));
    let catalog = build_catalog();
    let txn = async_store.begin_async().await.expect("begin");
    let bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, catalog);
    (AsyncExecutor::new(bridge), async_store)
}

#[tokio::test]
async fn async_executor_streams_rows() {
    let (mut executor, _store) = build_executor().await;

    let ddl = executor
        .execute_ddl_async("CREATE TABLE items (id INT PRIMARY KEY, value TEXT);")
        .await
        .expect("ddl");
    assert!(matches!(ddl, ExecutionResult::Success));

    let dml = executor
        .execute_dml_async("INSERT INTO items (id, value) VALUES (1, 'alpha'), (2, 'bravo');")
        .await
        .expect("dml");
    assert!(matches!(dml, ExecutionResult::RowsAffected(2)));

    let mut stream = executor.execute_async("SELECT id, value FROM items ORDER BY id;");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("row");
        rows.push(row.values);
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], SqlValue::Integer(1));
    assert_eq!(rows[0][1], SqlValue::Text("alpha".to_string()));
    assert_eq!(rows[1][0], SqlValue::Integer(2));
    assert_eq!(rows[1][1], SqlValue::Text("bravo".to_string()));
    drop(stream);

    executor.into_inner().async_commit().await.expect("commit");
}

#[tokio::test]
async fn async_executor_large_scan_is_streamed() {
    let (mut executor, _store) = build_executor().await;

    executor
        .execute_ddl_async("CREATE TABLE items (id INT PRIMARY KEY, value TEXT);")
        .await
        .expect("ddl");

    let total_rows = std::env::var("ALOPEX_ASYNC_TEST_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let batch_size = 1000usize;
    let mut next_id = 0usize;
    while next_id < total_rows {
        let end = (next_id + batch_size).min(total_rows);
        let mut values = String::new();
        for id in next_id..end {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str(&format!("({}, 'v{}')", id, id));
        }
        let sql = format!("INSERT INTO items (id, value) VALUES {values};");
        executor.execute_dml_async(&sql).await.expect("insert");
        next_id = end;
    }

    let rss_check = rss_check_enabled();
    let rss_limit = rss_limit_kb();
    let mut baseline_rss = None;
    let mut max_rss = None;
    if rss_check {
        baseline_rss = read_rss_kb();
        max_rss = baseline_rss;
    }

    let mut stream = executor.execute_async("SELECT id FROM items ORDER BY id;");
    let mut count = 0usize;
    while let Some(row) = stream.next().await {
        let row = row.expect("row");
        if row.values.len() != 1 {
            panic!("unexpected row width");
        }
        count += 1;
        if rss_check
            && count.is_multiple_of(1000)
            && let Some(rss) = read_rss_kb()
        {
            max_rss = Some(max_rss.map_or(rss, |max| max.max(rss)));
        }
    }
    assert_eq!(count, total_rows);
    if let (Some(baseline), Some(max_rss)) = (baseline_rss, max_rss) {
        let delta = max_rss.saturating_sub(baseline);
        assert!(
            delta <= rss_limit,
            "rss delta too high: {delta} KB > {rss_limit} KB"
        );
    }
    drop(stream);

    executor.into_inner().async_commit().await.expect("commit");
}

#[tokio::test]
async fn async_executor_stream_cancellation_is_safe() {
    let (mut executor, _store) = build_executor().await;

    executor
        .execute_ddl_async("CREATE TABLE items (id INT PRIMARY KEY, value TEXT);")
        .await
        .expect("ddl");
    executor
        .execute_dml_async(
            "INSERT INTO items (id, value) VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie');",
        )
        .await
        .expect("dml");

    let mut stream = executor.execute_async("SELECT id FROM items ORDER BY id;");
    let _ = stream.next().await.expect("row");
    drop(stream);

    let txn = executor.into_inner();
    let result = tokio::time::timeout(Duration::from_secs(2), txn.async_rollback())
        .await
        .expect("rollback timeout");
    if let Err(err) = result {
        assert!(matches!(
            err,
            alopex_sql::storage::error::StorageError::TransactionClosed
        ));
    }
}
