#![cfg(feature = "tokio")]

use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alopex_core::async_runtime::{BoxFuture, BoxStream};
use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::async_kv::AsyncKVTransaction;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KeySearchCancellation, KeySearchPage, KeySearchRequest};
use alopex_core::types::{Key, TxnMode, Value};
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::bulk::CopySecurityConfig;
use alopex_sql::executor::{AsyncExecutor, ExecutionResult};
use alopex_sql::planner::TableReferenceAccess;
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::{AsyncSqlTransaction, AsyncTxnBridge};
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

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

struct GatedAsyncTransaction {
    inner: AsyncKVTransactionAdapter,
    panic_next_put: Arc<AtomicBool>,
    block_next_put: Arc<AtomicBool>,
    put_started: Arc<tokio::sync::Notify>,
    release_put: Arc<tokio::sync::Notify>,
    commit_finished: Arc<tokio::sync::Notify>,
    block_rollback: Arc<AtomicBool>,
    rollback_started: Arc<tokio::sync::Notify>,
    release_rollback: Arc<tokio::sync::Notify>,
    rollback_finished: Arc<tokio::sync::Notify>,
}

impl<'txn> AsyncKVTransaction<'txn> for GatedAsyncTransaction {
    fn async_get<'a>(&'a self, key: &'a [u8]) -> BoxFuture<'a, alopex_core::Result<Option<Value>>> {
        self.inner.async_get(key)
    }

    fn async_put<'a>(
        &'a mut self,
        key: &'a [u8],
        value: &'a [u8],
    ) -> BoxFuture<'a, alopex_core::Result<()>> {
        if self.panic_next_put.swap(false, Ordering::SeqCst) {
            return Box::pin(async { panic!("controlled async put panic") });
        }
        if !self.block_next_put.swap(false, Ordering::SeqCst) {
            return self.inner.async_put(key, value);
        }
        Box::pin(async move {
            self.put_started.notify_one();
            self.release_put.notified().await;
            self.inner.async_put(key, value).await
        })
    }

    fn async_delete<'a>(&'a mut self, key: &'a [u8]) -> BoxFuture<'a, alopex_core::Result<()>> {
        self.inner.async_delete(key)
    }

    fn async_scan_prefix<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> BoxStream<'a, alopex_core::Result<(Key, Value)>> {
        self.inner.async_scan_prefix(prefix)
    }

    fn async_search_keys<'a>(
        &'a self,
        request: KeySearchRequest,
    ) -> BoxFuture<'a, alopex_core::Result<KeySearchPage>> {
        self.inner.async_search_keys(request)
    }

    fn async_search_keys_with_cancellation<'a>(
        &'a self,
        request: KeySearchRequest,
        cancellation: KeySearchCancellation,
    ) -> BoxFuture<'a, alopex_core::Result<KeySearchPage>> {
        self.inner
            .async_search_keys_with_cancellation(request, cancellation)
    }

    fn async_commit(self) -> BoxFuture<'txn, alopex_core::Result<()>> {
        Box::pin(async move {
            let result = self.inner.async_commit().await;
            self.commit_finished.notify_one();
            result
        })
    }

    fn async_rollback(self) -> BoxFuture<'txn, alopex_core::Result<()>> {
        if !self.block_rollback.swap(false, Ordering::SeqCst) {
            return self.inner.async_rollback();
        }
        Box::pin(async move {
            self.rollback_started.notify_one();
            self.release_rollback.notified().await;
            let result = self.inner.async_rollback().await;
            self.rollback_finished.notify_one();
            result
        })
    }
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

fn write_int_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .expect("record batch");
    let mut writer = ArrowWriter::try_new(File::create(path).expect("parquet file"), schema, None)
        .expect("parquet writer");
    writer.write(&batch).expect("parquet batch");
    writer.close().expect("parquet close");
}

#[tokio::test]
async fn btree_adaptive_async_reports_effective_byte_fallback() {
    use alopex_sql::executor::{MemoryPolicy, SpillPolicy};
    let store = Arc::new(AsyncKVStoreAdapter::from_arc(
        Arc::new(MemoryKV::new()),
        TxnMode::ReadWrite,
    ));
    let txn = store.begin_async().await.unwrap();
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, build_catalog());
    for sql in [
        "CREATE TABLE adaptive_async (id INT, v INT, wide TEXT)".to_owned(),
        format!(
            "INSERT INTO adaptive_async VALUES (1, 7, '{}')",
            "x".repeat(1024)
        ),
        "CREATE INDEX adaptive_async_v ON adaptive_async(v)".to_owned(),
    ] {
        bridge.async_execute(&sql).await.unwrap();
    }
    bridge.set_memory_policy(Some(MemoryPolicy::new(Some(256), SpillPolicy::FailFast)));
    let ExecutionResult::Query(rows) = bridge
        .async_execute("EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM adaptive_async WHERE v = 7")
        .await
        .unwrap()
    else {
        panic!()
    };
    let SqlValue::Text(json) = &rows.rows[0][0] else {
        panic!()
    };
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    let access = &json["physical_plan"]["access_path"];
    assert_eq!(access["adaptive"]["max_bytes"], 256);
    assert_eq!(access["adaptive"]["max_candidates"], 32);
    assert_eq!(access["actual"]["path"], "Scan");
    assert_eq!(access["actual"]["fallback"], "ByteBudget");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn read_parquet_honors_copy_allowed_dirs() {
    let temp = tempdir().expect("tempdir");
    let allowed_dir = temp.path().join("allowed");
    fs::create_dir(&allowed_dir).expect("allowed directory");
    let allowed_file = allowed_dir.join("allowed.parquet");
    let outside_file = temp.path().join("outside.parquet");
    write_int_parquet(&allowed_file);
    write_int_parquet(&outside_file);

    let store = Arc::new(MemoryKV::new());
    let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(
        Arc::clone(&store),
        TxnMode::ReadWrite,
    ));
    let catalog = build_catalog();
    let allowed_sql = format!("SELECT id FROM read_parquet('{}')", allowed_file.display());
    let outside_sql = format!("SELECT id FROM read_parquet('{}')", outside_file.display());

    let default_txn = async_store.begin_async().await.expect("begin default");
    let default_bridge =
        AsyncTxnBridge::with_catalog(default_txn, TxnMode::ReadWrite, Arc::clone(&catalog));
    let default_rows: Vec<_> = default_bridge.async_query(&outside_sql).collect().await;
    assert_eq!(default_rows.len(), 1);
    assert_eq!(
        default_rows[0].as_ref().expect("default row").values,
        vec![SqlValue::Integer(7)]
    );
    drop(default_bridge);

    let txn = async_store.begin_async().await.expect("begin");
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, catalog);
    bridge.set_copy_security(CopySecurityConfig {
        allowed_base_dirs: Some(vec![
            allowed_dir.canonicalize().expect("canonical allowed dir"),
        ]),
        allow_symlinks: false,
    });

    bridge
        .async_plan_for_routing(&allowed_sql)
        .await
        .expect("allowed path plans");
    let allowed_rows: Vec<_> = bridge.async_query(&allowed_sql).collect().await;
    assert_eq!(allowed_rows.len(), 1);
    assert_eq!(
        allowed_rows[0].as_ref().expect("allowed row").values,
        vec![SqlValue::Integer(7)]
    );

    let planning_error = bridge
        .async_plan_for_routing(&outside_sql)
        .await
        .expect_err("outside path must not plan");
    assert!(
        planning_error
            .to_string()
            .contains("path not in allowed directories"),
        "{planning_error}"
    );

    let outside_rows: Vec<_> = bridge.async_query(&outside_sql).collect().await;
    assert_eq!(outside_rows.len(), 1);
    let query_error = outside_rows[0]
        .as_ref()
        .expect_err("outside path must not execute");
    assert!(
        query_error
            .to_string()
            .contains("path not in allowed directories"),
        "{query_error}"
    );
    for prefix in ["EXPLAIN", "EXPLAIN ANALYZE"] {
        bridge
            .async_plan_for_routing(&format!("{prefix} {allowed_sql}"))
            .await
            .unwrap();
        assert!(matches!(
            bridge
                .async_execute(&format!("{prefix} {allowed_sql}"))
                .await
                .unwrap(),
            ExecutionResult::Query(_)
        ));
        for error in [
            bridge
                .async_plan_for_routing(&format!("{prefix} {outside_sql}"))
                .await
                .unwrap_err(),
            bridge
                .async_execute(&format!("{prefix} {outside_sql}"))
                .await
                .unwrap_err(),
        ] {
            assert!(
                error
                    .to_string()
                    .contains("path not in allowed directories"),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn async_explain_preserves_execution_permissions_and_rollback() {
    let store = AsyncKVStoreAdapter::from_arc(Arc::new(MemoryKV::new()), TxnMode::ReadWrite);
    let catalog = build_catalog();
    let mut setup = AsyncTxnBridge::with_catalog(
        store.begin_async().await.unwrap(),
        TxnMode::ReadWrite,
        catalog.clone(),
    );
    setup
        .async_execute("CREATE TABLE explained (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    setup
        .async_execute("INSERT INTO explained VALUES (1, 7)")
        .await
        .unwrap();
    setup.async_commit().await.unwrap();

    for mode in [TxnMode::ReadWrite, TxnMode::ReadOnly] {
        let mut bridge =
            AsyncTxnBridge::with_catalog(store.begin_async().await.unwrap(), mode, catalog.clone());
        let update = "UPDATE explained SET value = 9 WHERE id = 1";
        for sql in [
            format!("EXPLAIN {update}"),
            format!("EXPLAIN ANALYZE {update}"),
        ] {
            let plans = bridge.async_plan_for_routing(&sql).await.unwrap();
            assert_eq!(plans.len(), 1);
            assert_eq!(plans[0].table_references().len(), 1);
            assert_eq!(
                plans[0].table_references()[0].access,
                TableReferenceAccess::Write
            );
        }
        assert!(matches!(
            bridge
                .async_execute(&format!("EXPLAIN {update}"))
                .await
                .unwrap(),
            ExecutionResult::Query(_)
        ));
        let ExecutionResult::Query(before) = bridge
            .async_execute("SELECT value FROM explained")
            .await
            .unwrap()
        else {
            panic!("query expected")
        };
        assert_eq!(before.rows, vec![vec![SqlValue::Integer(7)]]);
        let analyzed = bridge
            .async_execute(&format!("EXPLAIN ANALYZE {update}"))
            .await;
        if mode == TxnMode::ReadOnly {
            assert!(matches!(
                analyzed,
                Err(alopex_sql::executor::ExecutorError::ReadOnlyTransaction { .. })
            ));
        } else {
            assert!(matches!(analyzed.unwrap(), ExecutionResult::Query(_)));
        }
        let ExecutionResult::Query(after) = bridge
            .async_execute("SELECT value FROM explained")
            .await
            .unwrap()
        else {
            panic!("query expected")
        };
        assert_eq!(
            after.rows,
            vec![vec![SqlValue::Integer(if mode == TxnMode::ReadOnly {
                7
            } else {
                9
            })]]
        );
        bridge.async_rollback().await.unwrap();
    }
    let mut verify = AsyncTxnBridge::with_catalog(
        store.begin_async().await.unwrap(),
        TxnMode::ReadWrite,
        catalog,
    );
    let ExecutionResult::Query(rows) = verify
        .async_execute("SELECT value FROM explained")
        .await
        .unwrap()
    else {
        panic!("query expected")
    };
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(7)]]);
    verify.async_rollback().await.unwrap();
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
async fn ctas_and_dml_subqueries_honor_configured_read_parquet_paths() {
    let temp = tempdir().unwrap();
    let allowed_dir = temp.path().join("allowed");
    fs::create_dir(&allowed_dir).unwrap();
    let allowed_file = allowed_dir.join("rows.parquet");
    let outside_file = temp.path().join("outside.parquet");
    write_int_parquet(&allowed_file);
    write_int_parquet(&outside_file);
    let store = AsyncKVStoreAdapter::from_arc(Arc::new(MemoryKV::new()), TxnMode::ReadWrite);
    let catalog = build_catalog();
    let mut bridge = AsyncTxnBridge::with_catalog(
        store.begin_async().await.unwrap(),
        TxnMode::ReadWrite,
        catalog.clone(),
    );
    bridge.set_copy_security(CopySecurityConfig {
        allowed_base_dirs: Some(vec![allowed_dir.canonicalize().unwrap()]),
        allow_symlinks: false,
    });
    bridge
        .async_execute("CREATE TABLE target (id INT, value INT)")
        .await
        .unwrap();
    bridge
        .async_execute("INSERT INTO target VALUES (7, 1)")
        .await
        .unwrap();
    let allowed = allowed_file.display();
    bridge
        .async_execute(&format!(
            "CREATE TABLE copied AS SELECT * FROM read_parquet('{allowed}')"
        ))
        .await
        .unwrap();
    bridge
        .async_execute(&format!(
            "UPDATE target SET value = (SELECT id FROM read_parquet('{allowed}'))"
        ))
        .await
        .unwrap();
    let outside = outside_file.display();
    for sql in [
        format!("CREATE TABLE rejected AS SELECT * FROM read_parquet('{outside}')"),
        format!("UPDATE target SET value = (SELECT id FROM read_parquet('{outside}'))"),
        format!("DELETE FROM target WHERE id IN (SELECT id FROM read_parquet('{outside}'))"),
    ] {
        let error = bridge.async_execute(&sql).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("path not in allowed directories"),
            "{error}"
        );
    }
    assert!(!catalog.read().unwrap().table_exists("rejected"));
    let ExecutionResult::Query(query) = bridge
        .async_execute("SELECT value FROM target")
        .await
        .unwrap()
    else {
        panic!("expected unchanged target");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Integer(7)]]);
    bridge.async_rollback().await.unwrap();
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
    result.expect("stream cancellation must restore transaction ownership");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn async_execute_multi_cancellation_restores_transaction_ownership() {
    let store = Arc::new(MemoryKV::new());
    let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(store, TxnMode::ReadWrite));
    let block_next_put = Arc::new(AtomicBool::new(false));
    let put_started = Arc::new(tokio::sync::Notify::new());
    let release_put = Arc::new(tokio::sync::Notify::new());
    let block_rollback = Arc::new(AtomicBool::new(false));
    let rollback_started = Arc::new(tokio::sync::Notify::new());
    let release_rollback = Arc::new(tokio::sync::Notify::new());
    let rollback_finished = Arc::new(tokio::sync::Notify::new());
    let txn = GatedAsyncTransaction {
        inner: async_store.begin_async().await.expect("begin"),
        panic_next_put: Arc::new(AtomicBool::new(false)),
        block_next_put: Arc::clone(&block_next_put),
        put_started: Arc::clone(&put_started),
        release_put: Arc::clone(&release_put),
        commit_finished: Arc::new(tokio::sync::Notify::new()),
        block_rollback,
        rollback_started,
        release_rollback,
        rollback_finished,
    };
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, build_catalog());
    bridge
        .async_execute("CREATE TABLE cancellation_rows (id INT PRIMARY KEY)")
        .await
        .expect("create table");

    block_next_put.store(true, Ordering::SeqCst);
    let mut execution =
        Box::pin(bridge.async_execute_multi("INSERT INTO cancellation_rows VALUES (1)"));
    assert!(futures::poll!(execution.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(2), put_started.notified())
        .await
        .expect("put must start");
    drop(execution);
    let mut rollback = Box::pin(bridge.async_rollback());
    assert!(futures::poll!(rollback.as_mut()).is_pending());
    release_put.notify_one();

    tokio::time::timeout(Duration::from_secs(2), rollback)
        .await
        .expect("rollback must not hang")
        .expect("cancelled execution must restore transaction ownership");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn async_execute_multi_idle_barrier_waits_for_transaction_ownership() {
    let store = Arc::new(MemoryKV::new());
    let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(store, TxnMode::ReadWrite));
    let block_next_put = Arc::new(AtomicBool::new(false));
    let put_started = Arc::new(tokio::sync::Notify::new());
    let release_put = Arc::new(tokio::sync::Notify::new());
    let txn = GatedAsyncTransaction {
        inner: async_store.begin_async().await.expect("begin"),
        panic_next_put: Arc::new(AtomicBool::new(false)),
        block_next_put: Arc::clone(&block_next_put),
        put_started: Arc::clone(&put_started),
        release_put: Arc::clone(&release_put),
        commit_finished: Arc::new(tokio::sync::Notify::new()),
        block_rollback: Arc::new(AtomicBool::new(false)),
        rollback_started: Arc::new(tokio::sync::Notify::new()),
        release_rollback: Arc::new(tokio::sync::Notify::new()),
        rollback_finished: Arc::new(tokio::sync::Notify::new()),
    };
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, build_catalog());
    bridge
        .async_execute("CREATE TABLE idle_barrier_rows (id INT PRIMARY KEY)")
        .await
        .expect("create table");

    block_next_put.store(true, Ordering::SeqCst);
    let mut execution =
        Box::pin(bridge.async_execute_multi("INSERT INTO idle_barrier_rows VALUES (1)"));
    assert!(futures::poll!(execution.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(2), put_started.notified())
        .await
        .expect("put must start");
    drop(execution);

    let mut idle = Box::pin(bridge.async_wait_for_idle());
    assert!(futures::poll!(idle.as_mut()).is_pending());
    release_put.notify_one();
    tokio::time::timeout(Duration::from_secs(2), idle)
        .await
        .expect("idle barrier must finish after transaction ownership is restored")
        .expect("idle barrier");
    bridge.async_rollback().await.expect("rollback");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn cancelled_rollback_future_still_finishes_rollback() {
    let store = Arc::new(MemoryKV::new());
    let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(store, TxnMode::ReadWrite));
    let block_next_put = Arc::new(AtomicBool::new(false));
    let put_started = Arc::new(tokio::sync::Notify::new());
    let release_put = Arc::new(tokio::sync::Notify::new());
    let block_rollback = Arc::new(AtomicBool::new(true));
    let rollback_started = Arc::new(tokio::sync::Notify::new());
    let release_rollback = Arc::new(tokio::sync::Notify::new());
    let rollback_finished = Arc::new(tokio::sync::Notify::new());
    let txn = GatedAsyncTransaction {
        inner: async_store.begin_async().await.expect("begin"),
        panic_next_put: Arc::new(AtomicBool::new(false)),
        block_next_put: Arc::clone(&block_next_put),
        put_started: Arc::clone(&put_started),
        release_put: Arc::clone(&release_put),
        commit_finished: Arc::new(tokio::sync::Notify::new()),
        block_rollback,
        rollback_started: Arc::clone(&rollback_started),
        release_rollback: Arc::clone(&release_rollback),
        rollback_finished: Arc::clone(&rollback_finished),
    };
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, build_catalog());
    bridge
        .async_execute("CREATE TABLE rollback_cancellation_rows (id INT PRIMARY KEY)")
        .await
        .expect("create table");
    block_next_put.store(true, Ordering::SeqCst);
    let mut execution =
        Box::pin(bridge.async_execute_multi("INSERT INTO rollback_cancellation_rows VALUES (1)"));
    assert!(futures::poll!(execution.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(2), put_started.notified())
        .await
        .expect("put must start");
    drop(execution);

    let mut rollback = Box::pin(bridge.async_rollback());
    assert!(futures::poll!(rollback.as_mut()).is_pending());
    drop(rollback);
    release_put.notify_one();
    tokio::time::timeout(Duration::from_secs(2), rollback_started.notified())
        .await
        .expect("rollback must start");
    release_rollback.notify_one();
    tokio::time::timeout(Duration::from_secs(2), rollback_finished.notified())
        .await
        .expect("cancelled rollback future must not cancel rollback");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn cancelled_commit_future_still_finishes_commit() {
    let store = Arc::new(MemoryKV::new());
    let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(store, TxnMode::ReadWrite));
    let block_next_put = Arc::new(AtomicBool::new(false));
    let put_started = Arc::new(tokio::sync::Notify::new());
    let release_put = Arc::new(tokio::sync::Notify::new());
    let commit_finished = Arc::new(tokio::sync::Notify::new());
    let txn = GatedAsyncTransaction {
        inner: async_store.begin_async().await.expect("begin"),
        panic_next_put: Arc::new(AtomicBool::new(false)),
        block_next_put: Arc::clone(&block_next_put),
        put_started: Arc::clone(&put_started),
        release_put: Arc::clone(&release_put),
        commit_finished: Arc::clone(&commit_finished),
        block_rollback: Arc::new(AtomicBool::new(false)),
        rollback_started: Arc::new(tokio::sync::Notify::new()),
        release_rollback: Arc::new(tokio::sync::Notify::new()),
        rollback_finished: Arc::new(tokio::sync::Notify::new()),
    };
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, build_catalog());
    bridge
        .async_execute("CREATE TABLE commit_cancellation_rows (id INT PRIMARY KEY)")
        .await
        .expect("create table");
    block_next_put.store(true, Ordering::SeqCst);
    let mut execution =
        Box::pin(bridge.async_execute_multi("INSERT INTO commit_cancellation_rows VALUES (1)"));
    assert!(futures::poll!(execution.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(2), put_started.notified())
        .await
        .expect("put must start");
    drop(execution);

    let mut commit = Box::pin(bridge.async_commit());
    assert!(futures::poll!(commit.as_mut()).is_pending());
    drop(commit);
    release_put.notify_one();
    tokio::time::timeout(Duration::from_secs(2), commit_finished.notified())
        .await
        .expect("cancelled commit future must not cancel commit");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn blocking_executor_panic_restores_transaction_ownership() {
    let store = Arc::new(MemoryKV::new());
    let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(store, TxnMode::ReadWrite));
    let panic_next_put = Arc::new(AtomicBool::new(false));
    let txn = GatedAsyncTransaction {
        inner: async_store.begin_async().await.expect("begin"),
        panic_next_put: Arc::clone(&panic_next_put),
        block_next_put: Arc::new(AtomicBool::new(false)),
        put_started: Arc::new(tokio::sync::Notify::new()),
        release_put: Arc::new(tokio::sync::Notify::new()),
        commit_finished: Arc::new(tokio::sync::Notify::new()),
        block_rollback: Arc::new(AtomicBool::new(false)),
        rollback_started: Arc::new(tokio::sync::Notify::new()),
        release_rollback: Arc::new(tokio::sync::Notify::new()),
        rollback_finished: Arc::new(tokio::sync::Notify::new()),
    };
    let mut bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, build_catalog());
    bridge
        .async_execute("CREATE TABLE panic_rows (id INT PRIMARY KEY)")
        .await
        .expect("create table");

    panic_next_put.store(true, Ordering::SeqCst);
    let error = bridge
        .async_execute("INSERT INTO panic_rows VALUES (1)")
        .await
        .expect_err("controlled panic must become an execution error");
    assert!(error.to_string().contains("blocking task panicked"));
    bridge
        .async_rollback()
        .await
        .expect("panic must restore transaction ownership before returning");
}
