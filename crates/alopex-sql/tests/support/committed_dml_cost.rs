#![cfg(feature = "tokio")]

use std::sync::{Arc, RwLock};
use std::time::Instant;

use alopex_core::Result as KvResult;
use alopex_core::async_runtime::{BoxFuture, BoxStream};
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{
    AsyncKVStore, AsyncKVTransaction, KeySearchCancellation, KeySearchPage, KeySearchRequest,
};
use alopex_core::types::TxnMode;
use alopex_core::types::{Key, Value};
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::AsyncExecutor;
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use alopex_sql::{AlopexDialect, ExecutionResult, Executor, Parser, Planner, SqlValue};

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

fn execute(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> ExecutionResult {
    let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
    assert_eq!(statements.len(), 1);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statements[0])
        .unwrap();
    executor.execute(plan).unwrap()
}

fn rows(result: ExecutionResult) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query")
    };
    result.rows
}

#[test]
#[ignore = "one engine/case per process; benchmark runner owns results"]
fn committed_dml_statement_cost() {
    let engine = std::env::var("ALOPEX_DML_COST_ENGINE").unwrap();
    let case = std::env::var("ALOPEX_DML_COST_CASE").unwrap();
    assert!(matches!(engine.as_str(), "sync" | "async"));
    let (sql, joined, delete) = match case.as_str() {
        "update" => ("UPDATE hw SET v=v+(SELECT 1) RETURNING id,v", false, false),
        "delete" => (
            "DELETE FROM hw WHERE EXISTS (SELECT 1) RETURNING id,v",
            false,
            true,
        ),
        "joined_update" => (
            "UPDATE hw SET v=s.v FROM source AS s WHERE hw.id=s.id RETURNING id,v",
            true,
            false,
        ),
        "joined_delete" => (
            "DELETE FROM hw USING source AS s WHERE hw.id=s.id RETURNING id,v",
            true,
            true,
        ),
        _ => panic!("unknown case"),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let a_barrier = barrier.clone();
        let a = tokio::task::spawn_blocking(move || a_barrier.wait());
        let b_barrier = barrier.clone();
        let b = tokio::task::spawn_blocking(move || b_barrier.wait());
        barrier.wait();
        a.await.unwrap();
        b.await.unwrap();
    });
    let warmup: usize = std::env::var("ALOPEX_DML_WARMUP")
        .unwrap_or_else(|_| "2".into())
        .parse()
        .unwrap();
    let measured: usize = std::env::var("ALOPEX_DML_SAMPLES")
        .unwrap_or_else(|_| "10".into())
        .parse()
        .unwrap();
    assert!(warmup > 0 && measured > 0 && measured <= 100);
    for sample in 0..warmup + measured {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(store.clone(), catalog.clone());
        execute(
            &mut executor,
            &catalog,
            "CREATE TABLE hw(id INTEGER PRIMARY KEY,v INTEGER)",
        );
        for first in (1..=600).step_by(128) {
            let values = (first..=(first + 127).min(600))
                .map(|id| format!("({id},{id})"))
                .collect::<Vec<_>>()
                .join(",");
            execute(
                &mut executor,
                &catalog,
                &format!("INSERT INTO hw VALUES {values}"),
            );
        }
        if joined {
            execute(
                &mut executor,
                &catalog,
                "CREATE TABLE source(id INTEGER,v INTEGER)",
            );
            execute(
                &mut executor,
                &catalog,
                "INSERT INTO source VALUES (1,1001),(599,1599),(999,1999)",
            );
        }
        let adapter = AsyncKVStoreAdapter::from_arc(store.clone(), TxnMode::ReadWrite);
        let async_catalog: Arc<RwLock<dyn Catalog + Send + Sync>> = catalog.clone();
        crate::measurement::reset();
        let started = Instant::now();
        let result = runtime.block_on(async {
            let txn = adapter.begin_async().await.unwrap();
            if engine == "sync" {
                let txn = AsyncOnlyTransaction(txn);
                assert!(!txn.supports_worker_tasks());
                let bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, async_catalog);
                let mut async_executor = AsyncExecutor::new(bridge);
                let result = async_executor.execute_dml_async(sql).await.unwrap();
                async_executor.into_inner().async_commit().await.unwrap();
                result
            } else {
                assert!(txn.supports_worker_tasks());
                let bridge = AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, async_catalog);
                let mut async_executor = AsyncExecutor::new(bridge);
                let result = async_executor.execute_dml_async(sql).await.unwrap();
                async_executor.into_inner().async_commit().await.unwrap();
                result
            }
        });
        let elapsed_ns = started.elapsed().as_nanos();
        let (peak_bytes, requested_bytes) = crate::measurement::finish();
        let returned = rows(result);
        let affected = if joined {
            vec![1, 599]
        } else {
            (1..=600).collect::<Vec<_>>()
        };
        let expected_returned = affected
            .iter()
            .map(|&id| {
                vec![
                    SqlValue::Integer(id),
                    SqlValue::Integer(if delete {
                        id
                    } else if joined {
                        id + 1000
                    } else {
                        id + 1
                    }),
                ]
            })
            .collect::<Vec<_>>();
        assert_eq!(returned, expected_returned);
        let final_rows = rows(execute(
            &mut executor,
            &catalog,
            "SELECT id,v FROM hw ORDER BY id",
        ));
        let expected_final = (1..=600)
            .filter(|id| !delete || (joined && !affected.contains(id)))
            .map(|id| {
                vec![
                    SqlValue::Integer(id),
                    SqlValue::Integer(if !delete && (!joined || affected.contains(&id)) {
                        if joined { id + 1000 } else { id + 1 }
                    } else {
                        id
                    }),
                ]
            })
            .collect::<Vec<_>>();
        assert_eq!(final_rows, expected_final);
        if sample >= warmup {
            println!(
                "DML_COST {}",
                serde_json::json!({"engine":engine,"case":case,"sample":sample-warmup,"rows":600,"elapsed_ns":elapsed_ns,"peak_bytes":peak_bytes,"requested_bytes":requested_bytes,"returned_rows":returned.len(),"final_rows":final_rows.len(),"oracle":"pass","policy":"unlimited","reference_facade":"AsyncTxnBridge_begin_execute_commit","warmup":warmup,"measured":measured,"execution_thread":"blocking_worker","cpu_count":1,"runtime_workers":0,"blocking_workers_max":2})
            );
        }
    }
}
