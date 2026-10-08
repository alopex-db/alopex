#![cfg(feature = "tokio")]

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::memory::SpillMetricsSink;
use alopex_sql::executor::{AsyncExecutor, ExecutionResult, MemoryPolicy, SpillPolicy};
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use futures::StreamExt;

type TestExecutor = AsyncExecutor<'static, AsyncTxnBridge<'static, AsyncKVTransactionAdapter>>;

async fn fixture(policy: MemoryPolicy) -> TestExecutor {
    let store = Arc::new(AsyncKVStoreAdapter::from_arc(
        Arc::new(MemoryKV::new()),
        TxnMode::ReadWrite,
    ));
    let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> =
        Arc::new(RwLock::new(MemoryCatalog::new()));
    let bridge = AsyncTxnBridge::with_catalog(
        store.begin_async().await.unwrap(),
        TxnMode::ReadWrite,
        catalog,
    );
    let mut executor = AsyncExecutor::new(bridge);
    executor
        .execute_ddl_async("CREATE TABLE hw (id INTEGER PRIMARY KEY, v INTEGER)")
        .await
        .unwrap();
    for first in (1..=600).step_by(128) {
        let values = (first..=(first + 127).min(600))
            .map(|id| format!("({id},{id})"))
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute_dml_async(&format!("INSERT INTO hw VALUES {values}"))
            .await
            .unwrap();
    }
    let mut bridge = executor.into_inner();
    bridge.set_memory_policy(Some(policy));
    AsyncExecutor::new(bridge)
}

async fn assert_original_rows(executor: TestExecutor) {
    let mut bridge = executor.into_inner();
    bridge.set_memory_policy(None);
    let mut executor = AsyncExecutor::new(bridge);
    let rows = executor
        .execute_async("SELECT id,v FROM hw ORDER BY id")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(rows.len(), 600);
    for (row, id) in rows.into_iter().zip(1..=600) {
        assert_eq!(row.unwrap().values, vec![SqlValue::Integer(id); 2]);
    }
    executor.into_inner().async_rollback().await.unwrap();
}

#[derive(Default)]
struct SpillObservation {
    files: AtomicU64,
    corrupt_directory: Option<PathBuf>,
}

impl SpillMetricsSink for SpillObservation {
    fn record_spill(&self, bytes: u64, files: u64) {
        assert!(bytes > 0);
        self.files.fetch_add(files, Ordering::Relaxed);
        if let Some(directory) = &self.corrupt_directory {
            // The real spool reports its flushed file before opening replay.
            // Change only the test-owned record header at that boundary.
            let entries = std::fs::read_dir(directory).unwrap().collect::<Vec<_>>();
            assert_eq!(entries.len(), 1);
            std::fs::OpenOptions::new()
                .write(true)
                .open(entries[0].as_ref().unwrap().path())
                .unwrap()
                .write_all(&u64::MAX.to_le_bytes())
                .unwrap();
        }
    }
}

#[tokio::test]
async fn dml_spill_preserves_rows_and_cleans_successful_update_and_delete() {
    let directory = tempfile::tempdir().unwrap();
    let observation = Arc::new(SpillObservation::default());
    let policy = MemoryPolicy::new(
        Some(4096),
        SpillPolicy::SpillToDisk {
            directory: directory.path().to_path_buf(),
        },
    )
    .with_metrics(observation.clone());
    let mut executor = fixture(policy).await;
    let result = executor
        .execute_dml_async("UPDATE hw SET v=v+(SELECT 1) RETURNING id,v")
        .await
        .unwrap();
    let ExecutionResult::Query(result) = result else {
        panic!("expected RETURNING rows")
    };
    assert_eq!(result.rows.len(), 600);
    for (row, id) in result.rows.iter().zip(1..=600) {
        assert_eq!(row, &vec![SqlValue::Integer(id), SqlValue::Integer(id + 1)]);
    }
    assert_eq!(observation.files.load(Ordering::Relaxed), 1);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    let result = executor
        .execute_dml_async("DELETE FROM hw WHERE EXISTS (SELECT 1) RETURNING id,v")
        .await
        .unwrap();
    let ExecutionResult::Query(result) = result else {
        panic!("expected RETURNING rows")
    };
    assert_eq!(result.rows.len(), 600);
    for (row, id) in result.rows.iter().zip(1..=600) {
        assert_eq!(row, &vec![SqlValue::Integer(id), SqlValue::Integer(id + 1)]);
    }
    assert_eq!(observation.files.load(Ordering::Relaxed), 2);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    executor.into_inner().async_rollback().await.unwrap();
}

#[tokio::test]
async fn async_joined_update_delete_preserve_returning_and_nonmatching_rows() {
    let mut executor = fixture(MemoryPolicy::new(None, SpillPolicy::FailFast)).await;
    executor
        .execute_ddl_async("CREATE TABLE source (id INTEGER, v INTEGER)")
        .await
        .unwrap();
    executor
        .execute_dml_async("INSERT INTO source VALUES (1,1001),(599,1599),(999,1999)")
        .await
        .unwrap();
    for sql in [
        "UPDATE hw SET v=source.v FROM source WHERE hw.id=source.id RETURNING id,v",
        "DELETE FROM hw USING source WHERE hw.id=source.id RETURNING id,v",
    ] {
        let result = executor.execute_dml_async(sql).await.unwrap();
        let ExecutionResult::Query(query) = result else {
            panic!("expected RETURNING rows")
        };
        assert_eq!(
            query.rows,
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(1001)],
                vec![SqlValue::Integer(599), SqlValue::Integer(1599)],
            ]
        );
    }
    let rows = executor
        .execute_async("SELECT id,v FROM hw ORDER BY id")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(rows.len(), 598);
    for (row, id) in rows
        .into_iter()
        .zip((1..=600).filter(|id| ![1, 599].contains(id)))
    {
        assert_eq!(row.unwrap().values, vec![SqlValue::Integer(id); 2]);
    }
    executor.into_inner().async_rollback().await.unwrap();
}

#[tokio::test]
async fn dml_spill_fail_fast_preserves_all_prior_writes() {
    let mut executor = fixture(MemoryPolicy::new(Some(4096), SpillPolicy::FailFast)).await;
    // Ordinary row-local DML must keep its batch path, not fill a statement spool.
    for sql in ["UPDATE hw SET v=v+1", "UPDATE hw SET v=v-1"] {
        assert!(matches!(
            executor.execute_dml_async(sql).await.unwrap(),
            ExecutionResult::RowsAffected(600)
        ));
    }
    let error = executor
        .execute_dml_async("UPDATE hw SET v=v+(SELECT 1)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("memory limit"), "{error}");
    assert_original_rows(executor).await;
}

#[tokio::test]
async fn subquery_cache_spills_shared_results_and_cleans_without_dml_writes() {
    let directory = tempfile::tempdir().unwrap();
    let observation = Arc::new(SpillObservation::default());
    let mut executor = fixture(
        MemoryPolicy::new(
            Some(4096),
            SpillPolicy::SpillToDisk {
                directory: directory.path().to_path_buf(),
            },
        )
        .with_metrics(observation.clone()),
    )
    .await;
    // No row reaches DML staging. Two result ranges share one cache file,
    // including appending the second result after probing the first range.
    let result = executor
        .execute_dml_async(
            "DELETE FROM hw WHERE id IN (SELECT id FROM hw) AND id IN (SELECT v FROM hw) AND id<0",
        )
        .await
        .unwrap();
    assert!(matches!(result, ExecutionResult::RowsAffected(0)));
    assert_eq!(observation.files.load(Ordering::Relaxed), 1);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    assert_original_rows(executor).await;
}

#[tokio::test]
async fn subquery_cache_honors_fail_fast_before_any_dml_write() {
    let mut executor = fixture(MemoryPolicy::new(Some(4096), SpillPolicy::FailFast)).await;
    let error = executor
        .execute_dml_async("DELETE FROM hw WHERE id IN (SELECT id FROM hw) AND id<0")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("memory limit"), "{error}");
    assert_original_rows(executor).await;
}

#[tokio::test]
async fn dml_spill_late_evaluation_error_cleans_file_without_applying() {
    let directory = tempfile::tempdir().unwrap();
    let mut executor = fixture(MemoryPolicy::new(
        Some(4096),
        SpillPolicy::SpillToDisk {
            directory: directory.path().to_path_buf(),
        },
    ))
    .await;
    let error = executor
        .execute_dml_async(
            "UPDATE hw SET v=(SELECT s.v FROM hw s WHERE s.id=hw.id OR (hw.id>512 AND s.id=1))+1",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple rows"), "{error}");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    assert_original_rows(executor).await;
}

#[tokio::test]
async fn dml_spill_rejects_corrupt_length_before_allocation_and_cleans_file() {
    let directory = tempfile::tempdir().unwrap();
    let observation = Arc::new(SpillObservation {
        files: AtomicU64::new(0),
        corrupt_directory: Some(directory.path().to_path_buf()),
    });
    let mut executor = fixture(
        MemoryPolicy::new(
            Some(4096),
            SpillPolicy::SpillToDisk {
                directory: directory.path().to_path_buf(),
            },
        )
        .with_metrics(observation.clone()),
    )
    .await;
    let error = executor
        .execute_dml_async("UPDATE hw SET v=v+(SELECT 1)")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("exceeds written maximum"),
        "{error}"
    );
    assert_eq!(observation.files.load(Ordering::Relaxed), 1);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    assert_original_rows(executor).await;
}
