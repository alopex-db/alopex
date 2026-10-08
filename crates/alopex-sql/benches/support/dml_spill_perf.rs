//! Separate async SQL resource-cost boundary; not the prepared SQL comparison.
use alopex_core::kv::AsyncKVStore;
use alopex_core::kv::async_adapter::AsyncKVStoreAdapter;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::memory::SpillMetricsSink;
use alopex_sql::executor::{AsyncExecutor, ExecutionResult, MemoryPolicy, SpillPolicy};
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use futures::StreamExt;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const SQL: &str =
    "DELETE FROM hw WHERE id IN (SELECT id FROM hw) AND id IN (SELECT v FROM hw) AND id<0";

#[derive(Default)]
struct Observation {
    bytes: AtomicU64,
    files: AtomicU64,
}

impl SpillMetricsSink for Observation {
    fn record_spill(&self, bytes: u64, files: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.files.fetch_add(files, Ordering::Relaxed);
    }
}

async fn run(engine: &str, n: i32, file: &mut std::fs::File, spill_parent: &str) -> Result<()> {
    for iteration in 0..8 {
        // Fixture, runtime construction, policy setup and transaction start are untimed.
        let store = Arc::new(AsyncKVStoreAdapter::from_arc(
            Arc::new(MemoryKV::new()),
            TxnMode::ReadWrite,
        ));
        let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> =
            Arc::new(RwLock::new(MemoryCatalog::new()));
        let bridge =
            AsyncTxnBridge::with_catalog(store.begin_async().await?, TxnMode::ReadWrite, catalog);
        let mut executor = AsyncExecutor::new(bridge);
        executor
            .execute_ddl_async("CREATE TABLE hw(id INTEGER PRIMARY KEY,v INTEGER)")
            .await?;
        for first in (1..=n).step_by(128) {
            let values = (first..=(first + 127).min(n))
                .map(|id| format!("({id},{id})"))
                .collect::<Vec<_>>()
                .join(",");
            executor
                .execute_dml_async(&format!("INSERT INTO hw VALUES {values}"))
                .await?;
        }
        let directory = tempfile::tempdir_in(spill_parent)?;
        let observation = Arc::new(Observation::default());
        let mut bridge = executor.into_inner();
        bridge.set_memory_policy(Some(
            MemoryPolicy::new(
                Some(4096),
                SpillPolicy::SpillToDisk {
                    directory: directory.path().to_path_buf(),
                },
            )
            .with_metrics(observation.clone()),
        ));
        let mut executor = AsyncExecutor::new(bridge);
        // Both engines include parsing/planning, async dispatch, execution and result consumption.
        let start = Instant::now();
        let result = executor.execute_dml_async(SQL).await?;
        let affected = match result {
            ExecutionResult::RowsAffected(count) => count,
            _ => return Err("unexpected DML result".into()),
        };
        std::hint::black_box(affected);
        let elapsed_ns = start.elapsed().as_nanos() as u64;
        let bytes = observation.bytes.load(Ordering::Relaxed);
        let files = observation.files.load(Ordering::Relaxed);
        let clean = std::fs::read_dir(directory.path())?.next().is_none();
        let mut bridge = executor.into_inner();
        bridge.set_memory_policy(None); // Verification is outside the 4096-byte statement boundary.
        let mut executor = AsyncExecutor::new(bridge);
        let rows = executor
            .execute_async("SELECT id,v FROM hw ORDER BY id")
            .collect::<Vec<_>>()
            .await;
        let original = rows.len() == n as usize
            && rows.iter().zip(1..=n).all(|(row, id)| {
                row.as_ref()
                    .is_ok_and(|row| row.values == vec![SqlValue::Integer(id); 2])
            });
        executor.into_inner().async_rollback().await?;
        let valid = affected == 0
            && original
            && clean
            && (engine != "fixed" || (files == 1 && bytes > 4096));
        serde_json::to_writer(
            &mut *file,
            &serde_json::json!({
                "kind":"sample", "engine":engine, "case":"two_membership_no_write", "rows":n,
                "phase":"async_sql_execute", "memory_limit_bytes":4096,
                "iteration":iteration, "warmup":iteration == 0, "elapsed_ns":elapsed_ns,
                "affected_rows":affected, "result_check":valid,
                "spill_bytes":bytes, "spill_files":files, "spill_cleanup":clean,
                "status":if valid { "pass" } else { "fail" }
            }),
        )?;
        writeln!(file)?;
        file.flush()?;
        file.sync_data()?;
        if !valid {
            return Err("result or spill contract mismatch".into());
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 5
        || !["baseline", "fixed"].contains(&args[0].as_str())
        || args[1] != "two_membership_no_write"
    {
        return Err("usage: dml_spill_perf baseline|fixed two_membership_no_write ROWS RAW_JSONL SPILL_PARENT".into());
    }
    let n = args[2].parse()?;
    if ![600, 1025].contains(&n) {
        return Err("rows must be 600 or 1025".into());
    }
    let mut file = std::fs::OpenOptions::new().append(true).open(&args[3])?;
    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?
        .block_on(run(&args[0], n, &mut file, &args[4]))
}
