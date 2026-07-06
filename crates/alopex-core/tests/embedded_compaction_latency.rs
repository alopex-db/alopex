#![cfg(not(target_arch = "wasm32"))]

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_core::{Error, KVStore, KVTransaction, Result, TxnMode};
use common::{compare_v05_to_current, format_comparison_line, run_with_warmup_and_median};
use tempfile::tempdir;

const SEED_ROWS: usize = 192;
const WORKLOAD_OPS: usize = 120;
const HOT_KEY_SPACE: usize = 192;

fn lsm_test_config() -> LsmKVConfig {
    let mut cfg = LsmKVConfig::default();
    cfg.memtable.flush_threshold = 256 * 1024;
    cfg.memtable.max_immutable_count = 8;
    cfg.checkpoint.wal_size_threshold = 64 * 1024;
    cfg.checkpoint.min_interval_ms = 0;
    cfg
}

fn seed_store(store: &LsmKV) -> Result<()> {
    for i in 0..SEED_ROWS {
        let key = format!("seed:{i:05}").into_bytes();
        let value = format!("value:{i:05}:{}", i % 97).into_bytes();
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(key, value)?;
        txn.commit_self()?;
    }
    store.flush()?;
    let _ = store.checkpoint()?;
    Ok(())
}

fn run_fixed_workload(store: &LsmKV) -> Result<()> {
    for i in 0..WORKLOAD_OPS {
        let key = format!("hot:{:04}", i % HOT_KEY_SPACE).into_bytes();
        let value = format!("bench:{i:08}").into_bytes();

        let mut write_txn = store.begin(TxnMode::ReadWrite)?;
        write_txn.put(key.clone(), value)?;
        write_txn.commit_self()?;

        let mut read_txn = store.begin(TxnMode::ReadOnly)?;
        let _ = read_txn.get(&key)?;
    }
    Ok(())
}

fn maintenance_loop(store: Arc<LsmKV>, stop: Arc<AtomicBool>, error_tx: mpsc::Sender<String>) {
    while !stop.load(Ordering::Relaxed) {
        let step = store
            .flush()
            .and_then(|_| store.checkpoint().map(|_| ()))
            .and_then(|_| store.compact());
        if let Err(err) = step {
            let _ = error_tx.send(err.to_string());
            return;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

fn run_scenario(with_maintenance: bool) -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("latency.lsm");
    let (store, _) = LsmKV::open_with_config(&db_path, lsm_test_config())?;
    seed_store(&store)?;

    let store = Arc::new(store);
    let stop = Arc::new(AtomicBool::new(false));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let worker = if with_maintenance {
        let store_bg = Arc::clone(&store);
        let stop_bg = Arc::clone(&stop);
        Some(thread::spawn(move || {
            maintenance_loop(store_bg, stop_bg, error_tx);
        }))
    } else {
        None
    };

    let workload_result = run_fixed_workload(store.as_ref());
    stop.store(true, Ordering::Relaxed);
    if let Some(handle) = worker {
        if handle.join().is_err() {
            return Err(Error::InvalidFormat(
                "maintenance thread panicked during latency scenario".to_string(),
            ));
        }
    }

    if let Ok(msg) = error_rx.try_recv() {
        return Err(Error::InvalidFormat(format!(
            "maintenance thread failure: {msg}"
        )));
    }

    workload_result
}

#[cfg_attr(not(feature = "lane_perf"), ignore)]
#[test]
fn embedded_compaction_gc_latency_within_25_percent() {
    let baseline_median = run_with_warmup_and_median(|| run_scenario(false))
        .expect("baseline scenario should succeed");
    let maintenance_median = run_with_warmup_and_median(|| run_scenario(true))
        .expect("maintenance scenario should succeed");

    let comparison = compare_v05_to_current(baseline_median, maintenance_median);
    let line = format_comparison_line(&comparison);
    assert!(
        comparison.degradation_ratio <= 0.25,
        "expected degradation_ratio <= 0.25, got {line}"
    );
}
