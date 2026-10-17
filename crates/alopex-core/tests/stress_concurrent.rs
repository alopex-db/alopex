#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::kv::AnyKV;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::LsmKVConfig;
use alopex_core::{
    Error as CoreError, KVStore, KVTransaction, Result as CoreResult, StorageFactory, StorageMode,
    TxnMode,
};
use common::{
    begin_op, selected_storage_modes, slo_presets, ExecutionModel, Lane, SloConfig,
    StressStorageMode, StressTestConfig, StressTestHarness, WorkloadConfig, WorkloadGenerator,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::task::JoinSet;

const MAX_RETRIES: usize = 20;

// All hot-key retries share the existing 60-second scenario budget. The
// harness's separate five-second setting detects lack of global progress;
// it is not a five-second guarantee for each logical write. A finite retry
// count can expire while durable writers still make progress. Count every
// conflict and stop starting attempts when the shared scenario budget expires.
const SAME_KEY_SCENARIO_TIMEOUT: Duration = Duration::from_secs(60);

fn same_key_retry_delay(worker: usize, retry: usize, elapsed: Duration) -> Option<Duration> {
    let remaining = SAME_KEY_SCENARIO_TIMEOUT.checked_sub(elapsed)?;
    if remaining.is_zero() {
        return None;
    }
    let ceiling_us = 250_u64 << retry.saturating_sub(1).min(6);
    // Distribute the whole interval rather than clustering later retries in
    // the first few milliseconds. The seed is deterministic for reproduction.
    let mut mixed = (worker as u64).wrapping_add((retry as u64).wrapping_mul(0x9e3779b97f4a7c15));
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
    mixed ^= mixed >> 31;
    Some(Duration::from_micros(1 + mixed % ceiling_us).min(remaining))
}

#[test]
fn same_key_retry_stops_at_deadline() {
    assert_eq!(same_key_retry_delay(0, 20, SAME_KEY_SCENARIO_TIMEOUT), None);
    assert_eq!(
        same_key_retry_delay(0, 1, SAME_KEY_SCENARIO_TIMEOUT + Duration::from_nanos(1)),
        None
    );
    assert_eq!(
        same_key_retry_delay(0, 20, SAME_KEY_SCENARIO_TIMEOUT - Duration::from_nanos(1)),
        Some(Duration::from_nanos(1))
    );
}

#[test]
fn same_key_retry_spreads_workers_across_the_late_window() {
    let delays: std::collections::HashSet<_> = (0..100)
        .map(|worker| same_key_retry_delay(worker, 20, Duration::ZERO).unwrap())
        .collect();
    assert!(delays.len() > 90);
    assert!(delays.iter().any(|delay| *delay < Duration::from_millis(8)));
    assert!(delays
        .iter()
        .any(|delay| *delay > Duration::from_millis(14)));
}

proptest::proptest! {
    #[test]
    fn same_key_retry_delays_are_bounded(
        worker in proptest::prelude::any::<usize>(),
        retry in proptest::prelude::any::<usize>(),
        elapsed_ns in 0_u64..61_000_000_000,
    ) {
        let elapsed = Duration::from_nanos(elapsed_ns);
        match same_key_retry_delay(worker, retry, elapsed) {
            Some(delay) => {
                proptest::prop_assert!(!delay.is_zero());
                proptest::prop_assert!(delay <= Duration::from_millis(16));
                proptest::prop_assert!(elapsed + delay <= SAME_KEY_SCENARIO_TIMEOUT);
            }
            None => proptest::prop_assert!(elapsed >= SAME_KEY_SCENARIO_TIMEOUT),
        }
    }
}

fn concurrency_config(
    name: &str,
    model: ExecutionModel,
    concurrency: usize,
    mode: StressStorageMode,
) -> StressTestConfig {
    let slo = if mode == StressStorageMode::Disk {
        (name == "same_key").then_some(SloConfig {
            max_error_ratio: Some(0.0),
            p9999_max_latency: Some(SAME_KEY_SCENARIO_TIMEOUT),
            // The scenario already has a 60-second budget for 2,000 writes.
            min_throughput: Some(2000.0 / 60.0),
            ..SloConfig::default()
        })
    } else {
        match model {
            ExecutionModel::SyncSingle => Some(SloConfig {
                min_throughput: Some(200.0),
                p99_max_latency: Some(Duration::from_secs(1)),
                p95_max_latency: Some(Duration::from_millis(800)),
                p999_max_latency: Some(Duration::from_secs(2)),
                p9999_max_latency: Some(Duration::from_secs(3)),
                max_error_ratio: Some(0.05),
            }),
            ExecutionModel::SyncMulti => Some(SloConfig {
                min_throughput: Some(500.0),
                p99_max_latency: Some(Duration::from_millis(400)),
                p95_max_latency: Some(Duration::from_millis(300)),
                p999_max_latency: Some(Duration::from_millis(900)),
                p9999_max_latency: Some(Duration::from_millis(1200)),
                max_error_ratio: Some(0.02),
            }),
            _ => slo_presets::get("concurrency"),
        }
    };
    StressTestConfig {
        name: format!("{name}_{}", mode.as_str()),
        lane: Lane::Nightly,
        execution_model: model,
        concurrency,
        scenario_timeout: SAME_KEY_SCENARIO_TIMEOUT,
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo,
    }
}

#[test]
fn same_key_disk_rejects_incomplete_or_slow_results() {
    let config = concurrency_config(
        "same_key",
        ExecutionModel::SyncMulti,
        100,
        StressStorageMode::Disk,
    );
    let slo = config
        .slo
        .expect("disk same-key workload requires an acceptance contract");
    for case in ["success", "error", "latency", "throughput"] {
        let metrics = common::MetricsCollector::new();
        for index in 0..2000 {
            if case == "error" && index == 1999 {
                metrics.record_error();
            } else {
                metrics.record_success();
            }
            metrics.record_latency(if case == "latency" && index == 1999 {
                SAME_KEY_SCENARIO_TIMEOUT + Duration::from_nanos(1)
            } else {
                Duration::from_millis(1)
            });
        }
        let window = if case == "throughput" {
            Duration::from_secs(61)
        } else {
            Duration::from_secs(60)
        };
        let result = metrics.verify_slo(&slo, &metrics.summary(window));
        assert_eq!(result.passed, case == "success", "{case}");
    }
}

fn new_shared_store(mode: StressStorageMode) -> CoreResult<(Arc<AnyKV>, Option<TempDir>)> {
    match mode {
        StressStorageMode::Memory => {
            Ok((Arc::new(AnyKV::Memory(alopex_core::MemoryKV::new())), None))
        }
        StressStorageMode::Disk => {
            let dir = TempDir::new()?;
            let cfg = LsmKVConfig {
                wal: WalConfig {
                    sync_mode: SyncMode::NoSync,
                    ..WalConfig::default()
                },
                ..LsmKVConfig::default()
            };
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg),
            })?;
            Ok((Arc::new(store), Some(dir)))
        }
    }
}

fn run_same_key(model: ExecutionModel, mode: StressStorageMode) {
    let cfg = concurrency_config("same_key", model, 100, mode);
    let harness = StressTestHarness::new(cfg).unwrap();
    let (store, _store_guard) = new_shared_store(mode).unwrap();
    let conflicts = Arc::new(AtomicU64::new(0));
    let scenario_started = Instant::now();
    let result = match model {
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            let conflicts_async = conflicts.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                let metrics = ctx.metrics.clone();
                let watchdog = ctx.watchdog.clone();
                let conflicts = conflicts_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..100 {
                        let metrics = metrics.clone();
                        let watchdog = watchdog.clone();
                        let store = store.clone();
                        let conflicts = conflicts.clone();
                        set.spawn(async move {
                            for _ in 0..20 {
                                let started = Instant::now();
                                let mut attempts = 0;
                                loop {
                                    if scenario_started.elapsed() >= SAME_KEY_SCENARIO_TIMEOUT {
                                        metrics.record_latency(started.elapsed());
                                        metrics.record_error();
                                        break;
                                    }
                                    let _op = watchdog.begin_operation();
                                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                                    let value = format!("v{tid}").into_bytes();
                                    txn.put(b"shared".to_vec(), value.clone())?;
                                    match txn.commit_self() {
                                        Ok(_) => {
                                            metrics.record_latency(started.elapsed());
                                            metrics.record_success();
                                            break;
                                        }
                                        Err(CoreError::TxnConflict)
                                            if scenario_started.elapsed()
                                                < SAME_KEY_SCENARIO_TIMEOUT =>
                                        {
                                            conflicts.fetch_add(1, Ordering::Relaxed);
                                            attempts += 1;
                                            if let Some(delay) = same_key_retry_delay(
                                                tid,
                                                attempts,
                                                scenario_started.elapsed(),
                                            ) {
                                                tokio::time::sleep(delay).await;
                                            }
                                            continue;
                                        }
                                        Err(CoreError::TxnConflict) => {
                                            conflicts.fetch_add(1, Ordering::Relaxed);
                                            metrics.record_latency(started.elapsed());
                                            metrics.record_error();
                                            break;
                                        }
                                        Err(e) => return Err(e),
                                    }
                                }
                                tokio::task::yield_now().await;
                            }
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(std::io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
        _ => {
            let store_sync = store.clone();
            let conflicts_sync = conflicts.clone();
            harness.run_concurrent(move |tid, ctx| {
                for _ in 0..20 {
                    let started = Instant::now();
                    let store = store_sync.clone();
                    let mut attempts = 0;
                    loop {
                        if scenario_started.elapsed() >= SAME_KEY_SCENARIO_TIMEOUT {
                            ctx.metrics.record_latency(started.elapsed());
                            ctx.metrics.record_error();
                            break;
                        }
                        let _op = begin_op(ctx);
                        let mut txn = store.begin(TxnMode::ReadWrite)?;
                        let value = format!("v{tid}").into_bytes();
                        txn.put(b"shared".to_vec(), value.clone())?;
                        match txn.commit_self() {
                            Ok(_) => {
                                ctx.metrics.record_latency(started.elapsed());
                                ctx.metrics.record_success();
                                break;
                            }
                            Err(CoreError::TxnConflict)
                                if scenario_started.elapsed() < SAME_KEY_SCENARIO_TIMEOUT =>
                            {
                                conflicts_sync.fetch_add(1, Ordering::Relaxed);
                                attempts += 1;
                                if let Some(delay) =
                                    same_key_retry_delay(tid, attempts, scenario_started.elapsed())
                                {
                                    std::thread::sleep(delay);
                                }
                                continue;
                            }
                            Err(CoreError::TxnConflict) => {
                                conflicts_sync.fetch_add(1, Ordering::Relaxed);
                                ctx.metrics.record_latency(started.elapsed());
                                ctx.metrics.record_error();
                                break;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    std::thread::yield_now();
                }
                Ok(())
            })
        }
    };
    eprintln!(
        "same_key mode={} model={model:?} conflicts={} exhausted={} successes={} latency_samples={} error={:?}",
        mode.as_str(),
        conflicts.load(Ordering::Relaxed),
        result.metrics.errors,
        result.metrics.successes,
        result.metrics.sample_count,
        result.error,
    );
    if !result.skipped {
        assert_eq!(result.metrics.successes + result.metrics.errors, 100 * 20);
        assert_eq!(result.metrics.sample_count, 100 * 20);
        assert_eq!(
            result.metrics.errors, 0,
            "same-key writes exceeded their retry deadline"
        );
    }
    assert!(
        result.is_success(),
        "same_key {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_read_write_conflict(model: ExecutionModel, mode: StressStorageMode) {
    let cfg = concurrency_config("rw_conflict", model, 50, mode);
    let harness = StressTestHarness::new(cfg).unwrap();
    let (store, _store_guard) = new_shared_store(mode).unwrap();
    let result = match model {
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let metrics = ctx.metrics.clone();
                let watchdog = ctx.watchdog.clone();
                let store = store_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..50 {
                        let metrics = metrics.clone();
                        let watchdog = watchdog.clone();
                        let store = store.clone();
                        set.spawn(async move {
                            for _ in 0..20 {
                                if tid % 2 == 0 {
                                    let mut attempts = 0;
                                    loop {
                                        let _op = watchdog.begin_operation();
                                        let mut txn = store.begin(TxnMode::ReadWrite)?;
                                        txn.put(b"hot".to_vec(), b"value".to_vec())?;
                                        match txn.commit_self() {
                                            Ok(_) => {
                                                metrics.record_success();
                                                let mut reader = store.begin(TxnMode::ReadOnly)?;
                                                let _ = reader.get(&b"hot".to_vec())?;
                                                metrics.record_success();
                                                break;
                                            }
                                            Err(CoreError::TxnConflict)
                                                if attempts < MAX_RETRIES =>
                                            {
                                                attempts += 1;
                                                tokio::task::yield_now().await;
                                                continue;
                                            }
                                            Err(CoreError::TxnConflict) => {
                                                metrics.record_error();
                                                break;
                                            }
                                            Err(e) => return Err(e),
                                        }
                                    }
                                } else {
                                    let _op = watchdog.begin_operation();
                                    let mut txn = store.begin(TxnMode::ReadOnly)?;
                                    let _ = txn.get(&b"hot".to_vec())?;
                                    metrics.record_success();
                                    let _ = txn.get(&b"hot".to_vec())?;
                                    metrics.record_success();
                                }
                            }
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(std::io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
        _ => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                for _ in 0..20 {
                    let store = store_sync.clone();
                    if tid % 2 == 0 {
                        let mut attempts = 0;
                        loop {
                            let _op = begin_op(ctx);
                            let mut txn = store.begin(TxnMode::ReadWrite)?;
                            txn.put(b"hot".to_vec(), b"value".to_vec())?;
                            match txn.commit_self() {
                                Ok(_) => {
                                    ctx.metrics.record_success();
                                    let mut reader = store.begin(TxnMode::ReadOnly)?;
                                    let _ = reader.get(&b"hot".to_vec())?;
                                    ctx.metrics.record_success();
                                    break;
                                }
                                Err(CoreError::TxnConflict) if attempts < MAX_RETRIES => {
                                    attempts += 1;
                                    std::thread::yield_now();
                                    continue;
                                }
                                Err(CoreError::TxnConflict) => {
                                    ctx.metrics.record_error();
                                    break;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    } else {
                        let _op = begin_op(ctx);
                        let mut txn = store.begin(TxnMode::ReadOnly)?;
                        let _ = txn.get(&b"hot".to_vec())?;
                        ctx.metrics.record_success();
                        let _ = txn.get(&b"hot".to_vec())?;
                        ctx.metrics.record_success();
                    }
                    std::thread::yield_now();
                }
                Ok(())
            })
        }
    };
    assert!(
        result.is_success(),
        "rw_conflict {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_long_short_mix(model: ExecutionModel, mode: StressStorageMode) {
    let cfg = concurrency_config("long_short", model, 32, mode);
    let harness = StressTestHarness::new(cfg).unwrap();
    let (store, _store_guard) = new_shared_store(mode).unwrap();
    let result = match model {
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let metrics = ctx.metrics.clone();
                let watchdog = ctx.watchdog.clone();
                let store = store_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..32 {
                        let metrics = metrics.clone();
                        let watchdog = watchdog.clone();
                        let store = store.clone();
                        set.spawn(async move {
                            for i in 0..20 {
                                let _op = watchdog.begin_operation();
                                let store = store.clone();
                                if tid % 4 == 0 {
                                    tokio::time::sleep(Duration::from_millis(2)).await;
                                }
                                let mut txn = store.begin(TxnMode::ReadWrite)?;
                                let key_primary = format!("k{tid}_{i}").into_bytes();
                                let key_shadow = format!("k{tid}_s{i}").into_bytes();
                                txn.put(key_primary, b"v".to_vec())?;
                                txn.put(key_shadow, b"v2".to_vec())?;
                                txn.commit_self()?;
                                metrics.record_success();
                                metrics.record_success();
                                tokio::task::yield_now().await;
                            }
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(std::io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
        _ => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                for i in 0..20 {
                    let _op = begin_op(ctx);
                    let store = store_sync.clone();
                    if tid % 4 == 0 {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    let key_primary = format!("k{tid}_{i}").into_bytes();
                    let key_shadow = format!("k{tid}_s{i}").into_bytes();
                    txn.put(key_primary, b"v".to_vec())?;
                    txn.put(key_shadow, b"v2".to_vec())?;
                    txn.commit_self()?;
                    ctx.metrics.record_success();
                    ctx.metrics.record_success();
                }
                Ok(())
            })
        }
    };
    assert!(
        result.is_success(),
        "long_short {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_deadlock_watchdog(model: ExecutionModel, mode: StressStorageMode) {
    let mut cfg = concurrency_config("deadlock_detection", model, 4, mode);
    cfg.operation_timeout = Duration::from_millis(50);
    let harness = StressTestHarness::new(cfg).unwrap();
    let result = harness.run_concurrent(|_tid, ctx| {
        let _op = begin_op(ctx);
        // ブロック時間を十分に取り、ウォッチドッグのチェック間隔(1s)を超える。
        std::thread::sleep(Duration::from_millis(1500));
        Ok(())
    });
    assert!(
        !result.is_success(),
        "deadlock detection should trip watchdog for {:?}",
        model
    );
}

fn run_backpressure(model: ExecutionModel, mode: StressStorageMode) {
    let cfg = concurrency_config("backpressure", model, 64, mode);
    let harness = StressTestHarness::new(cfg).unwrap();
    let (store, _store_guard) = new_shared_store(mode).unwrap();
    let result = match model {
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let metrics = ctx.metrics.clone();
                let watchdog = ctx.watchdog.clone();
                let store = store_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..64 {
                        let metrics = metrics.clone();
                        let watchdog = watchdog.clone();
                        let store = store.clone();
                        set.spawn(async move {
                            let mut generator = WorkloadGenerator::new(WorkloadConfig {
                                operation_count: 20,
                                key_space_size: 500,
                                value_size: 64,
                                seed: tid as u64,
                            });
                            for op in generator.generate_batch() {
                                match op {
                                    common::Operation::Get(k) => {
                                        let _op = watchdog.begin_operation();
                                        let mut txn = store.begin(TxnMode::ReadOnly)?;
                                        let _ = txn.get(&k)?;
                                        drop(_op);
                                        metrics.record_success();
                                    }
                                    common::Operation::Put(k, v) => {
                                        let key = k;
                                        let val = v;
                                        let mut attempts = 0;
                                        loop {
                                            let _op = watchdog.begin_operation();
                                            let mut txn = store.begin(TxnMode::ReadWrite)?;
                                            txn.put(key.clone(), val.clone())?;
                                            match txn.commit_self() {
                                                Ok(_) => {
                                                    drop(_op);
                                                    metrics.record_success();
                                                    break;
                                                }
                                                Err(CoreError::TxnConflict)
                                                    if attempts < MAX_RETRIES =>
                                                {
                                                    attempts += 1;
                                                    tokio::task::yield_now().await;
                                                    continue;
                                                }
                                                Err(CoreError::TxnConflict) => {
                                                    metrics.record_error();
                                                    break;
                                                }
                                                Err(e) => return Err(e),
                                            }
                                        }
                                    }
                                    common::Operation::Delete(k) => {
                                        let mut attempts = 0;
                                        let key = k;
                                        loop {
                                            let _op = watchdog.begin_operation();
                                            let mut txn = store.begin(TxnMode::ReadWrite)?;
                                            txn.delete(key.clone())?;
                                            match txn.commit_self() {
                                                Ok(_) => {
                                                    drop(_op);
                                                    metrics.record_success();
                                                    break;
                                                }
                                                Err(CoreError::TxnConflict)
                                                    if attempts < MAX_RETRIES =>
                                                {
                                                    attempts += 1;
                                                    tokio::task::yield_now().await;
                                                    continue;
                                                }
                                                Err(CoreError::TxnConflict) => {
                                                    metrics.record_error();
                                                    break;
                                                }
                                                Err(e) => return Err(e),
                                            }
                                        }
                                    }
                                    common::Operation::Scan(prefix) => {
                                        let _op = watchdog.begin_operation();
                                        let mut txn = store.begin(TxnMode::ReadOnly)?;
                                        let _ = txn.scan_prefix(&prefix)?;
                                        drop(_op);
                                        metrics.record_success();
                                    }
                                }
                                tokio::task::yield_now().await;
                            }
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(std::io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
        _ => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                let store = store_sync.clone();
                let mut generator = WorkloadGenerator::new(WorkloadConfig {
                    operation_count: 20,
                    key_space_size: 500,
                    value_size: 64,
                    seed: tid as u64,
                });
                for op in generator.generate_batch() {
                    match op {
                        common::Operation::Get(k) => {
                            let _op = begin_op(ctx);
                            let mut txn = store.begin(TxnMode::ReadOnly)?;
                            let _ = txn.get(&k)?;
                            drop(_op);
                            ctx.metrics.record_success();
                        }
                        common::Operation::Put(k, v) => {
                            let key = k;
                            let val = v;
                            let mut attempts = 0;
                            loop {
                                let _op = begin_op(ctx);
                                let mut txn = store.begin(TxnMode::ReadWrite)?;
                                txn.put(key.clone(), val.clone())?;
                                match txn.commit_self() {
                                    Ok(_) => {
                                        drop(_op);
                                        ctx.metrics.record_success();
                                        break;
                                    }
                                    Err(CoreError::TxnConflict) if attempts < MAX_RETRIES => {
                                        attempts += 1;
                                        std::thread::yield_now();
                                        continue;
                                    }
                                    Err(CoreError::TxnConflict) => {
                                        ctx.metrics.record_error();
                                        break;
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                        }
                        common::Operation::Delete(k) => {
                            let key = k;
                            let mut attempts = 0;
                            loop {
                                let _op = begin_op(ctx);
                                let mut txn = store.begin(TxnMode::ReadWrite)?;
                                txn.delete(key.clone())?;
                                match txn.commit_self() {
                                    Ok(_) => {
                                        drop(_op);
                                        ctx.metrics.record_success();
                                        break;
                                    }
                                    Err(CoreError::TxnConflict) if attempts < MAX_RETRIES => {
                                        attempts += 1;
                                        std::thread::yield_now();
                                        continue;
                                    }
                                    Err(CoreError::TxnConflict) => {
                                        ctx.metrics.record_error();
                                        break;
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                        }
                        common::Operation::Scan(prefix) => {
                            let _op = begin_op(ctx);
                            let mut txn = store.begin(TxnMode::ReadOnly)?;
                            let _ = txn.scan_prefix(&prefix)?;
                            drop(_op);
                            ctx.metrics.record_success();
                        }
                    }
                }
                Ok(())
            })
        }
    };
    assert!(
        result.is_success(),
        "backpressure {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_recovery_after_spike(model: ExecutionModel, mode: StressStorageMode) {
    let cfg = concurrency_config("recovery_spike", model, 32, mode);
    let harness = StressTestHarness::new(cfg).unwrap();
    let (store, _store_guard) = new_shared_store(mode).unwrap();
    let result = match model {
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let metrics = ctx.metrics.clone();
                let watchdog = ctx.watchdog.clone();
                let store = store_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..32 {
                        let metrics = metrics.clone();
                        let watchdog = watchdog.clone();
                        let store = store.clone();
                        set.spawn(async move {
                            // spike phase
                            for i in 0..50 {
                                let _op = watchdog.begin_operation();
                                let mut txn = store.begin(TxnMode::ReadWrite)?;
                                let key = format!("spike_{tid}_{i}").into_bytes();
                                txn.put(key, b"v".to_vec())?;
                                txn.commit_self()?;
                                drop(_op);
                                metrics.record_success();
                            }
                            // recovery phase: read a sample
                            let _op = watchdog.begin_operation();
                            let mut txn = store.begin(TxnMode::ReadOnly)?;
                            let _ = txn.get(&b"spike_0_0".to_vec())?;
                            drop(_op);
                            metrics.record_success();
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(std::io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
        _ => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                let store = store_sync.clone();
                // spike phase
                for i in 0..50 {
                    let _op = begin_op(ctx);
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    let key = format!("spike_{tid}_{i}").into_bytes();
                    txn.put(key, b"v".to_vec())?;
                    txn.commit_self()?;
                    drop(_op);
                    ctx.metrics.record_success();
                }
                // recovery phase: read a sample
                let _op = begin_op(ctx);
                let mut txn = store.begin(TxnMode::ReadOnly)?;
                let _ = txn.get(&b"spike_0_0".to_vec())?;
                drop(_op);
                ctx.metrics.record_success();
                Ok(())
            })
        }
    };
    assert!(
        result.is_success(),
        "recovery_after_spike {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_concurrent_same_key_write() {
    for mode in selected_storage_modes() {
        for model in [
            ExecutionModel::SyncSingle,
            ExecutionModel::SyncMulti,
            ExecutionModel::AsyncSingle,
            ExecutionModel::AsyncMulti,
        ] {
            run_same_key(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_read_write_conflict() {
    for mode in selected_storage_modes() {
        for model in [
            ExecutionModel::SyncSingle,
            ExecutionModel::SyncMulti,
            ExecutionModel::AsyncSingle,
            ExecutionModel::AsyncMulti,
        ] {
            run_read_write_conflict(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_long_short_transaction_mix() {
    for mode in selected_storage_modes() {
        for model in [
            ExecutionModel::SyncSingle,
            ExecutionModel::SyncMulti,
            ExecutionModel::AsyncSingle,
            ExecutionModel::AsyncMulti,
        ] {
            run_long_short_mix(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_deadlock_detection() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_deadlock_watchdog(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_backpressure_under_load() {
    for mode in selected_storage_modes() {
        for model in [
            ExecutionModel::SyncSingle,
            ExecutionModel::SyncMulti,
            ExecutionModel::AsyncSingle,
            ExecutionModel::AsyncMulti,
        ] {
            run_backpressure(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_recovery_after_spike() {
    for mode in selected_storage_modes() {
        for model in [
            ExecutionModel::SyncSingle,
            ExecutionModel::SyncMulti,
            ExecutionModel::AsyncSingle,
            ExecutionModel::AsyncMulti,
        ] {
            run_recovery_after_spike(model, mode);
        }
    }
}
