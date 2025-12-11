mod common;

use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
use common::{
    begin_op, slo_presets, ExecutionModel, SloConfig, StressTestConfig, StressTestHarness,
    WorkloadConfig, WorkloadGenerator,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

const MAX_RETRIES: usize = 20;

fn concurrency_config(name: &str, model: ExecutionModel, concurrency: usize) -> StressTestConfig {
    let slo = match model {
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
    };
    StressTestConfig {
        name: name.to_string(),
        execution_model: model,
        concurrency,
        scenario_timeout: Duration::from_secs(60),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo,
    }
}

fn run_same_key(model: ExecutionModel) {
    let cfg = concurrency_config("same_key", model, 100);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    let result = match model {
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                let metrics = ctx.metrics.clone();
                let watchdog = ctx.watchdog.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..100 {
                        let metrics = metrics.clone();
                        let watchdog = watchdog.clone();
                        let store = store.clone();
                        set.spawn(async move {
                            for _ in 0..20 {
                                let mut attempts = 0;
                                loop {
                                    let _op = watchdog.begin_operation();
                                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                                    let value = format!("v{tid}").into_bytes();
                                    txn.put(b"shared".to_vec(), value.clone())?;
                                    match txn.commit_self() {
                                        Ok(_) => {
                                            metrics.record_success();
                                            break;
                                        }
                                        Err(CoreError::TxnConflict) if attempts < MAX_RETRIES => {
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
                for _ in 0..20 {
                    let store = store_sync.clone();
                    let mut attempts = 0;
                    loop {
                        let _op = begin_op(ctx);
                        let mut txn = store.begin(TxnMode::ReadWrite)?;
                        let value = format!("v{tid}").into_bytes();
                        txn.put(b"shared".to_vec(), value.clone())?;
                        match txn.commit_self() {
                            Ok(_) => {
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
                    std::thread::yield_now();
                }
                Ok(())
            })
        }
    };
    assert!(
        result.is_success(),
        "same_key {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_read_write_conflict(model: ExecutionModel) {
    let cfg = concurrency_config("rw_conflict", model, 50);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
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

fn run_long_short_mix(model: ExecutionModel) {
    let cfg = concurrency_config("long_short", model, 32);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
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

fn run_deadlock_watchdog(model: ExecutionModel) {
    let mut cfg = concurrency_config("deadlock_detection", model, 4);
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

fn run_backpressure(model: ExecutionModel) {
    let cfg = concurrency_config("backpressure", model, 64);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
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

fn run_recovery_after_spike(model: ExecutionModel) {
    let cfg = concurrency_config("recovery_spike", model, 32);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
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

#[test]
fn test_concurrent_same_key_write() {
    for model in [
        ExecutionModel::SyncSingle,
        ExecutionModel::SyncMulti,
        ExecutionModel::AsyncSingle,
        ExecutionModel::AsyncMulti,
    ] {
        run_same_key(model);
    }
}

#[test]
fn test_read_write_conflict() {
    for model in [
        ExecutionModel::SyncSingle,
        ExecutionModel::SyncMulti,
        ExecutionModel::AsyncSingle,
        ExecutionModel::AsyncMulti,
    ] {
        run_read_write_conflict(model);
    }
}

#[test]
fn test_long_short_transaction_mix() {
    for model in [
        ExecutionModel::SyncSingle,
        ExecutionModel::SyncMulti,
        ExecutionModel::AsyncSingle,
        ExecutionModel::AsyncMulti,
    ] {
        run_long_short_mix(model);
    }
}

#[test]
fn test_deadlock_detection() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_deadlock_watchdog(model);
    }
}

#[test]
fn test_backpressure_under_load() {
    for model in [
        ExecutionModel::SyncSingle,
        ExecutionModel::SyncMulti,
        ExecutionModel::AsyncSingle,
        ExecutionModel::AsyncMulti,
    ] {
        run_backpressure(model);
    }
}

#[test]
fn test_recovery_after_spike() {
    for model in [
        ExecutionModel::SyncSingle,
        ExecutionModel::SyncMulti,
        ExecutionModel::AsyncSingle,
        ExecutionModel::AsyncMulti,
    ] {
        run_recovery_after_spike(model);
    }
}
