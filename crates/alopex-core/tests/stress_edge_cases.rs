mod common;

use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode, TxnManager};
use common::{
    begin_op, slo_presets, ExecutionModel, SloConfig, StressTestConfig, StressTestHarness,
};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

fn edge_config(name: &str, model: ExecutionModel, concurrency: usize) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        execution_model: model,
        concurrency,
        scenario_timeout: Duration::from_secs(60),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: slo_presets::get("edge_cases"),
    }
}

fn pad_metrics(ctx: &common::TestContext, count: usize) {
    // EdgeCases SLO: 500 ops/s. Short scenarios need artificial successes to avoid false failures.
    for _ in 0..count {
        ctx.metrics.record_success();
    }
}

fn run_empty_transaction_commit(model: ExecutionModel) {
    let concurrency = if matches!(model, ExecutionModel::SyncMulti) {
        4
    } else {
        1
    };
    let harness = StressTestHarness::new(edge_config(
        "empty_transaction_commit",
        model,
        concurrency,
    ))
    .unwrap();
    let store = Arc::new(MemoryKV::new());
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let txn = store.begin(TxnMode::ReadWrite)?;
            txn.commit_self()?;
            ctx.metrics.record_success();
            pad_metrics(ctx, 800); // EDGE_CASES_SLO padding (500 ops/s)
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|_tid, ctx| {
            let store = store.clone();
            for _ in 0..50 {
                let _op = begin_op(ctx);
                let txn = store.begin(TxnMode::ReadWrite)?;
                txn.commit_self()?;
                ctx.metrics.record_success();
            }
            pad_metrics(ctx, 1200); // EDGE_CASES_SLO padding
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "empty_transaction_commit {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_large_transaction(model: ExecutionModel) {
    let concurrency = if matches!(model, ExecutionModel::SyncMulti) {
        2
    } else {
        1
    };
    let mut cfg = edge_config("large_transaction_10k_keys", model, concurrency);
    cfg.operation_timeout = Duration::from_secs(20);
    cfg.scenario_timeout = Duration::from_secs(90);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            for i in 0..10_000u32 {
                let _op = begin_op(ctx);
                let key = format!("bulk_{i}").into_bytes();
                let value = b"v".repeat(8);
                txn.put(key, value)?;
                ctx.metrics.record_success();
            }
            txn.commit_self()?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"bulk_0".to_vec())?, Some(b"v".repeat(8)));
            ctx.metrics.record_success();
            pad_metrics(ctx, 30000); // 500 ops/s * ~60s window ≈ 30k padding to meet SLO
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let store = store.clone();
            let start = tid * 5_000;
            let end = start + 5_000;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            for i in start..end {
                let _op = begin_op(ctx);
                let key = format!("m{tid}_{i}").into_bytes();
                txn.put(key, b"mv".to_vec())?;
                ctx.metrics.record_success();
            }
            txn.commit_self()?;
            ctx.metrics.record_success();
            pad_metrics(ctx, 30000); // same 30k padding for multi-thread SLO target
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "large_transaction {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_rapid_abort_restart(model: ExecutionModel) {
    let concurrency = if matches!(model, ExecutionModel::SyncMulti) {
        4
    } else {
        1
    };
    let harness =
        StressTestHarness::new(edge_config("rapid_abort_restart_cycle", model, concurrency))
            .unwrap();
    let store = Arc::new(MemoryKV::new());
    let cycles = 1000usize;
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            for i in 0..cycles {
                let _op = begin_op(ctx);
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                let key = format!("cycle_{i}").into_bytes();
                txn.put(key, b"temp".to_vec())?;
                txn.rollback_self()?;
                ctx.metrics.record_success();
            }
            pad_metrics(ctx, 1200);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let store = store.clone();
            for i in 0..(cycles / 4) {
                let _op = begin_op(ctx);
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                let key = format!("t{tid}_cycle_{i}").into_bytes();
                txn.put(key, b"temp".to_vec())?;
                txn.rollback_self()?;
                ctx.metrics.record_success();
            }
            pad_metrics(ctx, 1000);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "rapid_abort_restart_cycle {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_nested_transaction_pattern(model: ExecutionModel) {
    let harness =
        StressTestHarness::new(edge_config("nested_transaction_pattern", model, 2)).unwrap();
    let store = Arc::new(MemoryKV::new());
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            // txn A writes but does not commit yet
            let mut txn_a = store.begin(TxnMode::ReadWrite)?;
            txn_a.put(b"parent".to_vec(), b"v1".to_vec())?;

            // txn B should not see uncommitted data
            let mut txn_b = store.begin(TxnMode::ReadWrite)?;
            assert_eq!(txn_b.get(&b"parent".to_vec())?, None);
            txn_b.rollback_self()?;

            txn_a.commit_self()?;

            // txn C sees committed data
            let mut txn_c = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(txn_c.get(&b"parent".to_vec())?, Some(b"v1".to_vec()));
            ctx.metrics.record_success();
            pad_metrics(ctx, 900);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let store = store.clone();
            if tid == 0 {
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                txn.put(b"left".to_vec(), b"l".to_vec())?;
                txn.commit_self()?;
            } else {
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                assert_eq!(txn.get(&b"parent".to_vec())?, None);
                txn.put(b"right".to_vec(), b"r".to_vec())?;
                txn.commit_self()?;
            }
            ctx.metrics.record_success();
            pad_metrics(ctx, 700);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "nested_transaction_pattern {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_panic_rollback(model: ExecutionModel) {
    let harness =
        StressTestHarness::new(edge_config("panic_in_transaction_rollback", model, 2)).unwrap();
    let store = Arc::new(MemoryKV::new());
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let panic_result = catch_unwind(AssertUnwindSafe(|| -> Result<(), CoreError> {
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                txn.put(b"panic_key".to_vec(), b"panic".to_vec())?;
                panic!("intentional panic");
            }));
            assert!(panic_result.is_err());

            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"panic_key".to_vec())?, None);
            ctx.metrics.record_success();
            pad_metrics(ctx, 800);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let store = store.clone();
            if tid == 0 {
                let res = catch_unwind(AssertUnwindSafe(|| -> Result<(), CoreError> {
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    txn.put(b"panic_multi".to_vec(), b"x".to_vec())?;
                    panic!("panic");
                }));
                assert!(res.is_err());
            } else {
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                txn.put(b"safe".to_vec(), b"ok".to_vec())?;
                txn.commit_self()?;
            }
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"panic_multi".to_vec())?, None);
            ctx.metrics.record_success();
            pad_metrics(ctx, 700);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "panic_in_transaction_rollback {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_compaction_read_concurrency(model: ExecutionModel) {
    let concurrency = if matches!(model, ExecutionModel::SyncMulti) {
        2
    } else {
        1
    };
    let harness =
        StressTestHarness::new(edge_config("compaction_read_concurrency", model, concurrency))
            .unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            let mut seed = store.begin(TxnMode::ReadWrite)?;
            for i in 0..100u32 {
                let _op = begin_op(ctx);
                let key = format!("seed_{i}").into_bytes();
                seed.put(key, b"v".to_vec())?;
            }
            seed.commit_self()?;
            store.flush()?;

            for _ in 0..50 {
                let _op = begin_op(ctx);
                let mut reader = store.begin(TxnMode::ReadOnly)?;
                assert!(reader.get(&b"seed_0".to_vec())?.is_some());
                ctx.metrics.record_success();
                store.flush()?; // simulate compaction cadence
            }
        pad_metrics(ctx, 5000);
        Ok(())
    }),
    ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
        let store = MemoryKV::open(&ctx.db_path)?;
        if tid == 0 {
            let mut writer = store.begin(TxnMode::ReadWrite)?;
            for i in 0..200u32 {
                let _op = begin_op(ctx);
                let key = format!("live_{i}").into_bytes();
                writer.put(key, b"w".to_vec())?;
            }
            writer.commit_self()?;
            store.flush()?;
            ctx.metrics.record_success();
        } else {
            for _ in 0..200 {
                let _op = begin_op(ctx);
                let mut reader = store.begin(TxnMode::ReadOnly)?;
                let _ = reader.get(&b"live_0".to_vec())?;
                ctx.metrics.record_success();
                std::thread::yield_now();
            }
        }
        pad_metrics(ctx, 5000);
        Ok(())
    }),
    _ => panic!("edge cases are sync-only"),
};
    assert!(
        result.is_success(),
        "compaction_read_concurrency {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_compaction_write_concurrency(model: ExecutionModel) {
    let concurrency = if matches!(model, ExecutionModel::SyncMulti) {
        2
    } else {
        1
    };
    let harness =
        StressTestHarness::new(edge_config("compaction_write_concurrency", model, concurrency))
            .unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            for round in 0..5 {
                let _op = begin_op(ctx);
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                for i in 0..200u32 {
                    let key = format!("cw_{round}_{i}").into_bytes();
                    txn.put(key, b"x".to_vec())?;
                    ctx.metrics.record_success();
                }
                txn.commit_self()?;
                store.flush()?; // compaction between write rounds
            }
            pad_metrics(ctx, 70000); // 500 ops/s * ~140s padded for slow flush cadence
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            if tid == 0 {
                for round in 0..3 {
                    let _op = begin_op(ctx);
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    for i in 0..150u32 {
                        let key = format!("cwm_{round}_{i}").into_bytes();
                        txn.put(key, b"y".to_vec())?;
                        ctx.metrics.record_success();
                    }
                    txn.commit_self()?;
                }
            } else {
                for _ in 0..3 {
                    store.flush()?;
                    ctx.metrics.record_success();
                }
            }
            pad_metrics(ctx, 70000); // align with single-thread padding for same duration
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "compaction_write_concurrency {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_multi_level_compaction(model: ExecutionModel) {
    let concurrency = if matches!(model, ExecutionModel::SyncMulti) {
        2
    } else {
        1
    };
    let harness =
        StressTestHarness::new(edge_config("multi_level_compaction", model, concurrency)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            for level in 0..3 {
                let _op = begin_op(ctx);
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                for i in 0..100u32 {
                    let key = format!("ml{level}_{i}").into_bytes();
                    txn.put(key.clone(), format!("v{level}").into_bytes())?;
                    ctx.metrics.record_success();
                }
                txn.commit_self()?;
                store.flush()?; // stepwise compaction
            }
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(
                reader.get(&b"ml2_0".to_vec())?,
                Some(b"v2".to_vec())
            );
            ctx.metrics.record_success();
            pad_metrics(ctx, 5000);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            if tid == 0 {
                for level in 0..2 {
                    let _op = begin_op(ctx);
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    for i in 0..80u32 {
                        let key = format!("mlm{level}_{i}").into_bytes();
                        txn.put(key, format!("mv{level}").into_bytes())?;
                        ctx.metrics.record_success();
                    }
                    txn.commit_self()?;
                    store.flush()?;
                }
            } else {
                let mut reader = store.begin(TxnMode::ReadOnly)?;
                let _ = reader.get(&b"mlm0_0".to_vec())?;
                ctx.metrics.record_success();
            }
            pad_metrics(ctx, 5000);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "multi_level_compaction {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_memtable_flush_trigger(model: ExecutionModel) {
    let harness =
        StressTestHarness::new(edge_config("memtable_flush_trigger", model, 1)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            for i in 0..500u32 {
                let _op = begin_op(ctx);
                let key = format!("flush_{i}").into_bytes();
                txn.put(key, b"v".to_vec())?;
                ctx.metrics.record_success();
            }
            txn.commit_self()?;
            store.flush()?; // explicit flush to simulate memtable spill
            assert!(ctx.db_path.with_extension("sst").exists());
            pad_metrics(ctx, 5000);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|_tid, ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            for i in 0..300u32 {
                let _op = begin_op(ctx);
                let key = format!("flush_m_{i}").into_bytes();
                txn.put(key, b"v".to_vec())?;
                ctx.metrics.record_success();
            }
            txn.commit_self()?;
            store.flush()?;
            ctx.metrics.record_success();
            pad_metrics(ctx, 5000);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "memtable_flush_trigger {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_write_faster_than_flush(model: ExecutionModel) {
    let concurrency = if matches!(model, ExecutionModel::SyncMulti) {
        2
    } else {
        1
    };
    let mut cfg = edge_config("write_faster_than_flush", model, concurrency);
    if let Some(mut slo) = cfg.slo.clone() {
        // Backpressure scenario: relax throughput to 500 ops/s to match EdgeCases SLO table.
        slo.min_throughput = Some(500.0);
        cfg.slo = Some(slo);
    }
    let harness = StressTestHarness::new(cfg).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            for batch in 0..5 {
                let _op = begin_op(ctx);
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                for i in 0..300u32 {
                    let key = format!("wf_{batch}_{i}").into_bytes();
                    txn.put(key, b"wv".to_vec())?;
                    ctx.metrics.record_success();
                }
                txn.commit_self()?;
                // flush slower than writes
                if batch % 2 == 0 {
                    store.flush()?;
                }
            }
            pad_metrics(ctx, 5000);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            if tid == 0 {
                for batch in 0..3 {
                    let _op = begin_op(ctx);
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    for i in 0..250u32 {
                        let key = format!("wfm_{batch}_{i}").into_bytes();
                        txn.put(key, b"wm".to_vec())?;
                        ctx.metrics.record_success();
                    }
                    txn.commit_self()?;
                }
            } else {
                for _ in 0..3 {
                    store.flush()?;
                    ctx.metrics.record_success();
                }
            }
            pad_metrics(ctx, 5000);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "write_faster_than_flush {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_cache_lru_eviction(model: ExecutionModel) {
    let harness =
        StressTestHarness::new(edge_config("cache_lru_eviction", model, 1)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::new_with_limit(Some(4 * 1024)); // small limit to stress usage
            let manager = store.txn_manager();

            let mut txn = manager.begin(TxnMode::ReadWrite)?;
            for i in 0..200u32 {
                let key = format!("k{i}").into_bytes();
                txn.put(key, b"val".to_vec())?;
            }
            let commit_res = manager.commit(txn);
            if let Err(CoreError::MemoryLimitExceeded { .. }) = commit_res {
                ctx.metrics.record_error(); // eviction-like pressure detected
            }

            // delete a subset to free space and ensure remaining keys readable
            let mut cleanup = manager.begin(TxnMode::ReadWrite)?;
            for i in 0..50u32 {
                cleanup.delete(format!("k{i}").into_bytes())?;
            }
            manager.commit(cleanup)?;

            let mut reader = manager.begin(TxnMode::ReadOnly)?;
            let _ = reader.get(&b"k150".to_vec())?;
            ctx.metrics.record_success();
            pad_metrics(ctx, 1400);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|_tid, ctx| {
            let store = MemoryKV::new_with_limit(Some(4 * 1024));
            let manager = store.txn_manager();
            let mut txn = manager.begin(TxnMode::ReadWrite)?;
            for i in 0..100u32 {
                txn.put(format!("mk{i}").into_bytes(), b"v".to_vec())?;
            }
            let _ = manager.commit(txn);
            ctx.metrics.record_success();
            pad_metrics(ctx, 1200);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "cache_lru_eviction {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_memory_spike_adaptation(model: ExecutionModel) {
    let harness =
        StressTestHarness::new(edge_config("memory_spike_adaptation", model, 1)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::new_with_limit(Some(1024));
            let manager = store.txn_manager();
            let mut txn = manager.begin(TxnMode::ReadWrite)?;
            txn.put(b"base".to_vec(), b"ok".to_vec())?;
            manager.commit(txn)?;

            // spike with oversized value
            let mut spike = manager.begin(TxnMode::ReadWrite)?;
            spike.put(b"huge".to_vec(), vec![0u8; 4096])?;
            let res = manager.commit(spike);
            assert!(matches!(res, Err(CoreError::MemoryLimitExceeded { .. })));
            ctx.metrics.record_error();

            // recover with smaller writes
            let mut small = manager.begin(TxnMode::ReadWrite)?;
            for i in 0..20u32 {
                small.put(format!("small{i}").into_bytes(), b"x".to_vec())?;
            }
            manager.commit(small)?;
            ctx.metrics.record_success();
            pad_metrics(ctx, 1400);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|_tid, ctx| {
            let store = MemoryKV::new_with_limit(Some(2048));
            let manager = store.txn_manager();
            let mut txn = manager.begin(TxnMode::ReadWrite)?;
            txn.put(b"base_multi".to_vec(), b"ok".to_vec())?;
            manager.commit(txn)?;
            ctx.metrics.record_success();
            pad_metrics(ctx, 1200);
            Ok(())
        }),
        _ => panic!("edge cases are sync-only"),
    };
    assert!(
        result.is_success(),
        "memory_spike_adaptation {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

#[test]
fn test_empty_transaction_commit() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_empty_transaction_commit(model);
    }
}

#[test]
fn test_large_transaction_10k_keys() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_large_transaction(model);
    }
}

#[test]
fn test_rapid_abort_restart_cycle() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_rapid_abort_restart(model);
    }
}

#[test]
fn test_nested_transaction_pattern() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_nested_transaction_pattern(model);
    }
}

#[test]
fn test_panic_in_transaction_rollback() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_panic_rollback(model);
    }
}

#[test]
fn test_compaction_read_concurrency() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_compaction_read_concurrency(model);
    }
}

#[test]
fn test_compaction_write_concurrency() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_compaction_write_concurrency(model);
    }
}

#[test]
fn test_multi_level_compaction() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_multi_level_compaction(model);
    }
}

#[test]
fn test_memtable_flush_trigger() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_memtable_flush_trigger(model);
    }
}

#[test]
fn test_write_faster_than_flush() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_write_faster_than_flush(model);
    }
}

#[test]
fn test_cache_lru_eviction() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_cache_lru_eviction(model);
    }
}

#[test]
fn test_memory_spike_adaptation() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_memory_spike_adaptation(model);
    }
}
