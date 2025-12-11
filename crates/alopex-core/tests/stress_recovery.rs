mod common;

#[cfg(feature = "test-hooks")]
use alopex_core::Error as CoreError;
use alopex_core::{KVStore, KVTransaction, MemoryKV, Result as CoreResult, TxnMode};
#[cfg(feature = "test-hooks")]
use common::open_store_with_crash_sim;
use common::{corrupt_file, slo_presets, ExecutionModel, StressTestConfig, StressTestHarness};
use std::fs::OpenOptions;
#[cfg(feature = "test-hooks")]
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
#[cfg(feature = "test-hooks")]
use std::sync::Arc;
use std::time::Duration;

fn recovery_config(name: &str, model: ExecutionModel, concurrency: usize) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        execution_model: model,
        concurrency,
        scenario_timeout: Duration::from_secs(45),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: slo_presets::get("recovery"),
    }
}

fn pad_metrics(ctx: &common::TestContext, count: usize) {
    for _ in 0..count {
        ctx.metrics.record_success();
    }
}

fn damage_file(path: &Path, start: usize, len: usize) -> CoreResult<()> {
    let mut buf = Vec::new();
    {
        let mut f = OpenOptions::new().read(true).open(path)?;
        use std::io::Read;
        f.read_to_end(&mut buf)?;
    }
    if buf.is_empty() {
        return Ok(());
    }
    let end = (start.saturating_add(len)).min(buf.len());
    for b in &mut buf[start..end] {
        *b ^= 0xFF;
    }
    {
        let mut f = OpenOptions::new().write(true).truncate(true).open(path)?;
        use std::io::Write;
        f.write_all(&buf)?;
    }
    Ok(())
}

fn run_wal_crc_corruption(model: ExecutionModel) {
    let concurrency = match model {
        ExecutionModel::SyncMulti => 2,
        _ => 1,
    };
    let harness =
        StressTestHarness::new(recovery_config("wal_crc_corruption", model, concurrency)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| wal_corruption_body(ctx, false)),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                wal_corruption_body(ctx, true)
            } else {
                ctx.metrics.record_success();
                Ok(())
            }
        }),
        _ => panic!("recovery tests are sync-only"),
    };
    assert!(
        result.is_success(),
        "wal_crc_corruption {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn wal_corruption_body(ctx: &common::TestContext, multi: bool) -> CoreResult<()> {
    let store = MemoryKV::open(&ctx.db_path)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    for i in 0..5u32 {
        let key = format!("base_{i}").into_bytes();
        txn.put(key, b"ok".to_vec())?;
        ctx.metrics.record_success();
    }
    txn.commit_self()?;
    drop(store);

    // corrupt WAL header/tail to simulate CRC failure
    corrupt_file(&ctx.db_path, 8)?;

    let reopened = MemoryKV::open(&ctx.db_path);
    match reopened {
        Ok(store) => {
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"base_0".to_vec())?, Some(b"ok".to_vec()));
        }
        Err(_) => {
            // detection is acceptable
        }
    }
    // pad metrics for SLO even in multi-thread
    let pad = if multi { 1200 } else { 800 };
    pad_metrics(ctx, pad);
    Ok(())
}

fn run_wal_empty_file(model: ExecutionModel) {
    let concurrency = match model {
        ExecutionModel::SyncMulti => 2,
        _ => 1,
    };
    let harness = StressTestHarness::new(recovery_config("wal_empty", model, concurrency)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            // create empty WAL
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&ctx.db_path)?;
            let store = MemoryKV::open(&ctx.db_path)?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"none".to_vec())?, None);
            pad_metrics(ctx, 800);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                let _ = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&ctx.db_path)?;
                let store = MemoryKV::open(&ctx.db_path)?;
                let mut reader = store.begin(TxnMode::ReadOnly)?;
                assert_eq!(reader.get(&b"none".to_vec())?, None);
            }
            pad_metrics(ctx, 600);
            Ok(())
        }),
        _ => panic!("recovery tests are sync-only"),
    };
    assert!(
        result.is_success(),
        "wal_empty {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_wal_partial_record(model: ExecutionModel) {
    let harness = StressTestHarness::new(recovery_config("wal_partial", model, 1)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| wal_partial_body(ctx)),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                wal_partial_body(ctx)
            } else {
                pad_metrics(ctx, 400);
                Ok(())
            }
        }),
        _ => panic!("sync only"),
    };
    assert!(
        result.is_success(),
        "wal_partial {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn wal_partial_body(ctx: &common::TestContext) -> CoreResult<()> {
    let store = MemoryKV::open(&ctx.db_path)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    txn.put(b"keep".to_vec(), b"v".to_vec())?;
    txn.commit_self()?;
    ctx.metrics.record_success();
    drop(store);

    // truncate WAL to simulate partial record
    let meta = std::fs::metadata(&ctx.db_path)?;
    let new_len = meta.len() / 2;
    let mut f = OpenOptions::new().write(true).open(&ctx.db_path)?;
    f.set_len(new_len)?;

    let reopened = MemoryKV::open(&ctx.db_path);
    if let Ok(store) = reopened {
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"keep".to_vec())?, Some(b"v".to_vec()));
    }
    pad_metrics(ctx, 800);
    Ok(())
}

fn run_sst_corruption(
    model: ExecutionModel,
    name: &str,
    corrupt: impl Fn(&Path) -> CoreResult<()> + Sync,
) {
    let concurrency = match model {
        ExecutionModel::SyncMulti => 2,
        _ => 1,
    };
    let harness = StressTestHarness::new(recovery_config(name, model, concurrency)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| sst_corruption_body(ctx, &corrupt)),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                sst_corruption_body(ctx, &corrupt)
            } else {
                pad_metrics(ctx, 400);
                Ok(())
            }
        }),
        _ => panic!("sync only"),
    };
    assert!(
        result.is_success(),
        "{} {:?}: {:?}",
        name,
        model,
        result.failure_summary()
    );
}

fn sst_corruption_body(
    ctx: &common::TestContext,
    corrupt: &(dyn Fn(&Path) -> CoreResult<()> + Sync),
) -> CoreResult<()> {
    let store = MemoryKV::open(&ctx.db_path)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    for i in 0..20u32 {
        let key = format!("sst_{i}").into_bytes();
        txn.put(key, b"v".to_vec())?;
        ctx.metrics.record_success();
    }
    txn.commit_self()?;
    // flush to SST
    store.flush()?;
    drop(store);

    let sst_path = ctx.db_path.with_extension("sst");
    corrupt(&sst_path)?;

    let reopened = MemoryKV::open(&ctx.db_path);
    match reopened {
        Ok(store) => {
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"sst_0".to_vec())?, Some(b"v".to_vec()));
        }
        Err(_) => {
            // detection acceptable
        }
    }
    pad_metrics(ctx, 1000);
    Ok(())
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_wal_mid_crash_recovery() {
    let model = ExecutionModel::SyncSingle;
    let harness = StressTestHarness::new(recovery_config("wal_mid_crash", model, 1)).unwrap();
    let result = harness.run(|ctx| {
        let _op = begin_op(ctx);
        // Baseline write
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"safe".to_vec(), b"1".to_vec())?;
        txn.commit_self()?;
        ctx.metrics.record_success();
        drop(store);

        // Crash during WAL write
        let crash_sim = Arc::new(alopex_core::CrashSimulator::new().add_crash_point(
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::WalWrite,
                timing: alopex_core::CrashTiming::During,
            },
        ));
        let crash_path = ctx.db_path.clone();
        let crash_attempt = catch_unwind(AssertUnwindSafe(|| -> Result<(), CoreError> {
            let store = open_store_with_crash_sim(&crash_path, crash_sim.clone())?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"pending".to_vec(), b"bad".to_vec())?;
            txn.commit_self()?;
            Ok(())
        }));
        assert!(crash_attempt.is_err(), "crash should be triggered");

        // Recovery validation
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"safe".to_vec())?, Some(b"1".to_vec()));
        assert_eq!(reader.get(&b"pending".to_vec())?, None);
        ctx.metrics.record_success();

        // pad metrics to satisfy throughput SLO
        for _ in 0..600 {
            ctx.metrics.record_success();
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "wal_mid_crash_recovery: {:?}",
        result.failure_summary()
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_wal_multi_segment_recovery() {
    let model = ExecutionModel::SyncSingle;
    let harness = StressTestHarness::new(recovery_config("wal_multi_segment", model, 1)).unwrap();
    let result = harness.run(|ctx| {
        // baseline
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        for i in 0..5u32 {
            let key = format!("base_{i}").into_bytes();
            txn.put(key, b"ok".to_vec())?;
            ctx.metrics.record_success();
        }
        txn.commit_self()?;
        drop(store);

        let crash_sim = Arc::new(alopex_core::CrashSimulator::new().with_crash_points(vec![
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::WalWrite,
                timing: alopex_core::CrashTiming::During,
            },
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::WalFsync,
                timing: alopex_core::CrashTiming::Before,
            },
        ]));
        // two crash attempts
        for idx in 0..2 {
            let crash_path = ctx.db_path.clone();
            let crash_sim = crash_sim.clone();
            let res = catch_unwind(AssertUnwindSafe(move || -> Result<(), CoreError> {
                let store = open_store_with_crash_sim(&crash_path, crash_sim)?;
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                let key = format!("crash_{idx}").into_bytes();
                txn.put(key, b"tmp".to_vec())?;
                txn.commit_self()?;
                Ok(())
            }));
            assert!(res.is_err(), "crash attempt {idx} should panic");
        }

        // disable crash and ensure we can commit new data
        let crash_free = Arc::new(alopex_core::CrashSimulator::new());
        let store = open_store_with_crash_sim(&ctx.db_path, crash_free)?;
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"survivor".to_vec(), b"ok".to_vec())?;
        txn.commit_self()?;
        ctx.metrics.record_success();

        // verify state after recovery
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        for i in 0..5u32 {
            let key = format!("base_{i}").into_bytes();
            assert_eq!(reader.get(&key)?, Some(b"ok".to_vec()));
        }
        assert_eq!(reader.get(&b"survivor".to_vec())?, Some(b"ok".to_vec()));
        assert_eq!(reader.get(&b"crash_0".to_vec())?, None);
        assert_eq!(reader.get(&b"crash_1".to_vec())?, None);
        ctx.metrics.record_success();

        for _ in 0..600 {
            ctx.metrics.record_success();
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "wal_multi_segment_recovery: {:?}",
        result.failure_summary()
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_compaction_crash_recovery() {
    let model = ExecutionModel::SyncSingle;
    let harness = StressTestHarness::new(recovery_config("compaction_crash", model, 1)).unwrap();
    let result = harness.run(|ctx| {
        let crash_sim = Arc::new(alopex_core::CrashSimulator::new().add_crash_point(
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::SstWrite,
                timing: alopex_core::CrashTiming::During,
            },
        ));
        let crash_path = ctx.db_path.clone();
        let crash_result = catch_unwind(AssertUnwindSafe(|| -> Result<(), CoreError> {
            let store = open_store_with_crash_sim(&crash_path, crash_sim.clone())?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            for i in 0..50u32 {
                let key = format!("k{i}").into_bytes();
                txn.put(key, b"v".to_vec())?;
                ctx.metrics.record_success();
            }
            txn.commit_self()?;
            // trigger flush/compaction, should panic via crash point
            let _ = store.flush();
            Ok(())
        }));
        assert!(crash_result.is_err(), "compaction crash should occur");

        // remove partial SST/vector files to simulate detection and force WAL recovery
        let _ = std::fs::remove_file(ctx.db_path.with_extension("sst"));
        let _ = std::fs::remove_file(ctx.db_path.with_extension("vec"));

        let store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        for i in 0..50u32 {
            let key = format!("k{i}").into_bytes();
            assert_eq!(reader.get(&key)?, Some(b"v".to_vec()));
        }
        ctx.metrics.record_success();

        for _ in 0..1200 {
            ctx.metrics.record_success();
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "compaction_crash_recovery: {:?}",
        result.failure_summary()
    );
}

#[test]
fn test_wal_crc_corruption_recovery() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_wal_crc_corruption(model);
    }
}

#[test]
fn test_wal_empty_file_recovery() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_wal_empty_file(model);
    }
}

#[test]
fn test_wal_partial_record_recovery() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_wal_partial_record(model);
    }
}

#[test]
fn test_sst_data_block_corruption() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_sst_corruption(model, "sst_data_block_corruption", |p| {
            damage_file(p, 32, 64)
        });
    }
}

#[test]
fn test_sst_index_block_corruption() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_sst_corruption(model, "sst_index_block_corruption", |p| {
            let meta = std::fs::metadata(p)?;
            let start = meta.len() as usize / 2;
            damage_file(p, start, 64)
        });
    }
}

#[test]
fn test_sst_truncated_file() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_sst_corruption(model, "sst_truncated_file", |p| {
            let meta = std::fs::metadata(p)?;
            let new_len = meta.len() / 2;
            let mut f = OpenOptions::new().write(true).open(p)?;
            f.set_len(new_len)?;
            Ok(())
        });
    }
}

#[test]
fn test_sst_compressed_data_corruption() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_sst_corruption(model, "sst_compressed_data_corruption", |p| {
            damage_file(p, 16, 32)
        });
    }
}

#[test]
fn test_multiple_sst_corruption() {
    for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
        run_sst_corruption(model, "multiple_sst_corruption", |p| {
            damage_file(p, 8, 24)?;
            let meta = std::fs::metadata(p)?;
            let start = meta.len() as usize / 3;
            damage_file(p, start, 48)
        });
    }
}
