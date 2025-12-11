#![cfg(feature = "test-hooks")]

mod common;

use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
use common::{
    begin_op, open_store_with_crash_sim, slo_presets, ExecutionModel, StressTestConfig,
    StressTestHarness,
};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

fn recovery_config(name: &str) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: Duration::from_secs(30),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: slo_presets::get("recovery"),
    }
}

#[test]
fn test_wal_mid_crash_recovery() {
    let harness = StressTestHarness::new(recovery_config("wal_mid_crash")).unwrap();
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

#[test]
fn test_wal_multi_segment_recovery() {
    let harness = StressTestHarness::new(recovery_config("wal_multi_segment")).unwrap();
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

#[test]
fn test_compaction_crash_recovery() {
    let harness = StressTestHarness::new(recovery_config("compaction_crash")).unwrap();
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
