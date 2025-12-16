mod common;

use alopex_core::{KVStore, KVTransaction, TxnMode};
use common::*;
use std::time::Duration;

#[test]
fn test_harness_sync_single() {
    for mode in selected_storage_modes() {
        let cfg = StressTestConfig {
            name: format!("sync_single_{}", mode.as_str()),
            execution_model: ExecutionModel::SyncSingle,
            concurrency: 1,
            scenario_timeout: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(5),
            metrics_interval: Duration::from_secs(1),
            warmup_ops: 0,
            slo: None,
        };
        let harness = StressTestHarness::new(cfg).unwrap();
        let result = harness.run(|ctx| {
            let _op = begin_op(ctx);
            do_put_get_roundtrip(ctx, mode)?;
            ctx.metrics.record_success();
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: {}",
            mode.as_str(),
            result.failure_summary().unwrap_or_default()
        );
    }
}

#[test]
fn test_harness_sync_multi() {
    for mode in selected_storage_modes() {
        let cfg = StressTestConfig {
            name: format!("sync_multi_{}", mode.as_str()),
            execution_model: ExecutionModel::SyncMulti,
            concurrency: 4,
            scenario_timeout: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(5),
            metrics_interval: Duration::from_secs(1),
            warmup_ops: 0,
            slo: None,
        };
        let harness = StressTestHarness::new(cfg).unwrap();
        let result = harness.run_concurrent(|tid, ctx| {
            let _op = begin_op(ctx);
            let key = format!("k{tid}").into_bytes();
            let store = open_store_for_mode(&ctx.db_path, mode)?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(key.clone(), b"v".to_vec())?;
            txn.commit_self()?;
            ctx.metrics.record_success();
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: {}",
            mode.as_str(),
            result.failure_summary().unwrap_or_default()
        );
    }
}

#[test]
fn test_harness_async_multi() {
    for mode in selected_storage_modes() {
        let cfg = StressTestConfig {
            name: format!("async_multi_{}", mode.as_str()),
            execution_model: ExecutionModel::AsyncMulti,
            concurrency: 2,
            scenario_timeout: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(5),
            metrics_interval: Duration::from_secs(1),
            warmup_ops: 0,
            slo: None,
        };
        let harness = StressTestHarness::new(cfg).unwrap();
        let result = harness.run_async(|ctx| async move {
            let _op = begin_op(&ctx);
            tokio::time::sleep(Duration::from_millis(10)).await;
            do_put_get_roundtrip(&ctx, mode)?;
            ctx.metrics.record_success();
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: {}",
            mode.as_str(),
            result.failure_summary().unwrap_or_default()
        );
    }
}

#[test]
fn test_workload_generator_is_deterministic() {
    let mut g1 = WorkloadGenerator::new(WorkloadConfig {
        operation_count: 10,
        key_space_size: 10,
        value_size: 4,
        seed: 42,
    });
    let mut g2 = WorkloadGenerator::new(WorkloadConfig {
        operation_count: 10,
        key_space_size: 10,
        value_size: 4,
        seed: 42,
    });
    let b1 = g1.generate_batch();
    let b2 = g2.generate_batch();
    assert_eq!(format!("{:?}", b1), format!("{:?}", b2));
}
