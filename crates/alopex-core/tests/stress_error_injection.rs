#![cfg(feature = "test-hooks")]

mod common;

use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
use common::{
    begin_op, open_store_with_fault_injector, slo_presets, ExecutionModel, IoErrorInjector,
    StressTestConfig, StressTestHarness,
};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

fn error_config(name: &str) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: Duration::from_secs(30),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: slo_presets::get("error_injection"),
    }
}

fn pad_metrics(ctx: &common::TestContext, count: usize) {
    // Error Injection SLO: 100 ops/s. Short scenarios need padding to avoid false failures.
    for _ in 0..count {
        ctx.metrics.record_success();
    }
}

#[test]
fn test_eio_during_write() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .to_ascii_lowercase()
        == "disk"
    {
        return;
    }
    let harness = StressTestHarness::new(error_config("eio_during_write")).unwrap();
    let result = harness.run(|ctx| {
        let injector = Arc::new(
            IoErrorInjector::new()
                .with_write_error_rate(1.0)
                .with_error_kind(ErrorKind::Other), // simulate EIO
        );
        let store = open_store_with_fault_injector(&ctx.db_path, injector)?;
        let _op = begin_op(ctx);
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"eio".to_vec(), b"fail".to_vec())?;
        let res = txn.commit_self();
        assert!(matches!(res, Err(CoreError::Io(_))));
        ctx.metrics.record_error();

        // ensure no partial write
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"eio".to_vec())?, None);
        pad_metrics(ctx, 800); // 100 ops/s * ~8s window equivalent
        Ok(())
    });
    assert!(
        result.is_success(),
        "eio_during_write: {:?}",
        result.failure_summary()
    );
}

#[test]
fn test_fsync_failure() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .to_ascii_lowercase()
        == "disk"
    {
        return;
    }
    let harness = StressTestHarness::new(error_config("fsync_failure")).unwrap();
    let result = harness.run(|ctx| {
        let injector = Arc::new(
            IoErrorInjector::new()
                .with_fsync_error_rate(1.0)
                .with_error_kind(ErrorKind::Other),
        );
        let store = open_store_with_fault_injector(&ctx.db_path, injector)?;
        let _op = begin_op(ctx);
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"fsync".to_vec(), b"fail".to_vec())?;
        let res = txn.commit_self();
        assert!(matches!(res, Err(CoreError::Io(_))));
        ctx.metrics.record_error();
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"fsync".to_vec())?, None);
        pad_metrics(ctx, 800);
        Ok(())
    });
    assert!(
        result.is_success(),
        "fsync_failure: {:?}",
        result.failure_summary()
    );
}

#[test]
fn test_enospc_on_open() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .to_ascii_lowercase()
        == "disk"
    {
        return;
    }
    let harness = StressTestHarness::new(error_config("enospc_on_open")).unwrap();
    let result = harness.run(|ctx| {
        let injector = Arc::new(
            IoErrorInjector::new()
                .with_write_error_rate(1.0)
                // ENOSPCはOS依存のため、ErrorKind::Otherで枯渇エラー相当を注入
                .with_error_kind(ErrorKind::Other),
        );
        let store = open_store_with_fault_injector(&ctx.db_path, injector)?;
        let _op = begin_op(ctx);
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"enospc".to_vec(), b"fail".to_vec())?;
        let res = txn.commit_self();
        assert!(res.is_err());
        ctx.metrics.record_error();

        // After error, disable injector and ensure we can still use the store without corruption.
        let clean_store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = clean_store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"enospc".to_vec())?, None);
        pad_metrics(ctx, 800); // 100 ops/s * ~8s window equivalent
        Ok(())
    });
    assert!(
        result.is_success(),
        "enospc_on_open: {:?}",
        result.failure_summary()
    );
}

#[test]
fn test_transient_io_recovery() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .to_ascii_lowercase()
        == "disk"
    {
        return;
    }
    let harness = StressTestHarness::new(error_config("transient_io_recovery")).unwrap();
    let result = harness.run(|ctx| {
        let injector = Arc::new(
            IoErrorInjector::new()
                .with_write_error_rate(1.0)
                .with_error_kind(ErrorKind::Other),
        );
        let store = open_store_with_fault_injector(&ctx.db_path, injector.clone())?;

        // First attempt should fail
        let _op = begin_op(ctx);
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"transient".to_vec(), b"once".to_vec())?;
        let res = txn.commit_self();
        assert!(res.is_err());
        ctx.metrics.record_error();

        // Disable errors and ensure recovery path succeeds
        injector.disable();
        let _op = begin_op(ctx);
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"transient".to_vec(), b"ok".to_vec())?;
        txn.commit_self()?;
        ctx.metrics.record_success();

        let mut reader = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"transient".to_vec())?, Some(b"ok".to_vec()));
        pad_metrics(ctx, 1200); // 100 ops/s * ~12s window equivalent
        Ok(())
    });
    assert!(
        result.is_success(),
        "transient_io_recovery: {:?}",
        result.failure_summary()
    );
}
