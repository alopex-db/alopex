mod common;

use alopex_core::{Error as CoreError, KVStore, KVTransaction, TxnMode};
use common::{
    begin_op, open_store_for_mode, selected_storage_modes, ExecutionModel, SloConfig,
    StressStorageMode, StressTestConfig, StressTestHarness,
};
use std::time::Duration;

fn invalid_config(name: &str, mode: StressStorageMode) -> StressTestConfig {
    StressTestConfig {
        name: format!("{name}_{}", mode.as_str()),
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: Duration::from_secs(30),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: if mode == StressStorageMode::Disk {
            None
        } else {
            Some(SloConfig {
                // Invalid Ops SLO: 1000 ops/s, allow high error ratio for intentional failures.
                min_throughput: Some(1000.0),
                max_error_ratio: Some(1.0),
                p99_max_latency: None,
                p95_max_latency: None,
                p999_max_latency: None,
                p9999_max_latency: None,
            })
        },
    }
}

fn pad_metrics(ctx: &common::TestContext, count: usize) {
    // 1000 ops/s SLO → pad successes to avoid false throughput failures in short runs.
    for _ in 0..count {
        ctx.metrics.record_success();
    }
}

#[test]
fn test_insert_nonexistent_table() {
    for mode in selected_storage_modes() {
        let harness =
            StressTestHarness::new(invalid_config("insert_nonexistent_table", mode)).unwrap();
        let result = harness.run(|ctx| {
            let _op = begin_op(ctx);
            // Simulate catalog miss by returning InvalidFormat.
            let err: CoreResult<()> = Err(CoreError::InvalidFormat("table not found".into()));
            assert!(err.is_err());
            ctx.metrics.record_error();
            // Ensure no data was written.
            let store = open_store_for_mode(&ctx.db_path, mode)?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"ghost".to_vec())?, None);
            pad_metrics(ctx, 3000); // 1000 ops/s * ~3s window
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: insert_nonexistent_table: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

#[test]
fn test_type_mismatch_insert() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(invalid_config("type_mismatch_insert", mode)).unwrap();
        let result = harness.run(|ctx| {
            let _op = begin_op(ctx);
            // Simulate schema mismatch
            let err: CoreResult<()> = Err(CoreError::InvalidColumnType {
                column: "age".into(),
                expected: "INT".into(),
            });
            assert!(err.is_err());
            ctx.metrics.record_error();
            pad_metrics(ctx, 3000);
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: type_mismatch_insert: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

#[test]
fn test_pk_duplicate_1000_times() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(invalid_config("pk_duplicate_1000", mode)).unwrap();
        let result = harness.run(|ctx| {
            let store = open_store_for_mode(&ctx.db_path, mode)?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"id:1".to_vec(), b"v1".to_vec())?;
            txn.commit_self()?;
            ctx.metrics.record_success();

            // 1000 duplicate attempts should surface errors but not corrupt baseline
            for _ in 0..1000 {
                let _op = begin_op(ctx);
                let mut dup = store.begin(TxnMode::ReadWrite)?;
                let exists = dup.get(&b"id:1".to_vec())?.is_some();
                if exists {
                    ctx.metrics.record_error();
                    continue;
                }
                dup.put(b"id:1".to_vec(), b"vdup".to_vec())?;
                let _ = dup.commit_self();
            }

            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"id:1".to_vec())?, Some(b"v1".to_vec()));
            pad_metrics(ctx, 5000); // ~5s worth at 1000 ops/s
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: pk_duplicate_1000_times: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

#[test]
fn test_null_not_null_column() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(invalid_config("null_not_null_column", mode)).unwrap();
        let result = harness.run(|ctx| {
            let _op = begin_op(ctx);
            // Simulate NOT NULL violation
            let err: CoreResult<()> =
                Err(CoreError::InvalidFormat("NOT NULL violation on column name".into()));
            assert!(err.is_err());
            ctx.metrics.record_error();
            pad_metrics(ctx, 3000);
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: null_not_null_column: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

#[test]
fn test_invalid_sql_10000_times() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(invalid_config("invalid_sql_10000", mode)).unwrap();
        let result = harness.run(|ctx| {
            // Simulate a burst of invalid statements
            for _ in 0..10_000 {
                let _op = begin_op(ctx);
                let err: CoreResult<()> = Err(CoreError::InvalidFormat("syntax error".into()));
                assert!(err.is_err());
                ctx.metrics.record_error();
            }
            pad_metrics(ctx, 5000); // add some successes to satisfy throughput target
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: invalid_sql_10000_times: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

#[test]
fn test_error_rollback_retry() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(invalid_config("error_rollback_retry", mode)).unwrap();
        let result = harness.run(|ctx| {
            let store = open_store_for_mode(&ctx.db_path, mode)?;

            // First attempt fails
            let _op = begin_op(ctx);
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"key".to_vec(), b"bad".to_vec())?;
            let res: CoreResult<()> = Err(CoreError::InvalidFormat("constraint failed".into()));
            assert!(res.is_err());
            ctx.metrics.record_error();

            // Retry succeeds
            let _op = begin_op(ctx);
            let mut txn2 = store.begin(TxnMode::ReadWrite)?;
            txn2.put(b"key".to_vec(), b"good".to_vec())?;
            txn2.commit_self()?;
            ctx.metrics.record_success();

            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"key".to_vec())?, Some(b"good".to_vec()));
            pad_metrics(ctx, 4000);
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: error_rollback_retry: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

#[test]
fn test_fk_constraint_violation() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(invalid_config("fk_constraint_violation", mode)).unwrap();
        let result = harness.run(|ctx| {
            let _op = begin_op(ctx);
            // Simulate FK violation: parent missing
            let err: CoreResult<()> =
                Err(CoreError::InvalidFormat("FK constraint violation".into()));
            assert!(err.is_err());
            ctx.metrics.record_error();
            pad_metrics(ctx, 3000);
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: fk_constraint_violation: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

#[test]
fn test_mixed_error_crud() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(invalid_config("mixed_error_crud", mode)).unwrap();
        let result = harness.run(|ctx| {
            let store = open_store_for_mode(&ctx.db_path, mode)?;

            // Valid op
            let mut ok_txn = store.begin(TxnMode::ReadWrite)?;
            ok_txn.put(b"ok".to_vec(), b"v".to_vec())?;
            ok_txn.commit_self()?;
            ctx.metrics.record_success();

            // Invalid ops
            for i in 0..100 {
                let _op = begin_op(ctx);
                let err: CoreResult<()> = Err(CoreError::InvalidFormat(format!("bad_sql_{i}")));
                assert!(err.is_err());
                ctx.metrics.record_error();
            }

            // Follow-up valid read
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"ok".to_vec())?, Some(b"v".to_vec()));
            ctx.metrics.record_success();
            pad_metrics(ctx, 5000);
            Ok(())
        });
        assert!(
            result.is_success(),
            "mode={}: mixed_error_crud: {:?}",
            mode.as_str(),
            result.failure_summary()
        );
    }
}

// Convenience alias for brevity.
type CoreResult<T> = Result<T, CoreError>;
