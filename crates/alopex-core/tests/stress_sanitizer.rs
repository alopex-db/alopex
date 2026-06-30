#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::{KVStore, KVTransaction, TxnMode};
use common::{
    begin_op, do_put_get_roundtrip, selected_storage_modes, ExecutionModel, Lane, StressTestConfig,
    StressTestHarness,
};
use std::time::Duration;

#[cfg_attr(not(feature = "lane_sanitizer"), ignore)]
#[test]
fn test_sanitizer_kv_smoke() {
    let cfg = StressTestConfig {
        name: "sanitizer_kv_smoke".to_string(),
        lane: Lane::Sanitizer,
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
        for mode in selected_storage_modes() {
            do_put_get_roundtrip(ctx, mode)?;
            let store = common::open_store_for_mode(&ctx.db_path, mode)?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"san".to_vec(), b"itizer".to_vec())?;
            txn.commit_self()?;
            ctx.metrics.record_success();
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "sanitizer_kv_smoke: {:?}",
        result.failure_summary()
    );
}
