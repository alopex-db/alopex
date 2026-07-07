#![cfg(not(target_arch = "wasm32"))]

mod common;

use common::{
    run_full_consistency_checks, selected_storage_modes, ExecutionModel, Lane, StressTestConfig,
    StressTestHarness,
};
use std::time::Duration;

fn consistency_config(name: &str, lane: Lane) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        lane,
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: Duration::from_secs(30),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: None,
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_full_consistency_suite() {
    let harness =
        StressTestHarness::new(consistency_config("full_consistency_suite", Lane::Nightly))
            .unwrap();
    let modes = selected_storage_modes();
    let result = harness.run(|ctx| run_full_consistency_checks(ctx, &modes));
    assert!(
        result.is_success(),
        "full_consistency_suite: {:?}",
        result.failure_summary()
    );
}
