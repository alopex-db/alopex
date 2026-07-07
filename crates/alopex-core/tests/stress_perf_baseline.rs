#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::{KVStore, KVTransaction, TxnMode};
use common::{
    open_store_for_mode, storage_root_for_mode, ExecutionModel, Lane, StressStorageMode,
    StressTestConfig, StressTestHarness, WorkloadConfig, WorkloadGenerator,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PerfBaseline {
    metrics: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, Serialize)]
struct PerfViolation {
    metric: String,
    expected: String,
    actual: String,
    direction: String,
}

#[derive(Clone, Debug, Serialize)]
struct PerfReport {
    metrics: BTreeMap<String, f64>,
    baseline: Option<PerfBaseline>,
    violations: Vec<PerfViolation>,
    skipped: bool,
}

fn perf_config(name: &str) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        lane: Lane::Perf,
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: Duration::from_secs(60),
        operation_timeout: Duration::from_secs(10),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: None,
    }
}

fn baseline_dir() -> PathBuf {
    std::env::var("STRESS_BASELINE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/stress-baselines"))
}

fn baseline_path(name: &str) -> PathBuf {
    baseline_dir().join(format!("{name}.json"))
}

fn baseline_update_mode() -> bool {
    std::env::var("STRESS_BASELINE_UPDATE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn baseline_required() -> bool {
    std::env::var("STRESS_BASELINE_REQUIRED")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn baseline_margin_pct() -> f64 {
    std::env::var("STRESS_BASELINE_MARGIN_PCT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.20)
}

fn load_baseline(name: &str) -> Option<PerfBaseline> {
    let path = baseline_path(name);
    let body = fs::read_to_string(path).ok()?;
    serde_json::from_str(&body).ok()
}

fn save_baseline(name: &str, baseline: &PerfBaseline) {
    let path = baseline_path(name);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(body) = serde_json::to_string_pretty(baseline) {
        let _ = fs::write(path, body);
    }
}

fn compare_metrics(
    current: &BTreeMap<String, f64>,
    baseline: &PerfBaseline,
    expectations: &[(&str, bool)],
    margin_pct: f64,
) -> Vec<PerfViolation> {
    let mut violations = Vec::new();
    for (metric, higher_is_better) in expectations {
        let Some(current_val) = current.get(*metric) else {
            continue;
        };
        let Some(base_val) = baseline.metrics.get(*metric) else {
            continue;
        };
        if *higher_is_better {
            let min_allowed = base_val * (1.0 - margin_pct);
            if *current_val < min_allowed {
                violations.push(PerfViolation {
                    metric: metric.to_string(),
                    expected: format!(">= {:.3}", min_allowed),
                    actual: format!("{:.3}", current_val),
                    direction: "higher_is_better".to_string(),
                });
            }
        } else {
            let max_allowed = base_val * (1.0 + margin_pct);
            if *current_val > max_allowed {
                violations.push(PerfViolation {
                    metric: metric.to_string(),
                    expected: format!("<= {:.3}", max_allowed),
                    actual: format!("{:.3}", current_val),
                    direction: "lower_is_better".to_string(),
                });
            }
        }
    }
    violations
}

fn percentile(samples: &mut [u64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable();
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples.get(idx).copied().map(|v| v as f64).unwrap_or(0.0)
}

fn write_perf_report(ctx: &common::TestContext, file: &str, report: &PerfReport) {
    let Some(paths) = ctx.artifact_paths.as_ref() else {
        return;
    };
    let path = paths.checks_dir.join(file);
    if let Ok(body) = serde_json::to_string_pretty(report) {
        let _ = fs::write(path, body);
    }
}

#[cfg_attr(not(feature = "lane_perf"), ignore)]
#[test]
fn perf_mixed_point_latency() {
    let harness = StressTestHarness::new(perf_config("perf_mixed_point_latency")).unwrap();
    let result = harness.run(|ctx| {
        let mode = StressStorageMode::Memory;
        let store = open_store_for_mode(&ctx.db_path, mode)?;
        let mut rng = WorkloadGenerator::new(WorkloadConfig {
            operation_count: 300,
            key_space_size: 100,
            value_size: 128,
            seed: ctx.seed,
        });

        let mut latencies = Vec::new();
        let started = Instant::now();
        for _ in 0..300 {
            let op = rng.next_operation();
            let op_start = Instant::now();
            match op {
                common::Operation::Get(key) => {
                    let mut txn = store.begin(TxnMode::ReadOnly)?;
                    let _ = txn.get(&key)?;
                }
                common::Operation::Put(key, val) => {
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    txn.put(key, val)?;
                    txn.commit_self()?;
                }
                common::Operation::Delete(key) => {
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    txn.delete(key)?;
                    txn.commit_self()?;
                }
                common::Operation::Scan(prefix) => {
                    let mut txn = store.begin(TxnMode::ReadOnly)?;
                    let _ = txn.scan_prefix(&prefix)?.next();
                }
            }
            let elapsed = op_start.elapsed();
            ctx.metrics.record_success();
            ctx.metrics.record_latency(elapsed);
            latencies.push(elapsed.as_nanos() as u64);
        }
        let duration = started.elapsed().as_secs_f64().max(0.001);
        let mut metrics = BTreeMap::new();
        metrics.insert("throughput_ops_sec".to_string(), 300.0 / duration);
        let p50 = percentile(&mut latencies, 0.50);
        let p95 = percentile(&mut latencies, 0.95);
        let p99 = percentile(&mut latencies, 0.99);
        metrics.insert("p50_latency_ns".to_string(), p50);
        metrics.insert("p95_latency_ns".to_string(), p95);
        metrics.insert("p99_latency_ns".to_string(), p99);

        let baseline = load_baseline("perf_mixed_point_latency");
        let mut violations = Vec::new();
        if let Some(base) = baseline.clone() {
            let expectations = [
                ("throughput_ops_sec", true),
                ("p50_latency_ns", false),
                ("p95_latency_ns", false),
                ("p99_latency_ns", false),
            ];
            violations = compare_metrics(&metrics, &base, &expectations, baseline_margin_pct());
        } else if baseline_required() {
            violations.push(PerfViolation {
                metric: "baseline".to_string(),
                expected: "baseline required".to_string(),
                actual: "missing".to_string(),
                direction: "baseline".to_string(),
            });
        }

        let report = PerfReport {
            metrics: metrics.clone(),
            baseline: baseline.clone(),
            violations: violations.clone(),
            skipped: false,
        };
        write_perf_report(ctx, "perf_mixed_point_latency.json", &report);

        if baseline_update_mode() {
            save_baseline("perf_mixed_point_latency", &PerfBaseline { metrics });
        }
        if !violations.is_empty() {
            return Err(alopex_core::Error::InvalidFormat(
                "performance regression detected".into(),
            ));
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "perf_mixed_point_latency: {:?}",
        result.failure_summary()
    );
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(meta) = entry.metadata() {
                if meta.is_dir() {
                    total = total.saturating_add(dir_size(&path));
                } else {
                    total = total.saturating_add(meta.len());
                }
            }
        }
    }
    total
}

#[cfg_attr(not(feature = "lane_perf"), ignore)]
#[test]
fn perf_sequential_throughput_and_amplification() {
    let harness =
        StressTestHarness::new(perf_config("perf_sequential_throughput_and_amplification"))
            .unwrap();
    let result = harness.run(|ctx| {
        let mode = StressStorageMode::Disk;
        let store = open_store_for_mode(&ctx.db_path, mode)?;
        let root = storage_root_for_mode(&ctx.db_path, mode);
        let started = Instant::now();
        let mut bytes_written = 0u64;

        for i in 0..500u32 {
            let key = format!("seq_{i:06}").into_bytes();
            let value = vec![b'x'; 256];
            bytes_written += (key.len() + value.len()) as u64;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(key, value)?;
            txn.commit_self()?;
            ctx.metrics.record_success();
        }

        store.flush()?;
        let duration = started.elapsed().as_secs_f64().max(0.001);
        let throughput = 500.0 / duration;

        let wal_bytes = fs::metadata(root.join("lsm.wal"))
            .map(|m| m.len())
            .unwrap_or(0);
        let total_bytes = dir_size(&root);
        let write_amp = if bytes_written > 0 {
            (wal_bytes + total_bytes) as f64 / bytes_written as f64
        } else {
            0.0
        };
        let space_amp = if bytes_written > 0 {
            total_bytes as f64 / bytes_written as f64
        } else {
            0.0
        };

        let mut metrics = BTreeMap::new();
        metrics.insert("throughput_ops_sec".to_string(), throughput);
        metrics.insert("write_amplification".to_string(), write_amp);
        metrics.insert("space_amplification".to_string(), space_amp);

        let baseline = load_baseline("perf_sequential_throughput_and_amplification");
        let mut violations = Vec::new();
        if let Some(base) = baseline.clone() {
            let expectations = [
                ("throughput_ops_sec", true),
                ("write_amplification", false),
                ("space_amplification", false),
            ];
            violations = compare_metrics(&metrics, &base, &expectations, baseline_margin_pct());
        } else if baseline_required() {
            violations.push(PerfViolation {
                metric: "baseline".to_string(),
                expected: "baseline required".to_string(),
                actual: "missing".to_string(),
                direction: "baseline".to_string(),
            });
        }

        let report = PerfReport {
            metrics: metrics.clone(),
            baseline: baseline.clone(),
            violations: violations.clone(),
            skipped: false,
        };
        write_perf_report(
            ctx,
            "perf_sequential_throughput_and_amplification.json",
            &report,
        );

        if baseline_update_mode() {
            save_baseline(
                "perf_sequential_throughput_and_amplification",
                &PerfBaseline { metrics },
            );
        }
        if !violations.is_empty() {
            return Err(alopex_core::Error::InvalidFormat(
                "performance regression detected".into(),
            ));
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "perf_sequential_throughput_and_amplification: {:?}",
        result.failure_summary()
    );
}
