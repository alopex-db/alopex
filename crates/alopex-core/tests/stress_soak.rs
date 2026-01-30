mod common;

use alopex_core::{Error as CoreError, KVStore, KVTransaction, TxnMode};
use chrono::Utc;
use common::{
    begin_op, log_path, open_store_for_mode, prepare_artifacts, selected_storage_modes,
    storage_root_for_mode, ExecutionModel, Lane, StressStorageMode, StressTestConfig,
    StressTestHarness, WorkloadConfig, WorkloadGenerator,
};
use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

type CoreResult<T> = Result<T, CoreError>;

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_soak_point_workload_24h() {
    let result = run_point_soak(Lane::Nightly);
    assert!(
        result.is_success(),
        "soak_point_workload: {:?}",
        result.failure_summary()
    );
}

#[cfg_attr(not(feature = "lane_weekly"), ignore)]
#[test]
fn test_weekly_sequential_workload_24h() {
    let result = run_sequential_soak(Lane::Weekly);
    assert!(
        result.is_success(),
        "weekly_sequential_workload: {:?}",
        result.failure_summary()
    );
}

fn run_point_soak(lane: Lane) -> common::TestResult {
    let duration = soak_duration(lane);
    let batch_size = env_usize("STRESS_SOAK_BATCH_SIZE", 200).max(1);
    let value_size = env_usize("STRESS_SOAK_VALUE_SIZE", 128).max(8);
    let key_space = env_usize("STRESS_SOAK_KEY_SPACE", 100_000).max(128);
    let max_db_bytes = soak_max_db_bytes();

    let test_name = format!("soak_point_{lane}");
    let cfg = StressTestConfig {
        name: test_name.clone(),
        lane,
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: duration + Duration::from_secs(60),
        operation_timeout: Duration::from_secs(20),
        metrics_interval: Duration::from_secs(10),
        warmup_ops: 0,
        slo: common::slo_presets::get("concurrency"),
    };
    let harness = StressTestHarness::new(cfg).unwrap();
    harness.run(|ctx| {
        let _op = begin_op(ctx);
        let mut monitor = SoakMonitor::new(ctx.lane, &test_name);
        for mode in selected_storage_modes() {
            let store = open_store_for_mode(&ctx.db_path, mode)?;
            let mut gen = WorkloadGenerator::new(WorkloadConfig {
                operation_count: batch_size,
                key_space_size: key_space,
                value_size,
                seed: 0x5a0d_u64 ^ ctx.seed,
            });
            let started = Instant::now();
            while started.elapsed() < duration {
                let batch_start = Instant::now();
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                let mut successes = 0usize;
                let mut errors = 0usize;
                for op in gen.generate_batch() {
                    if apply_kv_op(&mut txn, op).is_ok() {
                        successes += 1;
                    } else {
                        errors += 1;
                    }
                }
                txn.commit_self()?;
                for _ in 0..successes {
                    ctx.metrics.record_success();
                }
                for _ in 0..errors {
                    ctx.metrics.record_error();
                }
                ctx.metrics.record_latency(batch_start.elapsed());
                monitor.maybe_check(&ctx.db_path, mode, max_db_bytes)?;
            }
        }
        Ok(())
    })
}

fn run_sequential_soak(lane: Lane) -> common::TestResult {
    let duration = soak_duration(lane);
    let batch_size = env_usize("STRESS_SOAK_BATCH_SIZE", 400).max(1);
    let value_size = env_usize("STRESS_SOAK_VALUE_SIZE", 256).max(8);
    let max_db_bytes = soak_max_db_bytes();

    let test_name = format!("soak_sequential_{lane}");
    let cfg = StressTestConfig {
        name: test_name.clone(),
        lane,
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: duration + Duration::from_secs(60),
        operation_timeout: Duration::from_secs(20),
        metrics_interval: Duration::from_secs(10),
        warmup_ops: 0,
        slo: common::slo_presets::get("concurrency"),
    };
    let harness = StressTestHarness::new(cfg).unwrap();
    harness.run(|ctx| {
        let _op = begin_op(ctx);
        let mut monitor = SoakMonitor::new(ctx.lane, &test_name);
        for mode in selected_storage_modes() {
            let store = open_store_for_mode(&ctx.db_path, mode)?;
            let mut seq = 0u64;
            let started = Instant::now();
            while started.elapsed() < duration {
                let batch_start = Instant::now();
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                for _ in 0..batch_size {
                    let key = format!("seq_{:012}", seq).into_bytes();
                    let val = vec![b'x'; value_size];
                    txn.put(key, val)?;
                    seq = seq.wrapping_add(1);
                    ctx.metrics.record_success();
                }
                txn.commit_self()?;
                ctx.metrics.record_latency(batch_start.elapsed());
                monitor.maybe_check(&ctx.db_path, mode, max_db_bytes)?;
            }
        }
        Ok(())
    })
}

fn apply_kv_op<'a>(txn: &mut impl KVTransaction<'a>, op: common::Operation) -> CoreResult<()> {
    match op {
        common::Operation::Get(key) => {
            let _ = txn.get(&key)?;
        }
        common::Operation::Put(key, val) => {
            txn.put(key, val)?;
        }
        common::Operation::Delete(key) => {
            txn.delete(key)?;
        }
        common::Operation::Scan(prefix) => {
            let _ = txn.scan_prefix(&prefix)?.next();
        }
    }
    Ok(())
}

fn soak_duration(lane: Lane) -> Duration {
    let default_secs = 24 * 60 * 60;
    let key = match lane {
        Lane::Soak => "STRESS_SOAK_DURATION_SECS",
        Lane::Weekly => "STRESS_WEEKLY_DURATION_SECS",
        _ => "STRESS_SOAK_DURATION_SECS",
    };
    Duration::from_secs(env_u64(key, default_secs))
}

fn soak_max_db_bytes() -> u64 {
    let mb = env_u64("STRESS_SOAK_MAX_DB_MB", 1024);
    mb.saturating_mul(1024 * 1024)
}

fn storage_path_for_mode(base_path: &Path, mode: StressStorageMode) -> PathBuf {
    match mode {
        StressStorageMode::Memory => base_path.to_path_buf(),
        StressStorageMode::Disk => storage_root_for_mode(base_path, mode),
    }
}

fn dir_size_bytes(path: &Path) -> u64 {
    if let Ok(meta) = fs::metadata(path) {
        if meta.is_file() {
            return meta.len();
        }
    }
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            total = total.saturating_add(dir_size_bytes(&entry.path()));
        }
    }
    total
}

struct SoakMonitor {
    last_check: Instant,
    check_interval: Duration,
    max_rss_mb: u64,
    timeline_path: Option<PathBuf>,
}

impl SoakMonitor {
    fn new(lane: Lane, name: &str) -> Self {
        let started_at = Utc::now();
        let timeline_path = prepare_artifacts(lane, name, &started_at)
            .map(|paths| log_path(&paths, "soak_resource.log"));
        Self {
            last_check: Instant::now(),
            check_interval: Duration::from_secs(env_u64("STRESS_SOAK_CHECK_INTERVAL_SECS", 30)),
            max_rss_mb: env_u64("STRESS_SOAK_MAX_RSS_MB", 0),
            timeline_path,
        }
    }

    fn maybe_check(
        &mut self,
        base_path: &Path,
        mode: StressStorageMode,
        max_disk_bytes: u64,
    ) -> CoreResult<()> {
        if self.last_check.elapsed() < self.check_interval {
            return Ok(());
        }
        self.last_check = Instant::now();
        let rss_mb = current_rss_mb();
        let disk_bytes = dir_size_bytes(&storage_path_for_mode(base_path, mode));
        self.log_event(rss_mb, disk_bytes);
        if self.max_rss_mb > 0 && rss_mb > self.max_rss_mb {
            self.log_limit("rss", rss_mb, self.max_rss_mb);
            return Err(CoreError::Io(std::io::Error::other(format!(
                "rss {} MB exceeded budget {} MB",
                rss_mb, self.max_rss_mb
            ))));
        }
        if max_disk_bytes > 0 && disk_bytes > max_disk_bytes {
            let max_mb = max_disk_bytes / (1024 * 1024);
            self.log_limit("disk", disk_bytes, max_disk_bytes);
            return Err(CoreError::Io(std::io::Error::other(format!(
                "disk usage {} bytes exceeded budget {} bytes ({} MB)",
                disk_bytes, max_disk_bytes, max_mb
            ))));
        }
        Ok(())
    }

    fn log_event(&self, rss_mb: u64, disk_bytes: u64) {
        let Some(path) = &self.timeline_path else {
            return;
        };
        let line = format!(
            "ts={} rss_mb={} disk_bytes={}\n",
            Utc::now().to_rfc3339(),
            rss_mb,
            disk_bytes
        );
        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(line.as_bytes())
            });
    }

    fn log_limit(&self, kind: &str, observed: u64, limit: u64) {
        let Some(path) = &self.timeline_path else {
            return;
        };
        let line = format!(
            "ts={} event=limit_exceeded kind={} observed={} limit={}\n",
            Utc::now().to_rfc3339(),
            kind,
            observed,
            limit
        );
        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(line.as_bytes())
            });
    }
}

fn current_rss_mb() -> u64 {
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                if let Some(kb) = rest.split_whitespace().next() {
                    if let Ok(kb) = kb.parse::<u64>() {
                        return kb / 1024;
                    }
                }
            }
        }
    }
    0
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}
