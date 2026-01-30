use alopex_core::{KVStore, KVTransaction, Result as CoreResult, TxnMode};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use super::artifacts::{
    binary_version, collect_env_metadata, command_hint, detect_git_sha, prepare_artifacts,
    storage_mode_env, system_info, topology_env, write_checks, write_command_txt, write_log,
    write_metrics, write_run_json, CheckSummary, RunMetadata,
};
use super::fixtures::{open_store_for_mode, StressStorageMode};
use super::lane::{should_run, Lane};
use super::metrics::{
    metrics_output_dir, MetricsCollector, MetricsReport, MetricsSummary, SloConfig, SloResult,
};
use super::replay::{deterministic_mode, seed_for_name};
use super::watchdog::{OperationGuard, Watchdog, WatchdogConfig, WatchdogResult};

/// 実行モデル（sync/async × single/multi）。
#[derive(Clone, Copy, Debug)]
pub enum ExecutionModel {
    SyncSingle,
    SyncMulti,
    AsyncSingle,
    AsyncMulti,
}

/// テストハーネス設定。
#[derive(Clone, Debug)]
pub struct StressTestConfig {
    pub name: String,
    pub lane: Lane,
    pub execution_model: ExecutionModel,
    pub concurrency: usize,
    pub scenario_timeout: Duration,
    pub operation_timeout: Duration,
    pub metrics_interval: Duration,
    pub warmup_ops: usize,
    pub slo: Option<SloConfig>,
}

impl Default for StressTestConfig {
    fn default() -> Self {
        Self {
            name: "unnamed_test".to_string(),
            lane: Lane::Ci,
            execution_model: ExecutionModel::SyncSingle,
            concurrency: 1,
            scenario_timeout: Duration::from_secs(300),
            operation_timeout: Duration::from_secs(30),
            metrics_interval: Duration::from_secs(10),
            warmup_ops: 0,
            slo: None,
        }
    }
}

/// テストコンテキスト。
#[derive(Clone)]
pub struct TestContext {
    pub db_path: std::path::PathBuf,
    pub watchdog: Arc<Watchdog>,
    pub metrics: Arc<MetricsCollector>,
    pub thread_id: usize,
    pub lane: Lane,
    pub seed: u64,
}

/// テスト結果。
pub struct TestResult {
    pub watchdog: WatchdogResult,
    pub metrics: MetricsSummary,
    pub slo: SloResult,
    pub duration: Duration,
    pub error: Option<String>,
    pub skipped: bool,
}

impl TestResult {
    pub fn is_success(&self) -> bool {
        self.skipped
            || (matches!(self.watchdog, WatchdogResult::Success)
                && self.slo.passed
                && self.error.is_none())
    }

    pub fn failure_summary(&self) -> Option<String> {
        if self.skipped {
            return None;
        }
        if self.is_success() {
            return None;
        }
        let mut reasons = Vec::new();
        if !matches!(self.watchdog, WatchdogResult::Success) {
            reasons.push(format!("Watchdog: {:?}", self.watchdog));
        }
        if !self.slo.passed {
            let violations: Vec<_> = self
                .slo
                .violations
                .iter()
                .map(|v| format!("{} expected {}, actual {}", v.metric, v.expected, v.actual))
                .collect();
            reasons.push(format!("SLO: {}", violations.join(", ")));
        }
        if let Some(err) = &self.error {
            reasons.push(format!("Error: {err}"));
        }
        Some(reasons.join("; "))
    }
}

struct ArtifactContext<'a> {
    started_at: DateTime<Utc>,
    duration: Duration,
    watchdog_result: &'a WatchdogResult,
    slo_result: &'a SloResult,
    summary: &'a MetricsSummary,
    error: Option<&'a str>,
    skipped: bool,
    report: Option<&'a MetricsReport>,
}

/// ストレステストハーネス。
pub struct StressTestHarness {
    pub config: StressTestConfig,
    watchdog: Arc<Watchdog>,
    metrics: Arc<MetricsCollector>,
    temp_dir: TempDir,
    seed: u64,
}

impl StressTestHarness {
    pub fn new(config: StressTestConfig) -> CoreResult<Self> {
        let temp_dir = TempDir::new()?;
        let watchdog = Arc::new(Watchdog::new(WatchdogConfig {
            operation_timeout: config.operation_timeout,
            scenario_timeout: config.scenario_timeout,
            ..Default::default()
        }));
        let metrics = Arc::new(MetricsCollector::new());
        let seed = seed_for_name(&config.name, 0x5eed_u64);
        Ok(Self {
            config,
            watchdog,
            metrics,
            temp_dir,
            seed,
        })
    }

    fn build_context(&self, thread_id: usize) -> TestContext {
        let db_path = self.temp_dir.path().join(format!("db-{thread_id}.wal"));
        TestContext {
            db_path,
            watchdog: self.watchdog.clone(),
            metrics: self.metrics.clone(),
            thread_id,
            lane: self.config.lane,
            seed: self.seed,
        }
    }

    /// Sync単体実行。
    pub fn run<F>(&self, test_fn: F) -> TestResult
    where
        F: FnOnce(&TestContext) -> CoreResult<()>,
    {
        let started_at = Utc::now();
        if !should_run(self.config.lane) {
            return self.skip_result(started_at);
        }
        let ctx = self.build_context(0);
        self.execute_sync(started_at, || test_fn(&ctx))
    }

    /// Sync並列実行。
    pub fn run_concurrent<F>(&self, test_fn: F) -> TestResult
    where
        F: Fn(usize, &TestContext) -> CoreResult<()> + Send + Sync,
    {
        let started_at = Utc::now();
        if !should_run(self.config.lane) {
            return self.skip_result(started_at);
        }
        let ctx = self.build_context(0);
        self.execute_concurrent(started_at, ctx, test_fn)
    }

    /// Async実行（current-thread）。
    pub fn run_async<F, Fut>(&self, test_fn: F) -> TestResult
    where
        F: Fn(TestContext) -> Fut,
        Fut: std::future::Future<Output = CoreResult<()>>,
    {
        let started_at = Utc::now();
        if !should_run(self.config.lane) {
            return self.skip_result(started_at);
        }
        let ctx = self.build_context(0);
        match self.config.execution_model {
            ExecutionModel::AsyncSingle => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio rt");
                self.execute_sync(started_at, || rt.block_on(test_fn(ctx.clone())))
            }
            ExecutionModel::AsyncMulti | ExecutionModel::SyncMulti | ExecutionModel::SyncSingle => {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(self.config.concurrency.max(1))
                    .build()
                    .expect("tokio rt");
                self.execute_sync(started_at, || rt.block_on(test_fn(ctx.clone())))
            }
        }
    }

    fn execute_sync<T>(
        &self,
        started_at: DateTime<Utc>,
        f: impl FnOnce() -> CoreResult<T>,
    ) -> TestResult {
        let start = Instant::now();
        let mut error = None;
        let watchdog = self.watchdog.clone();
        watchdog.start();
        let res = f();
        if let Err(e) = res {
            error = Some(format!("{e:?}"));
        }
        let watchdog_result = watchdog.finish();
        let duration = start.elapsed();
        let metrics_summary = self.metrics.summary(duration);
        let slo_result = if let Some(cfg) = &self.config.slo {
            self.metrics.verify_slo(cfg, &metrics_summary)
        } else {
            SloResult::passed()
        };
        let report = self.build_metrics_report(
            started_at,
            duration,
            &metrics_summary,
            &slo_result,
            &watchdog_result,
        );
        self.write_metrics_report(&report);
        let ctx = ArtifactContext {
            started_at,
            duration,
            watchdog_result: &watchdog_result,
            slo_result: &slo_result,
            summary: &metrics_summary,
            error: error.as_deref(),
            skipped: false,
            report: Some(&report),
        };
        self.write_artifacts(&ctx);
        TestResult {
            watchdog: watchdog_result,
            metrics: metrics_summary,
            slo: slo_result,
            duration,
            error,
            skipped: false,
        }
    }

    fn execute_concurrent<F>(
        &self,
        started_at: DateTime<Utc>,
        ctx: TestContext,
        test_fn: F,
    ) -> TestResult
    where
        F: Fn(usize, &TestContext) -> CoreResult<()> + Send + Sync,
    {
        let start = Instant::now();
        let watchdog = self.watchdog.clone();
        watchdog.start();
        let test_fn = Arc::new(test_fn);
        let error = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for tid in 0..self.config.concurrency.max(1) {
                let ctx_cloned = TestContext {
                    db_path: ctx.db_path.clone(),
                    watchdog: ctx.watchdog.clone(),
                    metrics: ctx.metrics.clone(),
                    thread_id: tid,
                    lane: ctx.lane,
                    seed: ctx.seed,
                };
                let tf = test_fn.clone();
                handles.push(scope.spawn(move || tf(tid, &ctx_cloned)));
            }
            let mut errs: Vec<String> = Vec::new();
            for h in handles {
                match h.join() {
                    Ok(res) => {
                        if let Err(e) = res {
                            errs.push(format!("{e:?}"));
                        }
                    }
                    Err(panic) => errs.push(format!("panic: {:?}", panic)),
                }
            }
            if errs.is_empty() {
                None
            } else {
                Some(errs.join("; "))
            }
        });

        let watchdog_result = watchdog.finish();
        let duration = start.elapsed();
        let metrics_summary = self.metrics.summary(duration);
        let slo_result = if let Some(cfg) = &self.config.slo {
            self.metrics.verify_slo(cfg, &metrics_summary)
        } else {
            SloResult::passed()
        };
        let report = self.build_metrics_report(
            started_at,
            duration,
            &metrics_summary,
            &slo_result,
            &watchdog_result,
        );
        self.write_metrics_report(&report);
        let ctx = ArtifactContext {
            started_at,
            duration,
            watchdog_result: &watchdog_result,
            slo_result: &slo_result,
            summary: &metrics_summary,
            error: error.as_deref(),
            skipped: false,
            report: Some(&report),
        };
        self.write_artifacts(&ctx);

        TestResult {
            watchdog: watchdog_result,
            metrics: metrics_summary,
            slo: slo_result,
            duration,
            error,
            skipped: false,
        }
    }

    fn build_metrics_report(
        &self,
        timestamp: DateTime<Utc>,
        duration: Duration,
        summary: &MetricsSummary,
        slo_result: &SloResult,
        watchdog_result: &WatchdogResult,
    ) -> MetricsReport {
        let execution_model = format!("{:?}", self.config.execution_model);
        MetricsReport::new(
            self.config.name.clone(),
            execution_model,
            timestamp,
            duration,
            summary.clone(),
            slo_result.clone(),
            format!("{:?}", watchdog_result),
        )
    }

    fn write_metrics_report(&self, report: &MetricsReport) {
        if let Ok(v) = std::env::var("STRESS_REPORT_DIR") {
            if v.is_empty() {
                return;
            }
        }

        let out_dir = metrics_output_dir();
        if out_dir.as_os_str().is_empty() {
            return;
        }
        if std::fs::create_dir_all(&out_dir).is_err() {
            return;
        }

        let sanitize = |s: &str| -> String {
            s.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect()
        };
        let execution_model = report.execution_model.clone();
        let stamp = report.timestamp.format("%Y%m%dT%H%M%SZ").to_string();
        let base = format!(
            "{}__{}__{}",
            sanitize(&self.config.name),
            sanitize(&execution_model),
            stamp
        );

        let _ = std::fs::write(out_dir.join(format!("{base}.json")), report.to_json());
        let _ = std::fs::write(out_dir.join(format!("{base}.md")), report.to_markdown());
    }

    fn skip_result(&self, started_at: DateTime<Utc>) -> TestResult {
        let duration = Duration::from_secs(0);
        let metrics_summary = MetricsSummary::default();
        let slo_result = SloResult::passed();
        let watchdog_result = WatchdogResult::Success;
        let ctx = ArtifactContext {
            started_at,
            duration,
            watchdog_result: &watchdog_result,
            slo_result: &slo_result,
            summary: &metrics_summary,
            error: None,
            skipped: true,
            report: None,
        };
        self.write_artifacts(&ctx);
        TestResult {
            watchdog: watchdog_result,
            metrics: metrics_summary,
            slo: slo_result,
            duration,
            error: None,
            skipped: true,
        }
    }

    fn write_artifacts(&self, ctx: &ArtifactContext<'_>) {
        let Some(paths) = prepare_artifacts(self.config.lane, &self.config.name, &ctx.started_at)
        else {
            return;
        };

        let run_meta = RunMetadata {
            test_name: self.config.name.clone(),
            lane: self.config.lane.to_string(),
            seed: self.seed,
            topology: topology_env(),
            binary_version: binary_version(),
            execution_model: format!("{:?}", self.config.execution_model),
            concurrency: self.config.concurrency,
            scenario_timeout_ms: self.config.scenario_timeout.as_millis() as u64,
            operation_timeout_ms: self.config.operation_timeout.as_millis() as u64,
            metrics_interval_ms: self.config.metrics_interval.as_millis() as u64,
            warmup_ops: self.config.warmup_ops,
            slo: self.config.slo.clone(),
            storage_mode: storage_mode_env(),
            replay: deterministic_mode(),
            started_at: ctx.started_at.to_rfc3339(),
            git_sha: detect_git_sha(),
            system: system_info(),
            env: collect_env_metadata(),
        };
        write_run_json(&paths, &run_meta);

        let package = std::env::var("CARGO_PKG_NAME").unwrap_or_else(|_| "alopex-core".to_string());
        let cmd = command_hint(self.config.lane, self.seed, &package);
        write_command_txt(&paths, &cmd);

        if let Some(report) = ctx.report {
            write_metrics(&paths, report);
        }

        let check = CheckSummary {
            skipped: ctx.skipped,
            success: !ctx.skipped
                && matches!(ctx.watchdog_result, WatchdogResult::Success)
                && ctx.slo_result.passed
                && ctx.error.is_none(),
            watchdog: format!("{:?}", ctx.watchdog_result),
            slo_passed: ctx.slo_result.passed,
            error: ctx.error.map(|v| v.to_string()),
            duration_ms: ctx.duration.as_millis() as u64,
        };
        write_checks(&paths, &check);

        let log = format!(
            "lane={} seed={} success={} skipped={} successes={} errors={} throughput_per_sec={:.2}\n",
            self.config.lane,
            self.seed,
            check.success,
            ctx.skipped,
            ctx.summary.successes,
            ctx.summary.errors,
            ctx.summary.throughput_per_sec
        );
        write_log(&paths, &log);
    }
}

/// 簡易的なテストユーティリティ。
pub fn do_put_get_roundtrip(ctx: &TestContext, mode: StressStorageMode) -> CoreResult<()> {
    let store = open_store_for_mode(&ctx.db_path, mode)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    let key = b"key".to_vec();
    let val = b"value".to_vec();
    txn.put(key.clone(), val.clone())?;
    txn.commit_self()?;

    let mut reader = store.begin(TxnMode::ReadOnly)?;
    let got = reader.get(&key)?;
    assert_eq!(got, Some(val));
    Ok(())
}

/// 操作ごとのウォッチドッグガード。
pub fn begin_op(ctx: &TestContext) -> OperationGuard<'_> {
    ctx.watchdog.begin_operation()
}
