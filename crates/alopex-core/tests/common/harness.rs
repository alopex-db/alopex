use alopex_core::{KVStore, KVTransaction, MemoryKV, Result as CoreResult, TxnMode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use super::metrics::{MetricsCollector, MetricsSummary, SloConfig, SloResult};
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
}

/// テスト結果。
pub struct TestResult {
    pub watchdog: WatchdogResult,
    pub metrics: MetricsSummary,
    pub slo: SloResult,
    pub duration: Duration,
    pub error: Option<String>,
}

impl TestResult {
    pub fn is_success(&self) -> bool {
        matches!(self.watchdog, WatchdogResult::Success) && self.slo.passed && self.error.is_none()
    }

    pub fn failure_summary(&self) -> Option<String> {
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

/// ストレステストハーネス。
pub struct StressTestHarness {
    pub config: StressTestConfig,
    watchdog: Arc<Watchdog>,
    metrics: Arc<MetricsCollector>,
    temp_dir: TempDir,
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
        Ok(Self {
            config,
            watchdog,
            metrics,
            temp_dir,
        })
    }

    fn build_context(&self, thread_id: usize) -> TestContext {
        let db_path = self.temp_dir.path().join(format!("db-{thread_id}.wal"));
        TestContext {
            db_path,
            watchdog: self.watchdog.clone(),
            metrics: self.metrics.clone(),
            thread_id,
        }
    }

    /// Sync単体実行。
    pub fn run<F>(&self, test_fn: F) -> TestResult
    where
        F: FnOnce(&TestContext) -> CoreResult<()>,
    {
        let ctx = self.build_context(0);
        self.execute_sync(|| test_fn(&ctx))
    }

    /// Sync並列実行。
    pub fn run_concurrent<F>(&self, test_fn: F) -> TestResult
    where
        F: Fn(usize, &TestContext) -> CoreResult<()> + Send + Sync,
    {
        let ctx = self.build_context(0);
        self.execute_concurrent(ctx, test_fn)
    }

    /// Async実行（current-thread）。
    pub fn run_async<F, Fut>(&self, test_fn: F) -> TestResult
    where
        F: Fn(TestContext) -> Fut,
        Fut: std::future::Future<Output = CoreResult<()>>,
    {
        let ctx = self.build_context(0);
        match self.config.execution_model {
            ExecutionModel::AsyncSingle => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio rt");
                self.execute_sync(|| rt.block_on(test_fn(ctx.clone())))
            }
            ExecutionModel::AsyncMulti | ExecutionModel::SyncMulti | ExecutionModel::SyncSingle => {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(self.config.concurrency.max(1))
                    .build()
                    .expect("tokio rt");
                self.execute_sync(|| rt.block_on(test_fn(ctx.clone())))
            }
        }
    }

    fn execute_sync<T>(&self, f: impl FnOnce() -> CoreResult<T>) -> TestResult {
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
        TestResult {
            watchdog: watchdog_result,
            metrics: metrics_summary,
            slo: slo_result,
            duration,
            error,
        }
    }

    fn execute_concurrent<F>(&self, ctx: TestContext, test_fn: F) -> TestResult
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
                };
                let tf = test_fn.clone();
                handles.push(scope.spawn(move || tf(tid, &ctx_cloned)));
            }
            let mut err: Option<String> = None;
            for h in handles {
                if let Err(e) = h.join() {
                    err = Some(format!("panic: {:?}", e));
                }
            }
            if let Some(e) = err {
                return Some(e);
            }
            None
        });

        let watchdog_result = watchdog.finish();
        let duration = start.elapsed();
        let metrics_summary = self.metrics.summary(duration);
        let slo_result = if let Some(cfg) = &self.config.slo {
            self.metrics.verify_slo(cfg, &metrics_summary)
        } else {
            SloResult::passed()
        };

        TestResult {
            watchdog: watchdog_result,
            metrics: metrics_summary,
            slo: slo_result,
            duration,
            error,
        }
    }
}

/// 簡易的なテストユーティリティ。
pub fn do_put_get_roundtrip(ctx: &TestContext) -> CoreResult<()> {
    let store = MemoryKV::open(&ctx.db_path)?;
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
pub fn begin_op(ctx: &TestContext) -> OperationGuard {
    ctx.watchdog.begin_operation()
}
