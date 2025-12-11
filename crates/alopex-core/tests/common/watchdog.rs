use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// ウォッチドッグ設定。
#[derive(Clone, Debug)]
pub struct WatchdogConfig {
    pub operation_timeout: Duration,
    pub scenario_timeout: Duration,
    pub lock_hold_warning: Duration,
    pub check_interval: Duration,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(30),
            scenario_timeout: Duration::from_secs(300),
            lock_hold_warning: Duration::from_secs(10),
            check_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Clone, Debug)]
pub enum WatchdogResult {
    Success,
    OperationTimeout,
    ScenarioTimeout,
}

/// 進捗監視のウォッチドッグ。
pub struct Watchdog {
    config: WatchdogConfig,
    start_time: Instant,
    last_progress_ns: AtomicU64,
    active: AtomicBool,
}

impl Watchdog {
    pub fn new(config: WatchdogConfig) -> Self {
        let now = Instant::now();
        Self {
            config,
            start_time: now,
            last_progress_ns: AtomicU64::new(0),
            active: AtomicBool::new(false),
        }
    }

    pub fn start(&self) {
        self.active.store(true, Ordering::Release);
        self.last_progress_ns.store(0, Ordering::Relaxed);
    }

    pub fn begin_operation(&self) -> OperationGuard {
        OperationGuard {
            watchdog: self,
            started: Instant::now(),
        }
    }

    pub fn report_progress(&self) {
        let elapsed = Instant::now()
            .saturating_duration_since(self.start_time)
            .as_nanos() as u64;
        self.last_progress_ns.store(elapsed, Ordering::Relaxed);
    }

    pub fn finish(&self) -> WatchdogResult {
        if !self.active.swap(false, Ordering::AcqRel) {
            return WatchdogResult::Success;
        }
        self.evaluate()
    }

    fn evaluate(&self) -> WatchdogResult {
        let now = Instant::now();
        let scenario_elapsed = now.duration_since(self.start_time);
        if scenario_elapsed > self.config.scenario_timeout {
            return WatchdogResult::ScenarioTimeout;
        }
        let last_progress_ns = self.last_progress_ns.load(Ordering::Relaxed);
        let last_progress = Duration::from_nanos(last_progress_ns);
        if scenario_elapsed.saturating_sub(last_progress) > self.config.operation_timeout {
            return WatchdogResult::OperationTimeout;
        }
        WatchdogResult::Success
    }
}

/// RAII操作ガード。
pub struct OperationGuard<'a> {
    pub(crate) watchdog: &'a Watchdog,
    started: Instant,
}

impl<'a> Drop for OperationGuard<'a> {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        if elapsed > self.watchdog.config.lock_hold_warning {
            // ここではログを出さず、進捗だけ更新する。
        }
        self.watchdog.report_progress();
    }
}
