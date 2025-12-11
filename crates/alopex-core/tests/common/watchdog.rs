use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
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

impl WatchdogResult {
    fn as_u64(&self) -> u64 {
        match self {
            WatchdogResult::Success => 0,
            WatchdogResult::OperationTimeout => 1,
            WatchdogResult::ScenarioTimeout => 2,
        }
    }

    fn from_u64(v: u64) -> Self {
        match v {
            1 => WatchdogResult::OperationTimeout,
            2 => WatchdogResult::ScenarioTimeout,
            _ => WatchdogResult::Success,
        }
    }
}

/// 進捗監視のウォッチドッグ。
pub struct Watchdog {
    config: WatchdogConfig,
    start_time: Mutex<Instant>,
    last_progress_ns: AtomicU64,
    active: AtomicBool,
    status: AtomicU64,
    monitor: Mutex<Option<JoinHandle<()>>>,
}

impl Watchdog {
    pub fn new(config: WatchdogConfig) -> Self {
        Self {
            config,
            start_time: Mutex::new(Instant::now()),
            last_progress_ns: AtomicU64::new(0),
            active: AtomicBool::new(false),
            status: AtomicU64::new(WatchdogResult::Success.as_u64()),
            monitor: Mutex::new(None),
        }
    }

    pub fn start(self: &Arc<Self>) {
        {
            let mut start = self.start_time.lock().unwrap();
            *start = Instant::now();
        }
        self.active.store(true, Ordering::Release);
        self.status
            .store(WatchdogResult::Success.as_u64(), Ordering::Release);
        self.last_progress_ns.store(0, Ordering::Relaxed);
        let check_interval = self.config.check_interval;
        let this = Arc::clone(self);
        let handle = thread::spawn(move || {
            while this.active.load(Ordering::Acquire) {
                thread::sleep(check_interval);
                let result = this.evaluate();
                if !matches!(result, WatchdogResult::Success) {
                    this.status.store(result.as_u64(), Ordering::Release);
                    this.active.store(false, Ordering::Release);
                    break;
                }
            }
        });
        let mut guard = self.monitor.lock().unwrap();
        if let Some(old) = guard.take() {
            let _ = old.join();
        }
        *guard = Some(handle);
    }

    pub fn begin_operation(&self) -> OperationGuard<'_> {
        OperationGuard {
            watchdog: self,
            started: Instant::now(),
        }
    }

    pub fn report_progress(&self) {
        let elapsed = Instant::now()
            .saturating_duration_since(*self.start_time.lock().unwrap())
            .as_nanos() as u64;
        self.last_progress_ns.store(elapsed, Ordering::Relaxed);
    }

    pub fn finish(self: &Arc<Self>) -> WatchdogResult {
        self.active.store(false, Ordering::Release);
        if let Some(handle) = self.monitor.lock().unwrap().take() {
            let _ = handle.join();
        }
        let result = self.evaluate();
        self.status.store(result.as_u64(), Ordering::Release);
        result
    }

    fn evaluate(&self) -> WatchdogResult {
        let now = Instant::now();
        let scenario_elapsed = now.duration_since(*self.start_time.lock().unwrap());
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

    pub fn current_status(&self) -> WatchdogResult {
        WatchdogResult::from_u64(self.status.load(Ordering::Acquire))
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
