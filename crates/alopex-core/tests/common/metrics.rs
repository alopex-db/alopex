use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// メトリクス集計。
pub struct MetricsCollector {
    successes: AtomicU64,
    errors: AtomicU64,
    latencies_ns: Mutex<Vec<u64>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            successes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latencies_ns: Mutex::new(Vec::new()),
        }
    }

    pub fn record_success(&self) {
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self, d: Duration) {
        let ns = d.as_nanos() as u64;
        let mut guard = self.latencies_ns.lock().unwrap();
        guard.push(ns);
    }

    pub fn summary(&self, window: Duration) -> MetricsSummary {
        let successes = self.successes.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let mut latencies = self.latencies_ns.lock().unwrap().clone();
        latencies.sort_unstable();
        MetricsSummary::from_samples(successes, errors, latencies, window)
    }

    pub fn verify_slo(&self, cfg: &SloConfig, summary: &MetricsSummary) -> SloResult {
        let mut violations = Vec::new();
        if let Some(p99) = cfg.p99_max_latency {
            if summary.p99.map(|v| v > p99).unwrap_or(false) {
                violations.push(SloViolation {
                    metric: "p99_latency_ns".into(),
                    expected: p99.as_nanos().to_string(),
                    actual: summary.p99.unwrap().as_nanos().to_string(),
                });
            }
        }
        if let Some(p95) = cfg.p95_max_latency {
            if summary.p95.map(|v| v > p95).unwrap_or(false) {
                violations.push(SloViolation {
                    metric: "p95_latency_ns".into(),
                    expected: p95.as_nanos().to_string(),
                    actual: summary.p95.unwrap().as_nanos().to_string(),
                });
            }
        }
        if let Some(p999) = cfg.p999_max_latency {
            if summary.p999.map(|v| v > p999).unwrap_or(false) {
                violations.push(SloViolation {
                    metric: "p999_latency_ns".into(),
                    expected: p999.as_nanos().to_string(),
                    actual: summary.p999.unwrap().as_nanos().to_string(),
                });
            }
        }
        if let Some(p9999) = cfg.p9999_max_latency {
            if summary.p9999.map(|v| v > p9999).unwrap_or(false) {
                violations.push(SloViolation {
                    metric: "p9999_latency_ns".into(),
                    expected: p9999.as_nanos().to_string(),
                    actual: summary.p9999.unwrap().as_nanos().to_string(),
                });
            }
        }
        if let Some(min_tp) = cfg.min_throughput {
            if summary.throughput_per_sec < min_tp {
                violations.push(SloViolation {
                    metric: "throughput".into(),
                    expected: format!("{min_tp}"),
                    actual: format!("{:.2}", summary.throughput_per_sec),
                });
            }
        }
        if let Some(max_err_ratio) = cfg.max_error_ratio {
            let total = summary.successes + summary.errors;
            if total > 0 {
                let ratio = summary.errors as f64 / total as f64;
                if ratio > max_err_ratio {
                    violations.push(SloViolation {
                        metric: "error_ratio".into(),
                        expected: format!("{:.4}", max_err_ratio),
                        actual: format!("{:.4}", ratio),
                    });
                }
            }
        }
        SloResult {
            passed: violations.is_empty(),
            violations,
        }
    }
}

/// メトリクスサマリー。
#[derive(Clone, Debug, Default)]
pub struct MetricsSummary {
    pub successes: u64,
    pub errors: u64,
    pub p50: Option<Duration>,
    pub p95: Option<Duration>,
    pub p99: Option<Duration>,
    pub p999: Option<Duration>,
    pub p9999: Option<Duration>,
    pub throughput_per_sec: f64,
}

impl MetricsSummary {
    fn from_samples(successes: u64, errors: u64, latencies_ns: Vec<u64>, window: Duration) -> Self {
        let percentile = |p: f64| -> Option<Duration> {
            if latencies_ns.is_empty() {
                return None;
            }
            let idx = ((latencies_ns.len() as f64 - 1.0) * p).round() as usize;
            latencies_ns.get(idx).copied().map(Duration::from_nanos)
        };
        let total_ops = successes + errors;
        let throughput_per_sec = if window.as_secs_f64() > 0.0 {
            total_ops as f64 / window.as_secs_f64()
        } else {
            0.0
        };

        Self {
            successes,
            errors,
            p50: percentile(0.50),
            p95: percentile(0.95),
            p99: percentile(0.99),
            p999: percentile(0.999),
            p9999: percentile(0.9999),
            throughput_per_sec,
        }
    }
}

/// SLO設定。
#[derive(Clone, Debug, Default)]
pub struct SloConfig {
    pub p99_max_latency: Option<Duration>,
    pub min_throughput: Option<f64>,
    pub p95_max_latency: Option<Duration>,
    pub p999_max_latency: Option<Duration>,
    pub p9999_max_latency: Option<Duration>,
    pub max_error_ratio: Option<f64>,
}

/// SLO判定結果。
#[derive(Clone, Debug)]
pub struct SloResult {
    pub passed: bool,
    pub violations: Vec<SloViolation>,
}

impl SloResult {
    pub fn passed() -> Self {
        Self {
            passed: true,
            violations: Vec::new(),
        }
    }
}

/// SLO違反詳細。
#[derive(Clone, Debug)]
pub struct SloViolation {
    pub metric: String,
    pub expected: String,
    pub actual: String,
}

/// 事前定義のSLOプリセット。
pub mod slo_presets {
    use super::SloConfig;
    use std::time::Duration;

    pub fn get(category: &str) -> Option<SloConfig> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "concurrency",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(200)),
                p95_max_latency: Some(Duration::from_millis(150)),
                p999_max_latency: Some(Duration::from_millis(400)),
                p9999_max_latency: Some(Duration::from_millis(800)),
                min_throughput: Some(1000.0),
                max_error_ratio: Some(0.01),
            },
        );
        map.insert(
            "recovery",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(500)),
                p95_max_latency: Some(Duration::from_millis(400)),
                p999_max_latency: Some(Duration::from_millis(800)),
                p9999_max_latency: Some(Duration::from_millis(1500)),
                min_throughput: Some(500.0),
                max_error_ratio: Some(0.02),
            },
        );
        map.insert(
            "edge_cases",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(400)),
                p95_max_latency: Some(Duration::from_millis(300)),
                p999_max_latency: Some(Duration::from_millis(700)),
                p9999_max_latency: Some(Duration::from_millis(1200)),
                min_throughput: Some(800.0),
                max_error_ratio: Some(0.02),
            },
        );
        map.insert(
            "error_injection",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(800)),
                p95_max_latency: Some(Duration::from_millis(600)),
                p999_max_latency: Some(Duration::from_millis(1500)),
                p9999_max_latency: Some(Duration::from_millis(2000)),
                min_throughput: Some(100.0),
                max_error_ratio: Some(0.05),
            },
        );
        map.insert(
            "multi_model",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(500)),
                p95_max_latency: Some(Duration::from_millis(400)),
                p999_max_latency: Some(Duration::from_millis(900)),
                p9999_max_latency: Some(Duration::from_millis(1500)),
                min_throughput: Some(500.0),
                max_error_ratio: Some(0.02),
            },
        );
        map.insert(
            "ddl_dml",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(800)),
                p95_max_latency: Some(Duration::from_millis(600)),
                p999_max_latency: Some(Duration::from_millis(1500)),
                p9999_max_latency: Some(Duration::from_millis(2000)),
                min_throughput: Some(100.0),
                max_error_ratio: Some(0.05),
            },
        );
        map.insert(
            "invalid_ops",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(500)),
                p95_max_latency: Some(Duration::from_millis(400)),
                p999_max_latency: Some(Duration::from_millis(900)),
                p9999_max_latency: Some(Duration::from_millis(1500)),
                min_throughput: Some(1000.0),
                max_error_ratio: Some(1.0),
            },
        );
        map.insert(
            "chaos",
            SloConfig {
                p99_max_latency: Some(Duration::from_millis(800)),
                p95_max_latency: Some(Duration::from_millis(600)),
                p999_max_latency: Some(Duration::from_millis(1500)),
                p9999_max_latency: Some(Duration::from_millis(2000)),
                min_throughput: Some(100.0),
                max_error_ratio: Some(0.1),
            },
        );
        map.get(category).cloned()
    }
}

/// メトリクスを記録するヘルパー。
pub struct ScopedMetric<'a> {
    metrics: &'a MetricsCollector,
    started: std::time::Instant,
}

impl<'a> ScopedMetric<'a> {
    pub fn new(metrics: &'a MetricsCollector) -> Self {
        Self {
            metrics,
            started: std::time::Instant::now(),
        }
    }

    pub fn success(self) {
        let elapsed = self.started.elapsed();
        self.metrics.record_latency(elapsed);
        self.metrics.record_success();
    }

    pub fn error(self) {
        let elapsed = self.started.elapsed();
        self.metrics.record_latency(elapsed);
        self.metrics.record_error();
    }
}
