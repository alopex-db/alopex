use chrono::{DateTime, Utc};
use serde::Serialize;
use std::path::PathBuf;
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
#[derive(Clone, Debug, Default, Serialize)]
pub struct MetricsSummary {
    pub successes: u64,
    pub errors: u64,
    pub p50: Option<Duration>,
    pub p95: Option<Duration>,
    pub p99: Option<Duration>,
    pub p999: Option<Duration>,
    pub p9999: Option<Duration>,
    pub throughput_per_sec: f64,
    pub sample_count: usize,
    pub outlier_count: usize,
    pub outlier_samples: Vec<Duration>,
    pub outlier_ratio: f64,
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
        let sample_count = latencies_ns.len();
        let total_ops = successes + errors;
        let throughput_per_sec = if window.as_secs_f64() > 0.0 {
            total_ops as f64 / window.as_secs_f64()
        } else {
            0.0
        };
        let p50 = percentile(0.50);
        let p95 = percentile(0.95);
        let p99 = percentile(0.99);
        let p999 = percentile(0.999);
        let p9999 = percentile(0.9999);
        let (outlier_count, outlier_samples, outlier_ratio) = if let Some(th) = p9999.map(|d| d.as_nanos() as u64) {
            let outliers: Vec<u64> = latencies_ns.iter().copied().filter(|v| *v > th).collect();
            let samples = outliers
                .iter()
                .rev()
                .take(100)
                .map(|ns| Duration::from_nanos(*ns))
                .collect();
            let ratio = if sample_count > 0 {
                outliers.len() as f64 / sample_count as f64
            } else {
                0.0
            };
            (outliers.len(), samples, ratio)
        } else {
            (0, Vec::new(), 0.0)
        };

        Self {
            successes,
            errors,
            p50,
            p95,
            p99,
            p999,
            p9999,
            throughput_per_sec,
            sample_count,
            outlier_count,
            outlier_samples,
            outlier_ratio,
        }
    }
}

/// SLO設定。
#[derive(Clone, Debug, Default, Serialize)]
pub struct SloConfig {
    pub p99_max_latency: Option<Duration>,
    pub min_throughput: Option<f64>,
    pub p95_max_latency: Option<Duration>,
    pub p999_max_latency: Option<Duration>,
    pub p9999_max_latency: Option<Duration>,
    pub max_error_ratio: Option<f64>,
}

/// SLO判定結果。
#[derive(Clone, Debug, Serialize)]
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
#[derive(Clone, Debug, Serialize)]
pub struct SloViolation {
    pub metric: String,
    pub expected: String,
    pub actual: String,
}

/// P99.99超の異常値レポート。
#[derive(Clone, Debug, Serialize)]
pub struct OutlierReport {
    pub p9999_latency: Duration,
    pub outlier_count: usize,
    pub outlier_samples: Vec<Duration>,
    pub outlier_ratio: f64,
}

impl OutlierReport {
    pub fn from_summary(summary: &MetricsSummary) -> Self {
        Self {
            p9999_latency: summary.p9999.unwrap_or(Duration::ZERO),
            outlier_count: summary.outlier_count,
            outlier_samples: summary.outlier_samples.clone(),
            outlier_ratio: summary.outlier_ratio,
        }
    }
}

/// メトリクスエクスポート形式。
#[derive(Clone, Debug, Serialize)]
pub struct MetricsReport {
    pub test_name: String,
    pub execution_model: String,
    pub timestamp: DateTime<Utc>,
    pub duration: Duration,
    pub summary: MetricsSummary,
    pub slo_result: SloResult,
    pub watchdog_result: String,
    pub outlier_report: OutlierReport,
}

impl MetricsReport {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        test_name: impl Into<String>,
        execution_model: impl Into<String>,
        timestamp: DateTime<Utc>,
        duration: Duration,
        summary: MetricsSummary,
        slo_result: SloResult,
        watchdog_result: impl Into<String>,
    ) -> Self {
        let outlier_report = OutlierReport::from_summary(&summary);
        Self {
            test_name: test_name.into(),
            execution_model: execution_model.into(),
            timestamp,
            duration,
            summary,
            slo_result,
            watchdog_result: watchdog_result.into(),
            outlier_report,
        }
    }

    /// JSON形式でエクスポート。
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// Markdown形式でエクスポート（P99.99超は参考値扱い）。
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("# Metrics Report: {}\n\n", self.test_name));
        out.push_str(&format!("- Execution Model: {}\n", self.execution_model));
        out.push_str(&format!("- Timestamp: {}\n", self.timestamp.to_rfc3339()));
        out.push_str(&format!("- Duration: {}\n", fmt_duration(self.duration)));
        out.push_str(&format!(
            "- Watchdog: {}\n",
            self.watchdog_result
        ));
        out.push_str(&format!(
            "- SLO: {}\n\n",
            if self.slo_result.passed { "PASS" } else { "FAIL" }
        ));

        out.push_str("## Metrics\n");
        out.push_str("| Metric | Value |\n| --- | --- |\n");
        out.push_str(&format!("| successes | {} |\n", self.summary.successes));
        out.push_str(&format!("| errors | {} |\n", self.summary.errors));
        out.push_str(&format!(
            "| throughput/sec | {:.2} |\n",
            self.summary.throughput_per_sec
        ));
        out.push_str(&format!("| p50 | {} |\n", fmt_opt_duration(self.summary.p50)));
        out.push_str(&format!("| p95 | {} |\n", fmt_opt_duration(self.summary.p95)));
        out.push_str(&format!("| p99 | {} |\n", fmt_opt_duration(self.summary.p99)));
        out.push_str(&format!(
            "| p99.9 | {} |\n",
            fmt_opt_duration(self.summary.p999)
        ));
        out.push_str(&format!(
            "| p99.99 | {} |\n",
            fmt_opt_duration(self.summary.p9999)
        ));
        out.push_str(&format!(
            "| outlier_count(>p99.99) | {} |\n",
            self.outlier_report.outlier_count
        ));
        out.push_str(&format!(
            "| outlier_ratio | {:.4} |\n",
            self.outlier_report.outlier_ratio
        ));

        out.push_str("\n## SLO Violations\n");
        if self.slo_result.violations.is_empty() {
            out.push_str("- None\n");
        } else {
            for v in &self.slo_result.violations {
                out.push_str(&format!(
                    "- {} expected {}, actual {}\n",
                    v.metric, v.expected, v.actual
                ));
            }
        }

        out.push_str("\n## Outlier Samples (reference)\n");
        if self.outlier_report.outlier_samples.is_empty() {
            out.push_str("- None\n");
        } else {
            out.push_str("- top samples (>p99.99): ");
            let samples: Vec<String> = self
                .outlier_report
                .outlier_samples
                .iter()
                .map(|d| fmt_duration(*d))
                .collect();
            out.push_str(&samples.join(", "));
            out.push('\n');
        }

        out
    }
}

/// レポート出力ディレクトリを返す（環境変数 `STRESS_REPORT_DIR` > デフォルト `target/stress-reports`）。
pub fn metrics_output_dir() -> PathBuf {
    std::env::var("STRESS_REPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/stress-reports"))
}

fn fmt_duration(d: Duration) -> String {
    format!("{:.3} ms", d.as_secs_f64() * 1000.0)
}

fn fmt_opt_duration(d: Option<Duration>) -> String {
    d.map(fmt_duration).unwrap_or_else(|| "-".into())
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
