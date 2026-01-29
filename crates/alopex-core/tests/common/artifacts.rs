use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::lane::Lane;
use super::metrics::MetricsReport;

#[derive(Clone, Debug)]
pub struct ArtifactPaths {
    pub root: PathBuf,
    pub logs_dir: PathBuf,
    pub checks_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunMetadata {
    pub test_name: String,
    pub lane: String,
    pub seed: u64,
    pub execution_model: String,
    pub concurrency: usize,
    pub scenario_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub metrics_interval_ms: u64,
    pub warmup_ops: usize,
    pub storage_mode: Option<String>,
    pub replay: bool,
    pub started_at: String,
    pub git_sha: Option<String>,
    pub system: SystemInfo,
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct CheckSummary {
    pub skipped: bool,
    pub success: bool,
    pub watchdog: String,
    pub slo_passed: bool,
    pub error: Option<String>,
    pub duration_ms: u64,
}

pub fn prepare_artifacts(
    lane: Lane,
    test_name: &str,
    started_at: &DateTime<Utc>,
) -> Option<ArtifactPaths> {
    if let Ok(value) = std::env::var("STRESS_ARTIFACTS_DIR") {
        if value.trim().is_empty() {
            return None;
        }
    }

    let root = artifacts_root();
    if root.as_os_str().is_empty() {
        return None;
    }
    let stamp = started_at.format("%Y%m%dT%H%M%SZ").to_string();
    let base = root
        .join(lane.as_str())
        .join(sanitize(test_name))
        .join(stamp);

    let logs_dir = base.join("logs");
    let checks_dir = base.join("checks");

    if std::fs::create_dir_all(&logs_dir).is_err() {
        return None;
    }
    if std::fs::create_dir_all(&checks_dir).is_err() {
        return None;
    }

    Some(ArtifactPaths {
        root: base,
        logs_dir,
        checks_dir,
    })
}

pub fn write_run_json(paths: &ArtifactPaths, metadata: &RunMetadata) {
    let path = paths.root.join("run.json");
    if let Ok(body) = serde_json::to_string_pretty(metadata) {
        let _ = std::fs::write(path, body);
    }
}

pub fn write_command_txt(paths: &ArtifactPaths, command: &str) {
    let path = paths.root.join("command.txt");
    let _ = std::fs::write(path, command);
}

pub fn write_metrics(paths: &ArtifactPaths, report: &MetricsReport) {
    let path = paths.root.join("metrics.json");
    let _ = std::fs::write(path, report.to_json());
}

pub fn write_checks(paths: &ArtifactPaths, summary: &CheckSummary) {
    let path = paths.checks_dir.join("summary.json");
    if let Ok(body) = serde_json::to_string_pretty(summary) {
        let _ = std::fs::write(path, body);
    }
}

pub fn write_log(paths: &ArtifactPaths, message: &str) {
    let path = paths.logs_dir.join("harness.log");
    let _ = std::fs::write(path, message);
}

fn artifacts_root() -> PathBuf {
    std::env::var("STRESS_ARTIFACTS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("artifacts"))
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn collect_env_metadata() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (key, value) in std::env::vars() {
        if key == "CI"
            || key.starts_with("STRESS_")
            || key.starts_with("RUST")
            || key.starts_with("CARGO_")
            || key.starts_with("GITHUB_")
        {
            out.insert(key, value);
        }
    }
    out
}

pub fn detect_git_sha() -> Option<String> {
    std::env::var("GITHUB_SHA")
        .ok()
        .or_else(|| std::env::var("GIT_SHA").ok())
}

pub fn command_hint(lane: Lane, seed: u64, package: &str) -> String {
    let feature = format!("lane_{}", lane.as_str());
    format!(
        "STRESS_LANE={lane} STRESS_SEED={seed} cargo test -p {package} --tests --features {feature} -- --ignored"
    )
}

pub fn system_info() -> SystemInfo {
    SystemInfo {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
    }
}

pub fn storage_mode_env() -> Option<String> {
    std::env::var("STRESS_STORAGE_MODE").ok()
}

pub fn log_path(paths: &ArtifactPaths, file: impl AsRef<Path>) -> PathBuf {
    paths.logs_dir.join(file)
}
