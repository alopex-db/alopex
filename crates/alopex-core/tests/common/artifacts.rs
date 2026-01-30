use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::lane::Lane;
use super::metrics::{MetricsReport, SloConfig};

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
    pub topology: Option<String>,
    pub binary_version: String,
    pub execution_model: String,
    pub concurrency: usize,
    pub scenario_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub metrics_interval_ms: u64,
    pub warmup_ops: usize,
    pub slo: Option<SloConfig>,
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
    pub kernel: String,
    pub cpu: String,
    pub mem_total_mb: u64,
    pub disk_total_gb: u64,
    pub compiler: String,
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
        if should_capture_env(&key) {
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
        "STRESS_LANE={lane} STRESS_SEED={seed} cargo test -p {package} --tests --features {feature}"
    )
}

pub fn topology_env() -> Option<String> {
    std::env::var("STRESS_TOPOLOGY")
        .ok()
        .and_then(|v| normalize_non_empty(&v))
}

pub fn binary_version() -> String {
    format!("{}@{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}

pub fn system_info() -> SystemInfo {
    SystemInfo {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        kernel: kernel_version(),
        cpu: cpu_info(),
        mem_total_mb: mem_total_mb(),
        disk_total_gb: disk_total_gb(),
        compiler: compiler_version(),
    }
}

pub fn storage_mode_env() -> Option<String> {
    std::env::var("STRESS_STORAGE_MODE").ok()
}

pub fn log_path(paths: &ArtifactPaths, file: impl AsRef<Path>) -> PathBuf {
    paths.logs_dir.join(file)
}

fn should_capture_env(key: &str) -> bool {
    if key == "CI"
        || key.starts_with("STRESS_")
        || key.starts_with("RUST")
        || key.starts_with("CARGO_")
        || key.starts_with("GITHUB_")
        || key.starts_with("RUNNER_")
    {
        return true;
    }
    matches!(
        key,
        "HOSTNAME"
            | "COMPUTERNAME"
            | "USER"
            | "USERNAME"
            | "SHELL"
            | "TERM"
            | "PWD"
            | "RUSTUP_HOME"
            | "RUSTUP_TOOLCHAIN"
            | "CARGO_HOME"
            | "CARGO_TARGET_DIR"
    )
}

fn normalize_non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn kernel_version() -> String {
    if let Some(val) = read_trimmed(Path::new("/proc/sys/kernel/osrelease")) {
        return val;
    }
    command_output("uname", &["-r"]).unwrap_or_else(|| "unknown".to_string())
}

fn cpu_info() -> String {
    if let Some(val) = linux_cpu_model() {
        return val;
    }
    if let Ok(val) = std::env::var("PROCESSOR_IDENTIFIER") {
        if let Some(val) = normalize_non_empty(&val) {
            return val;
        }
    }
    if let Some(val) = command_output("sysctl", &["-n", "machdep.cpu.brand_string"]) {
        return val;
    }
    "unknown".to_string()
}

fn mem_total_mb() -> u64 {
    if let Some(kb) = linux_mem_total_kb() {
        return kb / 1024;
    }
    if let Some(bytes) =
        command_output("sysctl", &["-n", "hw.memsize"]).and_then(|v| v.parse::<u64>().ok())
    {
        return bytes / 1024 / 1024;
    }
    0
}

fn disk_total_gb() -> u64 {
    disk_total_bytes(Path::new("/"))
        .map(|bytes| bytes / 1024 / 1024 / 1024)
        .unwrap_or(0)
}

fn compiler_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    command_output(&rustc, &["--version"]).unwrap_or_else(|| "unknown".to_string())
}

fn read_trimmed(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|v| normalize_non_empty(&v))
}

fn linux_cpu_model() -> Option<String> {
    let content = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("model name") {
            return normalize_non_empty(value.trim_start_matches(':'));
        }
        if let Some(value) = line.strip_prefix("Hardware") {
            return normalize_non_empty(value.trim_start_matches(':'));
        }
    }
    None
}

fn linux_mem_total_kb() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("MemTotal:") {
            let digits = value.split_whitespace().next()?;
            return digits.parse::<u64>().ok();
        }
    }
    None
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    normalize_non_empty(&text)
}

#[cfg(target_family = "unix")]
fn disk_total_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let res = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if res != 0 {
        return None;
    }
    let block_size = stat.f_frsize as u64;
    Some(block_size.saturating_mul(stat.f_blocks as u64))
}

#[cfg(not(target_family = "unix"))]
fn disk_total_bytes(_path: &Path) -> Option<u64> {
    None
}
