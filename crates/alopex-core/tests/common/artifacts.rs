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
    pub mode: EvidenceMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceMode {
    Public,
    Debug,
}

impl EvidenceMode {
    pub fn from_environment() -> Result<Self, &'static str> {
        Self::parse(
            std::env::var("STRESS_EVIDENCE_MODE").as_deref(),
            std::env::var_os("CI").is_some(),
        )
    }

    fn parse(mode: Result<&str, &std::env::VarError>, ci: bool) -> Result<Self, &'static str> {
        match mode {
            Ok("debug") if !ci => Ok(Self::Debug),
            Ok("debug") => Err("debug stress evidence is forbidden in CI"),
            Ok("public") | Err(&std::env::VarError::NotPresent) => Ok(Self::Public),
            _ => Err("invalid STRESS_EVIDENCE_MODE"),
        }
    }
}

#[derive(Serialize)]
pub struct PublicRunMetadata<'a> {
    pub schema_version: u8,
    pub test_name: &'a str,
    pub lane: &'a str,
    pub seed: u64,
    pub binary_version: &'a str,
    pub execution_model: &'a str,
    pub concurrency: usize,
    pub scenario_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub metrics_interval_ms: u64,
    pub warmup_ops: usize,
    pub slo: Option<PublicSloConfig>,
    pub replay: bool,
    pub git_sha: Option<String>,
}

#[derive(Serialize)]
pub struct PublicSloConfig {
    pub p99_max_latency_ns: Option<u64>,
    pub p95_max_latency_ns: Option<u64>,
    pub p999_max_latency_ns: Option<u64>,
    pub p9999_max_latency_ns: Option<u64>,
    pub min_throughput: Option<f64>,
    pub max_error_ratio: Option<f64>,
}

impl From<&SloConfig> for PublicSloConfig {
    fn from(config: &SloConfig) -> Self {
        assert!(config.min_throughput.is_none_or(f64::is_finite));
        assert!(config.max_error_ratio.is_none_or(f64::is_finite));
        let ns = |value: Option<std::time::Duration>| value.map(|v| v.as_nanos() as u64);
        Self {
            p99_max_latency_ns: ns(config.p99_max_latency),
            p95_max_latency_ns: ns(config.p95_max_latency),
            p999_max_latency_ns: ns(config.p999_max_latency),
            p9999_max_latency_ns: ns(config.p9999_max_latency),
            min_throughput: config.min_throughput,
            max_error_ratio: config.max_error_ratio,
        }
    }
}

#[derive(Serialize)]
pub struct PublicCheckSummary {
    pub schema_version: u8,
    pub skipped: bool,
    pub success: bool,
    pub code: &'static str,
    pub watchdog: &'static str,
    pub slo_passed: bool,
    pub duration_ms: u64,
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
    let parent = root.join(lane.as_str()).join(sanitize(test_name));
    let base = reserve_run_directory(&parent, &stamp)?;

    let mode = EvidenceMode::from_environment().expect("valid stress evidence mode");
    let logs_dir = base.join("logs");
    let checks_dir = base.join("checks");

    if mode == EvidenceMode::Debug {
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700))
                .expect("protect local debug artifacts");
        }
    }
    if mode == EvidenceMode::Debug && std::fs::create_dir_all(&logs_dir).is_err() {
        return None;
    }
    if std::fs::create_dir_all(&checks_dir).is_err() {
        return None;
    }

    Some(ArtifactPaths {
        root: base,
        logs_dir,
        checks_dir,
        mode,
    })
}

fn reserve_run_directory(parent: &Path, stamp: &str) -> Option<PathBuf> {
    std::fs::create_dir_all(parent).ok()?;
    for attempt in 0..10_000 {
        let base = parent.join(format!("{stamp}-{attempt:04}"));
        match std::fs::create_dir(&base) {
            Ok(()) => return Some(base),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return None,
        }
    }
    None
}

pub fn write_run_json(paths: &ArtifactPaths, metadata: &RunMetadata) {
    let path = paths.root.join("run.json");
    if let Ok(body) = serde_json::to_string_pretty(metadata) {
        let _ = std::fs::write(path, body);
    }
}

pub fn write_public_run_json(paths: &ArtifactPaths, metadata: &PublicRunMetadata<'_>) {
    write_json(&paths.root.join("run.json"), metadata);
}

pub fn write_public_metrics(paths: &ArtifactPaths, report: &MetricsReport) {
    #[derive(Serialize)]
    struct PublicMetrics {
        schema_version: u8,
        duration_ms: u64,
        successes: u64,
        errors: u64,
        sample_count: usize,
        throughput_per_sec: f64,
        p50_ns: Option<u64>,
        p95_ns: Option<u64>,
        p99_ns: Option<u64>,
        p999_ns: Option<u64>,
        p9999_ns: Option<u64>,
        outlier_count: usize,
        outlier_ratio: f64,
    }
    let summary = &report.summary;
    assert!(summary.throughput_per_sec.is_finite() && summary.outlier_ratio.is_finite());
    let nanos = |value: Option<std::time::Duration>| value.map(|v| v.as_nanos() as u64);
    write_json(
        &paths.root.join("metrics.json"),
        &PublicMetrics {
            schema_version: 1,
            duration_ms: report.duration.as_millis() as u64,
            successes: summary.successes,
            errors: summary.errors,
            sample_count: summary.sample_count,
            throughput_per_sec: summary.throughput_per_sec,
            p50_ns: nanos(summary.p50),
            p95_ns: nanos(summary.p95),
            p99_ns: nanos(summary.p99),
            p999_ns: nanos(summary.p999),
            p9999_ns: nanos(summary.p9999),
            outlier_count: summary.outlier_count,
            outlier_ratio: summary.outlier_ratio,
        },
    );
}

pub fn write_public_checks(paths: &ArtifactPaths, summary: &PublicCheckSummary) {
    write_json(&paths.checks_dir.join("summary.json"), summary);
}

fn write_json<T: Serialize>(path: &Path, value: &T) {
    let body = serde_json::to_vec_pretty(value).expect("serialize stress evidence");
    std::fs::write(path, body).expect("write stress evidence");
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
// `statvfs` field widths vary across Unix libc targets. Keep the explicit
// normalization even when the current target already exposes `u64` fields.
#[allow(clippy::unnecessary_cast)]
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

#[cfg(test)]
mod tests {
    use super::{reserve_run_directory, EvidenceMode, PublicRunMetadata};

    #[test]
    fn repeated_invocations_reserve_distinct_run_directories() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("ci").join("repeated_case");
        let first = reserve_run_directory(&parent, "20261122T010000Z").unwrap();
        let second = reserve_run_directory(&parent, "20261122T010000Z").unwrap();
        assert_ne!(first, second);
        assert!(first.is_dir() && second.is_dir());
    }

    #[test]
    fn evidence_mode_defaults_public_and_rejects_debug_in_ci() {
        assert_eq!(
            EvidenceMode::parse(Err(&std::env::VarError::NotPresent), false),
            Ok(EvidenceMode::Public)
        );
        assert_eq!(
            EvidenceMode::parse(Ok("public"), true),
            Ok(EvidenceMode::Public)
        );
        assert_eq!(
            EvidenceMode::parse(Ok("debug"), false),
            Ok(EvidenceMode::Debug)
        );
        assert!(EvidenceMode::parse(Ok("debug"), true).is_err());
        assert!(EvidenceMode::parse(Ok("unknown"), false).is_err());
    }

    #[test]
    fn public_run_schema_excludes_machine_and_free_text_fields() {
        let run = PublicRunMetadata {
            schema_version: 1,
            test_name: "case_one",
            lane: "nightly",
            seed: 7,
            binary_version: "alopex-core@0.8.17",
            execution_model: "SyncSingle",
            concurrency: 1,
            scenario_timeout_ms: 1000,
            operation_timeout_ms: 100,
            metrics_interval_ms: 10,
            warmup_ops: 0,
            slo: None,
            replay: true,
            git_sha: None,
        };
        let value = serde_json::to_value(run).unwrap();
        let fields = value.as_object().unwrap();
        assert_eq!(fields.len(), 14);
        for prohibited in [
            "system",
            "env",
            "topology",
            "storage_mode",
            "error",
            "command",
            "started_at",
        ] {
            assert!(!fields.contains_key(prohibited));
        }
    }
}
