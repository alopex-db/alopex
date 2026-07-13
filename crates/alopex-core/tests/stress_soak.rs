#![cfg(not(target_arch = "wasm32"))]

use alopex_core::Error as CoreError;
use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::tempdir;

type CoreResult<T> = Result<T, CoreError>;

#[test]
fn dir_size_bytes_sums_nested_files() {
    let dir = tempdir().expect("tempdir");
    let nested = dir.path().join("nested");
    fs::create_dir(&nested).expect("create nested dir");
    fs::write(dir.path().join("root.bin"), vec![1u8; 7]).expect("write root");
    fs::write(nested.join("child.bin"), vec![2u8; 11]).expect("write child");

    assert_eq!(dir_size_bytes(dir.path()), 18);
}

#[test]
fn monitor_allows_disk_usage_within_budget() {
    let dir = tempdir().expect("tempdir");
    fs::write(dir.path().join("data.bin"), vec![0u8; 16]).expect("write data");
    let mut monitor = SoakMonitor::new(Duration::ZERO, 0, None);

    monitor
        .maybe_check(dir.path(), 16)
        .expect("usage at budget should pass");
}

#[test]
fn monitor_rejects_disk_usage_over_budget() {
    let dir = tempdir().expect("tempdir");
    fs::write(dir.path().join("data.bin"), vec![0u8; 17]).expect("write data");
    let log_path = dir.path().join("soak_resource.log");
    let mut monitor = SoakMonitor::new(Duration::ZERO, 0, Some(log_path.clone()));

    let err = monitor
        .maybe_check(dir.path(), 16)
        .expect_err("usage over budget should fail");

    let message = err.to_string();
    assert!(message.contains("disk usage 17 bytes exceeded budget 16 bytes"));
    let log = fs::read_to_string(log_path).expect("read monitor log");
    assert!(log.contains("event=limit_exceeded kind=disk observed=17 limit=16"));
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
    fn new(check_interval: Duration, max_rss_mb: u64, timeline_path: Option<PathBuf>) -> Self {
        Self {
            last_check: Instant::now()
                .checked_sub(check_interval)
                .unwrap_or_else(Instant::now),
            check_interval,
            max_rss_mb,
            timeline_path,
        }
    }

    fn maybe_check(&mut self, path: &Path, max_disk_bytes: u64) -> CoreResult<()> {
        if self.last_check.elapsed() < self.check_interval {
            return Ok(());
        }
        self.last_check = Instant::now();
        let rss_mb = current_rss_mb();
        let disk_bytes = dir_size_bytes(path);
        self.log_event(rss_mb, disk_bytes);
        if self.max_rss_mb > 0 && rss_mb > self.max_rss_mb {
            self.log_limit("rss", rss_mb, self.max_rss_mb);
            return Err(CoreError::Io(std::io::Error::other(format!(
                "rss {} MB exceeded budget {} MB",
                rss_mb, self.max_rss_mb
            ))));
        }
        if max_disk_bytes > 0 && disk_bytes > max_disk_bytes {
            self.log_limit("disk", disk_bytes, max_disk_bytes);
            return Err(CoreError::Io(std::io::Error::other(format!(
                "disk usage {} bytes exceeded budget {} bytes",
                disk_bytes, max_disk_bytes
            ))));
        }
        Ok(())
    }

    fn log_event(&self, rss_mb: u64, disk_bytes: u64) {
        let Some(path) = &self.timeline_path else {
            return;
        };
        let line = format!("rss_mb={rss_mb} disk_bytes={disk_bytes}\n");
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
        let line = format!("event=limit_exceeded kind={kind} observed={observed} limit={limit}\n");
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
