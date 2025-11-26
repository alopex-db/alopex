//! Observability helpers: counters and sampled search latency.
//!
//! Counters are stored in-process and are lightweight; they are not exported to an external backend.
//! Latencies are sampled in-memory to approximate p50/p95 for debugging/CI assertions.

use std::sync::{Mutex, OnceLock};
use std::sync::atomic::{AtomicU64, Ordering};

/// In-memory metrics store.
struct Metrics {
    vectors_written: AtomicU64,
    vectors_dropped_dim_mismatch: AtomicU64,
    checksum_errors: AtomicU64,
    search_latency_samples: Mutex<Vec<u64>>,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| Metrics {
        vectors_written: AtomicU64::new(0),
        vectors_dropped_dim_mismatch: AtomicU64::new(0),
        checksum_errors: AtomicU64::new(0),
        search_latency_samples: Mutex::new(Vec::with_capacity(64)),
    })
}

/// Snapshot of metrics.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    /// Total vectors successfully written.
    pub vectors_written: u64,
    /// Vectors rejected due to dimension mismatch.
    pub vectors_dropped_dim_mismatch: u64,
    /// Detected checksum errors.
    pub checksum_errors: u64,
    /// Approximate p50 search latency in microseconds (if any samples exist).
    pub search_p50: Option<u64>,
    /// Approximate p95 search latency in microseconds (if any samples exist).
    pub search_p95: Option<u64>,
}

/// Record a successful vector write.
pub fn record_vector_written() {
    metrics()
        .vectors_written
        .fetch_add(1, Ordering::Relaxed);
    tracing::debug!("vector written");
}

/// Record a vector rejected due to dimension mismatch.
pub fn record_dim_mismatch() {
    metrics()
        .vectors_dropped_dim_mismatch
        .fetch_add(1, Ordering::Relaxed);
    tracing::warn!("vector rejected: dimension mismatch");
}

/// Record a checksum error detection.
pub fn record_checksum_error() {
    metrics().checksum_errors.fetch_add(1, Ordering::Relaxed);
    tracing::error!("checksum error detected");
}

/// Record a search latency sample in microseconds.
pub fn record_search_latency_micros(latency_us: u64) {
    let mut guard = metrics()
        .search_latency_samples
        .lock()
        .expect("mutex poisoned");
    if guard.len() >= 64 {
        guard.remove(0);
    }
    guard.push(latency_us);
}

/// Returns a snapshot of counters and approximate p50/p95 latencies.
pub fn snapshot() -> MetricsSnapshot {
    let m = metrics();
    let vectors_written = m.vectors_written.load(Ordering::Relaxed);
    let vectors_dropped_dim_mismatch = m.vectors_dropped_dim_mismatch.load(Ordering::Relaxed);
    let checksum_errors = m.checksum_errors.load(Ordering::Relaxed);
    let (search_p50, search_p95) = {
        let mut samples = m
            .search_latency_samples
            .lock()
            .expect("mutex poisoned")
            .clone();
        if samples.is_empty() {
            (None, None)
        } else {
            samples.sort_unstable();
            (
                Some(percentile(&samples, 50)),
                Some(percentile(&samples, 95)),
            )
        }
    };

    MetricsSnapshot {
        vectors_written,
        vectors_dropped_dim_mismatch,
        checksum_errors,
        search_p50,
        search_p95,
    }
}

fn percentile(sorted: &[u64], p: u64) -> u64 {
    let len = sorted.len();
    if len == 0 {
        return 0;
    }
    let rank = ((len - 1) as f64 * (p as f64 / 100.0)).ceil() as usize;
    sorted[rank.min(len - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        record_vector_written();
        record_vector_written();
        record_dim_mismatch();
        record_checksum_error();
        let snap = snapshot();
        assert_eq!(snap.vectors_written, 2);
        assert_eq!(snap.vectors_dropped_dim_mismatch, 1);
        assert_eq!(snap.checksum_errors, 1);
    }

    #[test]
    fn latency_quantiles() {
        for v in [10, 20, 30, 40, 50] {
            record_search_latency_micros(v);
        }
        let snap = snapshot();
        assert_eq!(snap.search_p50, Some(30));
        assert_eq!(snap.search_p95, Some(50));
    }
}
