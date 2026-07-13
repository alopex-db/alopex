//! Generic streaming memory tracking primitives.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{Error, Result};

/// Default memory threshold used before spilling streaming SQL state.
pub const DEFAULT_SPILL_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;

/// Policy for handling memory-limit crossings.
#[derive(Clone, Debug)]
pub enum SpillPolicy {
    /// Return an error immediately when memory usage exceeds the configured limit.
    FailFast,
    /// Allow the caller to spill buffered state to files in `directory`.
    SpillToDisk {
        /// Directory where spill files are allocated.
        directory: PathBuf,
    },
}

/// Sink for spill metrics emitted by generic streaming helpers.
pub trait SpillMetricsSink: Send + Sync {
    /// Record the total bytes and file count written by a spill operation.
    fn record_spill(&self, bytes: u64, files: u64);
}

/// Values that can report an estimated in-memory byte footprint.
pub trait ByteSized {
    /// Return the estimated memory footprint in bytes.
    fn estimated_bytes(&self) -> u64;
}

/// Memory accounting policy used by streaming SQL operators.
#[derive(Clone)]
pub struct MemoryPolicy {
    limit_bytes: Option<u64>,
    spill_policy: SpillPolicy,
    metrics: Option<Arc<dyn SpillMetricsSink>>,
}

impl MemoryPolicy {
    /// Create a memory policy with an optional byte limit and spill behavior.
    pub fn new(limit_bytes: Option<u64>, spill_policy: SpillPolicy) -> Self {
        Self {
            limit_bytes,
            spill_policy,
            metrics: None,
        }
    }

    /// Return the configured byte limit, if any.
    pub fn limit_bytes(&self) -> Option<u64> {
        self.limit_bytes
    }

    /// Return the configured spill behavior.
    pub fn spill_policy(&self) -> &SpillPolicy {
        &self.spill_policy
    }

    /// Attach a metrics sink to this policy.
    pub fn with_metrics(mut self, metrics: Arc<dyn SpillMetricsSink>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Return the spill directory when disk spilling is enabled.
    pub fn spill_directory(&self) -> Option<&Path> {
        match &self.spill_policy {
            SpillPolicy::SpillToDisk { directory } => Some(directory.as_path()),
            SpillPolicy::FailFast => None,
        }
    }

    /// Record a spill through the configured metrics sink, when present.
    pub fn record_spill(&self, bytes: u64, files: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.record_spill(bytes, files);
        }
    }

    /// Return whether `used_bytes` exceeds the configured limit.
    pub fn over_limit(&self, used_bytes: u64) -> bool {
        self.limit_bytes
            .map(|limit| used_bytes > limit)
            .unwrap_or(false)
    }

    /// Enforce this memory policy for `used_bytes`.
    pub fn enforce(&self, used_bytes: u64) -> Result<()> {
        let Some(limit) = self.limit_bytes else {
            return Ok(());
        };
        if used_bytes <= limit {
            return Ok(());
        }
        match &self.spill_policy {
            SpillPolicy::FailFast => Err(Error::MemoryLimitExceeded {
                limit: saturating_usize(limit),
                requested: saturating_usize(used_bytes),
            }),
            SpillPolicy::SpillToDisk { .. } => Ok(()),
        }
    }
}

impl std::fmt::Debug for MemoryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPolicy")
            .field("limit_bytes", &self.limit_bytes)
            .field("spill_policy", &self.spill_policy)
            .finish()
    }
}

/// Tracks estimated memory usage for a streaming operator.
#[derive(Clone, Debug)]
pub struct MemoryTracker {
    policy: MemoryPolicy,
    used_bytes: u64,
}

impl MemoryTracker {
    /// Create an empty tracker using `policy`.
    pub fn new(policy: MemoryPolicy) -> Self {
        Self {
            policy,
            used_bytes: 0,
        }
    }

    /// Return the currently tracked byte count.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    /// Return the memory policy used by this tracker.
    pub fn policy(&self) -> &MemoryPolicy {
        &self.policy
    }

    /// Return whether the tracked byte count exceeds the policy limit.
    pub fn over_limit(&self) -> bool {
        self.policy.over_limit(self.used_bytes)
    }

    /// Reset tracked memory to zero after a successful spill or drain.
    pub fn reset(&mut self) {
        self.used_bytes = 0;
    }

    /// Add a raw byte estimate and enforce the policy.
    pub fn add_bytes(&mut self, bytes: u64) -> Result<()> {
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.policy.enforce(self.used_bytes)
    }

    /// Add a row-like slice of byte-sized values.
    pub fn add_row<T: ByteSized>(&mut self, values: &[T]) -> Result<()> {
        self.add_values(values)
    }

    /// Add a slice of byte-sized values.
    pub fn add_values<T: ByteSized>(&mut self, values: &[T]) -> Result<()> {
        let bytes: u64 = values.iter().map(ByteSized::estimated_bytes).sum();
        self.add_bytes(bytes)
    }

    /// Add one byte-sized value.
    pub fn add_value<T: ByteSized>(&mut self, value: &T) -> Result<()> {
        self.add_bytes(value.estimated_bytes())
    }
}

fn saturating_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{ByteSized, MemoryPolicy, MemoryTracker, SpillPolicy};
    use crate::Error;

    #[derive(Clone)]
    struct TestValue(u64);

    impl ByteSized for TestValue {
        fn estimated_bytes(&self) -> u64 {
            self.0
        }
    }

    #[test]
    fn memory_tracker_detects_fail_fast_limit_exceeded() {
        let policy = MemoryPolicy::new(Some(10), SpillPolicy::FailFast);
        let mut tracker = MemoryTracker::new(policy);

        tracker.add_value(&TestValue(6)).unwrap();
        let err = tracker.add_value(&TestValue(5)).unwrap_err();

        assert!(tracker.over_limit());
        assert_eq!(tracker.used_bytes(), 11);
        assert!(matches!(
            err,
            Error::MemoryLimitExceeded {
                limit: 10,
                requested: 11
            }
        ));
    }

    #[test]
    fn spill_to_disk_policy_allows_limit_exceeded_until_caller_spills() {
        let dir = tempfile::tempdir().unwrap();
        let policy = MemoryPolicy::new(
            Some(4),
            SpillPolicy::SpillToDisk {
                directory: dir.path().to_path_buf(),
            },
        );
        let mut tracker = MemoryTracker::new(policy);

        tracker.add_values(&[TestValue(2), TestValue(3)]).unwrap();

        assert!(tracker.over_limit());
        assert_eq!(tracker.used_bytes(), 5);
        tracker.reset();
        assert_eq!(tracker.used_bytes(), 0);
    }
}
