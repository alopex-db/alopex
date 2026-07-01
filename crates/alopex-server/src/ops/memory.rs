use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use alopex_core::sql::stream::DEFAULT_SPILL_THRESHOLD_BYTES;
use alopex_sql::executor::memory::{MemoryPolicy, SpillMetricsSink, SpillPolicy as SqlSpillPolicy};

use crate::error::{Result, ServerError};
use crate::metrics::Metrics;

#[derive(Clone, Debug)]
pub enum SpillPolicy {
    FailFast,
    SpillToDisk { directory: PathBuf },
}

#[derive(Clone)]
pub struct MemoryControlPolicy {
    limit_bytes: Option<u64>,
    spill_policy: SpillPolicy,
    metrics: Option<Metrics>,
}

impl MemoryControlPolicy {
    pub fn from_env() -> Self {
        let limit_bytes = env::var("ALOPEX_MEMORY_LIMIT_BYTES")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SPILL_THRESHOLD_BYTES);

        let policy = env::var("ALOPEX_MEMORY_SPILL_POLICY")
            .unwrap_or_else(|_| "fail_fast".to_string())
            .to_ascii_lowercase();
        let spill_dir = env::var("ALOPEX_MEMORY_SPILL_DIR").ok().map(PathBuf::from);

        let spill_policy = match policy.as_str() {
            "spill" | "spill_to_disk" | "spill-to-disk" => spill_dir
                .map(|directory| SpillPolicy::SpillToDisk { directory })
                .unwrap_or(SpillPolicy::FailFast),
            _ => SpillPolicy::FailFast,
        };

        Self {
            limit_bytes: Some(limit_bytes),
            spill_policy,
            metrics: None,
        }
    }

    pub fn from_env_with_metrics(metrics: Metrics) -> Self {
        Self::from_env().with_metrics(metrics)
    }

    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn limit_bytes(&self) -> Option<u64> {
        self.limit_bytes
    }

    pub fn spill_policy(&self) -> &SpillPolicy {
        &self.spill_policy
    }

    pub fn sql_policy(&self) -> Option<MemoryPolicy> {
        let limit = self.limit_bytes?;
        let spill_policy = match &self.spill_policy {
            SpillPolicy::FailFast => SqlSpillPolicy::FailFast,
            SpillPolicy::SpillToDisk { directory } => SqlSpillPolicy::SpillToDisk {
                directory: directory.clone(),
            },
        };
        let mut policy = MemoryPolicy::new(Some(limit), spill_policy);
        if let Some(metrics) = &self.metrics {
            policy = policy.with_metrics(Arc::new(MetricsSpillSink {
                metrics: metrics.clone(),
            }));
        }
        Some(policy)
    }

    pub fn enforce_output_bytes(&self, bytes: u64) -> Result<()> {
        let Some(limit) = self.limit_bytes else {
            return Ok(());
        };
        if bytes <= limit {
            return Ok(());
        }
        self.enforce_limit(limit, bytes)
    }

    fn enforce_limit(&self, limit: u64, bytes: u64) -> Result<()> {
        match &self.spill_policy {
            SpillPolicy::FailFast => Err(ServerError::PayloadTooLarge(format!(
                "query memory limit exceeded: {bytes} bytes (limit {limit})"
            ))),
            SpillPolicy::SpillToDisk { .. } => Err(ServerError::PayloadTooLarge(format!(
                "query output exceeds memory limit: {bytes} bytes (limit {limit})"
            ))),
        }
    }
}

impl std::fmt::Debug for MemoryControlPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryControlPolicy")
            .field("limit_bytes", &self.limit_bytes)
            .field("spill_policy", &self.spill_policy)
            .finish()
    }
}

struct MetricsSpillSink {
    metrics: Metrics,
}

impl SpillMetricsSink for MetricsSpillSink {
    fn record_spill(&self, bytes: u64, files: u64) {
        self.metrics.record_spill(bytes, files);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::sql::stream::DEFAULT_SPILL_THRESHOLD_BYTES;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        value: Option<String>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvVarGuard {
        fn unset(key: &'static str) -> Self {
            let lock = ENV_LOCK.lock().unwrap();
            let value = env::var(key).ok();
            // SAFETY: This test module serializes all mutations of this env var with ENV_LOCK.
            unsafe {
                env::remove_var(key);
            }
            Self {
                key,
                value,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: The guard still holds ENV_LOCK, so restoration is serialized.
            unsafe {
                if let Some(value) = &self.value {
                    env::set_var(self.key, value);
                } else {
                    env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn from_env_uses_default_spill_threshold_when_limit_is_unset() {
        let _guard = EnvVarGuard::unset("ALOPEX_MEMORY_LIMIT_BYTES");

        let policy = MemoryControlPolicy::from_env();

        assert_eq!(policy.limit_bytes(), Some(DEFAULT_SPILL_THRESHOLD_BYTES));
    }
}
