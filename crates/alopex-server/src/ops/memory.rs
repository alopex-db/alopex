use std::env;
use std::path::PathBuf;
use std::sync::Arc;

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
            .and_then(|val| val.parse::<u64>().ok());

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
            limit_bytes,
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
