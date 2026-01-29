use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::executor::{ExecutorError, Result};
use crate::storage::SqlValue;

#[derive(Clone, Debug)]
pub enum SpillPolicy {
    FailFast,
    SpillToDisk { directory: PathBuf },
}

pub trait SpillMetricsSink: Send + Sync {
    fn record_spill(&self, bytes: u64, files: u64);
}

#[derive(Clone)]
pub struct MemoryPolicy {
    limit_bytes: Option<u64>,
    spill_policy: SpillPolicy,
    metrics: Option<Arc<dyn SpillMetricsSink>>,
}

impl MemoryPolicy {
    pub fn new(limit_bytes: Option<u64>, spill_policy: SpillPolicy) -> Self {
        Self {
            limit_bytes,
            spill_policy,
            metrics: None,
        }
    }

    pub fn limit_bytes(&self) -> Option<u64> {
        self.limit_bytes
    }

    pub fn spill_policy(&self) -> &SpillPolicy {
        &self.spill_policy
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn SpillMetricsSink>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn spill_directory(&self) -> Option<&Path> {
        match &self.spill_policy {
            SpillPolicy::SpillToDisk { directory } => Some(directory.as_path()),
            SpillPolicy::FailFast => None,
        }
    }

    pub fn record_spill(&self, bytes: u64, files: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.record_spill(bytes, files);
        }
    }

    pub fn over_limit(&self, used_bytes: u64) -> bool {
        self.limit_bytes
            .map(|limit| used_bytes > limit)
            .unwrap_or(false)
    }

    pub fn enforce(&self, used_bytes: u64) -> Result<()> {
        let Some(limit) = self.limit_bytes else {
            return Ok(());
        };
        if used_bytes <= limit {
            return Ok(());
        }
        match &self.spill_policy {
            SpillPolicy::FailFast => Err(ExecutorError::ResourceExhausted {
                message: format!("query memory limit exceeded: {used_bytes} bytes (limit {limit})"),
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

#[derive(Clone, Debug)]
pub struct MemoryTracker {
    policy: MemoryPolicy,
    used_bytes: u64,
}

impl MemoryTracker {
    pub fn new(policy: MemoryPolicy) -> Self {
        Self {
            policy,
            used_bytes: 0,
        }
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    pub fn policy(&self) -> &MemoryPolicy {
        &self.policy
    }

    pub fn over_limit(&self) -> bool {
        self.policy.over_limit(self.used_bytes)
    }

    pub fn reset(&mut self) {
        self.used_bytes = 0;
    }

    pub fn add_bytes(&mut self, bytes: u64) -> Result<()> {
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.policy.enforce(self.used_bytes)
    }

    pub fn add_row(&mut self, row: &[SqlValue]) -> Result<()> {
        self.add_values(row)
    }

    pub fn add_values(&mut self, values: &[SqlValue]) -> Result<()> {
        let bytes: u64 = values.iter().map(estimate_value_bytes).sum();
        self.add_bytes(bytes)
    }

    pub fn add_value(&mut self, value: &SqlValue) -> Result<()> {
        self.add_bytes(estimate_value_bytes(value))
    }
}

fn estimate_value_bytes(value: &SqlValue) -> u64 {
    match value {
        SqlValue::Null => 0,
        SqlValue::Integer(_) => 4,
        SqlValue::BigInt(_) => 8,
        SqlValue::Float(_) => 4,
        SqlValue::Double(_) => 8,
        SqlValue::Text(text) => text.len() as u64,
        SqlValue::Blob(blob) => blob.len() as u64,
        SqlValue::Boolean(_) => 1,
        SqlValue::Timestamp(_) => 8,
        SqlValue::Vector(values) => values.len() as u64 * 4,
    }
}
