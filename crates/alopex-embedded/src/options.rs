//! Database configuration options for the embedded API.

use crate::{EmbeddedConfig, SegmentConfigV2, StorageMode};
use std::path::Path;

/// Options for configuring how a database instance is opened.
///
/// Fields are private so new settings can be added without breaking callers.
/// Prefer this builder to a public [`EmbeddedConfig`] struct literal for new code.
#[derive(Debug, Clone, Default)]
pub struct DatabaseOptions {
    /// Whether to use in-memory mode.
    memory_mode: bool,
    /// Optional memory limit for in-memory mode (bytes).
    memory_limit: Option<usize>,
    segment_config: SegmentConfigV2,
    hnsw_cache_bytes: Option<u64>,
    hnsw_cache_entries: Option<usize>,
}

impl DatabaseOptions {
    /// Returns disk-backed options (default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates options configured for in-memory mode without a memory cap.
    pub fn in_memory() -> Self {
        Self {
            memory_mode: true,
            ..Self::default()
        }
    }

    /// Sets an optional memory limit (bytes) and enables in-memory mode.
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_mode = true;
        self.memory_limit = Some(bytes);
        self
    }

    /// Sets the columnar segment configuration.
    pub fn with_segment_config(mut self, config: SegmentConfigV2) -> Self {
        self.segment_config = config;
        self
    }

    /// Sets the HNSW cache byte budget and optional retained graph count.
    ///
    /// Zero bytes disables retention. `None` uses the default count of 128.
    /// `Some(0)` is rejected when opening the database, before creating storage.
    pub fn with_hnsw_cache_limits(mut self, bytes: u64, entries: Option<usize>) -> Self {
        self.hnsw_cache_bytes = Some(bytes);
        self.hnsw_cache_entries = entries;
        self
    }

    /// Returns true when in-memory mode is enabled.
    pub fn memory_mode(&self) -> bool {
        self.memory_mode
    }

    /// Returns the optional memory limit when configured.
    pub fn memory_limit(&self) -> Option<usize> {
        self.memory_limit
    }

    pub(crate) fn into_embedded_config(self, path: Option<&Path>) -> EmbeddedConfig {
        EmbeddedConfig {
            path: path.map(Path::to_path_buf),
            storage_mode: if self.memory_mode {
                StorageMode::InMemory
            } else {
                StorageMode::Disk
            },
            memory_limit: self.memory_limit,
            segment_config: self.segment_config,
            hnsw_cache_bytes: self.hnsw_cache_bytes,
            hnsw_cache_entries: self.hnsw_cache_entries,
        }
    }
}
