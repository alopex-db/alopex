//! Database configuration options for the embedded API.

use alopex_core::StorageMode;
use std::path::Path;

/// Options for configuring how a database instance is opened.
#[derive(Debug, Clone, Default)]
pub struct DatabaseOptions {
    /// Whether to use in-memory mode.
    memory_mode: bool,
    /// Optional memory limit for in-memory mode (bytes).
    memory_limit: Option<usize>,
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
            memory_limit: None,
        }
    }

    /// Sets an optional memory limit (bytes) and enables in-memory mode.
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_mode = true;
        self.memory_limit = Some(bytes);
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

    /// Convert the options to a core StorageMode.
    pub(crate) fn to_storage_mode(&self, path: Option<&Path>) -> StorageMode {
        if self.memory_mode {
            StorageMode::Memory {
                max_size: self.memory_limit,
            }
        } else {
            let disk_path = path.expect("disk mode requires a path");
            StorageMode::Disk {
                path: disk_path.to_path_buf(),
            }
        }
    }
}
