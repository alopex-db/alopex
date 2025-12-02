use crate::error::Result;
use crate::kv::memory::MemoryKV;
use std::path::PathBuf;

/// Storage mode selection for the KV store.
pub enum StorageMode {
    /// Disk-backed storage using WAL.
    Disk {
        /// Path to the WAL file (SSTable is derived from this path).
        path: PathBuf,
    },
    /// Pure in-memory storage with optional memory cap.
    Memory {
        /// Optional memory cap in bytes; None means unlimited.
        max_size: Option<usize>,
    },
}

/// Factory for creating storage instances based on mode.
pub struct StorageFactory;

impl StorageFactory {
    /// Create a KV store according to the requested mode.
    /// - Disk: uses MemoryKV::open(path) to honor WAL/SSTable behavior.
    /// - Memory: uses MemoryKV::new_with_limit and does not initialize WAL.
    pub fn create(mode: StorageMode) -> Result<MemoryKV> {
        match mode {
            StorageMode::Disk { path } => Ok(MemoryKV::open(&path)?),
            StorageMode::Memory { max_size } => Ok(MemoryKV::new_with_limit(max_size)),
        }
    }
}
