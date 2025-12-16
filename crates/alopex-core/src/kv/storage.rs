use crate::error::Result;
use crate::kv::any::AnyKV;
use crate::kv::memory::MemoryKV;
use crate::lsm::{LsmKV, LsmKVConfig};
use std::path::PathBuf;

/// Storage mode selection for the KV store.
pub enum StorageMode {
    /// Disk-backed storage using LSM-Tree (file-mode).
    Disk {
        /// データディレクトリ（内部で WAL/SSTable を作成）。
        path: PathBuf,
        /// LSM-Tree 設定（None = デフォルト）。
        config: Option<LsmKVConfig>,
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
    ///
    /// - Disk: LsmKV（file-mode）を使用する。
    /// - Memory: 既存の MemoryKV を使用する。
    pub fn create(mode: StorageMode) -> Result<AnyKV> {
        match mode {
            StorageMode::Disk { path, config } => {
                let kv = match config {
                    Some(cfg) => LsmKV::open_with_config(&path, cfg)?,
                    None => LsmKV::open(&path)?,
                };
                Ok(AnyKV::Lsm(Box::new(kv)))
            }
            StorageMode::Memory { max_size } => {
                Ok(AnyKV::Memory(MemoryKV::new_with_limit(max_size)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_memory_mode_returns_memorykv() {
        let store = StorageFactory::create(StorageMode::Memory { max_size: None }).unwrap();
        assert!(matches!(store, AnyKV::Memory(_)));
    }

    #[test]
    fn create_disk_mode_returns_lsmkv() {
        let dir = tempfile::tempdir().unwrap();
        let store = StorageFactory::create(StorageMode::Disk {
            path: dir.path().to_path_buf(),
            config: None,
        })
        .unwrap();
        assert!(matches!(store, AnyKV::Lsm(_)));
    }
}
