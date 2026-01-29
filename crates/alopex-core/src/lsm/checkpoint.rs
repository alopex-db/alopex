//! Checkpoint metadata persistence for WAL recovery.

use std::convert::TryInto;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::error::{Error, Result};

/// Checkpoint metadata format version.
pub const CHECKPOINT_META_VERSION: u32 = 1;
const CHECKPOINT_META_SIZE: usize = 24;

/// Checkpoint metadata persisted to disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMeta {
    /// Metadata version.
    pub version: u32,
    /// Checkpoint LSN.
    pub checkpoint_lsn: u64,
    /// Creation timestamp (epoch ms).
    pub created_at: u64,
    /// CRC32 over version/checkpoint_lsn/created_at.
    pub crc: u32,
}

impl CheckpointMeta {
    /// Create a new checkpoint metadata payload with computed CRC.
    pub fn new(checkpoint_lsn: u64, created_at: u64) -> Self {
        let mut meta = Self {
            version: CHECKPOINT_META_VERSION,
            checkpoint_lsn,
            created_at,
            crc: 0,
        };
        meta.crc = meta.compute_crc();
        meta
    }

    /// Serialize metadata into fixed-size little-endian bytes.
    pub fn to_bytes(&self) -> [u8; CHECKPOINT_META_SIZE] {
        let mut buf = [0u8; CHECKPOINT_META_SIZE];
        buf[0..4].copy_from_slice(&self.version.to_le_bytes());
        buf[4..12].copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf[12..20].copy_from_slice(&self.created_at.to_le_bytes());
        buf[20..24].copy_from_slice(&self.crc.to_le_bytes());
        buf
    }

    /// Deserialize metadata and validate CRC.
    pub fn from_bytes(bytes: &[u8; CHECKPOINT_META_SIZE]) -> Result<Self> {
        let version = u32::from_le_bytes(bytes[0..4].try_into().expect("fixed slice length"));
        if version != CHECKPOINT_META_VERSION {
            return Err(Error::InvalidFormat(format!(
                "unsupported checkpoint meta version: {version}"
            )));
        }
        let checkpoint_lsn =
            u64::from_le_bytes(bytes[4..12].try_into().expect("fixed slice length"));
        let created_at = u64::from_le_bytes(bytes[12..20].try_into().expect("fixed slice length"));
        let crc = u32::from_le_bytes(bytes[20..24].try_into().expect("fixed slice length"));
        let meta = Self {
            version,
            checkpoint_lsn,
            created_at,
            crc,
        };
        let computed = meta.compute_crc();
        if computed != crc {
            return Err(Error::ChecksumMismatch);
        }
        Ok(meta)
    }

    fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; CHECKPOINT_META_SIZE - 4];
        buf[0..4].copy_from_slice(&self.version.to_le_bytes());
        buf[4..12].copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf[12..20].copy_from_slice(&self.created_at.to_le_bytes());
        crc32fast::hash(&buf)
    }
}

/// Persist checkpoint metadata atomically to disk.
pub fn save_checkpoint_meta(path: &Path, meta: &CheckpointMeta) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    file.write_all(&meta.to_bytes())?;
    file.sync_data()?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Load checkpoint metadata, returning None when missing or corrupted.
pub fn load_checkpoint_meta(path: &Path) -> Result<Option<CheckpointMeta>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut buf = [0u8; CHECKPOINT_META_SIZE];
    if let Err(err) = file.read_exact(&mut buf) {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(err.into());
    }
    match CheckpointMeta::from_bytes(&buf) {
        Ok(meta) => Ok(Some(meta)),
        Err(Error::ChecksumMismatch) => Ok(None),
        Err(Error::InvalidFormat(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::kv::KVStore;
    use crate::kv::KVTransaction;
    use crate::lsm::wal::{SyncMode, WalConfig};
    use crate::lsm::{LsmKV, LsmKVConfig};
    use crate::types::TxnMode;
    use std::sync::atomic::Ordering;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    #[test]
    fn checkpoint_meta_roundtrip() {
        let meta = CheckpointMeta::new(42, 123);
        let bytes = meta.to_bytes();
        let decoded = CheckpointMeta::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn checkpoint_meta_crc_detects_corruption() {
        let meta = CheckpointMeta::new(7, 9);
        let mut bytes = meta.to_bytes();
        bytes[23] ^= 0xFF;
        let err = CheckpointMeta::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch));
    }

    #[test]
    fn checkpoint_persists_metadata() {
        let dir = tempdir().unwrap();
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), cfg).unwrap();
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.commit_self().unwrap();

        let result = store.checkpoint().unwrap();
        let meta_path = dir.path().join("checkpoint.meta");
        let meta = load_checkpoint_meta(&meta_path).unwrap().unwrap();
        assert_eq!(meta.checkpoint_lsn, result.checkpoint_lsn);
    }

    #[test]
    fn should_checkpoint_respects_threshold_and_interval() {
        let dir = tempdir().unwrap();
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            checkpoint: crate::lsm::CheckpointConfig {
                wal_size_threshold: 1,
                min_interval_ms: 60_000,
                auto_checkpoint: true,
            },
            ..Default::default()
        };
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), cfg).unwrap();

        store.wal_used_bytes.store(10, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        store.last_checkpoint_ms.store(now_ms, Ordering::Relaxed);
        assert!(!store.should_checkpoint());

        store
            .last_checkpoint_ms
            .store(now_ms.saturating_sub(120_000), Ordering::Relaxed);
        assert!(store.should_checkpoint());
    }
}
