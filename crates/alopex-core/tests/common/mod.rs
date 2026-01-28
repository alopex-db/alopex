#![allow(dead_code)]

use alopex_core::error::{Error, Result};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::lsm::wal::{WalSectionHeader, WAL_SECTION_HEADER_SIZE, WAL_SEGMENT_HEADER_SIZE};
use alopex_core::lsm::{LsmKV, LsmKVConfig, RecoveryResult};
use alopex_core::types::TxnMode;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Helper for simulating crash recovery scenarios against a temp data directory.
pub struct CrashSimulator {
    path: PathBuf,
    config: LsmKVConfig,
}

impl CrashSimulator {
    /// Create a simulator rooted at the provided data directory.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let _ = fs::create_dir_all(&path);
        Self {
            path,
            config: LsmKVConfig::default(),
        }
    }

    /// Mutably access the underlying configuration for custom test tuning.
    pub fn config_mut(&mut self) -> &mut LsmKVConfig {
        &mut self.config
    }

    /// Open the store with the current configuration.
    pub fn open_store(&self) -> Result<(LsmKV, RecoveryResult)> {
        LsmKV::open_with_config(&self.path, self.config.clone())
    }

    /// Write N committed entries and then drop the store to simulate a crash.
    pub fn crash_after_writes(&self, writes: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (store, _) = self.open_store()?;
        let mut out = Vec::with_capacity(writes);
        for i in 0..writes {
            let key = format!("key-{i}").into_bytes();
            let value = format!("value-{i}").into_bytes();
            let mut tx = store.begin(TxnMode::ReadWrite)?;
            tx.put(key.clone(), value.clone())?;
            tx.commit_self()?;
            out.push((key, value));
        }
        drop(store);
        Ok(out)
    }

    /// Zero out the last `bytes` of WAL data to simulate an incomplete write.
    pub fn truncate_wal(&self, bytes: u64) -> Result<()> {
        let wal_path = self.wal_path();
        let mut file = OpenOptions::new().read(true).write(true).open(&wal_path)?;
        let mut header_bytes = [0u8; WAL_SECTION_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let section = WalSectionHeader::from_bytes(&header_bytes);

        let max_segments = self.config.wal.max_segments as u64;
        if max_segments == 0 {
            return Err(Error::InvalidFormat("max_segments must be >= 1".into()));
        }
        let segment_size = self.config.wal.segment_size as u64;
        let segment_header = WAL_SEGMENT_HEADER_SIZE as u64;
        if segment_size <= segment_header {
            return Err(Error::InvalidFormat("segment size too small".into()));
        }
        let segment_data_len = segment_size - segment_header;
        let ring_len = segment_data_len
            .checked_mul(max_segments)
            .ok_or_else(|| Error::InvalidFormat("ring length overflow".into()))?;

        let mut remaining = bytes.min(ring_len);
        let mut logical = if section.end_offset >= remaining {
            section.end_offset - remaining
        } else {
            ring_len - (remaining - section.end_offset)
        };

        while remaining > 0 {
            let segment_index = logical / segment_data_len;
            let offset_in_segment = logical % segment_data_len;
            let remaining_in_segment = segment_data_len - offset_in_segment;
            let chunk = remaining.min(remaining_in_segment);
            let phys = (WAL_SECTION_HEADER_SIZE as u64)
                + (segment_index * segment_size)
                + segment_header
                + offset_in_segment;
            file.seek(SeekFrom::Start(phys))?;
            let zeros = vec![0u8; chunk as usize];
            file.write_all(&zeros)?;
            logical = (logical + chunk) % ring_len;
            remaining -= chunk;
        }
        file.sync_data()?;
        Ok(())
    }

    /// Corrupt a single WAL byte at the provided absolute file offset.
    pub fn corrupt_wal_byte(&self, offset: u64) -> Result<()> {
        let wal_path = self.wal_path();
        let mut file = OpenOptions::new().read(true).write(true).open(&wal_path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf)?;
        buf[0] ^= 0xFF;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&buf)?;
        file.sync_data()?;
        Ok(())
    }

    /// Reopen the store and run verification logic against recovery results.
    pub fn recover_and_verify<F>(&self, verifier: F) -> Result<RecoveryResult>
    where
        F: FnOnce(&LsmKV, &RecoveryResult) -> Result<()>,
    {
        let (store, recovery) = self.open_store()?;
        verifier(&store, &recovery)?;
        Ok(recovery)
    }

    fn wal_path(&self) -> PathBuf {
        self.path.join("lsm.wal")
    }
}
