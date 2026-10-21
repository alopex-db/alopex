//! Explicit LSM integration for bounded WAL overflow.
//!
//! This opt-in does not change default LSM configuration or enable Python/SQL
//! configuration. Spools remain charged until sidecar pruning; checkpoint-aware
//! garbage collection is a separate step.

use super::*;
use crate::storage::format::{
    AlopexFileReader, FileHeader, FileReader, FileSource, FileVersion, HEADER_SIZE,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use wal::operations::{OperationDecoder, OperationLimits, OperationStream};
use wal::overflow::{self as disk, OverflowLimits, RecoveryEvent, REFERENCE_BYTES};
use wal::spool::{SpoolIdentity, SpoolLimits};

pub(crate) const SPOOL_DIRECTORY: &str = "overflow";

/// Independent budgets for the opt-in LSM overflow path. These are not process
/// RSS limits: replay accounting charges key/value bytes plus 128 bytes per op;
/// allocator overhead and the caller's original write set are separate.
#[derive(Clone, Copy, Debug)]
pub struct LsmOverflowConfig {
    /// Per-spool encoded bytes and fragment bounds.
    pub(super) spool: SpoolLimits,
    /// Operation payload and individual key/value bounds.
    pub(super) operations: OperationLimits,
    /// All files in the owned spool directory, including abandoned temps.
    pub(super) directory_bytes: u64,
    /// Payload bytes per physical fragment.
    pub(super) fragment_bytes: u32,
    /// Maximum encoded inline record accepted during migration/recovery.
    pub(super) inline_bytes: usize,
    /// Temporary bytes for the atomic WAL v2-to-v3 copy.
    pub(super) activation_bytes: u64,
    /// Temporary bytes for the atomic container header upgrade.
    pub(super) container_fence_bytes: u64,
    /// Maximum charged bytes retained in the private recovery memtable.
    pub(super) replay_bytes: usize,
}

#[derive(Debug)]
pub(super) struct OverflowState {
    config: LsmOverflowConfig,
    pub(super) charged: std::sync::atomic::AtomicUsize,
}

impl OverflowState {
    pub(super) fn new(config: LsmOverflowConfig, recovered: &MemTable) -> Result<Self> {
        let charged = recovered
            .version_count()
            .checked_mul(112)
            .and_then(|extra| recovered.memory_usage_bytes().checked_add(extra))
            .ok_or_else(|| full("replay accounting overflow"))?;
        if charged > config.replay_bytes {
            return Err(full("private WAL recovery exceeds replay budget"));
        }
        Ok(Self {
            config,
            charged: std::sync::atomic::AtomicUsize::new(charged),
        })
    }
}

impl LsmOverflowConfig {
    /// Set transaction, spool-directory and private replay budgets explicitly.
    /// Defaults reserve at most 16 MiB per legacy inline record, 1 GiB for WAL
    /// activation and 64 MiB for a container fence copy. Builders can tighten or
    /// raise those independent bounds; open validates the resulting options.
    pub fn new(
        spool: SpoolLimits,
        operations: OperationLimits,
        directory_bytes: u64,
        fragment_bytes: u32,
        replay_bytes: usize,
    ) -> Self {
        Self {
            spool,
            operations,
            directory_bytes,
            fragment_bytes,
            replay_bytes,
            inline_bytes: 16 * 1024 * 1024,
            activation_bytes: 1024 * 1024 * 1024,
            container_fence_bytes: 64 * 1024 * 1024,
        }
    }

    /// Bound a legacy inline WAL record during activation and later recovery.
    pub fn with_inline_bytes(mut self, bytes: usize) -> Self {
        self.inline_bytes = bytes;
        self
    }
    /// Bound the temporary full-file WAL upgrade copy.
    pub fn with_activation_bytes(mut self, bytes: u64) -> Self {
        self.activation_bytes = bytes;
        self
    }
    /// Bound the temporary container copy used to publish the reader fence.
    pub fn with_container_fence_bytes(mut self, bytes: u64) -> Self {
        self.container_fence_bytes = bytes;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        disk::require_directory_sync()?;
        self.limits()?;
        if self.fragment_bytes == 0
            || self.fragment_bytes > wal::spool::MAX_FRAGMENT_BYTES
            || self.replay_bytes == 0
            || self.activation_bytes == 0
        {
            return Err(Error::InvalidFormat("invalid LSM overflow budgets".into()));
        }
        Ok(())
    }

    fn limits(&self) -> Result<OverflowLimits> {
        OverflowLimits::new(self.spool, self.directory_bytes, self.inline_bytes)
    }

    pub(crate) fn activate(
        &self,
        data: &Path,
        container: Option<&Path>,
        config: &WalConfig,
        last_lsn: u64,
        publication_directories: &[PathBuf],
    ) -> Result<WalWriter> {
        if let Some(path) = container {
            fence_container(path, data, last_lsn, self.container_fence_bytes)?;
        }
        fault(3)?;
        let spool = data.join(SPOOL_DIRECTORY);
        fs::create_dir_all(&spool)?;
        disk::sync_dir(&spool)?;
        disk::sync_dir(data)?;
        fault(4)?;
        for directory in publication_directories {
            disk::sync_dir(directory)?;
        }
        let path = data.join(container::SIDECAR_WAL_FILE);
        if disk::detect_version(&path, config)? == disk::FORMAT_VERSION {
            WalWriter::open_overflow(&path, config.clone())
        } else {
            disk::activate(&path, config.clone(), self.activation_bytes)
        }
    }
}

// Capture the first existing ancestor before the owner lock/rehydrate creates
// directories. Sync each newly created directory entry up to that ancestor,
// child first, before a v3 writer can acknowledge a commit.
pub(crate) fn publication_directories(
    data: &Path,
    container: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    let mut directories = Vec::new();
    for path in Some(data).into_iter().chain(container) {
        for parent in path.ancestors().skip(1) {
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            directories.push(parent.to_path_buf());
            match fs::metadata(parent) {
                Ok(metadata) if metadata.is_dir() => break,
                Ok(_) => {
                    return Err(Error::InvalidFormat(
                        "overflow publication ancestor is not a directory".into(),
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(directories)
}

fn full(message: &str) -> Error {
    std::io::Error::new(std::io::ErrorKind::StorageFull, message).into()
}

pub(crate) fn check_container_access(path: Option<&Path>, enabled: bool) -> Result<()> {
    if let Some(path) = path {
        // Preserve live-sidecar precedence: an unrelated stale/broken v2
        // container must not become a new open requirement. Restore (if needed)
        // and activation still perform full validation before trusting a file.
        let mut bytes = [0; HEADER_SIZE];
        if File::open(path)
            .and_then(|mut file| file.read_exact(&mut bytes))
            .is_ok()
        {
            if let Ok(header) = FileHeader::from_bytes(&bytes) {
                if header.version == FileVersion::WAL_OVERFLOW && !enabled {
                    return Err(Error::InvalidFormat(
                        "overflow container requires explicit LSM overflow budgets".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

// Copy + patch + fsync + rename + parent fsync; never modify the live header in
// place. A failed fence must not permit WAL activation. A retry after rename
// syncs the file and parent again before considering the fence durable.
fn fence_container(path: &Path, data: &Path, last_lsn: u64, budget: u64) -> Result<()> {
    let absent = match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(Error::InvalidFormat(
                "container fence requires a regular file".into(),
            ))
        }
        Ok(metadata) => metadata.len() == 0,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => return Err(error.into()),
    };
    if absent {
        if last_lsn != 0
            || fs::read_dir(data.join(container::SIDECAR_SST_DIR))?
                .next()
                .is_some()
        {
            return Err(Error::InvalidFormat(
                "converge the legacy database before enabling overflow".into(),
            ));
        }
        if budget < 4096 {
            return Err(full(
                "empty container fence requires a 4096-byte reservation",
            ));
        }
        container::write_container(path, &data.join(container::SIDECAR_SST_DIR), &[], 0)?;
    }
    let reader = AlopexFileReader::open(FileSource::Path(path.to_path_buf()))?;
    let mut header = *reader.header();
    drop(reader);
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if header.version == FileVersion::WAL_OVERFLOW {
        File::open(path)?.sync_all()?;
        return disk::sync_dir(parent);
    }
    if fs::metadata(path)?.len() > budget {
        return Err(full("container fence temporary exceeds budget"));
    }
    let temporary = path.with_extension("overflow-fence");
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let result = (|| {
        std::io::copy(&mut File::open(path)?, &mut output)?;
        header.version = FileVersion::WAL_OVERFLOW;
        output.seek(SeekFrom::Start(0))?;
        output.write_all(&header.to_bytes())?;
        output.sync_all()?;
        drop(output);
        fault(1)?;
        fs::rename(&temporary, path)?;
        fault(2)?;
        disk::sync_dir(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

type OpenRecovery = (WalWriter, MemTable, u64, RecoveryResult, u64);

pub(crate) fn recover(
    data: &Path,
    config: &WalConfig,
    options: &LsmOverflowConfig,
) -> Result<OpenRecovery> {
    let checkpoint = load_checkpoint_meta(&data.join(container::SIDECAR_CHECKPOINT_FILE))?;
    let checkpoint_lsn = checkpoint.as_ref().map(|m| m.checkpoint_lsn);
    let mut reader =
        WalReader::open_overflow(&data.join(container::SIDECAR_WAL_FILE), config.clone())?;
    let mem = MemTable::new();
    let mut decoder = None;
    let mut lsn = 0;
    let mut sequence = 0;
    let mut charged = 0usize;
    let mut apply = |op: WalBatchOp, lsn: u64, sequence: u64| -> Result<()> {
        charged = charged
            .checked_add(op.key.len())
            .and_then(|n| n.checked_add(op.value.as_ref().map_or(0, Vec::len)))
            .and_then(|n| n.checked_add(128))
            .ok_or_else(|| full("replay accounting overflow"))?;
        if charged > options.replay_bytes {
            return Err(full("private WAL recovery exceeds replay budget"));
        }
        match op.op_type {
            WalOpType::Put => mem.put(op.key, op.value.unwrap_or_default(), lsn, sequence),
            WalOpType::Delete => mem.delete(op.key, lsn, sequence),
        }
        Ok(())
    };
    let outcome = reader.recover_overflow(
        &data.join(SPOOL_DIRECTORY),
        checkpoint_lsn.unwrap_or(0),
        options.limits()?,
        |event| {
            match event {
                RecoveryEvent::Begin(descriptor) => {
                    lsn = descriptor.identity().lsn();
                    sequence = 1;
                    decoder = Some(OperationDecoder::new(
                        descriptor.payload_bytes(),
                        options.operations,
                    )?);
                }
                RecoveryEvent::Fragment(bytes) => decoder
                    .as_mut()
                    .ok_or_else(|| Error::InvalidFormat("fragment without begin".into()))?
                    .push(bytes, |op| {
                        apply(op, lsn, sequence)?;
                        sequence += 1;
                        Ok(())
                    })?,
                RecoveryEvent::End => decoder
                    .take()
                    .ok_or_else(|| Error::InvalidFormat("end without begin".into()))?
                    .finish()?,
                RecoveryEvent::Inline(entry) => match &entry.payload {
                    WalEntryPayload::Put { key, value } => apply(
                        WalBatchOp {
                            op_type: WalOpType::Put,
                            key: key.clone(),
                            value: Some(value.clone()),
                        },
                        entry.lsn,
                        0,
                    )?,
                    WalEntryPayload::Delete { key } => apply(
                        WalBatchOp {
                            op_type: WalOpType::Delete,
                            key: key.clone(),
                            value: None,
                        },
                        entry.lsn,
                        0,
                    )?,
                    WalEntryPayload::Batch(ops) => {
                        for (index, op) in ops.iter().enumerate() {
                            apply(op.clone(), entry.lsn, index as u64)?;
                        }
                    }
                },
            }
            Ok(())
        },
    )?;
    drop(reader);
    let mut writer =
        WalWriter::open_overflow(&data.join(container::SIDECAR_WAL_FILE), config.clone())?;
    if let Some(end) = outcome.stopped_at {
        writer.truncate_tail_to(end)?;
    }
    let recovery = RecoveryResult {
        entries_recovered: usize::try_from(outcome.records)
            .map_err(|_| full("recovery record count overflow"))?,
        last_lsn: outcome.last_lsn,
        warnings: Vec::new(),
        stop_reason: outcome
            .stopped_at
            .map(|_| "incomplete overflow WAL tail".into()),
        checkpoint_lsn,
    };
    Ok((
        writer,
        mem,
        outcome.last_lsn.saturating_add(1).max(1),
        recovery,
        checkpoint.map_or(0, |m| m.created_at),
    ))
}

impl LsmKV {
    pub(super) fn append_overflow_write_set(
        &self,
        writes: &BTreeMap<Key, Option<Value>>,
        state: &OverflowState,
    ) -> Result<u64> {
        let options = &state.config;
        // Every opt-in commit uses one reference; avoid an extra whole-batch Vec.
        let mut stream = OperationStream::new(writes, options.operations)?;
        let payload_bytes = stream.payload_bytes();
        let charged = writes.iter().try_fold(0usize, |total, (key, value)| {
            total
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.as_ref().map_or(0, Vec::len)))
                .and_then(|n| n.checked_add(128))
                .ok_or_else(|| full("write replay accounting overflow"))
        })?;
        if charged > options.replay_bytes {
            return Err(full("commit exceeds replay budget"));
        }
        if state.charged.load(Ordering::Relaxed) > options.replay_bytes - charged {
            self.checkpoint_locked()?;
        }
        self.prepare_wal_capacity(REFERENCE_BYTES as u64)?;
        let lsn = self.ts_oracle.next_timestamp();
        // The database lock and strictly increasing LSN provide local uniqueness.
        let mut transaction = [0; 16];
        transaction[..8].copy_from_slice(b"ALPXLSM1");
        transaction[8..].copy_from_slice(&lsn.to_le_bytes());
        let mut wal = self.wal.write().expect("lsm wal lock poisoned");
        wal.append_spool(
            &mut stream,
            &self.data_dir.join(SPOOL_DIRECTORY),
            SpoolIdentity::new(transaction, lsn)?,
            payload_bytes,
            options.fragment_bytes,
            options.limits()?,
        )?;
        state.charged.fetch_add(charged, Ordering::Relaxed);
        self.metrics.add_wal_write_bytes(REFERENCE_BYTES as u64);
        self.wal_used_bytes
            .store(wal.used_bytes(), Ordering::Relaxed);
        Ok(lsn)
    }
}

#[cfg(test)]
thread_local! { static FAIL_STAGE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) }; }
fn fault(_stage: u8) -> Result<()> {
    #[cfg(test)]
    if FAIL_STAGE.get() == _stage {
        return Err(std::io::Error::other("injected container fence failure").into());
    }
    Ok(())
}

#[cfg(all(test, not(windows)))]
mod tests;
