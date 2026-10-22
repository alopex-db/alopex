//! Explicit LSM integration for bounded WAL overflow.
//!
//! This opt-in preserves default LSM configuration. Collection follows durable
//! checkpoint publication; retained files always count against disk budgets.

mod gc;

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
    /// All files and unreleased retirement reservations in the spool directory.
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
    gc_failures: AtomicU64,
}

/// A snapshot of explicit overflow budgets and current usage. Byte counts are
/// logical file lengths plus unreleased GC reservations / replay accounting,
/// not allocated disk blocks or RSS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LsmOverflowStats {
    /// All regular files and unreleased GC reservations, including temporary
    /// and unknown files.
    pub directory_bytes: u64,
    /// Configured spool directory limit, including retirement reservations.
    pub directory_limit_bytes: u64,
    /// Charged live replay bytes (key/value plus 128 bytes per version).
    pub replay_bytes: usize,
    /// Configured private replay budget.
    pub replay_limit_bytes: usize,
    /// Cleanup attempts that failed since open; checkpoints remain committed.
    pub gc_failures: u64,
}

impl OverflowState {
    pub(super) fn validate_write(
        &self,
        usage: WriteSetUsage,
        key: &[u8],
        value: Option<&[u8]>,
        previous: Option<&Option<Value>>,
    ) -> Result<WriteSetUsage> {
        let value_len = value.map_or(0, <[u8]>::len);
        let single = self
            .config
            .operations
            .check_single_operation(key.len(), value_len)?;
        let old_bytes = previous.map(|old| key.len() + old.as_ref().map_or(0, Vec::len));
        let encoded = usage
            .encoded
            .checked_sub(old_bytes.map_or(0, |n| n as u64 + 17))
            .and_then(|n| n.checked_add(single - 16))
            .ok_or_else(|| full("write-set payload accounting overflow"))?;
        let payload = encoded
            .checked_add(16)
            .ok_or_else(|| full("write-set payload accounting overflow"))?;
        self.config.operations.check_payload(payload)?;
        disk::reservation_bytes(payload, self.config.fragment_bytes, self.config.limits()?)?;
        let replay = usage
            .replay
            .checked_sub(old_bytes.map_or(0, |n| n + 128))
            .and_then(|n| n.checked_add(key.len()))
            .and_then(|n| n.checked_add(value_len))
            .and_then(|n| n.checked_add(128))
            .ok_or_else(|| full("write-set replay accounting overflow"))?;
        if replay > self.config.replay_bytes {
            return Err(full("write-set exceeds replay budget"));
        }
        Ok(WriteSetUsage { encoded, replay })
    }

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
            gc_failures: AtomicU64::new(0),
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

    /// Validate budgets and platform support without filesystem side effects.
    pub fn validate(&self) -> Result<()> {
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
        crate::storage::publication::create_dir_all(&spool)?;
        finish_directory_publication(&spool)?;
        finish_directory_publication(data)?;
        fault(4)?;
        for directory in publication_directories {
            finish_directory_publication(directory)?;
        }
        let path = data.join(container::SIDECAR_WAL_FILE);
        disk::reconcile_activation_staging(&path, config, self.activation_bytes)?;
        if disk::detect_version(&path, config)? == disk::FORMAT_VERSION {
            WalWriter::open_overflow(&path, config.clone())
        } else {
            disk::activate(&path, config.clone(), self.activation_bytes)
        }
    }
}

// Windows creation already published every new directory parent-first through
// write-through moves. Verify the resulting paths; this is not a generic fsync
// replacement. The public overflow guard remains until full API qualification.
fn finish_directory_publication(path: &Path) -> Result<()> {
    #[cfg(windows)]
    if !fs::metadata(path)?.is_dir() {
        return Err(Error::InvalidFormat(
            "overflow publication target is not a directory".into(),
        ));
    }
    #[cfg(not(windows))]
    disk::sync_dir(path)?;
    Ok(())
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
    Error::WalCapacityExceeded {
        reason: message.into(),
    }
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

// Copy + patch + sync + publish; never modify the live header in place.
// A failed fence must not permit WAL activation. Unix retries sync the file and
// parent. Windows retries repeat the bounded copy and write-through publication:
// a read-only handle cannot flush, nor can it complete an uncertain publication.
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
    let temporary = path.with_extension("overflow-fence");
    let mut alternate = header;
    alternate.version = if header.version == FileVersion::WAL_OVERFLOW {
        FileVersion::CURRENT
    } else {
        FileVersion::WAL_OVERFLOW
    };
    let alternate = alternate.to_bytes();
    crate::storage::publication::staging::recover_prefix(
        path,
        &temporary,
        budget,
        |offset, bytes| {
            if offset < alternate.len() as u64 {
                let start = offset as usize;
                let length = bytes.len().min(alternate.len() - start);
                bytes[..length].copy_from_slice(&alternate[start..start + length]);
            }
            Ok(())
        },
    )?;
    #[cfg(not(windows))]
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    #[cfg(not(windows))]
    if header.version == FileVersion::WAL_OVERFLOW {
        File::open(path)?.sync_all()?;
        return disk::sync_dir(parent);
    }
    if fs::metadata(path)?.len() > budget {
        return Err(full("container fence temporary exceeds budget"));
    }
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
        crate::storage::publication::replace_file(&temporary, path)?;
        fault(2)?;
        #[cfg(not(windows))]
        disk::sync_dir(parent)?;
        Ok(())
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
) -> Result<(OpenRecovery, Option<Vec<gc::Reference>>)> {
    let checkpoint = load_checkpoint_meta(&data.join(container::SIDECAR_CHECKPOINT_FILE))?;
    let checkpoint_lsn = checkpoint.as_ref().map(|m| m.checkpoint_lsn);
    let mut reader =
        WalReader::open_overflow(&data.join(container::SIDECAR_WAL_FILE), config.clone())?;
    let mem = MemTable::new();
    let mut references = Vec::new();
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
                    // One valid nonempty write set already charges >=128 bytes.
                    // Bound even foreign empty streams without reserving the full
                    // configured budget up front. At most 24 bytes per identity;
                    // Vec geometric growth remains below the replay budget.
                    if references.len() >= options.replay_bytes / 128 {
                        return Err(full("recovery reference index exceeds budget"));
                    }
                    references.try_reserve(1).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::OutOfMemory,
                            "cannot reserve recovery reference index",
                        )
                    })?;
                    references.push((
                        descriptor.identity().transaction(),
                        descriptor.identity().lsn(),
                    ));
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
    // A torn/corrupt tail leaves uncertain evidence. Repair it durably, but
    // defer orphan deletion until a subsequent complete replay succeeds.
    let references = if outcome.stopped_at.is_none() {
        references.sort_unstable();
        Some(references)
    } else {
        None
    };
    Ok((
        (
            writer,
            mem,
            outcome.last_lsn.saturating_add(1).max(1),
            recovery,
            checkpoint.map_or(0, |m| m.created_at),
        ),
        references,
    ))
}

impl LsmKV {
    pub(super) fn collect_recovered_overflow(&self, references: &[gc::Reference]) {
        if let Some(state) = &self.overflow {
            if let Err(error) =
                gc::collect_recovered(&self.data_dir.join(SPOOL_DIRECTORY), references)
            {
                state.gc_failures.fetch_add(1, Ordering::Relaxed);
                warn!(%error, "Recovery cleanup failed; retained files remain charged");
            }
        }
    }

    /// Return usage for an explicitly enabled overflow store, or `None` for v2.
    /// A failed inventory is returned as an error, never reported as zero usage.
    pub fn overflow_stats(&self) -> Result<Option<LsmOverflowStats>> {
        let _guard = self.commit_lock.lock().expect("lsm commit_lock poisoned");
        self.overflow
            .as_ref()
            .map(|state| {
                Ok(LsmOverflowStats {
                    directory_bytes: disk::directory_usage(&self.data_dir.join(SPOOL_DIRECTORY))?,
                    directory_limit_bytes: state.config.directory_bytes,
                    replay_bytes: state.charged.load(Ordering::Relaxed),
                    replay_limit_bytes: state.config.replay_bytes,
                    gc_failures: state.gc_failures.load(Ordering::Relaxed),
                })
            })
            .transpose()
    }

    // Cleanup failure must not turn an already durable checkpoint into a failed
    // transaction. Retained files remain in directory_usage and block new
    // reservations if necessary. The next checkpoint retries collection.
    pub(super) fn collect_overflow(&self, checkpoint: u64) {
        if let Some(state) = &self.overflow {
            if let Err(error) = gc::collect(&self.data_dir.join(SPOOL_DIRECTORY), checkpoint) {
                state.gc_failures.fetch_add(1, Ordering::Relaxed);
                warn!(%error, checkpoint, "Overflow cleanup failed; retained files remain charged");
            }
        }
    }

    pub(super) fn append_overflow_write_set(
        &self,
        writes: &BTreeMap<Key, Option<Value>>,
        state: &OverflowState,
    ) -> Result<u64> {
        let options = &state.config;
        // Every opt-in commit uses one reference; avoid an extra whole-batch Vec.
        let mut stream = OperationStream::new(writes, options.operations)?;
        let payload_bytes = stream.payload_bytes();
        let reserved =
            disk::reservation_bytes(payload_bytes, options.fragment_bytes, options.limits()?)?;
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
        let used = disk::directory_usage(&self.data_dir.join(SPOOL_DIRECTORY))?;
        if state.charged.load(Ordering::Relaxed) > options.replay_bytes - charged
            || used > options.directory_bytes - reserved
        {
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

#[cfg(test)]
mod fence_tests;

#[cfg(test)]
mod lifecycle_tests;

#[cfg(all(test, not(windows)))]
mod publication_acceptance;
