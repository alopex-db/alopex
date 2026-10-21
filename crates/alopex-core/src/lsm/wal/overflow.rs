//! Native WAL v3 references and streaming spool recovery.
//!
//! These are low-level primitives, not activation of overflow in `LsmKV`.
//! The caller must exclusively own the WAL and spool directory, allocate strictly
//! increasing LSNs, and keep recovered state private until recovery succeeds.
//! A database integration must also fence its container before activating v3.
//! Windows publishes synced files using write-through moves in existing durable
//! directories. LSM activation remains gated until its other files and directory
//! creation use a durable publication protocol too.

#[cfg(windows)]
use crate::storage::publication::windows as windows_publication;

use super::spool::{read_verified_spool, write_spool, SpoolDescriptor, SpoolIdentity, SpoolLimits};
use super::*;
use std::fs;
use std::path::PathBuf;

/// Explicit opt-in format. Default v2 readers/writers continue rejecting it.
pub const FORMAT_VERSION: u16 = 3;
/// Fixed physical ring bytes per spool reference, including framing and CRC.
pub const REFERENCE_BYTES: usize = 53;
const REFERENCE_TAG: u8 = 3;

/// Independent per-spool, directory-file and inline-record byte bounds.
#[derive(Clone, Copy, Debug)]
pub struct OverflowLimits {
    spool: SpoolLimits,
    directory_bytes: u64,
    inline_bytes: usize,
}

impl OverflowLimits {
    /// Include every regular file in the owned spool directory, including temps.
    pub fn new(spool: SpoolLimits, directory_bytes: u64, inline_bytes: usize) -> Result<Self> {
        if directory_bytes == 0 || inline_bytes < WAL_ENTRY_FIXED_HEADER + 5 {
            return Err(invalid("invalid overflow directory or inline limit"));
        }
        Ok(Self {
            spool,
            directory_bytes,
            inline_bytes,
        })
    }
}

/// Events delivered to a private recovery sink. Fragment boundaries are physical,
/// and may split keys, values or operations. `End` completes one validated spool.
pub enum RecoveryEvent<'a> {
    /// A regular v2-compatible record contained in the v3 ring.
    Inline(&'a WalEntry),
    /// Start an overflow operation stream at the descriptor's LSN.
    Begin(SpoolDescriptor),
    /// One bounded payload fragment.
    Fragment(&'a [u8]),
    /// The second validation pass completed successfully.
    End,
}

/// Successful recovery, possibly ending at an incomplete ring tail.
#[derive(Debug, PartialEq, Eq)]
pub struct RecoveryOutcome {
    /// Highest validated LSN, including records covered by the checkpoint.
    pub last_lsn: u64,
    /// Number of complete records delivered after the checkpoint.
    pub records: u64,
    /// Incomplete ring tail to truncate before resuming writes, if present.
    pub stopped_at: Option<u64>,
}

fn invalid(message: &str) -> Error {
    Error::InvalidFormat(message.into())
}
fn full(message: &str) -> Error {
    Error::WalCapacityExceeded {
        reason: message.into(),
    }
}

fn reference(descriptor: SpoolDescriptor) -> [u8; REFERENCE_BYTES] {
    let mut bytes = [0; REFERENCE_BYTES];
    bytes[..8].copy_from_slice(&descriptor.identity().lsn().to_le_bytes());
    bytes[8..12]
        .copy_from_slice(&((REFERENCE_BYTES - WAL_ENTRY_FIXED_HEADER) as u32).to_le_bytes());
    bytes[12] = REFERENCE_TAG;
    bytes[13..15].copy_from_slice(&1u16.to_le_bytes());
    bytes[17..33].copy_from_slice(&descriptor.identity().transaction());
    bytes[33..41].copy_from_slice(&descriptor.payload_bytes().to_le_bytes());
    bytes[41..45].copy_from_slice(&descriptor.fragment_bytes().to_le_bytes());
    bytes[45..49].copy_from_slice(&descriptor.checksum().to_le_bytes());
    let crc = crc32fast::hash(&bytes[..49]);
    bytes[49..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

// The enclosing record CRC is checked by the ring scanner before this call.
fn descriptor(bytes: &[u8]) -> Result<SpoolDescriptor> {
    if bytes.len() != REFERENCE_BYTES || bytes[13..17] != [1, 0, 0, 0] {
        return Err(invalid(
            "unsupported overflow reference version, flags or length",
        ));
    }
    SpoolDescriptor::from_parts(
        SpoolIdentity::new(
            bytes[17..33].try_into().unwrap(),
            u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        )?,
        u64::from_le_bytes(bytes[33..41].try_into().unwrap()),
        u32::from_le_bytes(bytes[41..45].try_into().unwrap()),
        u32::from_le_bytes(bytes[45..49].try_into().unwrap()),
    )
}

fn spool_path(directory: &Path, identity: SpoolIdentity) -> PathBuf {
    let transaction: String = identity
        .transaction()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    directory.join(format!("{transaction}-{:016x}.spool", identity.lsn()))
}

pub(crate) fn directory_usage(directory: &Path) -> Result<u64> {
    if !fs::symlink_metadata(directory)?.file_type().is_dir() {
        return Err(invalid(
            "spool directory must be an exclusively owned directory",
        ));
    }
    let mut bytes = 0u64;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(invalid("non-regular file in spool directory"));
        }
        bytes = bytes
            .checked_add(entry.metadata()?.len())
            .ok_or_else(|| full("spool accounting overflow"))?;
    }
    Ok(bytes)
}

/// Validate and calculate a reservation without opening a spool or allocating
/// its payload. Used by the LSM owner before deciding to checkpoint for space.
pub(crate) fn reservation_bytes(
    payload: u64,
    fragment: u32,
    limits: OverflowLimits,
) -> Result<u64> {
    limits.spool.check(payload, fragment)?;
    let bytes = SpoolDescriptor::from_parts(SpoolIdentity::new([1; 16], 1)?, payload, fragment, 0)?
        .file_bytes()?;
    if bytes > limits.directory_bytes {
        return Err(full("single spool exceeds directory budget"));
    }
    Ok(bytes)
}

pub(crate) fn require_directory_sync() -> Result<()> {
    if cfg!(windows) {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "overflow WAL writes require durable directory entries; Windows support is pending",
        )
        .into())
    } else {
        Ok(())
    }
}

// Unlike the legacy best-effort container helper, never ignore missing paths or
// metadata errors: the reference must not outlive an unsynced directory entry.
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    require_directory_sync()?;
    let directory = File::open(path)?;
    if !directory.metadata()?.is_dir() {
        return Err(invalid("overflow sync target is not a directory"));
    }
    directory.sync_all()?;
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent_dir(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    sync_dir(parent)
}

// The source must already be synced and closed. A publication error can leave
// either name installed; callers must not acknowledge a transaction on error.
fn publish_file(temporary: &Path, path: &Path) -> Result<()> {
    crate::storage::publication::replace_file(temporary, path)?;
    #[cfg(not(windows))]
    sync_parent_dir(path)?;
    Ok(())
}

/// Validate that a configured ring can hold a reference before creating storage.
pub(crate) fn validate_wal_config(config: &WalConfig) -> Result<()> {
    let (_, _, _, ring_len) = compute_ring_layout(config)?;
    if ring_len < REFERENCE_BYTES as u64 {
        return Err(full("WAL ring cannot hold an overflow reference"));
    }
    Ok(())
}

/// Detect v3 explicitly without weakening the legacy v1/v2 reader fence.
pub(crate) fn detect_version(path: &Path, config: &WalConfig) -> Result<u16> {
    let mut file = File::open(path)?;
    let (segment_size, _, _, _) = compute_ring_layout(config)?;
    let mut bytes = [0; WAL_SEGMENT_HEADER_SIZE];
    file.seek(SeekFrom::Start(segment_header_offset(segment_size, 0)))?;
    file.read_exact(&mut bytes)?;
    if u16::from_le_bytes([bytes[4], bytes[5]]) == FORMAT_VERSION {
        Ok(WalSegmentHeader::from_bytes_version(&bytes, FORMAT_VERSION)?.version)
    } else {
        detect_wal_format_version(path, config)
    }
}

impl WalWriter {
    /// Create a fresh v3 WAL. The caller must hold exclusive database ownership.
    /// This does not upgrade or fence an existing container/database.
    /// The parent directory must already exist durably. On Windows, an owned
    /// `.overflow-create` file stages one WAL before publication; a stale staging
    /// file blocks creation and must be reconciled by the owner.
    pub fn create_overflow(
        path: &Path,
        config: WalConfig,
        segment_id_base: u64,
        first_lsn: u64,
    ) -> Result<Self> {
        compute_ring_layout(&config)?;
        #[cfg(windows)]
        {
            if path.try_exists()? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "overflow WAL already exists",
                )
                .into());
            }
            let temporary = path.with_extension("overflow-create");
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            let result = (|| {
                let writer = Self::create_version(
                    &temporary,
                    config.clone(),
                    segment_id_base,
                    first_lsn,
                    FORMAT_VERSION,
                )?;
                // Publish the complete file and its metadata, independent of SyncMode.
                writer.file.sync_all()?;
                drop(writer);
                windows_publication::publish(&temporary, path, false)?;
                Self::open_version(path, config, FORMAT_VERSION)
            })();
            if result.is_err() {
                let _ = fs::remove_file(&temporary);
            }
            result
        }
        #[cfg(not(windows))]
        {
            // Refuse accidental truncation of an existing WAL, unlike legacy create.
            OpenOptions::new().create_new(true).write(true).open(path)?;
            let writer =
                Self::create_version(path, config, segment_id_base, first_lsn, FORMAT_VERSION)?;
            sync_parent_dir(path)?;
            Ok(writer)
        }
    }

    /// Open v3 explicitly, after recovery resolved any uncertain append outcome.
    pub fn open_overflow(path: &Path, config: WalConfig) -> Result<Self> {
        Self::open_version(path, config, FORMAT_VERSION)
    }

    /// Prepare a synced spool, then append and force-sync its fixed-size reference.
    ///
    /// The directory must already exist durably and be exclusively owned. Known
    /// budget/ring errors occur before writing the spool. Uncertain reference I/O
    /// retains the spool and poisons this writer: reopen/recover before mutation.
    /// LSNs must increase strictly, as for regular low-level WAL appends.
    pub fn append_spool(
        &mut self,
        input: &mut impl Read,
        directory: &Path,
        identity: SpoolIdentity,
        payload_bytes: u64,
        fragment_bytes: u32,
        limits: OverflowLimits,
    ) -> Result<SpoolDescriptor> {
        self.ensure_healthy()?;
        if self.format_version != FORMAT_VERSION {
            return Err(invalid("overflow reference requires WAL v3"));
        }
        limits.spool.check(payload_bytes, fragment_bytes)?;
        let reserved = SpoolDescriptor::from_parts(identity, payload_bytes, fragment_bytes, 0)?
            .file_bytes()?;
        let used = directory_usage(directory)?;
        if used
            .checked_add(reserved)
            .is_none_or(|total| total > limits.directory_bytes)
        {
            return Err(full("overflow spool directory budget exceeded"));
        }
        if REFERENCE_BYTES as u64 > self.ring_len - self.used_bytes {
            return Err(full("WAL ring lacks room for overflow reference"));
        }
        let sealed = spool_path(directory, identity);
        if sealed.try_exists()? {
            return Err(invalid("spool identity already exists"));
        }
        let temporary = sealed.with_extension("tmp");
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        let prepared = (|| {
            let descriptor = write_spool(
                input,
                &mut output,
                identity,
                payload_bytes,
                fragment_bytes,
                limits.spool,
            )?;
            fault(1)?;
            output.sync_all()?;
            drop(output);
            fault(2)?;
            publish_file(&temporary, &sealed)?;
            fault(3)?;
            Ok(descriptor)
        })();
        let descriptor = match prepared {
            Ok(value) => value,
            Err(error) => {
                // No reference was attempted. Failed cleanup stays on disk and
                // is included in the next reservation; it is never discounted.
                let _ = fs::remove_file(&temporary);
                let _ = fs::remove_file(&sealed);
                #[cfg(not(windows))]
                let _ = sync_dir(directory);
                return Err(error);
            }
        };
        let committed = (|| {
            fault(4)?;
            self.append_encoded(&reference(descriptor), identity.lsn())?;
            fault(5)?;
            self.force_sync()?;
            Ok(())
        })();
        if let Err(error) = committed {
            self.poisoned = true;
            return Err(error);
        }
        Ok(descriptor)
    }
}

/// Atomically rewrite only v2 segment versions to v3, preserving the ring and
/// checkpoint files. No v1 migration or checkpoint-watermark reset occurs here.
///
/// Requires exclusive ownership with all WAL handles closed and the container
/// fence already durable. The caller reserves `max_temporary_bytes` separately
/// from spool disk. A post-rename sync error may leave v3 installed; inspect/reopen
/// rather than retrying writes through an old handle. Existing temp files block
/// activation and must be reconciled by the owning recovery layer.
pub fn activate(path: &Path, config: WalConfig, max_temporary_bytes: u64) -> Result<WalWriter> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid("WAL must be a regular file"));
    }
    // Header/geometry validation is bounded; do not materialize the v2 ring.
    let reader = WalReader::open(path, config.clone())?;
    drop(reader);
    let length = fs::metadata(path)?.len();
    if length > max_temporary_bytes {
        return Err(full("WAL activation temporary exceeds budget"));
    }
    let temporary = path.with_extension("overflow-upgrade");
    let mut output = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&temporary)?;
    let result = (|| {
        let mut input = File::open(path)?;
        std::io::copy(&mut input, &mut output)?;
        drop(input);
        let (segment_size, _, segments, _) = compute_ring_layout(&config)?;
        for index in 0..segments {
            let mut header =
                read_segment_header_version(&mut output, segment_size, index, WAL_VERSION)?;
            header.version = FORMAT_VERSION;
            header.crc32 = header.compute_crc();
            write_segment_header(&mut output, segment_size, index, &header)?;
        }
        fault(6)?;
        output.sync_all()?;
        drop(output);
        publish_file(&temporary, path)?;
        fault(7)?;
        WalWriter::open_overflow(path, config)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

impl WalReader {
    /// Open v3 without enabling the legacy replay API to discard new references.
    pub fn open_overflow(path: &Path, config: WalConfig) -> Result<Self> {
        Self::open_version(path, config, FORMAT_VERSION)
    }

    /// Replay complete records to a private sink with bounded temporary buffers.
    ///
    /// Missing/corrupt/unsupported referenced spools are hard errors, never a
    /// recoverable ring tail. Uncommitted files are ignored, not deleted. Only a
    /// durable checkpoint watermark authorizes skipping references. On any error,
    /// discard all sink state, including callbacks from earlier complete records.
    pub fn recover_overflow(
        &mut self,
        directory: &Path,
        checkpoint_lsn: u64,
        limits: OverflowLimits,
        mut visitor: impl FnMut(RecoveryEvent<'_>) -> Result<()>,
    ) -> Result<RecoveryOutcome> {
        if self.format_version != FORMAT_VERSION {
            return Err(invalid("overflow recovery requires WAL v3"));
        }
        if directory_usage(directory)? > limits.directory_bytes {
            return Err(full("spool directory exceeds recovery budget"));
        }
        let mut outcome = RecoveryOutcome {
            last_lsn: checkpoint_lsn,
            records: 0,
            stopped_at: None,
        };
        let mut previous = None;
        let mut cursor = self.section_header.start_offset;
        let mut remaining = self.used_bytes;
        while remaining > 0 {
            if remaining < (WAL_ENTRY_FIXED_HEADER + 5) as u64 {
                outcome.stopped_at = Some(cursor);
                break;
            }
            let prefix = read_ring_bytes(
                &mut self.file,
                cursor,
                WAL_ENTRY_FIXED_HEADER + 1,
                self.segment_size,
                self.segment_data_len,
                self.ring_len,
            )?;
            let total = u64::from(u32::from_le_bytes(prefix[8..12].try_into().unwrap()))
                + WAL_ENTRY_FIXED_HEADER as u64;
            if total > remaining || total < (WAL_ENTRY_FIXED_HEADER + 5) as u64 {
                outcome.stopped_at = Some(cursor);
                break;
            }
            let is_reference = prefix[12] == REFERENCE_TAG;
            if is_reference && total != REFERENCE_BYTES as u64 {
                return Err(invalid("invalid overflow reference length"));
            }
            if !is_reference && total > limits.inline_bytes as u64 {
                return Err(full("inline WAL record exceeds encoded-byte budget"));
            }
            let bytes = read_ring_bytes(
                &mut self.file,
                cursor,
                total as usize,
                self.segment_size,
                self.segment_data_len,
                self.ring_len,
            )?;
            let end = bytes.len() - 4;
            if crc32fast::hash(&bytes[..end])
                != u32::from_le_bytes(bytes[end..].try_into().unwrap())
            {
                outcome.stopped_at = Some(cursor);
                break;
            }
            let lsn = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            if lsn == 0 || previous.is_some_and(|last| lsn <= last) {
                return Err(invalid("overflow WAL LSNs must strictly increase"));
            }
            previous = Some(lsn);
            outcome.last_lsn = outcome.last_lsn.max(lsn);
            if is_reference {
                let expected = descriptor(&bytes)?;
                if lsn > checkpoint_lsn {
                    let path = spool_path(directory, expected.identity());
                    if !fs::symlink_metadata(&path)?.file_type().is_file() {
                        return Err(invalid("referenced spool is not a regular file"));
                    }
                    let mut file = File::open(path)?;
                    // Begin is delivered only after the entire first pass succeeds.
                    let mut begun = false;
                    read_verified_spool(&mut file, &expected, limits.spool, |fragment| {
                        if !begun {
                            visitor(RecoveryEvent::Begin(expected))?;
                            begun = true;
                        }
                        visitor(RecoveryEvent::Fragment(fragment))
                    })?;
                    visitor(RecoveryEvent::End)?;
                }
            } else {
                let (entry, consumed) = WalEntry::decode(&bytes)?;
                if consumed != bytes.len() {
                    return Err(invalid("trailing bytes in inline WAL record"));
                }
                if lsn > checkpoint_lsn {
                    visitor(RecoveryEvent::Inline(&entry))?;
                }
            }
            if lsn > checkpoint_lsn {
                outcome.records += 1;
            }
            cursor = (cursor + total) % self.ring_len;
            remaining -= total;
        }
        Ok(outcome)
    }
}

#[cfg(test)]
thread_local! { pub(crate) static FAIL_STAGE: Cell<u8> = const { Cell::new(0) }; }
fn fault(_stage: u8) -> Result<()> {
    #[cfg(test)]
    if FAIL_STAGE.get() == _stage {
        return Err(std::io::Error::other("injected overflow persistence failure").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
#[cfg(all(test, windows))]
mod windows_tests;

#[cfg(test)]
mod recovery_portability_tests;
