//! Bounded staging of evaluated DML changes before any statement writes.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::{ControlFlow, Range};
use std::path::PathBuf;

use alopex_core::sql::spill::{create_spill_file, ensure_spill_dir, spill_io_error};
use bincode::Options;
use serde::{Serialize, de::DeserializeOwned};

use crate::executor::memory::{
    DEFAULT_SPILL_THRESHOLD_BYTES, MemoryPolicy, MemoryTracker, SpillPolicy, map_core_memory_error,
};
use crate::executor::{ExecutorError, Result};

/// Keeps the ordinary batched DML path separate from statement-wide evaluation.
/// Memory is limited by the existing SQL memory policy; excess records are
/// written to one invocation-owned file. Successful replay reports cleanup
/// failures; error paths attempt cleanup without replacing the original error.
pub(crate) struct StatementSpool<T> {
    memory: MemoryTracker,
    records: Vec<Vec<u8>>,
    disk: Option<SpoolFile>,
    count: u64,
    max_record_bytes: usize,
    record: PhantomData<T>,
}

struct SpoolFile {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    reported_bytes: u64,
}

impl Drop for SpoolFile {
    fn drop(&mut self) {
        // Close before unlinking, including on platforms that disallow unlink
        // of an open writer. No incomplete spool is ever reused.
        drop(self.writer.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

impl<T: Serialize + DeserializeOwned> StatementSpool<T> {
    pub(crate) fn new(policy: Option<&MemoryPolicy>) -> Self {
        Self::with_policy(policy.cloned().unwrap_or_else(|| {
            MemoryPolicy::new(
                Some(DEFAULT_SPILL_THRESHOLD_BYTES),
                SpillPolicy::SpillToDisk {
                    directory: std::env::temp_dir(),
                },
            )
        }))
    }

    fn with_policy(policy: MemoryPolicy) -> Self {
        Self {
            memory: MemoryTracker::new(policy),
            records: Vec::new(),
            disk: None,
            count: 0,
            max_record_bytes: 0,
            record: PhantomData,
        }
    }

    pub(crate) fn push(&mut self, record: &T) -> Result<()> {
        let bytes = bincode::serialize(record).map_err(codec_error)?;
        let record_bytes = bytes.len();
        if let Some(disk) = &mut self.disk {
            write_record(disk.writer.as_mut().expect("spool is writable"), &bytes)?;
        } else {
            self.memory
                .add_bytes(bytes.len() as u64 + std::mem::size_of::<Vec<u8>>() as u64)
                .map_err(map_core_memory_error)?;
            // Even a caller with no explicit limit gets the standard SQL
            // spill threshold, rather than an unbounded staging Vec.
            if self.memory.used_bytes()
                > self
                    .memory
                    .policy()
                    .limit_bytes()
                    .unwrap_or(DEFAULT_SPILL_THRESHOLD_BYTES)
            {
                self.start_spill()?;
                write_record(self.disk.as_mut().unwrap().writer.as_mut().unwrap(), &bytes)?;
            } else {
                self.records.push(bytes);
            }
        }
        self.max_record_bytes = self.max_record_bytes.max(record_bytes);
        self.count += 1;
        Ok(())
    }

    fn start_spill(&mut self) -> Result<()> {
        let directory = self.memory.policy().spill_directory().ok_or_else(|| {
            ExecutorError::ResourceExhausted {
                message: "DML statement staging exceeded its memory limit".into(),
            }
        })?;
        ensure_spill_dir(directory)?;
        let (path, file) = create_spill_file(directory, "alopex-dml-statement")?;
        self.disk = Some(SpoolFile {
            path,
            writer: Some(BufWriter::new(file)),
            reported_bytes: 0,
        });
        let writer = self.disk.as_mut().unwrap().writer.as_mut().unwrap();
        // The shared allocator uses create_new. Restrict permissions before
        // writing SQL values when the default directory is a shared temp dir.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            writer
                .get_ref()
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(io_error)?;
        }
        for bytes in self.records.drain(..) {
            write_record(writer, &bytes)?;
        }
        self.records.shrink_to_fit();
        self.memory.reset();
        Ok(())
    }

    pub(crate) fn len(&self) -> u64 {
        self.count
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(disk) = &mut self.disk {
            let writer = disk.writer.as_mut().expect("spool is writable");
            writer.flush().map_err(io_error)?;
            let bytes_written = writer.get_ref().metadata().map_err(io_error)?.len();
            if bytes_written > disk.reported_bytes {
                self.memory.policy().record_spill(
                    bytes_written - disk.reported_bytes,
                    u64::from(disk.reported_bytes == 0),
                );
                disk.reported_bytes = bytes_written;
            }
        }
        Ok(())
    }

    /// Read a recorded result without materializing a spilled result again.
    /// The owner may append other results after this call returns.
    pub(crate) fn visit_range(
        &mut self,
        range: Range<u64>,
        mut visit: impl FnMut(T) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        if range.start > range.end || range.end > self.count {
            return Err(codec_error("invalid DML staging record range"));
        }
        self.flush()?;
        if let Some(disk) = &self.disk {
            let mut reader = BufReader::new(File::open(&disk.path).map_err(io_error)?);
            let file_length = reader.get_ref().metadata().map_err(io_error)?.len();
            for index in 0..range.end {
                let mut length = [0u8; 8];
                reader.read_exact(&mut length).map_err(io_error)?;
                let length = usize::try_from(u64::from_le_bytes(length)).map_err(codec_error)?;
                // Never trust a file length to allocate more than the largest
                // record this invocation actually wrote. Truncation is an
                // error, not successful EOF.
                if length > self.max_record_bytes {
                    return Err(codec_error("DML staging record exceeds written maximum"));
                }
                if index < range.start {
                    let end = reader
                        .stream_position()
                        .map_err(io_error)?
                        .checked_add(length as u64)
                        .ok_or_else(|| codec_error("DML staging offset overflow"))?;
                    if end > file_length {
                        return Err(codec_error("truncated DML staging record"));
                    }
                    reader.seek(SeekFrom::Start(end)).map_err(io_error)?;
                    continue;
                }
                let mut bytes = vec![0u8; length];
                reader.read_exact(&mut bytes).map_err(io_error)?;
                if visit(decode_record(&bytes)?)?.is_break() {
                    return Ok(());
                }
            }
            let mut trailing = [0u8; 1];
            if range.end == self.count && reader.read(&mut trailing).map_err(io_error)? != 0 {
                return Err(codec_error("trailing DML staging bytes"));
            }
        } else {
            for bytes in &self.records[range.start as usize..range.end as usize] {
                if visit(decode_record(bytes)?)?.is_break() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Surface successful-path cleanup failures; Drop handles earlier errors.
    pub(crate) fn finish(mut self) -> Result<()> {
        self.flush()?;
        if let Some(mut disk) = self.disk.take() {
            drop(disk.writer.take());
            std::fs::remove_file(&disk.path).map_err(io_error)?;
        }
        Ok(())
    }

    pub(crate) fn replay(mut self, mut apply: impl FnMut(T) -> Result<()>) -> Result<()> {
        self.visit_range(0..self.count, |record| {
            apply(record)?;
            Ok(ControlFlow::Continue(()))
        })?;
        self.finish()
    }
}

fn write_record(writer: &mut BufWriter<File>, bytes: &[u8]) -> Result<()> {
    writer
        .write_all(&(bytes.len() as u64).to_le_bytes())
        .and_then(|()| writer.write_all(bytes))
        .map_err(io_error)
}

fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(bytes.len() as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)
        .map_err(codec_error)
}

fn io_error(error: std::io::Error) -> ExecutorError {
    spill_io_error("DML statement spool", error).into()
}

fn codec_error(error: impl std::fmt::Display) -> ExecutorError {
    ExecutorError::InvalidOperation {
        operation: "DML statement spool".into(),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_replays_in_order_and_removes_owned_file() {
        let directory = tempfile::tempdir().unwrap();
        let mut spool = StatementSpool::<(u64, String)>::with_policy(MemoryPolicy::new(
            Some(32),
            SpillPolicy::SpillToDisk {
                directory: directory.path().to_path_buf(),
            },
        ));
        for id in 0..600 {
            spool.push(&(id, "x".repeat(64))).unwrap();
        }
        assert!(spool.records.is_empty());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        let mut expected = 0;
        spool
            .replay(|(id, value)| {
                assert_eq!(id, expected);
                assert_eq!(value, "x".repeat(64));
                expected += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(expected, 600);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn fail_fast_does_not_apply_any_records() {
        let mut spool = StatementSpool::<String>::with_policy(MemoryPolicy::new(
            Some(32),
            SpillPolicy::FailFast,
        ));
        assert!(matches!(
            spool.push(&"x".repeat(64)),
            Err(ExecutorError::ResourceExhausted { .. })
        ));
        assert!(spool.records.is_empty());
        assert!(spool.disk.is_none());
    }

    #[test]
    fn spill_cleanup_also_runs_after_evaluation_or_replay_error() {
        let directory = tempfile::tempdir().unwrap();
        for replay_error in [false, true] {
            let mut spool = StatementSpool::<u64>::with_policy(MemoryPolicy::new(
                Some(1),
                SpillPolicy::SpillToDisk {
                    directory: directory.path().to_path_buf(),
                },
            ));
            spool.push(&1).unwrap();
            if replay_error {
                assert!(spool.replay(|_| Err(codec_error("apply failed"))).is_err());
            } else {
                drop(spool);
            }
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn corrupt_record_length_fails_before_allocation_and_cleans_up() {
        let directory = tempfile::tempdir().unwrap();
        let mut spool = StatementSpool::<u64>::with_policy(MemoryPolicy::new(
            Some(1),
            SpillPolicy::SpillToDisk {
                directory: directory.path().to_path_buf(),
            },
        ));
        spool.push(&7).unwrap();
        let disk = spool.disk.as_mut().unwrap();
        disk.writer.as_mut().unwrap().flush().unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&disk.path)
            .unwrap()
            .write_all(&u64::MAX.to_le_bytes())
            .unwrap();
        let error = spool
            .replay(|_| panic!("corrupt record must not apply"))
            .unwrap_err();
        assert!(matches!(error, ExecutorError::InvalidOperation { .. }));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
