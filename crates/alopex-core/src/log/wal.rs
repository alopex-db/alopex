//! A Write-Ahead Log implementation.

use crate::error::{Error, Result};
use crate::types::{Key, TxnId, Value};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use tracing::warn;

/// A record in the Write-Ahead Log.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum WalRecord {
    /// Signals the start of a transaction's writes. Not strictly required but good for clarity.
    Begin(TxnId),
    /// A key-value pair to be written.
    Put(TxnId, Key, Value),
    /// A key to be deleted.
    Delete(TxnId, Key),
    /// Commits a transaction.
    Commit(TxnId),
}

/// A writer for the Write-Ahead Log.
pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    /// Creates a new WAL writer for the given file path.
    ///
    /// Before opening for append, the existing log is repaired by truncating it to
    /// the end of its last fully-valid record. A crash can leave a torn record at
    /// the tail (a partial or checksum-failing frame). Without repair, the next
    /// append would land *after* that torn record, leaving it permanently in the
    /// middle of the log; replay stops at the first torn record, so every record
    /// appended afterwards would be silently unrecoverable. Repairing on open
    /// guarantees appends always extend a fully-replayable log.
    pub fn new(path: &Path) -> Result<Self> {
        Self::repair_torn_tail(path)?;
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Truncates an existing WAL to the end of its last fully-valid record.
    ///
    /// No-op if the file does not exist or is already clean (the valid prefix spans
    /// the whole file).
    fn repair_torn_tail(path: &Path) -> Result<()> {
        let file_len = match std::fs::metadata(path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(Error::Io(e)),
        };
        if file_len == 0 {
            return Ok(());
        }

        let mut reader = WalReader::new(path)?;
        // Drive the iterator to the end; `valid_prefix_len` then holds the offset of
        // the last fully-parsed record. Any reader error is surfaced (a genuine I/O
        // fault must not be silently swallowed during repair).
        for record in reader.by_ref() {
            record?;
        }
        let valid_len = reader.valid_prefix_len();
        let stop_reason = reader.last_stop_reason();
        drop(reader);

        if valid_len < file_len {
            warn!(
                path = %path.display(),
                stop_reason = ?stop_reason,
                valid_len,
                file_len,
                "WAL recovery stopped at torn tail"
            );
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(valid_len)?;
            file.sync_all()?;
        }
        Ok(())
    }

    /// Appends a record to the log buffer.
    ///
    /// The record is serialized and framed with:
    /// - Length (u32)
    /// - Checksum (u32)
    /// - Data ([u8])
    ///
    /// This only writes into the in-process buffer; it does **not** fsync. The
    /// caller must invoke [`WalWriter::sync`] at the transaction commit boundary
    /// to make the appended records durable. Decoupling append from fsync turns
    /// an N-record transaction's N fsyncs into a single fsync at commit, which
    /// is the standard WAL group-commit-at-the-record-boundary pattern (cf.
    /// `lsm::wal::SyncMode`/`force_sync`, and fjall/mini-lsm reference impls).
    /// Durability semantics are unchanged: a commit is only acknowledged after
    /// [`WalWriter::sync`] returns, so every record of a committed transaction
    /// is on stable storage (v0.5 Durability requirement CORE-5.1).
    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let data = bincode::serialize(record).map_err(|e| Error::Io(std::io::Error::other(e)))?;
        let checksum = crc32fast::hash(&data);

        self.writer.write_all(&(data.len() as u32).to_le_bytes())?;
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&data)?;

        Ok(())
    }

    /// Flushes the buffer and fsyncs the underlying file, making all previously
    /// appended records durable.
    ///
    /// This is the durability point for a transaction commit: once it returns
    /// `Ok`, every record appended since the last `sync` is guaranteed to be on
    /// stable storage. A `sync` failure is propagated so the caller can refuse
    /// to acknowledge the commit (CORE-5.1: a WAL fsync failure must not be
    /// reported as a successful commit).
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

/// A reader for the Write-Ahead Log.
pub struct WalReader {
    reader: BufReader<File>,
    /// Total file length, used to detect a torn tail whose framed length points
    /// past the end of the file.
    file_len: u64,
    /// Bytes consumed so far, tracked to bound record-body allocation.
    consumed: u64,
    /// End offset of the last record that passed checksum verification and
    /// deserialized successfully.
    valid_prefix: u64,
    /// Reason iteration stopped before the physical end of the file, if a
    /// torn/corrupt tail was detected.
    last_stop_reason: Option<&'static str>,
}

impl WalReader {
    /// Creates a new WAL reader for the given file path.
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len();
        Ok(Self {
            reader: BufReader::new(file),
            file_len,
            consumed: 0,
            valid_prefix: 0,
            last_stop_reason: None,
        })
    }

    /// Byte offset of the end of the last fully-parsed record, i.e. the length of
    /// the valid, replayable prefix of the log.
    ///
    /// After iteration stops (either at a clean boundary or a torn tail), this is
    /// the offset to which the WAL should be truncated so that subsequent appends
    /// extend a fully-replayable log rather than being shadowed by a torn record
    /// left in the middle of the file.
    pub fn valid_prefix_len(&self) -> u64 {
        self.valid_prefix
    }

    /// Reason the reader stopped before the physical end of the file.
    fn last_stop_reason(&self) -> Option<&'static str> {
        self.last_stop_reason
    }
}

impl Iterator for WalReader {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let record_start = self.consumed;
        if self.valid_prefix != record_start {
            return None;
        }

        let mut header = [0u8; 8]; // 4 bytes for length, 4 for checksum
        let mut read = 0;

        while read < header.len() {
            match self.reader.read(&mut header[read..]) {
                Ok(0) if read == 0 => {
                    // Clean EOF on a record boundary.
                    return None;
                }
                Ok(0) => {
                    // Crash-tolerant behavior: treat a partially-written header as EOF.
                    self.last_stop_reason = Some("torn WAL record header");
                    return None;
                }
                Ok(n) => {
                    read += n;
                }
                Err(e) => return Some(Err(e.into())),
            }
        }
        self.consumed += header.len() as u64;

        let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let expected_checksum = u32::from_le_bytes(header[4..8].try_into().unwrap());

        // Crash-tolerant behavior: if the framed length points past the end of the
        // file, the header itself is part of a torn tail (its bytes straddle the
        // truncation point). Treat it as end-of-log instead of allocating a buffer
        // sized by an untrusted length, which guards against huge allocations on a
        // corrupt/garbage header.
        if len as u64 > self.file_len.saturating_sub(self.consumed) {
            self.last_stop_reason = Some("WAL record length exceeds remaining file bytes");
            return None;
        }

        let mut data = vec![0u8; len as usize];
        if let Err(e) = self.reader.read_exact(&mut data) {
            // Crash-tolerant behavior: treat a partially-written record body as EOF.
            //
            // This allows recovery from a torn/truncated WAL tail by replaying only the
            // successfully-written prefix.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                self.last_stop_reason = Some("torn WAL record body");
                return None;
            }
            return Some(Err(Error::Io(e)));
        }
        self.consumed += len as u64;

        let actual_checksum = crc32fast::hash(&data);
        if actual_checksum != expected_checksum {
            // Crash-tolerant behavior: a checksum mismatch marks the end of the
            // durable prefix. Records are appended atomically per-record and read
            // in order, so any prior record verified successfully; a mismatch can
            // therefore only arise on a torn tail where the truncation point left a
            // full-length header followed by a partially-written or garbage body.
            // Stop replay and recover the valid prefix, mirroring the torn-header
            // and torn-body handling above rather than failing the whole recovery.
            self.last_stop_reason = Some("WAL record checksum mismatch");
            return None;
        }

        match bincode::deserialize(&data) {
            Ok(record) => {
                self.valid_prefix = self.consumed;
                Some(Ok(record))
            }
            Err(_) => {
                self.last_stop_reason = Some("WAL record deserialization failed");
                None
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    struct VecWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for VecWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.0.lock().unwrap();
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn wal_reader_ignores_truncated_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.append(&WalRecord::Begin(TxnId(1))).unwrap();
            writer.append(&WalRecord::Commit(TxnId(1))).unwrap();
        }

        {
            // Simulate a crash after writing only part of the next header.
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&[0u8; 4]).unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        assert!(reader.next().unwrap().is_ok());
        assert!(reader.next().unwrap().is_ok());
        assert!(reader.next().is_none());
    }

    #[test]
    fn wal_reader_stops_at_checksum_mismatch_tail() {
        // A crash that truncates the file at an arbitrary byte (as a torn tail) can
        // leave a full-length header followed by a partial/garbage body whose length
        // still fits in the file. The body then fails the checksum. Recovery must
        // replay the valid prefix and stop, not abort the whole recovery.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.append(&WalRecord::Begin(TxnId(1))).unwrap();
            writer.append(&WalRecord::Commit(TxnId(1))).unwrap();
            // A third record whose body we will corrupt in place.
            writer.append(&WalRecord::Begin(TxnId(2))).unwrap();
        }

        {
            // Flip the last byte of the file: the framed length still fits, so
            // `read_exact` succeeds, but the checksum no longer matches.
            let len = std::fs::metadata(&path).unwrap().len();
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(len - 1)).unwrap();
            let mut last = [0u8; 1];
            use std::io::Read as _;
            file.read_exact(&mut last).unwrap();
            file.seek(SeekFrom::Start(len - 1)).unwrap();
            file.write_all(&[last[0] ^ 0xFF]).unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        assert!(reader.next().unwrap().is_ok());
        assert!(reader.next().unwrap().is_ok());
        // The corrupted third record is treated as a torn tail: clean end-of-log.
        assert!(reader.next().is_none());
    }

    #[test]
    fn wal_reader_stops_on_garbage_length_header() {
        // A torn tail whose header bytes straddle the truncation point can decode to
        // an enormous framed length. The reader must treat it as end-of-log rather
        // than allocating a buffer sized by the untrusted length.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.append(&WalRecord::Begin(TxnId(1))).unwrap();
        }

        {
            // Append a header claiming a 4 GiB body with no body following.
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&u32::MAX.to_le_bytes()).unwrap(); // length
            file.write_all(&0u32.to_le_bytes()).unwrap(); // checksum
        }

        let mut reader = WalReader::new(&path).unwrap();
        assert!(reader.next().unwrap().is_ok());
        assert!(reader.next().is_none());
    }

    #[test]
    fn wal_writer_logs_torn_tail_repair_warning() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.append(&WalRecord::Begin(TxnId(1))).unwrap();
            writer.sync().unwrap();
        }

        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&[0u8; 4]).unwrap();
        }

        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let make_writer = {
            let buf = buffer.clone();
            move || VecWriter(buf.clone())
        };
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(make_writer)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let _writer = WalWriter::new(&path).unwrap();

        let log = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        assert!(
            log.contains("WAL recovery stopped at torn tail"),
            "expected WAL repair warning, got: {}",
            log
        );
        assert!(
            log.contains("torn WAL record header"),
            "expected stop reason in warning, got: {}",
            log
        );
    }
}
