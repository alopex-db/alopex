//! A Write-Ahead Log implementation.

use crate::error::{Error, Result};
use crate::types::{Key, TxnId, Value};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

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
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Appends a record to the log.
    ///
    /// The record is serialized and framed with:
    /// - Length (u32)
    /// - Checksum (u32)
    /// - Data ([u8])
    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let data = bincode::serialize(record).map_err(|e| Error::Io(std::io::Error::other(e)))?;
        let checksum = crc32fast::hash(&data);

        self.writer.write_all(&(data.len() as u32).to_le_bytes())?;
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&data)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?; // Ensure data is written to disk

        Ok(())
    }
}

/// A reader for the Write-Ahead Log.
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    /// Creates a new WAL reader for the given file path.
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for WalReader {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
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
                    return None;
                }
                Ok(n) => {
                    read += n;
                }
                Err(e) => return Some(Err(e.into())),
            }
        }

        let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let expected_checksum = u32::from_le_bytes(header[4..8].try_into().unwrap());

        let mut data = vec![0u8; len as usize];
        if let Err(e) = self.reader.read_exact(&mut data) {
            // Crash-tolerant behavior: treat a partially-written record body as EOF.
            //
            // This allows recovery from a torn/truncated WAL tail by replaying only the
            // successfully-written prefix.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(Error::Io(e)));
        }

        let actual_checksum = crc32fast::hash(&data);
        if actual_checksum != expected_checksum {
            return Some(Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL checksum mismatch",
            ))));
        }

        match bincode::deserialize(&data) {
            Ok(record) => Some(Ok(record)),
            Err(e) => Some(Err(Error::Io(std::io::Error::other(e)))),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

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
}
