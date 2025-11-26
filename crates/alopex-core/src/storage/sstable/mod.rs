//! A minimal SSTable (Sorted String Table) implementation for durable storage.
//!
//! The format is intentionally simple:
//! - Header: `magic[4]`, `version[u16]`, `reserved[u16]`, `entry_count[u64]`
//! - Body: repeated `(key_len[u32], value_len[u32], key_bytes, value_bytes)`
//! - Footer: `magic[4]`, `entry_count[u64]`, `crc32[u32]` covering the body section
//!
//! Keys must be appended in sorted order. The reader validates the header/footer,
//! recomputes the CRC32 for the body, and builds an in-memory index of offsets for
//! straightforward lookups.

use crate::error::{Error, Result};
use crate::types::{Key, Value};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HEADER_MAGIC: &[u8; 4] = b"ALXS";
const FOOTER_MAGIC: &[u8; 4] = b"SSTF";
const VERSION: u16 = 1;
const HEADER_SIZE: u64 = 4 + 2 + 2 + 8; // magic + version + reserved + entry_count
const FOOTER_SIZE: u64 = 4 + 8 + 4; // magic + entry_count + crc32

fn build_header(entry_count: u64) -> [u8; HEADER_SIZE as usize] {
    let mut buf = [0u8; HEADER_SIZE as usize];
    buf[0..4].copy_from_slice(HEADER_MAGIC);
    buf[4..6].copy_from_slice(&VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // reserved
    buf[8..16].copy_from_slice(&entry_count.to_le_bytes());
    buf
}

fn build_footer(entry_count: u64, checksum: u32) -> [u8; FOOTER_SIZE as usize] {
    let mut buf = [0u8; FOOTER_SIZE as usize];
    buf[0..4].copy_from_slice(FOOTER_MAGIC);
    buf[4..12].copy_from_slice(&entry_count.to_le_bytes());
    buf[12..16].copy_from_slice(&checksum.to_le_bytes());
    buf
}

fn read_header(file: &mut File) -> Result<u64> {
    let mut buf = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;

    if &buf[0..4] != HEADER_MAGIC {
        return Err(Error::InvalidFormat("invalid SSTable header magic".into()));
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != VERSION {
        return Err(Error::InvalidFormat(format!(
            "unsupported SSTable version: {version}"
        )));
    }

    Ok(u64::from_le_bytes(buf[8..16].try_into().unwrap()))
}

fn read_footer(file: &mut File, file_len: u64) -> Result<(u64, u32)> {
    if file_len < HEADER_SIZE + FOOTER_SIZE {
        return Err(Error::InvalidFormat("file too small for SSTable".into()));
    }

    let mut buf = [0u8; FOOTER_SIZE as usize];
    file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))?;
    file.read_exact(&mut buf)?;

    if &buf[0..4] != FOOTER_MAGIC {
        return Err(Error::InvalidFormat("invalid SSTable footer magic".into()));
    }

    let entry_count = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let checksum = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    Ok((entry_count, checksum))
}

/// Metadata persisted in the SSTable footer.
#[derive(Debug, Clone, Copy)]
pub struct SstableFooter {
    /// Number of key-value entries written.
    pub entry_count: u64,
    /// CRC32 checksum over the entries section.
    pub checksum: u32,
}

/// A single index entry containing offsets for a record in the SSTable.
#[derive(Debug, Clone)]
pub struct SstableIndexEntry {
    /// The key for the indexed record.
    pub key: Key,
    /// Byte offset of the record start (length fields).
    pub offset: u64,
    /// Length of the key in bytes.
    pub key_len: u32,
    /// Length of the value in bytes.
    pub value_len: u32,
}

impl SstableIndexEntry {
    /// Returns the byte offset where the value begins.
    pub fn value_offset(&self) -> u64 {
        self.offset + 8 + self.key_len as u64
    }
}

/// Writer for building a single SSTable file.
pub struct SstableWriter {
    path: PathBuf,
    writer: File,
    hasher: Hasher,
    entry_count: u64,
    closed: bool,
    last_key: Option<Key>,
}

impl SstableWriter {
    /// Creates a new SSTable writer at the provided file path.
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut writer = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        writer.write_all(&build_header(0))?;

        Ok(Self {
            path: path.to_path_buf(),
            writer,
            hasher: Hasher::new(),
            entry_count: 0,
            closed: false,
            last_key: None,
        })
    }

    /// Appends a sorted key-value pair to the SSTable.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidFormat("writer already closed".into()));
        }
        if key.len() > u32::MAX as usize || value.len() > u32::MAX as usize {
            return Err(Error::InvalidFormat(
                "key or value too large for SSTable".into(),
            ));
        }
        if let Some(prev) = &self.last_key {
            if key < prev.as_slice() {
                return Err(Error::InvalidFormat(
                    "keys must be appended in sorted order".into(),
                ));
            }
        }

        let key_len = key.len() as u32;
        let value_len = value.len() as u32;

        let mut len_buf = [0u8; 8];
        len_buf[..4].copy_from_slice(&key_len.to_le_bytes());
        len_buf[4..].copy_from_slice(&value_len.to_le_bytes());

        self.writer.write_all(&len_buf)?;
        self.writer.write_all(key)?;
        self.writer.write_all(value)?;

        self.hasher.update(&len_buf);
        self.hasher.update(key);
        self.hasher.update(value);
        self.entry_count += 1;
        self.last_key = Some(key.to_vec());
        Ok(())
    }

    /// Finalizes the SSTable, writing the footer and updating the header.
    pub fn finish(mut self) -> Result<SstableFooter> {
        if self.closed {
            return Err(Error::InvalidFormat("writer already closed".into()));
        }

        let checksum = self.hasher.finalize();
        let footer = SstableFooter {
            entry_count: self.entry_count,
            checksum,
        };

        self.writer.write_all(&build_footer(footer.entry_count, footer.checksum))?;
        self.writer.flush()?;
        self.writer.sync_all()?;

        // Rewrite header with the final entry count.
        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(&build_header(footer.entry_count))?;
        self.writer.sync_all()?;

        self.closed = true;
        Ok(footer)
    }

    /// Returns the path of the SSTable being written.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Reader that validates and scans an SSTable file, constructing a simple index.
#[derive(Debug)]
pub struct SstableReader {
    file: File,
    index: Vec<SstableIndexEntry>,
    footer: SstableFooter,
}

impl SstableReader {
    /// Opens an SSTable from disk, verifying its checksum and building an index.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len();
        let header_entries = read_header(&mut file)?;
        let (footer_entries, checksum) = read_footer(&mut file, file_len)?;

        if header_entries != footer_entries {
            return Err(Error::InvalidFormat(
                "header/footer entry counts do not match".into(),
            ));
        }

        let entries_end = file_len
            .checked_sub(FOOTER_SIZE)
            .ok_or_else(|| Error::InvalidFormat("file shorter than footer".into()))?;

        file.seek(SeekFrom::Start(HEADER_SIZE))?;
        let mut reader = std::io::BufReader::new(file);
        let mut current_offset = HEADER_SIZE;
        let mut index = Vec::with_capacity(footer_entries as usize);
        let mut hasher = Hasher::new();
        let mut scratch = [0u8; 4096];

        for _ in 0..footer_entries {
            if current_offset + 8 > entries_end {
                return Err(Error::InvalidFormat(
                    "truncated entry header before footer".into(),
                ));
            }

            let record_start = current_offset;
            let mut len_buf = [0u8; 8];
            reader.read_exact(&mut len_buf)?;
            current_offset += 8;

            hasher.update(&len_buf);

            let key_len = u32::from_le_bytes(len_buf[..4].try_into().unwrap()) as u64;
            let value_len = u32::from_le_bytes(len_buf[4..].try_into().unwrap()) as u64;

            if current_offset + key_len + value_len > entries_end {
                return Err(Error::InvalidFormat(
                    "entry extends beyond footer boundary".into(),
                ));
            }

            let mut key = vec![0u8; key_len as usize];
            reader.read_exact(&mut key)?;
            hasher.update(&key);
            current_offset += key_len;

            // Stream value into the checksum without keeping it in memory.
            let mut remaining = value_len;
            while remaining > 0 {
                let chunk = std::cmp::min(remaining, scratch.len() as u64) as usize;
                reader.read_exact(&mut scratch[..chunk])?;
                hasher.update(&scratch[..chunk]);
                remaining -= chunk as u64;
            }
            current_offset += value_len;

            index.push(SstableIndexEntry {
                key,
                offset: record_start,
                key_len: key_len as u32,
                value_len: value_len as u32,
            });
        }

        if current_offset != entries_end {
            return Err(Error::InvalidFormat(
                "unexpected padding or trailing bytes before footer".into(),
            ));
        }

        let computed = hasher.finalize();
        if computed != checksum {
            return Err(Error::ChecksumMismatch);
        }

        let footer = SstableFooter {
            entry_count: footer_entries,
            checksum,
        };

        let file = reader.into_inner();
        Ok(Self { file, index, footer })
    }

    /// Returns the number of entries in the SSTable.
    pub fn entry_count(&self) -> u64 {
        self.footer.entry_count
    }

    /// Returns the checksum stored in the footer.
    pub fn checksum(&self) -> u32 {
        self.footer.checksum
    }

    /// Provides a read-only view into the in-memory index.
    pub fn index(&self) -> &[SstableIndexEntry] {
        &self.index
    }

    /// Attempts to read the value for the provided key.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        let idx = match self
            .index
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };

        let entry = &self.index[idx];
        self.file.seek(SeekFrom::Start(entry.value_offset()))?;
        let mut value = vec![0u8; entry.value_len as usize];
        self.file.read_exact(&mut value)?;
        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn key(s: &str) -> Key {
        s.as_bytes().to_vec()
    }

    fn value(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    #[test]
    fn writes_and_reads_sorted_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("table.sst");

        {
            let mut writer = SstableWriter::create(&path).unwrap();
            writer.append(&key("a"), &value("1")).unwrap();
            writer.append(&key("b"), &value("2")).unwrap();
            writer.append(&key("c"), &value("3")).unwrap();
            let footer = writer.finish().unwrap();
            assert_eq!(footer.entry_count, 3);
            assert_ne!(footer.checksum, 0);
        }

        let mut reader = SstableReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 3);
        assert_eq!(reader.get(&key("b")).unwrap(), Some(value("2")));
        assert_eq!(reader.get(&key("missing")).unwrap(), None);
        assert_eq!(reader.index().len(), 3);
    }

    #[test]
    fn rejects_unsorted_keys() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("table.sst");
        let mut writer = SstableWriter::create(&path).unwrap();
        writer.append(&key("b"), &value("1")).unwrap();
        let err = writer.append(&key("a"), &value("2")).unwrap_err();
        match err {
            Error::InvalidFormat(msg) => assert!(msg.contains("sorted")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn detects_checksum_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("table.sst");

        {
            let mut writer = SstableWriter::create(&path).unwrap();
            writer.append(&key("a"), &value("1")).unwrap();
            writer.finish().unwrap();
        }

        // Corrupt the file by flipping a byte in the value.
        {
            let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(
                HEADER_SIZE + 8 + "a".len() as u64, // skip header + len fields + key
            ))
            .unwrap();
            let mut b = [0u8; 1];
            file.read_exact(&mut b).unwrap();
            file.seek(SeekFrom::Current(-1)).unwrap();
            file.write_all(&[b[0] ^ 0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        let err = SstableReader::open(&path).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch));
    }
}
