//! SSTable implementation for the LSM-tree engine.
//!
//! This is a standalone (per-table) format used by the LSM layer. It is intentionally independent
//! from the legacy `storage::sstable` implementation.
//!
//! # Ordering / MVCC
//!
//! Entries are stored in sorted order by:
//! 1) `user_key` ascending (lexicographic)
//! 2) `timestamp` descending
//! 3) `sequence` descending
//!
//! Reads at a given `read_timestamp` return the first entry for a key whose `timestamp` is
//! `<= read_timestamp` (tombstones included).
//!
//! # On-disk layout (spec §3.3.2)
//!
//! ```text
//! Header (32B)
//! Data Blocks...
//! Index Block
//! Bloom Filter (optional)
//! Footer (48B)
//! ```
//!
//! Each data block is:
//! - `entry_count: u32`
//! - `uncompressed_len: u32`
//! - `payload: [u8]` (entries bytes, possibly compressed)
//! - `crc32: u32` over `entry_count || uncompressed_len || payload`
//!
//! The index block maps key ranges to block offsets/sizes.

use std::cmp::Ordering as CmpOrdering;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::lsm::buffer_pool::{BlockId, BufferPool, DataBlock};
use crate::lsm::metrics::LsmMetrics;
use crate::storage::{create_compressor, CompressionV2, Compressor};
use crate::types::{Key, Value};

const SST_MAGIC: [u8; 4] = *b"ASST";
const SST_MAGIC_REVERSE: [u8; 4] = *b"TSSA";
const SST_VERSION: u16 = 1;

const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 48;

const FLAG_BLOOM_PRESENT: u16 = 1 << 0;
const FLAG_COMPRESSION_MASK: u16 = 0b11 << 1;

fn crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

fn compression_to_flag(compression: CompressionType) -> u16 {
    match compression {
        CompressionType::None => 0,
        CompressionType::Lz4 => 1,
        CompressionType::Zstd { .. } => 2,
    }
}

fn compression_from_flag(flag: u16, zstd_level: i32) -> Result<CompressionType> {
    match flag {
        0 => Ok(CompressionType::None),
        1 => Ok(CompressionType::Lz4),
        2 => Ok(CompressionType::Zstd { level: zstd_level }),
        _ => Err(Error::InvalidFormat(
            "unknown SSTable compression flag".into(),
        )),
    }
}

fn compression_to_v2(compression: CompressionType) -> CompressionV2 {
    match compression {
        CompressionType::None => CompressionV2::None,
        CompressionType::Lz4 => CompressionV2::Lz4,
        CompressionType::Zstd { level } => CompressionV2::Zstd { level },
    }
}

/// SSTable configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SSTableConfig {
    /// Block size (default: 4KB).
    pub block_size: usize,
    /// Enable Bloom filter.
    pub enable_bloom_filter: bool,
    /// Desired Bloom filter false positive rate (default: 1%).
    pub bloom_filter_fpr: f64,
    /// Compression algorithm.
    pub compression: CompressionType,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            block_size: 4096,
            enable_bloom_filter: true,
            bloom_filter_fpr: 0.01,
            compression: CompressionType::None,
        }
    }
}

/// Compression algorithm for data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression.
    None,
    /// LZ4 compression (feature-gated).
    Lz4,
    /// Zstd compression with a configurable level (feature-gated).
    Zstd {
        /// Compression level.
        level: i32,
    },
}

/// SSTable entry type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SSTableEntryType {
    /// Put operation.
    Put = 0,
    /// Delete tombstone.
    Delete = 1,
}

impl TryFrom<u8> for SSTableEntryType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Put),
            1 => Ok(Self::Delete),
            _ => Err(Error::InvalidFormat("unknown SSTable entry type".into())),
        }
    }
}

/// One MVCC version stored in an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableEntry {
    /// User key bytes.
    pub key: Key,
    /// Optional value (None = tombstone).
    pub value: Option<Value>,
    /// MVCC timestamp.
    pub timestamp: u64,
    /// Sequence number for tie-breaking.
    pub sequence: u64,
}

fn compare_entries(a: (&[u8], u64, u64), b: (&[u8], u64, u64)) -> CmpOrdering {
    match a.0.cmp(b.0) {
        CmpOrdering::Equal => match b.1.cmp(&a.1) {
            CmpOrdering::Equal => b.2.cmp(&a.2),
            other => other,
        },
        other => other,
    }
}

/// SSTable header (32 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SSTableHeader {
    /// Format version.
    pub version: u16,
    /// Flags (bitset).
    pub flags: u16,
    /// Number of data blocks.
    pub block_count: u32,
    /// Number of entries.
    pub entry_count: u64,
    /// Minimum key length observed.
    pub min_key_len: u32,
    /// Maximum key length observed.
    pub max_key_len: u32,
    /// Reserved (currently used for compression flag in low bits and zstd level in high bits).
    pub reserved: u32,
}

impl SSTableHeader {
    /// Serialize to fixed-size bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut out = [0u8; HEADER_SIZE];
        out[0..4].copy_from_slice(&SST_MAGIC);
        out[4..6].copy_from_slice(&self.version.to_le_bytes());
        out[6..8].copy_from_slice(&self.flags.to_le_bytes());
        out[8..12].copy_from_slice(&self.block_count.to_le_bytes());
        out[12..20].copy_from_slice(&self.entry_count.to_le_bytes());
        out[20..24].copy_from_slice(&self.min_key_len.to_le_bytes());
        out[24..28].copy_from_slice(&self.max_key_len.to_le_bytes());
        out[28..32].copy_from_slice(&self.reserved.to_le_bytes());
        out
    }

    /// Deserialize and validate.
    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Result<Self> {
        if bytes[0..4] != SST_MAGIC {
            return Err(Error::InvalidFormat("SSTable header magic mismatch".into()));
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().expect("fixed slice length"));
        if version != SST_VERSION {
            return Err(Error::InvalidFormat(format!(
                "unsupported SSTable version: {version}"
            )));
        }
        let flags = u16::from_le_bytes(bytes[6..8].try_into().expect("fixed slice length"));
        let block_count = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed slice length"));
        let entry_count = u64::from_le_bytes(bytes[12..20].try_into().expect("fixed slice length"));
        let min_key_len = u32::from_le_bytes(bytes[20..24].try_into().expect("fixed slice length"));
        let max_key_len = u32::from_le_bytes(bytes[24..28].try_into().expect("fixed slice length"));
        let reserved = u32::from_le_bytes(bytes[28..32].try_into().expect("fixed slice length"));
        Ok(Self {
            version,
            flags,
            block_count,
            entry_count,
            min_key_len,
            max_key_len,
            reserved,
        })
    }
}

/// SSTable footer (48 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SSTableFooter {
    /// Index block offset (from file start).
    pub index_offset: u64,
    /// Index block size in bytes.
    pub index_size: u32,
    /// Bloom filter offset (from file start) or 0 if absent.
    pub bloom_offset: u64,
    /// Bloom filter size in bytes.
    pub bloom_size: u32,
    /// Minimum timestamp stored in this table.
    pub min_timestamp: u64,
    /// Maximum timestamp stored in this table.
    pub max_timestamp: u64,
    /// CRC32 over the entire file excluding the footer.
    pub file_crc32: u32,
}

impl SSTableFooter {
    /// Serialize to fixed-size bytes.
    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut out = [0u8; FOOTER_SIZE];
        out[0..8].copy_from_slice(&self.index_offset.to_le_bytes());
        out[8..12].copy_from_slice(&self.index_size.to_le_bytes());
        out[12..20].copy_from_slice(&self.bloom_offset.to_le_bytes());
        out[20..24].copy_from_slice(&self.bloom_size.to_le_bytes());
        out[24..32].copy_from_slice(&self.min_timestamp.to_le_bytes());
        out[32..40].copy_from_slice(&self.max_timestamp.to_le_bytes());
        out[40..44].copy_from_slice(&self.file_crc32.to_le_bytes());
        out[44..48].copy_from_slice(&SST_MAGIC_REVERSE);
        out
    }

    /// Deserialize and validate.
    pub fn from_bytes(bytes: &[u8; FOOTER_SIZE]) -> Result<Self> {
        if bytes[44..48] != SST_MAGIC_REVERSE {
            return Err(Error::InvalidFormat("SSTable footer magic mismatch".into()));
        }
        let index_offset = u64::from_le_bytes(bytes[0..8].try_into().expect("fixed slice length"));
        let index_size = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed slice length"));
        let bloom_offset =
            u64::from_le_bytes(bytes[12..20].try_into().expect("fixed slice length"));
        let bloom_size = u32::from_le_bytes(bytes[20..24].try_into().expect("fixed slice length"));
        let min_timestamp =
            u64::from_le_bytes(bytes[24..32].try_into().expect("fixed slice length"));
        let max_timestamp =
            u64::from_le_bytes(bytes[32..40].try_into().expect("fixed slice length"));
        let file_crc32 = u32::from_le_bytes(bytes[40..44].try_into().expect("fixed slice length"));
        Ok(Self {
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            min_timestamp,
            max_timestamp,
            file_crc32,
        })
    }
}

/// Index entry for a data block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableIndexEntry {
    /// Block start offset.
    pub offset: u64,
    /// Block size in bytes.
    pub size: u32,
    /// First user key contained.
    pub first_key: Key,
    /// Last user key contained.
    pub last_key: Key,
}

fn encode_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn encode_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn decode_u32(input: &[u8], cursor: &mut usize) -> Result<u32> {
    if input.len() < *cursor + 4 {
        return Err(Error::InvalidFormat("SSTable block/index truncated".into()));
    }
    let v = u32::from_le_bytes(
        input[*cursor..*cursor + 4]
            .try_into()
            .expect("fixed slice length"),
    );
    *cursor += 4;
    Ok(v)
}

fn decode_u64(input: &[u8], cursor: &mut usize) -> Result<u64> {
    if input.len() < *cursor + 8 {
        return Err(Error::InvalidFormat("SSTable block/index truncated".into()));
    }
    let v = u64::from_le_bytes(
        input[*cursor..*cursor + 8]
            .try_into()
            .expect("fixed slice length"),
    );
    *cursor += 8;
    Ok(v)
}

fn encode_index(entries: &[SSTableIndexEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_u32(&mut out, entries.len() as u32);
    for e in entries {
        encode_u64(&mut out, e.offset);
        encode_u32(&mut out, e.size);
        encode_u32(&mut out, e.first_key.len() as u32);
        out.extend_from_slice(&e.first_key);
        encode_u32(&mut out, e.last_key.len() as u32);
        out.extend_from_slice(&e.last_key);
    }
    out
}

fn decode_index(bytes: &[u8]) -> Result<Vec<SSTableIndexEntry>> {
    let mut cursor = 0usize;
    let count = decode_u32(bytes, &mut cursor)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let offset = decode_u64(bytes, &mut cursor)?;
        let size = decode_u32(bytes, &mut cursor)?;
        let first_len = decode_u32(bytes, &mut cursor)? as usize;
        if bytes.len() < cursor + first_len {
            return Err(Error::InvalidFormat(
                "SSTable index truncated (first key)".into(),
            ));
        }
        let first_key = bytes[cursor..cursor + first_len].to_vec();
        cursor += first_len;
        let last_len = decode_u32(bytes, &mut cursor)? as usize;
        if bytes.len() < cursor + last_len {
            return Err(Error::InvalidFormat(
                "SSTable index truncated (last key)".into(),
            ));
        }
        let last_key = bytes[cursor..cursor + last_len].to_vec();
        cursor += last_len;
        out.push(SSTableIndexEntry {
            offset,
            size,
            first_key,
            last_key,
        });
    }
    if cursor != bytes.len() {
        return Err(Error::InvalidFormat(
            "SSTable index block has trailing bytes".into(),
        ));
    }
    Ok(out)
}

/// Simple Bloom filter (bitset + k hashes).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    k: u32,
    bits: Vec<u8>,
}

impl BloomFilter {
    fn new(k: u32, bits: Vec<u8>) -> Self {
        Self { k, bits }
    }

    fn bit_len(&self) -> u32 {
        (self.bits.len() as u32) * 8
    }

    fn set_bit(&mut self, bit: u32) {
        let idx = (bit / 8) as usize;
        let off = (bit % 8) as u8;
        self.bits[idx] |= 1u8 << off;
    }

    fn get_bit(&self, bit: u32) -> bool {
        let idx = (bit / 8) as usize;
        let off = (bit % 8) as u8;
        (self.bits[idx] & (1u8 << off)) != 0
    }

    fn hashes(&self, key: &[u8]) -> (u32, u32) {
        let h1 = crc32fast::hash(key);
        let mut tmp = Vec::with_capacity(key.len() + 1);
        tmp.extend_from_slice(key);
        tmp.push(0xA5);
        let h2 = crc32fast::hash(&tmp);
        (h1, h2.max(1))
    }

    fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hashes(key);
        let m = self.bit_len();
        for i in 0..self.k {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % m;
            self.set_bit(bit);
        }
    }

    /// Check if the key may be present.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hashes(key);
        let m = self.bit_len();
        for i in 0..self.k {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % m;
            if !self.get_bit(bit) {
                return false;
            }
        }
        true
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_u32(&mut out, self.k);
        encode_u32(&mut out, self.bits.len() as u32);
        out.extend_from_slice(&self.bits);
        out
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0usize;
        let k = decode_u32(bytes, &mut cursor)?;
        let len = decode_u32(bytes, &mut cursor)? as usize;
        if bytes.len() < cursor + len {
            return Err(Error::InvalidFormat("SSTable bloom truncated".into()));
        }
        let bits = bytes[cursor..cursor + len].to_vec();
        cursor += len;
        if cursor != bytes.len() {
            return Err(Error::InvalidFormat(
                "SSTable bloom has trailing bytes".into(),
            ));
        }
        Ok(Self::new(k, bits))
    }
}

fn bloom_params(entry_count: u64, fpr: f64) -> (u32, usize) {
    let n = entry_count.max(1) as f64;
    let p = fpr.clamp(1e-9, 0.5);
    let m_bits = (-(n * p.ln()) / (2f64.ln().powi(2))).ceil().max(8.0);
    let k = ((m_bits / n) * 2f64.ln()).round().max(1.0);
    let m_bytes = (m_bits as usize).div_ceil(8);
    (k as u32, m_bytes.max(1))
}

/// Writer for a single SSTable file.
pub struct SSTableWriter {
    file: File,
    config: SSTableConfig,
    compressor: Box<dyn Compressor>,
    data_block_buf: Vec<u8>,
    data_block_entry_count: u32,
    block_index: Vec<SSTableIndexEntry>,
    entry_count: u64,
    last_key: Option<(Key, u64, u64)>,
    first_key_in_block: Option<Key>,
    last_key_in_block: Option<Key>,
    min_key_len: u32,
    max_key_len: u32,
    min_timestamp: u64,
    max_timestamp: u64,
    bloom: Option<BloomFilter>,
    /// Distinct user keys encountered (used to build Bloom filter at finish()).
    ///
    /// Note: this is an in-memory buffer. For very large tables it can still be substantial; a
    /// future improvement is to support incremental Bloom insertion with an upfront size estimate.
    bloom_keys: Vec<Key>,
    last_bloom_key: Option<Key>,
    closed: bool,
}

impl SSTableWriter {
    /// Create a new SSTable file.
    pub fn create(path: &Path, config: SSTableConfig) -> Result<Self> {
        if config.block_size < 64 {
            return Err(Error::InvalidFormat("SSTable block_size too small".into()));
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;

        let compression_v2 = compression_to_v2(config.compression);
        let compressor = create_compressor(compression_v2)
            .map_err(|e| Error::InvalidFormat(format!("SSTable compression unavailable: {e:?}")))?;

        let mut writer = Self {
            file,
            config,
            compressor,
            data_block_buf: Vec::new(),
            data_block_entry_count: 0,
            block_index: Vec::new(),
            entry_count: 0,
            last_key: None,
            first_key_in_block: None,
            last_key_in_block: None,
            min_key_len: u32::MAX,
            max_key_len: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            bloom: None,
            bloom_keys: Vec::new(),
            last_bloom_key: None,
            closed: false,
        };

        // Placeholder header.
        let header = SSTableHeader {
            version: SST_VERSION,
            flags: 0,
            block_count: 0,
            entry_count: 0,
            min_key_len: 0,
            max_key_len: 0,
            reserved: 0,
        };
        writer.file.write_all(&header.to_bytes())?;

        Ok(writer)
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)?;
        Ok(())
    }

    fn current_offset(&mut self) -> Result<u64> {
        Ok(self.file.stream_position()?)
    }

    fn encode_entry_into_block(&mut self, entry: &SSTableEntry) -> Result<()> {
        let entry_type = if entry.value.is_some() {
            SSTableEntryType::Put
        } else {
            SSTableEntryType::Delete
        };
        let key_len = entry.key.len();
        if key_len > u32::MAX as usize {
            return Err(Error::InvalidFormat("SSTable key too large".into()));
        }
        let val_len = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
        if val_len > u32::MAX as usize {
            return Err(Error::InvalidFormat("SSTable value too large".into()));
        }

        self.min_key_len = self.min_key_len.min(key_len as u32);
        self.max_key_len = self.max_key_len.max(key_len as u32);
        self.min_timestamp = self.min_timestamp.min(entry.timestamp);
        self.max_timestamp = self.max_timestamp.max(entry.timestamp);

        if self.first_key_in_block.is_none() {
            self.first_key_in_block = Some(entry.key.clone());
        }
        self.last_key_in_block = Some(entry.key.clone());

        self.data_block_buf.push(entry_type as u8);
        encode_u64(&mut self.data_block_buf, entry.timestamp);
        encode_u64(&mut self.data_block_buf, entry.sequence);
        encode_u32(&mut self.data_block_buf, key_len as u32);
        encode_u32(&mut self.data_block_buf, val_len as u32);
        self.data_block_buf.extend_from_slice(&entry.key);
        if let Some(v) = &entry.value {
            self.data_block_buf.extend_from_slice(v);
        }
        Ok(())
    }

    fn flush_block_if_needed(&mut self, upcoming_len: usize) -> Result<()> {
        let projected = self.data_block_buf.len().saturating_add(upcoming_len);
        let overhead = 4 /*entry_count*/ + 4 /*uncompressed_len*/ + 4 /*crc*/;
        if self.data_block_entry_count > 0 && projected + overhead > self.config.block_size {
            self.finish_block()?;
        }
        Ok(())
    }

    fn finish_block(&mut self) -> Result<()> {
        if self.data_block_entry_count == 0 {
            return Ok(());
        }

        let entry_count = self.data_block_entry_count;
        let uncompressed = std::mem::take(&mut self.data_block_buf);
        let compressed = self
            .compressor
            .compress(&uncompressed)
            .map_err(|e| Error::InvalidFormat(format!("SSTable compression failed: {e:?}")))?;

        let uncompressed_len: u32 = uncompressed
            .len()
            .try_into()
            .map_err(|_| Error::InvalidFormat("SSTable block too large".into()))?;
        let mut block = Vec::with_capacity(4 + 4 + compressed.len() + 4);
        block.extend_from_slice(&entry_count.to_le_bytes());
        block.extend_from_slice(&uncompressed_len.to_le_bytes());
        block.extend_from_slice(&compressed);
        let c = crc32(&block);
        block.extend_from_slice(&c.to_le_bytes());

        let offset = self.current_offset()?;
        self.write_bytes(&block)?;

        let first_key = self
            .first_key_in_block
            .take()
            .expect("block has at least one entry");
        let last_key = self
            .last_key_in_block
            .take()
            .expect("block has at least one entry");
        let size = block
            .len()
            .try_into()
            .map_err(|_| Error::InvalidFormat("SSTable block size overflow".into()))?;
        self.block_index.push(SSTableIndexEntry {
            offset,
            size,
            first_key,
            last_key,
        });

        self.data_block_entry_count = 0;
        Ok(())
    }

    /// Append an entry (must be in sorted order).
    pub fn append(&mut self, entry: SSTableEntry) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidFormat("SSTable writer already closed".into()));
        }

        if let Some((k, ts, seq)) = &self.last_key {
            let ord = compare_entries(
                (k.as_slice(), *ts, *seq),
                (entry.key.as_slice(), entry.timestamp, entry.sequence),
            );
            if ord != CmpOrdering::Less {
                return Err(Error::InvalidFormat(
                    "SSTable entries must be appended in sorted order".into(),
                ));
            }
        }
        self.last_key = Some((entry.key.clone(), entry.timestamp, entry.sequence));

        if self.config.enable_bloom_filter
            && self
                .last_bloom_key
                .as_ref()
                .is_none_or(|k| k.as_slice() != entry.key.as_slice())
        {
            self.bloom_keys.push(entry.key.clone());
            self.last_bloom_key = Some(entry.key.clone());
        }

        // Estimate entry size in block (uncompressed).
        let val_len = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
        let upcoming = 1 + 8 + 8 + 4 + 4 + entry.key.len() + val_len;
        self.flush_block_if_needed(upcoming)?;
        self.encode_entry_into_block(&entry)?;
        self.data_block_entry_count += 1;
        self.entry_count += 1;
        Ok(())
    }

    /// Finish writing and fsync the SSTable.
    pub fn finish(mut self) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidFormat("SSTable writer already closed".into()));
        }
        self.closed = true;

        // Build Bloom filter now that entry count is known (using buffered keys).
        if self.config.enable_bloom_filter {
            let (k, bytes) = bloom_params(self.entry_count, self.config.bloom_filter_fpr);
            let mut bloom = BloomFilter::new(k, vec![0u8; bytes]);
            for k in &self.bloom_keys {
                bloom.insert(k);
            }
            self.bloom = Some(bloom);
        }

        self.finish_block()?;

        // Write index block.
        let index_offset = self.current_offset()?;
        let index_bytes = encode_index(&self.block_index);
        self.write_bytes(&index_bytes)?;
        let index_size = index_bytes
            .len()
            .try_into()
            .map_err(|_| Error::InvalidFormat("SSTable index size overflow".into()))?;

        // Write bloom filter (optional).
        let bloom_bytes = self.bloom.as_ref().map(BloomFilter::to_bytes);
        let (bloom_offset, bloom_size) = if let Some(bytes) = bloom_bytes {
            let off = self.current_offset()?;
            self.write_bytes(&bytes)?;
            let size = bytes
                .len()
                .try_into()
                .map_err(|_| Error::InvalidFormat("SSTable bloom size overflow".into()))?;
            (off, size)
        } else {
            (0, 0)
        };

        let footer_offset = self.current_offset()?;

        // Patch header at offset 0.
        let mut flags = 0u16;
        if bloom_size > 0 {
            flags |= FLAG_BLOOM_PRESENT;
        }
        let compression_flag = compression_to_flag(self.config.compression) & 0x3;
        flags |= (compression_flag << 1) & FLAG_COMPRESSION_MASK;
        let zstd_level = match self.config.compression {
            CompressionType::Zstd { level } => level,
            _ => 0,
        };
        let zstd_level = zstd_level.clamp(0, 255) as u32;
        let reserved = zstd_level << 8;
        let header = SSTableHeader {
            version: SST_VERSION,
            flags,
            block_count: self.block_index.len() as u32,
            entry_count: self.entry_count,
            min_key_len: if self.entry_count == 0 {
                0
            } else {
                self.min_key_len
            },
            max_key_len: self.max_key_len,
            reserved,
        };
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header.to_bytes())?;

        // Compute CRC over the entire file excluding the footer.
        self.file.seek(SeekFrom::Start(0))?;
        let mut hasher = crc32fast::Hasher::new();
        let mut remaining = footer_offset;
        let mut buf = vec![0u8; 64 * 1024];
        while remaining > 0 {
            let chunk = (remaining as usize).min(buf.len());
            self.file.read_exact(&mut buf[..chunk])?;
            hasher.update(&buf[..chunk]);
            remaining -= chunk as u64;
        }
        let file_crc32 = hasher.finalize();

        let footer = SSTableFooter {
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            min_timestamp: if self.entry_count == 0 {
                0
            } else {
                self.min_timestamp
            },
            max_timestamp: self.max_timestamp,
            file_crc32,
        };

        // Footer is not part of file CRC.
        self.file.seek(SeekFrom::Start(footer_offset))?;
        self.file.write_all(&footer.to_bytes())?;
        self.file.sync_data()?;
        Ok(())
    }
}

/// Reader for SSTable files.
pub struct SSTableReader {
    file: File,
    _path: PathBuf,
    header: SSTableHeader,
    footer: SSTableFooter,
    index: Vec<SSTableIndexEntry>,
    bloom: Option<BloomFilter>,
    compressor: Box<dyn Compressor>,
}

impl SSTableReader {
    /// Open and validate an SSTable file.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < (HEADER_SIZE + FOOTER_SIZE) as u64 {
            return Err(Error::InvalidFormat("SSTable file too small".into()));
        }
        let data_end = file_len - (FOOTER_SIZE as u64);

        let mut header_bytes = [0u8; HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_bytes)?;
        let header = SSTableHeader::from_bytes(&header_bytes)?;

        let mut footer_bytes = [0u8; FOOTER_SIZE];
        file.seek(SeekFrom::Start(file_len - (FOOTER_SIZE as u64)))?;
        file.read_exact(&mut footer_bytes)?;
        let footer = SSTableFooter::from_bytes(&footer_bytes)?;

        let header_bloom_present = (header.flags & FLAG_BLOOM_PRESENT) != 0;
        let footer_bloom_present = footer.bloom_size > 0;
        if header_bloom_present != footer_bloom_present {
            return Err(Error::InvalidFormat(
                "SSTable bloom presence mismatch between header flag and footer size".into(),
            ));
        }

        // Validate footer ranges early to avoid OOM / invalid seeks on corrupted files.
        let index_end = footer
            .index_offset
            .checked_add(footer.index_size as u64)
            .ok_or_else(|| Error::InvalidFormat("SSTable index range overflow".into()))?;
        if footer.index_offset < HEADER_SIZE as u64 || index_end > data_end {
            return Err(Error::InvalidFormat(
                "SSTable index range is out of bounds".into(),
            ));
        }

        let has_bloom = footer_bloom_present;
        let bloom_end = footer
            .bloom_offset
            .checked_add(footer.bloom_size as u64)
            .ok_or_else(|| Error::InvalidFormat("SSTable bloom range overflow".into()))?;
        if has_bloom {
            if footer.bloom_offset < HEADER_SIZE as u64 || bloom_end > data_end {
                return Err(Error::InvalidFormat(
                    "SSTable bloom range is out of bounds".into(),
                ));
            }
            let index_range = (footer.index_offset, index_end);
            let bloom_range = (footer.bloom_offset, bloom_end);
            let overlaps = index_range.0 < bloom_range.1 && bloom_range.0 < index_range.1;
            if overlaps {
                return Err(Error::InvalidFormat(
                    "SSTable index and bloom ranges overlap".into(),
                ));
            }
        }

        // Validate file CRC.
        file.seek(SeekFrom::Start(0))?;
        let mut hasher = crc32fast::Hasher::new();
        let mut remaining = data_end;
        let mut buf = vec![0u8; 64 * 1024];
        while remaining > 0 {
            let chunk = (remaining as usize).min(buf.len());
            file.read_exact(&mut buf[..chunk])?;
            hasher.update(&buf[..chunk]);
            remaining -= chunk as u64;
        }
        let computed = hasher.finalize();
        if computed != footer.file_crc32 {
            return Err(Error::ChecksumMismatch);
        }

        // Decode compression from header flags/reserved.
        let compression_flag = (header.flags & FLAG_COMPRESSION_MASK) >> 1;
        let zstd_level = ((header.reserved >> 8) & 0x00FF) as i32;
        let compression = compression_from_flag(compression_flag, zstd_level)?;
        let compressor = create_compressor(compression_to_v2(compression))
            .map_err(|e| Error::InvalidFormat(format!("SSTable compression unavailable: {e:?}")))?;

        // Load index block.
        file.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_bytes = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut index_bytes)?;
        let index = decode_index(&index_bytes)?;

        // Load bloom filter if present.
        let bloom = if footer_bloom_present {
            file.seek(SeekFrom::Start(footer.bloom_offset))?;
            let mut bloom_bytes = vec![0u8; footer.bloom_size as usize];
            file.read_exact(&mut bloom_bytes)?;
            Some(BloomFilter::from_bytes(&bloom_bytes)?)
        } else {
            None
        };

        Ok(Self {
            file,
            _path: path.to_path_buf(),
            header,
            footer,
            index,
            bloom,
            compressor,
        })
    }

    /// Return the number of entries.
    pub fn entry_count(&self) -> u64 {
        self.header.entry_count
    }

    /// Minimum timestamp stored in this table.
    pub fn min_timestamp(&self) -> u64 {
        self.footer.min_timestamp
    }

    /// Maximum timestamp stored in this table.
    pub fn max_timestamp(&self) -> u64 {
        self.footer.max_timestamp
    }

    /// Get the latest visible entry for the key at `read_timestamp`.
    pub fn get(&mut self, key: &[u8], read_timestamp: u64) -> Result<Option<SSTableEntry>> {
        self.get_cached(None, None, 0, key, read_timestamp)
    }

    /// バッファプールを利用して `get` を行う。
    pub fn get_with_buffer_pool(
        &mut self,
        buffer_pool: &BufferPool,
        metrics: &LsmMetrics,
        file_id: u64,
        key: &[u8],
        read_timestamp: u64,
    ) -> Result<Option<SSTableEntry>> {
        self.get_cached(
            Some(buffer_pool),
            Some(metrics),
            file_id,
            key,
            read_timestamp,
        )
    }

    fn get_cached(
        &mut self,
        buffer_pool: Option<&BufferPool>,
        metrics: Option<&LsmMetrics>,
        file_id: u64,
        key: &[u8],
        read_timestamp: u64,
    ) -> Result<Option<SSTableEntry>> {
        if let Some(bloom) = &self.bloom {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }

        let mut left = 0usize;
        let mut right = self.index.len();
        while left < right {
            let mid = (left + right) / 2;
            if self.index[mid].last_key.as_slice() < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if left >= self.index.len() {
            return Ok(None);
        }
        let mut idx = left;
        while idx < self.index.len() {
            let block_meta = &self.index[idx];
            if block_meta.first_key.as_slice() > key {
                break;
            }
            if key <= block_meta.last_key.as_slice() {
                let block = self.read_block_cached(buffer_pool, metrics, file_id, idx)?;
                let entries = self.decode_block_entries(&block)?;
                if let Some(found) = find_in_entries(&entries, key, read_timestamp) {
                    return Ok(Some(found));
                }
            }
            idx += 1;
        }
        Ok(None)
    }

    fn read_block_cached(
        &mut self,
        buffer_pool: Option<&BufferPool>,
        metrics: Option<&LsmMetrics>,
        file_id: u64,
        idx: usize,
    ) -> Result<Vec<u8>> {
        let entry = self
            .index
            .get(idx)
            .ok_or_else(|| Error::InvalidFormat("SSTable index out of bounds".into()))?;

        if let Some(pool) = buffer_pool {
            let id = BlockId {
                file_id,
                block_offset: entry.offset,
            };
            if let Some(hit) = pool.get(&id) {
                return Ok(hit.as_slice().to_vec());
            }
        }

        self.file.seek(SeekFrom::Start(entry.offset))?;
        let mut bytes = vec![0u8; entry.size as usize];
        self.file.read_exact(&mut bytes)?;
        if let Some(m) = metrics {
            m.add_sstable_read_bytes(bytes.len() as u64);
        }

        if bytes.len() < 4 + 4 {
            return Err(Error::InvalidFormat("SSTable block too small".into()));
        }
        let stored_crc = u32::from_le_bytes(
            bytes[bytes.len() - 4..]
                .try_into()
                .expect("fixed slice length"),
        );
        let computed_crc = crc32(&bytes[..bytes.len() - 4]);
        if stored_crc != computed_crc {
            return Err(Error::ChecksumMismatch);
        }

        if let Some(pool) = buffer_pool {
            let id = BlockId {
                file_id,
                block_offset: entry.offset,
            };
            let _ = pool.put(id, Arc::new(DataBlock::new(bytes.clone())));
        }
        Ok(bytes)
    }

    fn decode_block_entries(&mut self, block: &[u8]) -> Result<Vec<SSTableEntry>> {
        if block.len() < 4 + 4 + 4 {
            return Err(Error::InvalidFormat("SSTable block too small".into()));
        }
        let entry_count =
            u32::from_le_bytes(block[0..4].try_into().expect("fixed slice length")) as usize;
        let uncompressed_len =
            u32::from_le_bytes(block[4..8].try_into().expect("fixed slice length")) as usize;
        let payload = &block[8..block.len() - 4];
        let decompressed = self
            .compressor
            .decompress(payload, uncompressed_len)
            .map_err(|e| Error::InvalidFormat(format!("SSTable decompression failed: {e:?}")))?;
        decode_entries(&decompressed, entry_count)
    }

    /// Scan keys with the given prefix, returning the latest visible version per key.
    ///
    /// Note: tombstones are returned as entries with `value == None`.
    pub fn scan_prefix(&mut self, prefix: &[u8], read_timestamp: u64) -> Result<Vec<SSTableEntry>> {
        self.scan_prefix_cached(None, None, 0, prefix, read_timestamp)
    }

    /// バッファプールを利用して `scan_prefix` を行う。
    pub fn scan_prefix_with_buffer_pool(
        &mut self,
        buffer_pool: &BufferPool,
        metrics: &LsmMetrics,
        file_id: u64,
        prefix: &[u8],
        read_timestamp: u64,
    ) -> Result<Vec<SSTableEntry>> {
        self.scan_prefix_cached(
            Some(buffer_pool),
            Some(metrics),
            file_id,
            prefix,
            read_timestamp,
        )
    }

    fn scan_prefix_cached(
        &mut self,
        buffer_pool: Option<&BufferPool>,
        metrics: Option<&LsmMetrics>,
        file_id: u64,
        prefix: &[u8],
        read_timestamp: u64,
    ) -> Result<Vec<SSTableEntry>> {
        let end = next_prefix(prefix);
        self.scan_range_cached(
            buffer_pool,
            metrics,
            file_id,
            prefix,
            end.as_deref().unwrap_or(&[]),
            read_timestamp,
        )
    }

    /// Scan keys in `[start, end)`, returning the latest visible version per key.
    ///
    /// Note: tombstones are returned as entries with `value == None`.
    pub fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        read_timestamp: u64,
    ) -> Result<Vec<SSTableEntry>> {
        self.scan_range_cached(None, None, 0, start, end, read_timestamp)
    }

    /// バッファプールを利用して `scan_range` を行う。
    pub fn scan_range_with_buffer_pool(
        &mut self,
        buffer_pool: &BufferPool,
        metrics: &LsmMetrics,
        file_id: u64,
        start: &[u8],
        end: &[u8],
        read_timestamp: u64,
    ) -> Result<Vec<SSTableEntry>> {
        self.scan_range_cached(
            Some(buffer_pool),
            Some(metrics),
            file_id,
            start,
            end,
            read_timestamp,
        )
    }

    fn scan_range_cached(
        &mut self,
        buffer_pool: Option<&BufferPool>,
        metrics: Option<&LsmMetrics>,
        file_id: u64,
        start: &[u8],
        end: &[u8],
        read_timestamp: u64,
    ) -> Result<Vec<SSTableEntry>> {
        let mut out = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        for block_idx in 0..self.index.len() {
            let meta = &self.index[block_idx];
            if !end.is_empty() && meta.first_key.as_slice() >= end {
                break;
            }
            if meta.last_key.as_slice() < start {
                continue;
            }
            let block = self.read_block_cached(buffer_pool, metrics, file_id, block_idx)?;
            let entries = self.decode_block_entries(&block)?;
            for e in entries {
                if e.key.as_slice() < start {
                    continue;
                }
                if !end.is_empty() && e.key.as_slice() >= end {
                    return Ok(out);
                }
                if last_user_key.as_deref() == Some(e.key.as_slice()) {
                    continue;
                }
                if e.timestamp > read_timestamp {
                    continue;
                }
                last_user_key = Some(e.key.clone());
                out.push(e);
            }
        }
        Ok(out)
    }
}

fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }
    let mut out = prefix.to_vec();
    for i in (0..out.len()).rev() {
        if out[i] != 0xFF {
            out[i] = out[i].wrapping_add(1);
            out.truncate(i + 1);
            return Some(out);
        }
    }
    None
}

fn decode_entries(bytes: &[u8], expected_count: usize) -> Result<Vec<SSTableEntry>> {
    let mut cursor = 0usize;
    let mut out = Vec::with_capacity(expected_count);
    while cursor < bytes.len() {
        if bytes.len() < cursor + 1 + 8 + 8 + 4 + 4 {
            return Err(Error::InvalidFormat("SSTable entry truncated".into()));
        }
        let entry_type = SSTableEntryType::try_from(bytes[cursor])?;
        cursor += 1;
        let timestamp = u64::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .expect("fixed slice length"),
        );
        cursor += 8;
        let sequence = u64::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .expect("fixed slice length"),
        );
        cursor += 8;
        let key_len = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("fixed slice length"),
        ) as usize;
        cursor += 4;
        let value_len = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("fixed slice length"),
        ) as usize;
        cursor += 4;
        if bytes.len() < cursor + key_len + value_len {
            return Err(Error::InvalidFormat("SSTable entry truncated (kv)".into()));
        }
        let key = bytes[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let value = match entry_type {
            SSTableEntryType::Put => Some(bytes[cursor..cursor + value_len].to_vec()),
            SSTableEntryType::Delete => None,
        };
        cursor += value_len;
        out.push(SSTableEntry {
            key,
            value,
            timestamp,
            sequence,
        });
    }
    if out.len() != expected_count {
        return Err(Error::InvalidFormat("SSTable entry count mismatch".into()));
    }
    Ok(out)
}

fn find_in_entries(
    entries: &[SSTableEntry],
    key: &[u8],
    read_timestamp: u64,
) -> Option<SSTableEntry> {
    for e in entries {
        match e.key.as_slice().cmp(key) {
            CmpOrdering::Less => continue,
            CmpOrdering::Greater => break,
            CmpOrdering::Equal => {
                if e.timestamp <= read_timestamp {
                    return Some(e.clone());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sstable_header_footer_roundtrip() {
        let h = SSTableHeader {
            version: SST_VERSION,
            flags: 3,
            block_count: 10,
            entry_count: 99,
            min_key_len: 1,
            max_key_len: 7,
            reserved: 0xABCD,
        };
        let bytes = h.to_bytes();
        let d = SSTableHeader::from_bytes(&bytes).unwrap();
        assert_eq!(d, h);

        let f = SSTableFooter {
            index_offset: 123,
            index_size: 456,
            bloom_offset: 0,
            bloom_size: 0,
            min_timestamp: 10,
            max_timestamp: 20,
            file_crc32: 0xDEADBEEF,
        };
        let bytes = f.to_bytes();
        let d = SSTableFooter::from_bytes(&bytes).unwrap();
        assert_eq!(d, f);
    }

    #[test]
    fn sstable_writer_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let config = SSTableConfig {
            enable_bloom_filter: false,
            compression: CompressionType::None,
            ..Default::default()
        };
        let writer = SSTableWriter::create(&path, config).unwrap();
        drop(writer);
    }

    #[test]
    fn sstable_writer_reader_roundtrip_get_versions() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t2.sst");
        let config = SSTableConfig {
            enable_bloom_filter: true,
            compression: CompressionType::None,
            block_size: 256,
            ..Default::default()
        };

        let mut w = SSTableWriter::create(&path, config).unwrap();
        w.append(SSTableEntry {
            key: b"a".to_vec(),
            value: Some(b"v2".to_vec()),
            timestamp: 20,
            sequence: 1,
        })
        .unwrap();
        w.append(SSTableEntry {
            key: b"a".to_vec(),
            value: Some(b"v1".to_vec()),
            timestamp: 10,
            sequence: 1,
        })
        .unwrap();
        w.append(SSTableEntry {
            key: b"b".to_vec(),
            value: None,
            timestamp: 15,
            sequence: 1,
        })
        .unwrap();
        w.append(SSTableEntry {
            key: b"c".to_vec(),
            value: Some(Vec::new()),
            timestamp: 7,
            sequence: 1,
        })
        .unwrap();
        w.finish().unwrap();

        let mut r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.get(b"a", 9).unwrap(), None);
        assert_eq!(
            r.get(b"a", 10).unwrap().unwrap().value.unwrap(),
            b"v1".to_vec()
        );
        assert_eq!(
            r.get(b"a", 20).unwrap().unwrap().value.unwrap(),
            b"v2".to_vec()
        );
        assert!(r.get(b"b", 99).unwrap().unwrap().value.is_none());
        assert_eq!(
            r.get(b"c", 99).unwrap().unwrap().value.unwrap(),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn sstable_get_handles_versions_split_across_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t3.sst");
        let config = SSTableConfig {
            enable_bloom_filter: false,
            compression: CompressionType::None,
            block_size: 128,
            ..Default::default()
        };
        let mut w = SSTableWriter::create(&path, config).unwrap();
        w.append(SSTableEntry {
            key: b"k".to_vec(),
            value: Some(vec![0xAA; 64]),
            timestamp: 20,
            sequence: 1,
        })
        .unwrap();
        w.append(SSTableEntry {
            key: b"k".to_vec(),
            value: Some(vec![0xBB; 64]),
            timestamp: 10,
            sequence: 1,
        })
        .unwrap();
        w.finish().unwrap();

        let mut r = SSTableReader::open(&path).unwrap();
        assert_eq!(r.get(b"k", 15).unwrap().unwrap().timestamp, 10);
        assert_eq!(r.get(b"k", 25).unwrap().unwrap().timestamp, 20);
    }

    #[test]
    fn sstable_scan_prefix_returns_latest_visible_per_key_in_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("scan_prefix.sst");
        let config = SSTableConfig {
            enable_bloom_filter: true,
            compression: CompressionType::None,
            block_size: 256,
            ..Default::default()
        };

        {
            let mut w = SSTableWriter::create(&path, config).unwrap();
            w.append(SSTableEntry {
                key: b"p:a".to_vec(),
                value: Some(b"v2".to_vec()),
                timestamp: 20,
                sequence: 1,
            })
            .unwrap();
            w.append(SSTableEntry {
                key: b"p:a".to_vec(),
                value: Some(b"v1".to_vec()),
                timestamp: 10,
                sequence: 1,
            })
            .unwrap();
            w.append(SSTableEntry {
                key: b"p:b".to_vec(),
                value: Some(b"x".to_vec()),
                timestamp: 15,
                sequence: 1,
            })
            .unwrap();
            w.append(SSTableEntry {
                key: b"p:c".to_vec(),
                value: None,
                timestamp: 12,
                sequence: 1,
            })
            .unwrap();
            w.append(SSTableEntry {
                key: b"q:z".to_vec(),
                value: Some(b"no".to_vec()),
                timestamp: 99,
                sequence: 1,
            })
            .unwrap();
            w.finish().unwrap();
        }

        // reopen して scan できること
        let mut r = SSTableReader::open(&path).unwrap();
        let got = r.scan_prefix(b"p:", 20).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].key, b"p:a".to_vec());
        assert_eq!(got[0].value.as_deref(), Some(b"v2".as_slice()));
        assert_eq!(got[1].key, b"p:b".to_vec());
        assert_eq!(got[2].key, b"p:c".to_vec());
        assert!(got[2].value.is_none());

        // read_timestamp により、見える版が変わる
        let got = r.scan_prefix(b"p:", 15).unwrap();
        assert_eq!(got[0].key, b"p:a".to_vec());
        assert_eq!(got[0].value.as_deref(), Some(b"v1".as_slice()));
    }

    #[test]
    fn sstable_scan_range_is_end_exclusive() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("scan_range.sst");
        let config = SSTableConfig {
            enable_bloom_filter: false,
            compression: CompressionType::None,
            block_size: 256,
            ..Default::default()
        };

        let mut w = SSTableWriter::create(&path, config).unwrap();
        w.append(SSTableEntry {
            key: b"a".to_vec(),
            value: Some(b"1".to_vec()),
            timestamp: 10,
            sequence: 1,
        })
        .unwrap();
        w.append(SSTableEntry {
            key: b"b".to_vec(),
            value: Some(b"2".to_vec()),
            timestamp: 10,
            sequence: 1,
        })
        .unwrap();
        w.append(SSTableEntry {
            key: b"c".to_vec(),
            value: Some(b"3".to_vec()),
            timestamp: 10,
            sequence: 1,
        })
        .unwrap();
        w.append(SSTableEntry {
            key: b"d".to_vec(),
            value: Some(b"4".to_vec()),
            timestamp: 10,
            sequence: 1,
        })
        .unwrap();
        w.finish().unwrap();

        let mut r = SSTableReader::open(&path).unwrap();
        let got = r.scan_range(b"b", b"d", 100).unwrap();
        let keys: Vec<_> = got.into_iter().map(|e| e.key).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn next_prefix_handles_all_ff() {
        assert_eq!(next_prefix(b""), None);
        assert_eq!(next_prefix(&[0x00]), Some(vec![0x01]));
        assert_eq!(next_prefix(&[0x01, 0x02]), Some(vec![0x01, 0x03]));
        assert_eq!(next_prefix(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(next_prefix(&[0xFF]), None);
        assert_eq!(next_prefix(&[0xFF, 0xFF]), None);
    }
}
