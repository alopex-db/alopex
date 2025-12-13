//! Write-Ahead Log (WAL) primitives for the LSM file mode.
//!
//! This module defines the on-disk WAL segment header and entry layout used by
//! the circular buffer inside a single `.alopex` file. Writer/reader
//! implementations build on top of these primitives.

use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// WAL file magic ("AWAL").
pub const WAL_MAGIC: [u8; 4] = *b"AWAL";
/// WAL format version (uint16).
pub const WAL_VERSION: u16 = 1;
/// Fixed segment header size (bytes).
pub const WAL_SEGMENT_HEADER_SIZE: usize = 28;
/// WAL section header size (bytes) for circular buffer start/end offsets.
pub const WAL_SECTION_HEADER_SIZE: usize = 16;
/// Fixed overhead for an entry before payload bytes (LSN + length).
pub const WAL_ENTRY_FIXED_HEADER: usize = 8 + 4;

/// Top-level WAL entry type.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEntryType {
    /// Put operation (single).
    Put = 0,
    /// Delete (tombstone) operation (single).
    Delete = 1,
    /// Batched operations encoded in a single entry.
    Batch = 2,
}

impl TryFrom<u8> for WalEntryType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Put),
            1 => Ok(Self::Delete),
            2 => Ok(Self::Batch),
            other => Err(Error::InvalidFormat(format!(
                "unknown WAL entry type: {other}"
            ))),
        }
    }
}

/// Operation type inside a batch payload.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOpType {
    /// Put operation.
    Put = 0,
    /// Delete (tombstone) operation.
    Delete = 1,
}

impl TryFrom<u8> for WalOpType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Put),
            1 => Ok(Self::Delete),
            other => Err(Error::InvalidFormat(format!(
                "unknown WAL batch op type: {other}"
            ))),
        }
    }
}

/// Configuration for WAL circular buffer segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalConfig {
    /// Segment size in bytes (default: 64MB).
    pub segment_size: usize,
    /// Maximum number of segments (default: 8).
    pub max_segments: usize,
    /// Fsync strategy.
    pub sync_mode: SyncMode,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024,
            max_segments: 8,
            sync_mode: SyncMode::EveryWrite,
        }
    }
}

/// Sync policy for WAL writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMode {
    /// fsync on every append (safest).
    EveryWrite,
    /// fsync periodically based on batch size or timeout.
    ///
    /// Note: this mode can leave the WAL's section/segment headers temporarily stale after a crash.
    /// Recovery must validate headers and fall back safely on inconsistencies.
    BatchSync {
        /// Max bytes to buffer before fsync.
        max_batch_size: usize,
        /// Max wait time (ms) before forcing fsync.
        max_wait_ms: u64,
    },
    /// Rely on OS buffering (fastest, least safe).
    ///
    /// Note: after a crash, recent appends and/or header updates may be missing.
    NoSync,
}

/// Fixed-size WAL segment header (28 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSegmentHeader {
    /// Format version.
    pub version: u16,
    /// Segment identifier.
    pub segment_id: u64,
    /// First LSN stored in this segment (including fragments).
    ///
    /// Entries may cross segment boundaries, so the data region may begin with a fragment of an
    /// entry with this LSN. Do not assume decoding can start at the segment's first byte.
    pub first_lsn: u64,
    /// CRC32 for bytes [0..22).
    pub crc32: u32,
    /// Reserved field (kept for alignment/forward-compat).
    pub reserved: u16,
}

impl WalSegmentHeader {
    /// Create a new header with computed CRC.
    pub fn new(segment_id: u64, first_lsn: u64) -> Self {
        let crc32 = compute_crc(WAL_VERSION, segment_id, first_lsn);
        Self {
            version: WAL_VERSION,
            segment_id,
            first_lsn,
            crc32,
            reserved: 0,
        }
    }

    /// Serialize the header to fixed-size bytes (28B).
    pub fn to_bytes(&self) -> [u8; WAL_SEGMENT_HEADER_SIZE] {
        let mut buf = [0u8; WAL_SEGMENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&WAL_MAGIC);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..14].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[14..22].copy_from_slice(&self.first_lsn.to_le_bytes());
        buf[22..26].copy_from_slice(&self.crc32.to_le_bytes());
        buf[26..28].copy_from_slice(&self.reserved.to_le_bytes());
        buf
    }

    /// Deserialize and validate a header.
    pub fn from_bytes(bytes: &[u8; WAL_SEGMENT_HEADER_SIZE]) -> Result<Self> {
        if bytes[0..4] != WAL_MAGIC {
            return Err(Error::InvalidFormat("WAL magic mismatch".into()));
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != WAL_VERSION {
            return Err(Error::InvalidFormat(format!(
                "unsupported WAL version: {version}"
            )));
        }

        let segment_id = u64::from_le_bytes(bytes[6..14].try_into().expect("fixed slice length"));
        let first_lsn = u64::from_le_bytes(bytes[14..22].try_into().expect("fixed slice length"));
        let stored_crc = u32::from_le_bytes(bytes[22..26].try_into().expect("fixed slice length"));
        let reserved = u16::from_le_bytes(bytes[26..28].try_into().expect("fixed slice length"));

        let header = Self {
            version,
            segment_id,
            first_lsn,
            crc32: stored_crc,
            reserved,
        };

        let computed = header.compute_crc();
        if computed != stored_crc {
            return Err(Error::ChecksumMismatch);
        }

        Ok(header)
    }

    fn compute_crc(&self) -> u32 {
        compute_crc(self.version, self.segment_id, self.first_lsn)
    }
}

/// A single batch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBatchOp {
    /// Operation type.
    pub op_type: WalOpType,
    /// Key bytes.
    pub key: Vec<u8>,
    /// Value bytes for Put (may be empty). None for Delete.
    pub value: Option<Vec<u8>>,
}

impl WalBatchOp {
    fn encoded_len(&self) -> usize {
        let val_len = self.value.as_ref().map(|v| v.len()).unwrap_or(0);
        1 + varint_len(self.key.len() as u64)
            + self.key.len()
            + varint_len(val_len as u64)
            + val_len
    }
}

/// WAL entry payload variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalEntryPayload {
    /// Single Put entry.
    Put {
        /// Key bytes.
        key: Vec<u8>,
        /// Value bytes (may be empty).
        value: Vec<u8>,
    },
    /// Single Delete entry.
    Delete {
        /// Key bytes.
        key: Vec<u8>,
    },
    /// Batched Put/Delete operations.
    Batch(Vec<WalBatchOp>),
}

/// WAL entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    /// Monotonic log sequence number.
    pub lsn: u64,
    /// Entry payload.
    pub payload: WalEntryPayload,
}

impl WalEntry {
    /// Construct a Put entry (value may be empty).
    pub fn put(lsn: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            lsn,
            payload: WalEntryPayload::Put { key, value },
        }
    }

    /// Construct a Delete entry.
    pub fn delete(lsn: u64, key: Vec<u8>) -> Self {
        Self {
            lsn,
            payload: WalEntryPayload::Delete { key },
        }
    }

    /// Construct a Batch entry.
    pub fn batch(lsn: u64, operations: Vec<WalBatchOp>) -> Self {
        Self {
            lsn,
            payload: WalEntryPayload::Batch(operations),
        }
    }

    /// Total encoded length including header and CRC.
    pub fn encoded_len(&self) -> usize {
        let body_len = match &self.payload {
            WalEntryPayload::Put { key, value } => {
                1 + varint_len(key.len() as u64)
                    + key.len()
                    + varint_len(value.len() as u64)
                    + value.len()
            }
            WalEntryPayload::Delete { key } => {
                1 + varint_len(key.len() as u64) + key.len() + varint_len(0)
            }
            WalEntryPayload::Batch(ops) => {
                1 + varint_len(ops.len() as u64)
                    + ops.iter().map(WalBatchOp::encoded_len).sum::<usize>()
            }
        };
        WAL_ENTRY_FIXED_HEADER + body_len + 4 // + CRC32
    }

    /// Encode the entry to bytes.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut body = Vec::with_capacity(self.encoded_len() - WAL_ENTRY_FIXED_HEADER - 4);
        match &self.payload {
            WalEntryPayload::Put { key, value } => {
                body.push(WalEntryType::Put as u8);
                encode_varint(key.len() as u64, &mut body);
                body.extend_from_slice(key);
                encode_varint(value.len() as u64, &mut body);
                body.extend_from_slice(value);
            }
            WalEntryPayload::Delete { key } => {
                body.push(WalEntryType::Delete as u8);
                encode_varint(key.len() as u64, &mut body);
                body.extend_from_slice(key);
                encode_varint(0, &mut body);
            }
            WalEntryPayload::Batch(ops) => {
                body.push(WalEntryType::Batch as u8);
                encode_varint(ops.len() as u64, &mut body);
                for op in ops {
                    body.push(op.op_type as u8);
                    encode_varint(op.key.len() as u64, &mut body);
                    body.extend_from_slice(&op.key);
                    let val_len = op.value.as_ref().map(|v| v.len()).unwrap_or(0);
                    encode_varint(val_len as u64, &mut body);
                    if let Some(value) = &op.value {
                        body.extend_from_slice(value);
                    }
                }
            }
        }

        let payload_len = body.len();
        let total_len_field = payload_len
            .checked_add(4)
            .ok_or_else(|| Error::InvalidFormat("WAL entry too large".into()))?;
        if total_len_field > u32::MAX as usize {
            return Err(Error::InvalidFormat("WAL entry too large".into()));
        }

        let mut out = Vec::with_capacity(WAL_ENTRY_FIXED_HEADER + payload_len + 4);
        out.extend_from_slice(&self.lsn.to_le_bytes());
        out.extend_from_slice(&(total_len_field as u32).to_le_bytes());
        out.extend_from_slice(&body);

        // CRC over the entire entry except the CRC field itself (header + body).
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        Ok(out)
    }

    /// Decode a single entry from the provided buffer, returning the entry and bytes consumed.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < WAL_ENTRY_FIXED_HEADER {
            return Err(Error::InvalidFormat(
                "buffer too small for WAL entry header".into(),
            ));
        }

        let lsn = u64::from_le_bytes(buf[0..8].try_into().expect("fixed slice length"));
        let payload_and_crc_len =
            u32::from_le_bytes(buf[8..12].try_into().expect("fixed slice length")) as usize;
        let total_len = WAL_ENTRY_FIXED_HEADER + payload_and_crc_len;
        if buf.len() < total_len {
            return Err(Error::InvalidFormat(
                "buffer truncated for WAL entry payload".into(),
            ));
        }
        if payload_and_crc_len < 1 + 4 {
            return Err(Error::InvalidFormat("WAL entry payload too small".into()));
        }

        let body_len = payload_and_crc_len - 4;
        let body = &buf[WAL_ENTRY_FIXED_HEADER..WAL_ENTRY_FIXED_HEADER + body_len];
        let stored_crc = u32::from_le_bytes(
            buf[WAL_ENTRY_FIXED_HEADER + body_len..total_len]
                .try_into()
                .expect("fixed slice length"),
        );
        let computed_crc = crc32fast::hash(&buf[..WAL_ENTRY_FIXED_HEADER + body_len]);
        if stored_crc != computed_crc {
            return Err(Error::ChecksumMismatch);
        }

        let entry_type = WalEntryType::try_from(body[0])?;
        let mut cursor = 1;

        let payload = match entry_type {
            WalEntryType::Put => {
                let (key_len, key_len_bytes) = decode_varint(&body[cursor..])?;
                cursor += key_len_bytes;
                let key_len = key_len as usize;
                if body_len < cursor + key_len {
                    return Err(Error::InvalidFormat("WAL entry truncated (key)".into()));
                }
                let key = body[cursor..cursor + key_len].to_vec();
                cursor += key_len;

                let (val_len, val_len_bytes) = decode_varint(&body[cursor..])?;
                cursor += val_len_bytes;
                let val_len = val_len as usize;
                if body_len < cursor + val_len {
                    return Err(Error::InvalidFormat("WAL entry truncated (value)".into()));
                }
                let value = body[cursor..cursor + val_len].to_vec();
                cursor += val_len;

                if cursor != body_len {
                    return Err(Error::InvalidFormat(
                        "WAL entry has trailing bytes after Put".into(),
                    ));
                }

                WalEntryPayload::Put { key, value }
            }
            WalEntryType::Delete => {
                let (key_len, key_len_bytes) = decode_varint(&body[cursor..])?;
                cursor += key_len_bytes;
                let key_len = key_len as usize;
                if body_len < cursor + key_len {
                    return Err(Error::InvalidFormat("WAL entry truncated (key)".into()));
                }
                let key = body[cursor..cursor + key_len].to_vec();
                cursor += key_len;

                let (val_len, val_len_bytes) = decode_varint(&body[cursor..])?;
                cursor += val_len_bytes;
                if val_len != 0 {
                    return Err(Error::InvalidFormat(
                        "delete entry must have zero-length value".into(),
                    ));
                }
                if cursor != body_len {
                    return Err(Error::InvalidFormat(
                        "WAL entry has trailing bytes after Delete".into(),
                    ));
                }
                WalEntryPayload::Delete { key }
            }
            WalEntryType::Batch => {
                let (op_count, op_count_bytes) = decode_varint(&body[cursor..])?;
                cursor += op_count_bytes;
                let op_count = op_count as usize;
                let mut ops = Vec::with_capacity(op_count);
                for _ in 0..op_count {
                    if cursor >= body_len {
                        return Err(Error::InvalidFormat(
                            "WAL batch truncated before op type".into(),
                        ));
                    }
                    let op_type = WalOpType::try_from(body[cursor])?;
                    cursor += 1;

                    let (key_len, key_len_bytes) = decode_varint(&body[cursor..])?;
                    cursor += key_len_bytes;
                    let key_len = key_len as usize;
                    if body_len < cursor + key_len {
                        return Err(Error::InvalidFormat("WAL batch truncated (key)".into()));
                    }
                    let key = body[cursor..cursor + key_len].to_vec();
                    cursor += key_len;

                    let (val_len, val_len_bytes) = decode_varint(&body[cursor..])?;
                    cursor += val_len_bytes;
                    let val_len = val_len as usize;
                    if body_len < cursor + val_len {
                        return Err(Error::InvalidFormat("WAL batch truncated (value)".into()));
                    }
                    let value = if op_type == WalOpType::Delete {
                        if val_len != 0 {
                            return Err(Error::InvalidFormat(
                                "batch delete must have zero-length value".into(),
                            ));
                        }
                        None
                    } else {
                        Some(body[cursor..cursor + val_len].to_vec())
                    };
                    cursor += val_len;

                    ops.push(WalBatchOp {
                        op_type,
                        key,
                        value,
                    });
                }

                if cursor != body_len {
                    return Err(Error::InvalidFormat(
                        "WAL batch has trailing unparsed bytes".into(),
                    ));
                }

                WalEntryPayload::Batch(ops)
            }
        };

        Ok((Self { lsn, payload }, total_len))
    }
}

fn encode_varint(mut n: u64, buf: &mut Vec<u8>) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

fn decode_varint(data: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        let bits = (byte & 0x7F) as u64;
        result |= bits << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(Error::InvalidFormat("varint overflow".into()));
        }
    }
    Err(Error::InvalidFormat("varint truncated".into()))
}

fn varint_len(mut n: u64) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

fn compute_crc(version: u16, segment_id: u64, first_lsn: u64) -> u32 {
    let mut buf = [0u8; WAL_SEGMENT_HEADER_SIZE - 6]; // up to CRC field (22 bytes)
    buf[0..4].copy_from_slice(&WAL_MAGIC);
    buf[4..6].copy_from_slice(&version.to_le_bytes());
    buf[6..14].copy_from_slice(&segment_id.to_le_bytes());
    buf[14..22].copy_from_slice(&first_lsn.to_le_bytes());
    crc32fast::hash(&buf)
}

fn ring_distance(start: u64, end: u64, len: u64) -> u64 {
    if start <= end {
        end - start
    } else {
        len - (start - end)
    }
}

fn compute_ring_layout(config: &WalConfig) -> Result<(u64, u64, u64, u64)> {
    if config.max_segments == 0 {
        return Err(Error::InvalidFormat("max_segments must be >= 1".into()));
    }
    let segment_size = config.segment_size as u64;
    let segment_header_bytes = WAL_SEGMENT_HEADER_SIZE as u64;
    if segment_size <= segment_header_bytes {
        return Err(Error::InvalidFormat(
            "WAL segment size too small for segment header".into(),
        ));
    }
    let segment_data_len = segment_size - segment_header_bytes;
    let max_segments = config.max_segments as u64;
    let ring_len = segment_data_len
        .checked_mul(max_segments)
        .ok_or_else(|| Error::InvalidFormat("WAL ring length overflow".into()))?;
    if ring_len > WalSectionHeader::OFFSET_MASK {
        return Err(Error::InvalidFormat(
            "WAL ring length exceeds offset encoding capacity".into(),
        ));
    }
    Ok((segment_size, segment_data_len, max_segments, ring_len))
}

fn segment_header_offset(segment_size: u64, segment_index: u64) -> u64 {
    (WAL_SECTION_HEADER_SIZE as u64) + (segment_index * segment_size)
}

fn read_segment_header(
    file: &mut File,
    segment_size: u64,
    segment_index: u64,
) -> Result<WalSegmentHeader> {
    let off = segment_header_offset(segment_size, segment_index);
    file.seek(SeekFrom::Start(off))?;
    let mut bytes = [0u8; WAL_SEGMENT_HEADER_SIZE];
    file.read_exact(&mut bytes)?;
    WalSegmentHeader::from_bytes(&bytes)
}

fn write_segment_header(
    file: &mut File,
    segment_size: u64,
    segment_index: u64,
    header: &WalSegmentHeader,
) -> Result<()> {
    let off = segment_header_offset(segment_size, segment_index);
    file.seek(SeekFrom::Start(off))?;
    file.write_all(&header.to_bytes())?;
    Ok(())
}

fn ring_logical_to_physical(
    logical_offset: u64,
    segment_size: u64,
    segment_data_len: u64,
) -> Result<u64> {
    if segment_data_len == 0 {
        return Err(Error::InvalidFormat("segment data length is zero".into()));
    }
    let segment_index = logical_offset / segment_data_len;
    let offset_in_segment = logical_offset % segment_data_len;
    Ok(segment_header_offset(segment_size, segment_index)
        + (WAL_SEGMENT_HEADER_SIZE as u64)
        + offset_in_segment)
}

fn read_ring_bytes(
    file: &mut File,
    mut logical_offset: u64,
    len: usize,
    segment_size: u64,
    segment_data_len: u64,
    ring_len: u64,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let offset_in_segment = logical_offset % segment_data_len;
        let remaining_in_segment = (segment_data_len - offset_in_segment) as usize;
        let chunk_len = remaining_in_segment.min(len - out.len());
        let phys = ring_logical_to_physical(logical_offset, segment_size, segment_data_len)?;
        file.seek(SeekFrom::Start(phys))?;
        let mut buf = vec![0u8; chunk_len];
        file.read_exact(&mut buf)?;
        out.extend_from_slice(&buf);
        logical_offset = (logical_offset + (chunk_len as u64)) % ring_len;
    }
    Ok(out)
}

impl WalWriter {
    fn write_ring(
        &mut self,
        mut logical_offset: u64,
        mut data: &[u8],
        entry_lsn: u64,
    ) -> Result<()> {
        while !data.is_empty() {
            let offset_in_segment = logical_offset % self.segment_data_len;
            if offset_in_segment == 0 {
                let segment_index = logical_offset / self.segment_data_len;
                let header = WalSegmentHeader::new(self.segment_id_base + segment_index, entry_lsn);
                write_segment_header(&mut self.file, self.segment_size, segment_index, &header)?;
            }
            let remaining_in_segment = (self.segment_data_len - offset_in_segment) as usize;
            let chunk_len = remaining_in_segment.min(data.len());

            let phys =
                ring_logical_to_physical(logical_offset, self.segment_size, self.segment_data_len)?;
            self.file.seek(SeekFrom::Start(phys))?;
            self.file.write_all(&data[..chunk_len])?;

            logical_offset = (logical_offset + (chunk_len as u64)) % self.ring_len;
            data = &data[chunk_len..];
        }
        Ok(())
    }
}

fn persist_section_header(file: &mut File, offset: u64, header: &WalSectionHeader) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&header.to_bytes())?;
    Ok(())
}

fn load_section_header(file: &mut File, offset: u64) -> Result<WalSectionHeader> {
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; WAL_SECTION_HEADER_SIZE];
    file.read_exact(&mut bytes)?;
    Ok(WalSectionHeader::from_bytes(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    use tempfile::tempdir;

    #[test]
    fn segment_header_roundtrip() {
        let header = WalSegmentHeader::new(42, 100);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), WAL_SEGMENT_HEADER_SIZE);
        let decoded = WalSegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(header.segment_id, decoded.segment_id);
        assert_eq!(header.first_lsn, decoded.first_lsn);
        assert_eq!(header.version, decoded.version);
    }

    #[test]
    fn segment_header_crc_mismatch() {
        let mut header = WalSegmentHeader::new(1, 1).to_bytes();
        header[0] ^= 0xFF; // break magic
        let err = WalSegmentHeader::from_bytes(&header).unwrap_err();
        matches!(err, Error::InvalidFormat(_));
    }

    #[test]
    fn wal_entry_encode_decode_put() {
        let entry = WalEntry::put(10, b"key".to_vec(), b"value".to_vec());
        let encoded = entry.encode().unwrap();
        let (decoded, consumed) = WalEntry::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, entry);
    }

    #[test]
    fn wal_entry_encode_decode_delete() {
        let entry = WalEntry::delete(11, b"gone".to_vec());
        let encoded = entry.encode().unwrap();
        let (decoded, consumed) = WalEntry::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, entry);
    }

    #[test]
    fn wal_entry_crc_detects_corruption() {
        let entry = WalEntry::put(12, b"k".to_vec(), b"v".to_vec());
        let mut encoded = entry.encode().unwrap();
        *encoded.last_mut().unwrap() ^= 0x10;
        let err = WalEntry::decode(&encoded).unwrap_err();
        matches!(err, Error::ChecksumMismatch);
    }

    #[test]
    fn varint_helpers() {
        let values = [
            0u64,
            1,
            127,
            128,
            16384,
            u32::MAX as u64,
            u64::from(u32::MAX) + 1,
        ];
        for &v in &values {
            let mut buf = Vec::new();
            encode_varint(v, &mut buf);
            let (decoded, read) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(read, buf.len());
            assert_eq!(buf.len(), varint_len(v));
        }
    }

    #[test]
    fn wal_entry_crc_covers_header() {
        let entry = WalEntry::put(20, b"key".to_vec(), b"value".to_vec());
        let mut encoded = entry.encode().unwrap();
        encoded[0] ^= 0xFF; // corrupt LSN byte
        let err = WalEntry::decode(&encoded).unwrap_err();
        matches!(err, Error::ChecksumMismatch);
    }

    #[test]
    fn wal_entry_retains_empty_value_put() {
        let entry = WalEntry::put(30, b"key".to_vec(), Vec::new());
        let encoded = entry.encode().unwrap();
        let (decoded, _) = WalEntry::decode(&encoded).unwrap();
        matches!(decoded.payload, WalEntryPayload::Put { .. });
        if let WalEntryPayload::Put { value, .. } = decoded.payload {
            assert_eq!(value.len(), 0);
        } else {
            panic!("expected Put payload");
        }
    }

    #[test]
    fn wal_batch_roundtrip() {
        let ops = vec![
            WalBatchOp {
                op_type: WalOpType::Put,
                key: b"a".to_vec(),
                value: Some(b"1".to_vec()),
            },
            WalBatchOp {
                op_type: WalOpType::Delete,
                key: b"b".to_vec(),
                value: None,
            },
        ];
        let entry = WalEntry::batch(40, ops.clone());
        let encoded = entry.encode().unwrap();
        let (decoded, consumed) = WalEntry::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.payload, WalEntryPayload::Batch(ops));
    }

    #[test]
    fn wal_section_header_roundtrip() {
        let header = WalSectionHeader {
            start_offset: 128,
            end_offset: 4096,
            is_full: false,
        };
        let bytes = header.to_bytes();
        let decoded = WalSectionHeader::from_bytes(&bytes);
        assert_eq!(decoded, header);
    }

    #[test]
    fn wal_writer_appends_and_updates_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let config = WalConfig {
            segment_size: 4096,
            max_segments: 1,
            ..Default::default()
        };
        let mut writer = WalWriter::create(&path, config, 1, 100).unwrap();
        let entry = WalEntry::put(100, b"key".to_vec(), b"value".to_vec());
        let encoded_len = entry.encode().unwrap().len() as u64;

        let offset = writer.append(&entry).unwrap();
        assert_eq!(
            offset,
            (WAL_SECTION_HEADER_SIZE + WAL_SEGMENT_HEADER_SIZE) as u64
        );

        let mut file = File::open(&path).unwrap();
        let mut hdr = [0u8; WAL_SECTION_HEADER_SIZE];
        file.read_exact(&mut hdr).unwrap();
        let section = WalSectionHeader::from_bytes(&hdr);
        assert_eq!(section.start_offset, 0);
        assert_eq!(section.end_offset, encoded_len);
        assert!(!section.is_full);

        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut buf = vec![0u8; encoded_len as usize];
        file.read_exact(&mut buf).unwrap();
        let (decoded, consumed) = WalEntry::decode(&buf).unwrap();
        assert_eq!(consumed, encoded_len as usize);
        assert_eq!(decoded, entry);
    }

    #[test]
    fn wal_writer_wraps_when_full() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_wrap");
        let entry = WalEntry::put(1, b"a".to_vec(), b"1".to_vec());
        let entry_len = entry.encode().unwrap().len() as u64;
        let header_bytes = (WAL_SECTION_HEADER_SIZE + WAL_SEGMENT_HEADER_SIZE) as u64;
        let ring_len = (entry_len + (entry_len / 2)).max(entry_len + 1);
        let segment_size = (WAL_SEGMENT_HEADER_SIZE as u64) + ring_len;
        let config = WalConfig {
            segment_size: segment_size as usize,
            max_segments: 1,
            ..Default::default()
        };

        let mut writer = WalWriter::create(&path, config, 2, 10).unwrap();
        let first_offset = writer.append(&entry).unwrap();
        assert_eq!(first_offset, header_bytes);

        // Simulate checkpoint freeing the first entry.
        writer.advance_start(entry_len).unwrap();

        let second_offset = writer.append(&entry).unwrap();
        assert_eq!(second_offset, header_bytes);

        let mut file = File::open(&path).unwrap();
        let mut hdr = [0u8; WAL_SECTION_HEADER_SIZE];
        file.read_exact(&mut hdr).unwrap();
        let section = WalSectionHeader::from_bytes(&hdr);
        assert_eq!(section.end_offset, entry_len);
        assert!(!section.is_full);

        file.seek(SeekFrom::Start(second_offset)).unwrap();
        let mut buf = vec![0u8; entry_len as usize];
        file.read_exact(&mut buf).unwrap();
        let (decoded, consumed) = WalEntry::decode(&buf).unwrap();
        assert_eq!(consumed as u64, entry_len);
        assert_eq!(decoded, entry);
    }

    #[test]
    fn wal_writer_refuses_overwrite_without_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_no_overwrite");
        let entry = WalEntry::put(1, b"a".to_vec(), b"1".to_vec());
        let entry_len = entry.encode().unwrap().len() as u64;
        let ring_len = entry_len + (entry_len / 2);
        let segment_size = (WAL_SEGMENT_HEADER_SIZE as u64) + ring_len;
        let config = WalConfig {
            segment_size: segment_size as usize,
            max_segments: 1,
            ..Default::default()
        };
        let mut writer = WalWriter::create(&path, config, 2, 10).unwrap();
        writer.append(&entry).unwrap();
        assert!(writer.append(&entry).is_err());
    }

    #[test]
    fn wal_writer_advances_start_and_reclaims_space() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_reclaim");
        let entry = WalEntry::put(1, b"k".to_vec(), b"v".to_vec());
        let entry_len = entry.encode().unwrap().len() as u64;
        let header_bytes = (WAL_SECTION_HEADER_SIZE + WAL_SEGMENT_HEADER_SIZE) as u64;
        let ring_len = entry_len * 2;
        let segment_size = (WAL_SEGMENT_HEADER_SIZE as u64) + ring_len;
        let config = WalConfig {
            segment_size: segment_size as usize,
            max_segments: 1,
            ..Default::default()
        };
        let mut writer = WalWriter::create(&path, config, 5, 50).unwrap();

        writer.append(&entry).unwrap();
        writer.advance_start(entry_len).unwrap();

        let second = writer.append(&entry).unwrap();
        assert_eq!(second, header_bytes);
    }

    #[test]
    fn wal_section_header_can_represent_full_buffer() {
        let header = WalSectionHeader {
            start_offset: 0,
            end_offset: 0,
            is_full: true,
        };
        let bytes = header.to_bytes();
        let decoded = WalSectionHeader::from_bytes(&bytes);
        assert_eq!(decoded, header);
    }

    #[test]
    fn wal_writer_persists_full_state_and_open_respects_it() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_full");
        let entry = WalEntry::put(1, b"k".to_vec(), vec![0; 32]);
        let entry_len = entry.encode().unwrap().len() as u64;
        let segment_size = (WAL_SEGMENT_HEADER_SIZE as u64) + entry_len;
        let config = WalConfig {
            segment_size: segment_size as usize,
            max_segments: 1,
            ..Default::default()
        };

        let mut writer = WalWriter::create(&path, config.clone(), 7, 1).unwrap();
        writer.append(&entry).unwrap();

        let mut file = File::open(&path).unwrap();
        let mut hdr = [0u8; WAL_SECTION_HEADER_SIZE];
        file.read_exact(&mut hdr).unwrap();
        let section = WalSectionHeader::from_bytes(&hdr);
        assert!(section.is_full);
        assert_eq!(section.start_offset, 0);
        assert_eq!(section.end_offset, 0);

        let mut reopened = WalWriter::open(&path, config).unwrap();
        assert!(reopened
            .append(&WalEntry::put(2, b"x".to_vec(), b"y".to_vec()))
            .is_err());
    }

    #[test]
    fn wal_writer_multi_segment_entry_crosses_boundary_and_is_readable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_multi");
        let entry = WalEntry::put(10, b"k".to_vec(), vec![0xAB; 64]);
        let encoded = entry.encode().unwrap();
        let segment_data_len = (encoded.len() - 1) as u64; // force boundary crossing
        let config = WalConfig {
            segment_size: (WAL_SEGMENT_HEADER_SIZE as u64 + segment_data_len) as usize,
            max_segments: 2,
            ..Default::default()
        };

        let mut writer = WalWriter::create(&path, config.clone(), 1000, entry.lsn).unwrap();
        let start_physical = writer.append(&entry).unwrap();
        assert_eq!(
            start_physical,
            (WAL_SECTION_HEADER_SIZE + WAL_SEGMENT_HEADER_SIZE) as u64
        );

        // Re-open and validate segment headers + ring contents.
        let _reopened = WalWriter::open(&path, config.clone()).unwrap();

        fn read_ring_bytes(
            file: &mut File,
            mut logical_offset: u64,
            len: usize,
            segment_size: u64,
            segment_data_len: u64,
            ring_len: u64,
        ) -> Vec<u8> {
            let mut out = Vec::with_capacity(len);
            while out.len() < len {
                let offset_in_segment = logical_offset % segment_data_len;
                let remaining_in_segment = (segment_data_len - offset_in_segment) as usize;
                let chunk_len = remaining_in_segment.min(len - out.len());
                let phys = ring_logical_to_physical(logical_offset, segment_size, segment_data_len)
                    .unwrap();
                file.seek(SeekFrom::Start(phys)).unwrap();
                let mut buf = vec![0u8; chunk_len];
                file.read_exact(&mut buf).unwrap();
                out.extend_from_slice(&buf);
                logical_offset = (logical_offset + chunk_len as u64) % ring_len;
            }
            out
        }

        let mut file = File::open(&path).unwrap();
        let (segment_size, segment_data_len, _max_segments, ring_len) =
            compute_ring_layout(&config).unwrap();

        // First two segment headers should have been initialized/updated with the base id.
        let h0 = read_segment_header(&mut file, segment_size, 0).unwrap();
        let h1 = read_segment_header(&mut file, segment_size, 1).unwrap();
        assert_eq!(h0.segment_id, 1000);
        assert_eq!(h1.segment_id, 1001);
        assert_eq!(h0.first_lsn, entry.lsn);
        assert_eq!(h1.first_lsn, entry.lsn);

        let bytes = read_ring_bytes(
            &mut file,
            0,
            encoded.len(),
            segment_size,
            segment_data_len,
            ring_len,
        );
        let (decoded, consumed) = WalEntry::decode(&bytes).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, entry);
    }
}

/// WAL section header for circular buffer bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSectionHeader {
    /// Read pointer offset (inclusive), as a logical ring offset.
    pub start_offset: u64,
    /// Write pointer offset (exclusive), as a logical ring offset.
    pub end_offset: u64,
    /// Whether the buffer is full (disambiguates `start_offset == end_offset`).
    ///
    /// This is persisted by storing a flag in the MSB of the serialized `end_offset`. Therefore,
    /// the ring length must stay within [`WalSectionHeader::OFFSET_MASK`].
    pub is_full: bool,
}

impl WalSectionHeader {
    const FULL_FLAG: u64 = 1u64 << 63;
    const OFFSET_MASK: u64 = !Self::FULL_FLAG;

    /// Serialize to 16 bytes (start/end offsets).
    pub fn to_bytes(&self) -> [u8; WAL_SECTION_HEADER_SIZE] {
        let mut buf = [0u8; WAL_SECTION_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.start_offset.to_le_bytes());
        let mut end = self.end_offset & Self::OFFSET_MASK;
        if self.is_full {
            end |= Self::FULL_FLAG;
        }
        buf[8..16].copy_from_slice(&end.to_le_bytes());
        buf
    }

    /// Deserialize from 16 bytes.
    pub fn from_bytes(bytes: &[u8; WAL_SECTION_HEADER_SIZE]) -> Self {
        let start_offset = u64::from_le_bytes(bytes[0..8].try_into().expect("fixed slice length"));
        let raw_end = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed slice length"));
        let is_full = (raw_end & Self::FULL_FLAG) != 0;
        let end_offset = raw_end & Self::OFFSET_MASK;
        Self {
            start_offset,
            end_offset,
            is_full,
        }
    }
}

/// WAL writer for circular buffer sections.
#[derive(Debug)]
pub struct WalWriter {
    file: File,
    config: WalConfig,
    section_header: WalSectionHeader,
    segment_id_base: u64,
    segment_size: u64,
    segment_data_len: u64,
    ring_len: u64,
    /// Bytes currently used in the buffer.
    used_bytes: u64,
    /// Bytes written since last fsync (BatchSync mode).
    pending_sync: usize,
    /// Timestamp of last fsync (BatchSync mode).
    last_sync: Instant,
}

impl WalWriter {
    /// Create a new WAL writer and initialize section + segment headers.
    ///
    /// The WAL section size is `segment_size * max_segments` and each segment starts with a
    /// [`WalSegmentHeader`], followed by the segment's data region.
    ///
    /// `segment_id_base` is used to assign stable per-segment IDs as
    /// `segment_id_base + segment_index`.
    pub fn create(
        path: &Path,
        config: WalConfig,
        segment_id_base: u64,
        first_lsn: u64,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let (segment_size, segment_data_len, max_segments, ring_len) =
            compute_ring_layout(&config)?;
        let wal_section_size = (WAL_SECTION_HEADER_SIZE as u64)
            .checked_add(
                segment_size
                    .checked_mul(max_segments)
                    .ok_or_else(|| Error::InvalidFormat("WAL section size overflow".into()))?,
            )
            .ok_or_else(|| Error::InvalidFormat("WAL section size overflow".into()))?;
        file.set_len(wal_section_size)?;

        let section_header = WalSectionHeader {
            start_offset: 0,
            end_offset: 0,
            is_full: false,
        };
        persist_section_header(&mut file, 0, &section_header)?;

        for segment_index in 0..max_segments {
            let header_first_lsn = if segment_index == 0 { first_lsn } else { 0 };
            let header = WalSegmentHeader::new(segment_id_base + segment_index, header_first_lsn);
            write_segment_header(&mut file, segment_size, segment_index, &header)?;
        }
        file.sync_data()?;

        Ok(Self {
            file,
            config,
            section_header,
            segment_id_base,
            segment_size,
            segment_data_len,
            ring_len,
            used_bytes: 0,
            pending_sync: 0,
            last_sync: Instant::now(),
        })
    }

    /// Open an existing WAL writer, resuming from the stored end offset.
    pub fn open(path: &Path, config: WalConfig) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let (segment_size, segment_data_len, max_segments, ring_len) =
            compute_ring_layout(&config)?;

        let mut header_bytes = [0u8; WAL_SECTION_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let section_header = WalSectionHeader::from_bytes(&header_bytes);

        if section_header.start_offset >= ring_len {
            return Err(Error::InvalidFormat(
                "WAL start offset exceeds ring length".into(),
            ));
        }
        if section_header.end_offset >= ring_len {
            return Err(Error::InvalidFormat(
                "WAL end offset exceeds ring length".into(),
            ));
        }

        let used_bytes = if section_header.is_full {
            ring_len
        } else {
            ring_distance(
                section_header.start_offset,
                section_header.end_offset,
                ring_len,
            )
        };

        // Validate all segment headers (magic/version/crc).
        let mut segment_id_base: Option<u64> = None;
        for segment_index in 0..max_segments {
            let header = read_segment_header(&mut file, segment_size, segment_index)?;
            if segment_index == 0 {
                segment_id_base = Some(header.segment_id);
            } else if let Some(base) = segment_id_base {
                if header.segment_id != base + segment_index {
                    return Err(Error::InvalidFormat(
                        "WAL segment_id sequence mismatch".into(),
                    ));
                }
            }
        }

        Ok(Self {
            file,
            config,
            section_header,
            segment_id_base: segment_id_base.unwrap_or(0),
            segment_size,
            segment_data_len,
            ring_len,
            used_bytes,
            pending_sync: 0,
            last_sync: Instant::now(),
        })
    }

    /// Advance the read pointer (start_offset) after a checkpoint.
    pub fn advance_start(&mut self, new_start: u64) -> Result<()> {
        if new_start >= self.ring_len {
            return Err(Error::InvalidFormat(
                "WAL start offset exceeds ring length".into(),
            ));
        }
        let current = self.section_header.start_offset;
        let distance = ring_distance(current, new_start, self.ring_len);
        if distance > self.used_bytes {
            return Err(Error::InvalidFormat(
                "WAL start offset advances beyond written data".into(),
            ));
        }
        self.used_bytes -= distance;
        self.section_header.start_offset = new_start;
        self.section_header.is_full = false;

        if self.used_bytes == 0 {
            // Prefer reusing the reclaimed region immediately by resetting pointers.
            self.section_header.start_offset = 0;
            self.section_header.end_offset = 0;
            self.section_header.is_full = false;
        }

        persist_section_header(&mut self.file, 0, &self.section_header)?;
        Ok(())
    }

    /// Append a WAL entry into the circular buffer, returning the file offset written.
    pub fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        let encoded = entry.encode()?;
        let entry_len = encoded.len() as u64;
        if entry_len > self.ring_len {
            return Err(Error::InvalidFormat(
                "WAL entry exceeds ring capacity".into(),
            ));
        }

        let free_space = self.ring_len - self.used_bytes;
        if entry_len > free_space {
            return Err(Error::InvalidFormat(
                "WAL buffer is full; cannot append entry".into(),
            ));
        }

        // If the buffer is empty, always start from the beginning of the data region.
        if self.used_bytes == 0 && !self.section_header.is_full {
            self.section_header.start_offset = 0;
            self.section_header.end_offset = 0;
        }

        let write_offset = self.section_header.end_offset;
        let file_offset =
            ring_logical_to_physical(write_offset, self.segment_size, self.segment_data_len)?;
        self.write_ring(write_offset, &encoded, entry.lsn)?;

        let new_end = write_offset + entry_len;
        self.section_header.end_offset = new_end % self.ring_len;
        self.used_bytes += entry_len;
        self.section_header.is_full = self.used_bytes == self.ring_len;
        persist_section_header(&mut self.file, 0, &self.section_header)?;

        self.maybe_sync(encoded.len())?;
        Ok(file_offset)
    }

    fn maybe_sync(&mut self, bytes_written: usize) -> Result<()> {
        match self.config.sync_mode {
            SyncMode::EveryWrite => {
                self.file.sync_data()?;
                self.pending_sync = 0;
                self.last_sync = Instant::now();
            }
            SyncMode::BatchSync {
                max_batch_size,
                max_wait_ms,
            } => {
                self.pending_sync += bytes_written;
                let elapsed = self.last_sync.elapsed();
                let should_sync = self.pending_sync >= max_batch_size
                    || elapsed >= Duration::from_millis(max_wait_ms);
                if should_sync {
                    self.file.sync_data()?;
                    self.pending_sync = 0;
                    self.last_sync = Instant::now();
                }
            }
            SyncMode::NoSync => {
                // no-op
            }
        }
        Ok(())
    }

    /// Total logical ring length in bytes.
    pub fn ring_len(&self) -> u64 {
        self.ring_len
    }
}

/// Result of WAL replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplay {
    /// Successfully decoded entries, in order.
    pub entries: Vec<WalEntry>,
    /// If replay stopped early, the logical ring offset where it stopped.
    pub stopped_at: Option<u64>,
    /// If replay stopped early, the reason (e.g. checksum mismatch / truncated entry).
    pub stop_reason: Option<String>,
}

/// WAL reader for crash recovery.
#[derive(Debug)]
pub struct WalReader {
    file: File,
    config: WalConfig,
    section_header: WalSectionHeader,
    segment_id_base: u64,
    segment_size: u64,
    segment_data_len: u64,
    ring_len: u64,
    used_bytes: u64,
}

impl WalReader {
    /// Open an existing WAL section for replay.
    ///
    /// The reader replays entries from `start_offset` (inclusive) up to `end_offset` (exclusive)
    /// within the logical ring.
    pub fn open(path: &Path, config: WalConfig) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;

        let (segment_size, segment_data_len, max_segments, ring_len) =
            compute_ring_layout(&config)?;
        let wal_section_size = (WAL_SECTION_HEADER_SIZE as u64)
            .checked_add(
                segment_size
                    .checked_mul(max_segments)
                    .ok_or_else(|| Error::InvalidFormat("WAL section size overflow".into()))?,
            )
            .ok_or_else(|| Error::InvalidFormat("WAL section size overflow".into()))?;

        let file_len = file.metadata()?.len();
        if file_len < wal_section_size {
            return Err(Error::InvalidFormat(
                "WAL file is smaller than configured section size".into(),
            ));
        }

        let section_header = load_section_header(&mut file, 0)?;
        if section_header.start_offset >= ring_len {
            return Err(Error::InvalidFormat(
                "WAL start offset exceeds ring length".into(),
            ));
        }
        if section_header.end_offset >= ring_len {
            return Err(Error::InvalidFormat(
                "WAL end offset exceeds ring length".into(),
            ));
        }

        let used_bytes = if section_header.is_full {
            ring_len
        } else {
            ring_distance(
                section_header.start_offset,
                section_header.end_offset,
                ring_len,
            )
        };

        let mut segment_id_base: Option<u64> = None;
        for segment_index in 0..max_segments {
            let header = read_segment_header(&mut file, segment_size, segment_index)?;
            if segment_index == 0 {
                segment_id_base = Some(header.segment_id);
            } else if let Some(base) = segment_id_base {
                if header.segment_id != base + segment_index {
                    return Err(Error::InvalidFormat(
                        "WAL segment_id sequence mismatch".into(),
                    ));
                }
            }
        }

        Ok(Self {
            file,
            config,
            section_header,
            segment_id_base: segment_id_base.unwrap_or(0),
            segment_size,
            segment_data_len,
            ring_len,
            used_bytes,
        })
    }

    /// Replay entries in the WAL section.
    ///
    /// If a corrupted or incomplete entry is detected, replay stops and returns the entries
    /// decoded so far with `stop_reason` populated. This supports crash recovery where the tail
    /// of the WAL may be partially written.
    pub fn replay(&mut self) -> Result<WalReplay> {
        let mut entries = Vec::new();
        let mut cursor = self.section_header.start_offset;
        let mut remaining = self.used_bytes;
        let mut last_lsn: Option<u64> = None;

        while remaining > 0 {
            if remaining < WAL_ENTRY_FIXED_HEADER as u64 {
                return Ok(WalReplay {
                    entries,
                    stopped_at: Some(cursor),
                    stop_reason: Some("WAL entry header truncated".into()),
                });
            }

            let header_bytes = read_ring_bytes(
                &mut self.file,
                cursor,
                WAL_ENTRY_FIXED_HEADER,
                self.segment_size,
                self.segment_data_len,
                self.ring_len,
            )?;
            let payload_and_crc_len =
                u32::from_le_bytes(header_bytes[8..12].try_into().expect("fixed slice length"))
                    as u64;
            let total_len = (WAL_ENTRY_FIXED_HEADER as u64)
                .checked_add(payload_and_crc_len)
                .ok_or_else(|| Error::InvalidFormat("WAL entry length overflow".into()))?;

            if total_len == 0 || total_len > self.ring_len {
                return Ok(WalReplay {
                    entries,
                    stopped_at: Some(cursor),
                    stop_reason: Some("WAL entry length is invalid".into()),
                });
            }

            if total_len > remaining {
                return Ok(WalReplay {
                    entries,
                    stopped_at: Some(cursor),
                    stop_reason: Some("WAL entry truncated at tail".into()),
                });
            }

            let entry_bytes = read_ring_bytes(
                &mut self.file,
                cursor,
                total_len as usize,
                self.segment_size,
                self.segment_data_len,
                self.ring_len,
            )?;

            let decoded = match WalEntry::decode(&entry_bytes) {
                Ok((entry, consumed)) => {
                    if consumed as u64 != total_len {
                        return Ok(WalReplay {
                            entries,
                            stopped_at: Some(cursor),
                            stop_reason: Some("WAL entry decode consumed unexpected length".into()),
                        });
                    }
                    entry
                }
                Err(err) => {
                    return Ok(WalReplay {
                        entries,
                        stopped_at: Some(cursor),
                        stop_reason: Some(format!("WAL entry decode failed: {err}")),
                    });
                }
            };

            if let Some(prev) = last_lsn {
                if decoded.lsn <= prev {
                    return Ok(WalReplay {
                        entries,
                        stopped_at: Some(cursor),
                        stop_reason: Some("WAL LSN is not strictly increasing".into()),
                    });
                }
            }
            last_lsn = Some(decoded.lsn);

            entries.push(decoded);
            cursor = (cursor + total_len) % self.ring_len;
            remaining -= total_len;
        }

        Ok(WalReplay {
            entries,
            stopped_at: None,
            stop_reason: None,
        })
    }

    /// Current section header (start/end/is_full).
    pub fn section_header(&self) -> &WalSectionHeader {
        &self.section_header
    }

    /// Total logical ring length in bytes.
    pub fn ring_len(&self) -> u64 {
        self.ring_len
    }

    /// Segment ID base (segment_id = base + index).
    pub fn segment_id_base(&self) -> u64 {
        self.segment_id_base
    }

    /// Configuration used by this reader.
    pub fn config(&self) -> &WalConfig {
        &self.config
    }
}

#[cfg(test)]
mod reader {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wal_reader_replays_entries_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_reader_basic");
        let config = WalConfig {
            segment_size: 4096,
            max_segments: 1,
            ..Default::default()
        };

        let mut writer = WalWriter::create(&path, config.clone(), 10, 1).unwrap();
        let e1 = WalEntry::put(1, b"a".to_vec(), b"1".to_vec());
        let e2 = WalEntry::delete(2, b"b".to_vec());
        writer.append(&e1).unwrap();
        writer.append(&e2).unwrap();

        let mut reader = WalReader::open(&path, config).unwrap();
        let replay = reader.replay().unwrap();
        assert_eq!(replay.stop_reason, None);
        assert_eq!(replay.entries, vec![e1, e2]);
    }

    #[test]
    fn wal_reader_skips_entries_before_start_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_reader_start");
        let config = WalConfig {
            segment_size: 4096,
            max_segments: 1,
            ..Default::default()
        };

        let mut writer = WalWriter::create(&path, config.clone(), 10, 1).unwrap();
        let e1 = WalEntry::put(1, b"a".to_vec(), b"1".to_vec());
        let e2 = WalEntry::put(2, b"b".to_vec(), b"2".to_vec());
        let e1_len = e1.encode().unwrap().len() as u64;
        writer.append(&e1).unwrap();
        writer.append(&e2).unwrap();

        writer.advance_start(e1_len).unwrap();

        let mut reader = WalReader::open(&path, config).unwrap();
        let replay = reader.replay().unwrap();
        assert_eq!(replay.stop_reason, None);
        assert_eq!(replay.entries, vec![e2]);
    }

    #[test]
    fn wal_reader_stops_on_corrupt_tail_and_returns_prefix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_reader_corrupt_tail");
        let config = WalConfig {
            segment_size: 4096,
            max_segments: 1,
            ..Default::default()
        };

        let mut writer = WalWriter::create(&path, config.clone(), 10, 1).unwrap();
        let e1 = WalEntry::put(1, b"a".to_vec(), b"1".to_vec());
        let e2 = WalEntry::put(2, b"b".to_vec(), b"2".to_vec());
        let e2_len = e2.encode().unwrap().len() as u64;
        writer.append(&e1).unwrap();

        // Simulate a crash where the section header advanced for e2, but its bytes are not valid.
        let start_of_e2 = writer.section_header.end_offset;
        let mut corrupted_header = writer.section_header.clone();
        corrupted_header.end_offset = (start_of_e2 + e2_len) % writer.ring_len;
        persist_section_header(&mut writer.file, 0, &corrupted_header).unwrap();

        let mut reader = WalReader::open(&path, config).unwrap();
        let replay = reader.replay().unwrap();
        assert_eq!(replay.entries, vec![e1]);
        assert!(replay.stop_reason.is_some());
    }

    #[test]
    fn wal_reader_replays_entry_crossing_segment_boundary() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_reader_multi");
        let entry = WalEntry::put(10, b"k".to_vec(), vec![0xCD; 64]);
        let encoded = entry.encode().unwrap();
        let segment_data_len = (encoded.len() - 1) as u64; // force boundary crossing
        let config = WalConfig {
            segment_size: (WAL_SEGMENT_HEADER_SIZE as u64 + segment_data_len) as usize,
            max_segments: 2,
            ..Default::default()
        };

        let mut writer = WalWriter::create(&path, config.clone(), 2000, entry.lsn).unwrap();
        writer.append(&entry).unwrap();

        let mut reader = WalReader::open(&path, config).unwrap();
        let replay = reader.replay().unwrap();
        assert_eq!(replay.stop_reason, None);
        assert_eq!(replay.entries, vec![entry]);
    }
}
