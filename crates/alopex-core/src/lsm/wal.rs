//! Write-Ahead Log (WAL) primitives for the LSM file mode.
//!
//! This module defines the on-disk WAL segment header and entry layout used by
//! the circular buffer inside a single `.alopex` file. Writer/reader
//! implementations build on top of these primitives.

use std::convert::TryFrom;

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
    BatchSync {
        /// Max bytes to buffer before fsync.
        max_batch_size: usize,
        /// Max wait time (ms) before forcing fsync.
        max_wait_ms: u64,
    },
    /// Rely on OS buffering (fastest, least safe).
    NoSync,
}

/// Fixed-size WAL segment header (28 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSegmentHeader {
    /// Format version.
    pub version: u16,
    /// Segment identifier.
    pub segment_id: u64,
    /// First LSN stored in this segment.
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

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        let bytes = header.to_bytes();
        let decoded = WalSectionHeader::from_bytes(&bytes);
        assert_eq!(decoded, header);
    }
}
/// WAL section header for circular buffer bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSectionHeader {
    /// Read pointer offset (inclusive).
    pub start_offset: u64,
    /// Write pointer offset (exclusive).
    pub end_offset: u64,
}

impl WalSectionHeader {
    /// Serialize to 16 bytes (start/end offsets).
    pub fn to_bytes(&self) -> [u8; WAL_SECTION_HEADER_SIZE] {
        let mut buf = [0u8; WAL_SECTION_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.start_offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.end_offset.to_le_bytes());
        buf
    }

    /// Deserialize from 16 bytes.
    pub fn from_bytes(bytes: &[u8; WAL_SECTION_HEADER_SIZE]) -> Self {
        let start_offset = u64::from_le_bytes(bytes[0..8].try_into().expect("fixed slice length"));
        let end_offset = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed slice length"));
        Self {
            start_offset,
            end_offset,
        }
    }
}
