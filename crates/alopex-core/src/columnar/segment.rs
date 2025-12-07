//! Columnar segment writer/reader with chunked IO and checksum verification.
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::columnar::encoding::{
    decode_column, encode_column, Column, Compression, Encoding, LogicalType,
};
use crate::columnar::error::{ColumnarError, Result};
use crc32fast::Hasher;

const MAGIC: &[u8] = b"ALXC";
const VERSION: u16 = 1;
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024; // 16 MiB guard to avoid unbounded allocations.

/// Metadata describing a segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Logical type of the stored column.
    pub logical_type: LogicalType,
    /// Encoding applied to each chunk.
    pub encoding: Encoding,
    /// Compression applied after encoding.
    pub compression: Compression,
    /// Maximum rows per chunk.
    pub chunk_rows: usize,
    /// Whether to append a checksum per chunk.
    pub chunk_checksum: bool,
}

/// Writes a single-column segment to `path`, chunked by `meta.chunk_rows`.
pub fn write_segment(path: &Path, column: &Column, meta: &SegmentMeta) -> Result<()> {
    // v1 セグメントは Float32 と Fixed 長 251 以上をサポートしない。
    match meta.logical_type {
        LogicalType::Float32 => {
            return Err(ColumnarError::InvalidFormat(
                "segment v1 does not support Float32".into(),
            ))
        }
        LogicalType::Fixed(len) if len >= 251 => {
            return Err(ColumnarError::InvalidFormat(
                "segment v1 fixed length must be <= 250".into(),
            ))
        }
        _ => {}
    }

    let mut file = File::create(path)?;

    // Header
    file.write_all(MAGIC)?;
    file.write_all(&VERSION.to_le_bytes())?;
    let logical_byte = logical_to_byte(meta.logical_type)?;
    file.write_all(&[
        logical_byte,
        encoding_to_byte(meta.encoding),
        compression_to_byte(meta.compression),
    ])?;
    file.write_all(&(meta.chunk_rows as u32).to_le_bytes())?;
    file.write_all(&[meta.chunk_checksum as u8])?;

    let total_rows = column_len(column);
    file.write_all(&(total_rows as u32).to_le_bytes())?;

    // Chunked payload
    let mut start = 0;
    while start < total_rows {
        let end = usize::min(start + meta.chunk_rows, total_rows);
        let chunk = slice_column(column, start, end - start)?;
        let encoded = encode_column(
            &chunk,
            meta.encoding,
            meta.compression,
            false,
            meta.logical_type,
        )?;
        let mut checksum_bytes = [0u8; 4];
        if meta.chunk_checksum {
            let mut hasher = Hasher::new();
            hasher.update(&encoded);
            checksum_bytes = hasher.finalize().to_le_bytes();
        }

        file.write_all(&((end - start) as u32).to_le_bytes())?;
        file.write_all(&(encoded.len() as u32).to_le_bytes())?;
        file.write_all(&encoded)?;
        if meta.chunk_checksum {
            file.write_all(&checksum_bytes)?;
        }
        start = end;
    }
    Ok(())
}

/// Streaming reader for a columnar segment.
pub struct SegmentReader {
    meta: SegmentMeta,
    file: File,
    remaining_rows: usize,
}

impl SegmentReader {
    /// Opens a segment file and validates the header.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(ColumnarError::InvalidFormat("invalid segment magic".into()));
        }
        let mut version_bytes = [0u8; 2];
        file.read_exact(&mut version_bytes)?;
        let version = u16::from_le_bytes(version_bytes);
        if version != VERSION {
            return Err(ColumnarError::InvalidFormat(
                "unsupported segment version".into(),
            ));
        }

        let mut kind = [0u8; 3];
        file.read_exact(&mut kind)?;
        let logical_type = byte_to_logical(kind[0])?;
        let encoding = byte_to_encoding(kind[1])?;
        let compression = byte_to_compression(kind[2])?;

        let mut chunk_rows_bytes = [0u8; 4];
        file.read_exact(&mut chunk_rows_bytes)?;
        let chunk_rows = u32::from_le_bytes(chunk_rows_bytes) as usize;

        let mut checksum_flag = [0u8; 1];
        file.read_exact(&mut checksum_flag)?;
        let chunk_checksum = checksum_flag[0] != 0;

        let mut total_rows_bytes = [0u8; 4];
        file.read_exact(&mut total_rows_bytes)?;
        let remaining_rows = u32::from_le_bytes(total_rows_bytes) as usize;

        let meta = SegmentMeta {
            logical_type,
            encoding,
            compression,
            chunk_rows,
            chunk_checksum,
        };

        Ok(Self {
            meta,
            file,
            remaining_rows,
        })
    }

    /// Returns an iterator that yields decoded column chunks.
    pub fn iter(&mut self) -> ChunkIter<'_> {
        ChunkIter { reader: self }
    }
}

/// Iterator over column chunks. Buffers only one chunk at a time.
pub struct ChunkIter<'a> {
    reader: &'a mut SegmentReader,
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.remaining_rows == 0 {
            return None;
        }

        let mut row_bytes = [0u8; 4];
        if let Err(e) = self.reader.file.read_exact(&mut row_bytes) {
            return Some(Err(ColumnarError::InvalidFormat(format!(
                "chunk rows read failed: {e}"
            ))));
        }
        let rows = u32::from_le_bytes(row_bytes) as usize;
        if rows == 0 || rows > self.reader.meta.chunk_rows {
            return Some(Err(ColumnarError::CorruptedSegment {
                reason: "chunk rows exceed declared limit".into(),
            }));
        }

        let mut len_bytes = [0u8; 4];
        if let Err(e) = self.reader.file.read_exact(&mut len_bytes) {
            return Some(Err(ColumnarError::InvalidFormat(format!(
                "chunk length read failed: {e}"
            ))));
        }
        let len = u32::from_le_bytes(len_bytes) as usize;
        if len > MAX_CHUNK_BYTES {
            return Some(Err(ColumnarError::CorruptedSegment {
                reason: "chunk encoded size exceeds limit".into(),
            }));
        }

        let mut buf = vec![0u8; len];
        if let Err(e) = self.reader.file.read_exact(&mut buf) {
            return Some(Err(ColumnarError::InvalidFormat(format!(
                "chunk payload read failed: {e}"
            ))));
        }

        if self.reader.meta.chunk_checksum {
            let mut crc_bytes = [0u8; 4];
            if let Err(e) = self.reader.file.read_exact(&mut crc_bytes) {
                return Some(Err(ColumnarError::InvalidFormat(format!(
                    "chunk checksum read failed: {e}"
                ))));
            }
            let expected = u32::from_le_bytes(crc_bytes);
            let mut hasher = Hasher::new();
            hasher.update(&buf);
            let computed = hasher.finalize();
            if expected != computed {
                return Some(Err(ColumnarError::CorruptedSegment {
                    reason: "checksum mismatch".into(),
                }));
            }
        }

        self.reader.remaining_rows = self.reader.remaining_rows.saturating_sub(rows);
        Some(decode_column(
            &buf,
            self.reader.meta.logical_type,
            self.reader.meta.encoding,
            self.reader.meta.compression,
            false,
        ))
    }
}

fn logical_to_byte(logical: LogicalType) -> Result<u8> {
    match logical {
        LogicalType::Int64 => Ok(0),
        LogicalType::Float64 => Ok(1),
        LogicalType::Bool => Ok(2),
        LogicalType::Binary => Ok(3),
        LogicalType::Float32 => Err(ColumnarError::InvalidFormat(
            "segment v1 does not support Float32".into(),
        )),
        LogicalType::Fixed(len) => {
            // 4..=254 are reserved for fixed lengths 0..=250. 255 is treated as
            // a clamped legacy sentinel for >=251 to avoid panics.
            if len >= 251 {
                Ok(255)
            } else {
                Ok(4 + (len as u8))
            }
        }
    }
}

fn byte_to_logical(byte: u8) -> Result<LogicalType> {
    match byte {
        0 => Ok(LogicalType::Int64),
        1 => Ok(LogicalType::Float64),
        2 => Ok(LogicalType::Bool),
        3 => Ok(LogicalType::Binary),
        255 => Ok(LogicalType::Fixed(251)),
        b if b >= 4 => Ok(LogicalType::Fixed((b - 4) as u16)),
        _ => Err(ColumnarError::InvalidFormat("unknown logical type".into())),
    }
}

fn encoding_to_byte(enc: Encoding) -> u8 {
    match enc {
        Encoding::Plain => 0,
        Encoding::Dictionary => 1,
        Encoding::Rle => 2,
        Encoding::Bitpack => 3,
    }
}

fn byte_to_encoding(byte: u8) -> Result<Encoding> {
    match byte {
        0 => Ok(Encoding::Plain),
        1 => Ok(Encoding::Dictionary),
        2 => Ok(Encoding::Rle),
        3 => Ok(Encoding::Bitpack),
        _ => Err(ColumnarError::InvalidFormat("unknown encoding".into())),
    }
}

fn compression_to_byte(comp: Compression) -> u8 {
    match comp {
        Compression::None => 0,
        Compression::Lz4 => 1,
    }
}

fn byte_to_compression(byte: u8) -> Result<Compression> {
    match byte {
        0 => Ok(Compression::None),
        1 => Ok(Compression::Lz4),
        _ => Err(ColumnarError::InvalidFormat("unknown compression".into())),
    }
}

fn column_len(column: &Column) -> usize {
    match column {
        Column::Int64(v) => v.len(),
        Column::Float32(v) => v.len(),
        Column::Float64(v) => v.len(),
        Column::Bool(v) => v.len(),
        Column::Binary(v) => v.len(),
        Column::Fixed { values, .. } => values.len(),
    }
}

fn slice_column(column: &Column, start: usize, len: usize) -> Result<Column> {
    match column {
        Column::Int64(v) => Ok(Column::Int64(v[start..start + len].to_vec())),
        Column::Float32(v) => Ok(Column::Float32(v[start..start + len].to_vec())),
        Column::Float64(v) => Ok(Column::Float64(v[start..start + len].to_vec())),
        Column::Bool(v) => Ok(Column::Bool(v[start..start + len].to_vec())),
        Column::Binary(v) => Ok(Column::Binary(v[start..start + len].to_vec())),
        Column::Fixed {
            len: fixed_len,
            values,
        } => Ok(Column::Fixed {
            len: *fixed_len,
            values: values[start..start + len].to_vec(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn segment_roundtrip_plain_int64() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg.alx");
        let meta = SegmentMeta {
            logical_type: LogicalType::Int64,
            encoding: Encoding::Plain,
            compression: Compression::None,
            chunk_rows: 2,
            chunk_checksum: true,
        };
        let col = Column::Int64(vec![1, 2, 3, 4, 5]);
        write_segment(&path, &col, &meta).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let mut out = Vec::new();
        for chunk in reader.iter() {
            match chunk.unwrap() {
                Column::Int64(vals) => out.extend(vals),
                _ => panic!("expected int64"),
            }
        }
        assert_eq!(out, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn checksum_failure_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg_bad.alx");
        let meta = SegmentMeta {
            logical_type: LogicalType::Int64,
            encoding: Encoding::Plain,
            compression: Compression::None,
            chunk_rows: 3,
            chunk_checksum: true,
        };
        let col = Column::Int64(vec![10, 20, 30]);
        write_segment(&path, &col, &meta).unwrap();

        // Corrupt a byte in the payload of the first chunk (after header + row_count + len).
        let mut bytes = std::fs::read(&path).unwrap();
        let header_len = 4 + 2 + 3 + 4 + 1 + 4; // magic + version + kind + chunk_rows + flag + total_rows
        let payload_len =
            u32::from_le_bytes(bytes[header_len + 4..header_len + 8].try_into().unwrap()) as usize;
        let payload_start = header_len + 8;
        if payload_len > 0 {
            bytes[payload_start] ^= 0xAA;
        } else {
            // fallback: flip checksum byte
            bytes[payload_start + payload_len] ^= 0xAA;
        }
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let err = reader.iter().next().unwrap().unwrap_err();
        assert!(matches!(err, ColumnarError::CorruptedSegment { .. }));
    }

    #[test]
    fn chunk_rows_over_limit_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg_over.alx");
        let meta = SegmentMeta {
            logical_type: LogicalType::Int64,
            encoding: Encoding::Plain,
            compression: Compression::None,
            chunk_rows: 2,
            chunk_checksum: false,
        };
        let col = Column::Int64(vec![1, 2, 3, 4]);
        write_segment(&path, &col, &meta).unwrap();

        // Bump first chunk row count to exceed declared chunk_rows.
        let mut bytes = std::fs::read(&path).unwrap();
        let header_len = 4 + 2 + 3 + 4 + 1 + 4;
        let bad_rows: u32 = 10;
        bytes[header_len..header_len + 4].copy_from_slice(&bad_rows.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let err = reader.iter().next().unwrap().unwrap_err();
        assert!(matches!(err, ColumnarError::CorruptedSegment { .. }));
    }

    #[test]
    fn logical_type_byte_mapping_legacy_safe() {
        // Fixed length below the boundary maps into 4..=254.
        assert_eq!(logical_to_byte(LogicalType::Fixed(250)).unwrap(), 254);
        assert_eq!(byte_to_logical(254).unwrap(), LogicalType::Fixed(250));

        // Legacy sentinel 255 is kept for fixed >=251, decoded as clamped 251.
        assert_eq!(logical_to_byte(LogicalType::Fixed(251)).unwrap(), 255);
        assert_eq!(byte_to_logical(255).unwrap(), LogicalType::Fixed(251));
    }

    #[test]
    fn float32_is_rejected_in_v1_segment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg_f32.alx");
        let meta = SegmentMeta {
            logical_type: LogicalType::Float32,
            encoding: Encoding::Plain,
            compression: Compression::None,
            chunk_rows: 2,
            chunk_checksum: false,
        };
        let col = Column::Float32(vec![1.0, 2.0]);
        let err = write_segment(&path, &col, &meta).unwrap_err();
        assert!(matches!(err, ColumnarError::InvalidFormat(_)));
    }

    #[test]
    fn fixed_length_over_250_is_rejected_in_v1_segment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg_fixed_over.alx");
        let meta = SegmentMeta {
            logical_type: LogicalType::Fixed(300),
            encoding: Encoding::Plain,
            compression: Compression::None,
            chunk_rows: 2,
            chunk_checksum: false,
        };
        let col = Column::Fixed {
            len: 300,
            values: vec![vec![0u8; 300]],
        };
        let err = write_segment(&path, &col, &meta).unwrap_err();
        assert!(matches!(err, ColumnarError::InvalidFormat(_)));
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn lz4_dictionary_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg_dict.alx");
        let meta = SegmentMeta {
            logical_type: LogicalType::Binary,
            encoding: Encoding::Dictionary,
            compression: Compression::Lz4,
            chunk_rows: 4,
            chunk_checksum: false,
        };
        let col = Column::Binary(vec![
            b"aa".to_vec(),
            b"bb".to_vec(),
            b"aa".to_vec(),
            b"cc".to_vec(),
        ]);
        write_segment(&path, &col, &meta).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let mut out = Vec::new();
        for chunk in reader.iter() {
            match chunk.unwrap() {
                Column::Binary(vals) => out.extend(vals),
                _ => panic!("expected binary"),
            }
        }
        assert_eq!(
            out,
            vec![
                b"aa".to_vec(),
                b"bb".to_vec(),
                b"aa".to_vec(),
                b"cc".to_vec()
            ]
        );
    }
}
