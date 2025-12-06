//! Columnar encoding/decoding utilities for fixed/variable-length data with optional compression.
use std::convert::TryInto;

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::columnar::error::{ColumnarError, Result};

/// Logical data type of a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalType {
    /// Signed 64-bit integer.
    Int64,
    /// 64-bit floating point.
    Float64,
    /// Boolean value.
    Bool,
    /// Arbitrary binary (variable-length).
    Binary,
    /// Fixed-length binary of `len` bytes.
    Fixed(u16),
}

/// Encoding strategy for a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    /// Raw values.
    Plain,
    /// Dictionary encoding with indexes.
    Dictionary,
    /// Run-length encoding.
    Rle,
    /// Bit-packed representation (bools).
    Bitpack,
}

/// Compression applied after encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    /// No compression.
    None,
    /// LZ4 block compression.
    Lz4,
}

/// In-memory representation of a column.
#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    /// Column of i64 values.
    Int64(Vec<i64>),
    /// Column of f64 values.
    Float64(Vec<f64>),
    /// Column of bool values.
    Bool(Vec<bool>),
    /// Column of variable-length binary values.
    Binary(Vec<Vec<u8>>),
    /// Column of fixed-length binary values.
    Fixed {
        /// Fixed byte length for each value.
        len: usize,
        /// Fixed-length binary values.
        values: Vec<Vec<u8>>,
    },
}

/// Encode a column with the given encoding/compression and optional checksum.
///
/// The optional checksum computes CRC32 over the stored bytes (post-compression).
pub fn encode_column(
    column: &Column,
    encoding: Encoding,
    compression: Compression,
    checksum: bool,
    logical_type: LogicalType,
) -> Result<Vec<u8>> {
    validate_logical(column, logical_type)?;
    let mut payload = match encoding {
        Encoding::Plain => encode_plain(column)?,
        Encoding::Dictionary => encode_dictionary(column)?,
        Encoding::Rle => encode_rle(column)?,
        Encoding::Bitpack => encode_bitpack(column)?,
    };

    if let Compression::Lz4 = compression {
        #[cfg(feature = "compression-lz4")]
        {
            let orig_len: u32 =
                payload
                    .len()
                    .try_into()
                    .map_err(|_| ColumnarError::CorruptedSegment {
                        reason: "payload too large for lz4".into(),
                    })?;
            let compressed = lz4::block::compress(&payload, None, false).map_err(|e| {
                ColumnarError::CorruptedSegment {
                    reason: e.to_string(),
                }
            })?;
            let mut buf = Vec::with_capacity(4 + compressed.len());
            buf.extend_from_slice(&orig_len.to_le_bytes());
            buf.extend_from_slice(&compressed);
            payload = buf;
        }
        #[cfg(not(feature = "compression-lz4"))]
        {
            return Err(ColumnarError::CorruptedSegment {
                reason: "lz4 compression is disabled (feature compression-lz4)".into(),
            });
        }
    }

    if checksum {
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        payload.extend_from_slice(&crc.to_le_bytes());
    }

    Ok(payload)
}

/// Decode bytes into a column using the specified logical type, encoding, and compression.
pub fn decode_column(
    bytes: &[u8],
    logical_type: LogicalType,
    encoding: Encoding,
    compression: Compression,
    checksum: bool,
) -> Result<Column> {
    let data = if checksum {
        if bytes.len() < 4 {
            return Err(ColumnarError::CorruptedSegment {
                reason: "checksum missing".into(),
            });
        }
        let (content, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let expected = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(content);
        let computed = hasher.finalize();
        if expected != computed {
            return Err(ColumnarError::ChecksumMismatch);
        }
        content
    } else {
        bytes
    };

    let decompressed = match compression {
        Compression::None => data.to_vec(),
        Compression::Lz4 => {
            #[cfg(feature = "compression-lz4")]
            {
                if data.len() < 4 {
                    return Err(ColumnarError::CorruptedSegment {
                        reason: "lz4 header too short".into(),
                    });
                }
                let orig_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as i32;
                lz4::block::decompress(&data[4..], Some(orig_len as i32)).map_err(|e| {
                    ColumnarError::CorruptedSegment {
                        reason: e.to_string(),
                    }
                })?
            }
            #[cfg(not(feature = "compression-lz4"))]
            {
                return Err(ColumnarError::CorruptedSegment {
                    reason: "lz4 compression is disabled (feature compression-lz4)".into(),
                });
            }
        }
    };

    match encoding {
        Encoding::Plain => decode_plain(&decompressed, logical_type),
        Encoding::Dictionary => decode_dictionary(&decompressed, logical_type),
        Encoding::Rle => decode_rle(&decompressed, logical_type),
        Encoding::Bitpack => decode_bitpack(&decompressed, logical_type),
    }
}

fn validate_logical(column: &Column, logical: LogicalType) -> Result<()> {
    match (column, logical) {
        (Column::Int64(_), LogicalType::Int64)
        | (Column::Float64(_), LogicalType::Float64)
        | (Column::Bool(_), LogicalType::Bool)
        | (Column::Binary(_), LogicalType::Binary) => Ok(()),
        (Column::Fixed { len, .. }, LogicalType::Fixed(flen)) if *len == flen as usize => Ok(()),
        (_, LogicalType::Fixed(_)) => Err(ColumnarError::CorruptedSegment {
            reason: "fixed length mismatch".into(),
        }),
        _ => Err(ColumnarError::CorruptedSegment {
            reason: "logical type mismatch".into(),
        }),
    }
}

fn encode_plain(column: &Column) -> Result<Vec<u8>> {
    match column {
        Column::Int64(values) => {
            let mut buf = Vec::with_capacity(4 + values.len() * 8);
            buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for v in values {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Ok(buf)
        }
        Column::Float64(values) => {
            let mut buf = Vec::with_capacity(4 + values.len() * 8);
            buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for v in values {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Ok(buf)
        }
        Column::Bool(values) => {
            let mut buf = Vec::with_capacity(4 + values.len());
            buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for v in values {
                buf.push(*v as u8);
            }
            Ok(buf)
        }
        Column::Binary(values) => encode_varlen(values),
        Column::Fixed { len, values } => {
            for v in values {
                if v.len() != *len {
                    return Err(ColumnarError::CorruptedSegment {
                        reason: "fixed value length mismatch".into(),
                    });
                }
            }
            let mut buf = Vec::with_capacity(6 + values.len() * *len);
            buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
            buf.extend_from_slice(&(*len as u16).to_le_bytes());
            for v in values {
                buf.extend_from_slice(v);
            }
            Ok(buf)
        }
    }
}

fn encode_varlen(values: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for v in values {
        let len: u32 = v
            .len()
            .try_into()
            .map_err(|_| ColumnarError::CorruptedSegment {
                reason: "value too long".into(),
            })?;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(v);
    }
    Ok(buf)
}

fn decode_plain(bytes: &[u8], logical: LogicalType) -> Result<Column> {
    if bytes.len() < 4 {
        return Err(ColumnarError::CorruptedSegment {
            reason: "plain header too short".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut pos = 4;
    match logical {
        LogicalType::Int64 => {
            if bytes.len() < pos + count * 8 {
                return Err(ColumnarError::CorruptedSegment {
                    reason: "plain int64 truncated".into(),
                });
            }
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let v = i64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
                out.push(v);
                pos += 8;
            }
            Ok(Column::Int64(out))
        }
        LogicalType::Float64 => {
            if bytes.len() < pos + count * 8 {
                return Err(ColumnarError::CorruptedSegment {
                    reason: "plain float64 truncated".into(),
                });
            }
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let v = f64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
                out.push(v);
                pos += 8;
            }
            Ok(Column::Float64(out))
        }
        LogicalType::Bool => {
            if bytes.len() < pos + count {
                return Err(ColumnarError::CorruptedSegment {
                    reason: "plain bool truncated".into(),
                });
            }
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                out.push(bytes[pos] != 0);
                pos += 1;
            }
            Ok(Column::Bool(out))
        }
        LogicalType::Binary => decode_varlen(&bytes[4..], count).map(Column::Binary),
        LogicalType::Fixed(len) => {
            if bytes.len() < pos + 2 {
                return Err(ColumnarError::CorruptedSegment {
                    reason: "fixed header truncated".into(),
                });
            }
            let stored_len = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if stored_len as u16 != len {
                return Err(ColumnarError::CorruptedSegment {
                    reason: "fixed length mismatch".into(),
                });
            }
            let expected = pos + count * stored_len;
            if bytes.len() < expected {
                return Err(ColumnarError::CorruptedSegment {
                    reason: "fixed values truncated".into(),
                });
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let end = pos + stored_len;
                values.push(bytes[pos..end].to_vec());
                pos = end;
            }
            Ok(Column::Fixed {
                len: stored_len,
                values,
            })
        }
    }
}

fn decode_varlen(bytes: &[u8], count: usize) -> Result<Vec<Vec<u8>>> {
    let mut pos = 0;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err(ColumnarError::CorruptedSegment {
                reason: "varlen length truncated".into(),
            });
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            return Err(ColumnarError::CorruptedSegment {
                reason: "varlen value truncated".into(),
            });
        }
        values.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(values)
}

fn encode_dictionary(column: &Column) -> Result<Vec<u8>> {
    let values = match column {
        Column::Binary(v) => v,
        Column::Fixed { values, .. } => values,
        _ => {
            return Err(ColumnarError::CorruptedSegment {
                reason: "dictionary encoding requires binary data".into(),
            })
        }
    };

    let mut dict: Vec<Vec<u8>> = Vec::new();
    let mut indices = Vec::with_capacity(values.len());
    for v in values {
        if let Some((idx, _)) = dict.iter().enumerate().find(|(_, existing)| *existing == v) {
            indices.push(idx as u32);
        } else {
            let idx = dict.len() as u32;
            dict.push(v.clone());
            indices.push(idx);
        }
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    for entry in &dict {
        let len: u32 = entry
            .len()
            .try_into()
            .map_err(|_| ColumnarError::CorruptedSegment {
                reason: "dict entry too long".into(),
            })?;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(entry);
    }
    for idx in indices {
        buf.extend_from_slice(&idx.to_le_bytes());
    }
    Ok(buf)
}

fn decode_dictionary(bytes: &[u8], logical: LogicalType) -> Result<Column> {
    if bytes.len() < 8 {
        return Err(ColumnarError::CorruptedSegment {
            reason: "dictionary header too short".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let dict_count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;

    let mut pos = 8;
    let mut dict = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        if pos + 4 > bytes.len() {
            return Err(ColumnarError::CorruptedSegment {
                reason: "dict length truncated".into(),
            });
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            return Err(ColumnarError::CorruptedSegment {
                reason: "dict entry truncated".into(),
            });
        }
        dict.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }

    let expected_idx_bytes =
        count
            .checked_mul(4)
            .ok_or_else(|| ColumnarError::CorruptedSegment {
                reason: "index overflow".into(),
            })?;
    if pos + expected_idx_bytes > bytes.len() {
        return Err(ColumnarError::CorruptedSegment {
            reason: "dictionary indices truncated".into(),
        });
    }

    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let idx = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let entry = dict
            .get(idx)
            .ok_or_else(|| ColumnarError::CorruptedSegment {
                reason: "dictionary index out of bounds".into(),
            })?;
        values.push(entry.clone());
    }

    match logical {
        LogicalType::Binary => Ok(Column::Binary(values)),
        LogicalType::Fixed(len) => {
            for v in &values {
                if v.len() != len as usize {
                    return Err(ColumnarError::CorruptedSegment {
                        reason: "fixed length mismatch".into(),
                    });
                }
            }
            Ok(Column::Fixed {
                len: len as usize,
                values,
            })
        }
        _ => Err(ColumnarError::CorruptedSegment {
            reason: "dictionary logical mismatch".into(),
        }),
    }
}

fn encode_rle(column: &Column) -> Result<Vec<u8>> {
    match column {
        Column::Int64(values) => {
            encode_rle_nums(values.iter().map(|v| v.to_le_bytes().to_vec()), 8)
        }
        Column::Float64(values) => {
            encode_rle_nums(values.iter().map(|v| v.to_le_bytes().to_vec()), 8)
        }
        Column::Bool(values) => {
            let mut runs = Vec::new();
            let mut iter = values.iter().copied();
            if let Some(mut current) = iter.next() {
                let mut len = 1u32;
                for v in iter {
                    if v == current && len < u32::MAX {
                        len += 1;
                    } else {
                        runs.push((current as u8, len));
                        current = v;
                        len = 1;
                    }
                }
                runs.push((current as u8, len));
            }
            let mut buf = Vec::new();
            buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
            buf.extend_from_slice(&(runs.len() as u32).to_le_bytes());
            for (val, len) in runs {
                buf.push(val);
                buf.extend_from_slice(&len.to_le_bytes());
            }
            Ok(buf)
        }
        _ => Err(ColumnarError::CorruptedSegment {
            reason: "rle only supports numeric/bool".into(),
        }),
    }
}

fn encode_rle_nums<I>(iter: I, width: usize) -> Result<Vec<u8>>
where
    I: Iterator<Item = Vec<u8>>,
{
    let mut runs: Vec<(Vec<u8>, u32)> = Vec::new();
    let mut it = iter.peekable();
    if let Some(first) = it.next() {
        let mut current = first;
        let mut len = 1u32;
        for v in it {
            if v == current && len < u32::MAX {
                len += 1;
            } else {
                runs.push((current, len));
                current = v;
                len = 1;
            }
        }
        runs.push((current, len));
    }

    let mut buf = Vec::new();
    let total: u32 = runs.iter().map(|(_, l)| *l).sum();
    buf.extend_from_slice(&total.to_le_bytes());
    buf.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (val, len) in runs {
        if val.len() != width {
            return Err(ColumnarError::CorruptedSegment {
                reason: "rle width mismatch".into(),
            });
        }
        buf.extend_from_slice(&val);
        buf.extend_from_slice(&len.to_le_bytes());
    }
    Ok(buf)
}

fn decode_rle(bytes: &[u8], logical: LogicalType) -> Result<Column> {
    if bytes.len() < 8 {
        return Err(ColumnarError::CorruptedSegment {
            reason: "rle header too short".into(),
        });
    }
    let total = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let run_count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut pos = 8;

    match logical {
        LogicalType::Int64 | LogicalType::Float64 => {
            let width = 8;
            let mut out: Vec<Vec<u8>> = Vec::with_capacity(run_count);
            let mut lengths = Vec::with_capacity(run_count);
            for _ in 0..run_count {
                if pos + width + 4 > bytes.len() {
                    return Err(ColumnarError::CorruptedSegment {
                        reason: "rle numeric truncated".into(),
                    });
                }
                out.push(bytes[pos..pos + width].to_vec());
                pos += width;
                lengths.push(u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize);
                pos += 4;
            }
            let mut values = Vec::with_capacity(total);
            for (val_bytes, len) in out.into_iter().zip(lengths) {
                for _ in 0..len {
                    let val_arr: [u8; 8] = val_bytes.as_slice().try_into().unwrap();
                    let v = if matches!(logical, LogicalType::Int64) {
                        ColumnValue::I64(i64::from_le_bytes(val_arr))
                    } else {
                        ColumnValue::F64(f64::from_le_bytes(val_arr))
                    };
                    values.push(v);
                }
            }
            match logical {
                LogicalType::Int64 => Ok(Column::Int64(
                    values
                        .into_iter()
                        .map(|v| match v {
                            ColumnValue::I64(x) => x,
                            _ => unreachable!(),
                        })
                        .collect(),
                )),
                LogicalType::Float64 => Ok(Column::Float64(
                    values
                        .into_iter()
                        .map(|v| match v {
                            ColumnValue::F64(x) => x,
                            _ => unreachable!(),
                        })
                        .collect(),
                )),
                _ => unreachable!(),
            }
        }
        LogicalType::Bool => {
            let mut runs = Vec::with_capacity(run_count);
            for _ in 0..run_count {
                if pos + 5 > bytes.len() {
                    return Err(ColumnarError::CorruptedSegment {
                        reason: "rle bool truncated".into(),
                    });
                }
                let val = bytes[pos] != 0;
                pos += 1;
                let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                runs.push((val, len));
            }
            let mut out = Vec::with_capacity(total);
            for (val, len) in runs {
                out.extend(std::iter::repeat(val).take(len));
            }
            Ok(Column::Bool(out))
        }
        _ => Err(ColumnarError::CorruptedSegment {
            reason: "rle logical mismatch".into(),
        }),
    }
}

enum ColumnValue {
    I64(i64),
    F64(f64),
}

fn encode_bitpack(column: &Column) -> Result<Vec<u8>> {
    let values = match column {
        Column::Bool(v) => v,
        _ => {
            return Err(ColumnarError::CorruptedSegment {
                reason: "bitpack supports bool only".into(),
            })
        }
    };
    let count = values.len();
    let mut buf = Vec::with_capacity(4 + (count + 7) / 8);
    buf.extend_from_slice(&(count as u32).to_le_bytes());
    let mut current = 0u8;
    let mut bit = 0;
    for v in values {
        if *v {
            current |= 1 << bit;
        }
        bit += 1;
        if bit == 8 {
            buf.push(current);
            current = 0;
            bit = 0;
        }
    }
    if bit > 0 {
        buf.push(current);
    }
    Ok(buf)
}

fn decode_bitpack(bytes: &[u8], logical: LogicalType) -> Result<Column> {
    if logical != LogicalType::Bool {
        return Err(ColumnarError::CorruptedSegment {
            reason: "bitpack logical mismatch".into(),
        });
    }
    if bytes.len() < 4 {
        return Err(ColumnarError::CorruptedSegment {
            reason: "bitpack header too short".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let needed = 4 + (count + 7) / 8;
    if bytes.len() < needed {
        return Err(ColumnarError::CorruptedSegment {
            reason: "bitpack data truncated".into(),
        });
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let byte = bytes[4 + (i / 8)];
        let bit = i % 8;
        out.push(byte & (1 << bit) != 0);
    }
    Ok(Column::Bool(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_int64_roundtrip() {
        let col = Column::Int64(vec![1, -2, 3]);
        let encoded = encode_column(
            &col,
            Encoding::Plain,
            Compression::None,
            true,
            LogicalType::Int64,
        )
        .unwrap();
        let decoded = decode_column(
            &encoded,
            LogicalType::Int64,
            Encoding::Plain,
            Compression::None,
            true,
        )
        .unwrap();
        assert_eq!(col, decoded);
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn dictionary_binary_roundtrip_lz4() {
        let col = Column::Binary(vec![b"aa".to_vec(), b"bb".to_vec(), b"aa".to_vec()]);
        let encoded = encode_column(
            &col,
            Encoding::Dictionary,
            Compression::Lz4,
            true,
            LogicalType::Binary,
        )
        .unwrap();
        let decoded = decode_column(
            &encoded,
            LogicalType::Binary,
            Encoding::Dictionary,
            Compression::Lz4,
            true,
        )
        .unwrap();
        assert_eq!(col, decoded);
    }

    #[test]
    fn rle_bool_roundtrip() {
        let col = Column::Bool(vec![true, true, true, false, false, true]);
        let encoded = encode_column(
            &col,
            Encoding::Rle,
            Compression::None,
            false,
            LogicalType::Bool,
        )
        .unwrap();
        let decoded = decode_column(
            &encoded,
            LogicalType::Bool,
            Encoding::Rle,
            Compression::None,
            false,
        )
        .unwrap();
        assert_eq!(col, decoded);
    }

    #[test]
    fn bitpack_bool_roundtrip() {
        let col = Column::Bool(vec![
            true, false, true, true, false, false, false, true, true,
        ]);
        let encoded = encode_column(
            &col,
            Encoding::Bitpack,
            Compression::None,
            true,
            LogicalType::Bool,
        )
        .unwrap();
        let decoded = decode_column(
            &encoded,
            LogicalType::Bool,
            Encoding::Bitpack,
            Compression::None,
            true,
        )
        .unwrap();
        assert_eq!(col, decoded);
    }

    #[test]
    fn checksum_mismatch_detected() {
        let col = Column::Int64(vec![42]);
        let mut encoded = encode_column(
            &col,
            Encoding::Plain,
            Compression::None,
            true,
            LogicalType::Int64,
        )
        .unwrap();
        encoded[5] ^= 0xFF; // flip a byte in payload
        let err = decode_column(
            &encoded,
            LogicalType::Int64,
            Encoding::Plain,
            Compression::None,
            true,
        )
        .unwrap_err();
        assert!(matches!(err, ColumnarError::ChecksumMismatch));
    }
}
