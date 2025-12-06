//! V2 Encoding algorithms for columnar storage.
//!
//! Extends the base encoding with advanced algorithms:
//! - Delta encoding for sorted integers
//! - Frame of Reference (FOR) for small-range integers
//! - Patched FOR (PFOR) for integers with outliers
//! - Byte Stream Split for floating point
//! - Incremental String for sorted strings

use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

use super::encoding::{Column, LogicalType};

/// V2 Encoding strategy for a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncodingV2 {
    /// Raw values (V1 compatible).
    Plain,
    /// Dictionary encoding with indexes (V1 compatible).
    Dictionary,
    /// Run-length encoding (V1 compatible).
    Rle,
    /// Bit-packed representation for bools (V1 compatible).
    Bitpack,
    /// Delta encoding for sorted integers.
    Delta,
    /// Delta-length encoding for variable-length data.
    DeltaLength,
    /// Byte stream split for floating point values.
    ByteStreamSplit,
    /// Frame of Reference encoding for small-range integers.
    FOR,
    /// Patched Frame of Reference for integers with outliers.
    PFOR,
    /// Incremental string encoding for sorted strings.
    IncrementalString,
}

/// Null bitmap for nullable columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bitmap {
    /// Bit vector where 1 = valid, 0 = null.
    bits: Vec<u8>,
    /// Number of values.
    len: usize,
}

impl Bitmap {
    /// Create a new bitmap with all values valid.
    pub fn all_valid(len: usize) -> Self {
        let num_bytes = len.div_ceil(8);
        Self {
            bits: vec![0xFF; num_bytes],
            len,
        }
    }

    /// Create a new bitmap from a boolean slice.
    pub fn from_bools(valid: &[bool]) -> Self {
        let len = valid.len();
        let num_bytes = len.div_ceil(8);
        let mut bits = vec![0u8; num_bytes];
        for (i, &v) in valid.iter().enumerate() {
            if v {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        Self { bits, len }
    }

    /// Create a new bitmap with all values set to invalid (null).
    pub fn new(len: usize) -> Self {
        let num_bytes = len.div_ceil(8);
        Self {
            bits: vec![0u8; num_bytes],
            len,
        }
    }

    /// Check if value at index is valid (not null).
    pub fn is_valid(&self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        (self.bits[index / 8] & (1 << (index % 8))) != 0
    }

    /// Alias for is_valid - get the validity at index.
    pub fn get(&self, index: usize) -> bool {
        self.is_valid(index)
    }

    /// Set the validity at index.
    pub fn set(&mut self, index: usize, valid: bool) {
        if index >= self.len {
            return;
        }
        if valid {
            self.bits[index / 8] |= 1 << (index % 8);
        } else {
            self.bits[index / 8] &= !(1 << (index % 8));
        }
    }

    /// Get the number of null values.
    pub fn null_count(&self) -> usize {
        self.len
            - self
                .bits
                .iter()
                .map(|b| b.count_ones() as usize)
                .sum::<usize>()
    }

    /// Encode bitmap to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.bits.len());
        buf.extend_from_slice(&(self.len as u32).to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Decode bitmap from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(Error::InvalidFormat("bitmap header too short".into()));
        }
        let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let num_bytes = len.div_ceil(8);
        if bytes.len() < 4 + num_bytes {
            return Err(Error::InvalidFormat("bitmap data truncated".into()));
        }
        Ok(Self {
            bits: bytes[4..4 + num_bytes].to_vec(),
            len,
        })
    }

    /// Get the length.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Encoder trait for V2 encodings.
pub trait Encoder: Send + Sync {
    /// Encode column data with optional null bitmap.
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>>;
    /// Get the encoding type.
    fn encoding_type(&self) -> EncodingV2;
}

/// Decoder trait for V2 encodings.
pub trait Decoder: Send + Sync {
    /// Decode bytes to column data with optional null bitmap.
    fn decode(
        &self,
        data: &[u8],
        num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)>;
}

// ============================================================================
// Delta Encoding - for sorted integers
// ============================================================================

/// Delta encoder for sorted integer sequences.
///
/// Stores first value + deltas between consecutive values.
/// Efficient for monotonically increasing sequences (timestamps, IDs).
pub struct DeltaEncoder;

impl Encoder for DeltaEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        let values = match data {
            Column::Int64(v) => v,
            _ => return Err(Error::InvalidFormat("delta encoding requires Int64".into())),
        };

        if values.is_empty() {
            let mut buf = Vec::with_capacity(8);
            buf.extend_from_slice(&0u32.to_le_bytes()); // count
            buf.extend_from_slice(&0u8.to_le_bytes()); // has_bitmap
            return Ok(buf);
        }

        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // Write bitmap flag and data
        if let Some(bitmap) = null_bitmap {
            buf.push(1u8);
            buf.extend_from_slice(&bitmap.to_bytes());
        } else {
            buf.push(0u8);
        }

        // First value
        buf.extend_from_slice(&values[0].to_le_bytes());

        // Deltas (variable-length zigzag encoded)
        for window in values.windows(2) {
            let delta = window[1].wrapping_sub(window[0]);
            let zigzag = zigzag_encode(delta);
            encode_varint(zigzag, &mut buf);
        }

        Ok(buf)
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::Delta
    }
}

/// Delta decoder.
pub struct DeltaDecoder;

impl Decoder for DeltaDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        if logical_type != LogicalType::Int64 {
            return Err(Error::InvalidFormat("delta decoding requires Int64".into()));
        }

        if data.len() < 5 {
            return Err(Error::InvalidFormat("delta header too short".into()));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let has_bitmap = data[4] != 0;
        let mut pos = 5;

        let bitmap = if has_bitmap {
            let bm = Bitmap::from_bytes(&data[pos..])?;
            pos += 4 + bm.len().div_ceil(8);
            Some(bm)
        } else {
            None
        };

        if count == 0 {
            return Ok((Column::Int64(vec![]), bitmap));
        }

        if pos + 8 > data.len() {
            return Err(Error::InvalidFormat("delta first value truncated".into()));
        }

        let first = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let mut values = Vec::with_capacity(count);
        values.push(first);

        let mut current = first;
        for _ in 1..count {
            let (zigzag, bytes_read) = decode_varint(&data[pos..])?;
            pos += bytes_read;
            let delta = zigzag_decode(zigzag);
            current = current.wrapping_add(delta);
            values.push(current);
        }

        Ok((Column::Int64(values), bitmap))
    }
}

// ============================================================================
// FOR (Frame of Reference) Encoding - for small-range integers
// ============================================================================

/// Frame of Reference encoder.
///
/// Stores min value + bit-packed offsets from min.
/// Efficient when all values fit in a small range.
pub struct ForEncoder;

impl Encoder for ForEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        let values = match data {
            Column::Int64(v) => v,
            _ => return Err(Error::InvalidFormat("FOR encoding requires Int64".into())),
        };

        if values.is_empty() {
            let mut buf = Vec::with_capacity(5);
            buf.extend_from_slice(&0u32.to_le_bytes());
            buf.push(0u8); // has_bitmap
            return Ok(buf);
        }

        let min_val = *values.iter().min().unwrap();
        let max_val = *values.iter().max().unwrap();
        let range = (max_val - min_val) as u64;

        // Calculate bits needed
        let bits_needed = if range == 0 {
            1
        } else {
            64 - range.leading_zeros()
        } as u8;

        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // Write bitmap flag and data
        if let Some(bitmap) = null_bitmap {
            buf.push(1u8);
            buf.extend_from_slice(&bitmap.to_bytes());
        } else {
            buf.push(0u8);
        }

        // Reference value and bit width
        buf.extend_from_slice(&min_val.to_le_bytes());
        buf.push(bits_needed);

        // Bit-pack the offsets
        let mut bit_buffer = 0u64;
        let mut bits_in_buffer = 0u8;

        for &v in values {
            let offset = (v - min_val) as u64;
            bit_buffer |= offset << bits_in_buffer;
            bits_in_buffer += bits_needed;

            while bits_in_buffer >= 8 {
                buf.push(bit_buffer as u8);
                bit_buffer >>= 8;
                bits_in_buffer -= 8;
            }
        }

        // Flush remaining bits
        if bits_in_buffer > 0 {
            buf.push(bit_buffer as u8);
        }

        Ok(buf)
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::FOR
    }
}

/// Frame of Reference decoder.
pub struct ForDecoder;

impl Decoder for ForDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        if logical_type != LogicalType::Int64 {
            return Err(Error::InvalidFormat("FOR decoding requires Int64".into()));
        }

        if data.len() < 5 {
            return Err(Error::InvalidFormat("FOR header too short".into()));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let has_bitmap = data[4] != 0;
        let mut pos = 5;

        let bitmap = if has_bitmap {
            let bm = Bitmap::from_bytes(&data[pos..])?;
            pos += 4 + bm.len().div_ceil(8);
            Some(bm)
        } else {
            None
        };

        if count == 0 {
            return Ok((Column::Int64(vec![]), bitmap));
        }

        if pos + 9 > data.len() {
            return Err(Error::InvalidFormat("FOR reference truncated".into()));
        }

        let reference = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let bits_per_value = data[pos];
        pos += 1;

        let mut values = Vec::with_capacity(count);
        let mask = if bits_per_value >= 64 {
            u64::MAX
        } else {
            (1u64 << bits_per_value) - 1
        };

        let mut bit_buffer = 0u64;
        let mut bits_in_buffer = 0u8;
        let mut byte_pos = pos;

        for _ in 0..count {
            while bits_in_buffer < bits_per_value && byte_pos < data.len() {
                bit_buffer |= (data[byte_pos] as u64) << bits_in_buffer;
                bits_in_buffer += 8;
                byte_pos += 1;
            }

            // Check for truncated data before subtraction to prevent underflow
            if bits_in_buffer < bits_per_value {
                return Err(Error::InvalidFormat("FOR data truncated".into()));
            }

            let offset = bit_buffer & mask;
            bit_buffer >>= bits_per_value;
            bits_in_buffer -= bits_per_value;

            values.push(reference + offset as i64);
        }

        Ok((Column::Int64(values), bitmap))
    }
}

// ============================================================================
// PFOR (Patched Frame of Reference) - for integers with outliers
// ============================================================================

/// Patched FOR encoder.
///
/// Like FOR but handles outliers separately.
/// Efficient when most values fit in small range with few exceptions.
pub struct PforEncoder {
    /// Percentile threshold for determining bit width (0.0-1.0).
    pub percentile: f64,
}

impl Default for PforEncoder {
    fn default() -> Self {
        Self { percentile: 0.9 }
    }
}

impl Encoder for PforEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        let values = match data {
            Column::Int64(v) => v,
            _ => return Err(Error::InvalidFormat("PFOR encoding requires Int64".into())),
        };

        if values.is_empty() {
            let mut buf = Vec::with_capacity(5);
            buf.extend_from_slice(&0u32.to_le_bytes());
            buf.push(0u8); // has_bitmap
            return Ok(buf);
        }

        let min_val = *values.iter().min().unwrap();

        // Calculate offsets and find percentile-based max
        let mut offsets: Vec<u64> = values.iter().map(|&v| (v - min_val) as u64).collect();
        let mut sorted_offsets = offsets.clone();
        sorted_offsets.sort_unstable();

        let percentile_idx =
            ((values.len() as f64 * self.percentile) as usize).min(values.len() - 1);
        let percentile_max = sorted_offsets[percentile_idx];

        let bits_needed = if percentile_max == 0 {
            1
        } else {
            64 - percentile_max.leading_zeros()
        } as u8;

        let max_packed = if bits_needed >= 64 {
            u64::MAX
        } else {
            (1u64 << bits_needed) - 1
        };

        // Find exceptions (values that don't fit)
        let mut exceptions: Vec<(u32, u64)> = Vec::new();
        for (i, offset) in offsets.iter_mut().enumerate() {
            if *offset > max_packed {
                exceptions.push((i as u32, *offset));
                *offset = 0; // Will be patched
            }
        }

        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // Write bitmap flag and data
        if let Some(bitmap) = null_bitmap {
            buf.push(1u8);
            buf.extend_from_slice(&bitmap.to_bytes());
        } else {
            buf.push(0u8);
        }

        // Reference, bit width, exception count
        buf.extend_from_slice(&min_val.to_le_bytes());
        buf.push(bits_needed);
        buf.extend_from_slice(&(exceptions.len() as u32).to_le_bytes());

        // Bit-pack main values
        let mut bit_buffer = 0u64;
        let mut bits_in_buffer = 0u8;

        for &offset in &offsets {
            bit_buffer |= (offset & max_packed) << bits_in_buffer;
            bits_in_buffer += bits_needed;

            while bits_in_buffer >= 8 {
                buf.push(bit_buffer as u8);
                bit_buffer >>= 8;
                bits_in_buffer -= 8;
            }
        }

        if bits_in_buffer > 0 {
            buf.push(bit_buffer as u8);
        }

        // Write exceptions
        for (idx, val) in exceptions {
            buf.extend_from_slice(&idx.to_le_bytes());
            buf.extend_from_slice(&val.to_le_bytes());
        }

        Ok(buf)
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::PFOR
    }
}

/// Patched FOR decoder.
pub struct PforDecoder;

impl Decoder for PforDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        if logical_type != LogicalType::Int64 {
            return Err(Error::InvalidFormat("PFOR decoding requires Int64".into()));
        }

        if data.len() < 5 {
            return Err(Error::InvalidFormat("PFOR header too short".into()));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let has_bitmap = data[4] != 0;
        let mut pos = 5;

        let bitmap = if has_bitmap {
            let bm = Bitmap::from_bytes(&data[pos..])?;
            pos += 4 + bm.len().div_ceil(8);
            Some(bm)
        } else {
            None
        };

        if count == 0 {
            return Ok((Column::Int64(vec![]), bitmap));
        }

        if pos + 13 > data.len() {
            return Err(Error::InvalidFormat("PFOR header truncated".into()));
        }

        let reference = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let bits_per_value = data[pos];
        pos += 1;
        let exception_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Decode bit-packed values
        let mut values = Vec::with_capacity(count);
        let mask = if bits_per_value >= 64 {
            u64::MAX
        } else {
            (1u64 << bits_per_value) - 1
        };

        let mut bit_buffer = 0u64;
        let mut bits_in_buffer = 0u8;

        for _ in 0..count {
            while bits_in_buffer < bits_per_value && pos < data.len() {
                bit_buffer |= (data[pos] as u64) << bits_in_buffer;
                bits_in_buffer += 8;
                pos += 1;
            }

            // Check for truncated data before subtraction to prevent underflow
            if bits_in_buffer < bits_per_value {
                return Err(Error::InvalidFormat("PFOR data truncated".into()));
            }

            let offset = bit_buffer & mask;
            bit_buffer >>= bits_per_value;
            bits_in_buffer -= bits_per_value;

            values.push(reference + offset as i64);
        }

        // Align to byte boundary for exceptions
        if bits_in_buffer > 0 && bits_in_buffer < 8 {
            // Skip partial byte already consumed
        }

        // Apply exceptions
        for _ in 0..exception_count {
            if pos + 12 > data.len() {
                return Err(Error::InvalidFormat("PFOR exception truncated".into()));
            }
            let idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let val = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            if idx < values.len() {
                values[idx] = reference + val as i64;
            }
        }

        Ok((Column::Int64(values), bitmap))
    }
}

// ============================================================================
// ByteStreamSplit - for floating point
// ============================================================================

/// Byte stream split encoder.
///
/// Splits float bytes into separate streams for better compression.
/// Each byte position forms its own stream.
pub struct ByteStreamSplitEncoder;

impl Encoder for ByteStreamSplitEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        let (bytes_per_value, raw_bytes): (usize, Vec<u8>) = match data {
            Column::Float64(values) => {
                let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
                (8, bytes)
            }
            Column::Int64(values) => {
                let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
                (8, bytes)
            }
            _ => {
                return Err(Error::InvalidFormat(
                    "ByteStreamSplit requires Float64 or Int64".into(),
                ))
            }
        };

        let num_values = raw_bytes.len() / bytes_per_value;

        let mut buf = Vec::new();
        buf.extend_from_slice(&(num_values as u32).to_le_bytes());
        buf.push(bytes_per_value as u8);

        // Write bitmap flag and data
        if let Some(bitmap) = null_bitmap {
            buf.push(1u8);
            buf.extend_from_slice(&bitmap.to_bytes());
        } else {
            buf.push(0u8);
        }

        // Split into byte streams
        for byte_idx in 0..bytes_per_value {
            for value_idx in 0..num_values {
                buf.push(raw_bytes[value_idx * bytes_per_value + byte_idx]);
            }
        }

        Ok(buf)
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::ByteStreamSplit
    }
}

/// Byte stream split decoder.
pub struct ByteStreamSplitDecoder;

impl Decoder for ByteStreamSplitDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        if data.len() < 6 {
            return Err(Error::InvalidFormat(
                "ByteStreamSplit header too short".into(),
            ));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let bytes_per_value = data[4] as usize;
        let has_bitmap = data[5] != 0;
        let mut pos = 6;

        let bitmap = if has_bitmap {
            let bm = Bitmap::from_bytes(&data[pos..])?;
            pos += 4 + bm.len().div_ceil(8);
            Some(bm)
        } else {
            None
        };

        if count == 0 {
            return match logical_type {
                LogicalType::Float64 => Ok((Column::Float64(vec![]), bitmap)),
                LogicalType::Int64 => Ok((Column::Int64(vec![]), bitmap)),
                _ => Err(Error::InvalidFormat(
                    "ByteStreamSplit logical type mismatch".into(),
                )),
            };
        }

        let expected_size = count * bytes_per_value;
        if pos + expected_size > data.len() {
            return Err(Error::InvalidFormat(
                "ByteStreamSplit data truncated".into(),
            ));
        }

        // Reconstruct values from byte streams
        let mut raw_bytes = vec![0u8; expected_size];
        for byte_idx in 0..bytes_per_value {
            for value_idx in 0..count {
                raw_bytes[value_idx * bytes_per_value + byte_idx] = data[pos];
                pos += 1;
            }
        }

        match logical_type {
            LogicalType::Float64 => {
                let values: Vec<f64> = raw_bytes
                    .chunks_exact(8)
                    .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
                    .collect();
                Ok((Column::Float64(values), bitmap))
            }
            LogicalType::Int64 => {
                let values: Vec<i64> = raw_bytes
                    .chunks_exact(8)
                    .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
                    .collect();
                Ok((Column::Int64(values), bitmap))
            }
            _ => Err(Error::InvalidFormat(
                "ByteStreamSplit requires Float64 or Int64".into(),
            )),
        }
    }
}

// ============================================================================
// IncrementalString - for sorted strings
// ============================================================================

/// Incremental string encoder.
///
/// Stores common prefix length + suffix for sorted strings.
/// Efficient for lexicographically sorted string columns.
pub struct IncrementalStringEncoder;

impl Encoder for IncrementalStringEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        let values = match data {
            Column::Binary(v) => v,
            _ => {
                return Err(Error::InvalidFormat(
                    "IncrementalString encoding requires Binary".into(),
                ))
            }
        };

        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // Write bitmap flag and data
        if let Some(bitmap) = null_bitmap {
            buf.push(1u8);
            buf.extend_from_slice(&bitmap.to_bytes());
        } else {
            buf.push(0u8);
        }

        if values.is_empty() {
            return Ok(buf);
        }

        // First value: full length + data
        buf.extend_from_slice(&(values[0].len() as u32).to_le_bytes());
        buf.extend_from_slice(&values[0]);

        // Subsequent values: prefix length + suffix
        for window in values.windows(2) {
            let prev = &window[0];
            let curr = &window[1];

            let common_prefix = prev
                .iter()
                .zip(curr.iter())
                .take_while(|(a, b)| a == b)
                .count();

            let suffix = &curr[common_prefix..];

            buf.extend_from_slice(&(common_prefix as u16).to_le_bytes());
            buf.extend_from_slice(&(suffix.len() as u16).to_le_bytes());
            buf.extend_from_slice(suffix);
        }

        Ok(buf)
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::IncrementalString
    }
}

/// Incremental string decoder.
pub struct IncrementalStringDecoder;

impl Decoder for IncrementalStringDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        if logical_type != LogicalType::Binary {
            return Err(Error::InvalidFormat(
                "IncrementalString decoding requires Binary".into(),
            ));
        }

        if data.len() < 5 {
            return Err(Error::InvalidFormat(
                "IncrementalString header too short".into(),
            ));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let has_bitmap = data[4] != 0;
        let mut pos = 5;

        let bitmap = if has_bitmap {
            let bm = Bitmap::from_bytes(&data[pos..])?;
            pos += 4 + bm.len().div_ceil(8);
            Some(bm)
        } else {
            None
        };

        if count == 0 {
            return Ok((Column::Binary(vec![]), bitmap));
        }

        let mut values = Vec::with_capacity(count);

        // First value
        if pos + 4 > data.len() {
            return Err(Error::InvalidFormat(
                "IncrementalString first len truncated".into(),
            ));
        }
        let first_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + first_len > data.len() {
            return Err(Error::InvalidFormat(
                "IncrementalString first value truncated".into(),
            ));
        }
        let mut current = data[pos..pos + first_len].to_vec();
        pos += first_len;
        values.push(current.clone());

        // Subsequent values
        for _ in 1..count {
            if pos + 4 > data.len() {
                return Err(Error::InvalidFormat(
                    "IncrementalString header truncated".into(),
                ));
            }
            let prefix_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let suffix_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            if pos + suffix_len > data.len() {
                return Err(Error::InvalidFormat(
                    "IncrementalString suffix truncated".into(),
                ));
            }

            current.truncate(prefix_len);
            current.extend_from_slice(&data[pos..pos + suffix_len]);
            pos += suffix_len;

            values.push(current.clone());
        }

        Ok((Column::Binary(values), bitmap))
    }
}

// ============================================================================
// RLE (Run-Length Encoding) - for Bool/Int64
// ============================================================================

/// Run-Length encoder for Bool and Int64.
///
/// Format: count(u32) + has_bitmap(u8) + bitmap? + run_count(u32) + {value, run_len}*
/// - For Bool: value is 1 byte (0 or 1)
/// - For Int64: value is 8 bytes (i64 LE)
/// - run_len is u32 LE
pub struct RleEncoder;

impl Encoder for RleEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        match data {
            Column::Bool(values) => encode_rle_bool(values, null_bitmap),
            Column::Int64(values) => encode_rle_int64(values, null_bitmap),
            _ => Err(Error::InvalidFormat(
                "RLE encoding requires Bool or Int64".into(),
            )),
        }
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::Rle
    }
}

fn encode_rle_bool(values: &[bool], null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

    // Write bitmap flag and data
    if let Some(bitmap) = null_bitmap {
        buf.push(1u8);
        buf.extend_from_slice(&bitmap.to_bytes());
    } else {
        buf.push(0u8);
    }

    if values.is_empty() {
        buf.extend_from_slice(&0u32.to_le_bytes()); // run_count = 0
        return Ok(buf);
    }

    // Build runs
    let mut runs: Vec<(bool, u32)> = Vec::new();
    let mut current_value = values[0];
    let mut current_run_len = 1u32;

    for &v in values.iter().skip(1) {
        if v == current_value {
            current_run_len += 1;
        } else {
            runs.push((current_value, current_run_len));
            current_value = v;
            current_run_len = 1;
        }
    }
    runs.push((current_value, current_run_len));

    // Write run count
    buf.extend_from_slice(&(runs.len() as u32).to_le_bytes());

    // Write runs: {value(1 byte), run_len(4 bytes)}*
    for (value, run_len) in runs {
        buf.push(value as u8);
        buf.extend_from_slice(&run_len.to_le_bytes());
    }

    Ok(buf)
}

fn encode_rle_int64(values: &[i64], null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

    // Write bitmap flag and data
    if let Some(bitmap) = null_bitmap {
        buf.push(1u8);
        buf.extend_from_slice(&bitmap.to_bytes());
    } else {
        buf.push(0u8);
    }

    if values.is_empty() {
        buf.extend_from_slice(&0u32.to_le_bytes()); // run_count = 0
        return Ok(buf);
    }

    // Build runs
    let mut runs: Vec<(i64, u32)> = Vec::new();
    let mut current_value = values[0];
    let mut current_run_len = 1u32;

    for &v in values.iter().skip(1) {
        if v == current_value {
            current_run_len += 1;
        } else {
            runs.push((current_value, current_run_len));
            current_value = v;
            current_run_len = 1;
        }
    }
    runs.push((current_value, current_run_len));

    // Write run count
    buf.extend_from_slice(&(runs.len() as u32).to_le_bytes());

    // Write runs: {value(8 bytes), run_len(4 bytes)}*
    for (value, run_len) in runs {
        buf.extend_from_slice(&value.to_le_bytes());
        buf.extend_from_slice(&run_len.to_le_bytes());
    }

    Ok(buf)
}

/// Run-Length decoder for Bool and Int64.
pub struct RleDecoder;

impl Decoder for RleDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        match logical_type {
            LogicalType::Bool => decode_rle_bool(data),
            LogicalType::Int64 => decode_rle_int64(data),
            _ => Err(Error::InvalidFormat(
                "RLE decoding requires Bool or Int64".into(),
            )),
        }
    }
}

fn decode_rle_bool(data: &[u8]) -> Result<(Column, Option<Bitmap>)> {
    if data.len() < 5 {
        return Err(Error::InvalidFormat("RLE header too short".into()));
    }

    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let has_bitmap = data[4] != 0;
    let mut pos = 5;

    let bitmap = if has_bitmap {
        let bm = Bitmap::from_bytes(&data[pos..])?;
        pos += 4 + bm.len().div_ceil(8);
        Some(bm)
    } else {
        None
    };

    if pos + 4 > data.len() {
        return Err(Error::InvalidFormat("RLE run_count truncated".into()));
    }

    let run_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    if count == 0 {
        return Ok((Column::Bool(vec![]), bitmap));
    }

    let mut values = Vec::with_capacity(count);

    for _ in 0..run_count {
        if pos + 5 > data.len() {
            return Err(Error::InvalidFormat("RLE bool run truncated".into()));
        }
        let value = data[pos] != 0;
        pos += 1;
        let run_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        for _ in 0..run_len {
            values.push(value);
        }
    }

    if values.len() != count {
        return Err(Error::InvalidFormat("RLE count mismatch".into()));
    }

    Ok((Column::Bool(values), bitmap))
}

fn decode_rle_int64(data: &[u8]) -> Result<(Column, Option<Bitmap>)> {
    if data.len() < 5 {
        return Err(Error::InvalidFormat("RLE header too short".into()));
    }

    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let has_bitmap = data[4] != 0;
    let mut pos = 5;

    let bitmap = if has_bitmap {
        let bm = Bitmap::from_bytes(&data[pos..])?;
        pos += 4 + bm.len().div_ceil(8);
        Some(bm)
    } else {
        None
    };

    if pos + 4 > data.len() {
        return Err(Error::InvalidFormat("RLE run_count truncated".into()));
    }

    let run_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    if count == 0 {
        return Ok((Column::Int64(vec![]), bitmap));
    }

    let mut values = Vec::with_capacity(count);

    for _ in 0..run_count {
        if pos + 12 > data.len() {
            return Err(Error::InvalidFormat("RLE int64 run truncated".into()));
        }
        let value = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let run_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        for _ in 0..run_len {
            values.push(value);
        }
    }

    if values.len() != count {
        return Err(Error::InvalidFormat("RLE count mismatch".into()));
    }

    Ok((Column::Int64(values), bitmap))
}

// ============================================================================
// Dictionary Encoding - for Binary/Fixed
// ============================================================================

/// Dictionary encoder for Binary and Fixed columns.
///
/// Format: count(u32) + has_bitmap(u8) + bitmap? + dict_count(u32) + dict_entries + indices[u32]*
/// - dict_entries for Binary: {len(u32) + bytes}*
/// - dict_entries for Fixed: bytes* (fixed length known from type)
/// - indices: u32 LE index into dictionary
pub struct DictionaryEncoder;

impl Encoder for DictionaryEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        match data {
            Column::Binary(values) => encode_dict_binary(values, null_bitmap),
            Column::Fixed { len, values } => encode_dict_fixed(*len, values, null_bitmap),
            _ => Err(Error::InvalidFormat(
                "Dictionary encoding requires Binary or Fixed".into(),
            )),
        }
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::Dictionary
    }
}

fn encode_dict_binary(values: &[Vec<u8>], null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
    use std::collections::HashMap;

    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

    // Write bitmap flag and data
    if let Some(bitmap) = null_bitmap {
        buf.push(1u8);
        buf.extend_from_slice(&bitmap.to_bytes());
    } else {
        buf.push(0u8);
    }

    if values.is_empty() {
        buf.extend_from_slice(&0u32.to_le_bytes()); // dict_count = 0
        return Ok(buf);
    }

    // Build dictionary
    let mut dict: Vec<&Vec<u8>> = Vec::new();
    let mut dict_map: HashMap<&Vec<u8>, u32> = HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());

    for v in values {
        if let Some(&idx) = dict_map.get(v) {
            indices.push(idx);
        } else {
            let idx = dict.len() as u32;
            dict.push(v);
            dict_map.insert(v, idx);
            indices.push(idx);
        }
    }

    // Write dict_count
    buf.extend_from_slice(&(dict.len() as u32).to_le_bytes());

    // Write dictionary entries: {len(u32) + bytes}*
    for entry in &dict {
        buf.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        buf.extend_from_slice(entry);
    }

    // Write indices
    for idx in indices {
        buf.extend_from_slice(&idx.to_le_bytes());
    }

    Ok(buf)
}

fn encode_dict_fixed(
    fixed_len: usize,
    values: &[Vec<u8>],
    null_bitmap: Option<&Bitmap>,
) -> Result<Vec<u8>> {
    use std::collections::HashMap;

    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

    // Write bitmap flag and data
    if let Some(bitmap) = null_bitmap {
        buf.push(1u8);
        buf.extend_from_slice(&bitmap.to_bytes());
    } else {
        buf.push(0u8);
    }

    // Write fixed length
    buf.extend_from_slice(&(fixed_len as u16).to_le_bytes());

    if values.is_empty() {
        buf.extend_from_slice(&0u32.to_le_bytes()); // dict_count = 0
        return Ok(buf);
    }

    // Build dictionary
    let mut dict: Vec<&Vec<u8>> = Vec::new();
    let mut dict_map: HashMap<&Vec<u8>, u32> = HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());

    for v in values {
        if let Some(&idx) = dict_map.get(v) {
            indices.push(idx);
        } else {
            let idx = dict.len() as u32;
            dict.push(v);
            dict_map.insert(v, idx);
            indices.push(idx);
        }
    }

    // Write dict_count
    buf.extend_from_slice(&(dict.len() as u32).to_le_bytes());

    // Write dictionary entries: fixed-length bytes*
    for entry in &dict {
        buf.extend_from_slice(entry);
    }

    // Write indices
    for idx in indices {
        buf.extend_from_slice(&idx.to_le_bytes());
    }

    Ok(buf)
}

/// Dictionary decoder for Binary and Fixed columns.
pub struct DictionaryDecoder;

impl Decoder for DictionaryDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        match logical_type {
            LogicalType::Binary => decode_dict_binary(data),
            LogicalType::Fixed(fixed_len) => decode_dict_fixed(data, fixed_len as usize),
            _ => Err(Error::InvalidFormat(
                "Dictionary decoding requires Binary or Fixed".into(),
            )),
        }
    }
}

fn decode_dict_binary(data: &[u8]) -> Result<(Column, Option<Bitmap>)> {
    if data.len() < 5 {
        return Err(Error::InvalidFormat("Dictionary header too short".into()));
    }

    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let has_bitmap = data[4] != 0;
    let mut pos = 5;

    let bitmap = if has_bitmap {
        let bm = Bitmap::from_bytes(&data[pos..])?;
        pos += 4 + bm.len().div_ceil(8);
        Some(bm)
    } else {
        None
    };

    if pos + 4 > data.len() {
        return Err(Error::InvalidFormat(
            "Dictionary dict_count truncated".into(),
        ));
    }

    let dict_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    if count == 0 {
        return Ok((Column::Binary(vec![]), bitmap));
    }

    // Read dictionary entries
    let mut dict: Vec<Vec<u8>> = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        if pos + 4 > data.len() {
            return Err(Error::InvalidFormat(
                "Dictionary entry len truncated".into(),
            ));
        }
        let entry_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + entry_len > data.len() {
            return Err(Error::InvalidFormat(
                "Dictionary entry data truncated".into(),
            ));
        }
        dict.push(data[pos..pos + entry_len].to_vec());
        pos += entry_len;
    }

    // Read indices and reconstruct values
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > data.len() {
            return Err(Error::InvalidFormat("Dictionary index truncated".into()));
        }
        let idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if idx >= dict.len() {
            return Err(Error::InvalidFormat("Dictionary index out of range".into()));
        }
        values.push(dict[idx].clone());
    }

    Ok((Column::Binary(values), bitmap))
}

fn decode_dict_fixed(data: &[u8], expected_len: usize) -> Result<(Column, Option<Bitmap>)> {
    if data.len() < 5 {
        return Err(Error::InvalidFormat("Dictionary header too short".into()));
    }

    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let has_bitmap = data[4] != 0;
    let mut pos = 5;

    let bitmap = if has_bitmap {
        let bm = Bitmap::from_bytes(&data[pos..])?;
        pos += 4 + bm.len().div_ceil(8);
        Some(bm)
    } else {
        None
    };

    if pos + 2 > data.len() {
        return Err(Error::InvalidFormat(
            "Dictionary fixed_len truncated".into(),
        ));
    }
    let fixed_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;

    if fixed_len != expected_len {
        return Err(Error::InvalidFormat(
            "Dictionary fixed length mismatch".into(),
        ));
    }

    if pos + 4 > data.len() {
        return Err(Error::InvalidFormat(
            "Dictionary dict_count truncated".into(),
        ));
    }

    let dict_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    if count == 0 {
        return Ok((
            Column::Fixed {
                len: fixed_len,
                values: vec![],
            },
            bitmap,
        ));
    }

    // Read dictionary entries (fixed length)
    let mut dict: Vec<Vec<u8>> = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        if pos + fixed_len > data.len() {
            return Err(Error::InvalidFormat(
                "Dictionary fixed entry truncated".into(),
            ));
        }
        dict.push(data[pos..pos + fixed_len].to_vec());
        pos += fixed_len;
    }

    // Read indices and reconstruct values
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > data.len() {
            return Err(Error::InvalidFormat("Dictionary index truncated".into()));
        }
        let idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if idx >= dict.len() {
            return Err(Error::InvalidFormat("Dictionary index out of range".into()));
        }
        values.push(dict[idx].clone());
    }

    Ok((
        Column::Fixed {
            len: fixed_len,
            values,
        },
        bitmap,
    ))
}

// ============================================================================
// Bitpack Encoding - for Bool
// ============================================================================

/// Bitpack encoder for Bool columns.
///
/// Format: count(u32) + has_bitmap(u8) + bitmap? + packed_bytes
/// - Each bit represents one bool value (1 = true, 0 = false)
/// - Bits are packed LSB first within each byte
pub struct BitpackEncoder;

impl Encoder for BitpackEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        let values = match data {
            Column::Bool(v) => v,
            _ => {
                return Err(Error::InvalidFormat(
                    "Bitpack encoding requires Bool".into(),
                ))
            }
        };

        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // Write bitmap flag and data
        if let Some(bitmap) = null_bitmap {
            buf.push(1u8);
            buf.extend_from_slice(&bitmap.to_bytes());
        } else {
            buf.push(0u8);
        }

        if values.is_empty() {
            return Ok(buf);
        }

        // Pack bool values into bytes
        let num_bytes = values.len().div_ceil(8);
        let mut packed = vec![0u8; num_bytes];

        for (i, &v) in values.iter().enumerate() {
            if v {
                packed[i / 8] |= 1 << (i % 8);
            }
        }

        buf.extend_from_slice(&packed);

        Ok(buf)
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::Bitpack
    }
}

/// Bitpack decoder for Bool columns.
pub struct BitpackDecoder;

impl Decoder for BitpackDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        if logical_type != LogicalType::Bool {
            return Err(Error::InvalidFormat(
                "Bitpack decoding requires Bool".into(),
            ));
        }

        if data.len() < 5 {
            return Err(Error::InvalidFormat("Bitpack header too short".into()));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let has_bitmap = data[4] != 0;
        let mut pos = 5;

        let bitmap = if has_bitmap {
            let bm = Bitmap::from_bytes(&data[pos..])?;
            pos += 4 + bm.len().div_ceil(8);
            Some(bm)
        } else {
            None
        };

        if count == 0 {
            return Ok((Column::Bool(vec![]), bitmap));
        }

        let num_bytes = count.div_ceil(8);
        if pos + num_bytes > data.len() {
            return Err(Error::InvalidFormat("Bitpack data truncated".into()));
        }

        let packed = &data[pos..pos + num_bytes];

        // Unpack bool values
        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            let value = (packed[i / 8] & (1 << (i % 8))) != 0;
            values.push(value);
        }

        Ok((Column::Bool(values), bitmap))
    }
}

// ============================================================================
// Encoding Selection
// ============================================================================

/// Statistics used for encoding selection.
#[derive(Default)]
pub struct EncodingHints {
    /// Is data sorted?
    pub is_sorted: bool,
    /// Number of distinct values.
    pub distinct_count: usize,
    /// Total number of values.
    pub total_count: usize,
    /// For integers: value range (max - min).
    pub value_range: Option<u64>,
    /// Percentage of values within main range (for PFOR).
    pub in_range_ratio: Option<f64>,
}

/// Select optimal encoding based on data type and hints.
///
/// Returns the best encoding for the given data characteristics.
/// Falls back to Plain if no better encoding is applicable.
pub fn select_encoding(logical_type: LogicalType, hints: &EncodingHints) -> EncodingV2 {
    match logical_type {
        LogicalType::Int64 => select_int_encoding(hints),
        LogicalType::Float64 => EncodingV2::ByteStreamSplit,
        LogicalType::Bool => select_bool_encoding(hints),
        LogicalType::Binary => select_binary_encoding(hints),
        LogicalType::Fixed(_) => select_binary_encoding(hints),
    }
}

fn select_bool_encoding(hints: &EncodingHints) -> EncodingV2 {
    // Low cardinality with runs: use RLE
    if hints.total_count > 0 && hints.distinct_count <= 2 {
        // If there are likely many consecutive runs, RLE is efficient
        // Heuristic: if average run length > 4, use RLE
        let avg_run_len = hints.total_count / hints.distinct_count.max(1);
        if avg_run_len >= 4 {
            return EncodingV2::Rle;
        }
    }

    // Default: Bitpack is always efficient for bool
    EncodingV2::Bitpack
}

fn select_int_encoding(hints: &EncodingHints) -> EncodingV2 {
    // Sorted data: use Delta encoding
    if hints.is_sorted {
        return EncodingV2::Delta;
    }

    // Small range: use FOR
    if let Some(range) = hints.value_range {
        let bits_needed = if range == 0 {
            1
        } else {
            64 - range.leading_zeros()
        };
        if bits_needed <= 16 {
            // Check if PFOR would be better (has outliers)
            if let Some(ratio) = hints.in_range_ratio {
                if (0.9..1.0).contains(&ratio) {
                    return EncodingV2::PFOR;
                }
            }
            return EncodingV2::FOR;
        }
    }

    // Low cardinality with runs: use RLE
    if hints.total_count > 0 && hints.distinct_count > 0 {
        let avg_run_len = hints.total_count / hints.distinct_count;
        if avg_run_len >= 4 {
            return EncodingV2::Rle;
        }
    }

    EncodingV2::Plain
}

fn select_binary_encoding(hints: &EncodingHints) -> EncodingV2 {
    // Sorted strings: use IncrementalString
    if hints.is_sorted {
        return EncodingV2::IncrementalString;
    }

    // Low cardinality: use Dictionary
    if hints.total_count > 0 && hints.distinct_count > 0 {
        let cardinality_ratio = hints.distinct_count as f64 / hints.total_count as f64;
        if cardinality_ratio < 0.5 {
            return EncodingV2::Dictionary;
        }
    }

    EncodingV2::Plain
}

/// Create an encoder for the given encoding type.
pub fn create_encoder(encoding: EncodingV2) -> Box<dyn Encoder> {
    match encoding {
        EncodingV2::Plain => Box::new(PlainEncoder),
        EncodingV2::Delta => Box::new(DeltaEncoder),
        EncodingV2::FOR => Box::new(ForEncoder),
        EncodingV2::PFOR => Box::new(PforEncoder::default()),
        EncodingV2::ByteStreamSplit => Box::new(ByteStreamSplitEncoder),
        EncodingV2::IncrementalString => Box::new(IncrementalStringEncoder),
        EncodingV2::Rle => Box::new(RleEncoder),
        EncodingV2::Dictionary => Box::new(DictionaryEncoder),
        EncodingV2::Bitpack => Box::new(BitpackEncoder),
        // DeltaLength not yet implemented
        EncodingV2::DeltaLength => Box::new(PlainEncoder),
    }
}

/// Create a decoder for the given encoding type.
pub fn create_decoder(encoding: EncodingV2) -> Box<dyn Decoder> {
    match encoding {
        EncodingV2::Plain => Box::new(PlainDecoder),
        EncodingV2::Delta => Box::new(DeltaDecoder),
        EncodingV2::FOR => Box::new(ForDecoder),
        EncodingV2::PFOR => Box::new(PforDecoder),
        EncodingV2::ByteStreamSplit => Box::new(ByteStreamSplitDecoder),
        EncodingV2::IncrementalString => Box::new(IncrementalStringDecoder),
        EncodingV2::Rle => Box::new(RleDecoder),
        EncodingV2::Dictionary => Box::new(DictionaryDecoder),
        EncodingV2::Bitpack => Box::new(BitpackDecoder),
        // DeltaLength not yet implemented
        EncodingV2::DeltaLength => Box::new(PlainDecoder),
    }
}

// ============================================================================
// Plain Encoder/Decoder (fallback)
// ============================================================================

/// Plain encoder (fallback for unsupported types).
pub struct PlainEncoder;

impl Encoder for PlainEncoder {
    fn encode(&self, data: &Column, null_bitmap: Option<&Bitmap>) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Write bitmap flag and data first
        if let Some(bitmap) = null_bitmap {
            buf.push(1u8);
            buf.extend_from_slice(&bitmap.to_bytes());
        } else {
            buf.push(0u8);
        }

        match data {
            Column::Int64(values) => {
                buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for v in values {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Column::Float64(values) => {
                buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for v in values {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Column::Bool(values) => {
                buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for v in values {
                    buf.push(*v as u8);
                }
            }
            Column::Binary(values) => {
                buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for v in values {
                    buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    buf.extend_from_slice(v);
                }
            }
            Column::Fixed { len, values } => {
                buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
                buf.extend_from_slice(&(*len as u16).to_le_bytes());
                for v in values {
                    buf.extend_from_slice(v);
                }
            }
        }

        Ok(buf)
    }

    fn encoding_type(&self) -> EncodingV2 {
        EncodingV2::Plain
    }
}

/// Plain decoder (fallback).
pub struct PlainDecoder;

impl Decoder for PlainDecoder {
    fn decode(
        &self,
        data: &[u8],
        _num_values: usize,
        logical_type: LogicalType,
    ) -> Result<(Column, Option<Bitmap>)> {
        if data.is_empty() {
            return Err(Error::InvalidFormat("plain data empty".into()));
        }

        let has_bitmap = data[0] != 0;
        let mut pos = 1;

        let bitmap = if has_bitmap {
            let bm = Bitmap::from_bytes(&data[pos..])?;
            pos += 4 + bm.len().div_ceil(8);
            Some(bm)
        } else {
            None
        };

        if pos + 4 > data.len() {
            return Err(Error::InvalidFormat("plain count truncated".into()));
        }

        let count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let column = match logical_type {
            LogicalType::Int64 => {
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos + 8 > data.len() {
                        return Err(Error::InvalidFormat("plain int64 truncated".into()));
                    }
                    values.push(i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                    pos += 8;
                }
                Column::Int64(values)
            }
            LogicalType::Float64 => {
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos + 8 > data.len() {
                        return Err(Error::InvalidFormat("plain float64 truncated".into()));
                    }
                    values.push(f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                    pos += 8;
                }
                Column::Float64(values)
            }
            LogicalType::Bool => {
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos >= data.len() {
                        return Err(Error::InvalidFormat("plain bool truncated".into()));
                    }
                    values.push(data[pos] != 0);
                    pos += 1;
                }
                Column::Bool(values)
            }
            LogicalType::Binary => {
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos + 4 > data.len() {
                        return Err(Error::InvalidFormat("plain binary len truncated".into()));
                    }
                    let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    if pos + len > data.len() {
                        return Err(Error::InvalidFormat("plain binary data truncated".into()));
                    }
                    values.push(data[pos..pos + len].to_vec());
                    pos += len;
                }
                Column::Binary(values)
            }
            LogicalType::Fixed(fixed_len) => {
                if pos + 2 > data.len() {
                    return Err(Error::InvalidFormat("plain fixed len truncated".into()));
                }
                let stored_len =
                    u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if stored_len != fixed_len as usize {
                    return Err(Error::InvalidFormat("plain fixed length mismatch".into()));
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos + stored_len > data.len() {
                        return Err(Error::InvalidFormat("plain fixed data truncated".into()));
                    }
                    values.push(data[pos..pos + stored_len].to_vec());
                    pos += stored_len;
                }
                Column::Fixed {
                    len: stored_len,
                    values,
                }
            }
        };

        Ok((column, bitmap))
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Zigzag encode a signed integer.
#[inline]
fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// Zigzag decode to signed integer.
#[inline]
fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Encode a u64 as variable-length integer.
fn encode_varint(mut n: u64, buf: &mut Vec<u8>) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

/// Decode a variable-length integer, returning (value, bytes_read).
fn decode_varint(data: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encoding_sorted_integers() {
        let values = vec![100i64, 105, 110, 115, 120, 125];
        let col = Column::Int64(values.clone());

        let encoder = DeltaEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = DeltaDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Int64)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Int64(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn test_for_encoding_small_range() {
        let values = vec![1000i64, 1005, 1002, 1008, 1001, 1007];
        let col = Column::Int64(values.clone());

        let encoder = ForEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = ForDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Int64)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Int64(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn test_pfor_encoding_with_outliers() {
        // Most values in small range, with some outliers
        let mut values = vec![10i64, 12, 11, 15, 13, 14, 10, 11];
        values.push(1000000); // outlier
        values.push(12);
        values.push(2000000); // outlier
        let col = Column::Int64(values.clone());

        let encoder = PforEncoder::default();
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = PforDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Int64)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Int64(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn test_byte_stream_split_floats() {
        let values = vec![1.5f64, 2.7, 3.14159, 4.0, 5.5];
        let col = Column::Float64(values.clone());

        let encoder = ByteStreamSplitEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = ByteStreamSplitDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Float64)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Float64(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Float64 column");
        }
    }

    #[test]
    fn test_incremental_string_sorted() {
        let values: Vec<Vec<u8>> = vec![
            b"apple".to_vec(),
            b"application".to_vec(),
            b"apply".to_vec(),
            b"banana".to_vec(),
            b"bandana".to_vec(),
        ];
        let col = Column::Binary(values.clone());

        let encoder = IncrementalStringEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = IncrementalStringDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Binary)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Binary(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Binary column");
        }
    }

    #[test]
    fn test_encoding_fallback_to_plain() {
        // Test that select_encoding returns Plain for unsupported scenarios
        let hints = EncodingHints {
            is_sorted: false,
            distinct_count: 1000,
            total_count: 1000,
            value_range: Some(u64::MAX), // Very large range
            in_range_ratio: None,
        };

        let encoding = select_encoding(LogicalType::Int64, &hints);
        assert_eq!(encoding, EncodingV2::Plain);
    }

    #[test]
    fn test_bitmap_operations() {
        let bitmap = Bitmap::from_bools(&[true, false, true, true, false]);
        assert!(bitmap.is_valid(0));
        assert!(!bitmap.is_valid(1));
        assert!(bitmap.is_valid(2));
        assert!(bitmap.is_valid(3));
        assert!(!bitmap.is_valid(4));
        assert_eq!(bitmap.null_count(), 2);
        assert_eq!(bitmap.len(), 5);

        // Roundtrip
        let bytes = bitmap.to_bytes();
        let restored = Bitmap::from_bytes(&bytes).unwrap();
        assert_eq!(restored.len(), bitmap.len());
        for i in 0..bitmap.len() {
            assert_eq!(restored.is_valid(i), bitmap.is_valid(i));
        }
    }

    #[test]
    fn test_delta_with_bitmap() {
        let values = vec![100i64, 105, 110, 115, 120];
        let bitmap = Bitmap::from_bools(&[true, false, true, true, false]);
        let col = Column::Int64(values.clone());

        let encoder = DeltaEncoder;
        let encoded = encoder.encode(&col, Some(&bitmap)).unwrap();

        let decoder = DeltaDecoder;
        let (decoded, decoded_bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Int64)
            .unwrap();

        assert!(decoded_bitmap.is_some());
        let decoded_bitmap = decoded_bitmap.unwrap();
        assert_eq!(decoded_bitmap.null_count(), 2);

        if let Column::Int64(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn test_zigzag_encoding() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);

        for n in [-1000i64, -1, 0, 1, 1000, i64::MIN, i64::MAX] {
            assert_eq!(zigzag_decode(zigzag_encode(n)), n);
        }
    }

    #[test]
    fn test_varint_encoding() {
        let mut buf = Vec::new();
        encode_varint(300, &mut buf);
        let (decoded, bytes_read) = decode_varint(&buf).unwrap();
        assert_eq!(decoded, 300);
        assert_eq!(bytes_read, 2);

        buf.clear();
        encode_varint(0, &mut buf);
        let (decoded, bytes_read) = decode_varint(&buf).unwrap();
        assert_eq!(decoded, 0);
        assert_eq!(bytes_read, 1);
    }

    #[test]
    fn test_select_encoding_sorted_int() {
        let hints = EncodingHints {
            is_sorted: true,
            distinct_count: 100,
            total_count: 100,
            value_range: Some(100),
            in_range_ratio: None,
        };
        assert_eq!(
            select_encoding(LogicalType::Int64, &hints),
            EncodingV2::Delta
        );
    }

    #[test]
    fn test_select_encoding_small_range() {
        let hints = EncodingHints {
            is_sorted: false,
            distinct_count: 100,
            total_count: 100,
            value_range: Some(255), // Fits in 8 bits
            in_range_ratio: Some(1.0),
        };
        assert_eq!(select_encoding(LogicalType::Int64, &hints), EncodingV2::FOR);
    }

    #[test]
    fn test_select_encoding_float() {
        let hints = EncodingHints::default();
        assert_eq!(
            select_encoding(LogicalType::Float64, &hints),
            EncodingV2::ByteStreamSplit
        );
    }

    #[test]
    fn test_select_encoding_sorted_binary() {
        let hints = EncodingHints {
            is_sorted: true,
            distinct_count: 100,
            total_count: 100,
            value_range: None,
            in_range_ratio: None,
        };
        assert_eq!(
            select_encoding(LogicalType::Binary, &hints),
            EncodingV2::IncrementalString
        );
    }

    // ========================================================================
    // RLE Tests
    // ========================================================================

    #[test]
    fn test_rle_bool_roundtrip() {
        // Runs of consecutive values
        let values = vec![
            true, true, true, false, false, true, true, true, true, false,
        ];
        let col = Column::Bool(values.clone());

        let encoder = RleEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = RleDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Bool)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Bool(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Bool column");
        }
    }

    #[test]
    fn test_rle_bool_roundtrip_with_bitmap() {
        let values = vec![true, true, false, false, true];
        let bitmap = Bitmap::from_bools(&[true, false, true, true, false]);
        let col = Column::Bool(values.clone());

        let encoder = RleEncoder;
        let encoded = encoder.encode(&col, Some(&bitmap)).unwrap();

        let decoder = RleDecoder;
        let (decoded, decoded_bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Bool)
            .unwrap();

        assert!(decoded_bitmap.is_some());
        let decoded_bitmap = decoded_bitmap.unwrap();
        assert_eq!(decoded_bitmap.null_count(), 2);

        if let Column::Bool(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Bool column");
        }
    }

    #[test]
    fn test_rle_int64_roundtrip() {
        // Runs of consecutive values
        let values = vec![100i64, 100, 100, 200, 200, 100, 100, 100, 100, 300];
        let col = Column::Int64(values.clone());

        let encoder = RleEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = RleDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Int64)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Int64(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn test_rle_int64_roundtrip_with_bitmap() {
        let values = vec![100i64, 100, 200, 200, 100];
        let bitmap = Bitmap::from_bools(&[true, false, true, true, false]);
        let col = Column::Int64(values.clone());

        let encoder = RleEncoder;
        let encoded = encoder.encode(&col, Some(&bitmap)).unwrap();

        let decoder = RleDecoder;
        let (decoded, decoded_bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Int64)
            .unwrap();

        assert!(decoded_bitmap.is_some());
        let decoded_bitmap = decoded_bitmap.unwrap();
        assert_eq!(decoded_bitmap.null_count(), 2);

        if let Column::Int64(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn test_rle_empty() {
        let col = Column::Bool(vec![]);

        let encoder = RleEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = RleDecoder;
        let (decoded, bitmap) = decoder.decode(&encoded, 0, LogicalType::Bool).unwrap();

        assert!(bitmap.is_none());
        if let Column::Bool(decoded_values) = decoded {
            assert!(decoded_values.is_empty());
        } else {
            panic!("Expected Bool column");
        }
    }

    // ========================================================================
    // Dictionary Tests
    // ========================================================================

    #[test]
    fn test_dictionary_binary_roundtrip() {
        // Low cardinality strings
        let values: Vec<Vec<u8>> = vec![
            b"apple".to_vec(),
            b"banana".to_vec(),
            b"apple".to_vec(),
            b"cherry".to_vec(),
            b"banana".to_vec(),
            b"apple".to_vec(),
        ];
        let col = Column::Binary(values.clone());

        let encoder = DictionaryEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = DictionaryDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Binary)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Binary(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Binary column");
        }
    }

    #[test]
    fn test_dictionary_binary_roundtrip_with_bitmap() {
        let values: Vec<Vec<u8>> = vec![
            b"apple".to_vec(),
            b"banana".to_vec(),
            b"apple".to_vec(),
            b"cherry".to_vec(),
            b"banana".to_vec(),
        ];
        let bitmap = Bitmap::from_bools(&[true, false, true, true, false]);
        let col = Column::Binary(values.clone());

        let encoder = DictionaryEncoder;
        let encoded = encoder.encode(&col, Some(&bitmap)).unwrap();

        let decoder = DictionaryDecoder;
        let (decoded, decoded_bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Binary)
            .unwrap();

        assert!(decoded_bitmap.is_some());
        let decoded_bitmap = decoded_bitmap.unwrap();
        assert_eq!(decoded_bitmap.null_count(), 2);

        if let Column::Binary(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Binary column");
        }
    }

    #[test]
    fn test_dictionary_fixed_roundtrip() {
        // Fixed-length values (e.g., UUIDs)
        let values: Vec<Vec<u8>> = vec![
            vec![1, 2, 3, 4],
            vec![5, 6, 7, 8],
            vec![1, 2, 3, 4],
            vec![9, 10, 11, 12],
            vec![5, 6, 7, 8],
        ];
        let col = Column::Fixed {
            len: 4,
            values: values.clone(),
        };

        let encoder = DictionaryEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = DictionaryDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Fixed(4))
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Fixed {
            len,
            values: decoded_values,
        } = decoded
        {
            assert_eq!(len, 4);
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Fixed column");
        }
    }

    #[test]
    fn test_dictionary_empty() {
        let col = Column::Binary(vec![]);

        let encoder = DictionaryEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = DictionaryDecoder;
        let (decoded, bitmap) = decoder.decode(&encoded, 0, LogicalType::Binary).unwrap();

        assert!(bitmap.is_none());
        if let Column::Binary(decoded_values) = decoded {
            assert!(decoded_values.is_empty());
        } else {
            panic!("Expected Binary column");
        }
    }

    // ========================================================================
    // Bitpack Tests
    // ========================================================================

    #[test]
    fn test_bitpack_bool_roundtrip() {
        let values = vec![
            true, false, true, true, false, true, false, false, true, true,
        ];
        let col = Column::Bool(values.clone());

        let encoder = BitpackEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        // Verify compression: 10 bools should take ~2 bytes (packed) vs 10 bytes (plain)
        // Header is 5 bytes (count + has_bitmap), so total should be ~7 bytes
        assert!(encoded.len() < 10);

        let decoder = BitpackDecoder;
        let (decoded, bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Bool)
            .unwrap();

        assert!(bitmap.is_none());
        if let Column::Bool(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Bool column");
        }
    }

    #[test]
    fn test_bitpack_bool_roundtrip_with_bitmap() {
        let values = vec![true, false, true, true, false];
        let bitmap = Bitmap::from_bools(&[true, false, true, true, false]);
        let col = Column::Bool(values.clone());

        let encoder = BitpackEncoder;
        let encoded = encoder.encode(&col, Some(&bitmap)).unwrap();

        let decoder = BitpackDecoder;
        let (decoded, decoded_bitmap) = decoder
            .decode(&encoded, values.len(), LogicalType::Bool)
            .unwrap();

        assert!(decoded_bitmap.is_some());
        let decoded_bitmap = decoded_bitmap.unwrap();
        assert_eq!(decoded_bitmap.null_count(), 2);

        if let Column::Bool(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Bool column");
        }
    }

    #[test]
    fn test_bitpack_empty() {
        let col = Column::Bool(vec![]);

        let encoder = BitpackEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = BitpackDecoder;
        let (decoded, bitmap) = decoder.decode(&encoded, 0, LogicalType::Bool).unwrap();

        assert!(bitmap.is_none());
        if let Column::Bool(decoded_values) = decoded {
            assert!(decoded_values.is_empty());
        } else {
            panic!("Expected Bool column");
        }
    }

    #[test]
    fn test_bitpack_large() {
        // Test with more than 8 values to verify multi-byte packing
        let values: Vec<bool> = (0..100).map(|i| i % 3 == 0).collect();
        let col = Column::Bool(values.clone());

        let encoder = BitpackEncoder;
        let encoded = encoder.encode(&col, None).unwrap();

        // 100 bools should pack into 13 bytes (100/8 rounded up)
        // Plus 5 bytes header = 18 bytes total
        assert_eq!(encoded.len(), 5 + 13);

        let decoder = BitpackDecoder;
        let (decoded, _) = decoder
            .decode(&encoded, values.len(), LogicalType::Bool)
            .unwrap();

        if let Column::Bool(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Bool column");
        }
    }

    // ========================================================================
    // Select Encoding Tests for New Encodings
    // ========================================================================

    #[test]
    fn test_select_encoding_bool_bitpack() {
        // Bool with short runs should use Bitpack (avg run len < 4)
        let hints = EncodingHints {
            is_sorted: false,
            distinct_count: 2,
            total_count: 6, // avg run length = 3 < 4
            value_range: None,
            in_range_ratio: None,
        };
        assert_eq!(
            select_encoding(LogicalType::Bool, &hints),
            EncodingV2::Bitpack
        );
    }

    #[test]
    fn test_select_encoding_bool_rle() {
        // Bool with long runs should use RLE
        let hints = EncodingHints {
            is_sorted: false,
            distinct_count: 2,
            total_count: 100, // avg run length = 50
            value_range: None,
            in_range_ratio: None,
        };
        assert_eq!(select_encoding(LogicalType::Bool, &hints), EncodingV2::Rle);
    }

    #[test]
    fn test_select_encoding_int64_rle() {
        // Int64 with many repeating values
        let hints = EncodingHints {
            is_sorted: false,
            distinct_count: 5,
            total_count: 100,            // avg run length = 20
            value_range: Some(u64::MAX), // large range so FOR won't be selected
            in_range_ratio: None,
        };
        assert_eq!(select_encoding(LogicalType::Int64, &hints), EncodingV2::Rle);
    }

    #[test]
    fn test_select_encoding_binary_dictionary() {
        // Low cardinality binary should use Dictionary
        let hints = EncodingHints {
            is_sorted: false,
            distinct_count: 10,
            total_count: 100, // 10% cardinality
            value_range: None,
            in_range_ratio: None,
        };
        assert_eq!(
            select_encoding(LogicalType::Binary, &hints),
            EncodingV2::Dictionary
        );
    }

    #[test]
    fn test_select_encoding_binary_plain() {
        // High cardinality binary should use Plain
        let hints = EncodingHints {
            is_sorted: false,
            distinct_count: 80,
            total_count: 100, // 80% cardinality
            value_range: None,
            in_range_ratio: None,
        };
        assert_eq!(
            select_encoding(LogicalType::Binary, &hints),
            EncodingV2::Plain
        );
    }

    // ========================================================================
    // Create Encoder/Decoder Tests
    // ========================================================================

    #[test]
    fn test_create_encoder_rle() {
        let encoder = create_encoder(EncodingV2::Rle);
        assert_eq!(encoder.encoding_type(), EncodingV2::Rle);
    }

    #[test]
    fn test_create_encoder_dictionary() {
        let encoder = create_encoder(EncodingV2::Dictionary);
        assert_eq!(encoder.encoding_type(), EncodingV2::Dictionary);
    }

    #[test]
    fn test_create_encoder_bitpack() {
        let encoder = create_encoder(EncodingV2::Bitpack);
        assert_eq!(encoder.encoding_type(), EncodingV2::Bitpack);
    }

    #[test]
    fn test_create_decoder_roundtrip_via_factory() {
        // Test RLE via factory
        let values = vec![true, true, true, false, false];
        let col = Column::Bool(values.clone());

        let encoder = create_encoder(EncodingV2::Rle);
        let encoded = encoder.encode(&col, None).unwrap();

        let decoder = create_decoder(EncodingV2::Rle);
        let (decoded, _) = decoder
            .decode(&encoded, values.len(), LogicalType::Bool)
            .unwrap();

        if let Column::Bool(decoded_values) = decoded {
            assert_eq!(decoded_values, values);
        } else {
            panic!("Expected Bool column");
        }
    }
}
