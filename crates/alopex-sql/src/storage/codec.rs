use std::convert::TryFrom;

use crate::planner::ResolvedType;

use super::error::{Result, StorageError};
use super::value::SqlValue;

const MAX_INLINE_BYTES: usize = 16 * 1024 * 1024; // 16 MiB guard for Text/Blob payloads
const MAX_VECTOR_LEN: usize = 4 * 1024 * 1024; // 4 million elements (~16 MiB of f32)

/// RowCodec converts between `Vec<SqlValue>` and a binary TLV format with a null bitmap.
///
/// Format:
/// ```text
/// [column_count: u16 LE]
/// [null_bitmap: ceil(count/8) bytes] // bit=1 means NULL
/// for each non-null column:
///     [type_tag: u8]
///     [value_bytes: variable length]
/// ```
pub struct RowCodec;

impl RowCodec {
    /// Encode a row into binary form.
    pub fn encode(row: &[SqlValue]) -> Vec<u8> {
        let column_count =
            u16::try_from(row.len()).expect("row column count exceeds u16::MAX (design limit)");
        let null_bytes = (column_count as usize).div_ceil(8);

        // Pre-allocate roughly: header + bitmap + average 8 bytes per column.
        let mut buf = Vec::with_capacity(2 + null_bytes + row.len() * 8);
        buf.extend_from_slice(&column_count.to_le_bytes());

        let mut null_bitmap = vec![0u8; null_bytes];
        for (idx, val) in row.iter().enumerate() {
            if val.is_null() {
                null_bitmap[idx / 8] |= 1 << (idx % 8);
            }
        }
        buf.extend_from_slice(&null_bitmap);

        for value in row {
            if value.is_null() {
                continue;
            }
            buf.push(value.type_tag());
            encode_value(value, &mut buf);
        }

        buf
    }

    /// Decode a row from binary form.
    pub fn decode(bytes: &[u8]) -> Result<Vec<SqlValue>> {
        let mut cursor = 0;
        if bytes.len() < 2 {
            return Err(StorageError::CorruptedData {
                reason: "missing column count".into(),
            });
        }

        let column_count =
            u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;

        let null_bytes = column_count.div_ceil(8);
        if bytes.len() < cursor + null_bytes {
            return Err(StorageError::CorruptedData {
                reason: "missing null bitmap".into(),
            });
        }
        let null_bitmap = &bytes[cursor..cursor + null_bytes];
        cursor += null_bytes;

        let mut values = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            let is_null = (null_bitmap[idx / 8] & (1 << (idx % 8))) != 0;
            if is_null {
                values.push(SqlValue::Null);
                continue;
            }

            if cursor >= bytes.len() {
                return Err(StorageError::CorruptedData {
                    reason: "missing type tag".into(),
                });
            }
            let tag = bytes[cursor];
            cursor += 1;

            let value = decode_value(tag, bytes, &mut cursor)?;
            values.push(value);
        }

        if cursor != bytes.len() {
            return Err(StorageError::CorruptedData {
                reason: "trailing bytes after decoding row".into(),
            });
        }

        Ok(values)
    }

    /// Decode with schema validation.
    pub fn decode_with_schema(bytes: &[u8], schema: &[ResolvedType]) -> Result<Vec<SqlValue>> {
        let values = Self::decode(bytes)?;

        if values.len() != schema.len() {
            return Err(StorageError::CorruptedData {
                reason: format!(
                    "column count mismatch: encoded={}, expected={}",
                    values.len(),
                    schema.len()
                ),
            });
        }

        values
            .into_iter()
            .zip(schema.iter())
            .map(|(value, ty)| ensure_type(value, ty))
            .collect()
    }
}

fn encode_value(value: &SqlValue, buf: &mut Vec<u8>) {
    match value {
        SqlValue::Null => {}
        SqlValue::Integer(v) => buf.extend_from_slice(&v.to_le_bytes()),
        SqlValue::BigInt(v) => buf.extend_from_slice(&v.to_le_bytes()),
        SqlValue::Float(v) => buf.extend_from_slice(&v.to_bits().to_le_bytes()),
        SqlValue::Double(v) => buf.extend_from_slice(&v.to_bits().to_le_bytes()),
        SqlValue::Text(s) => {
            let len = u32::try_from(s.len())
                .expect("text length exceeds u32::MAX (design limit for row encoding)");
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        SqlValue::Blob(bytes) => {
            let len = u32::try_from(bytes.len())
                .expect("blob length exceeds u32::MAX (design limit for row encoding)");
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        SqlValue::Boolean(b) => buf.push(u8::from(*b)),
        SqlValue::Timestamp(v) => buf.extend_from_slice(&v.to_le_bytes()),
        SqlValue::Vector(values) => {
            let len = u32::try_from(values.len())
                .expect("vector length exceeds u32::MAX (design limit for row encoding)");
            buf.extend_from_slice(&len.to_le_bytes());
            for f in values {
                buf.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
    }
}

fn decode_value(tag: u8, bytes: &[u8], cursor: &mut usize) -> Result<SqlValue> {
    let mut take = |len: usize, reason: &'static str| -> Result<&[u8]> {
        let end = cursor
            .checked_add(len)
            .ok_or_else(|| StorageError::CorruptedData {
                reason: reason.to_string(),
            })?;
        if end > bytes.len() {
            return Err(StorageError::CorruptedData {
                reason: reason.to_string(),
            });
        }
        let slice = &bytes[*cursor..end];
        *cursor = end;
        Ok(slice)
    };

    match tag {
        0x00 => Ok(SqlValue::Null),
        0x01 => {
            let raw = take(4, "truncated Integer value")?;
            Ok(SqlValue::Integer(i32::from_le_bytes(
                raw.try_into().unwrap(),
            )))
        }
        0x02 => {
            let raw = take(8, "truncated BigInt value")?;
            Ok(SqlValue::BigInt(i64::from_le_bytes(
                raw.try_into().unwrap(),
            )))
        }
        0x03 => {
            let raw = take(4, "truncated Float value")?;
            Ok(SqlValue::Float(f32::from_bits(u32::from_le_bytes(
                raw.try_into().unwrap(),
            ))))
        }
        0x04 => {
            let raw = take(8, "truncated Double value")?;
            Ok(SqlValue::Double(f64::from_bits(u64::from_le_bytes(
                raw.try_into().unwrap(),
            ))))
        }
        0x05 => {
            let len_bytes = take(4, "truncated Text length")?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            if len > MAX_INLINE_BYTES {
                return Err(StorageError::CorruptedData {
                    reason: format!("text length exceeds limit: {len}"),
                });
            }
            let raw = take(len, "truncated Text payload")?;
            let s = String::from_utf8(raw.to_vec()).map_err(|_| StorageError::CorruptedData {
                reason: "invalid UTF-8 in Text".into(),
            })?;
            Ok(SqlValue::Text(s))
        }
        0x06 => {
            let len_bytes = take(4, "truncated Blob length")?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            if len > MAX_INLINE_BYTES {
                return Err(StorageError::CorruptedData {
                    reason: format!("blob length exceeds limit: {len}"),
                });
            }
            let raw = take(len, "truncated Blob payload")?;
            Ok(SqlValue::Blob(raw.to_vec()))
        }
        0x07 => {
            let raw = take(1, "truncated Boolean")?[0];
            match raw {
                0 => Ok(SqlValue::Boolean(false)),
                1 => Ok(SqlValue::Boolean(true)),
                other => Err(StorageError::CorruptedData {
                    reason: format!("invalid boolean value: {}", other),
                }),
            }
        }
        0x08 => {
            let raw = take(8, "truncated Timestamp value")?;
            Ok(SqlValue::Timestamp(i64::from_le_bytes(
                raw.try_into().unwrap(),
            )))
        }
        0x09 => {
            let len_bytes = take(4, "truncated Vector length")?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            if len > MAX_VECTOR_LEN {
                return Err(StorageError::CorruptedData {
                    reason: format!("vector length exceeds limit: {len}"),
                });
            }
            let total = len
                .checked_mul(4)
                .ok_or_else(|| StorageError::CorruptedData {
                    reason: "vector length overflow".into(),
                })?;
            let raw = take(total, "truncated Vector payload")?;

            let mut values = Vec::with_capacity(len);
            for chunk in raw.chunks_exact(4) {
                values.push(f32::from_bits(u32::from_le_bytes(
                    chunk.try_into().unwrap(),
                )));
            }
            Ok(SqlValue::Vector(values))
        }
        other => Err(StorageError::CorruptedData {
            reason: format!("unknown type tag: 0x{other:02x}"),
        }),
    }
}

fn ensure_type(value: SqlValue, expected: &ResolvedType) -> Result<SqlValue> {
    use ResolvedType::*;
    match (expected, value) {
        (_, SqlValue::Null) => Ok(SqlValue::Null),
        (Integer, SqlValue::Integer(v)) => Ok(SqlValue::Integer(v)),
        (BigInt, SqlValue::BigInt(v)) => Ok(SqlValue::BigInt(v)),
        (Float, SqlValue::Float(v)) => Ok(SqlValue::Float(v)),
        (Double, SqlValue::Double(v)) => Ok(SqlValue::Double(v)),
        (Text, SqlValue::Text(s)) => Ok(SqlValue::Text(s)),
        (Blob, SqlValue::Blob(b)) => Ok(SqlValue::Blob(b)),
        (Boolean, SqlValue::Boolean(v)) => Ok(SqlValue::Boolean(v)),
        (Timestamp, SqlValue::Timestamp(v)) => Ok(SqlValue::Timestamp(v)),
        (Vector { dimension, .. }, SqlValue::Vector(values)) => {
            if values.len() as u32 == *dimension {
                Ok(SqlValue::Vector(values))
            } else {
                Err(StorageError::TypeMismatch {
                    expected: format!("Vector(dim={})", dimension),
                    actual: format!("Vector(dim={})", values.len()),
                })
            }
        }
        (expected_ty, actual) => Err(StorageError::TypeMismatch {
            expected: expected_ty.type_name().to_string(),
            actual: actual.type_name().to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn values_equal(a: &SqlValue, b: &SqlValue) -> bool {
        match (a, b) {
            (SqlValue::Float(x), SqlValue::Float(y)) => x.to_bits() == y.to_bits(),
            (SqlValue::Double(x), SqlValue::Double(y)) => x.to_bits() == y.to_bits(),
            (SqlValue::Vector(xs), SqlValue::Vector(ys)) => {
                xs.len() == ys.len()
                    && xs
                        .iter()
                        .zip(ys.iter())
                        .all(|(x, y)| x.to_bits() == y.to_bits())
            }
            _ => a == b,
        }
    }

    fn row_equal(a: &[SqlValue], b: &[SqlValue]) -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b.iter())
                .all(|(lhs, rhs)| values_equal(lhs, rhs))
    }

    fn sql_value_strategy() -> impl Strategy<Value = SqlValue> {
        let finite_f32 = any::<f32>();
        let finite_f64 = any::<f64>();
        prop_oneof![
            Just(SqlValue::Null),
            any::<i32>().prop_map(SqlValue::Integer),
            any::<i64>().prop_map(SqlValue::BigInt),
            finite_f32.prop_map(SqlValue::Float),
            finite_f64.prop_map(SqlValue::Double),
            ".*".prop_map(SqlValue::Text),
            proptest::collection::vec(any::<u8>(), 0..32).prop_map(SqlValue::Blob),
            any::<bool>().prop_map(SqlValue::Boolean),
            any::<i64>().prop_map(SqlValue::Timestamp),
            proptest::collection::vec(any::<f32>(), 0..8).prop_map(SqlValue::Vector),
        ]
    }

    #[test]
    fn roundtrip_preserves_all_types() {
        let row = vec![
            SqlValue::Null,
            SqlValue::Integer(42),
            SqlValue::BigInt(-42),
            SqlValue::Float(1.5),
            SqlValue::Double(-2.5),
            SqlValue::Text("hello".into()),
            SqlValue::Blob(vec![0x01, 0x02]),
            SqlValue::Boolean(true),
            SqlValue::Timestamp(1_700_000_000),
            SqlValue::Vector(vec![0.1, 0.2, 0.3]),
        ];

        let encoded = RowCodec::encode(&row);
        let decoded = RowCodec::decode(&encoded).unwrap();

        assert!(row_equal(&row, &decoded));
    }

    #[test]
    fn null_bitmap_is_respected() {
        let row = vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Integer(2)];
        let encoded = RowCodec::encode(&row);
        let decoded = RowCodec::decode(&encoded).unwrap();
        assert!(matches!(decoded[1], SqlValue::Null));
    }

    #[test]
    fn corruption_is_detected_for_truncated_payload() {
        let row = vec![SqlValue::Text("abc".into())];
        let mut encoded = RowCodec::encode(&row);
        encoded.pop(); // truncate
        let err = RowCodec::decode(&encoded).unwrap_err();
        assert!(matches!(err, StorageError::CorruptedData { .. }));
    }

    #[test]
    fn corruption_is_detected_for_unknown_tag() {
        // column_count=1, null_bitmap=0, tag=0xFF (invalid)
        let bytes = vec![1, 0, 0, 0xFF];
        let err = RowCodec::decode(&bytes).unwrap_err();
        assert!(matches!(err, StorageError::CorruptedData { .. }));
    }

    #[test]
    fn oversized_lengths_are_rejected() {
        // Text length = MAX_INLINE_BYTES + 1 with no payload.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(1u16).to_le_bytes()); // column count
        bytes.push(0); // null bitmap
        bytes.push(0x05); // text tag
        let too_large = (super::MAX_INLINE_BYTES as u32) + 1;
        bytes.extend_from_slice(&too_large.to_le_bytes());
        let err = RowCodec::decode(&bytes).unwrap_err();
        assert!(matches!(err, StorageError::CorruptedData { .. }));
    }

    #[test]
    fn oversized_vector_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(1u16).to_le_bytes()); // column count
        bytes.push(0); // null bitmap
        bytes.push(0x09); // vector tag
        let too_large = (super::MAX_VECTOR_LEN as u32) + 1;
        bytes.extend_from_slice(&too_large.to_le_bytes());
        let err = RowCodec::decode(&bytes).unwrap_err();
        assert!(matches!(err, StorageError::CorruptedData { .. }));
    }

    #[test]
    fn decode_with_schema_validates_types() {
        let row = vec![SqlValue::Vector(vec![1.0, 2.0])];
        let encoded = RowCodec::encode(&row);
        let schema = vec![ResolvedType::Vector {
            dimension: 3,
            metric: crate::ast::ddl::VectorMetric::Cosine,
        }];
        let err = RowCodec::decode_with_schema(&encoded, &schema).unwrap_err();
        assert!(matches!(err, StorageError::TypeMismatch { .. }));
    }

    proptest! {
        #[test]
        fn proptest_roundtrip(row in proptest::collection::vec(sql_value_strategy(), 0..16)) {
            let encoded = RowCodec::encode(&row);
            let decoded = RowCodec::decode(&encoded).unwrap();
            prop_assert!(row_equal(&row, &decoded));
        }

        #[test]
        fn decode_with_schema_matches_lengths(row in proptest::collection::vec(sql_value_strategy(), 1..5)) {
            let schema: Vec<ResolvedType> = row.iter().map(|v| v.resolved_type()).collect();
            let encoded = RowCodec::encode(&row);
            let decoded = RowCodec::decode_with_schema(&encoded, &schema).unwrap();
            prop_assert!(row_equal(&row, &decoded));
        }
    }
}
