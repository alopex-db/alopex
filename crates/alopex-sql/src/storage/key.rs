use std::convert::TryInto;

use super::error::{Result, StorageError};
use super::value::SqlValue;

/// KeyEncoder generates lexicographically ordered keys for table rows and indexes.
///
/// Layouts:
/// - Row key:   0x01 | table_id (u32 BE) | row_id (u64 BE)
/// - Index key: 0x02 | index_id (u32 BE) | encoded_value(s) | row_id (u64 BE)
/// - Sequence:  0x04 | table_id (u32 BE)
pub struct KeyEncoder;

impl KeyEncoder {
    /// SQL row key.
    pub fn row_key(table_id: u32, row_id: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + 8);
        buf.push(0x01);
        buf.extend_from_slice(&table_id.to_be_bytes());
        buf.extend_from_slice(&row_id.to_be_bytes());
        buf
    }

    /// Decode SQL row key.
    pub fn decode_row_key(key: &[u8]) -> Result<(u32, u64)> {
        if key.len() != 1 + 4 + 8 || key[0] != 0x01 {
            return Err(StorageError::InvalidKeyFormat);
        }
        let table_id = u32::from_be_bytes(key[1..5].try_into().unwrap());
        let row_id = u64::from_be_bytes(key[5..].try_into().unwrap());
        Ok((table_id, row_id))
    }

    /// Prefix for all rows of a table.
    pub fn table_prefix(table_id: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4);
        buf.push(0x01);
        buf.extend_from_slice(&table_id.to_be_bytes());
        buf
    }

    /// Index key (single value).
    pub fn index_key(index_id: u32, value: &SqlValue, row_id: u64) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(1 + 4 + 16);
        buf.push(0x02);
        buf.extend_from_slice(&index_id.to_be_bytes());
        encode_index_value(value, &mut buf)?;
        buf.extend_from_slice(&row_id.to_be_bytes());
        Ok(buf)
    }

    /// Index key (composite).
    pub fn composite_index_key(index_id: u32, values: &[SqlValue], row_id: u64) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(1 + 4 + values.len() * 16 + 8);
        buf.push(0x02);
        buf.extend_from_slice(&index_id.to_be_bytes());
        for v in values {
            encode_index_value(v, &mut buf)?;
        }
        buf.extend_from_slice(&row_id.to_be_bytes());
        Ok(buf)
    }

    /// Prefix for all entries of an index.
    pub fn index_prefix(index_id: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4);
        buf.push(0x02);
        buf.extend_from_slice(&index_id.to_be_bytes());
        buf
    }

    /// Prefix for equality lookups on a specific value within an index.
    pub fn index_value_prefix(index_id: u32, value: &SqlValue) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(1 + 4 + 16);
        buf.push(0x02);
        buf.extend_from_slice(&index_id.to_be_bytes());
        encode_index_value(value, &mut buf)?;
        Ok(buf)
    }

    /// Sequence key for auto-increment RowID tracking.
    pub fn sequence_key(table_id: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4);
        buf.push(0x04);
        buf.extend_from_slice(&table_id.to_be_bytes());
        buf
    }
}

fn encode_index_value(value: &SqlValue, buf: &mut Vec<u8>) -> Result<()> {
    match value {
        SqlValue::Null => {
            buf.push(0x00);
        }
        SqlValue::Integer(v) => {
            buf.push(0x01);
            let x = (*v as u32) ^ 0x8000_0000;
            buf.extend_from_slice(&x.to_be_bytes());
        }
        SqlValue::BigInt(v) => {
            buf.push(0x02);
            let x = (*v as u64) ^ 0x8000_0000_0000_0000;
            buf.extend_from_slice(&x.to_be_bytes());
        }
        SqlValue::Float(v) => {
            buf.push(0x03);
            buf.extend_from_slice(&encode_ordered_f32(*v).to_be_bytes());
        }
        SqlValue::Double(v) => {
            buf.push(0x04);
            buf.extend_from_slice(&encode_ordered_f64(*v).to_be_bytes());
        }
        SqlValue::Text(s) => {
            buf.push(0x05);
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00); // terminator
        }
        SqlValue::Blob(bytes) => {
            buf.push(0x06);
            let len = u32::try_from(bytes.len())
                .expect("blob length exceeds u32::MAX (index encoding limit)");
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        SqlValue::Boolean(b) => {
            buf.push(0x07);
            buf.push(u8::from(*b));
        }
        SqlValue::Timestamp(v) => {
            buf.push(0x08);
            let x = (*v as u64) ^ 0x8000_0000_0000_0000;
            buf.extend_from_slice(&x.to_be_bytes());
        }
        SqlValue::Vector(_values) => {
            // Vector ordering is undefined for BTree lexicographic indexes.
            return Err(StorageError::TypeMismatch {
                expected: "indexable scalar type".into(),
                actual: "Vector".into(),
            });
        }
    }
    Ok(())
}

fn encode_ordered_f32(v: f32) -> u32 {
    let bits = v.to_bits();
    if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    }
}

fn encode_ordered_f64(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use proptest::prelude::*;

    #[test]
    fn row_key_roundtrip() {
        let key = KeyEncoder::row_key(10, 42);
        assert_eq!(key.len(), 13);
        let (t, r) = KeyEncoder::decode_row_key(&key).unwrap();
        assert_eq!((t, r), (10, 42));
    }

    #[test]
    fn table_prefix_matches_row_key_prefix() {
        let prefix = KeyEncoder::table_prefix(7);
        let key = KeyEncoder::row_key(7, 1);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn index_prefix_matches_index_key_prefix() {
        let prefix = KeyEncoder::index_prefix(5);
            let key = KeyEncoder::index_key(5, &SqlValue::Integer(1), 99).unwrap();
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn integer_ordering_matches_lexicographic() {
        assert_monotonic_ints((-128..=127).collect());
    }

    #[test]
    fn bigint_ordering_matches_lexicographic() {
        assert_monotonic_i64(vec![
            i64::MIN,
            -10,
            -1,
            0,
            1,
            2,
            100,
            i64::MAX,
        ]);
    }

    #[test]
    fn float_ordering_matches_lexicographic_with_specials() {
        let values = vec![
            -f32::INFINITY,
            -1.5,
            -0.0,
            0.0,
            0.5,
            f32::INFINITY,
            f32::NAN,
        ];
        assert_monotonic_f32(values);
    }

    #[test]
    fn double_ordering_matches_lexicographic_with_specials() {
        let values = vec![
            -f64::INFINITY,
            -123.456,
            -0.0,
            0.0,
            1.2345,
            f64::INFINITY,
            f64::NAN,
        ];
        assert_monotonic_f64(values);
    }

    #[test]
    fn text_ordering_handles_ascii_and_multibyte() {
        let values = vec![
            "",
            "a",
            "aa",
            "b",
            "é",
            "あ",
            "あい",
            "🍣",
        ];
        assert_monotonic_text(values);
    }

    #[test]
    fn blob_ordering_respects_length_then_bytes() {
        let values = vec![
            vec![],
            vec![0x00],
            vec![0x00, 0x01],
            vec![0x01],
            vec![0x01, 0x00],
            vec![0xFF],
        ];
        assert_monotonic_blob(values);
    }

    #[test]
    fn boolean_ordering() {
        assert_monotonic_bool();
    }

    #[test]
    fn vector_is_rejected_for_index_key() {
        let err = KeyEncoder::index_key(1, &SqlValue::Vector(vec![1.0, 2.0]), 0).unwrap_err();
        match err {
            StorageError::TypeMismatch { actual, .. } => {
                assert_eq!(actual, "Vector");
            }
            other => panic!("expected TypeMismatch for Vector, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_ordering() {
        let values = vec![-5, -1, 0, 1, 10, i64::MAX];
        assert_monotonic_timestamp(values);
    }

    #[test]
    fn composite_key_maintains_lexicographic_tuple_order() {
        let mut tuples = vec![
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 2),
            (2, 0),
            (2, 2),
        ];
        tuples.sort();

        let mut prev: Option<Vec<u8>> = None;
        for (i, (a, b)) in tuples.iter().enumerate() {
            let key = KeyEncoder::composite_index_key(
                9,
                &[SqlValue::Integer(*a), SqlValue::Integer(*b)],
                i as u64,
            )
            .unwrap();
            if let Some(p) = prev {
                assert!(
                    p < key,
                    "composite key ordering violated for ({a},{b}) at position {i}"
                );
            }
            prev = Some(key);
        }
    }

    fn assert_monotonic_ints(values: Vec<i32>) {
        let mut sorted = values;
        sorted.sort();
        let mut prev: Option<(Vec<u8>, i32)> = None;
        for (i, v) in sorted.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::Integer(*v), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = prev_v.cmp(v);
                let key_ord = prev_key.cmp(&key);
                assert!(
                    ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                    "integer ordering mismatch: prev={prev_v}, curr={v}"
                );
            }
            prev = Some((key, *v));
        }
    }

    fn assert_monotonic_i64(values: Vec<i64>) {
        let mut sorted = values;
        sorted.sort();
        let mut prev: Option<(Vec<u8>, i64)> = None;
        for (i, v) in sorted.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::BigInt(*v), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = prev_v.cmp(v);
                let key_ord = prev_key.cmp(&key);
                assert!(
                    ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                    "bigint ordering mismatch: prev={prev_v}, curr={v}"
                );
            }
            prev = Some((key, *v));
        }
    }

    fn assert_monotonic_f32(values: Vec<f32>) {
        let mut sorted = values;
        sorted.sort_by(|a, b| a.total_cmp(b));
        let mut prev: Option<(Vec<u8>, f32)> = None;
        for (i, v) in sorted.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::Float(*v), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = prev_v.total_cmp(v);
                let key_ord = prev_key.cmp(&key);
                assert!(
                    ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                    "float ordering mismatch: prev={prev_v}, curr={v}"
                );
            }
            prev = Some((key, *v));
        }
    }

    fn assert_monotonic_f64(values: Vec<f64>) {
        let mut sorted = values;
        sorted.sort_by(|a, b| a.total_cmp(b));
        let mut prev: Option<(Vec<u8>, f64)> = None;
        for (i, v) in sorted.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::Double(*v), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = prev_v.total_cmp(v);
                let key_ord = prev_key.cmp(&key);
                assert!(
                    ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                    "double ordering mismatch: prev={prev_v}, curr={v}"
                );
            }
            prev = Some((key, *v));
        }
    }

    fn assert_monotonic_text(values: Vec<&str>) {
        let mut sorted = values;
        sorted.sort();
        let mut prev: Option<(Vec<u8>, &str)> = None;
        for (i, v) in sorted.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::Text((*v).to_string()), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = prev_v.cmp(v);
                let key_ord = prev_key.cmp(&key);
                assert!(
                    ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                    "text ordering mismatch: prev={prev_v}, curr={v}"
                );
            }
            prev = Some((key, *v));
        }
    }

    fn assert_monotonic_blob(values: Vec<Vec<u8>>) {
        let mut sorted = values;
        sorted.sort_by(|a, b| match a.len().cmp(&b.len()) {
            Ordering::Equal => a.cmp(b),
            other => other,
        });
        let mut prev: Option<(Vec<u8>, Vec<u8>)> = None;
        for (i, v) in sorted.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::Blob(v.clone()), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = match prev_v.len().cmp(&v.len()) {
                    Ordering::Equal => prev_v.cmp(v),
                    other => other,
                };
                let key_ord = prev_key.cmp(&key);
                assert!(
                    ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                    "blob ordering mismatch: prev={prev_v:?}, curr={v:?}"
                );
            }
            prev = Some((key, v.clone()));
        }
    }

    fn assert_monotonic_bool() {
        let values = vec![false, true];
        let mut prev: Option<(Vec<u8>, bool)> = None;
        for (i, v) in values.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::Boolean(*v), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = prev_v.cmp(v);
                let key_ord = prev_key.cmp(&key);
                assert_eq!(ordering, key_ord);
            }
            prev = Some((key, *v));
        }
    }

    fn assert_monotonic_timestamp(values: Vec<i64>) {
        let mut sorted = values;
        sorted.sort();
        let mut prev: Option<(Vec<u8>, i64)> = None;
        for (i, v) in sorted.iter().enumerate() {
            let key = KeyEncoder::index_key(1, &SqlValue::Timestamp(*v), i as u64).unwrap();
            if let Some((prev_key, prev_v)) = prev {
                let ordering = prev_v.cmp(v);
                let key_ord = prev_key.cmp(&key);
                assert!(
                    ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                    "timestamp ordering mismatch: prev={prev_v}, curr={v}"
                );
            }
            prev = Some((key, *v));
        }
    }

    proptest! {
        #[test]
        fn prop_integer_order_matches_encoded(a in any::<i32>(), b in any::<i32>()) {
            let va = SqlValue::Integer(a);
            let vb = SqlValue::Integer(b);
            let ka = KeyEncoder::index_key(1, &va, 0).unwrap();
            let kb = KeyEncoder::index_key(1, &vb, 1).unwrap();
            let ord = a.cmp(&b);
            let kord = ka.cmp(&kb);
            prop_assert!(ord == kord || (ord == Ordering::Equal && kord == Ordering::Less));
        }

        #[test]
        fn prop_bigint_order_matches_encoded(a in any::<i64>(), b in any::<i64>()) {
            let va = SqlValue::BigInt(a);
            let vb = SqlValue::BigInt(b);
            let ka = KeyEncoder::index_key(1, &va, 0).unwrap();
            let kb = KeyEncoder::index_key(1, &vb, 1).unwrap();
            let ord = a.cmp(&b);
            let kord = ka.cmp(&kb);
            prop_assert!(ord == kord || (ord == Ordering::Equal && kord == Ordering::Less));
        }

        #[test]
        fn prop_float_order_matches_encoded(a in any::<f32>(), b in any::<f32>()) {
            let va = SqlValue::Float(a);
            let vb = SqlValue::Float(b);
            let ka = KeyEncoder::index_key(1, &va, 0).unwrap();
            let kb = KeyEncoder::index_key(1, &vb, 1).unwrap();
            let ord = a.total_cmp(&b);
            let kord = ka.cmp(&kb);
            prop_assert!(ord == kord || (ord == Ordering::Equal && kord == Ordering::Less));
        }

        #[test]
        fn prop_double_order_matches_encoded(a in any::<f64>(), b in any::<f64>()) {
            let va = SqlValue::Double(a);
            let vb = SqlValue::Double(b);
            let ka = KeyEncoder::index_key(1, &va, 0).unwrap();
            let kb = KeyEncoder::index_key(1, &vb, 1).unwrap();
            let ord = a.total_cmp(&b);
            let kord = ka.cmp(&kb);
            prop_assert!(ord == kord || (ord == Ordering::Equal && kord == Ordering::Less));
        }

        #[test]
        fn prop_text_order_matches_encoded(a in ".*", b in ".*") {
            let va = SqlValue::Text(a.clone());
            let vb = SqlValue::Text(b.clone());
            let ka = KeyEncoder::index_key(1, &va, 0).unwrap();
            let kb = KeyEncoder::index_key(1, &vb, 1).unwrap();
            let ord = a.cmp(&b);
            let kord = ka.cmp(&kb);
            prop_assert!(ord == kord || (ord == Ordering::Equal && kord == Ordering::Less));
        }

        #[test]
        fn prop_blob_order_matches_encoded(a in proptest::collection::vec(any::<u8>(), 0..32), b in proptest::collection::vec(any::<u8>(), 0..32)) {
            let va = SqlValue::Blob(a.clone());
            let vb = SqlValue::Blob(b.clone());
            let ka = KeyEncoder::index_key(1, &va, 0).unwrap();
            let kb = KeyEncoder::index_key(1, &vb, 1).unwrap();
            let ord = match a.len().cmp(&b.len()) {
                Ordering::Equal => a.cmp(&b),
                other => other,
            };
            let kord = ka.cmp(&kb);
            prop_assert!(ord == kord || (ord == Ordering::Equal && kord == Ordering::Less));
        }
    }
}
