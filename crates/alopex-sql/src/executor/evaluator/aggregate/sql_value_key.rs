use std::convert::TryFrom;

use crate::storage::SqlValue;

/// Hashable wrapper for a single SqlValue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlValueKey {
    bytes: Vec<u8>,
}

impl SqlValueKey {
    pub fn from_value(value: &SqlValue) -> Self {
        Self {
            bytes: value_to_bytes(value),
        }
    }
}

/// Convert SqlValue to hashable bytes.
pub(crate) fn value_to_bytes(value: &SqlValue) -> Vec<u8> {
    let payload_len = match value {
        SqlValue::Null => 0,
        SqlValue::Integer(_) => std::mem::size_of::<i32>(),
        SqlValue::BigInt(_) => std::mem::size_of::<i64>(),
        SqlValue::Float(_) => std::mem::size_of::<u32>(),
        SqlValue::Double(_) => std::mem::size_of::<u64>(),
        SqlValue::Text(s) => s.len(),
        SqlValue::Blob(bytes) => bytes.len(),
        SqlValue::Boolean(_) => std::mem::size_of::<u8>(),
        SqlValue::Timestamp(_) => std::mem::size_of::<i64>(),
        SqlValue::Vector(values) => values.len() * std::mem::size_of::<u32>(),
    };

    let mut bytes = Vec::with_capacity(1 + 4 + payload_len);
    bytes.push(value.type_tag());

    let len = u32::try_from(payload_len)
        .expect("value bytes exceed u32::MAX (design limit for aggregation keys)");
    bytes.extend_from_slice(&len.to_le_bytes());

    match value {
        SqlValue::Null => {}
        SqlValue::Integer(v) => bytes.extend_from_slice(&v.to_le_bytes()),
        SqlValue::BigInt(v) => bytes.extend_from_slice(&v.to_le_bytes()),
        SqlValue::Float(v) => bytes.extend_from_slice(&v.to_bits().to_le_bytes()),
        SqlValue::Double(v) => bytes.extend_from_slice(&v.to_bits().to_le_bytes()),
        SqlValue::Text(s) => bytes.extend_from_slice(s.as_bytes()),
        SqlValue::Blob(blob) => bytes.extend_from_slice(blob),
        SqlValue::Boolean(v) => bytes.push(u8::from(*v)),
        SqlValue::Timestamp(v) => bytes.extend_from_slice(&v.to_le_bytes()),
        SqlValue::Vector(values) => {
            for v in values {
                bytes.extend_from_slice(&v.to_bits().to_le_bytes());
            }
        }
    }

    bytes
}
