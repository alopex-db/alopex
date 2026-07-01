use alopex_core::sql::stream::ByteSized;

use crate::storage::SqlValue;

pub use alopex_core::sql::stream::{
    DEFAULT_SPILL_THRESHOLD_BYTES, MemoryPolicy, MemoryTracker, SpillMetricsSink, SpillPolicy,
};

impl ByteSized for SqlValue {
    fn estimated_bytes(&self) -> u64 {
        match self {
            SqlValue::Null => 0,
            SqlValue::Integer(_) => 4,
            SqlValue::BigInt(_) => 8,
            SqlValue::Float(_) => 4,
            SqlValue::Double(_) => 8,
            SqlValue::Text(text) => text.len() as u64,
            SqlValue::Blob(blob) => blob.len() as u64,
            SqlValue::Boolean(_) => 1,
            SqlValue::Timestamp(_) => 8,
            SqlValue::Vector(values) => values.len() as u64 * 4,
        }
    }
}
