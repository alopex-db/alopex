use alopex_core::sql::stream::ByteSized;

use crate::executor::ExecutorError;
use crate::storage::SqlValue;

pub use alopex_core::sql::stream::{
    DEFAULT_SPILL_THRESHOLD_BYTES, MemoryPolicy, MemoryTracker, SpillMetricsSink, SpillPolicy,
};

pub(crate) fn map_core_memory_error(err: alopex_core::Error) -> ExecutorError {
    match err {
        alopex_core::Error::MemoryLimitExceeded { limit, requested } => {
            ExecutorError::ResourceExhausted {
                message: format!("query memory limit exceeded: {requested} bytes (limit {limit})"),
            }
        }
        other => ExecutorError::Core(other),
    }
}

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
            SqlValue::Date(_) => 4,
            SqlValue::Time(_) => 8,
            SqlValue::Interval { .. } => 16,
            SqlValue::Decimal(_) => 17,
            SqlValue::Json(value) => value.as_str().len() as u64,
            SqlValue::Vector(values) => values.len() as u64 * 4,
            SqlValue::Array(values) => values.iter().map(ByteSized::estimated_bytes).sum(),
            SqlValue::Map(values) => values
                .iter()
                .map(|(key, value)| key.estimated_bytes() + value.estimated_bytes())
                .sum(),
            SqlValue::Struct(values) => values
                .iter()
                .map(|(name, value)| name.len() as u64 + value.estimated_bytes())
                .sum(),
        }
    }
}
