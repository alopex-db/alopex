//! Columnar storage utilities.

pub mod encoding;
pub mod encoding_v2;
pub mod error;
pub mod kvs_bridge;
pub mod memory;
pub mod segment;
pub mod segment_v2;
pub mod statistics;

pub use error::{ColumnarError, Result as ColumnarResult};
