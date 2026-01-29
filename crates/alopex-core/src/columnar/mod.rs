//! Columnar storage utilities.

#[cfg(feature = "async")]
pub mod async_columnar;
pub mod encoding;
pub mod encoding_v2;
pub mod error;
pub mod kvs_bridge;
#[cfg(not(target_arch = "wasm32"))]
pub mod memory;
pub mod segment;
pub mod segment_v2;
pub mod statistics;

#[cfg(feature = "tokio")]
pub use async_columnar::AsyncColumnarReaderAdapter;
#[cfg(feature = "async")]
pub use async_columnar::{AsyncColumnarReader, ColumnId, RowBatch, Segment, SegmentId};
pub use encoding::Encoding;
pub use encoding_v2::EncodingV2;
pub use error::ColumnarError;
pub use kvs_bridge::ColumnarKvsBridge;
#[cfg(not(target_arch = "wasm32"))]
pub use memory::InMemorySegmentStore;
pub use segment::SegmentReader;
pub use segment_v2::{SegmentReaderV2, SegmentWriterV2};
pub use statistics::{ColumnStatistics, SegmentStatistics};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod disk;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod integration;
