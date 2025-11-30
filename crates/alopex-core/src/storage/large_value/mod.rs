//! Chunked large value storage with typed/opaque metadata.
//!
//! This module provides a minimal writer/reader for streaming large values
//! in fixed-size chunks with a crc32-protected body. Both typed columns and
//! opaque blobs share the same container format. Backpressure is controlled
//! by `chunk_size`: both writer and reader only buffer a single chunk at a time
//! to keep memory bounded at O(chunk).

/// Chunked writer/reader primitives.
pub mod chunk;
/// Helpers for streaming large values to/from standard IO traits.
pub mod stream;

pub use chunk::{
    LargeValueChunkInfo, LargeValueKind, LargeValueMeta, LargeValueReader, LargeValueWriter,
    DEFAULT_CHUNK_SIZE,
};
pub use stream::{copy_from_reader, drain_to_writer};
