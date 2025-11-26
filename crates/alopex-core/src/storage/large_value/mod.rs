//! Chunked large value storage with typed/opaque metadata.
//!
//! This module provides a minimal writer/reader for streaming large values
//! in fixed-size chunks with a crc32-protected body. Both typed columns and
//! opaque blobs share the same container format.

/// Chunked writer/reader primitives.
pub mod chunk;

pub use chunk::{
    LargeValueChunkInfo, LargeValueKind, LargeValueMeta, LargeValueReader, LargeValueWriter,
    DEFAULT_CHUNK_SIZE,
};
