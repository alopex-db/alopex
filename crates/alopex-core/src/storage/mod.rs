//! Storage layer modules for durable data paths.

pub mod checksum;
pub mod compression;
pub mod flush;
pub mod format;
pub mod large_value;
pub mod sstable;

// Re-export V2 compression types for columnar storage
pub use compression::{create_compressor, CompressionV2, Compressor, NoneCompressor};

#[cfg(feature = "compression-lz4")]
pub use compression::Lz4Compressor;

#[cfg(feature = "compression-zstd")]
pub use compression::ZstdCompressor;
