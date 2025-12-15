//! The core crate for AlopexDB, providing low-level storage primitives.

#![deny(missing_docs)]

pub mod columnar;
pub mod compaction;
pub mod error;
pub mod kv;
pub mod log;
pub mod lsm;
pub mod obs;
pub mod storage;
pub mod txn;
pub mod types;
pub mod vector;

pub use columnar::encoding::{
    decode_column, encode_column, Column, Compression, Encoding, LogicalType,
};
pub use columnar::segment::{write_segment, ChunkIter, SegmentMeta, SegmentReader};
pub use error::{Error, Result};
pub use kv::memory::{MemoryKV, MemoryStats, MemoryTransaction, MemoryTxnManager};
pub use kv::storage::{StorageFactory, StorageMode};
pub use kv::{KVStore, KVTransaction};
pub use storage::large_value::{
    LargeValueChunkInfo, LargeValueKind, LargeValueMeta, LargeValueReader, LargeValueWriter,
    DEFAULT_CHUNK_SIZE,
};
pub use txn::TxnManager;
pub use types::{Key, TxnId, TxnMode, Value};
pub use vector::columnar::{
    AppendResult, EncodedColumn, SearchStats, VectorSearchParams, VectorSearchResult,
    VectorSegment, VectorStoreConfig, VectorStoreManager,
};
pub use vector::flat::{search_flat, ScoredItem};
pub use vector::hnsw::{HnswConfig, HnswIndex, HnswSearchResult, HnswStats};
pub use vector::simd::{select_kernel, DistanceKernel, ScalarKernel};
pub use vector::{score, validate_dimensions, Metric, VectorType};
