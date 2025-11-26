//! The core crate for AlopexDB, providing low-level storage primitives.

#![deny(missing_docs)]

pub mod error;
pub mod kv;
pub mod log;
pub mod storage;
pub mod txn;
pub mod types;
pub mod vector;

pub use error::{Error, Result};
pub use kv::memory::{MemoryKV, MemoryTransaction, MemoryTxnManager};
pub use kv::{KVStore, KVTransaction};
pub use storage::large_value::{
    LargeValueChunkInfo, LargeValueKind, LargeValueMeta, LargeValueReader, LargeValueWriter,
    DEFAULT_CHUNK_SIZE,
};
pub use txn::TxnManager;
pub use types::{Key, TxnId, TxnMode, Value};
pub use vector::{score, validate_dimensions, Metric, VectorType};
