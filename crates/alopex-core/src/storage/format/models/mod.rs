//! ファイル形式で利用するシリアライズモデル群。
//!
//! bincodeのエンコード設定もここで一元管理する。

pub mod ephemeral;
pub mod metadata;
pub mod raft;

pub use ephemeral::{
    EphemeralDataGcConfig, IntentEntry, IntentSection, IntentType, LockEntry, LockSection, LockType,
};
pub use metadata::{
    bincode_config, ColumnDefinition, IndexDefinition, Metadata, RangeMetadata, TableSchema,
    VectorIndexMetadata, VectorMetric,
};
pub use raft::{RaftEntryType, RaftLogEntry, RaftLogSection};
