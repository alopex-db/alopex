//! 複数の KV 実装（MemoryKV / LsmKV / S3KV）を 1 つの型にまとめるためのラッパー。
//!
//! `KVStore` / `KVTransaction` は GAT を含むため `dyn KVStore` としてオブジェクト安全に扱いづらい。
//! そのため、列挙型で具体型をまとめて扱う。

use crate::error::Result;
use crate::kv::memory::{MemoryKV, MemoryTransaction, MemoryTxnManager};
use crate::kv::{KVStore, KVTransaction};
use crate::lsm::{LsmKV, LsmTransaction, LsmTxnManagerRef};
use crate::txn::TxnManager;
use crate::types::{TxnId, TxnMode};

#[cfg(feature = "s3")]
use crate::kv::s3::S3KV;

/// ディスク/インメモリ/S3 いずれの KV も扱えるラッパー。
pub enum AnyKV {
    /// 既存のインメモリ KV（永続化パスも持つ）。
    Memory(MemoryKV),
    /// LSM-Tree（file-mode）。
    Lsm(Box<LsmKV>),
    /// S3-backed storage (requires `s3` feature).
    #[cfg(feature = "s3")]
    S3(Box<S3KV>),
}

impl AnyKV {
    /// MemTable をフラッシュする。
    pub fn flush(&self) -> Result<()> {
        match self {
            Self::Memory(kv) => kv.flush(),
            Self::Lsm(kv) => kv.flush(),
            #[cfg(feature = "s3")]
            Self::S3(kv) => kv.flush(),
        }
    }
}

/// `AnyKV` のトランザクション。
pub enum AnyKVTransaction<'a> {
    /// MemoryKV のトランザクション。
    Memory(MemoryTransaction<'a>),
    /// LsmKV のトランザクション。
    Lsm(LsmTransaction<'a>),
}

impl<'a> KVTransaction<'a> for AnyKVTransaction<'a> {
    fn id(&self) -> TxnId {
        match self {
            Self::Memory(tx) => tx.id(),
            Self::Lsm(tx) => tx.id(),
        }
    }

    fn mode(&self) -> TxnMode {
        match self {
            Self::Memory(tx) => tx.mode(),
            Self::Lsm(tx) => tx.mode(),
        }
    }

    fn get(&mut self, key: &crate::types::Key) -> Result<Option<crate::types::Value>> {
        match self {
            Self::Memory(tx) => tx.get(key),
            Self::Lsm(tx) => tx.get(key),
        }
    }

    fn put(&mut self, key: crate::types::Key, value: crate::types::Value) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.put(key, value),
            Self::Lsm(tx) => tx.put(key, value),
        }
    }

    fn delete(&mut self, key: crate::types::Key) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.delete(key),
            Self::Lsm(tx) => tx.delete(key),
        }
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (crate::types::Key, crate::types::Value)> + '_>> {
        match self {
            Self::Memory(tx) => tx.scan_prefix(prefix),
            Self::Lsm(tx) => tx.scan_prefix(prefix),
        }
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (crate::types::Key, crate::types::Value)> + '_>> {
        match self {
            Self::Memory(tx) => tx.scan_range(start, end),
            Self::Lsm(tx) => tx.scan_range(start, end),
        }
    }

    fn commit_self(self) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.commit_self(),
            Self::Lsm(tx) => tx.commit_self(),
        }
    }

    fn rollback_self(self) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.rollback_self(),
            Self::Lsm(tx) => tx.rollback_self(),
        }
    }
}

/// `AnyKV` のトランザクションマネージャ（薄いラッパー）。
#[derive(Clone, Copy)]
pub enum AnyKVManager<'a> {
    /// MemoryKV のマネージャ参照。
    Memory(&'a MemoryTxnManager),
    /// LsmKV のマネージャ参照。
    Lsm(LsmTxnManagerRef<'a>),
}

impl<'a> TxnManager<'a, AnyKVTransaction<'a>> for AnyKVManager<'a> {
    fn begin(&'a self, mode: TxnMode) -> Result<AnyKVTransaction<'a>> {
        match self {
            Self::Memory(m) => Ok(AnyKVTransaction::Memory(m.begin(mode)?)),
            Self::Lsm(m) => Ok(AnyKVTransaction::Lsm(m.begin(mode)?)),
        }
    }

    fn commit(&'a self, txn: AnyKVTransaction<'a>) -> Result<()> {
        txn.commit_self()
    }

    fn rollback(&'a self, txn: AnyKVTransaction<'a>) -> Result<()> {
        txn.rollback_self()
    }
}

impl KVStore for AnyKV {
    type Transaction<'a>
        = AnyKVTransaction<'a>
    where
        Self: 'a;
    type Manager<'a>
        = AnyKVManager<'a>
    where
        Self: 'a;

    fn txn_manager(&self) -> Self::Manager<'_> {
        match self {
            Self::Memory(kv) => AnyKVManager::Memory(kv.txn_manager()),
            Self::Lsm(kv) => AnyKVManager::Lsm(kv.as_ref().txn_manager()),
            #[cfg(feature = "s3")]
            Self::S3(kv) => AnyKVManager::Lsm(kv.inner().txn_manager()),
        }
    }

    fn begin(&self, mode: TxnMode) -> Result<Self::Transaction<'_>> {
        match self {
            Self::Memory(kv) => Ok(AnyKVTransaction::Memory(kv.begin(mode)?)),
            Self::Lsm(kv) => Ok(AnyKVTransaction::Lsm(kv.as_ref().begin(mode)?)),
            #[cfg(feature = "s3")]
            Self::S3(kv) => Ok(AnyKVTransaction::Lsm(kv.inner().begin(mode)?)),
        }
    }
}
