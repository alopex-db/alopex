//! Delegate real storage operations while bounding a repeated target MAX scan.
use std::sync::{Arc, Mutex};

use alopex_core::Result;
use alopex_core::kv::memory::{MemoryKV, MemoryTransaction};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::txn::TxnManager;
use alopex_core::types::{Key, TxnId, TxnMode, Value};

pub const MARKER: &str = "target MAX scan repeated";

#[derive(Default)]
struct Guard {
    key: Option<Key>,
    scans: usize,
}

#[derive(Default)]
pub struct GuardedStore {
    inner: MemoryKV,
    guard: Arc<Mutex<Guard>>,
}

impl GuardedStore {
    pub fn arm(&self, key: Key) {
        *self.guard.lock().unwrap() = Guard {
            key: Some(key),
            scans: 0,
        };
    }

    pub fn disarm(&self) -> usize {
        let mut guard = self.guard.lock().unwrap();
        guard.key = None;
        guard.scans
    }
}

pub struct GuardedTransaction<'a> {
    inner: MemoryTransaction<'a>,
    guard: Arc<Mutex<Guard>>,
}

impl KVStore for GuardedStore {
    type Transaction<'a> = GuardedTransaction<'a>;
    type Manager<'a> = &'a Self;
    fn txn_manager(&self) -> Self::Manager<'_> {
        self
    }
    fn begin(&self, mode: TxnMode) -> Result<Self::Transaction<'_>> {
        Ok(GuardedTransaction {
            inner: self.inner.begin(mode)?,
            guard: self.guard.clone(),
        })
    }
}

impl<'a> TxnManager<'a, GuardedTransaction<'a>> for &'a GuardedStore {
    fn begin(&'a self, mode: TxnMode) -> Result<GuardedTransaction<'a>> {
        KVStore::begin(*self, mode)
    }
    fn commit(&'a self, txn: GuardedTransaction<'a>) -> Result<()> {
        txn.commit_self()
    }
    fn rollback(&'a self, txn: GuardedTransaction<'a>) -> Result<()> {
        txn.rollback_self()
    }
}

impl<'a> KVTransaction<'a> for GuardedTransaction<'a> {
    fn id(&self) -> TxnId {
        self.inner.id()
    }
    fn mode(&self) -> TxnMode {
        self.inner.mode()
    }
    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.inner.get(key)
    }
    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.inner.put(key, value)
    }
    fn delete(&mut self, key: Key) -> Result<()> {
        self.inner.delete(key)
    }
    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.inner.scan_prefix(prefix)
    }
    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        {
            let mut guard = self.guard.lock().unwrap();
            if guard.key.as_deref() == Some(start) {
                guard.scans += 1;
                if guard.scans >= 2 {
                    return Err(std::io::Error::other(MARKER).into());
                }
            }
        }
        self.inner.scan_range(start, end)
    }
    fn scan_from(&mut self, start: &[u8]) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.inner.scan_from(start)
    }
    fn commit_self(self) -> Result<()> {
        self.inner.commit_self()
    }
    fn rollback_self(self) -> Result<()> {
        self.inner.rollback_self()
    }
}
