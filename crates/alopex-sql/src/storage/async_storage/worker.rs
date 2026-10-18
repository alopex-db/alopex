//! Borrowed access to the synchronous transaction on its owning worker.

use std::marker::PhantomData;

use alopex_core::kv::{JournalPendingWrite, KVStore, KVTransaction};
use alopex_core::txn::TxnManager;
use alopex_core::types::{Key, TxnId, TxnMode, Value};
use alopex_core::{Error, Result};

pub(super) struct WorkerStore<'txn>(PhantomData<fn(&'txn ())>);
pub(super) struct WorkerManager<'txn>(PhantomData<fn(&'txn ())>);
pub(super) struct WorkerTransaction<'view, 'txn> {
    pub(super) raw: &'view mut (dyn KVTransaction<'txn> + 'view),
}

impl<'view, 'txn> KVTransaction<'view> for WorkerTransaction<'view, 'txn> {
    fn id(&self) -> TxnId {
        self.raw.id()
    }
    fn mode(&self) -> TxnMode {
        self.raw.mode()
    }
    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.raw.get(key)
    }
    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.raw.put(key, value)
    }
    fn delete(&mut self, key: Key) -> Result<()> {
        self.raw.delete(key)
    }
    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.raw.scan_prefix(prefix)
    }
    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.raw.scan_range(start, end)
    }
    fn scan_from(&mut self, start: &[u8]) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.raw.scan_from(start)
    }
    fn journal_pending_writes(&self) -> Option<Vec<JournalPendingWrite>> {
        self.raw.journal_pending_writes()
    }
    fn visit_pending_write_keys(&self, visitor: &mut dyn FnMut(&[u8])) -> bool {
        self.raw.visit_pending_write_keys(visitor)
    }
    fn commit_self(self) -> Result<()> {
        Err(Error::InvalidFormat("borrowed worker cannot commit".into()))
    }
    fn rollback_self(self) -> Result<()> {
        Err(Error::InvalidFormat(
            "borrowed worker cannot roll back".into(),
        ))
    }
}

impl<'view, 'txn> TxnManager<'view, WorkerTransaction<'view, 'txn>> for WorkerManager<'txn> {
    fn begin(&'view self, _mode: TxnMode) -> Result<WorkerTransaction<'view, 'txn>> {
        Err(Error::InvalidFormat(
            "worker transaction is already open".into(),
        ))
    }
    fn commit(&'view self, _txn: WorkerTransaction<'view, 'txn>) -> Result<()> {
        Err(Error::InvalidFormat("borrowed worker cannot commit".into()))
    }
    fn rollback(&'view self, _txn: WorkerTransaction<'view, 'txn>) -> Result<()> {
        Err(Error::InvalidFormat(
            "borrowed worker cannot roll back".into(),
        ))
    }
}

impl<'txn> KVStore for WorkerStore<'txn> {
    type Transaction<'view>
        = WorkerTransaction<'view, 'txn>
    where
        Self: 'view;
    type Manager<'view>
        = WorkerManager<'txn>
    where
        Self: 'view;
    fn txn_manager(&self) -> Self::Manager<'_> {
        WorkerManager(PhantomData)
    }
    fn begin(&self, _mode: TxnMode) -> Result<Self::Transaction<'_>> {
        Err(Error::InvalidFormat(
            "worker transaction is already open".into(),
        ))
    }
}
