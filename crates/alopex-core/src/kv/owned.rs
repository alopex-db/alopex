//! Owned key-value session contracts for long-lived local consumers.
//!
//! The legacy [`crate::kv::KVStore`] API intentionally returns transactions borrowed from the
//! store.  Those transactions remain the compatibility API.  Python and asynchronous stream
//! paths need a different boundary: every transaction, cursor, and store reference must be
//! owned without extending a Rust borrow through `unsafe` or a foreign-runtime lifetime.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::kv::read_at::{ReadAtCapability, ReadAtPoint, ReadAtResult};
use crate::kv::KVTransaction;
use crate::txn::{OwnedLeaseOutcome, OwnedReadSessionStatus, OwnedTransactionSessionStatus};
use crate::types::{Key, TxnId, TxnMode, Value};

/// An incremental key/value cursor whose state outlives the method that opened it.
///
/// Implementations must own every snapshot guard and backend reference needed to advance.  A
/// cursor must never borrow an [`OwnedKVTransaction`] through a raw pointer or an extended
/// lifetime.
pub trait OwnedKVScan: Send {
    /// Return one key/value pair, or `None` after normal exhaustion.
    fn next_entry(&mut self) -> Result<Option<(Key, Value)>>;

    /// Release backend cursor resources.  Repeated calls must be harmless.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A transaction that owns its backend state instead of borrowing a [`crate::kv::KVStore`].
///
/// `commit` and `rollback` consume the boxed transaction.  This makes a terminal action
/// unrepeatable even if a caller retains an `OwnedTransactionSession` handle.
pub trait OwnedKVTransaction: Send {
    /// Return this transaction's stable identifier.
    fn id(&self) -> TxnId;

    /// Return whether this transaction permits writes.
    fn mode(&self) -> TxnMode;

    /// Read one key at the transaction's snapshot.
    fn get(&mut self, key: &Key) -> Result<Option<Value>>;

    /// Stage one value for commit.
    fn put(&mut self, key: Key, value: Value) -> Result<()>;

    /// Stage one deletion for commit.
    fn delete(&mut self, key: Key) -> Result<()>;

    /// Open an owned prefix cursor at the transaction's snapshot.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Box<dyn OwnedKVScan>>;

    /// Open an owned half-open range cursor at the transaction's snapshot.
    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<Box<dyn OwnedKVScan>>;

    /// Commit once.  The caller cannot use this transaction afterwards.
    fn commit(self: Box<Self>) -> Result<()>;

    /// Roll back once.  The caller cannot use this transaction afterwards.
    fn rollback(self: Box<Self>) -> Result<()>;
}

/// A short-lived [`KVTransaction`] view over an owned transaction.
///
/// This adapter is only for synchronous compatibility operations such as the embedded SQL
/// executor.  It cannot commit or roll back: the [`OwnedTransactionSession`] remains the only
/// owner of the terminal transition.  Public streams must use [`OwnedKVScan`] directly.
///
/// The legacy `KVTransaction` iterator has no error channel for `next`, so a cursor is fully
/// collected before it is exposed through that trait.  It is consequently not a streaming
/// primitive and is never used by public stream paths.
pub struct OwnedKVTransactionAdapter<'a> {
    transaction: &'a mut dyn OwnedKVTransaction,
}

impl<'a> OwnedKVTransactionAdapter<'a> {
    /// Borrow an owned transaction for one synchronous compatibility operation.
    pub fn new(transaction: &'a mut dyn OwnedKVTransaction) -> Self {
        Self { transaction }
    }

    fn collect_cursor(cursor: Box<dyn OwnedKVScan>) -> Result<Vec<(Key, Value)>> {
        let mut cursor = cursor;
        let mut entries = Vec::new();
        while let Some(entry) = cursor.next_entry()? {
            entries.push(entry);
        }
        cursor.close()?;
        Ok(entries)
    }
}

impl<'a> KVTransaction<'a> for OwnedKVTransactionAdapter<'a> {
    fn id(&self) -> TxnId {
        self.transaction.id()
    }

    fn mode(&self) -> TxnMode {
        self.transaction.mode()
    }

    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.transaction.get(key)
    }

    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.transaction.put(key, value)
    }

    fn delete(&mut self, key: Key) -> Result<()> {
        self.transaction.delete(key)
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        let entries = Self::collect_cursor(self.transaction.scan_prefix(prefix)?)?;
        Ok(Box::new(entries.into_iter()))
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        let entries = Self::collect_cursor(self.transaction.scan_range(start, end)?)?;
        Ok(Box::new(entries.into_iter()))
    }

    fn commit_self(self) -> Result<()> {
        Err(Error::InvalidParameter {
            param: "owned_transaction_adapter".to_owned(),
            reason: "terminal ownership belongs to OwnedTransactionSession".to_owned(),
        })
    }

    fn rollback_self(self) -> Result<()> {
        Err(Error::InvalidParameter {
            param: "owned_transaction_adapter".to_owned(),
            reason: "terminal ownership belongs to OwnedTransactionSession".to_owned(),
        })
    }
}

/// A store that can create an owned transaction without returning a borrowed facade.
///
/// Backend-specific implementations are introduced separately.  The contract is deliberately
/// distinct from [`crate::kv::KVStore`] so existing borrowed APIs retain their v0.7 behavior.
pub trait OwnedKVStore: Send + Sync + 'static {
    /// Begin a transaction whose lifetime is independent of the `Arc<Self>` call site.
    fn begin_owned_kv_transaction(
        self: Arc<Self>,
        mode: TxnMode,
    ) -> Result<Box<dyn OwnedKVTransaction>>;

    /// Reports whether this store can bind an owned transaction to a retained
    /// cluster read point. A normal local owned transaction is never evidence
    /// for a distributed snapshot, so the safe default is unavailable.
    fn owned_read_at_capability(&self) -> ReadAtCapability {
        ReadAtCapability::unavailable("owned backend does not prove retained cluster read points")
    }

    /// Begin an owned transaction at one complete, cluster-issued read point.
    ///
    /// Backends must override this only after proving the requested data,
    /// metadata, schema, and index epochs are retained together. The default
    /// deliberately refuses to substitute [`Self::begin_owned_kv_transaction`]
    /// for `point`.
    fn begin_owned_kv_transaction_at(
        self: Arc<Self>,
        point: &ReadAtPoint,
        _mode: TxnMode,
    ) -> ReadAtResult<Box<dyn OwnedKVTransaction>> {
        Err(self.owned_read_at_capability().unavailable_error(
            point,
            "owned backend did not implement begin_owned_kv_transaction_at",
        ))
    }
}

/// Options shared by owned read session factories.
///
/// Resource limits and deadlines belong to higher query/DataFrame layers.  Keeping this type
/// explicit makes that later wiring additive without changing the ownership boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OwnedReadOptions {}

/// Factory for owned read and transaction sessions.
pub trait OwnedSessionFactory: OwnedKVStore {
    /// Begin an owned read-only session.
    fn begin_owned_read(self: Arc<Self>, _options: OwnedReadOptions) -> Result<OwnedReadSession> {
        let transaction = self.begin_owned_kv_transaction(TxnMode::ReadOnly)?;
        OwnedReadSession::new(transaction)
    }

    /// Begin an owned transaction session.
    fn begin_owned_transaction(self: Arc<Self>, mode: TxnMode) -> Result<OwnedTransactionSession> {
        let transaction = self.begin_owned_kv_transaction(mode)?;
        Ok(OwnedTransactionSession::new(transaction))
    }

    /// Begin an owned transaction at the supplied immutable cluster read point.
    ///
    /// This is the only owned-session entry point available to a distributed
    /// participant. It preserves the local API above while rejecting backends
    /// that have not proven retention of the complete read fence.
    fn begin_owned_transaction_at(
        self: Arc<Self>,
        point: &ReadAtPoint,
        mode: TxnMode,
    ) -> ReadAtResult<OwnedTransactionSession> {
        let transaction = self.begin_owned_kv_transaction_at(point, mode)?;
        Ok(OwnedTransactionSession::new(transaction))
    }
}

impl<T> OwnedSessionFactory for T where T: OwnedKVStore {}

/// Common operations exposed by an owned read session.
pub trait OwnedReadSessionApi: Send + Sync {
    /// Return the current one-way lifecycle state.
    fn status(&self) -> OwnedReadSessionStatus;

    /// Acquire the only lease that may advance this session's transaction/cursors.
    fn acquire_lease(&self) -> Result<OwnedReadLease>;

    /// Close the session and release its read-only transaction.
    fn close(&self) -> Result<OwnedReadSessionStatus>;
}

/// Common operations exposed by an owned transaction session.
pub trait OwnedTransactionSessionApi: Send + Sync {
    /// Return the current one-way lifecycle state.
    fn status(&self) -> OwnedTransactionSessionStatus;

    /// Acquire the only active stream lease for this transaction.
    fn acquire_lease(&self) -> Result<OwnedTransactionLease>;

    /// Commit once after every active lease has released as committable.
    fn commit(&self) -> Result<OwnedTransactionSessionStatus>;

    /// Roll back once, including the conservative `must abort` path.
    fn rollback(&self) -> Result<OwnedTransactionSessionStatus>;
}

/// A read-only session that owns a transaction until a stream terminal action releases it.
#[derive(Clone)]
pub struct OwnedReadSession {
    inner: Arc<Mutex<OwnedReadInner>>,
}

struct OwnedReadInner {
    transaction: Option<Box<dyn OwnedKVTransaction>>,
    status: OwnedReadSessionStatus,
}

impl Drop for OwnedReadInner {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            let _ = transaction.rollback();
        }
    }
}

impl OwnedReadSession {
    /// Wrap a backend-owned read-only transaction.
    pub fn new(transaction: Box<dyn OwnedKVTransaction>) -> Result<Self> {
        if transaction.mode() != TxnMode::ReadOnly {
            return Err(Error::InvalidParameter {
                param: "owned_read_session".to_owned(),
                reason: "requires a read-only transaction".to_owned(),
            });
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(OwnedReadInner {
                transaction: Some(transaction),
                status: OwnedReadSessionStatus::Open,
            })),
        })
    }

    /// Return the current lifecycle state.
    pub fn status(&self) -> OwnedReadSessionStatus {
        self.lock().status
    }

    /// Acquire the only lease allowed to advance this read session.
    pub fn acquire_lease(&self) -> Result<OwnedReadLease> {
        let mut inner = self.lock();
        if inner.status != OwnedReadSessionStatus::Open || inner.transaction.is_none() {
            return Err(Error::TxnClosed);
        }
        inner.status = OwnedReadSessionStatus::LeaseActive;
        Ok(OwnedReadLease {
            inner: self.inner.clone(),
            released: false,
        })
    }

    /// Close this session.  Closing is idempotent after any terminal state.
    pub fn close(&self) -> Result<OwnedReadSessionStatus> {
        finish_read_session(&self.inner, OwnedLeaseOutcome::Closed)
    }

    fn lock(&self) -> MutexGuard<'_, OwnedReadInner> {
        self.inner
            .lock()
            .expect("owned read session mutex poisoned")
    }
}

impl OwnedReadSessionApi for OwnedReadSession {
    fn status(&self) -> OwnedReadSessionStatus {
        Self::status(self)
    }

    fn acquire_lease(&self) -> Result<OwnedReadLease> {
        Self::acquire_lease(self)
    }

    fn close(&self) -> Result<OwnedReadSessionStatus> {
        Self::close(self)
    }
}

impl fmt::Debug for OwnedReadSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedReadSession")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

/// The single active owner allowed to advance an [`OwnedReadSession`].
pub struct OwnedReadLease {
    inner: Arc<Mutex<OwnedReadInner>>,
    released: bool,
}

impl OwnedReadLease {
    /// Execute one operation while this lease is active.
    ///
    /// The closure cannot retain a borrowed transaction.  It may instead return an owned cursor
    /// from [`OwnedKVTransaction::scan_prefix`] or [`OwnedKVTransaction::scan_range`].
    pub fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&mut dyn OwnedKVTransaction) -> Result<T>,
    ) -> Result<T> {
        let mut inner = self
            .inner
            .lock()
            .expect("owned read session mutex poisoned");
        if inner.status != OwnedReadSessionStatus::LeaseActive {
            return Err(Error::TxnClosed);
        }
        operation(inner.transaction.as_deref_mut().ok_or(Error::TxnClosed)?)
    }

    /// End the lease and release the read transaction exactly once.
    pub fn finish(mut self, outcome: OwnedLeaseOutcome) -> Result<OwnedReadSessionStatus> {
        let result = finish_read_session(&self.inner, outcome);
        self.released = true;
        result
    }
}

impl Drop for OwnedReadLease {
    fn drop(&mut self) {
        if !self.released {
            let _ = finish_read_session(&self.inner, OwnedLeaseOutcome::Closed);
            self.released = true;
        }
    }
}

fn finish_read_session(
    inner: &Arc<Mutex<OwnedReadInner>>,
    outcome: OwnedLeaseOutcome,
) -> Result<OwnedReadSessionStatus> {
    let (transaction, target) = {
        let mut inner = inner.lock().expect("owned read session mutex poisoned");
        if inner.status.is_terminal() {
            return Ok(inner.status);
        }
        let target = OwnedReadSessionStatus::from(outcome);
        let transaction = inner.transaction.take();
        inner.status = target;
        (transaction, target)
    };

    if let Some(transaction) = transaction {
        if transaction.rollback().is_err() {
            let mut inner = inner.lock().expect("owned read session mutex poisoned");
            inner.status = OwnedReadSessionStatus::Failed;
            return Err(Error::TxnClosed);
        }
    }
    Ok(target)
}

/// A transaction session that serializes stream leases and owns exactly one terminal action.
#[derive(Clone)]
pub struct OwnedTransactionSession {
    inner: Arc<Mutex<OwnedTransactionInner>>,
}

struct OwnedTransactionInner {
    transaction: Option<Box<dyn OwnedKVTransaction>>,
    status: OwnedTransactionSessionStatus,
}

impl Drop for OwnedTransactionInner {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            let _ = transaction.rollback();
        }
    }
}

impl OwnedTransactionSession {
    /// Wrap one backend-owned transaction.
    pub fn new(transaction: Box<dyn OwnedKVTransaction>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(OwnedTransactionInner {
                transaction: Some(transaction),
                status: OwnedTransactionSessionStatus::Open,
            })),
        }
    }

    /// Return the current transaction/lease lifecycle state.
    pub fn status(&self) -> OwnedTransactionSessionStatus {
        self.lock().status
    }

    /// Acquire a single active lease.  A later lease may start only after normal exhaustion.
    pub fn acquire_lease(&self) -> Result<OwnedTransactionLease> {
        let mut inner = self.lock();
        if !inner.status.can_acquire_lease() || inner.transaction.is_none() {
            return Err(Error::TxnClosed);
        }
        inner.status = OwnedTransactionSessionStatus::LeaseActive;
        Ok(OwnedTransactionLease {
            inner: self.inner.clone(),
            released: false,
        })
    }

    /// Run one finite compatibility operation without exposing the active lease.
    ///
    /// This is intentionally distinct from a stream lease: the operation cannot retain a
    /// cursor, and a successful or classified operation error releases the transaction as
    /// committable.  A panic or an abandoned lease remains conservative and marks the session
    /// `MustAbort` through [`OwnedTransactionLease::drop`].
    pub fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&mut dyn OwnedKVTransaction) -> Result<T>,
    ) -> Result<T> {
        let lease = self.acquire_lease()?;
        let result = lease.with_transaction(operation);
        let release = lease.finish(OwnedLeaseOutcome::Exhausted);
        match (result, release) {
            (Ok(value), Ok(_)) => Ok(value),
            (Err(error), Ok(_)) => Err(error),
            (_, Err(error)) => Err(error),
        }
    }

    /// Commit this owned transaction once.
    pub fn commit(&self) -> Result<OwnedTransactionSessionStatus> {
        self.finish_transaction(true)
    }

    /// Roll back this owned transaction once.
    pub fn rollback(&self) -> Result<OwnedTransactionSessionStatus> {
        self.finish_transaction(false)
    }

    fn finish_transaction(&self, commit: bool) -> Result<OwnedTransactionSessionStatus> {
        let transaction = {
            let mut inner = self.lock();
            let allowed = if commit {
                inner.status.can_commit()
            } else {
                inner.status.can_rollback()
            };
            if !allowed {
                return Err(Error::TxnClosed);
            }
            inner.status = OwnedTransactionSessionStatus::Closed;
            inner.transaction.take().ok_or(Error::TxnClosed)?
        };

        let result = if commit {
            transaction.commit()
        } else {
            transaction.rollback()
        };
        let mut inner = self.lock();
        inner.status = if result.is_ok() {
            if commit {
                OwnedTransactionSessionStatus::Committed
            } else {
                OwnedTransactionSessionStatus::RolledBack
            }
        } else {
            OwnedTransactionSessionStatus::Closed
        };
        match result {
            Ok(()) => Ok(inner.status),
            Err(error) => Err(error),
        }
    }

    fn lock(&self) -> MutexGuard<'_, OwnedTransactionInner> {
        self.inner
            .lock()
            .expect("owned transaction session mutex poisoned")
    }
}

impl OwnedTransactionSessionApi for OwnedTransactionSession {
    fn status(&self) -> OwnedTransactionSessionStatus {
        Self::status(self)
    }

    fn acquire_lease(&self) -> Result<OwnedTransactionLease> {
        Self::acquire_lease(self)
    }

    fn commit(&self) -> Result<OwnedTransactionSessionStatus> {
        Self::commit(self)
    }

    fn rollback(&self) -> Result<OwnedTransactionSessionStatus> {
        Self::rollback(self)
    }
}

impl fmt::Debug for OwnedTransactionSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedTransactionSession")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

/// The only active owner allowed to advance an [`OwnedTransactionSession`].
pub struct OwnedTransactionLease {
    inner: Arc<Mutex<OwnedTransactionInner>>,
    released: bool,
}

impl OwnedTransactionLease {
    /// Execute one operation while the transaction's stream lease is active.
    pub fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&mut dyn OwnedKVTransaction) -> Result<T>,
    ) -> Result<T> {
        let mut inner = self
            .inner
            .lock()
            .expect("owned transaction session mutex poisoned");
        if inner.status != OwnedTransactionSessionStatus::LeaseActive {
            return Err(Error::TxnClosed);
        }
        operation(inner.transaction.as_deref_mut().ok_or(Error::TxnClosed)?)
    }

    /// Release the active lease and record its effect on later commit/rollback.
    pub fn finish(mut self, outcome: OwnedLeaseOutcome) -> Result<OwnedTransactionSessionStatus> {
        let mut inner = self
            .inner
            .lock()
            .expect("owned transaction session mutex poisoned");
        if inner.status != OwnedTransactionSessionStatus::LeaseActive {
            self.released = true;
            return Err(Error::TxnClosed);
        }
        inner.status = OwnedTransactionSessionStatus::from(outcome);
        self.released = true;
        Ok(inner.status)
    }
}

impl Drop for OwnedTransactionLease {
    fn drop(&mut self) {
        if !self.released {
            let mut inner = self
                .inner
                .lock()
                .expect("owned transaction session mutex poisoned");
            if inner.status == OwnedTransactionSessionStatus::LeaseActive {
                inner.status = OwnedTransactionSessionStatus::MustAbort;
            }
            self.released = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::{
        OwnedKVScan, OwnedKVTransaction, OwnedKVTransactionAdapter, OwnedReadSession,
        OwnedSessionFactory, OwnedTransactionSession,
    };
    use crate::error::Result;
    use crate::kv::{memory::MemoryKV, KVTransaction};
    use crate::txn::{OwnedLeaseOutcome, OwnedReadSessionStatus, OwnedTransactionSessionStatus};
    use crate::types::{Key, TxnId, TxnMode, Value};

    #[derive(Default)]
    struct Calls {
        commits: AtomicUsize,
        rollbacks: AtomicUsize,
    }

    struct TestCursor;

    impl OwnedKVScan for TestCursor {
        fn next_entry(&mut self) -> Result<Option<(Key, Value)>> {
            Ok(None)
        }
    }

    struct TestTransaction {
        calls: Arc<Calls>,
        mode: TxnMode,
    }

    impl TestTransaction {
        fn new(calls: Arc<Calls>, mode: TxnMode) -> Self {
            Self { calls, mode }
        }
    }

    impl OwnedKVTransaction for TestTransaction {
        fn id(&self) -> TxnId {
            TxnId(7)
        }

        fn mode(&self) -> TxnMode {
            self.mode
        }

        fn get(&mut self, _key: &Key) -> Result<Option<Value>> {
            Ok(None)
        }

        fn put(&mut self, _key: Key, _value: Value) -> Result<()> {
            Ok(())
        }

        fn delete(&mut self, _key: Key) -> Result<()> {
            Ok(())
        }

        fn scan_prefix(&mut self, _prefix: &[u8]) -> Result<Box<dyn OwnedKVScan>> {
            Ok(Box::new(TestCursor))
        }

        fn scan_range(&mut self, _start: &[u8], _end: &[u8]) -> Result<Box<dyn OwnedKVScan>> {
            Ok(Box::new(TestCursor))
        }

        fn commit(self: Box<Self>) -> Result<()> {
            self.calls.commits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn rollback(self: Box<Self>) -> Result<()> {
            self.calls.rollbacks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn read_session_releases_its_transaction_once_at_terminal_outcome() {
        let calls = Arc::new(Calls::default());
        let session = OwnedReadSession::new(Box::new(TestTransaction::new(
            calls.clone(),
            TxnMode::ReadOnly,
        )))
        .unwrap();

        let lease = session.acquire_lease().unwrap();
        assert_eq!(session.status(), OwnedReadSessionStatus::LeaseActive);
        assert!(session.acquire_lease().is_err());
        assert_eq!(
            lease.finish(OwnedLeaseOutcome::Exhausted).unwrap(),
            OwnedReadSessionStatus::Exhausted
        );
        assert_eq!(calls.rollbacks.load(Ordering::SeqCst), 1);
        assert_eq!(session.close().unwrap(), OwnedReadSessionStatus::Exhausted);
        assert_eq!(calls.rollbacks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn transaction_session_blocks_commit_while_leased_then_commits_once() {
        let calls = Arc::new(Calls::default());
        let session = OwnedTransactionSession::new(Box::new(TestTransaction::new(
            calls.clone(),
            TxnMode::ReadWrite,
        )));

        let lease = session.acquire_lease().unwrap();
        assert!(session.commit().is_err());
        assert_eq!(
            lease.finish(OwnedLeaseOutcome::Exhausted).unwrap(),
            OwnedTransactionSessionStatus::Committable
        );
        assert_eq!(
            session.commit().unwrap(),
            OwnedTransactionSessionStatus::Committed
        );
        assert_eq!(calls.commits.load(Ordering::SeqCst), 1);
        assert!(session.rollback().is_err());
        assert_eq!(calls.rollbacks.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cancelled_or_dropped_lease_requires_a_single_rollback() {
        let calls = Arc::new(Calls::default());
        let session = OwnedTransactionSession::new(Box::new(TestTransaction::new(
            calls.clone(),
            TxnMode::ReadWrite,
        )));

        let lease = session.acquire_lease().unwrap();
        assert_eq!(
            lease.finish(OwnedLeaseOutcome::Cancelled).unwrap(),
            OwnedTransactionSessionStatus::MustAbort
        );
        assert!(session.commit().is_err());
        assert_eq!(
            session.rollback().unwrap(),
            OwnedTransactionSessionStatus::RolledBack
        );
        assert_eq!(calls.rollbacks.load(Ordering::SeqCst), 1);

        let drop_calls = Arc::new(Calls::default());
        let dropped = OwnedTransactionSession::new(Box::new(TestTransaction::new(
            drop_calls.clone(),
            TxnMode::ReadWrite,
        )));
        drop(dropped.acquire_lease().unwrap());
        assert_eq!(dropped.status(), OwnedTransactionSessionStatus::MustAbort);
        drop(dropped);
        assert_eq!(drop_calls.rollbacks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn compatibility_adapter_never_consumes_the_owned_terminal_transition() {
        let store = Arc::new(MemoryKV::new());
        let session = store
            .clone()
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();

        session
            .with_transaction(|owned| {
                let mut compatibility = OwnedKVTransactionAdapter::new(owned);
                compatibility.put(b"adapter".to_vec(), b"value".to_vec())?;
                let rows = compatibility.scan_prefix(b"adapter")?.collect::<Vec<_>>();
                assert_eq!(rows, vec![(b"adapter".to_vec(), b"value".to_vec())]);
                assert!(compatibility.commit_self().is_err());
                Ok(())
            })
            .unwrap();

        assert_eq!(session.status(), OwnedTransactionSessionStatus::Committable);
        session.commit().unwrap();

        let read = store.begin_owned_read(Default::default()).unwrap();
        let lease = read.acquire_lease().unwrap();
        let value = lease
            .with_transaction(|transaction| transaction.get(&b"adapter".to_vec()))
            .unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
    }
}
