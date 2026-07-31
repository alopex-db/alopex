//! Durable, fenced transaction-participant boundary.
//!
//! This module deliberately owns no distributed coordinator or transport. It
//! binds a single participant's existing owned MVCC transaction to a proven
//! [`ReadAtPoint`], records the full write set in a separate WAL journal, and
//! makes every terminal decision one-way. A durable decision without a durable
//! local-apply acknowledgement is represented as recovery pending rather than
//! as a successful commit or rollback.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::kv::{OwnedSessionFactory, OwnedTransactionSession, ReadAtError, ReadAtPoint};
use crate::log::wal::{ParticipantWalState, ParticipantWalWrite, WalReader, WalRecord, WalWriter};
use crate::types::{Key, TxnMode, Value};

/// Stable participant identity, idempotency key, and immutable read fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantIdentity {
    /// Coordinator-issued transaction identifier.
    pub transaction_id: String,
    /// Stable request identifier used for duplicate-decision lookup.
    pub request_id: String,
    /// Digest of the authenticated participant request envelope.
    pub request_fingerprint: String,
    /// The complete cluster-issued snapshot fence.
    pub read_point: ReadAtPoint,
}

impl ParticipantIdentity {
    /// Construct a participant identity from non-empty stable identifiers.
    pub fn new(
        transaction_id: impl Into<String>,
        request_id: impl Into<String>,
        request_fingerprint: impl Into<String>,
        read_point: ReadAtPoint,
    ) -> std::result::Result<Self, ParticipantTransactionError> {
        let identity = Self {
            transaction_id: transaction_id.into(),
            request_id: request_id.into(),
            request_fingerprint: request_fingerprint.into(),
            read_point,
        };
        if identity.transaction_id.is_empty()
            || identity.request_id.is_empty()
            || identity.request_fingerprint.is_empty()
        {
            return Err(ParticipantTransactionError::InvalidIdentity);
        }
        Ok(identity)
    }
}

/// One key mutation captured in the participant's immutable prepare record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantWrite {
    /// Key affected by the write set.
    pub key: Key,
    /// Value to write, or `None` for a deletion.
    pub value: Option<Value>,
}

/// One durable participant journal phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantJournalPhase {
    /// The read fence and full write set reached stable storage.
    Prepared,
    /// Commit is durable but local commit may not be durably acknowledged yet.
    CommitDecision,
    /// Local commit completed and its acknowledgement reached stable storage.
    CommitApplied,
    /// Abort is durable but local rollback may not be durably acknowledged yet.
    AbortDecision,
    /// Local rollback completed and its acknowledgement reached stable storage.
    AbortApplied,
}

impl ParticipantJournalPhase {
    fn from_wal(state: ParticipantWalState) -> Self {
        match state {
            ParticipantWalState::Prepared => Self::Prepared,
            ParticipantWalState::CommitDecision => Self::CommitDecision,
            ParticipantWalState::CommitApplied => Self::CommitApplied,
            ParticipantWalState::AbortDecision => Self::AbortDecision,
            ParticipantWalState::AbortApplied => Self::AbortApplied,
        }
    }

    fn wal_state(self) -> ParticipantWalState {
        match self {
            Self::Prepared => ParticipantWalState::Prepared,
            Self::CommitDecision => ParticipantWalState::CommitDecision,
            Self::CommitApplied => ParticipantWalState::CommitApplied,
            Self::AbortDecision => ParticipantWalState::AbortDecision,
            Self::AbortApplied => ParticipantWalState::AbortApplied,
        }
    }

    fn allows_next(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Prepared, Self::CommitDecision)
                    | (Self::Prepared, Self::AbortDecision)
                    | (Self::CommitDecision, Self::CommitApplied)
                    | (Self::AbortDecision, Self::AbortApplied)
            )
    }
}

/// Full durable participant record retained by the decision store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantDecisionRecord {
    /// Immutable participant identity and read fence.
    pub identity: ParticipantIdentity,
    /// Complete staged write set in deterministic key order.
    pub writes: Vec<ParticipantWrite>,
    /// Last durable phase for this request.
    pub phase: ParticipantJournalPhase,
}

impl ParticipantDecisionRecord {
    fn outcome(&self) -> ParticipantDecisionResult {
        match self.phase {
            ParticipantJournalPhase::Prepared => ParticipantDecisionResult::Prepared,
            ParticipantJournalPhase::CommitApplied => ParticipantDecisionResult::Committed,
            ParticipantJournalPhase::AbortApplied => ParticipantDecisionResult::Aborted,
            ParticipantJournalPhase::CommitDecision => ParticipantDecisionResult::RecoveryPending {
                decision: Some(ParticipantDecision::Commit),
                reason: "commit decision is durable but local commit acknowledgement is absent"
                    .to_owned(),
            },
            ParticipantJournalPhase::AbortDecision => ParticipantDecisionResult::RecoveryPending {
                decision: Some(ParticipantDecision::Abort),
                reason: "abort decision is durable but local rollback acknowledgement is absent"
                    .to_owned(),
            },
        }
    }

    fn compatible_with(&self, other: &Self) -> bool {
        self.identity == other.identity && self.writes == other.writes
    }
}

/// Commit or abort decision sent by the coordinator to one participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantDecision {
    /// Publish the prepared write set only after the caller's durable decision.
    Commit,
    /// Discard the prepared write set without exposing it.
    Abort,
}

/// Observable result of preparing or applying one participant decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParticipantDecisionResult {
    /// Prepare is durably recorded and waits for a terminal decision.
    Prepared,
    /// The local write set is durably committed exactly once.
    Committed,
    /// The local write set is discarded and has no committed visibility.
    Aborted,
    /// A WAL fsync or terminal apply/acknowledgement is uncertain.
    RecoveryPending {
        /// Durable decision if one was recorded before uncertainty.
        decision: Option<ParticipantDecision>,
        /// Stable diagnostic detail for recovery tooling.
        reason: String,
    },
}

/// Lifecycle of an in-process participant transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantTransactionState {
    /// Reads and writes may be staged under the fixed read point.
    Active,
    /// A durable prepare record exists and only a terminal decision is allowed.
    Prepared,
    /// Commit was durably applied.
    Committed,
    /// Abort was durably applied.
    Aborted,
    /// The decision or its acknowledgement is uncertain and cannot be retried in place.
    RecoveryPending,
}

/// Result of opening a participant request at a fixed read point.
pub enum ParticipantOpenResult {
    /// A new participant owns the fenced MVCC session.
    Open(ParticipantTransaction),
    /// A duplicate request returns its stored result without opening or applying again.
    Idempotent(ParticipantDecisionResult),
}

impl fmt::Debug for ParticipantOpenResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open(transaction) => formatter.debug_tuple("Open").field(transaction).finish(),
            Self::Idempotent(result) => formatter.debug_tuple("Idempotent").field(result).finish(),
        }
    }
}

/// Stable failure returned by the participant boundary.
#[derive(Debug, thiserror::Error)]
pub enum ParticipantTransactionError {
    /// A required stable identity field was empty.
    #[error("participant transaction identity fields must be non-empty")]
    InvalidIdentity,
    /// The backend did not prove the supplied cluster read point.
    #[error(transparent)]
    ReadPoint(#[from] ReadAtError),
    /// Existing core storage or MVCC operation failed.
    #[error(transparent)]
    Storage(#[from] Error),
    /// A request ID was reused for a different participant payload.
    #[error("participant request id conflicts with a different identity or write set")]
    IdempotencyConflict,
    /// The requested operation is illegal for the current one-way lifecycle.
    #[error("participant transaction is not active for {operation}")]
    InvalidState {
        /// Operation rejected before it could affect storage.
        operation: &'static str,
    },
    /// A conflicting terminal decision followed an already durable result.
    #[error("participant already has an incompatible terminal decision")]
    DecisionConflict,
}

/// Storage abstraction for durable participant decision records.
///
/// Implementations must make [`Self::append`] durable before returning `Ok`.
/// The transaction boundary treats an append error as uncertain and never
/// reports a successful terminal result from it.
pub trait ParticipantDecisionStore: Send + Sync {
    /// Look up the last durable record for one request identifier.
    fn lookup(&self, request_id: &str) -> Result<Option<ParticipantDecisionRecord>>;

    /// Append and fsync the supplied next durable participant record.
    fn append(&self, record: ParticipantDecisionRecord) -> Result<()>;
}

/// File-backed participant store using the core framed WAL writer.
#[derive(Clone)]
pub struct WalParticipantDecisionStore {
    inner: Arc<Mutex<WalParticipantDecisionStoreInner>>,
}

struct WalParticipantDecisionStoreInner {
    writer: WalWriter,
    records: BTreeMap<String, ParticipantDecisionRecord>,
}

impl WalParticipantDecisionStore {
    /// Open or create a dedicated participant journal and restore its valid WAL prefix.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut records = BTreeMap::new();
        if path.exists() {
            let reader = WalReader::new(path)?;
            for item in reader {
                let WalRecord::ParticipantState {
                    transaction_id,
                    request_id,
                    request_fingerprint,
                    data_epoch,
                    metadata_version,
                    schema_epoch,
                    index_epoch,
                    writes,
                    state,
                } = item?
                else {
                    continue;
                };
                let record = ParticipantDecisionRecord {
                    identity: ParticipantIdentity {
                        transaction_id,
                        request_id: request_id.clone(),
                        request_fingerprint,
                        read_point: ReadAtPoint::new(
                            data_epoch,
                            metadata_version,
                            schema_epoch,
                            index_epoch,
                        ),
                    },
                    writes: writes
                        .into_iter()
                        .map(|write| ParticipantWrite {
                            key: write.key,
                            value: write.value,
                        })
                        .collect(),
                    phase: ParticipantJournalPhase::from_wal(state),
                };
                verify_next_record(records.get(&request_id), &record)?;
                records.insert(request_id, record);
            }
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(WalParticipantDecisionStoreInner {
                writer: WalWriter::new(path)?,
                records,
            })),
        })
    }
}

impl ParticipantDecisionStore for WalParticipantDecisionStore {
    fn lookup(&self, request_id: &str) -> Result<Option<ParticipantDecisionRecord>> {
        Ok(self
            .inner
            .lock()
            .expect("participant decision store mutex poisoned")
            .records
            .get(request_id)
            .cloned())
    }

    fn append(&self, record: ParticipantDecisionRecord) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .expect("participant decision store mutex poisoned");
        let request_id = record.identity.request_id.clone();
        verify_next_record(inner.records.get(&request_id), &record)?;
        if inner.records.get(&request_id) == Some(&record) {
            return Ok(());
        }
        inner.writer.append(&WalRecord::ParticipantState {
            transaction_id: record.identity.transaction_id.clone(),
            request_id: request_id.clone(),
            request_fingerprint: record.identity.request_fingerprint.clone(),
            data_epoch: record.identity.read_point.data_epoch,
            metadata_version: record.identity.read_point.metadata_version,
            schema_epoch: record.identity.read_point.schema_epoch,
            index_epoch: record.identity.read_point.index_epoch,
            writes: record
                .writes
                .iter()
                .map(|write| ParticipantWalWrite {
                    key: write.key.clone(),
                    value: write.value.clone(),
                })
                .collect(),
            state: record.phase.wal_state(),
        })?;
        // A participant journal append is usable only after this fsync. If it
        // fails, callers classify the outcome as recovery pending.
        inner.writer.sync()?;
        inner.records.insert(request_id, record);
        Ok(())
    }
}

fn verify_next_record(
    previous: Option<&ParticipantDecisionRecord>,
    next: &ParticipantDecisionRecord,
) -> Result<()> {
    let Some(previous) = previous else {
        if next.phase != ParticipantJournalPhase::Prepared {
            return Err(Error::InvalidFormat(
                "participant journal must begin with prepared".to_owned(),
            ));
        }
        return Ok(());
    };
    if !previous.compatible_with(next) {
        return Err(Error::InvalidParameter {
            param: "participant_request_id".to_owned(),
            reason: "request ID cannot be reused with a different identity or write set".to_owned(),
        });
    }
    if !previous.phase.allows_next(next.phase) {
        return Err(Error::InvalidFormat(
            "participant journal transition is not one-way".to_owned(),
        ));
    }
    Ok(())
}

/// A single MVCC participant tied to an immutable read point and decision journal.
pub struct ParticipantTransaction {
    store: Arc<dyn ParticipantDecisionStore>,
    inner: Mutex<ParticipantTransactionInner>,
}

struct ParticipantTransactionInner {
    identity: ParticipantIdentity,
    session: OwnedTransactionSession,
    writes: BTreeMap<Key, Option<Value>>,
    state: ParticipantTransactionInnerState,
}

#[derive(Debug, Clone)]
enum ParticipantTransactionInnerState {
    Active,
    Prepared,
    Committed,
    Aborted,
    RecoveryPending {
        decision: Option<ParticipantDecision>,
        reason: String,
    },
}

impl ParticipantTransaction {
    /// Open a participant only through an owned store that proves `identity`'s read point.
    pub fn begin<S>(
        store: Arc<S>,
        identity: ParticipantIdentity,
        decision_store: Arc<dyn ParticipantDecisionStore>,
    ) -> std::result::Result<ParticipantOpenResult, ParticipantTransactionError>
    where
        S: OwnedSessionFactory,
    {
        if let Some(existing) = decision_store.lookup(&identity.request_id)? {
            if existing.identity != identity {
                return Err(ParticipantTransactionError::IdempotencyConflict);
            }
            return Ok(ParticipantOpenResult::Idempotent(existing.outcome()));
        }
        let session = store
            .begin_owned_transaction_at(&identity.read_point, TxnMode::ReadWrite)
            .map_err(ParticipantTransactionError::ReadPoint)?;
        Ok(ParticipantOpenResult::Open(Self {
            store: decision_store,
            inner: Mutex::new(ParticipantTransactionInner {
                identity,
                session,
                writes: BTreeMap::new(),
                state: ParticipantTransactionInnerState::Active,
            }),
        }))
    }

    /// Return a copy of the immutable participant identity and read fence.
    pub fn identity(&self) -> ParticipantIdentity {
        self.lock().identity.clone()
    }

    /// Return the observable one-way participant state.
    pub fn state(&self) -> ParticipantTransactionState {
        match &self.lock().state {
            ParticipantTransactionInnerState::Active => ParticipantTransactionState::Active,
            ParticipantTransactionInnerState::Prepared => ParticipantTransactionState::Prepared,
            ParticipantTransactionInnerState::Committed => ParticipantTransactionState::Committed,
            ParticipantTransactionInnerState::Aborted => ParticipantTransactionState::Aborted,
            ParticipantTransactionInnerState::RecoveryPending { .. } => {
                ParticipantTransactionState::RecoveryPending
            }
        }
    }

    /// Read from the staged write set first, then from the fixed owned snapshot.
    pub fn get(
        &self,
        key: &Key,
    ) -> std::result::Result<Option<Value>, ParticipantTransactionError> {
        let inner = self.lock();
        ensure_active(&inner, "get")?;
        if let Some(value) = inner.writes.get(key) {
            return Ok(value.clone());
        }
        inner
            .session
            .with_transaction(|transaction| transaction.get(key))
            .map_err(ParticipantTransactionError::Storage)
    }

    /// Stage one value for the later durable prepare record and terminal decision.
    pub fn put(
        &self,
        key: Key,
        value: Value,
    ) -> std::result::Result<(), ParticipantTransactionError> {
        let mut inner = self.lock();
        ensure_active(&inner, "put")?;
        inner
            .session
            .with_transaction(|transaction| transaction.put(key.clone(), value.clone()))
            .map_err(ParticipantTransactionError::Storage)?;
        inner.writes.insert(key, Some(value));
        Ok(())
    }

    /// Stage one deletion for the later durable prepare record and terminal decision.
    pub fn delete(&self, key: Key) -> std::result::Result<(), ParticipantTransactionError> {
        let mut inner = self.lock();
        ensure_active(&inner, "delete")?;
        inner
            .session
            .with_transaction(|transaction| transaction.delete(key.clone()))
            .map_err(ParticipantTransactionError::Storage)?;
        inner.writes.insert(key, None);
        Ok(())
    }

    /// Durably record the full write set before accepting a terminal decision.
    pub fn prepare(
        &self,
    ) -> std::result::Result<ParticipantDecisionResult, ParticipantTransactionError> {
        let mut inner = self.lock();
        match inner.state {
            ParticipantTransactionInnerState::Prepared => {
                return Ok(ParticipantDecisionResult::Prepared)
            }
            ParticipantTransactionInnerState::RecoveryPending { .. } => {
                return Ok(recovery_result(&inner.state));
            }
            _ => ensure_active(&inner, "prepare")?,
        }
        let record = current_record(&inner, ParticipantJournalPhase::Prepared);
        if let Err(error) = self.store.append(record) {
            inner.state = ParticipantTransactionInnerState::RecoveryPending {
                decision: None,
                reason: format!("prepare WAL sync failed: {error}"),
            };
            return Ok(recovery_result(&inner.state));
        }
        inner.state = ParticipantTransactionInnerState::Prepared;
        Ok(ParticipantDecisionResult::Prepared)
    }

    /// Apply a commit or abort decision once; any uncertain durability is recovery pending.
    pub fn decide(
        &self,
        decision: ParticipantDecision,
    ) -> std::result::Result<ParticipantDecisionResult, ParticipantTransactionError> {
        let mut inner = self.lock();
        match inner.state {
            ParticipantTransactionInnerState::Committed => {
                return match decision {
                    ParticipantDecision::Commit => Ok(ParticipantDecisionResult::Committed),
                    ParticipantDecision::Abort => {
                        Err(ParticipantTransactionError::DecisionConflict)
                    }
                };
            }
            ParticipantTransactionInnerState::Aborted => {
                return match decision {
                    ParticipantDecision::Abort => Ok(ParticipantDecisionResult::Aborted),
                    ParticipantDecision::Commit => {
                        Err(ParticipantTransactionError::DecisionConflict)
                    }
                };
            }
            ParticipantTransactionInnerState::RecoveryPending { .. } => {
                return Ok(recovery_result(&inner.state));
            }
            ParticipantTransactionInnerState::Active => {
                return Err(ParticipantTransactionError::InvalidState {
                    operation: "decide before prepare",
                });
            }
            ParticipantTransactionInnerState::Prepared => {}
        }

        let decision_phase = match decision {
            ParticipantDecision::Commit => ParticipantJournalPhase::CommitDecision,
            ParticipantDecision::Abort => ParticipantJournalPhase::AbortDecision,
        };
        if let Err(error) = self.store.append(current_record(&inner, decision_phase)) {
            inner.state = ParticipantTransactionInnerState::RecoveryPending {
                decision: Some(decision),
                reason: format!("decision WAL sync failed: {error}"),
            };
            return Ok(recovery_result(&inner.state));
        }

        let terminal_apply = match decision {
            ParticipantDecision::Commit => inner.session.commit(),
            ParticipantDecision::Abort => inner.session.rollback(),
        };
        if let Err(error) = terminal_apply {
            inner.state = ParticipantTransactionInnerState::RecoveryPending {
                decision: Some(decision),
                reason: format!("local terminal apply failed after durable decision: {error}"),
            };
            return Ok(recovery_result(&inner.state));
        }

        let applied_phase = match decision {
            ParticipantDecision::Commit => ParticipantJournalPhase::CommitApplied,
            ParticipantDecision::Abort => ParticipantJournalPhase::AbortApplied,
        };
        if let Err(error) = self.store.append(current_record(&inner, applied_phase)) {
            inner.state = ParticipantTransactionInnerState::RecoveryPending {
                decision: Some(decision),
                reason: format!("terminal acknowledgement WAL sync failed: {error}"),
            };
            return Ok(recovery_result(&inner.state));
        }
        inner.state = match decision {
            ParticipantDecision::Commit => ParticipantTransactionInnerState::Committed,
            ParticipantDecision::Abort => ParticipantTransactionInnerState::Aborted,
        };
        Ok(match decision {
            ParticipantDecision::Commit => ParticipantDecisionResult::Committed,
            ParticipantDecision::Abort => ParticipantDecisionResult::Aborted,
        })
    }

    fn lock(&self) -> MutexGuard<'_, ParticipantTransactionInner> {
        self.inner
            .lock()
            .expect("participant transaction mutex poisoned")
    }
}

impl fmt::Debug for ParticipantTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParticipantTransaction")
            .field("identity", &self.identity())
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

fn ensure_active(
    inner: &ParticipantTransactionInner,
    operation: &'static str,
) -> std::result::Result<(), ParticipantTransactionError> {
    if matches!(inner.state, ParticipantTransactionInnerState::Active) {
        Ok(())
    } else {
        Err(ParticipantTransactionError::InvalidState { operation })
    }
}

fn current_record(
    inner: &ParticipantTransactionInner,
    phase: ParticipantJournalPhase,
) -> ParticipantDecisionRecord {
    ParticipantDecisionRecord {
        identity: inner.identity.clone(),
        writes: inner
            .writes
            .iter()
            .map(|(key, value)| ParticipantWrite {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        phase,
    }
}

fn recovery_result(state: &ParticipantTransactionInnerState) -> ParticipantDecisionResult {
    match state {
        ParticipantTransactionInnerState::RecoveryPending { decision, reason } => {
            ParticipantDecisionResult::RecoveryPending {
                decision: *decision,
                reason: reason.clone(),
            }
        }
        _ => unreachable!("recovery result is requested only for recovery-pending state"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use super::{
        ParticipantDecision, ParticipantDecisionRecord, ParticipantDecisionResult,
        ParticipantDecisionStore, ParticipantIdentity, ParticipantOpenResult,
        ParticipantTransaction, ParticipantTransactionState, WalParticipantDecisionStore,
    };
    use crate::error::{Error, Result};
    use crate::kv::{
        OwnedKVScan, OwnedKVStore, OwnedKVTransaction, ReadAtCapability, ReadAtPoint, ReadAtResult,
    };
    use crate::types::{Key, TxnId, TxnMode, Value};

    #[derive(Default)]
    struct TestStore {
        data: Arc<Mutex<BTreeMap<Key, Value>>>,
        commits: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
    }

    struct TestTransaction {
        data: Arc<Mutex<BTreeMap<Key, Value>>>,
        writes: BTreeMap<Key, Option<Value>>,
        commits: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
    }

    struct EmptyScan;

    impl OwnedKVScan for EmptyScan {
        fn next_entry(&mut self) -> Result<Option<(Key, Value)>> {
            Ok(None)
        }
    }

    impl OwnedKVTransaction for TestTransaction {
        fn id(&self) -> TxnId {
            TxnId(1)
        }

        fn mode(&self) -> TxnMode {
            TxnMode::ReadWrite
        }

        fn get(&mut self, key: &Key) -> Result<Option<Value>> {
            Ok(self.writes.get(key).cloned().unwrap_or_else(|| {
                self.data
                    .lock()
                    .expect("data mutex poisoned")
                    .get(key)
                    .cloned()
            }))
        }

        fn put(&mut self, key: Key, value: Value) -> Result<()> {
            self.writes.insert(key, Some(value));
            Ok(())
        }

        fn delete(&mut self, key: Key) -> Result<()> {
            self.writes.insert(key, None);
            Ok(())
        }

        fn scan_prefix(&mut self, _prefix: &[u8]) -> Result<Box<dyn OwnedKVScan>> {
            Ok(Box::new(EmptyScan))
        }

        fn scan_range(&mut self, _start: &[u8], _end: &[u8]) -> Result<Box<dyn OwnedKVScan>> {
            Ok(Box::new(EmptyScan))
        }

        fn commit(self: Box<Self>) -> Result<()> {
            let mut data = self.data.lock().expect("data mutex poisoned");
            for (key, value) in self.writes {
                if let Some(value) = value {
                    data.insert(key, value);
                } else {
                    data.remove(&key);
                }
            }
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn rollback(self: Box<Self>) -> Result<()> {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl OwnedKVStore for TestStore {
        fn begin_owned_kv_transaction(
            self: Arc<Self>,
            _mode: TxnMode,
        ) -> Result<Box<dyn OwnedKVTransaction>> {
            Ok(Box::new(TestTransaction {
                data: self.data.clone(),
                writes: BTreeMap::new(),
                commits: self.commits.clone(),
                rollbacks: self.rollbacks.clone(),
            }))
        }

        fn owned_read_at_capability(&self) -> ReadAtCapability {
            ReadAtCapability::Available {
                readable_from_epoch: 7,
                readable_through_epoch: 7,
            }
        }

        fn begin_owned_kv_transaction_at(
            self: Arc<Self>,
            point: &ReadAtPoint,
            mode: TxnMode,
        ) -> ReadAtResult<Box<dyn OwnedKVTransaction>> {
            self.owned_read_at_capability().validate(point)?;
            self.begin_owned_kv_transaction(mode).map_err(|error| {
                crate::kv::ReadAtError::Unavailable {
                    requested_epoch: point.data_epoch,
                    reason: error.to_string(),
                }
            })
        }
    }

    fn identity(request_id: &str) -> ParticipantIdentity {
        ParticipantIdentity::new(
            "transaction-a",
            request_id,
            "authenticated-payload-digest",
            ReadAtPoint::new(7, 8, 9, 10),
        )
        .unwrap()
    }

    fn open(
        store: Arc<TestStore>,
        journal: Arc<dyn ParticipantDecisionStore>,
        request_id: &str,
    ) -> ParticipantTransaction {
        match ParticipantTransaction::begin(store, identity(request_id), journal).unwrap() {
            ParticipantOpenResult::Open(transaction) => transaction,
            ParticipantOpenResult::Idempotent(result) => {
                panic!("unexpected duplicate result: {result:?}")
            }
        }
    }

    #[test]
    fn fixed_read_point_write_set_and_abort_never_expose_writes() {
        let directory = tempdir().unwrap();
        let journal = Arc::new(
            WalParticipantDecisionStore::open(directory.path().join("participant.wal")).unwrap(),
        );
        let store = Arc::new(TestStore::default());
        let transaction = open(store.clone(), journal, "request-abort");
        transaction
            .put(b"row".to_vec(), b"uncommitted".to_vec())
            .unwrap();
        assert_eq!(
            transaction.get(&b"row".to_vec()).unwrap(),
            Some(b"uncommitted".to_vec())
        );
        assert_eq!(store.data.lock().unwrap().get(b"row".as_slice()), None);
        assert_eq!(
            transaction.prepare().unwrap(),
            ParticipantDecisionResult::Prepared
        );
        assert_eq!(
            transaction.decide(ParticipantDecision::Abort).unwrap(),
            ParticipantDecisionResult::Aborted
        );
        assert_eq!(store.data.lock().unwrap().get(b"row".as_slice()), None);
        assert_eq!(store.rollbacks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn commit_is_once_and_reopen_returns_the_durable_idempotent_result() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("participant.wal");
        let journal = Arc::new(WalParticipantDecisionStore::open(&path).unwrap());
        let store = Arc::new(TestStore::default());
        let transaction = open(store.clone(), journal.clone(), "request-commit");
        transaction
            .put(b"row".to_vec(), b"committed".to_vec())
            .unwrap();
        transaction.prepare().unwrap();
        assert_eq!(
            transaction.decide(ParticipantDecision::Commit).unwrap(),
            ParticipantDecisionResult::Committed
        );
        assert_eq!(
            transaction.decide(ParticipantDecision::Commit).unwrap(),
            ParticipantDecisionResult::Committed
        );
        assert_eq!(store.commits.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.data.lock().unwrap().get(b"row".as_slice()),
            Some(&b"committed".to_vec())
        );

        let recovered = Arc::new(WalParticipantDecisionStore::open(&path).unwrap());
        assert!(matches!(
            ParticipantTransaction::begin(store, identity("request-commit"), recovered).unwrap(),
            ParticipantOpenResult::Idempotent(ParticipantDecisionResult::Committed)
        ));
    }

    struct FailingDecisionStore;

    impl ParticipantDecisionStore for FailingDecisionStore {
        fn lookup(&self, _request_id: &str) -> Result<Option<ParticipantDecisionRecord>> {
            Ok(None)
        }

        fn append(&self, _record: ParticipantDecisionRecord) -> Result<()> {
            Err(Error::Io(std::io::Error::other(
                "injected WAL fsync failure",
            )))
        }
    }

    #[test]
    fn wal_fsync_failure_is_recovery_pending_not_success() {
        let store = Arc::new(TestStore::default());
        let transaction = open(
            store,
            Arc::new(FailingDecisionStore),
            "request-fsync-failure",
        );
        transaction.put(b"row".to_vec(), b"value".to_vec()).unwrap();
        assert!(matches!(
            transaction.prepare().unwrap(),
            ParticipantDecisionResult::RecoveryPending { decision: None, .. }
        ));
        assert_eq!(
            transaction.state(),
            ParticipantTransactionState::RecoveryPending
        );
    }
}
