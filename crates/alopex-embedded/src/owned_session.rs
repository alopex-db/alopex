//! Owned embedded-session factories for long-lived local consumers.
//!
//! The existing [`crate::Database::begin`] API remains a borrowed Rust facade. This module is the
//! separate boundary used by Python and asynchronous stream work: it clones the database's
//! storage `Arc` and returns only core-owned session state.

use std::collections::HashMap;
use std::sync::Arc;

use alopex_cluster::crdt::{CrdtOperationEnvelope, CrdtOutcome};
use alopex_cluster::{
    FailureClass, IdempotencyResult, OperationState, RoutingOutcome, RoutingOutcomeKind,
};
use alopex_core::kv::{
    AnyKV, OwnedKVTransactionAdapter, OwnedReadOptions, OwnedReadSession,
    OwnedSessionFactory as CoreOwnedSessionFactory, OwnedTransactionSession,
};
use alopex_core::vector::hnsw::{HnswIndex, HnswTransactionState};
use alopex_core::TxnMode;
use alopex_sql::catalog::CatalogOverlay;
use alopex_sql::storage::LocalRangeChangeJournal;

use crate::{Database, Error, Result};

/// Builds the explicit async capability response for a Counter create.
///
/// An owned embedded session is a non-borrowing local lifecycle boundary; it
/// is not an async CRDT executor. Returning this result prevents a hidden
/// synchronous fallback from writing the CRDT ledger through an async-named
/// API before such an executor is implemented and approved.
pub(crate) fn unsupported_async_counter_create(envelope: CrdtOperationEnvelope) -> CrdtOutcome {
    let reason = "embedded_async_crdt_unavailable";
    let common = envelope.common_fields(
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(envelope.range.clone()),
            envelope.state_epoch,
            reason,
        ),
        false,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.to_owned(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::unsupported(common, reason)
}

/// Builds the explicit async capability response for a Counter read.
///
/// Read operations must not silently call the synchronous embedded projection
/// from an async-named API. Until an owned async CRDT executor exists, the
/// capability is explicitly unsupported and leaves the durable projection
/// untouched.
pub(crate) fn unsupported_async_counter_read(envelope: CrdtOperationEnvelope) -> CrdtOutcome {
    let reason = "embedded_async_crdt_unavailable";
    let common = envelope.common_fields(
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(envelope.range.clone()),
            envelope.state_epoch,
            reason,
        ),
        false,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.to_owned(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::unsupported(common, reason)
}

/// Builds the explicit async capability response for a Counter increment.
///
/// The owned-session boundary does not provide an async CRDT executor. In
/// particular, this must not invoke the synchronous increment path because
/// that would mutate the durable operation ledger behind an async-named API.
pub(crate) fn unsupported_async_counter_increment(envelope: CrdtOperationEnvelope) -> CrdtOutcome {
    let reason = "embedded_async_crdt_unavailable";
    let common = envelope.common_fields(
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(envelope.range.clone()),
            envelope.state_epoch,
            reason,
        ),
        false,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.to_owned(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::unsupported(common, reason)
}

/// Builds the explicit async capability response for a Counter decrement.
///
/// The owned-session boundary does not provide an async CRDT executor. In
/// particular, this must not invoke the synchronous decrement path because
/// that would mutate the durable operation ledger behind an async-named API.
pub(crate) fn unsupported_async_counter_decrement(envelope: CrdtOperationEnvelope) -> CrdtOutcome {
    let reason = "embedded_async_crdt_unavailable";
    let common = envelope.common_fields(
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(envelope.range.clone()),
            envelope.state_epoch,
            reason,
        ),
        false,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.to_owned(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::unsupported(common, reason)
}

/// Builds the explicit async capability response for a Set create.
///
/// The owned-session boundary has no native async CRDT executor. In
/// particular, this must not invoke the synchronous Set-create path because
/// that would mutate the durable operation ledger behind an async-named API.
pub(crate) fn unsupported_async_set_create(envelope: CrdtOperationEnvelope) -> CrdtOutcome {
    let reason = "embedded_async_crdt_unavailable";
    let common = envelope.common_fields(
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(envelope.range.clone()),
            envelope.state_epoch,
            reason,
        ),
        false,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.to_owned(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::unsupported(common, reason)
}

/// Builds the explicit async capability response for a Set add.
///
/// The owned-session boundary has no native async CRDT executor. In
/// particular, this must not invoke the synchronous Set-add path because that
/// would mutate the durable operation ledger behind an async-named API.
pub(crate) fn unsupported_async_set_add(envelope: CrdtOperationEnvelope) -> CrdtOutcome {
    let reason = "embedded_async_crdt_unavailable";
    let common = envelope.common_fields(
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(envelope.range.clone()),
            envelope.state_epoch,
            reason,
        ),
        false,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.to_owned(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::unsupported(common, reason)
}

/// Builds the explicit async capability response for a Set read.
///
/// Read operations must not silently call the synchronous embedded projection
/// from an async-named API. Until an owned async CRDT executor exists, the
/// capability is explicitly unsupported and leaves the durable projection
/// untouched.
pub(crate) fn unsupported_async_set_read(envelope: CrdtOperationEnvelope) -> CrdtOutcome {
    let reason = "embedded_async_crdt_unavailable";
    let common = envelope.common_fields(
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            Some(envelope.range.clone()),
            envelope.state_epoch,
            reason,
        ),
        false,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.to_owned(),
            state: OperationState::Rejected,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::unsupported(common, reason)
}

/// Factory for owned local read and transaction sessions from one embedded database.
///
/// The factory owns the same `Arc<AnyKV>` as its source [`Database`]. Consequently, a session
/// and every cursor it opens keep the backend alive without borrowing the database or promoting
/// a legacy [`crate::Transaction`] lifetime.
#[derive(Clone)]
pub struct EmbeddedOwnedSessionFactory {
    store: Arc<AnyKV>,
}

impl EmbeddedOwnedSessionFactory {
    pub(crate) fn new(store: Arc<AnyKV>) -> Self {
        Self { store }
    }

    /// Begin one owned read-only session.
    pub fn begin_read(&self, options: OwnedReadOptions) -> Result<OwnedReadSession> {
        self.store
            .clone()
            .begin_owned_read(options)
            .map_err(Error::Core)
    }

    /// Begin one owned transaction session.
    ///
    /// A transaction's active lease, terminal effect, and exactly-once commit/rollback are
    /// enforced by the returned core session. This factory never performs an implicit commit.
    pub fn begin_transaction(&self, mode: TxnMode) -> Result<OwnedTransactionSession> {
        self.store
            .clone()
            .begin_owned_transaction(mode)
            .map_err(Error::Core)
    }
}

impl Database {
    /// Return an owned-session factory bound to this embedded-local database.
    pub fn owned_session_factory(&self) -> EmbeddedOwnedSessionFactory {
        EmbeddedOwnedSessionFactory::new(self.store.clone())
    }

    /// Begin an owned read-only session without changing the borrowed [`Self::begin`] API.
    pub fn begin_owned_read(&self, options: OwnedReadOptions) -> Result<OwnedReadSession> {
        self.owned_session_factory().begin_read(options)
    }

    /// Begin an owned transaction session without changing the borrowed [`Self::begin`] API.
    pub fn begin_owned_transaction(&self, mode: TxnMode) -> Result<OwnedTransactionSession> {
        self.owned_session_factory().begin_transaction(mode)
    }

    /// Begin an owned embedded transaction from an `Arc` database handle.
    ///
    /// This is the safe replacement for foreign bindings that formerly extended the lifetime of
    /// [`crate::Transaction`].  The database `Arc`, core-owned transaction, catalog overlay, and
    /// commit bookkeeping remain together until one explicit terminal transition.
    pub fn begin_owned_embedded_transaction(
        self: Arc<Self>,
        mode: TxnMode,
    ) -> Result<OwnedEmbeddedTransaction> {
        let session = self.begin_owned_transaction(mode)?;
        let journal = if mode == TxnMode::ReadWrite
            && self.store.range_change_journal_capability()
                == alopex_core::kv::RangeChangeJournalCapability::Supported
        {
            let scope = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                crate::sql_api::local_journal_scope(&*catalog)
            };
            Some(
                session
                    .with_transaction(|transaction| {
                        let mut transaction = OwnedKVTransactionAdapter::new(transaction);
                        LocalRangeChangeJournal::capture(&mut transaction, scope)
                    })
                    .map_err(Error::Core)?,
            )
        } else {
            None
        };
        Ok(OwnedEmbeddedTransaction {
            db: self,
            session,
            overlay: CatalogOverlay::new(),
            catalog_modified: false,
            journal,
            hnsw_indices: HashMap::new(),
            vector_cache_invalidated: false,
        })
    }
}

/// An embedded-local transaction that owns all state required by a Python or async handle.
///
/// The type deliberately has no borrowed lifetime.  Finite compatibility operations borrow the
/// owned KV transaction only for their duration; public stream leases clone `session` and retain
/// ownership through the core state machine.
pub struct OwnedEmbeddedTransaction {
    pub(crate) db: Arc<Database>,
    pub(crate) session: OwnedTransactionSession,
    pub(crate) overlay: CatalogOverlay,
    pub(crate) catalog_modified: bool,
    pub(crate) journal: Option<LocalRangeChangeJournal>,
    pub(crate) hnsw_indices: HashMap<String, (HnswIndex, HnswTransactionState)>,
    pub(crate) vector_cache_invalidated: bool,
}

impl OwnedEmbeddedTransaction {
    /// Return the core session shared with a transaction-owned stream lease.
    pub fn session(&self) -> OwnedTransactionSession {
        self.session.clone()
    }

    /// Read one key inside this owned transaction.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.session
            .with_transaction(|transaction| transaction.get(&key.to_vec()))
            .map_err(Error::Core)
    }

    /// Stage one key/value pair inside this owned transaction.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.vector_cache_invalidated = true;
        self.session
            .with_transaction(|transaction| transaction.put(key.to_vec(), value.to_vec()))
            .map_err(Error::Core)
    }

    /// Stage one deletion inside this owned transaction.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.vector_cache_invalidated = true;
        self.session
            .with_transaction(|transaction| transaction.delete(key.to_vec()))
            .map_err(Error::Core)
    }

    /// Execute SQL without committing this transaction.
    ///
    /// The implementation is in `sql_api.rs` so it uses the same planner, catalog overlay, and
    /// error mapping as the borrowed compatibility transaction.
    pub fn execute_sql(&mut self, sql: &str) -> Result<crate::SqlResult> {
        crate::sql_api::execute_sql_owned(self, sql)
    }

    /// Preflight a streamable local SELECT against this transaction's catalog overlay.
    ///
    /// The plan copies its required catalog metadata before returning, so the resulting stream
    /// lease retains no borrow of this transaction or its overlay.
    pub fn preflight_sql_stream(&self, sql: &str) -> Result<crate::OwnedSqlStreamPlan> {
        crate::OwnedSqlStreamPlan::preflight_in_transaction(&self.db, &self.overlay, sql)
    }

    /// Commit the owned transaction after staging catalog and range-change metadata.
    pub fn commit(&mut self) -> Result<()> {
        let mut preparation = Ok(());
        let journal = self.journal.take();
        self.session
            .with_transaction(|transaction| {
                let mut transaction = alopex_core::kv::any::AnyKVTransaction::Owned(
                    OwnedKVTransactionAdapter::new(transaction),
                );
                for (index, state) in self.hnsw_indices.values_mut() {
                    if preparation.is_ok() {
                        preparation = index
                            .commit_staged(&mut transaction, state)
                            .map_err(Error::Core);
                    }
                }
                let mut catalog = self.db.sql_catalog.write().expect("catalog lock poisoned");
                if preparation.is_ok() {
                    preparation = catalog
                        .persist_overlay(&mut transaction, &self.overlay)
                        .map_err(|error| Error::Sql(error.into()));
                }
                if preparation.is_ok() {
                    if let Some(journal) = journal {
                        preparation = journal
                            .stage(&mut transaction)
                            .map(|_| ())
                            .map_err(Error::Core);
                    }
                }
                Ok(())
            })
            .map_err(Error::Core)?;
        preparation?;

        self.session.commit().map_err(Error::Core)?;
        let overlay = std::mem::take(&mut self.overlay);
        let mut catalog = self.db.sql_catalog.write().expect("catalog lock poisoned");
        catalog.apply_overlay(overlay);
        drop(catalog);
        if self.catalog_modified {
            self.db.invalidate_table_info_cache();
        }
        if self.vector_cache_invalidated {
            let mut cache = self
                .db
                .vector_cache
                .write()
                .expect("vector cache lock poisoned");
            *cache = None;
        }
        if !self.hnsw_indices.is_empty() {
            let hnsw_indices = std::mem::take(&mut self.hnsw_indices);
            let mut cache = self
                .db
                .hnsw_cache
                .write()
                .expect("hnsw cache lock poisoned");
            for (name, (index, _)) in hnsw_indices {
                cache.insert(name, Arc::new(index));
            }
        }
        Ok(())
    }

    /// Roll back the owned transaction once.
    pub fn rollback(&mut self) -> Result<()> {
        self.session.rollback().map_err(Error::Core)?;
        for (index, state) in self.hnsw_indices.values_mut() {
            let _ = index.rollback(state);
        }
        self.hnsw_indices.clear();
        self.overlay = CatalogOverlay::default();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::kv::OwnedReadOptions;
    use alopex_core::txn::OwnedLeaseOutcome;
    use alopex_core::TxnMode;
    use std::sync::Arc;

    #[test]
    fn embedded_factory_uses_owned_lifecycle_without_changing_borrowed_transactions() {
        let database = Database::new();
        let session = database
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();
        let lease = session.acquire_lease().unwrap();
        lease
            .with_transaction(|transaction| transaction.put(b"owned".to_vec(), b"value".to_vec()))
            .unwrap();
        assert!(session.commit().is_err());
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
        session.commit().unwrap();

        let mut borrowed = database.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(
            borrowed.get(b"owned".as_ref()).unwrap(),
            Some(b"value".to_vec())
        );
        borrowed.commit().unwrap();

        let read = database
            .owned_session_factory()
            .begin_read(OwnedReadOptions::default())
            .unwrap();
        let lease = read.acquire_lease().unwrap();
        let mut cursor = lease
            .with_transaction(|transaction| transaction.scan_prefix(b"own"))
            .unwrap();
        assert_eq!(
            cursor.next_entry().unwrap(),
            Some((b"owned".to_vec(), b"value".to_vec()))
        );
        assert_eq!(cursor.next_entry().unwrap(), None);
        cursor.close().unwrap();
        drop(cursor);
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
    }

    #[test]
    fn dropped_embedded_owned_transaction_rolls_back_staged_writes() {
        let database = Database::new();
        let session = database
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();
        let lease = session.acquire_lease().unwrap();
        lease
            .with_transaction(|transaction| transaction.put(b"discard".to_vec(), b"value".to_vec()))
            .unwrap();
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
        drop(session);

        let mut borrowed = database.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(borrowed.get(b"discard".as_ref()).unwrap(), None);
        borrowed.commit().unwrap();
    }

    #[test]
    fn owned_embedded_transaction_preserves_sql_visibility_until_explicit_commit() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE owned_sql (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();

        let mut transaction = Arc::clone(&database)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        transaction
            .execute_sql("INSERT INTO owned_sql (id, value) VALUES (1, 'staged')")
            .unwrap();
        let alopex_sql::ExecutionResult::Query(query) = transaction
            .execute_sql("SELECT value FROM owned_sql WHERE id = 1")
            .unwrap()
        else {
            panic!("owned transaction select must return a query")
        };
        assert_eq!(query.rows.len(), 1);

        let alopex_sql::ExecutionResult::Query(before_commit) = database
            .execute_sql("SELECT value FROM owned_sql WHERE id = 1")
            .unwrap()
        else {
            panic!("database select must return a query")
        };
        assert!(before_commit.rows.is_empty());

        transaction.commit().unwrap();
        let alopex_sql::ExecutionResult::Query(after_commit) = database
            .execute_sql("SELECT value FROM owned_sql WHERE id = 1")
            .unwrap()
        else {
            panic!("database select must return a query")
        };
        assert_eq!(after_commit.rows.len(), 1);
    }

    #[test]
    fn owned_embedded_transaction_preserves_vector_and_similarity_workflows() {
        let database = Arc::new(Database::new());
        let mut transaction = Arc::clone(&database)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        transaction
            .upsert_vector(b"first", b"one", &[1.0, 0.0], alopex_core::Metric::Cosine)
            .unwrap();
        transaction
            .upsert_vector(b"second", b"two", &[0.0, 1.0], alopex_core::Metric::Cosine)
            .unwrap();
        let results = transaction
            .search_similar(&[1.0, 0.0], alopex_core::Metric::Cosine, 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"first".to_vec());
        transaction.commit().unwrap();

        let mut reader = Arc::clone(&database)
            .begin_owned_embedded_transaction(TxnMode::ReadOnly)
            .unwrap();
        assert_eq!(
            reader
                .get_vector(b"second", alopex_core::Metric::Cosine)
                .unwrap(),
            Some(vec![0.0, 1.0])
        );
        reader.rollback().unwrap();
    }
}
