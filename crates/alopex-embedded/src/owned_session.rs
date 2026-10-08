//! Owned embedded-session factories for long-lived local consumers.
//!
//! The existing [`crate::Database::begin`] API remains a borrowed Rust facade. This module is the
//! separate boundary used by Python and asynchronous stream work: it clones the database's
//! storage `Arc` and returns only core-owned session state.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alopex_core::kv::{
    AnyKV, KVTransaction, KeySearchPage, KeySearchRequest, OwnedKVTransactionAdapter,
    OwnedReadOptions, OwnedReadSession, OwnedSessionFactory as CoreOwnedSessionFactory,
    OwnedTransactionSession,
};
use alopex_core::vector::hnsw::{HnswIndex, HnswTransactionState};
use alopex_core::TxnMode;
use alopex_sql::catalog::CatalogOverlay;
use alopex_sql::storage::LocalRangeChangeJournal;

use crate::{Database, Error, Result, VectorKeyIndex};

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
            vector_index: None,
            vector_index_dirty: false,
            failed: false,
            savepoints: Vec::new(),
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
    pub(crate) vector_index: Option<VectorKeyIndex>,
    pub(crate) vector_index_dirty: bool,
    pub(crate) failed: bool,
    savepoints: Vec<OwnedEmbeddedSavepoint>,
}

struct OwnedEmbeddedSavepoint {
    name: String,
    core_id: u64,
    overlay: CatalogOverlay,
    catalog_modified: bool,
    vector_cache_invalidated: bool,
    vector_index: Option<VectorKeyIndex>,
    vector_index_dirty: bool,
    hnsw_index_names: HashSet<String>,
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

    /// Collect key-value pairs whose keys start with `prefix`.
    pub fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(alopex_core::Key, Vec<u8>)>> {
        let prefix = prefix.to_vec();
        self.session
            .with_transaction(|transaction| {
                let mut scan = transaction.scan_prefix(&prefix)?;
                let mut entries = Vec::new();
                while let Some(entry) = scan.next_entry()? {
                    entries.push(entry);
                }
                scan.close()?;
                Ok(entries)
            })
            .map_err(Error::Core)
    }

    /// Collect key-value pairs in the half-open range `[start, end)`.
    pub fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(alopex_core::Key, Vec<u8>)>> {
        let start = start.to_vec();
        let end = end.to_vec();
        self.session
            .with_transaction(|transaction| {
                let mut scan = transaction.scan_range(&start, &end)?;
                let mut entries = Vec::new();
                while let Some(entry) = scan.next_entry()? {
                    entries.push(entry);
                }
                scan.close()?;
                Ok(entries)
            })
            .map_err(Error::Core)
    }

    /// Search opaque keys with the shared bounded search contract.
    pub fn search_keys(&mut self, request: &KeySearchRequest) -> Result<KeySearchPage> {
        self.session
            .with_transaction(|transaction| transaction.search_keys(request))
            .map_err(Error::Core)
    }

    /// Execute SQL without committing this transaction.
    ///
    /// The implementation is in `sql_api.rs` so it uses the same planner, catalog overlay, and
    /// error mapping as the borrowed compatibility transaction.
    pub fn execute_sql(&mut self, sql: &str) -> Result<crate::SqlResult> {
        crate::sql_api::execute_sql_owned(self, sql)
    }

    /// Execute one parsed SQL statement with native bound values without committing.
    ///
    /// Callers that own an embedded transaction can preserve typed parameter values
    /// instead of rendering them into SQL text before execution.
    pub fn execute_prepared_statement(
        &mut self,
        statement: &alopex_sql::Statement,
        parameters: &[alopex_sql::SqlValue],
    ) -> Result<crate::SqlResult> {
        crate::sql_api::execute_prepared_owned(self, statement, parameters)
    }

    pub(crate) fn execute_prepared_many<I, V, F>(
        &mut self,
        statement: &alopex_sql::Statement,
        rows: I,
        validate: F,
    ) -> Result<Vec<crate::SqlResult>>
    where
        I: IntoIterator<Item = V>,
        V: AsRef<[alopex_sql::SqlValue]>,
        F: FnMut(&[alopex_sql::SqlValue]) -> Result<()>,
    {
        crate::sql_api::execute_prepared_many_owned(self, statement, rows, validate)
    }

    /// Create a named SQL savepoint.
    pub fn create_savepoint(&mut self, name: &str) -> Result<()> {
        if self.failed {
            return Err(Error::TxnFailed);
        }
        self.stage_hnsw_changes()?;
        let core_id = self.session.create_savepoint().map_err(Error::Core)?;
        self.savepoints.push(OwnedEmbeddedSavepoint {
            name: name.to_owned(),
            core_id,
            overlay: self.overlay.clone(),
            catalog_modified: self.catalog_modified,
            vector_cache_invalidated: self.vector_cache_invalidated,
            vector_index: self.vector_index.clone(),
            vector_index_dirty: self.vector_index_dirty,
            hnsw_index_names: self.hnsw_indices.keys().cloned().collect(),
        });
        Ok(())
    }

    /// Roll back to the most recent matching named SQL savepoint.
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let position = self.savepoint_position(name)?;
        let savepoint = &self.savepoints[position];
        let hnsw_index_names = savepoint.hnsw_index_names.clone();
        self.session
            .rollback_to_savepoint(savepoint.core_id)
            .map_err(Error::Core)?;
        self.overlay = savepoint.overlay.clone();
        self.catalog_modified = savepoint.catalog_modified;
        self.vector_cache_invalidated = savepoint.vector_cache_invalidated;
        self.vector_index = savepoint.vector_index.clone();
        self.vector_index_dirty = savepoint.vector_index_dirty;
        self.rollback_hnsw_to_savepoint(&hnsw_index_names)?;
        self.failed = false;
        self.savepoints.truncate(position + 1);
        Ok(())
    }

    /// Release the most recent matching named SQL savepoint and nested savepoints.
    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        if self.failed {
            return Err(Error::TxnFailed);
        }
        let position = self.savepoint_position(name)?;
        self.session
            .release_savepoint(self.savepoints[position].core_id)
            .map_err(Error::Core)?;
        self.savepoints.truncate(position);
        Ok(())
    }

    fn savepoint_position(&self, name: &str) -> Result<usize> {
        self.savepoints
            .iter()
            .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::SavepointNotFound(name.to_owned()))
    }

    /// Make direct HNSW changes visible to SQL and discard the superseded graph.
    pub(crate) fn stage_hnsw_before_sql(&mut self) -> Result<()> {
        if self.failed {
            return Err(Error::TxnFailed);
        }
        if !self.hnsw_indices.is_empty() {
            if let Err(error) = self.stage_hnsw_changes() {
                self.failed = true;
                return Err(error);
            }
            // SQL owns its own graph and may update or drop an index. The next
            // direct HNSW operation must reload the transaction's current KV state.
            self.hnsw_indices.clear();
        }
        Ok(())
    }

    /// Stage direct HNSW changes before capturing the core transaction savepoint.
    fn stage_hnsw_changes(&mut self) -> Result<()> {
        let mut preparation = Ok(());
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
                Ok(())
            })
            .map_err(Error::Core)?;
        preparation
    }

    /// Restore direct HNSW state to the set present when the savepoint was created.
    fn rollback_hnsw_to_savepoint(&mut self, retained: &HashSet<String>) -> Result<()> {
        // A newer savepoint stages HNSW changes and clears their undo snapshot.
        // The core transaction has already rolled back to the requested point;
        // reload that version instead of using the latest graph's undo state.
        let restored = self
            .session
            .with_transaction(|transaction| {
                let mut transaction = OwnedKVTransactionAdapter::new(transaction);
                let mut restored = HashMap::with_capacity(retained.len());
                for name in retained {
                    let index = HnswIndex::load(name, &mut transaction)?;
                    restored.insert(name.clone(), (index, HnswTransactionState::default()));
                }
                Ok(restored)
            })
            .map_err(Error::Core)?;
        self.hnsw_indices = restored;
        Ok(())
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
        if self.failed {
            return Err(Error::TxnFailed);
        }
        let mut preparation = Ok(());
        let vector_index = if self.vector_index_dirty {
            Some(
                crate::encode_index(
                    self.vector_index
                        .as_ref()
                        .map_or(&[] as &[alopex_core::Key], VectorKeyIndex::keys),
                )
                .map_err(Error::Core)?,
            )
        } else {
            None
        };
        let journal = self.journal.take();
        self.session
            .with_transaction(|transaction| {
                let mut transaction = alopex_core::kv::any::AnyKVTransaction::Owned(
                    OwnedKVTransactionAdapter::new(transaction),
                );
                if let Some(encoded) = &vector_index {
                    preparation = transaction
                        .put(crate::VECTOR_INDEX_KEY.to_vec(), encoded.clone())
                        .map_err(Error::Core);
                }
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

        let overlay = std::mem::take(&mut self.overlay);
        let hnsw_indices = std::mem::take(&mut self.hnsw_indices);
        self.db.commit_with_hnsw_cache_changes(
            || {
                self.session
                    .commit_with_observer(|transaction| {
                        self.db.hnsw_cache_changes(|visitor| {
                            transaction.visit_pending_write_keys(visitor)
                        })
                    })
                    .map_err(Error::Core)
            },
            || {
                let mut catalog = self.db.sql_catalog.write().expect("catalog lock poisoned");
                catalog.apply_overlay(overlay);
                drop(catalog);
                if !hnsw_indices.is_empty() {
                    let mut cache = self
                        .db
                        .hnsw_cache
                        .write()
                        .expect("hnsw cache lock poisoned");
                    for (name, (index, _)) in hnsw_indices {
                        cache.insert(name, Arc::new(index));
                    }
                }
            },
        )?;
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
    use alopex_core::{HnswConfig, Metric};
    use std::sync::Arc;

    #[test]
    fn owned_prepared_many_after_direct_hnsw_noop_uses_latest_vector() {
        use alopex_sql::storage::SqlValue;
        use alopex_sql::{AlopexDialect, ExecutionResult, Parser};

        let db = Arc::new(Database::open_in_memory().unwrap());
        db.execute_sql(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2, L2));
             INSERT INTO items VALUES (1, [0.0, 0.0]);
             CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;",
        )
        .unwrap();
        let index_name = "idx_items_embedding";
        let (before, _) = db.search_hnsw(index_name, &[0.0, 0.0], 1, Some(8)).unwrap();
        assert_eq!(before.len(), 1);
        let key = before[0].key.clone();
        let metadata = before[0].metadata.clone();
        let mut transaction = Arc::clone(&db)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        transaction
            .upsert_to_hnsw(index_name, &key, &[0.0, 0.0], &metadata)
            .unwrap();
        let statement = Parser::parse_sql(
            &AlopexDialect,
            "UPDATE items SET embedding = [9.0, 0.0] WHERE id = 1",
        )
        .unwrap()
        .remove(0);
        let results = transaction
            .execute_prepared_many(&statement, [Vec::<SqlValue>::new()], |_| Ok(()))
            .unwrap();
        assert_eq!(results.len(), 1);
        transaction.commit().unwrap();

        let ExecutionResult::Query(result) = db
            .execute_sql("SELECT embedding FROM items WHERE id = 1")
            .unwrap()
        else {
            panic!("expected SQL query result");
        };
        assert_eq!(result.rows, vec![vec![SqlValue::Vector(vec![9.0, 0.0])]]);
        let (after, _) = db.search_hnsw(index_name, &[9.0, 0.0], 1, Some(8)).unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].key, key);
        assert_eq!(after[0].metadata, metadata);
        assert_eq!(after[0].distance, 0.0);
    }

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

    #[test]
    fn owned_embedded_nested_savepoint_rollback_discards_staged_hnsw_mutations() {
        for fail_sql in [false, true] {
            let database = Arc::new(Database::new());
            database
                .create_hnsw_index(
                    "vec_idx",
                    HnswConfig::default()
                        .with_dimension(2)
                        .with_metric(Metric::L2),
                )
                .unwrap();

            let mut transaction = Arc::clone(&database)
                .begin_owned_embedded_transaction(TxnMode::ReadWrite)
                .unwrap();
            transaction
                .upsert_to_hnsw("vec_idx", b"keep", &[0.0, 0.0], b"")
                .unwrap();
            transaction.create_savepoint("s1").unwrap();
            transaction
                .upsert_to_hnsw("vec_idx", b"discard", &[1.0, 0.0], b"")
                .unwrap();
            // Capturing s2 stages the HNSW delta into the core transaction. An
            // older savepoint must still restore the graph that existed at s1.
            if fail_sql {
                // Planning fails after the direct graph has been staged and cleared.
                assert!(transaction
                    .execute_sql("SELECT * FROM missing_table")
                    .is_err());
                assert!(matches!(transaction.commit(), Err(Error::TxnFailed)));
            } else {
                transaction.create_savepoint("s2").unwrap();
            }
            transaction.rollback_to_savepoint("s1").unwrap();
            transaction.commit().unwrap();

            let (results, _) = database
                .search_hnsw("vec_idx", &[0.0, 0.0], 10, Some(10))
                .unwrap();
            let mut keys: Vec<_> = results.into_iter().map(|result| result.key).collect();
            keys.sort();
            assert_eq!(keys, vec![b"keep".to_vec()]);
        }
    }

    #[test]
    fn owned_embedded_savepoint_rollback_discards_hnsw_mutations() {
        let database = Arc::new(Database::new());
        let config = HnswConfig::default()
            .with_dimension(2)
            .with_metric(Metric::L2);
        database
            .create_hnsw_index("vec_idx", config.clone())
            .unwrap();
        database.create_hnsw_index("post_idx", config).unwrap();

        let mut transaction = Arc::clone(&database)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        transaction
            .upsert_to_hnsw("vec_idx", b"keep", &[0.0, 0.0], b"")
            .unwrap();
        transaction.create_savepoint("before_hnsw").unwrap();
        transaction
            .upsert_to_hnsw("vec_idx", b"discard", &[1.0, 0.0], b"")
            .unwrap();
        transaction
            .upsert_to_hnsw_batch(
                "vec_idx",
                &[b"discard_batch".to_vec()],
                &[&[2.0, 0.0]],
                None,
            )
            .unwrap();
        assert!(transaction.delete_from_hnsw("vec_idx", b"keep").unwrap());
        transaction
            .upsert_to_hnsw("post_idx", b"post_discard", &[3.0, 0.0], b"")
            .unwrap();
        transaction.rollback_to_savepoint("before_hnsw").unwrap();
        transaction.commit().unwrap();

        let (results, _) = database
            .search_hnsw("vec_idx", &[0.0, 0.0], 10, Some(10))
            .unwrap();
        let mut keys: Vec<_> = results.into_iter().map(|result| result.key).collect();
        keys.sort();
        assert_eq!(keys, vec![b"keep".to_vec()]);

        let (post_results, _) = database
            .search_hnsw("post_idx", &[3.0, 0.0], 10, Some(10))
            .unwrap();
        assert!(post_results.is_empty());
    }
}
