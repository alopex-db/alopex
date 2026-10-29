use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use alopex_cluster::{AuthenticatedSubject, TableLifecycleEffect};
use alopex_core::async_runtime::{BoxFuture, BoxStream};
use alopex_sql::catalog::{IndexMetadata, TableMetadata};
use alopex_sql::executor::{ExecutionResult, ExecutorError, Row};
use alopex_sql::planner::PlannedStatement;
use alopex_sql::storage::erased::ErasedAsyncSqlTransaction;
use dashmap::DashMap;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::error::{Result, ServerError};

/// Session identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionId(Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for SessionId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// Session lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SessionState {
    Idle,
    InTransaction,
    Committing,
    RollingBack,
}

/// Snapshot of a session for safe sharing.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SessionSnapshot {
    pub id: SessionId,
    /// Authenticated subject bound when the session was created for remote work.
    pub authenticated_subject: Option<AuthenticatedSubject>,
    pub has_transaction: bool,
    pub created_at: SystemTime,
    pub last_active: SystemTime,
    pub expires_at: SystemTime,
    pub state: SessionState,
}

/// Transaction handle for a session.
#[derive(Clone)]
pub struct TxnHandle {
    inner: Arc<TxnHandleInner>,
}

/// A table and its attached indexes at one transaction lifecycle boundary.
#[derive(Clone)]
pub struct CatalogTableState {
    pub(crate) name: String,
    pub(crate) table: Option<TableMetadata>,
    pub(crate) indexes: Vec<IndexMetadata>,
}

#[derive(Clone)]
pub enum CatalogRollbackEffect {
    /// Restore only the table names affected by a schema evolution request.
    /// Keep each request in order; repeated ALTERs must unwind in reverse order.
    RestoreTables {
        before: Vec<CatalogTableState>,
        after: Vec<CatalogTableState>,
    },
    DropTable {
        table_name: String,
        table_id: u32,
    },
    CreateTable {
        table: Box<TableMetadata>,
        indexes: Vec<IndexMetadata>,
    },
    DropIndex {
        index_name: String,
        index_id: u32,
    },
    CreateIndex {
        index: Box<IndexMetadata>,
        table_id: u32,
    },
}

pub(crate) struct CommitFinalization {
    pub transaction: alopex_sql::executor::Result<()>,
    pub lifecycle_effects: Vec<TableLifecycleEffect>,
    pub catalog_rollback_effects: Vec<CatalogRollbackEffect>,
}

pub(crate) struct RollbackFinalization {
    pub transaction: alopex_sql::executor::Result<()>,
    pub catalog_rollback_effects: Vec<CatalogRollbackEffect>,
}

struct TxnHandleInner {
    operation_gate: Arc<tokio::sync::Mutex<()>>,
    catalog_txn_guard: tokio::sync::Mutex<Option<SessionCatalogTxnGuard>>,
    txn: tokio::sync::Mutex<Option<Box<dyn ErasedAsyncSqlTransaction>>>,
    pending_table_lifecycle_effects: tokio::sync::Mutex<Vec<TableLifecycleEffect>>,
    pending_catalog_rollback_effects: tokio::sync::Mutex<Vec<CatalogRollbackEffect>>,
    created_at: SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionCatalogTxnGuardKind {
    None,
    Read,
    Write,
}

pub(crate) enum SessionCatalogTxnGuard {
    Read {
        _guard: tokio::sync::OwnedRwLockReadGuard<()>,
    },
    Write {
        _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
        _ordering: tokio::sync::OwnedMutexGuard<()>,
    },
}

impl TxnHandle {
    pub fn new(txn: Box<dyn ErasedAsyncSqlTransaction>) -> Self {
        Self {
            inner: Arc::new(TxnHandleInner {
                operation_gate: Arc::new(tokio::sync::Mutex::new(())),
                catalog_txn_guard: tokio::sync::Mutex::new(None),
                txn: tokio::sync::Mutex::new(Some(txn)),
                pending_table_lifecycle_effects: tokio::sync::Mutex::new(Vec::new()),
                pending_catalog_rollback_effects: tokio::sync::Mutex::new(Vec::new()),
                created_at: SystemTime::now(),
            }),
        }
    }

    pub fn created_at(&self) -> SystemTime {
        self.inner.created_at
    }

    pub fn execute<'a>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, alopex_sql::executor::Result<ExecutionResult>> {
        Box::pin(async move {
            let _operation = self.lock_operation().await;
            let mut guard = self.inner.txn.lock().await;
            let txn = guard
                .as_mut()
                .ok_or_else(|| ExecutorError::InvalidOperation {
                    operation: "execute".into(),
                    reason: "transaction is closed".into(),
                })?;
            txn.execute(sql).await
        })
    }

    pub fn execute_multi<'a>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, alopex_sql::executor::Result<Vec<ExecutionResult>>> {
        Box::pin(async move {
            let operation = self.lock_operation().await;
            self.execute_multi_with_operation(sql, &operation).await
        })
    }

    pub(crate) async fn lock_operation(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.inner.operation_gate).lock_owned().await
    }

    pub(crate) async fn catalog_txn_guard_kind(
        &self,
        _operation: &tokio::sync::OwnedMutexGuard<()>,
    ) -> SessionCatalogTxnGuardKind {
        match self.inner.catalog_txn_guard.lock().await.as_ref() {
            Some(SessionCatalogTxnGuard::Read { .. }) => SessionCatalogTxnGuardKind::Read,
            Some(SessionCatalogTxnGuard::Write { .. }) => SessionCatalogTxnGuardKind::Write,
            None => SessionCatalogTxnGuardKind::None,
        }
    }

    pub(crate) async fn store_catalog_read_guard(
        &self,
        guard: tokio::sync::OwnedRwLockReadGuard<()>,
        _operation: &tokio::sync::OwnedMutexGuard<()>,
    ) {
        let previous = self
            .inner
            .catalog_txn_guard
            .lock()
            .await
            .replace(SessionCatalogTxnGuard::Read { _guard: guard });
        debug_assert!(previous.is_none(), "catalog transaction guard replaced");
    }

    pub(crate) async fn store_catalog_write_guard(
        &self,
        guard: tokio::sync::OwnedRwLockWriteGuard<()>,
        ordering: tokio::sync::OwnedMutexGuard<()>,
        _operation: &tokio::sync::OwnedMutexGuard<()>,
    ) {
        let previous =
            self.inner
                .catalog_txn_guard
                .lock()
                .await
                .replace(SessionCatalogTxnGuard::Write {
                    _guard: guard,
                    _ordering: ordering,
                });
        debug_assert!(previous.is_none(), "catalog transaction guard replaced");
    }

    pub(crate) async fn take_catalog_txn_guard(
        &self,
        _operation: &tokio::sync::OwnedMutexGuard<()>,
    ) -> Option<SessionCatalogTxnGuard> {
        self.inner.catalog_txn_guard.lock().await.take()
    }

    pub(crate) fn execute_multi_with_operation<'a>(
        &'a self,
        sql: &'a str,
        _operation: &'a tokio::sync::OwnedMutexGuard<()>,
    ) -> BoxFuture<'a, alopex_sql::executor::Result<Vec<ExecutionResult>>> {
        Box::pin(async move {
            let mut guard = self.inner.txn.lock().await;
            let txn = guard
                .as_mut()
                .ok_or_else(|| ExecutorError::InvalidOperation {
                    operation: "execute_multi".into(),
                    reason: "transaction is closed".into(),
                })?;
            txn.execute_multi(sql).await
        })
    }

    pub fn query<'a>(&'a self, sql: &'a str) -> BoxStream<'a, alopex_sql::executor::Result<Row>> {
        let (sender, receiver) = mpsc::channel(32);
        let sql = sql.to_string();
        let inner = Arc::clone(&self.inner);

        tokio::spawn(async move {
            if sender.is_closed() {
                return;
            }
            let _operation = Arc::clone(&inner.operation_gate).lock_owned().await;
            let guard = inner.txn.lock().await;
            if sender.is_closed() {
                return;
            }
            let Some(txn) = guard.as_ref() else {
                let _ = sender
                    .send(Err(ExecutorError::InvalidOperation {
                        operation: "query".into(),
                        reason: "transaction is closed".into(),
                    }))
                    .await;
                return;
            };
            let mut stream = txn.query(&sql);
            while let Some(item) = stream.next().await {
                if sender.send(item).await.is_err() {
                    break;
                }
            }
        });

        Box::pin(ReceiverStream::new(receiver))
    }

    pub(crate) fn query_with_operation<'a>(
        &'a self,
        sql: &'a str,
        _operation: &'a tokio::sync::OwnedMutexGuard<()>,
    ) -> BoxStream<'a, alopex_sql::executor::Result<Row>> {
        let (sender, receiver) = mpsc::channel(32);
        let sql = sql.to_string();
        let inner = Arc::clone(&self.inner);

        tokio::spawn(async move {
            if sender.is_closed() {
                return;
            }
            let guard = inner.txn.lock().await;
            if sender.is_closed() {
                return;
            }
            let Some(txn) = guard.as_ref() else {
                let _ = sender
                    .send(Err(ExecutorError::InvalidOperation {
                        operation: "query".into(),
                        reason: "transaction is closed".into(),
                    }))
                    .await;
                return;
            };
            let mut stream = txn.query(&sql);
            while let Some(item) = stream.next().await {
                if sender.send(item).await.is_err() {
                    break;
                }
            }
        });

        Box::pin(ReceiverStream::new(receiver))
    }

    pub(crate) fn wait_for_idle_with_operation<'a>(
        &'a self,
        _operation: &'a tokio::sync::OwnedMutexGuard<()>,
    ) -> BoxFuture<'a, alopex_sql::executor::Result<()>> {
        Box::pin(async move {
            let guard = self.inner.txn.lock().await;
            let txn = guard
                .as_ref()
                .ok_or_else(|| ExecutorError::InvalidOperation {
                    operation: "wait_for_idle".into(),
                    reason: "transaction is closed".into(),
                })?;
            txn.wait_for_idle().await
        })
    }

    pub fn plan_for_routing<'a>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, alopex_sql::executor::Result<Vec<PlannedStatement>>> {
        Box::pin(async move {
            let guard = self.inner.txn.lock().await;
            let txn = guard
                .as_ref()
                .ok_or_else(|| ExecutorError::InvalidOperation {
                    operation: "plan_for_routing".into(),
                    reason: "transaction is closed".into(),
                })?;
            txn.plan_for_routing(sql).await
        })
    }

    pub async fn buffer_table_lifecycle_effects(&self, effects: Vec<TableLifecycleEffect>) {
        if effects.is_empty() {
            return;
        }
        self.inner
            .pending_table_lifecycle_effects
            .lock()
            .await
            .extend(effects);
    }

    pub async fn buffer_catalog_rollback_effects(&self, effects: Vec<CatalogRollbackEffect>) {
        if effects.is_empty() {
            return;
        }
        self.inner
            .pending_catalog_rollback_effects
            .lock()
            .await
            .extend(effects);
    }

    pub async fn commit(self) -> alopex_sql::executor::Result<Vec<TableLifecycleEffect>> {
        let finalization = self.finalize_commit().await;
        finalization.transaction?;
        Ok(finalization.lifecycle_effects)
    }

    pub(crate) async fn finalize_commit(self) -> CommitFinalization {
        let operation = self.lock_operation().await;
        self.finalize_commit_with_operation(&operation).await
    }

    pub(crate) async fn finalize_commit_with_operation(
        self,
        _operation: &tokio::sync::OwnedMutexGuard<()>,
    ) -> CommitFinalization {
        let mut guard = self.inner.txn.lock().await;
        let transaction = match guard.take() {
            Some(txn) => txn.commit_boxed().await,
            None => Err(ExecutorError::InvalidOperation {
                operation: "commit".into(),
                reason: "transaction is closed".into(),
            }),
        };
        drop(guard);
        let lifecycle_effects = {
            let mut effects = self.inner.pending_table_lifecycle_effects.lock().await;
            std::mem::take(&mut *effects)
        };
        let catalog_rollback_effects = {
            let mut effects = self.inner.pending_catalog_rollback_effects.lock().await;
            std::mem::take(&mut *effects)
        };
        CommitFinalization {
            transaction,
            lifecycle_effects,
            catalog_rollback_effects,
        }
    }

    pub async fn rollback(self) -> alopex_sql::executor::Result<Vec<CatalogRollbackEffect>> {
        let finalization = self.finalize_rollback().await;
        finalization.transaction?;
        Ok(finalization.catalog_rollback_effects)
    }

    pub(crate) async fn finalize_rollback(self) -> RollbackFinalization {
        let operation = self.lock_operation().await;
        self.finalize_rollback_with_operation(&operation).await
    }

    pub(crate) async fn finalize_rollback_with_operation(
        self,
        _operation: &tokio::sync::OwnedMutexGuard<()>,
    ) -> RollbackFinalization {
        let mut guard = self.inner.txn.lock().await;
        let transaction = match guard.take() {
            Some(txn) => txn.rollback_boxed().await,
            None => Err(ExecutorError::InvalidOperation {
                operation: "rollback".into(),
                reason: "transaction is closed".into(),
            }),
        };
        drop(guard);
        self.inner
            .pending_table_lifecycle_effects
            .lock()
            .await
            .clear();
        let catalog_rollback_effects = {
            let mut effects = self.inner.pending_catalog_rollback_effects.lock().await;
            std::mem::take(&mut *effects)
        };
        RollbackFinalization {
            transaction,
            catalog_rollback_effects,
        }
    }

    #[cfg(test)]
    pub(crate) fn operation_is_locked(&self) -> bool {
        self.inner.operation_gate.try_lock().is_err()
    }
}

/// Session configuration.
#[derive(Clone, Copy, Debug)]
pub struct SessionConfig {
    pub ttl: Duration,
}

/// Transaction factory for session manager.
pub type TransactionFactory =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Box<dyn ErasedAsyncSqlTransaction>>> + Send + Sync>;

/// Session manager for server.
pub struct SessionManager {
    sessions: DashMap<SessionId, Session>,
    config: SessionConfig,
    txn_factory: TransactionFactory,
    expired_handle_finalizer: OnceLock<Arc<dyn Fn(TxnHandle) + Send + Sync>>,
}

struct Session {
    id: SessionId,
    authenticated_subject: Option<AuthenticatedSubject>,
    txn_handle: Option<TxnHandle>,
    created_at: SystemTime,
    last_active: SystemTime,
    expires_at: SystemTime,
    state: SessionState,
}

impl SessionManager {
    pub fn new(config: SessionConfig, txn_factory: TransactionFactory) -> Self {
        Self {
            sessions: DashMap::new(),
            config,
            txn_factory,
            expired_handle_finalizer: OnceLock::new(),
        }
    }

    pub(crate) fn set_expired_handle_finalizer(
        &self,
        finalizer: Arc<dyn Fn(TxnHandle) + Send + Sync>,
    ) {
        self.expired_handle_finalizer
            .set(finalizer)
            .unwrap_or_else(|_| panic!("expired session finalizer must only be installed once"));
    }

    fn remove_expired(&self, id: &SessionId) {
        let Some((_, mut session)) = self.sessions.remove(id) else {
            return;
        };
        if let Some(handle) = session.txn_handle.take() {
            if let Some(finalizer) = self.expired_handle_finalizer.get() {
                finalizer(handle);
            }
        }
    }

    pub async fn create_session(&self) -> Result<SessionId> {
        self.create_session_with_subject(None).await
    }

    /// Create a session whose authority is permanently bound to a validated
    /// remote-read delegation subject.
    pub async fn create_authenticated_session(
        &self,
        subject: AuthenticatedSubject,
    ) -> Result<SessionId> {
        self.create_session_with_subject(Some(subject)).await
    }

    async fn create_session_with_subject(
        &self,
        authenticated_subject: Option<AuthenticatedSubject>,
    ) -> Result<SessionId> {
        let now = SystemTime::now();
        let id = SessionId::new();
        let session = Session {
            id: id.clone(),
            authenticated_subject,
            txn_handle: None,
            created_at: now,
            last_active: now,
            expires_at: now + self.config.ttl,
            state: SessionState::Idle,
        };
        self.sessions.insert(id.clone(), session);
        Ok(id)
    }

    /// Return the subject bound to a remote-read session.
    ///
    /// Ordinary legacy sessions intentionally have no subject and cannot be
    /// repurposed as a remote worker session.
    pub async fn authenticated_subject(&self, id: &SessionId) -> Result<AuthenticatedSubject> {
        let snapshot = self.get_session(id).await?;
        snapshot.authenticated_subject.ok_or_else(|| {
            ServerError::Unauthorized("remote read requires a subject-bound session".into())
        })
    }

    pub async fn get_session(&self, id: &SessionId) -> Result<SessionSnapshot> {
        let entry = self
            .sessions
            .get(id)
            .ok_or_else(|| ServerError::NotFound("session not found".into()))?;
        if entry.expires_at <= SystemTime::now() {
            drop(entry);
            self.remove_expired(id);
            return Err(ServerError::SessionExpired("session expired".into()));
        }
        Ok(SessionSnapshot {
            id: entry.id.clone(),
            authenticated_subject: entry.authenticated_subject.clone(),
            has_transaction: entry.txn_handle.is_some(),
            created_at: entry.created_at,
            last_active: entry.last_active,
            expires_at: entry.expires_at,
            state: entry.state,
        })
    }

    pub async fn begin_transaction(&self, id: &SessionId) -> Result<TxnHandle> {
        let mut entry = self
            .sessions
            .get_mut(id)
            .ok_or_else(|| ServerError::NotFound("session not found".into()))?;
        if entry.expires_at <= SystemTime::now() {
            drop(entry);
            self.remove_expired(id);
            return Err(ServerError::SessionExpired("session expired".into()));
        }
        if entry.txn_handle.is_some() {
            return Err(ServerError::Conflict("transaction already active".into()));
        }
        let txn = (self.txn_factory)().await?;
        let handle = TxnHandle::new(txn);
        entry.txn_handle = Some(handle.clone());
        entry.last_active = SystemTime::now();
        entry.state = SessionState::InTransaction;
        Ok(handle)
    }

    pub async fn get_transaction(&self, id: &SessionId) -> Result<TxnHandle> {
        let mut entry = self
            .sessions
            .get_mut(id)
            .ok_or_else(|| ServerError::NotFound("session not found".into()))?;
        if entry.expires_at <= SystemTime::now() {
            drop(entry);
            self.remove_expired(id);
            return Err(ServerError::SessionExpired("session expired".into()));
        }
        let handle = entry
            .txn_handle
            .clone()
            .ok_or_else(|| ServerError::BadRequest("transaction not started".into()))?;
        entry.last_active = SystemTime::now();
        entry.state = SessionState::InTransaction;
        Ok(handle)
    }

    pub async fn execute_in_session(&self, id: &SessionId, sql: &str) -> Result<ExecutionResult> {
        let handle = {
            let mut entry = self
                .sessions
                .get_mut(id)
                .ok_or_else(|| ServerError::NotFound("session not found".into()))?;
            if entry.expires_at <= SystemTime::now() {
                drop(entry);
                self.remove_expired(id);
                return Err(ServerError::SessionExpired("session expired".into()));
            }
            let handle = entry
                .txn_handle
                .clone()
                .ok_or_else(|| ServerError::BadRequest("transaction not started".into()))?;
            entry.last_active = SystemTime::now();
            handle
        };

        handle
            .execute(sql)
            .await
            .map_err(|err| ServerError::Sql(err.into()))
    }

    pub async fn commit(&self, id: &SessionId) -> Result<Vec<TableLifecycleEffect>> {
        let handle = self.take_handle(id, SessionState::Committing)?;
        let effects = handle
            .commit()
            .await
            .map_err(|err| ServerError::Sql(err.into()))?;
        Ok(effects)
    }

    pub async fn rollback(&self, id: &SessionId) -> Result<Vec<CatalogRollbackEffect>> {
        let handle = self.take_handle(id, SessionState::RollingBack)?;
        let effects = handle
            .rollback()
            .await
            .map_err(|err| ServerError::Sql(err.into()))?;
        Ok(effects)
    }

    pub(crate) fn take_handle_for_finalization(
        &self,
        id: &SessionId,
        state: SessionState,
    ) -> Result<TxnHandle> {
        self.take_handle(id, state)
    }

    pub fn cleanup_expired(&self) {
        let now = SystemTime::now();
        let expired: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|entry| entry.expires_at <= now)
            .map(|entry| entry.id.clone())
            .collect();
        for id in expired {
            self.remove_expired(&id);
        }
    }

    #[cfg(test)]
    pub(crate) fn active_session_count(&self) -> usize {
        self.sessions.len()
    }

    #[cfg(test)]
    pub(crate) fn expire_now(&self, id: &SessionId) {
        self.sessions
            .get_mut(id)
            .expect("session must exist")
            .expires_at = SystemTime::UNIX_EPOCH;
    }

    fn take_handle(&self, id: &SessionId, state: SessionState) -> Result<TxnHandle> {
        let mut entry = self
            .sessions
            .get_mut(id)
            .ok_or_else(|| ServerError::NotFound("session not found".into()))?;
        if entry.expires_at <= SystemTime::now() {
            drop(entry);
            self.remove_expired(id);
            return Err(ServerError::SessionExpired("session expired".into()));
        }
        let handle = entry
            .txn_handle
            .take()
            .ok_or_else(|| ServerError::BadRequest("transaction not started".into()))?;
        entry.state = state;
        entry.last_active = SystemTime::now();
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTransaction {
        fail_finalization: bool,
        idle_gate: Option<Arc<tokio::sync::Semaphore>>,
        query_started: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    impl ErasedAsyncSqlTransaction for TestTransaction {
        fn execute<'a>(
            &'a mut self,
            _sql: &'a str,
        ) -> BoxFuture<'a, alopex_sql::executor::Result<ExecutionResult>> {
            Box::pin(async { Ok(ExecutionResult::Success) })
        }

        fn execute_multi<'a>(
            &'a mut self,
            _sql: &'a str,
        ) -> BoxFuture<'a, alopex_sql::executor::Result<Vec<ExecutionResult>>> {
            Box::pin(async { Ok(vec![ExecutionResult::Success]) })
        }

        fn query<'a>(&'a self, _sql: &'a str) -> BoxStream<'a, alopex_sql::executor::Result<Row>> {
            if let Some(query_started) = &self.query_started {
                query_started.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Box::pin(futures::stream::empty())
        }

        fn plan_for_routing<'a>(
            &'a self,
            _sql: &'a str,
        ) -> BoxFuture<'a, alopex_sql::executor::Result<Vec<PlannedStatement>>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn wait_for_idle(&self) -> BoxFuture<'_, alopex_sql::executor::Result<()>> {
            Box::pin(async move {
                if let Some(idle_gate) = &self.idle_gate {
                    idle_gate.acquire().await.expect("idle gate").forget();
                }
                Ok(())
            })
        }

        fn commit_boxed(self: Box<Self>) -> BoxFuture<'static, alopex_sql::executor::Result<()>> {
            Box::pin(async move {
                if self.fail_finalization {
                    Err(ExecutorError::InvalidOperation {
                        operation: "commit".into(),
                        reason: "controlled failure".into(),
                    })
                } else {
                    Ok(())
                }
            })
        }

        fn rollback_boxed(self: Box<Self>) -> BoxFuture<'static, alopex_sql::executor::Result<()>> {
            Box::pin(async move {
                if self.fail_finalization {
                    Err(ExecutorError::InvalidOperation {
                        operation: "rollback".into(),
                        reason: "controlled failure".into(),
                    })
                } else {
                    Ok(())
                }
            })
        }
    }

    fn manager() -> SessionManager {
        SessionManager::new(
            SessionConfig {
                ttl: Duration::from_secs(60),
            },
            Arc::new(|| {
                Box::pin(async {
                    Err(ServerError::Internal(
                        "test factory must not create a transaction".into(),
                    ))
                })
            }),
        )
    }

    #[tokio::test]
    async fn only_authenticated_sessions_expose_a_remote_read_subject() {
        let manager = manager();
        let anonymous = manager.create_session().await.unwrap();
        assert!(matches!(
            manager.authenticated_subject(&anonymous).await,
            Err(ServerError::Unauthorized(_))
        ));

        let subject = AuthenticatedSubject::new("user-a");
        let bound = manager
            .create_authenticated_session(subject.clone())
            .await
            .unwrap();
        assert_eq!(
            manager.authenticated_subject(&bound).await.unwrap(),
            subject
        );
    }

    #[tokio::test]
    async fn timeout_response_does_not_allow_rollback_to_overtake_effect_buffering() {
        let handle = TxnHandle::new(Box::new(TestTransaction {
            fail_finalization: false,
            idle_gate: None,
            query_started: None,
        }));
        let allow_effect_buffering = Arc::new(tokio::sync::Notify::new());
        let allow_effect_buffering_for_coordinator = Arc::clone(&allow_effect_buffering);
        let handle_for_coordinator = handle.clone();
        let (timeout_response_tx, timeout_response_rx) = tokio::sync::oneshot::channel();

        let coordinator = tokio::spawn(async move {
            let operation = handle_for_coordinator.lock_operation().await;
            timeout_response_tx
                .send(())
                .expect("timeout response receiver");
            allow_effect_buffering_for_coordinator.notified().await;
            handle_for_coordinator
                .buffer_catalog_rollback_effects(vec![CatalogRollbackEffect::DropTable {
                    table_name: "timed_out_table".into(),
                    table_id: 41,
                }])
                .await;
            drop(operation);
        });

        timeout_response_rx.await.expect("timeout response");
        let mut rollback = tokio::spawn(handle.rollback());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut rollback)
                .await
                .is_err(),
            "rollback must wait until the coordinator buffers catalog effects"
        );

        allow_effect_buffering.notify_one();
        coordinator.await.expect("coordinator");
        let effects = rollback
            .await
            .expect("rollback task")
            .expect("rollback result");
        assert_eq!(effects.len(), 1);
        match &effects[0] {
            CatalogRollbackEffect::DropTable {
                table_name,
                table_id,
            } => {
                assert_eq!(table_name, "timed_out_table");
                assert_eq!(*table_id, 41);
            }
            _ => panic!("rollback must receive the buffered catalog effect"),
        }
    }

    #[tokio::test]
    async fn idle_barrier_keeps_session_operation_locked_until_worker_cleanup() {
        let idle_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handle = TxnHandle::new(Box::new(TestTransaction {
            fail_finalization: false,
            idle_gate: Some(Arc::clone(&idle_gate)),
            query_started: None,
        }));
        let operation = handle.lock_operation().await;
        let mut idle = Box::pin(handle.wait_for_idle_with_operation(&operation));

        assert!(futures::poll!(idle.as_mut()).is_pending());
        assert!(handle.operation_is_locked());
        idle_gate.add_permits(1);
        idle.await.expect("idle barrier");
        assert!(handle.operation_is_locked());
        drop(operation);
        assert!(!handle.operation_is_locked());
    }

    #[tokio::test]
    async fn dropped_session_stream_does_not_start_query_after_txn_wait() {
        let query_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = TxnHandle::new(Box::new(TestTransaction {
            fail_finalization: false,
            idle_gate: None,
            query_started: Some(Arc::clone(&query_started)),
        }));
        let operation = handle.lock_operation().await;
        let txn_guard = handle.inner.txn.lock().await;
        let stream = handle.query_with_operation("SELECT 1", &operation);
        drop(stream);
        drop(txn_guard);
        tokio::task::yield_now().await;

        assert!(!query_started.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_finalization_still_returns_catalog_rollback_effects() {
        let commit_handle = TxnHandle::new(Box::new(TestTransaction {
            fail_finalization: true,
            idle_gate: None,
            query_started: None,
        }));
        commit_handle
            .buffer_catalog_rollback_effects(vec![CatalogRollbackEffect::DropTable {
                table_name: "commit_failure".into(),
                table_id: 51,
            }])
            .await;
        let commit = commit_handle.finalize_commit().await;
        assert!(commit.transaction.is_err());
        assert_eq!(commit.catalog_rollback_effects.len(), 1);

        let rollback_handle = TxnHandle::new(Box::new(TestTransaction {
            fail_finalization: true,
            idle_gate: None,
            query_started: None,
        }));
        rollback_handle
            .buffer_catalog_rollback_effects(vec![CatalogRollbackEffect::DropTable {
                table_name: "rollback_failure".into(),
                table_id: 52,
            }])
            .await;
        let rollback = rollback_handle.finalize_rollback().await;
        assert!(rollback.transaction.is_err());
        assert_eq!(rollback.catalog_rollback_effects.len(), 1);
    }
}
