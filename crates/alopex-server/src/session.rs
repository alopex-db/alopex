use std::sync::Arc;
use std::time::{Duration, SystemTime};

use alopex_cluster::{AuthenticatedSubject, TableLifecycleEffect};
use alopex_core::async_runtime::{BoxFuture, BoxStream};
use alopex_sql::catalog::TableMetadata;
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

#[derive(Clone)]
pub enum CatalogRollbackEffect {
    DropTable { table_name: String },
    CreateTable { table: Box<TableMetadata> },
}

struct TxnHandleInner {
    txn: tokio::sync::Mutex<Option<Box<dyn ErasedAsyncSqlTransaction>>>,
    pending_table_lifecycle_effects: tokio::sync::Mutex<Vec<TableLifecycleEffect>>,
    pending_catalog_rollback_effects: tokio::sync::Mutex<Vec<CatalogRollbackEffect>>,
    created_at: SystemTime,
}

impl TxnHandle {
    pub fn new(txn: Box<dyn ErasedAsyncSqlTransaction>) -> Self {
        Self {
            inner: Arc::new(TxnHandleInner {
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
            let guard = inner.txn.lock().await;
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
        let mut guard = self.inner.txn.lock().await;
        let txn = guard
            .take()
            .ok_or_else(|| ExecutorError::InvalidOperation {
                operation: "commit".into(),
                reason: "transaction is closed".into(),
            })?;
        txn.commit_boxed().await?;
        let mut effects = self.inner.pending_table_lifecycle_effects.lock().await;
        Ok(std::mem::take(&mut *effects))
    }

    pub async fn rollback(self) -> alopex_sql::executor::Result<Vec<CatalogRollbackEffect>> {
        let mut guard = self.inner.txn.lock().await;
        let txn = guard
            .take()
            .ok_or_else(|| ExecutorError::InvalidOperation {
                operation: "rollback".into(),
                reason: "transaction is closed".into(),
            })?;
        let result = txn.rollback_boxed().await;
        self.inner
            .pending_table_lifecycle_effects
            .lock()
            .await
            .clear();
        result?;
        let mut effects = self.inner.pending_catalog_rollback_effects.lock().await;
        Ok(std::mem::take(&mut *effects))
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
            self.sessions.remove(id);
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
            self.sessions.remove(id);
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
            self.sessions.remove(id);
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
                self.sessions.remove(id);
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

    pub fn cleanup_expired(&self) {
        let now = SystemTime::now();
        let expired: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|entry| entry.expires_at <= now)
            .map(|entry| entry.id.clone())
            .collect();
        for id in expired {
            self.sessions.remove(&id);
        }
    }

    #[cfg(test)]
    pub(crate) fn active_session_count(&self) -> usize {
        self.sessions.len()
    }

    fn take_handle(&self, id: &SessionId, state: SessionState) -> Result<TxnHandle> {
        let mut entry = self
            .sessions
            .get_mut(id)
            .ok_or_else(|| ServerError::NotFound("session not found".into()))?;
        if entry.expires_at <= SystemTime::now() {
            drop(entry);
            self.sessions.remove(id);
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
}
