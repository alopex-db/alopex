use std::sync::Arc;
use std::time::{Duration, SystemTime};

use alopex_core::async_runtime::{BoxFuture, BoxStream};
use alopex_sql::executor::{ExecutionResult, ExecutorError, Row};
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

struct TxnHandleInner {
    txn: tokio::sync::Mutex<Option<Box<dyn ErasedAsyncSqlTransaction>>>,
    created_at: SystemTime,
}

impl TxnHandle {
    pub fn new(txn: Box<dyn ErasedAsyncSqlTransaction>) -> Self {
        Self {
            inner: Arc::new(TxnHandleInner {
                txn: tokio::sync::Mutex::new(Some(txn)),
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

    pub async fn commit(self) -> alopex_sql::executor::Result<()> {
        let mut guard = self.inner.txn.lock().await;
        let txn = guard
            .take()
            .ok_or_else(|| ExecutorError::InvalidOperation {
                operation: "commit".into(),
                reason: "transaction is closed".into(),
            })?;
        txn.commit_boxed().await
    }

    pub async fn rollback(self) -> alopex_sql::executor::Result<()> {
        let mut guard = self.inner.txn.lock().await;
        let txn = guard
            .take()
            .ok_or_else(|| ExecutorError::InvalidOperation {
                operation: "rollback".into(),
                reason: "transaction is closed".into(),
            })?;
        txn.rollback_boxed().await
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
        let now = SystemTime::now();
        let id = SessionId::new();
        let session = Session {
            id: id.clone(),
            txn_handle: None,
            created_at: now,
            last_active: now,
            expires_at: now + self.config.ttl,
            state: SessionState::Idle,
        };
        self.sessions.insert(id.clone(), session);
        Ok(id)
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

    pub async fn commit(&self, id: &SessionId) -> Result<()> {
        let handle = self.take_handle(id, SessionState::Committing)?;
        handle
            .commit()
            .await
            .map_err(|err| ServerError::Sql(err.into()))?;
        Ok(())
    }

    pub async fn rollback(&self, id: &SessionId) -> Result<()> {
        let handle = self.take_handle(id, SessionState::RollingBack)?;
        handle
            .rollback()
            .await
            .map_err(|err| ServerError::Sql(err.into()))?;
        Ok(())
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
