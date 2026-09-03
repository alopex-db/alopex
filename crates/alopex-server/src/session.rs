use std::sync::Arc;
use std::time::{Duration, SystemTime};

use alopex_cluster::{AuthenticatedSubject, TableLifecycleEffect};
use alopex_core::async_runtime::{BoxFuture, BoxStream};
use alopex_sql::catalog::TableMetadata;
use alopex_sql::executor::{ExecutionResult, ExecutorError, Row};
use alopex_sql::parser::Parser;
use alopex_sql::planner::PlannedStatement;
use alopex_sql::storage::erased::ErasedAsyncSqlTransaction;
use alopex_sql::AlopexDialect;
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
    sql_characteristics: tokio::sync::Mutex<SqlTransactionCharacteristics>,
    pending_table_lifecycle_effects: tokio::sync::Mutex<Vec<TableLifecycleEffect>>,
    pending_catalog_rollback_effects: tokio::sync::Mutex<Vec<CatalogRollbackEffect>>,
    created_at: SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionStartMode {
    Deferred,
    Immediate,
}

#[derive(Clone, Debug)]
struct TransactionOptions {
    access_mode: Option<TransactionAccessMode>,
    start_mode: Option<TransactionStartMode>,
}

#[derive(Clone, Debug)]
struct SqlTransactionCharacteristics {
    access_mode: TransactionAccessMode,
    start_mode: Option<TransactionStartMode>,
    locked: bool,
}

impl Default for SqlTransactionCharacteristics {
    fn default() -> Self {
        Self {
            access_mode: TransactionAccessMode::ReadWrite,
            start_mode: None,
            locked: false,
        }
    }
}

impl SqlTransactionCharacteristics {
    fn lock(&mut self) {
        self.locked = true;
    }

    fn is_read_only(&self) -> bool {
        self.access_mode == TransactionAccessMode::ReadOnly
    }

    fn apply_begin(&mut self, options: TransactionOptions) -> alopex_sql::executor::Result<()> {
        self.apply_options("BEGIN", options)
    }

    fn apply_start(&mut self, options: TransactionOptions) -> alopex_sql::executor::Result<()> {
        self.apply_options("START TRANSACTION", options)
    }

    fn apply_set(&mut self, options: TransactionOptions) -> alopex_sql::executor::Result<()> {
        self.apply_options("SET TRANSACTION", options)
    }

    fn apply_options(
        &mut self,
        operation: &'static str,
        options: TransactionOptions,
    ) -> alopex_sql::executor::Result<()> {
        if self.locked {
            return Err(ExecutorError::InvalidOperation {
                operation: operation.to_string(),
                reason:
                    "transaction characteristics can only be changed before the first statement"
                        .to_string(),
            });
        }
        if let Some(access_mode) = options.access_mode {
            self.access_mode = access_mode;
        }
        if let Some(start_mode) = options.start_mode {
            self.start_mode = Some(start_mode);
        }
        Ok(())
    }
}

#[derive(Debug)]
enum TxnControlStatement {
    Begin(TransactionOptions),
    Start(TransactionOptions),
    Set(TransactionOptions),
}

impl TxnHandle {
    pub fn new(txn: Box<dyn ErasedAsyncSqlTransaction>) -> Self {
        Self {
            inner: Arc::new(TxnHandleInner {
                txn: tokio::sync::Mutex::new(Some(txn)),
                sql_characteristics: tokio::sync::Mutex::new(
                    SqlTransactionCharacteristics::default(),
                ),
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
            self.execute_multi(sql)
                .await?
                .into_iter()
                .last()
                .ok_or_else(|| ExecutorError::InvalidOperation {
                    operation: "execute".into(),
                    reason: "empty SQL".into(),
                })
        })
    }

    pub fn execute_multi<'a>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, alopex_sql::executor::Result<Vec<ExecutionResult>>> {
        Box::pin(async move {
            if let Some(control) = parse_transaction_control(sql)? {
                let mut characteristics = self.inner.sql_characteristics.lock().await;
                match control {
                    TxnControlStatement::Begin(options) => characteristics.apply_begin(options)?,
                    TxnControlStatement::Start(options) => characteristics.apply_start(options)?,
                    TxnControlStatement::Set(options) => characteristics.apply_set(options)?,
                }
                return Ok(vec![ExecutionResult::Success]);
            }
            let parsed = Parser::parse_sql(&AlopexDialect, sql).ok();
            if let Some(statements) = parsed.as_ref() {
                let mut characteristics = self.inner.sql_characteristics.lock().await;
                if characteristics.is_read_only()
                    && statements
                        .iter()
                        .any(|statement| !statement.kind.is_query())
                {
                    return Err(ExecutorError::ReadOnlyTransaction {
                        operation: "mutating statement".to_string(),
                    });
                }
                characteristics.lock();
            }
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
            if parse_transaction_control(&sql).ok().flatten().is_some() {
                let _ = sender
                    .send(Err(ExecutorError::InvalidOperation {
                        operation: "query".into(),
                        reason: "transaction control statements cannot be streamed".into(),
                    }))
                    .await;
                return;
            }
            if let Ok(statements) = Parser::parse_sql(&AlopexDialect, &sql) {
                let mut characteristics = inner.sql_characteristics.lock().await;
                if characteristics.is_read_only()
                    && statements
                        .iter()
                        .any(|statement| !statement.kind.is_query())
                {
                    let _ = sender
                        .send(Err(ExecutorError::ReadOnlyTransaction {
                            operation: "mutating statement".to_string(),
                        }))
                        .await;
                    return;
                }
                characteristics.lock();
            }
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
            if parse_transaction_control(sql)?.is_some() {
                return Ok(Vec::new());
            }
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

fn parse_transaction_control(
    sql: &str,
) -> alopex_sql::executor::Result<Option<TxnControlStatement>> {
    let mut normalized = sql.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    while let Some(stripped) = normalized.strip_suffix(';') {
        normalized = stripped.trim_end();
    }
    if normalized.is_empty() || normalized.contains(';') {
        return Ok(None);
    }

    let mut tokens = normalized
        .replace(',', " ")
        .split_whitespace()
        .map(|token| token.to_uppercase())
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return Ok(None);
    }

    match tokens[0].as_str() {
        "BEGIN" => {
            tokens.remove(0);
            if tokens.first().is_some_and(|token| token == "TRANSACTION") {
                tokens.remove(0);
            }
            Ok(Some(TxnControlStatement::Begin(parse_transaction_options(
                &tokens, "BEGIN",
            )?)))
        }
        "START" => {
            if tokens.len() < 2 || tokens[1] != "TRANSACTION" {
                return Ok(None);
            }
            Ok(Some(TxnControlStatement::Start(parse_transaction_options(
                &tokens[2..],
                "START TRANSACTION",
            )?)))
        }
        "SET" => {
            if tokens.len() < 2 || tokens[1] != "TRANSACTION" {
                return Ok(None);
            }
            Ok(Some(TxnControlStatement::Set(parse_transaction_options(
                &tokens[2..],
                "SET TRANSACTION",
            )?)))
        }
        _ => Ok(None),
    }
}

fn parse_transaction_options(
    tokens: &[String],
    operation: &'static str,
) -> alopex_sql::executor::Result<TransactionOptions> {
    let mut options = TransactionOptions {
        access_mode: None,
        start_mode: None,
    };
    let mut index = 0usize;
    while index < tokens.len() {
        match tokens[index].as_str() {
            "READ" => {
                let modifier =
                    tokens
                        .get(index + 1)
                        .ok_or_else(|| ExecutorError::InvalidOperation {
                            operation: operation.to_string(),
                            reason: "READ must be followed by ONLY or WRITE".to_string(),
                        })?;
                options.access_mode = match modifier.as_str() {
                    "ONLY" => Some(TransactionAccessMode::ReadOnly),
                    "WRITE" => Some(TransactionAccessMode::ReadWrite),
                    _ => {
                        return Err(ExecutorError::InvalidOperation {
                            operation: operation.to_string(),
                            reason: "READ must be followed by ONLY or WRITE".to_string(),
                        });
                    }
                };
                index += 2;
            }
            "ISOLATION" => {
                if tokens.get(index + 1).map(|token| token.as_str()) != Some("LEVEL") {
                    return Err(ExecutorError::InvalidOperation {
                        operation: operation.to_string(),
                        reason: "ISOLATION must be followed by LEVEL".to_string(),
                    });
                }
                let level_first =
                    tokens
                        .get(index + 2)
                        .ok_or_else(|| ExecutorError::InvalidOperation {
                            operation: operation.to_string(),
                            reason: "missing isolation level".to_string(),
                        })?;
                let level_second = tokens.get(index + 3).map(|token| token.as_str());
                match (level_first.as_str(), level_second) {
                    ("REPEATABLE", Some("READ")) => {
                        index += 4;
                    }
                    ("READ", Some("UNCOMMITTED")) => {
                        return Err(ExecutorError::UnsupportedTransactionIsolationLevel {
                            level: "READ UNCOMMITTED".to_string(),
                        });
                    }
                    ("READ", Some("COMMITTED")) => {
                        return Err(ExecutorError::UnsupportedTransactionIsolationLevel {
                            level: "READ COMMITTED".to_string(),
                        });
                    }
                    ("SERIALIZABLE", _) => {
                        return Err(ExecutorError::UnsupportedTransactionIsolationLevel {
                            level: "SERIALIZABLE".to_string(),
                        });
                    }
                    (other, _) => {
                        return Err(ExecutorError::UnsupportedTransactionIsolationLevel {
                            level: other.to_string(),
                        });
                    }
                };
            }
            "DEFERRED" => {
                options.start_mode = Some(TransactionStartMode::Deferred);
                index += 1;
            }
            "IMMEDIATE" => {
                options.start_mode = Some(TransactionStartMode::Immediate);
                index += 1;
            }
            unknown => {
                return Err(ExecutorError::UnsupportedTransactionCharacteristic {
                    option: unknown.to_string(),
                });
            }
        }
    }

    Ok(options)
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
    use alopex_sql::executor::ExecutorError;

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

    #[test]
    fn parse_begin_and_set_transaction_characteristics() {
        let begin = parse_transaction_control("BEGIN").expect("parse begin");
        assert!(matches!(begin, Some(TxnControlStatement::Begin(_))));

        let set =
            parse_transaction_control("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                .expect("parse set");
        assert!(matches!(set, Some(TxnControlStatement::Set(_))));

        let start = parse_transaction_control("START TRANSACTION IMMEDIATE READ WRITE")
            .expect("parse start");
        assert!(matches!(start, Some(TxnControlStatement::Start(_))));
    }

    #[test]
    fn unsupported_isolation_levels_are_rejected_with_typed_error() {
        let err = parse_transaction_control("START TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .expect_err("unsupported level must fail");
        assert!(matches!(
            err,
            ExecutorError::UnsupportedTransactionIsolationLevel { .. }
        ));
    }

    #[test]
    fn characteristics_are_locked_after_first_statement() {
        let mut characteristics = SqlTransactionCharacteristics::default();
        characteristics
            .apply_set(TransactionOptions {
                access_mode: Some(TransactionAccessMode::ReadOnly),
                start_mode: None,
            })
            .expect("set before lock");
        assert!(characteristics.is_read_only());
        characteristics.lock();
        let err = characteristics
            .apply_set(TransactionOptions {
                access_mode: Some(TransactionAccessMode::ReadWrite),
                start_mode: None,
            })
            .expect_err("locked characteristics must fail");
        assert!(matches!(err, ExecutorError::InvalidOperation { .. }));
    }
}
