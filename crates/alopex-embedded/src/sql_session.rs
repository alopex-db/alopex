use std::sync::Arc;

use alopex_sql::{
    AlopexDialect, CommitMetadata, ExecutionResult, ExecutionStepError, ExecutionStepErrorKind,
    ExecutionStepKind, ExecutionStepOutcome, ExecutionStepResult, Parser, SharedExecutionReport,
    SharedExecutionRequest, StatementKind, TransactionAccessMode, TransactionIsolationLevel,
};

use crate::{Database, Error, OwnedEmbeddedTransaction, Result, SqlResult, TxnMode};

/// Observable lifecycle state for one SQL session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlSessionState {
    /// Statements execute in auto-commit transactions.
    Idle,
    /// An explicit read-write transaction is active.
    Active,
    /// A statement failed; only `ROLLBACK` may recover the session.
    Failed,
}

/// Effective characteristics for the current or next explicit SQL transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlTransactionCharacteristics {
    /// Engine mapping for the transaction's snapshot behavior.
    pub isolation_level: TransactionIsolationLevel,
    /// Whether SQL mutation statements are allowed.
    pub access_mode: TransactionAccessMode,
}

impl Default for SqlTransactionCharacteristics {
    fn default() -> Self {
        Self {
            isolation_level: TransactionIsolationLevel::RepeatableRead,
            access_mode: TransactionAccessMode::ReadWrite,
        }
    }
}

/// A SQL session that owns at most one explicit embedded transaction.
pub struct SqlSession {
    database: Arc<Database>,
    transaction: Option<OwnedEmbeddedTransaction>,
    state: SqlSessionState,
    characteristics: SqlTransactionCharacteristics,
    characteristics_locked: bool,
}

impl Database {
    /// Create a SQL session whose explicit transactions use this database.
    pub fn sql_session(self: &Arc<Self>) -> SqlSession {
        SqlSession {
            database: Arc::clone(self),
            transaction: None,
            state: SqlSessionState::Idle,
            characteristics: SqlTransactionCharacteristics::default(),
            characteristics_locked: false,
        }
    }
}

impl SqlSession {
    /// Return the current lifecycle state.
    pub fn state(&self) -> SqlSessionState {
        self.state
    }

    /// Return the effective explicit-transaction characteristics.
    pub fn transaction_characteristics(&self) -> SqlTransactionCharacteristics {
        self.characteristics
    }

    /// Execute ordered transaction, commit-barrier, and post-commit-read steps.
    ///
    /// The first transaction statement opens a transaction when the session is
    /// idle. Execution stops at the first error. A failed commit suppresses all
    /// later reads, while a post-commit read error preserves the successful
    /// commit result already present in the report.
    pub fn execute_shared(&mut self, request: SharedExecutionRequest) -> SharedExecutionReport {
        let SharedExecutionRequest {
            execution_id,
            transaction_id,
            steps,
        } = request;
        let mut committed = false;
        let mut results = Vec::with_capacity(steps.len());

        for (step_index, step) in steps.into_iter().enumerate() {
            let outcome = match step.kind {
                ExecutionStepKind::TransactionStatement { .. } if committed => Self::step_error(
                    ExecutionStepErrorKind::InvalidOrder,
                    "transaction statement follows the commit barrier",
                ),
                ExecutionStepKind::TransactionStatement { sql } => {
                    let result = if self.state == SqlSessionState::Idle {
                        self.begin(None, None).and_then(|_| self.execute_sql(&sql))
                    } else {
                        self.execute_sql(&sql)
                    };
                    match result {
                        Ok(result) => ExecutionStepOutcome::Execution(result),
                        Err(error) => Self::step_error(ExecutionStepErrorKind::Transaction, error),
                    }
                }
                ExecutionStepKind::CommitBarrier if committed => Self::step_error(
                    ExecutionStepErrorKind::InvalidOrder,
                    "commit barrier follows a successful commit barrier",
                ),
                ExecutionStepKind::CommitBarrier => match self.commit() {
                    Ok(_) => {
                        committed = true;
                        ExecutionStepOutcome::Commit(CommitMetadata {
                            transaction_id: transaction_id.clone(),
                        })
                    }
                    Err(error) => Self::step_error(ExecutionStepErrorKind::Commit, error),
                },
                ExecutionStepKind::PostCommitRead { .. } if !committed => Self::step_error(
                    ExecutionStepErrorKind::InvalidOrder,
                    "post-commit read precedes a successful commit barrier",
                ),
                ExecutionStepKind::PostCommitRead { sql } => {
                    match self.execute_post_commit_read(&sql) {
                        Ok(result) => ExecutionStepOutcome::Execution(result),
                        Err(error) => {
                            Self::step_error(ExecutionStepErrorKind::PostCommitRead, error)
                        }
                    }
                }
            };
            let failed = matches!(outcome, ExecutionStepOutcome::Error(_));
            results.push(ExecutionStepResult {
                execution_id: execution_id.clone(),
                transaction_id: transaction_id.clone(),
                step_id: step.step_id,
                step_index,
                outcome,
            });
            if failed {
                break;
            }
        }

        SharedExecutionReport {
            execution_id,
            transaction_id,
            steps: results,
        }
    }

    /// Execute exactly one SQL statement in this session.
    pub fn execute_sql(&mut self, sql: &str) -> Result<SqlResult> {
        let statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(alopex_sql::SqlError::from)?;
        if statements.len() != 1 {
            return Err(Error::SqlSessionRequiresSingleStatement);
        }

        match &statements[0].kind {
            StatementKind::Begin {
                isolation_level,
                access_mode,
            } => self.begin(*isolation_level, *access_mode),
            StatementKind::SetTransaction {
                isolation_level,
                access_mode,
            } => self.set_transaction(*isolation_level, *access_mode),
            StatementKind::Commit => self.commit(),
            StatementKind::Rollback => self.rollback(),
            StatementKind::Savepoint { name } => self.savepoint(name),
            StatementKind::RollbackToSavepoint { name } => self.rollback_to_savepoint(name),
            StatementKind::ReleaseSavepoint { name } => self.release_savepoint(name),
            kind => self.execute_statement(sql, kind),
        }
    }

    fn begin(
        &mut self,
        isolation_level: Option<TransactionIsolationLevel>,
        access_mode: Option<TransactionAccessMode>,
    ) -> Result<SqlResult> {
        if self.state != SqlSessionState::Idle {
            return Err(self.invalid("BEGIN"));
        }
        Self::validate_isolation(isolation_level)?;
        let characteristics = SqlTransactionCharacteristics {
            isolation_level: isolation_level.unwrap_or(TransactionIsolationLevel::RepeatableRead),
            access_mode: access_mode.unwrap_or(TransactionAccessMode::ReadWrite),
        };
        let mode = match characteristics.access_mode {
            TransactionAccessMode::ReadOnly => TxnMode::ReadOnly,
            TransactionAccessMode::ReadWrite => TxnMode::ReadWrite,
        };
        let transaction = Arc::clone(&self.database).begin_owned_embedded_transaction(mode)?;
        self.transaction = Some(transaction);
        self.characteristics = characteristics;
        self.characteristics_locked = isolation_level.is_some() || access_mode.is_some();
        self.state = SqlSessionState::Active;
        Ok(ExecutionResult::Success)
    }

    fn set_transaction(
        &mut self,
        isolation_level: Option<TransactionIsolationLevel>,
        access_mode: Option<TransactionAccessMode>,
    ) -> Result<SqlResult> {
        if self.state != SqlSessionState::Active {
            return Err(self.invalid("SET TRANSACTION"));
        }
        if self.characteristics_locked {
            return Err(Error::SqlTransactionCharacteristicsLocked);
        }
        Self::validate_isolation(isolation_level)?;
        if let Some(isolation_level) = isolation_level {
            self.characteristics.isolation_level = isolation_level;
        }
        if let Some(access_mode) = access_mode {
            self.characteristics.access_mode = access_mode;
        }
        self.characteristics_locked = true;
        Ok(ExecutionResult::Success)
    }

    fn commit(&mut self) -> Result<SqlResult> {
        if self.state != SqlSessionState::Active {
            return Err(self.invalid("COMMIT"));
        }
        let result = self
            .transaction
            .as_mut()
            .expect("active SQL session owns a transaction")
            .commit();
        if result.is_ok() {
            self.transaction = None;
            self.state = SqlSessionState::Idle;
            self.reset_characteristics();
        } else {
            self.state = SqlSessionState::Failed;
        }
        result.map(|()| ExecutionResult::Success)
    }

    fn rollback(&mut self) -> Result<SqlResult> {
        if !matches!(
            self.state,
            SqlSessionState::Active | SqlSessionState::Failed
        ) {
            return Err(self.invalid("ROLLBACK"));
        }
        let result = self
            .transaction
            .as_mut()
            .expect("non-idle SQL session owns a transaction")
            .rollback();
        self.transaction = None;
        self.state = SqlSessionState::Idle;
        self.reset_characteristics();
        result.map(|()| ExecutionResult::Success)
    }

    fn savepoint(&mut self, name: &str) -> Result<SqlResult> {
        if self.state != SqlSessionState::Active {
            return Err(self.invalid("SAVEPOINT"));
        }
        self.characteristics_locked = true;
        self.transaction
            .as_mut()
            .expect("active SQL session owns a transaction")
            .create_savepoint(name)?;
        Ok(ExecutionResult::Success)
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<SqlResult> {
        if !matches!(
            self.state,
            SqlSessionState::Active | SqlSessionState::Failed
        ) {
            return Err(self.invalid("ROLLBACK TO SAVEPOINT"));
        }
        self.characteristics_locked = true;
        self.transaction
            .as_mut()
            .expect("non-idle SQL session owns a transaction")
            .rollback_to_savepoint(name)?;
        self.state = SqlSessionState::Active;
        Ok(ExecutionResult::Success)
    }

    fn release_savepoint(&mut self, name: &str) -> Result<SqlResult> {
        if self.state != SqlSessionState::Active {
            return Err(self.invalid("RELEASE SAVEPOINT"));
        }
        self.characteristics_locked = true;
        self.transaction
            .as_mut()
            .expect("active SQL session owns a transaction")
            .release_savepoint(name)?;
        Ok(ExecutionResult::Success)
    }

    fn execute_statement(&mut self, sql: &str, kind: &StatementKind) -> Result<SqlResult> {
        match self.state {
            SqlSessionState::Idle => self.database.execute_sql(sql),
            SqlSessionState::Active => {
                self.characteristics_locked = true;
                if self.characteristics.access_mode == TransactionAccessMode::ReadOnly
                    && !kind.is_query()
                {
                    return Err(Error::TxnReadOnly);
                }
                let result = self
                    .transaction
                    .as_mut()
                    .expect("active SQL session owns a transaction")
                    .execute_sql(sql);
                if result.is_err() {
                    self.state = SqlSessionState::Failed;
                }
                result
            }
            SqlSessionState::Failed => Err(self.invalid("statement")),
        }
    }

    fn invalid(&self, statement: &'static str) -> Error {
        Error::InvalidSqlTransactionTransition {
            statement,
            state: self.state,
        }
    }

    fn step_error(
        kind: ExecutionStepErrorKind,
        error: impl std::fmt::Display,
    ) -> ExecutionStepOutcome {
        ExecutionStepOutcome::Error(ExecutionStepError {
            kind,
            message: error.to_string(),
        })
    }

    fn execute_post_commit_read(&self, sql: &str) -> Result<SqlResult> {
        let statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(alopex_sql::SqlError::from)?;
        if statements.len() != 1 {
            return Err(Error::SqlSessionRequiresSingleStatement);
        }
        if !statements[0].kind.is_query() {
            return Err(Error::PostCommitReadRequiresQuery);
        }
        self.database
            .execute_sql_multi(sql)?
            .pop()
            .ok_or(Error::SqlSessionRequiresSingleStatement)
    }

    fn validate_isolation(level: Option<TransactionIsolationLevel>) -> Result<()> {
        if level.is_some_and(|level| level != TransactionIsolationLevel::RepeatableRead) {
            return Err(Error::UnsupportedSqlTransactionIsolation(
                level.expect("checked as some"),
            ));
        }
        Ok(())
    }

    fn reset_characteristics(&mut self) {
        self.characteristics = SqlTransactionCharacteristics::default();
        self.characteristics_locked = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_sql::{
        ExecutionStep, ExecutionStepErrorKind, ExecutionStepKind, ExecutionStepOutcome,
        SharedExecutionRequest,
    };

    #[test]
    fn shared_execution_orders_and_correlates_mutation_commit_and_fresh_read() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = database.sql_session();

        let report = session.execute_shared(SharedExecutionRequest::new(
            "execution-1",
            "transaction-1",
            vec![
                ExecutionStep::new(
                    "mutation-1",
                    ExecutionStepKind::TransactionStatement {
                        sql: "INSERT INTO items (id) VALUES (1)".into(),
                    },
                ),
                ExecutionStep::new("commit-1", ExecutionStepKind::CommitBarrier),
                ExecutionStep::new(
                    "read-1",
                    ExecutionStepKind::PostCommitRead {
                        sql: "SELECT id FROM items".into(),
                    },
                ),
            ],
        ));

        assert_eq!(report.execution_id, "execution-1");
        assert_eq!(report.transaction_id, "transaction-1");
        assert_eq!(
            report
                .steps
                .iter()
                .map(|step| (step.step_index, step.step_id.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "mutation-1"), (1, "commit-1"), (2, "read-1")]
        );
        assert!(matches!(
            report.steps[0].outcome,
            ExecutionStepOutcome::Execution(ExecutionResult::RowsAffected(1))
        ));
        assert!(matches!(
            report.steps[1].outcome,
            ExecutionStepOutcome::Commit(ref metadata)
                if metadata.transaction_id == "transaction-1"
        ));
        let ExecutionStepOutcome::Execution(ExecutionResult::Query(ref rows)) =
            report.steps[2].outcome
        else {
            panic!("post-commit read must return a query result")
        };
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(session.state(), SqlSessionState::Idle);
    }

    #[test]
    fn shared_execution_stops_before_post_read_when_commit_fails() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut losing_session = database.sql_session();
        let mut winning_session = database.sql_session();
        losing_session.execute_sql("BEGIN").unwrap();
        winning_session.execute_sql("BEGIN").unwrap();
        losing_session
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        winning_session
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        winning_session.execute_sql("COMMIT").unwrap();

        let report = losing_session.execute_shared(SharedExecutionRequest::new(
            "execution-conflict",
            "transaction-conflict",
            vec![
                ExecutionStep::new(
                    "mutation-2",
                    ExecutionStepKind::TransactionStatement {
                        sql: "INSERT INTO items (id) VALUES (2)".into(),
                    },
                ),
                ExecutionStep::new("commit-conflict", ExecutionStepKind::CommitBarrier),
                ExecutionStep::new(
                    "read-must-not-run",
                    ExecutionStepKind::PostCommitRead {
                        sql: "SELECT id FROM items".into(),
                    },
                ),
            ],
        ));

        assert_eq!(report.steps.len(), 2);
        assert!(matches!(
            report.steps[1].outcome,
            ExecutionStepOutcome::Error(ref error)
                if error.kind == ExecutionStepErrorKind::Commit
        ));
        assert!(report
            .steps
            .iter()
            .all(|step| step.step_id != "read-must-not-run"));
        assert_eq!(losing_session.state(), SqlSessionState::Failed);
    }

    #[test]
    fn shared_execution_keeps_commit_success_when_post_read_fails() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = database.sql_session();

        let report = session.execute_shared(SharedExecutionRequest::new(
            "execution-read-error",
            "transaction-read-error",
            vec![
                ExecutionStep::new(
                    "mutation",
                    ExecutionStepKind::TransactionStatement {
                        sql: "INSERT INTO items (id) VALUES (1)".into(),
                    },
                ),
                ExecutionStep::new("commit", ExecutionStepKind::CommitBarrier),
                ExecutionStep::new(
                    "read-error",
                    ExecutionStepKind::PostCommitRead {
                        sql: "SELECT id FROM missing".into(),
                    },
                ),
            ],
        ));

        assert_eq!(report.steps.len(), 3);
        assert!(matches!(
            report.steps[1].outcome,
            ExecutionStepOutcome::Commit(_)
        ));
        assert!(matches!(
            report.steps[2].outcome,
            ExecutionStepOutcome::Error(ref error)
                if error.kind == ExecutionStepErrorKind::PostCommitRead
        ));
        let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(
            rows.rows.len(),
            1,
            "the successful commit must stay visible"
        );
        assert_eq!(session.state(), SqlSessionState::Idle);
    }

    #[test]
    fn shared_execution_rejects_mutation_in_post_commit_read_context() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = database.sql_session();

        let report = session.execute_shared(SharedExecutionRequest::new(
            "execution-read-only",
            "transaction-read-only",
            vec![
                ExecutionStep::new(
                    "mutation",
                    ExecutionStepKind::TransactionStatement {
                        sql: "INSERT INTO items (id) VALUES (1)".into(),
                    },
                ),
                ExecutionStep::new("commit", ExecutionStepKind::CommitBarrier),
                ExecutionStep::new(
                    "not-a-read",
                    ExecutionStepKind::PostCommitRead {
                        sql: "INSERT INTO items (id) VALUES (2)".into(),
                    },
                ),
            ],
        ));

        assert!(matches!(
            report.steps[2].outcome,
            ExecutionStepOutcome::Error(ref error)
                if error.kind == ExecutionStepErrorKind::PostCommitRead
        ));
        let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(rows.rows.len(), 1, "post-commit reads must not mutate");
    }

    #[test]
    fn explicit_transaction_controls_visibility_and_returns_to_idle() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = database.sql_session();

        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        let ExecutionResult::Query(outside) = database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert!(outside.rows.is_empty());

        session.execute_sql("COMMIT").unwrap();
        assert_eq!(session.state(), SqlSessionState::Idle);
        let ExecutionResult::Query(committed) =
            database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(committed.rows.len(), 1);
    }

    #[test]
    fn invalid_transition_is_typed_and_statement_failure_requires_rollback() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = database.sql_session();

        assert!(matches!(
            session.execute_sql("COMMIT"),
            Err(Error::InvalidSqlTransactionTransition {
                state: SqlSessionState::Idle,
                ..
            })
        ));
        session.execute_sql("START TRANSACTION").unwrap();
        session
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        assert!(session
            .execute_sql("INSERT INTO missing (id) VALUES (2)")
            .is_err());
        assert_eq!(session.state(), SqlSessionState::Failed);
        assert!(session.execute_sql("COMMIT").is_err());
        session.execute_sql("ROLLBACK").unwrap();
        assert_eq!(session.state(), SqlSessionState::Idle);

        let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert!(rows.rows.is_empty());
    }

    #[test]
    fn owned_transaction_cannot_commit_after_a_statement_failure() {
        let database = Arc::new(Database::new());
        let mut transaction = Arc::clone(&database)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();

        assert!(transaction.execute_sql("SELECT * FROM missing").is_err());
        assert!(matches!(transaction.commit(), Err(Error::TxnFailed)));
        transaction.rollback().unwrap();
    }

    #[test]
    fn control_transition_table_is_deterministic() {
        let cases = [
            (
                SqlSessionState::Idle,
                "BEGIN",
                true,
                SqlSessionState::Active,
            ),
            (
                SqlSessionState::Idle,
                "COMMIT",
                false,
                SqlSessionState::Idle,
            ),
            (
                SqlSessionState::Idle,
                "ROLLBACK",
                false,
                SqlSessionState::Idle,
            ),
            (
                SqlSessionState::Active,
                "BEGIN",
                false,
                SqlSessionState::Active,
            ),
            (
                SqlSessionState::Active,
                "COMMIT",
                true,
                SqlSessionState::Idle,
            ),
            (
                SqlSessionState::Active,
                "ROLLBACK",
                true,
                SqlSessionState::Idle,
            ),
        ];

        for (initial, statement, succeeds, expected) in cases {
            let database = Arc::new(Database::new());
            let mut session = database.sql_session();
            if initial == SqlSessionState::Active {
                session.execute_sql("BEGIN").unwrap();
            }
            assert_eq!(
                session.execute_sql(statement).is_ok(),
                succeeds,
                "{initial:?} + {statement}"
            );
            assert_eq!(session.state(), expected, "{initial:?} + {statement}");
        }
    }

    #[test]
    fn transaction_local_ddl_and_reads_are_isolated_until_commit() {
        let database = Arc::new(Database::new());
        let mut session = database.sql_session();
        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql("CREATE TABLE staged (id INTEGER PRIMARY KEY)")
            .unwrap();
        session
            .execute_sql("INSERT INTO staged (id) VALUES (1)")
            .unwrap();
        let ExecutionResult::Query(inside) = session.execute_sql("SELECT id FROM staged").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(inside.rows.len(), 1);
        assert!(database.execute_sql("SELECT id FROM staged").is_err());

        session.execute_sql("ROLLBACK").unwrap();
        assert!(database.execute_sql("SELECT id FROM staged").is_err());
    }

    #[test]
    fn autocommit_failure_leaves_the_session_idle_and_reusable() {
        let database = Arc::new(Database::new());
        let mut session = database.sql_session();
        assert!(session.execute_sql("SELECT * FROM missing").is_err());
        assert_eq!(session.state(), SqlSessionState::Idle);
        assert!(session.execute_sql("SELECT 1").is_ok());
    }

    #[test]
    fn terminal_commit_failure_does_not_wedge_the_session() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut first = database.sql_session();
        let mut second = database.sql_session();
        first.execute_sql("BEGIN").unwrap();
        second.execute_sql("BEGIN").unwrap();
        first
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        second
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        first.execute_sql("COMMIT").unwrap();

        assert!(second.execute_sql("COMMIT").is_err());
        assert_eq!(second.state(), SqlSessionState::Failed);
        assert!(second.execute_sql("ROLLBACK").is_err());
        assert_eq!(second.state(), SqlSessionState::Idle);
        assert!(second.execute_sql("SELECT 1").is_ok());
    }

    #[test]
    fn nested_duplicate_savepoints_use_the_latest_name_and_keep_rollback_target() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = database.sql_session();
        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        session.execute_sql("SAVEPOINT Retry").unwrap();
        session
            .execute_sql("INSERT INTO items (id) VALUES (2)")
            .unwrap();
        session.execute_sql("SAVEPOINT retry").unwrap();
        session
            .execute_sql("INSERT INTO items (id) VALUES (3)")
            .unwrap();

        session.execute_sql("ROLLBACK TO SAVEPOINT RETRY").unwrap();
        session.execute_sql("RELEASE SAVEPOINT retry").unwrap();
        session.execute_sql("ROLLBACK TO SAVEPOINT Retry").unwrap();
        session.execute_sql("COMMIT").unwrap();

        let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(rows.rows.len(), 1);
    }

    #[test]
    fn releasing_a_savepoint_discards_descendants_and_missing_names_are_typed() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = database.sql_session();
        session.execute_sql("BEGIN").unwrap();
        session.execute_sql("SAVEPOINT outer_point").unwrap();
        session
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        session.execute_sql("SAVEPOINT inner_point").unwrap();
        session
            .execute_sql("INSERT INTO items (id) VALUES (2)")
            .unwrap();
        session
            .execute_sql("RELEASE SAVEPOINT outer_point")
            .unwrap();

        assert!(matches!(
            session.execute_sql("ROLLBACK TO SAVEPOINT inner_point"),
            Err(Error::SavepointNotFound(name)) if name == "inner_point"
        ));
        assert_eq!(session.state(), SqlSessionState::Active);
        session.execute_sql("COMMIT").unwrap();
        let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(rows.rows.len(), 2);
    }

    #[test]
    fn rollback_to_savepoint_recovers_failed_state_and_catalog_overlay() {
        let database = Arc::new(Database::new());
        let mut session = database.sql_session();
        assert!(matches!(
            session.execute_sql("SAVEPOINT idle_point"),
            Err(Error::InvalidSqlTransactionTransition {
                state: SqlSessionState::Idle,
                ..
            })
        ));
        session.execute_sql("BEGIN").unwrap();
        session.execute_sql("SAVEPOINT safe").unwrap();
        session
            .execute_sql("CREATE TABLE staged (id INTEGER PRIMARY KEY)")
            .unwrap();
        session
            .execute_sql("INSERT INTO staged (id) VALUES (1)")
            .unwrap();
        assert!(session.execute_sql("SELECT * FROM missing").is_err());
        assert_eq!(session.state(), SqlSessionState::Failed);
        assert!(matches!(
            session.execute_sql("ROLLBACK TO SAVEPOINT absent"),
            Err(Error::SavepointNotFound(name)) if name == "absent"
        ));
        assert_eq!(session.state(), SqlSessionState::Failed);

        session.execute_sql("ROLLBACK TO SAVEPOINT safe").unwrap();
        assert_eq!(session.state(), SqlSessionState::Active);
        session.execute_sql("COMMIT").unwrap();
        assert!(database.execute_sql("SELECT id FROM staged").is_err());
    }

    #[test]
    fn repeatable_read_only_keeps_its_start_snapshot_and_rejects_mutation() {
        let database = Arc::new(Database::new());
        database
            .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
        database
            .execute_sql("INSERT INTO items (id) VALUES (1)")
            .unwrap();
        let mut session = database.sql_session();
        session
            .execute_sql("START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .unwrap();

        let ExecutionResult::Query(before) = session.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(before.rows.len(), 1);
        database
            .execute_sql("INSERT INTO items (id) VALUES (2)")
            .unwrap();
        let ExecutionResult::Query(after) = session.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(after.rows.len(), 1);
        assert!(matches!(
            session.execute_sql("INSERT INTO items (id) VALUES (3)"),
            Err(Error::TxnReadOnly)
        ));
        assert_eq!(session.state(), SqlSessionState::Active);
        session.execute_sql("COMMIT").unwrap();

        let ExecutionResult::Query(committed) =
            database.execute_sql("SELECT id FROM items").unwrap()
        else {
            panic!("SELECT must return rows")
        };
        assert_eq!(committed.rows.len(), 2);
    }

    #[test]
    fn set_transaction_is_allowed_only_before_transactional_work() {
        let database = Arc::new(Database::new());
        let mut session = database.sql_session();
        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .unwrap();
        assert_eq!(
            session.transaction_characteristics(),
            SqlTransactionCharacteristics {
                isolation_level: TransactionIsolationLevel::RepeatableRead,
                access_mode: TransactionAccessMode::ReadOnly,
            }
        );
        assert!(matches!(
            session.execute_sql("CREATE TABLE blocked (id INTEGER)"),
            Err(Error::TxnReadOnly)
        ));
        assert!(matches!(
            session.execute_sql("SET TRANSACTION READ WRITE"),
            Err(Error::SqlTransactionCharacteristicsLocked)
        ));
        session.execute_sql("ROLLBACK").unwrap();
        assert_eq!(
            session.transaction_characteristics(),
            SqlTransactionCharacteristics::default()
        );

        session.execute_sql("BEGIN").unwrap();
        session.execute_sql("SELECT 1").unwrap();
        assert!(matches!(
            session.execute_sql("SET TRANSACTION READ ONLY"),
            Err(Error::SqlTransactionCharacteristicsLocked)
        ));
        session.execute_sql("ROLLBACK").unwrap();
    }

    #[test]
    fn unsupported_isolation_is_typed_and_does_not_start_a_transaction() {
        let database = Arc::new(Database::new());
        let mut session = database.sql_session();
        assert!(matches!(
            session.execute_sql("START TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
            Err(Error::UnsupportedSqlTransactionIsolation(
                TransactionIsolationLevel::Serializable
            ))
        ));
        assert_eq!(session.state(), SqlSessionState::Idle);
        assert!(session.execute_sql("SELECT 1").is_ok());
    }
}
