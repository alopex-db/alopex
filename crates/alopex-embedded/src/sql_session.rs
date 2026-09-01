use std::sync::Arc;

use alopex_sql::{AlopexDialect, ExecutionResult, Parser, StatementKind};

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

/// A SQL session that owns at most one explicit embedded transaction.
pub struct SqlSession {
    database: Arc<Database>,
    transaction: Option<OwnedEmbeddedTransaction>,
    state: SqlSessionState,
}

impl Database {
    /// Create a SQL session whose explicit transactions use this database.
    pub fn sql_session(self: &Arc<Self>) -> SqlSession {
        SqlSession {
            database: Arc::clone(self),
            transaction: None,
            state: SqlSessionState::Idle,
        }
    }
}

impl SqlSession {
    /// Return the current lifecycle state.
    pub fn state(&self) -> SqlSessionState {
        self.state
    }

    /// Execute exactly one SQL statement in this session.
    pub fn execute_sql(&mut self, sql: &str) -> Result<SqlResult> {
        let statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(alopex_sql::SqlError::from)?;
        if statements.len() != 1 {
            return Err(Error::SqlSessionRequiresSingleStatement);
        }

        match &statements[0].kind {
            StatementKind::Begin => self.begin(),
            StatementKind::Commit => self.commit(),
            StatementKind::Rollback => self.rollback(),
            StatementKind::Savepoint { name } => self.savepoint(name),
            StatementKind::RollbackToSavepoint { name } => self.rollback_to_savepoint(name),
            StatementKind::ReleaseSavepoint { name } => self.release_savepoint(name),
            _ => self.execute_statement(sql),
        }
    }

    fn begin(&mut self) -> Result<SqlResult> {
        if self.state != SqlSessionState::Idle {
            return Err(self.invalid("BEGIN"));
        }
        self.transaction =
            Some(Arc::clone(&self.database).begin_owned_embedded_transaction(TxnMode::ReadWrite)?);
        self.state = SqlSessionState::Active;
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
        result.map(|()| ExecutionResult::Success)
    }

    fn savepoint(&mut self, name: &str) -> Result<SqlResult> {
        if self.state != SqlSessionState::Active {
            return Err(self.invalid("SAVEPOINT"));
        }
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
        self.transaction
            .as_mut()
            .expect("active SQL session owns a transaction")
            .release_savepoint(name)?;
        Ok(ExecutionResult::Success)
    }

    fn execute_statement(&mut self, sql: &str) -> Result<SqlResult> {
        match self.state {
            SqlSessionState::Idle => self.database.execute_sql(sql),
            SqlSessionState::Active => {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
