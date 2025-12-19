use alopex_core::kv::KVStore;
use alopex_core::types::TxnMode;
use alopex_core::KVTransaction;
use alopex_sql::catalog::CatalogOverlay;
use alopex_sql::catalog::TxnCatalogView;
use alopex_sql::executor::Executor;
use alopex_sql::storage::TxnBridge;
use alopex_sql::AlopexDialect;
use alopex_sql::Parser;
use alopex_sql::Planner;
use alopex_sql::Statement;
use alopex_sql::StatementKind;

use crate::Database;
use crate::Error;
use crate::Result;
use crate::SqlResult;
use crate::Transaction;

fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = AlopexDialect;
    Parser::parse_sql(&dialect, sql).map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))
}

fn stmt_requires_write(stmt: &Statement) -> bool {
    !matches!(stmt.kind, StatementKind::Select(_))
}

fn plan_stmt<'a, S: KVStore>(
    catalog: &'a alopex_sql::catalog::PersistentCatalog<S>,
    overlay: &'a CatalogOverlay,
    stmt: &Statement,
) -> Result<alopex_sql::LogicalPlan> {
    let view = TxnCatalogView::new(catalog, overlay);
    let planner = Planner::new(&view);
    planner
        .plan(stmt)
        .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))
}

impl Database {
    /// SQL を実行する（auto-commit）。
    ///
    /// - DDL/DML は ReadWrite トランザクションで実行し、成功時に自動コミットする。
    /// - SELECT は ReadOnly トランザクションで実行する。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::Database;
    /// use alopex_sql::ExecutionResult;
    ///
    /// let db = Database::new();
    /// let result = db.execute_sql(
    ///     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
    /// ).unwrap();
    /// assert!(matches!(result, ExecutionResult::Success));
    /// ```
    pub fn execute_sql(&self, sql: &str) -> Result<SqlResult> {
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(alopex_sql::ExecutionResult::Success);
        }

        let mode = if stmts.iter().any(stmt_requires_write) {
            TxnMode::ReadWrite
        } else {
            TxnMode::ReadOnly
        };

        let mut txn = self.store.begin(mode).map_err(Error::Core)?;
        let mut overlay = CatalogOverlay::new();
        let mut borrowed =
            TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut txn, mode, &mut overlay);

        let mut executor: Executor<_, _> =
            Executor::new(self.store.clone(), self.sql_catalog.clone());

        let mut last = alopex_sql::ExecutionResult::Success;
        for stmt in &stmts {
            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt)?
            };

            last = executor
                .execute_in_txn(plan, &mut borrowed)
                .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;
        }

        drop(borrowed);

        // `execute_in_txn()` 成功時に HNSW flush 済み（失敗時は abandon 済み）なので、
        // ここでは KV commit と overlay 適用のみを行う。
        //
        // commit_self は `txn` を消費するため、失敗時に rollback はできない。
        txn.commit_self().map_err(Error::Core)?;
        if mode == TxnMode::ReadWrite {
            let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");
            catalog.apply_overlay(overlay);
        }
        Ok(last)
    }
}

impl<'a> Transaction<'a> {
    /// SQL を実行する（外部トランザクション利用）。
    ///
    /// 同一トランザクション内の複数回呼び出しでカタログ変更が見えるよう、`CatalogOverlay` は
    /// `Transaction` が所有して保持する。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::{Database, TxnMode};
    ///
    /// let db = Database::new();
    /// let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    /// txn.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);").unwrap();
    /// txn.execute_sql("INSERT INTO t (id) VALUES (1);").unwrap();
    /// txn.commit().unwrap();
    /// ```
    pub fn execute_sql(&mut self, sql: &str) -> Result<SqlResult> {
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(alopex_sql::ExecutionResult::Success);
        }

        let store = self.db.store.clone();
        let sql_catalog = self.db.sql_catalog.clone();

        let txn = self.inner.as_mut().ok_or(Error::TxnCompleted)?;
        let mode = txn.mode();

        let mut borrowed =
            TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(txn, mode, &mut self.overlay);
        let mut executor: Executor<_, _> = Executor::new(store, sql_catalog.clone());

        let mut last = alopex_sql::ExecutionResult::Success;
        for stmt in &stmts {
            let plan = {
                let catalog = sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt)?
            };

            last = executor
                .execute_in_txn(plan, &mut borrowed)
                .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;
        }

        Ok(last)
    }
}
