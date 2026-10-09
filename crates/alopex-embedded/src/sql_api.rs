use alopex_core::kv::RangeChangeJournalCapability;
use alopex_core::kv::{any::AnyKVTransaction, KVStore, OwnedKVTransactionAdapter, ReadAtPoint};
use alopex_core::types::TxnMode;
use alopex_core::KVTransaction;
use alopex_sql::catalog::TxnCatalogView;
use alopex_sql::catalog::{Catalog, CatalogOverlay};
use alopex_sql::executor::query::execute_query_streaming;
use alopex_sql::executor::query::iterator::VecIterator;
use alopex_sql::executor::query::RowIterator;
use alopex_sql::executor::{
    build_streaming_pipeline, ColumnInfo, ExecutionResult, Executor, QueryRowIterator, Row,
};
use alopex_sql::planner::typed_expr::Projection;
use alopex_sql::storage::{
    LocalRangeChangeJournal, RangeChangeJournalScope, SqlTxn, SqlValue, TxnBridge,
};
use alopex_sql::AlopexDialect;
use alopex_sql::Parser;
use alopex_sql::Planner;
use alopex_sql::Statement;
use alopex_sql::StatementKind;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::Database;
use crate::Error;
use crate::OwnedEmbeddedTransaction;
use crate::Result;
use crate::SqlResult;
use crate::Transaction;

/// Streaming row access for FR-7 compliance.
///
/// This struct provides access to query results in a streaming fashion,
/// where the transaction is kept alive for the duration of row iteration.
/// The lifetime `'a` is tied to the transaction scope.
pub struct StreamingRows<'a> {
    columns: Vec<ColumnInfo>,
    iter: Box<dyn RowIterator + 'a>,
    projection: Projection,
    schema: Vec<alopex_sql::catalog::ColumnMetadata>,
}

impl<'a> StreamingRows<'a> {
    /// Get column information for the query result.
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Fetch the next row, returning `None` when exhausted.
    ///
    /// Rows are fetched on-demand from storage, enabling true streaming.
    pub fn next_row(&mut self) -> Result<Option<Vec<SqlValue>>> {
        match self.iter.next_row() {
            Some(result) => {
                let row = result.map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;
                let projected = self.project_row(&row)?;
                Ok(Some(projected))
            }
            None => Ok(None),
        }
    }

    /// Apply projection to a row.
    fn project_row(&self, row: &Row) -> Result<Vec<SqlValue>> {
        match &self.projection {
            Projection::All(names) => {
                // Return values in the order specified by names
                let mut result = Vec::with_capacity(names.len());
                for name in names {
                    let idx = self
                        .schema
                        .iter()
                        .position(|c| &c.name == name)
                        .ok_or_else(|| {
                            Error::Sql(alopex_sql::SqlError::Execution {
                                message: format!("column not found: {}", name),
                                code: "ALOPEX-E020",
                            })
                        })?;
                    result.push(row.values.get(idx).cloned().unwrap_or(SqlValue::Null));
                }
                Ok(result)
            }
            Projection::Columns(cols) => {
                use alopex_sql::executor::evaluator::{evaluate, EvalContext};
                let ctx = EvalContext::new(&row.values);
                let mut result = Vec::with_capacity(cols.len());
                for col in cols {
                    let value = evaluate(&col.expr, &ctx)
                        .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;
                    result.push(value);
                }
                Ok(result)
            }
        }
    }
}

/// Result type for callback-based streaming query.
pub enum StreamingQueryResult<R> {
    /// DDL operation success.
    Success,
    /// DML operation with affected row count.
    RowsAffected(u64),
    /// Query result processed by callback.
    QueryProcessed(R),
}

/// Streaming SQL execution result for FR-7 compliance.
///
/// This enum enables true streaming output for SELECT queries by returning
/// an iterator instead of a materialized Vec.
pub enum SqlStreamingResult {
    /// DDL operation success (CREATE/DROP TABLE/INDEX).
    Success,
    /// DML operation success with affected row count.
    RowsAffected(u64),
    /// Query result with streaming row iterator.
    Query(QueryRowIterator<'static>),
}

fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = AlopexDialect;
    Parser::parse_sql(&dialect, sql).map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))
}

fn stmt_requires_write(stmt: &Statement) -> bool {
    stmt.kind.requires_write()
}

fn stmt_changes_catalog(stmt: &Statement) -> bool {
    if let StatementKind::Explain {
        analyze: true,
        statement,
        ..
    } = &stmt.kind
    {
        return stmt_changes_catalog(statement);
    }
    matches!(
        stmt.kind,
        StatementKind::CreateTable(_)
            | StatementKind::DropTable(_)
            | StatementKind::CreateView(_)
            | StatementKind::DropView(_)
            | StatementKind::AlterTable(_)
            | StatementKind::CreateIndex(_)
            | StatementKind::DropIndex(_)
            | StatementKind::CreateSequence(_)
            | StatementKind::AlterSequence(_)
            | StatementKind::DropSequence(_)
    )
}

fn stmt_changes_user_data(stmt: &Statement) -> bool {
    if let StatementKind::Explain {
        analyze: true,
        statement,
        ..
    } = &stmt.kind
    {
        return stmt_changes_user_data(statement);
    }
    matches!(
        stmt.kind,
        StatementKind::Insert(_)
            | StatementKind::Update(_)
            | StatementKind::Delete(_)
            | StatementKind::Merge(_)
            | StatementKind::Copy(_)
            | StatementKind::AlterTable(_)
            | StatementKind::Truncate(_)
    )
}

fn stmt_can_stream(stmt: &Statement) -> bool {
    matches!(
        stmt.kind,
        StatementKind::Select(_) | StatementKind::Values(_)
    )
}

pub(crate) fn local_journal_scope<C: Catalog>(catalog: &C) -> RangeChangeJournalScope {
    let mut index_tables = BTreeMap::new();
    for table in catalog.list_tables() {
        for index in catalog.get_indexes_for_table(&table.name) {
            index_tables.insert(index.index_id, table.table_id);
        }
    }
    RangeChangeJournalScope::local(index_tables)
}

fn plan_stmt<'a, S: KVStore>(
    catalog: &'a alopex_sql::catalog::PersistentCatalog<S>,
    overlay: &'a CatalogOverlay,
    stmt: &Statement,
    parameters: Option<&[SqlValue]>,
) -> Result<alopex_sql::LogicalPlan> {
    let view = TxnCatalogView::new(catalog, overlay);
    let planner = match parameters {
        Some(parameters) => Planner::with_parameters(&view, parameters),
        None => Planner::new(&view),
    };
    planner
        .plan(stmt)
        .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))
}

/// Execute one SQL string inside an [`OwnedEmbeddedTransaction`] without committing it.
///
/// The legacy executor only needs a `KVTransaction` during this finite call.  We adapt the
/// owned transaction for that scope and leave its terminal ownership with the core session.
/// This preserves the catalog overlay and routing behaviour of [`Transaction::execute_sql`]
/// without extending any borrow beyond the call.
pub(crate) fn execute_sql_owned(
    transaction: &mut OwnedEmbeddedTransaction,
    sql: &str,
) -> Result<SqlResult> {
    let statements = match parse_sql(sql) {
        Ok(statements) => statements,
        Err(error) => {
            transaction.failed = true;
            return Err(error);
        }
    };
    execute_statements_owned(transaction, &statements, None)
}

pub(crate) fn execute_prepared_owned(
    transaction: &mut OwnedEmbeddedTransaction,
    statement: &Statement,
    parameters: &[SqlValue],
) -> Result<SqlResult> {
    execute_statements_owned(
        transaction,
        std::slice::from_ref(statement),
        Some(parameters),
    )
}

pub(crate) fn execute_prepared_many_owned<I, V, F>(
    transaction: &mut OwnedEmbeddedTransaction,
    statement: &Statement,
    rows: I,
    mut validate: F,
) -> Result<Vec<SqlResult>>
where
    I: IntoIterator<Item = V>,
    V: AsRef<[SqlValue]>,
    F: FnMut(&[SqlValue]) -> Result<()>,
{
    transaction.stage_hnsw_before_sql()?;
    if statement.kind.requires_write() {
        let mut vector_cache = transaction
            .db
            .vector_cache
            .write()
            .expect("vector cache lock poisoned");
        *vector_cache = None;
    }

    let db = Arc::clone(&transaction.db);
    let session = transaction.session.clone();
    let overlay = &mut transaction.overlay;
    let catalog_modified = &mut transaction.catalog_modified;
    let mut outcome: Result<Vec<SqlResult>> = Ok(Vec::new());

    let session_result = session
        .with_transaction(|owned| {
            outcome = (|| {
                let mut raw = AnyKVTransaction::Owned(OwnedKVTransactionAdapter::new(owned));
                let mode = raw.mode();
                let mut borrowed =
                    TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut raw, mode, overlay);
                let mut executor: Executor<_, _> =
                    Executor::new(db.store.clone(), db.sql_catalog.clone());
                let results = executor.execute_hnsw_batch_in_txn(
                    &mut borrowed,
                    |batch, borrowed| {
                        let mut results = Vec::new();
                        for row in rows {
                            let parameters = row.as_ref();
                            validate(parameters)?;
                            let plan = {
                                let catalog = db.sql_catalog.read().expect("catalog lock poisoned");
                                let (_, overlay) = borrowed.split_parts();
                                plan_stmt(&*catalog, &*overlay, statement, Some(parameters))?
                            };

                            {
                                let catalog = db.sql_catalog.read().expect("catalog lock poisoned");
                                let (_, overlay) = borrowed.split_parts();
                                let view = TxnCatalogView::new(&*catalog, &*overlay);
                                db.record_routing(&view, statement, 0);
                            }

                            results.push(
                                batch.execute(plan, borrowed).map_err(|error| {
                                    Error::Sql(alopex_sql::SqlError::from(error))
                                })?,
                            );
                        }
                        Ok(results)
                    },
                    |error| Error::Sql(alopex_sql::SqlError::from(error)),
                )?;
                if stmt_changes_catalog(statement) {
                    *catalog_modified = true;
                }
                Ok(results)
            })();
            Ok(())
        })
        .map_err(Error::Core);
    if let Err(error) = session_result {
        transaction.failed = true;
        return Err(error);
    }
    if outcome.is_err() {
        transaction.failed = true;
    }
    outcome
}

fn execute_statements_owned(
    transaction: &mut OwnedEmbeddedTransaction,
    statements: &[Statement],
    parameters: Option<&[SqlValue]>,
) -> Result<SqlResult> {
    if statements.is_empty() {
        return Ok(alopex_sql::ExecutionResult::Success);
    }

    transaction.stage_hnsw_before_sql()?;
    if statements.iter().any(stmt_requires_write) {
        let mut vector_cache = transaction
            .db
            .vector_cache
            .write()
            .expect("vector cache lock poisoned");
        *vector_cache = None;
    }

    let db = Arc::clone(&transaction.db);
    let session = transaction.session.clone();
    let overlay = &mut transaction.overlay;
    let catalog_modified = &mut transaction.catalog_modified;
    let mut outcome = Ok(alopex_sql::ExecutionResult::Success);

    let session_result = session
        .with_transaction(|owned| {
            outcome = (|| {
                let mut raw = AnyKVTransaction::Owned(OwnedKVTransactionAdapter::new(owned));
                let mode = raw.mode();
                let mut borrowed =
                    TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut raw, mode, overlay);
                let mut executor: Executor<_, _> =
                    Executor::new(db.store.clone(), db.sql_catalog.clone());
                let mut last = alopex_sql::ExecutionResult::Success;

                for (statement_index, stmt) in statements.iter().enumerate() {
                    let plan = {
                        let catalog = db.sql_catalog.read().expect("catalog lock poisoned");
                        let (_, overlay) = borrowed.split_parts();
                        plan_stmt(&*catalog, &*overlay, stmt, parameters)?
                    };

                    {
                        let catalog = db.sql_catalog.read().expect("catalog lock poisoned");
                        let (_, overlay) = borrowed.split_parts();
                        let view = TxnCatalogView::new(&*catalog, &*overlay);
                        db.record_routing(&view, stmt, statement_index);
                    }

                    last = executor
                        .execute_in_txn(plan, &mut borrowed)
                        .map_err(|error| Error::Sql(alopex_sql::SqlError::from(error)))?;
                }

                if statements.iter().any(stmt_changes_catalog) {
                    *catalog_modified = true;
                }
                Ok(last)
            })();
            Ok(())
        })
        .map_err(Error::Core);
    if let Err(error) = session_result {
        transaction.failed = true;
        return Err(error);
    }
    if outcome.is_err() {
        transaction.failed = true;
    }
    outcome
}

/// Build column info from projection and schema.
fn build_column_info(
    projection: &Projection,
    schema: &[alopex_sql::catalog::ColumnMetadata],
) -> Result<Vec<ColumnInfo>> {
    match projection {
        Projection::All(names) => {
            let mut cols = Vec::with_capacity(names.len());
            for name in names {
                let meta = schema.iter().find(|c| &c.name == name).ok_or_else(|| {
                    Error::Sql(alopex_sql::SqlError::Execution {
                        message: format!("column not found: {}", name),
                        code: "ALOPEX-E020",
                    })
                })?;
                cols.push(ColumnInfo::new(name.clone(), meta.data_type.clone()));
            }
            Ok(cols)
        }
        Projection::Columns(cols) => {
            let mut result = Vec::with_capacity(cols.len());
            for (i, col) in cols.iter().enumerate() {
                let name = col
                    .alias
                    .clone()
                    .or_else(|| {
                        if let alopex_sql::planner::typed_expr::TypedExprKind::ColumnRef {
                            column,
                            ..
                        } = &col.expr.kind
                        {
                            Some(column.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| format!("col_{}", i));
                result.push(ColumnInfo::new(name, col.expr.resolved_type.clone()));
            }
            Ok(result)
        }
    }
}

impl Database {
    /// Opens a read-only SQL storage transaction at a cluster-issued fenced
    /// read point. The returned transaction retains data, metadata, schema,
    /// and index identities through [`ReadAtPoint`].
    ///
    /// This API never falls back to `begin(ReadOnly)`: an unavailable or
    /// expired point returns [`Error::ReadAt`] before any SQL rows exist.
    pub fn begin_read_at_sql(
        &self,
        point: ReadAtPoint,
    ) -> Result<alopex_sql::storage::SqlTransaction<'_, alopex_core::kv::AnyKV>> {
        let transaction = self.store.begin_read_at(&point).map_err(Error::ReadAt)?;
        Ok(TxnBridge::from_read_at(transaction, point))
    }

    /// SQL を実行する（auto-commit）。
    ///
    /// - DDL/DML は ReadWrite トランザクションで実行し、成功時に自動コミットする。
    /// - SELECT は ReadOnly トランザクションで実行する。
    /// - 複数文はすべて同一トランザクションで実行し、最後の文の結果を返す。
    ///   文ごとの結果が必要な場合は [`Database::execute_sql_multi`] を使用する。
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
        Ok(self
            .execute_sql_multi(sql)?
            .pop()
            .unwrap_or(alopex_sql::ExecutionResult::Success))
    }

    /// Atomically import CSV from an application-owned reader.
    pub fn copy_from_csv_reader(
        &self,
        table: &str,
        reader: impl std::io::Read + 'static,
        header: bool,
    ) -> Result<SqlResult> {
        let mut transaction = self.begin(TxnMode::ReadWrite)?;
        let result = {
            let mut borrowed = TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(
                transaction.inner.as_mut().ok_or(Error::TxnCompleted)?,
                TxnMode::ReadWrite,
                &mut transaction.overlay,
            );
            let (mut txn, overlay) = borrowed.split_parts();
            let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
            let view = TxnCatalogView::new(&*catalog, overlay);
            let result = alopex_sql::executor::bulk::execute_copy_from_csv_reader(
                &mut txn,
                &view,
                table,
                reader,
                alopex_sql::executor::bulk::CopyOptions { header },
            )
            .map_err(|error| Error::Sql(alopex_sql::SqlError::from(error)))?;
            txn.flush_hnsw()
                .map_err(|error| Error::Sql(alopex_sql::SqlError::from(error)))?;
            result
        };
        transaction.commit()?;
        Ok(result)
    }

    /// Export CSV to an application-owned writer.
    pub fn copy_to_csv_writer(
        &self,
        table: &str,
        writer: &mut impl std::io::Write,
        header: bool,
    ) -> Result<SqlResult> {
        let executor: Executor<_, _> = Executor::new(self.store.clone(), self.sql_catalog.clone());
        executor
            .copy_to_csv_writer(table, writer, header)
            .map_err(|error| Error::Sql(alopex_sql::SqlError::from(error)))
    }

    /// Return persisted sequence definitions and current allocation state.
    pub fn list_sequences(&self) -> Result<Vec<alopex_sql::executor::SequenceInfo>> {
        let executor: Executor<_, _> = Executor::new(self.store.clone(), self.sql_catalog.clone());
        executor
            .list_sequences()
            .map_err(|error| Error::Sql(alopex_sql::SqlError::from(error)))
    }

    /// SQL を実行し、文ごとの実行結果を返す（auto-commit）。
    ///
    /// すべての文を同一トランザクションで実行し、成功時に自動コミットする。
    /// いずれかの文が失敗した場合はトランザクション全体がロールバックされ、
    /// エラーを返す。入力に文が含まれない場合は空の `Vec` を返す。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::Database;
    /// use alopex_sql::ExecutionResult;
    ///
    /// let db = Database::new();
    /// let results = db.execute_sql_multi(
    ///     "CREATE TABLE users (id INTEGER PRIMARY KEY); INSERT INTO users (id) VALUES (1);",
    /// ).unwrap();
    /// assert_eq!(results.len(), 2);
    /// assert!(matches!(results[0], ExecutionResult::Success));
    /// assert!(matches!(results[1], ExecutionResult::RowsAffected(1)));
    /// ```
    pub fn execute_sql_multi(&self, sql: &str) -> Result<Vec<SqlResult>> {
        let stmts = parse_sql(sql)?;
        self.execute_statements(&stmts, None)
    }

    pub(crate) fn execute_prepared_statement(
        &self,
        statement: &Statement,
        parameters: &[SqlValue],
    ) -> Result<SqlResult> {
        self.execute_statements(std::slice::from_ref(statement), Some(parameters))?
            .pop()
            .ok_or(Error::SqlSessionRequiresSingleStatement)
    }

    fn execute_statements(
        &self,
        stmts: &[Statement],
        parameters: Option<&[SqlValue]>,
    ) -> Result<Vec<SqlResult>> {
        if stmts.is_empty() {
            return Ok(Vec::new());
        }

        // PRAGMA controls must execute against the store itself. Running them
        // through the external-transaction bridge would hide the store from
        // the executor and correctly reject the operation. Keep the
        // auto-commit public API usable for a standalone PRAGMA.
        if stmts.len() == 1 {
            let overlay = CatalogOverlay::new();
            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                plan_stmt(&*catalog, &overlay, &stmts[0], parameters)?
            };
            if matches!(stmts[0].kind, StatementKind::Pragma { .. })
                || alopex_sql::executor::is_store_direct_plan(&plan)
            {
                let mut executor: Executor<_, _> =
                    Executor::new(self.store.clone(), self.sql_catalog.clone());
                return executor
                    .execute(plan)
                    .map(|result| vec![result])
                    .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)));
            }
        }

        let requires_write = stmts.iter().any(stmt_requires_write);
        let mode = if requires_write {
            TxnMode::ReadWrite
        } else {
            TxnMode::ReadOnly
        };

        // A cache entry and the catalog consulted by execution are valid only
        // for the storage snapshot that supplied them. Keep the read gate
        // through read transaction start, cache snapshot, planning, and
        // execution so a committed DDL writer cannot split those views.
        let cache_gate = if mode == TxnMode::ReadOnly {
            Some(
                self.hnsw_cache_gate
                    .read()
                    .expect("hnsw cache gate lock poisoned"),
            )
        } else {
            None
        };
        let mut txn = self.store.begin(mode).map_err(Error::Core)?;
        let (hnsw_cache_epoch, cached_hnsw_entries) = if mode == TxnMode::ReadOnly {
            let (epoch, cached) = self.hnsw_cache_snapshot();
            (Some(epoch), cached)
        } else {
            (None, Vec::new())
        };
        let journal = if mode == TxnMode::ReadWrite
            && stmts.iter().any(stmt_changes_user_data)
            && self.store.range_change_journal_capability()
                == RangeChangeJournalCapability::Supported
        {
            let scope = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                local_journal_scope(&*catalog)
            };
            Some(LocalRangeChangeJournal::capture(&mut txn, scope).map_err(Error::Core)?)
        } else {
            None
        };
        let mut overlay = CatalogOverlay::new();
        let mut borrowed =
            TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut txn, mode, &mut overlay);
        if hnsw_cache_epoch.is_some() {
            borrowed.seed_hnsw_read_cache(cached_hnsw_entries);
        }

        let mut executor: Executor<_, _> =
            Executor::new(self.store.clone(), self.sql_catalog.clone());
        let mut results = Vec::with_capacity(stmts.len());
        for (statement_index, stmt) in stmts.iter().enumerate() {
            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt, parameters)?
            };

            {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                self.record_routing(&view, stmt, statement_index);
            }

            results.push(
                executor
                    .execute_in_txn(plan, &mut borrowed)
                    .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?,
            );
            #[cfg(test)]
            if mode == TxnMode::ReadOnly {
                if let Some(barrier) = self
                    .hnsw_cache_after_executor_barrier
                    .lock()
                    .expect("hnsw cache after executor barrier lock poisoned")
                    .clone()
                {
                    barrier.wait();
                    barrier.wait();
                }
            }
        }

        let hnsw_cache_entries = if mode == TxnMode::ReadOnly {
            borrowed.cloned_hnsw_entries()
        } else {
            Vec::new()
        };
        drop(borrowed);
        drop(cache_gate);

        if let Some(journal) = journal {
            journal.stage(&mut txn).map_err(Error::Core)?;
        }

        // `execute_in_txn()` 成功時に HNSW flush 済み（失敗時は abandon 済み）なので、
        // ここでは KV commit と overlay 適用のみを行う。
        //
        // commit_self は `txn` を消費するため、失敗時に rollback はできない。
        if mode == TxnMode::ReadWrite {
            let catalog_deleted = overlay.has_deletions();
            self.commit_with_hnsw_cache_changes(
                || {
                    // ponytail: deletions clear all graphs; track affected names if DDL cost matters.
                    let changed = if catalog_deleted {
                        None
                    } else {
                        self.hnsw_cache_changes(|visitor| txn.visit_pending_write_keys(visitor))
                    };
                    txn.commit_self()
                        .map(|()| ((), changed))
                        .map_err(Error::Core)
                },
                || {
                    let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");
                    catalog.apply_overlay(overlay);
                },
            )?;
        } else {
            txn.commit_self().map_err(Error::Core)?;
        }
        if let Some(epoch) = hnsw_cache_epoch {
            self.hnsw_cache_insert_if_current(epoch, hnsw_cache_entries);
        }
        if stmts.iter().any(stmt_changes_catalog) {
            self.invalidate_table_info_cache();
        }
        if requires_write {
            let mut vector_cache = self
                .vector_cache
                .write()
                .expect("vector cache lock poisoned");
            *vector_cache = None;
        }
        Ok(results)
    }

    /// Execute SQL with callback-based streaming for SELECT queries (FR-7).
    ///
    /// This method provides true streaming by keeping the transaction alive
    /// during row iteration. The callback receives a `StreamingRows` that
    /// yields rows on-demand from storage.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Callback function that processes the streaming rows
    /// * `R` - Return type from the callback
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::Database;
    ///
    /// let db = Database::new();
    /// db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").unwrap();
    /// db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');").unwrap();
    ///
    /// // Process rows with streaming - transaction stays alive during callback.
    /// // Propagate `next_row` errors with `?`; swallowing them (e.g. with
    /// // `while let Ok(Some(...))`) silently truncates the result set.
    /// let result = db.execute_sql_with_rows("SELECT * FROM users;", |mut rows| {
    ///     let mut names = Vec::new();
    ///     while let Some(row) = rows.next_row()? {
    ///         if let Some(alopex_sql::storage::SqlValue::Text(name)) = row.get(1) {
    ///             names.push(name.clone());
    ///         }
    ///     }
    ///     Ok(names)
    /// }).unwrap();
    /// ```
    pub fn execute_sql_with_rows<F, R>(&self, sql: &str, f: F) -> Result<StreamingQueryResult<R>>
    where
        F: FnOnce(StreamingRows<'_>) -> Result<R>,
    {
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(StreamingQueryResult::Success);
        }

        // For streaming SELECT, use the new pipeline
        if stmts.len() == 1 && stmt_can_stream(&stmts[0]) {
            let stmt = &stmts[0];
            let mode = TxnMode::ReadOnly;

            let mut txn = self.store.begin(mode).map_err(Error::Core)?;
            let mut overlay = CatalogOverlay::new();
            let mut borrowed =
                TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut txn, mode, &mut overlay);

            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay_ref) = borrowed.split_parts();
                plan_stmt(&*catalog, overlay_ref, stmt, None)?
            };

            if alopex_sql::executor::is_store_direct_plan(&plan) {
                let mut executor: Executor<_, _> =
                    Executor::new(self.store.clone(), self.sql_catalog.clone());
                let result = executor
                    .execute(plan)
                    .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;
                let ExecutionResult::Query(query) = result else {
                    return Err(Error::Sql(alopex_sql::SqlError::Execution {
                        message: "store-direct system function did not return rows".into(),
                        code: "ALOPEX-E022",
                    }));
                };
                let column_names: Vec<String> = query
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect();
                let schema: Vec<alopex_sql::catalog::ColumnMetadata> = query
                    .columns
                    .iter()
                    .map(|column| {
                        alopex_sql::catalog::ColumnMetadata::new(
                            &column.name,
                            column.data_type.clone(),
                        )
                    })
                    .collect();
                let rows: Vec<Row> = query
                    .rows
                    .into_iter()
                    .enumerate()
                    .map(|(index, values)| Row::new(index as u64, values))
                    .collect();
                let iter = VecIterator::new(rows, schema.clone());
                let streaming_rows = StreamingRows {
                    columns: query.columns,
                    iter: Box::new(iter),
                    projection: Projection::All(column_names),
                    schema,
                };
                let result = f(streaming_rows)?;
                return Ok(StreamingQueryResult::QueryProcessed(result));
            }

            let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
            let (mut sql_txn, overlay_ref) = borrowed.split_parts();
            let view = TxnCatalogView::new(&*catalog, overlay_ref);

            // Build streaming pipeline - iterator lifetime tied to sql_txn
            let (iter, projection, schema) = build_streaming_pipeline(&mut sql_txn, &view, plan)
                .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;

            // Build column info from projection and schema
            let columns = build_column_info(&projection, &schema)?;

            // Create StreamingRows and pass to callback
            let streaming_rows = StreamingRows {
                columns,
                iter,
                projection,
                schema,
            };

            // Execute callback with streaming rows
            let result = f(streaming_rows)?;

            // Clean up and commit after callback completes
            drop(catalog);
            drop(borrowed);
            txn.commit_self().map_err(Error::Core)?;

            return Ok(StreamingQueryResult::QueryProcessed(result));
        }

        // Fall back to standard execution for non-SELECT or multi-statement
        let exec_result = self.execute_sql(sql)?;
        match exec_result {
            alopex_sql::ExecutionResult::Success => Ok(StreamingQueryResult::Success),
            alopex_sql::ExecutionResult::RowsAffected(n) => {
                Ok(StreamingQueryResult::RowsAffected(n))
            }
            alopex_sql::ExecutionResult::Query(_qr) => {
                // For non-streaming path, we can't provide true streaming
                // Return an error indicating streaming is not available
                Err(Error::Sql(alopex_sql::SqlError::Execution {
                    message: "Streaming not available for multi-statement or complex queries"
                        .into(),
                    code: "ALOPEX-E021",
                }))
            }
        }
    }

    /// Execute SQL and return a streaming result for SELECT queries (FR-7).
    ///
    /// This method returns a `SqlStreamingResult` that contains an iterator
    /// for query results, enabling true streaming output without materializing
    /// all rows upfront.
    ///
    /// # Note
    ///
    /// Only single SELECT statements are supported for streaming. Multi-statement
    /// SQL or non-SELECT statements fall back to the standard execution path.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use alopex_embedded::{Database, SqlStreamingResult};
    ///
    /// let db = Database::new();
    /// db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")?;
    /// db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
    ///
    /// // Propagate `next_row` errors with `?`; swallowing them (e.g. with
    /// // `while let Ok(Some(...))`) silently truncates the result set.
    /// let result = db.execute_sql_streaming("SELECT * FROM users;")?;
    /// if let SqlStreamingResult::Query(mut iter) = result {
    ///     while let Some(row) = iter.next_row()? {
    ///         println!("{:?}", row);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute_sql_streaming(&self, sql: &str) -> Result<SqlStreamingResult> {
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(SqlStreamingResult::Success);
        }

        // For streaming, only support single SELECT statement
        if stmts.len() == 1 && stmt_can_stream(&stmts[0]) {
            let stmt = &stmts[0];
            let mode = TxnMode::ReadOnly;

            let mut txn = self.store.begin(mode).map_err(Error::Core)?;
            let mut overlay = CatalogOverlay::new();
            let mut borrowed =
                TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut txn, mode, &mut overlay);

            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt, None)?
            };

            let (mut sql_txn, _overlay) = borrowed.split_parts();

            let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
            let view = TxnCatalogView::new(&*catalog, _overlay);
            let iter = execute_query_streaming(&mut sql_txn, &view, plan)
                .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;

            drop(catalog);
            drop(borrowed);

            txn.commit_self().map_err(Error::Core)?;

            return Ok(SqlStreamingResult::Query(iter));
        }

        // Fall back to standard execution for non-streaming cases
        let result = self.execute_sql(sql)?;
        match result {
            alopex_sql::ExecutionResult::Success => Ok(SqlStreamingResult::Success),
            alopex_sql::ExecutionResult::RowsAffected(n) => Ok(SqlStreamingResult::RowsAffected(n)),
            alopex_sql::ExecutionResult::Query(qr) => {
                // Convert materialized result to streaming iterator
                use alopex_sql::executor::query::iterator::VecIterator;
                use alopex_sql::executor::Row;
                use alopex_sql::planner::typed_expr::Projection;

                let column_names: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
                let schema: Vec<alopex_sql::catalog::ColumnMetadata> = qr
                    .columns
                    .iter()
                    .map(|c| alopex_sql::catalog::ColumnMetadata::new(&c.name, c.data_type.clone()))
                    .collect();
                let rows: Vec<Row> = qr
                    .rows
                    .into_iter()
                    .enumerate()
                    .map(|(i, values)| Row::new(i as u64, values))
                    .collect();
                let iter = VecIterator::new(rows, schema.clone());
                let query_iter =
                    QueryRowIterator::new(Box::new(iter), Projection::All(column_names), schema);
                Ok(SqlStreamingResult::Query(query_iter))
            }
        }
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

        if stmts.iter().any(stmt_requires_write) {
            let mut vector_cache = self
                .db
                .vector_cache
                .write()
                .expect("vector cache lock poisoned");
            *vector_cache = None;
        }

        let store = self.db.store.clone();
        let sql_catalog = self.db.sql_catalog.clone();

        let txn = self.inner.as_mut().ok_or(Error::TxnCompleted)?;
        // SQL loads its graph from this transaction's KV state. Stage direct
        // changes first, then discard the old graph so later direct operations
        // reload any SQL updates instead of overwriting them at commit.
        for (index, state) in self.hnsw_indices.values_mut() {
            index.commit_staged(txn, state).map_err(Error::Core)?;
        }
        self.hnsw_indices.clear();
        let mode = txn.mode();

        let mut borrowed =
            TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(txn, mode, &mut self.overlay);
        let mut executor: Executor<_, _> = Executor::new(store, sql_catalog.clone());

        let mut last = alopex_sql::ExecutionResult::Success;
        for (statement_index, stmt) in stmts.iter().enumerate() {
            let plan = {
                let catalog = sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt, None)?
            };

            {
                let catalog = sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                self.db.record_routing(&view, stmt, statement_index);
            }

            last = executor
                .execute_in_txn(plan, &mut borrowed)
                .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;
        }

        if stmts.iter().any(stmt_changes_catalog) {
            self.catalog_modified = true;
        }
        Ok(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::kv::decode_range_change;
    use alopex_core::kv::RangeChangePayload;
    use std::sync::{mpsc, Arc, Barrier};
    use std::time::Duration;

    #[test]
    fn auto_commit_stages_sql_row_and_index_changes_before_visibility() {
        let db = Database::open_in_memory().unwrap();
        db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        db.execute_sql("CREATE INDEX idx_users_name ON users (name);")
            .unwrap();
        db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice');")
            .unwrap();

        let mut reader = db.store.begin(TxnMode::ReadOnly).unwrap();
        let records = reader
            .scan_prefix(b"\x00alopex/range-change/")
            .unwrap()
            .filter_map(|(_, value)| decode_range_change(&value).ok())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 1);
        assert!(records[0]
            .payload
            .iter()
            .any(|payload| matches!(payload, RangeChangePayload::UpsertRow { .. })));
        assert!(records[0]
            .payload
            .iter()
            .any(|payload| matches!(payload, RangeChangePayload::UpsertIndex { .. })));
    }

    #[test]
    fn explicit_sql_transaction_stages_journal_before_commit() {
        let db = Database::open_in_memory().unwrap();
        db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        let mut transaction = db.begin(TxnMode::ReadWrite).unwrap();
        transaction
            .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice');")
            .unwrap();
        transaction.commit().unwrap();

        let records = db
            .snapshot()
            .into_iter()
            .filter_map(|(_, value)| decode_range_change(&value).ok())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 1);
        assert!(records[0]
            .payload
            .iter()
            .any(|payload| matches!(payload, RangeChangePayload::UpsertRow { .. })));
    }

    #[test]
    fn embedded_read_at_returns_retention_error_before_a_sql_session_exists() {
        let db = Database::open_in_memory().unwrap();
        let point = ReadAtPoint::new(1, 2, 3, 4);

        assert!(matches!(
            db.begin_read_at_sql(point),
            Err(Error::ReadAt(alopex_core::ReadAtError::Unavailable { .. }))
        ));
    }

    #[test]
    fn csv_reader_commit_updates_hnsw_cache_and_journal() {
        let mut observations = Vec::new();
        for persistent in [false, true] {
            for indexed in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let db = warmed_sql_hnsw_indexes(if persistent {
                    Database::open(directory.path()).unwrap()
                } else {
                    Database::open_in_memory().unwrap()
                });
                let before = db.hnsw_cache.read().unwrap().clone();
                let (epoch, stale) = db.hnsw_cache_snapshot();
                let journal_before = range_change_entries(&db);
                let (table, csv) = if indexed {
                    ("cache_items_a", "id,embedding\n3,\"[20.0,0.0]\"\n")
                } else {
                    ("cache_plain", "id\n3\n")
                };
                assert!(matches!(
                    db.copy_from_csv_reader(table, std::io::Cursor::new(csv), true)
                        .unwrap(),
                    ExecutionResult::RowsAffected(1)
                ));
                let retained = {
                    let after = db.hnsw_cache.read().unwrap();
                    ["cache_sql_a", "cache_sql_b"].map(|name| {
                        after
                            .get(name)
                            .is_some_and(|index| Arc::ptr_eq(index, &before[name]))
                    })
                };
                let epoch_advanced = db
                    .hnsw_cache_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
                    == epoch + 1;
                db.hnsw_cache_insert_if_current(epoch, stale);
                let distance = db
                    .search_hnsw("cache_sql_a", &[20.0, 0.0], 1, Some(8))
                    .unwrap()
                    .0[0]
                    .distance;
                assert_eq!(
                    db.search_hnsw("cache_sql_b", &[0.0, 0.0], 1, Some(8))
                        .unwrap()
                        .0[0]
                        .distance,
                    0.0
                );
                let ExecutionResult::Query(rows) = db
                    .execute_sql(&format!("SELECT id FROM {table} WHERE id = 3"))
                    .unwrap()
                else {
                    panic!("expected imported row");
                };
                assert_eq!(rows.rows, vec![vec![SqlValue::Integer(3)]]);
                if indexed {
                    let ExecutionResult::Query(rows) = db
                        .execute_sql("SELECT embedding FROM cache_items_a WHERE id = 3")
                        .unwrap()
                    else {
                        panic!("expected imported vector");
                    };
                    assert_eq!(rows.rows, vec![vec![SqlValue::Vector(vec![20.0, 0.0])]]);
                    let mut read = db.store.begin(TxnMode::ReadOnly).unwrap();
                    let graph = alopex_core::HnswIndex::load("cache_sql_a", &mut read).unwrap();
                    assert_eq!(
                        graph.search(&[20.0, 0.0], 1, Some(8)).unwrap().0[0].distance,
                        0.0
                    );
                    read.rollback_self().unwrap();
                }
                let journal_after = range_change_entries(&db);
                let journal_ok = journal_after.len() == journal_before.len() + 1
                    && journal_after
                        .iter()
                        .filter(|entry| !journal_before.contains(entry))
                        // Epoch counters share the prefix but are not serialized records.
                        .filter(|(key, _)| !key.starts_with(b"\x00alopex/range-change/epoch/"))
                        .any(|(_, bytes)| {
                            let record = decode_range_change(bytes).unwrap();
                            record.payload.iter().any(|payload| {
                                matches!(payload, RangeChangePayload::UpsertRow { .. })
                            }) && record.payload.iter().any(|payload| {
                                matches!(payload, RangeChangePayload::UpsertIndex { .. })
                            })
                        });
                eprintln!("CSV persistent={persistent} indexed={indexed}: retained={retained:?} epoch_advanced={epoch_advanced} distance={distance} journal_ok={journal_ok}; stored row verified");
                observations.push((
                    retained == [!indexed, true],
                    epoch_advanced,
                    distance == if indexed { 0.0 } else { 11.0 },
                    journal_ok,
                ));
            }
        }
        assert_eq!(observations, vec![(true, true, true, true); 4]);
    }

    fn range_change_entries(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut read = db.store.begin(TxnMode::ReadOnly).unwrap();
        let entries = read
            .scan_prefix(b"\x00alopex/range-change/")
            .unwrap()
            .collect();
        read.rollback_self().unwrap();
        entries
    }

    #[test]
    fn failed_csv_reader_preserves_rows_graphs_and_journal() {
        for persistent in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let db = warmed_sql_hnsw_indexes(if persistent {
                Database::open(directory.path()).unwrap()
            } else {
                Database::open_in_memory().unwrap()
            });
            let before = db.hnsw_cache.read().unwrap().clone();
            let epoch = db
                .hnsw_cache_epoch
                .load(std::sync::atomic::Ordering::Acquire);
            let journal_before = range_change_entries(&db);
            // Cross the bulk reader's 1024-row batch so KV writes precede the bad row.
            let mut csv = (3..1027)
                .map(|id| format!("{id},\"[20.0,0.0]\"\n"))
                .collect::<String>();
            csv.push_str("invalid,\"[20.0,0.0]\"\n");
            assert!(db
                .copy_from_csv_reader("cache_items_a", std::io::Cursor::new(csv), false)
                .is_err());
            assert_eq!(
                db.hnsw_cache_epoch
                    .load(std::sync::atomic::Ordering::Acquire),
                epoch
            );
            for name in ["cache_sql_a", "cache_sql_b"] {
                assert!(Arc::ptr_eq(
                    &db.hnsw_cache.read().unwrap()[name],
                    &before[name]
                ));
                assert_eq!(
                    db.search_hnsw(name, &[20.0, 0.0], 1, Some(8)).unwrap().0[0].distance,
                    11.0
                );
            }
            let ExecutionResult::Query(rows) = db
                .execute_sql("SELECT id FROM cache_items_a ORDER BY id")
                .unwrap()
            else {
                panic!("expected original rows");
            };
            assert_eq!(
                rows.rows,
                vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
            );
            assert_eq!(range_change_entries(&db), journal_before);
            let mut read = db.store.begin(TxnMode::ReadOnly).unwrap();
            let graph = alopex_core::HnswIndex::load("cache_sql_a", &mut read).unwrap();
            assert_eq!(
                graph.search(&[20.0, 0.0], 1, Some(8)).unwrap().0[0].distance,
                11.0
            );
            read.rollback_self().unwrap();
            eprintln!(
                "CSV failure persistent={persistent}: cache/epoch/rows/graph/journal unchanged"
            );
        }
    }

    #[test]
    fn auto_sql_commits_preserve_unmodified_hnsw_caches() {
        let mut observations = Vec::new();
        for persistent in [false, true] {
            for indexed in [false, true] {
                for api in ["single", "multi", "prepared"] {
                    let directory = tempfile::tempdir().unwrap();
                    let db = warmed_sql_hnsw_indexes(if persistent {
                        Database::open(directory.path()).unwrap()
                    } else {
                        Database::open_in_memory().unwrap()
                    });
                    let before = db.hnsw_cache.read().unwrap().clone();
                    let (epoch, stale) = db.hnsw_cache_snapshot();
                    let sql = if indexed {
                        "UPDATE cache_items_a SET embedding = [20.0, 0.0] WHERE id = 1"
                    } else {
                        "INSERT INTO cache_plain VALUES (1)"
                    };
                    match api {
                        "single" => {
                            db.execute_sql(sql).unwrap();
                        }
                        "multi" => {
                            db.execute_sql_multi(&format!("{sql}; SELECT 1")).unwrap();
                        }
                        "prepared" => {
                            let sql = if indexed {
                                "UPDATE cache_items_a SET embedding = [20.0, 0.0] WHERE id = ?"
                            } else {
                                "INSERT INTO cache_plain VALUES (?)"
                            };
                            let mut statement = db.prepare(sql).unwrap();
                            statement.bind(1, SqlValue::Integer(1)).unwrap();
                            statement.execute().unwrap();
                        }
                        _ => unreachable!(),
                    }
                    let retained = {
                        let after = db.hnsw_cache.read().unwrap();
                        ["cache_sql_a", "cache_sql_b"].map(|name| {
                            after
                                .get(name)
                                .is_some_and(|index| Arc::ptr_eq(index, &before[name]))
                        })
                    };
                    assert_eq!(
                        db.hnsw_cache_epoch
                            .load(std::sync::atomic::Ordering::Acquire),
                        epoch + 1
                    );
                    db.hnsw_cache_insert_if_current(epoch, stale);
                    for (name, distance) in [
                        ("cache_sql_a", if indexed { 9.0 } else { 0.0 }),
                        ("cache_sql_b", 0.0),
                    ] {
                        let (rows, _) = db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap();
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].distance, distance);
                    }
                    let ExecutionResult::Query(rows) = db.execute_sql(if indexed {
                        "SELECT id FROM cache_items_a ORDER BY vector_distance(embedding, [0.0, 0.0], 'l2') LIMIT 1"
                    } else {
                        "SELECT id FROM cache_plain"
                    }).unwrap() else { panic!("expected query rows"); };
                    assert_eq!(
                        rows.rows,
                        vec![vec![SqlValue::Integer(if indexed { 2 } else { 1 })]]
                    );
                    if indexed {
                        let ExecutionResult::Query(rows) = db
                            .execute_sql("SELECT embedding FROM cache_items_a WHERE id = 1")
                            .unwrap()
                        else {
                            panic!("expected stored vector");
                        };
                        assert_eq!(rows.rows, vec![vec![SqlValue::Vector(vec![20.0, 0.0])]]);
                    }
                    eprintln!("auto SQL persistent={persistent} indexed={indexed} api={api}: retained={retained:?}; epoch/storage/search verified");
                    observations.push((retained, [!indexed, true]));
                }
            }
        }
        for (actual, expected) in observations {
            assert_eq!(actual, expected);
        }
    }

    fn warmed_sql_hnsw_indexes(database: Database) -> Arc<Database> {
        let db = Arc::new(database);
        db.execute_sql("CREATE TABLE cache_plain (id INTEGER PRIMARY KEY)")
            .unwrap();
        for suffix in ["a", "b"] {
            db.execute_sql(&format!(
                "CREATE TABLE cache_items_{suffix} (id INTEGER PRIMARY KEY, embedding VECTOR(2, L2));\
                 CREATE INDEX cache_sql_{suffix} ON cache_items_{suffix} (embedding) USING HNSW;\
                 INSERT INTO cache_items_{suffix} VALUES (1, [0.0, 0.0]), (2, [9.0, 0.0]);"
            )).unwrap();
        }
        for name in ["cache_sql_a", "cache_sql_b"] {
            let (rows, _) = db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].distance, 0.0);
        }
        db
    }

    #[test]
    fn auto_sql_failed_write_and_catalog_deletion_preserve_cache_contracts() {
        for persistent in [false, true] {
            for operation in ["failed_multi", "drop_index", "drop_table"] {
                let directory = tempfile::tempdir().unwrap();
                let db = warmed_sql_hnsw_indexes(if persistent {
                    Database::open(directory.path()).unwrap()
                } else {
                    Database::open_in_memory().unwrap()
                });
                let before = db.hnsw_cache.read().unwrap().clone();
                let (epoch, stale) = db.hnsw_cache_snapshot();
                if operation == "failed_multi" {
                    assert!(db
                        .execute_sql_multi(
                            "UPDATE cache_items_a SET embedding = [20.0, 0.0] WHERE id = 1;\
                         INSERT INTO cache_items_a VALUES (1, [30.0, 0.0]);"
                        )
                        .is_err());
                    assert_eq!(
                        db.hnsw_cache_epoch
                            .load(std::sync::atomic::Ordering::Acquire),
                        epoch
                    );
                    for name in ["cache_sql_a", "cache_sql_b"] {
                        assert!(Arc::ptr_eq(
                            &db.hnsw_cache.read().unwrap()[name],
                            &before[name]
                        ));
                        assert_eq!(
                            db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap().0[0].distance,
                            0.0
                        );
                    }
                    let ExecutionResult::Query(rows) = db.execute_sql(
                        "SELECT id FROM cache_items_a ORDER BY vector_distance(embedding, [0.0, 0.0], 'l2') LIMIT 1"
                    ).unwrap() else { panic!("expected query rows"); };
                    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(1)]]);
                    let ExecutionResult::Query(rows) = db
                        .execute_sql("SELECT embedding FROM cache_items_a WHERE id = 1")
                        .unwrap()
                    else {
                        panic!("expected stored vector");
                    };
                    assert_eq!(rows.rows, vec![vec![SqlValue::Vector(vec![0.0, 0.0])]]);
                } else {
                    db.execute_sql(if operation == "drop_index" {
                        "DROP INDEX cache_sql_a"
                    } else {
                        "DROP TABLE cache_items_a"
                    })
                    .unwrap();
                    assert_eq!(
                        db.hnsw_cache_epoch
                            .load(std::sync::atomic::Ordering::Acquire),
                        epoch + 1
                    );
                    assert!(db.hnsw_cache.read().unwrap().is_empty());
                    db.hnsw_cache_insert_if_current(epoch, stale);
                    assert!(db.hnsw_cache.read().unwrap().is_empty());
                    assert!(db
                        .search_hnsw("cache_sql_a", &[0.0, 0.0], 1, Some(8))
                        .is_err());
                    assert_eq!(
                        db.search_hnsw("cache_sql_b", &[0.0, 0.0], 1, Some(8))
                            .unwrap()
                            .0[0]
                            .distance,
                        0.0
                    );
                }
                eprintln!("auto SQL control persistent={persistent} operation={operation}: PASS");
            }
        }
    }

    #[test]
    fn auto_commit_knn_reads_reuse_the_database_hnsw_cache() {
        let db = Database::open_in_memory().unwrap();
        db.execute_sql(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2, L2));\
             CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;\
             INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]);",
        )
        .unwrap();
        assert!(db.hnsw_cache.read().unwrap().is_empty());

        let query = "EXPLAIN SELECT id FROM items \
            ORDER BY vector_distance(embedding, [0.0, 0.0], 'l2') ASC LIMIT 1";
        db.execute_sql(query).unwrap();
        let first = db
            .hnsw_cache
            .read()
            .unwrap()
            .get("idx_items_embedding")
            .cloned()
            .expect("the first SQL kNN read must populate the cache");

        db.execute_sql(query).unwrap();
        let second = db
            .hnsw_cache
            .read()
            .unwrap()
            .get("idx_items_embedding")
            .cloned()
            .expect("the cached index must remain available");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn auto_sql_read_holds_hnsw_cache_gate_until_execution_completes() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY);")
            .unwrap();
        db.execute_sql("INSERT INTO items (id) VALUES (1);")
            .unwrap();

        let after_executor_barrier = Arc::new(Barrier::new(2));
        let write_gate_barrier = Arc::new(Barrier::new(2));
        *db.hnsw_cache_after_executor_barrier.lock().unwrap() =
            Some(Arc::clone(&after_executor_barrier));
        *db.hnsw_cache_write_gate_barrier.lock().unwrap() = Some(Arc::clone(&write_gate_barrier));
        let epoch = db
            .hnsw_cache_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let (read_done_tx, read_done_rx) = mpsc::channel();
        let (write_done_tx, write_done_rx) = mpsc::channel();
        let (write_gate_acquired_tx, write_gate_acquired_rx) = mpsc::channel();
        *db.hnsw_cache_write_gate_acquired.lock().unwrap() = Some(write_gate_acquired_tx);

        std::thread::scope(|scope| {
            let reader_db = Arc::clone(&db);
            scope.spawn(move || {
                reader_db.execute_sql("SELECT id FROM items;").unwrap();
                read_done_tx.send(()).unwrap();
            });
            // The actual auto-SQL SELECT completed its executor call and is
            // paused before releasing the read gate.
            after_executor_barrier.wait();

            let writer_db = Arc::clone(&db);
            scope.spawn(move || {
                writer_db
                    .commit_with_hnsw_cache_invalidation(|| Ok::<(), ()>(()))
                    .unwrap();
                write_done_tx.send(()).unwrap();
            });
            // The writer reached the instruction immediately before write
            // gate acquisition. It cannot acquire that gate while the real
            // SELECT is paused after executor completion.
            write_gate_barrier.wait();
            write_gate_barrier.wait();
            assert!(matches!(
                write_gate_acquired_rx.recv_timeout(Duration::from_millis(250)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            assert_eq!(
                db.hnsw_cache_epoch
                    .load(std::sync::atomic::Ordering::Acquire),
                epoch
            );

            after_executor_barrier.wait();
            read_done_rx.recv().unwrap();
            write_gate_acquired_rx.recv().unwrap();
            write_done_rx.recv().unwrap();
        });
        assert_eq!(
            db.hnsw_cache_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            epoch + 1
        );
    }

    #[test]
    fn auto_commit_hnsw_read_reports_writer_gate_wait() {
        let (db, _, query, _) = hnsw_cache_test_fixture();
        let db = Arc::new(db);
        db.execute_sql("CREATE TABLE writes (id INTEGER PRIMARY KEY);")
            .unwrap();
        db.execute_sql(&query).unwrap();

        let after_executor_barrier = Arc::new(Barrier::new(2));
        let write_gate_barrier = Arc::new(Barrier::new(2));
        *db.hnsw_cache_after_executor_barrier.lock().unwrap() =
            Some(Arc::clone(&after_executor_barrier));
        *db.hnsw_cache_write_gate_barrier.lock().unwrap() = Some(Arc::clone(&write_gate_barrier));
        let (write_gate_wait_tx, write_gate_wait_rx) = mpsc::channel();
        *db.hnsw_cache_write_gate_wait.lock().unwrap() = Some(write_gate_wait_tx);
        let (write_gate_wait_started_tx, write_gate_wait_started_rx) = mpsc::channel();
        *db.hnsw_cache_write_gate_wait_started.lock().unwrap() = Some(write_gate_wait_started_tx);
        let (read_done_tx, read_done_rx) = mpsc::channel();
        let (write_done_tx, write_done_rx) = mpsc::channel();

        std::thread::scope(|scope| {
            let reader_db = Arc::clone(&db);
            scope.spawn(move || {
                reader_db.execute_sql(&query).unwrap();
                read_done_tx.send(()).unwrap();
            });
            after_executor_barrier.wait();

            let writer_db = Arc::clone(&db);
            scope.spawn(move || {
                writer_db
                    .execute_sql("INSERT INTO writes (id) VALUES (1);")
                    .unwrap();
                write_done_tx.send(()).unwrap();
            });
            write_gate_barrier.wait();
            write_gate_barrier.wait();
            let writer_reached_gate_request =
                write_gate_wait_started_rx.recv_timeout(Duration::from_secs(1));
            let writer_blocked = matches!(
                write_gate_wait_rx.recv_timeout(Duration::from_millis(250)),
                Err(mpsc::RecvTimeoutError::Timeout)
            );
            after_executor_barrier.wait();
            read_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let wait = write_gate_wait_rx.recv_timeout(Duration::from_secs(1));
            write_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(
                writer_reached_gate_request.is_ok(),
                "writer must reach the measured gate request before the read is released"
            );
            assert!(
                writer_blocked,
                "writer must not acquire the gate while the auto-commit HNSW read is in flight"
            );
            assert!(
                wait.unwrap() >= Duration::from_millis(250),
                "writer gate wait must cover the reader-held interval"
            );
        });
        *db.hnsw_cache_after_executor_barrier.lock().unwrap() = None;
        *db.hnsw_cache_write_gate_barrier.lock().unwrap() = None;
        *db.hnsw_cache_write_gate_wait.lock().unwrap() = None;
        *db.hnsw_cache_write_gate_wait_started.lock().unwrap() = None;
    }

    #[test]
    fn failed_commit_does_not_publish_hnsw_cache_state() {
        let (db, _, query, _) = hnsw_cache_test_fixture();
        db.execute_sql(&query).unwrap();
        let epoch = db
            .hnsw_cache_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let cached = db
            .hnsw_cache
            .read()
            .unwrap()
            .get("idx_items_embedding")
            .cloned()
            .expect("the read must populate the HNSW cache");
        let update_called = std::sync::atomic::AtomicBool::new(false);

        let result = db.commit_with_hnsw_cache_update(
            || Err::<(), ()>(()),
            || update_called.store(true, std::sync::atomic::Ordering::Release),
        );

        assert_eq!(result, Err(()));
        assert!(!update_called.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            db.hnsw_cache_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            epoch
        );
        let retained = db
            .hnsw_cache
            .read()
            .unwrap()
            .get("idx_items_embedding")
            .cloned()
            .expect("a failed commit must retain the prior HNSW cache entry");
        assert!(Arc::ptr_eq(&cached, &retained));
    }

    fn two_warmed_direct_hnsw_indexes() -> Arc<Database> {
        warm_direct_hnsw_indexes(Database::open_in_memory().unwrap())
    }

    fn warm_direct_hnsw_indexes(database: Database) -> Arc<Database> {
        let db = Arc::new(database);
        let config = alopex_core::HnswConfig::default()
            .with_dimension(2)
            .with_metric(alopex_core::Metric::L2)
            .with_m(8)
            .with_ef_construction(32);
        for name in ["cache_a", "cache_b"] {
            db.create_hnsw_index(name, config.clone()).unwrap();
        }
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        for name in ["cache_a", "cache_b"] {
            txn.upsert_to_hnsw(name, b"key", &[0.0, 0.0], b"seed")
                .unwrap();
        }
        txn.commit().unwrap();
        for name in ["cache_a", "cache_b"] {
            let (rows, stats) = db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].key, b"key");
            assert_eq!(rows[0].distance, 0.0);
            assert!(stats.nodes_visited > 0);
            assert!(db.hnsw_cache.read().unwrap().contains_key(name));
        }
        db
    }

    #[test]
    fn direct_hnsw_lifecycle_preserves_unrelated_cached_graphs() {
        let mut retained = Vec::new();
        for persistent in [false, true] {
            for operation in ["create", "replace", "drop", "compact"] {
                let directory = tempfile::tempdir().unwrap();
                let db = warm_direct_hnsw_indexes(if persistent {
                    Database::open(directory.path()).unwrap()
                } else {
                    Database::open_in_memory().unwrap()
                });
                if operation == "compact" {
                    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                    txn.upsert_to_hnsw("cache_a", b"deleted", &[1.0, 0.0], b"")
                        .unwrap();
                    txn.commit().unwrap();
                    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                    assert!(txn.delete_from_hnsw("cache_a", b"deleted").unwrap());
                    txn.commit().unwrap();
                    db.search_hnsw("cache_b", &[0.0, 0.0], 1, Some(8)).unwrap();
                }
                let before = db.hnsw_cache.read().unwrap().clone();
                let (epoch, stale) = db.hnsw_cache_snapshot();
                let config = alopex_core::HnswConfig::default()
                    .with_dimension(2)
                    .with_metric(alopex_core::Metric::L2);
                match operation {
                    "create" => db.create_hnsw_index("cache_c", config).unwrap(),
                    "replace" => db.create_hnsw_index("cache_a", config).unwrap(),
                    "drop" => db.drop_hnsw_index("cache_a").unwrap(),
                    "compact" => {
                        assert_eq!(db.compact_hnsw_index("cache_a").unwrap().vectors_removed, 1);
                    }
                    _ => unreachable!(),
                }
                // Inspect before a search can hide an unnecessary graph reload.
                let kept = db
                    .hnsw_cache
                    .read()
                    .unwrap()
                    .get("cache_b")
                    .is_some_and(|index| Arc::ptr_eq(index, &before["cache_b"]));
                retained.push(kept);
                assert_eq!(
                    db.hnsw_cache_epoch
                        .load(std::sync::atomic::Ordering::Acquire),
                    epoch + 1
                );
                db.hnsw_cache_insert_if_current(epoch, stale);
                if operation == "drop" {
                    assert!(db.search_hnsw("cache_a", &[0.0, 0.0], 1, Some(8)).is_err());
                } else {
                    let name = if operation == "create" {
                        "cache_c"
                    } else {
                        "cache_a"
                    };
                    let (rows, _) = db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap();
                    assert_eq!(rows.len(), usize::from(operation == "compact"));
                }
                // Bypass the cache to prove committed storage agrees with warm reads.
                let mut read = db.store.begin(TxnMode::ReadOnly).unwrap();
                for name in ["cache_a", "cache_b", "cache_c"] {
                    let persisted = alopex_core::vector::hnsw::HnswIndex::load(name, &mut read);
                    if (name == "cache_a" && operation == "drop")
                        || (name == "cache_c" && operation != "create")
                    {
                        assert!(persisted.is_err());
                    } else {
                        let (rows, _) = persisted.unwrap().search(&[0.0, 0.0], 1, Some(8)).unwrap();
                        let expected = usize::from(
                            name == "cache_b" || (name == "cache_a" && operation != "replace"),
                        );
                        assert_eq!(rows.len(), expected);
                        if expected == 1 {
                            assert_eq!(rows[0].key, b"key");
                            assert_eq!(rows[0].distance, 0.0);
                        }
                    }
                }
                read.rollback_self().unwrap();
                eprintln!("persistent={persistent} lifecycle={operation} unrelated_cache_retained={kept}; storage/epoch verified");
            }
        }
        assert_eq!(retained, vec![true; 8]);
    }

    #[test]
    fn owned_lsm_conflict_preserves_hnsw_cache_state() {
        let directory = tempfile::tempdir().unwrap();
        let db = warm_direct_hnsw_indexes(Database::open(directory.path()).unwrap());
        let mut losing = Arc::clone(&db)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        losing.put(b"conflict", b"loser").unwrap();
        losing
            .upsert_to_hnsw("cache_a", b"key", &[9.0, 0.0], b"loser")
            .unwrap();
        let mut winning = db.begin(TxnMode::ReadWrite).unwrap();
        winning.put(b"conflict", b"winner").unwrap();
        winning.commit().unwrap();
        let before = db.hnsw_cache.read().unwrap().clone();
        let epoch = db
            .hnsw_cache_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        assert!(matches!(
            losing.commit(),
            Err(Error::Core(alopex_core::Error::TxnConflict))
        ));
        assert!(matches!(
            losing.session().commit(),
            Err(alopex_core::Error::TxnClosed)
        ));
        assert_eq!(
            db.hnsw_cache_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            epoch
        );
        for name in ["cache_a", "cache_b"] {
            assert!(Arc::ptr_eq(
                &db.hnsw_cache.read().unwrap()[name],
                &before[name]
            ));
            let (rows, _) = db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap();
            assert_eq!(rows[0].key, b"key");
            assert_eq!(rows[0].distance, 0.0);
        }
        let mut read = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(read.get(b"conflict").unwrap(), Some(b"winner".to_vec()));
        read.rollback().unwrap();
    }

    #[test]
    fn unrelated_kv_commit_preserves_warmed_hnsw_cache() {
        let mut observations = Vec::new();
        for (owned, persistent) in [(false, false), (true, false), (false, true), (true, true)] {
            let directory = tempfile::tempdir().unwrap();
            let db = warm_direct_hnsw_indexes(if persistent {
                Database::open(directory.path()).unwrap()
            } else {
                Database::open_in_memory().unwrap()
            });
            let before = db.hnsw_cache.read().unwrap().clone();
            if owned {
                let mut txn = Arc::clone(&db)
                    .begin_owned_embedded_transaction(TxnMode::ReadWrite)
                    .unwrap();
                txn.put(b"application:unrelated", b"committed").unwrap();
                txn.commit().unwrap();
            } else {
                let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                txn.put(b"application:unrelated", b"committed").unwrap();
                txn.commit().unwrap();
            }
            // Observe before a search can hide invalidation by repopulating the cache.
            let retained = {
                let after = db.hnsw_cache.read().unwrap();
                ["cache_a", "cache_b"].map(|name| {
                    after
                        .get(name)
                        .is_some_and(|index| Arc::ptr_eq(index, &before[name]))
                })
            };
            let mut read = db.begin(TxnMode::ReadOnly).unwrap();
            assert_eq!(
                read.get(b"application:unrelated").unwrap(),
                Some(b"committed".to_vec())
            );
            read.rollback().unwrap();
            for name in ["cache_a", "cache_b"] {
                let (rows, _) = db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].key, b"key");
                assert_eq!(rows[0].distance, 0.0);
            }
            eprintln!(
                "unrelated KV commit owned={owned} persistent={persistent}: retained={retained:?}; committed value/search verified"
            );
            observations.push(retained);
        }
        // Both public transaction owners reach the oracle even on the old clear-all implementation.
        assert_eq!(observations, vec![[true, true]; 4]);
    }

    #[test]
    fn direct_hnsw_commit_invalidates_only_changed_index() {
        let mut observations = Vec::new();
        for (owned, persistent) in [(false, false), (true, false), (false, true), (true, true)] {
            let directory = tempfile::tempdir().unwrap();
            let db = warm_direct_hnsw_indexes(if persistent {
                Database::open(directory.path()).unwrap()
            } else {
                Database::open_in_memory().unwrap()
            });
            let before = db.hnsw_cache.read().unwrap().clone();
            if owned {
                let mut txn = Arc::clone(&db)
                    .begin_owned_embedded_transaction(TxnMode::ReadWrite)
                    .unwrap();
                txn.upsert_to_hnsw("cache_a", b"key", &[9.0, 0.0], b"updated")
                    .unwrap();
                txn.commit().unwrap();
            } else {
                let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                txn.upsert_to_hnsw("cache_a", b"key", &[9.0, 0.0], b"updated")
                    .unwrap();
                txn.commit().unwrap();
            }
            let (stale_a_retained, untouched_b_retained) = {
                let after = db.hnsw_cache.read().unwrap();
                (
                    after
                        .get("cache_a")
                        .is_some_and(|index| Arc::ptr_eq(index, &before["cache_a"])),
                    after
                        .get("cache_b")
                        .is_some_and(|index| Arc::ptr_eq(index, &before["cache_b"])),
                )
            };
            for (name, query) in [("cache_a", [9.0, 0.0]), ("cache_b", [0.0, 0.0])] {
                let (rows, _) = db.search_hnsw(name, &query, 1, Some(8)).unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].key, b"key");
                assert_eq!(rows[0].distance, 0.0);
            }
            eprintln!(
                "direct HNSW commit owned={owned} persistent={persistent}: stale_a={stale_a_retained}, retained_b={untouched_b_retained}; latest search verified"
            );
            observations.push((stale_a_retained, untouched_b_retained));
        }
        // A may be evicted or replaced; B must retain its already-loaded graph.
        assert_eq!(observations, vec![(false, true); 4]);
    }

    #[test]
    fn uncached_hnsw_commit_preserves_other_warmed_indexes() {
        let mut observations = Vec::new();
        for owned in [false, true] {
            let db = two_warmed_direct_hnsw_indexes();
            let config = alopex_core::HnswConfig::default()
                .with_dimension(2)
                .with_metric(alopex_core::Metric::L2)
                .with_m(8)
                .with_ef_construction(32);
            db.create_hnsw_index("cache_c", config).unwrap();
            let mut seed = db.begin(TxnMode::ReadWrite).unwrap();
            seed.upsert_to_hnsw("cache_c", b"key", &[0.0, 0.0], b"seed")
                .unwrap();
            seed.commit().unwrap();
            // Establish a cold C independently of transaction commit's cache publication.
            db.hnsw_cache.write().unwrap().remove("cache_c");
            for name in ["cache_a", "cache_b"] {
                db.search_hnsw(name, &[0.0, 0.0], 1, Some(8)).unwrap();
            }
            let before = db.hnsw_cache.read().unwrap().clone();
            assert!(!before.contains_key("cache_c"));
            if owned {
                let mut txn = Arc::clone(&db)
                    .begin_owned_embedded_transaction(TxnMode::ReadWrite)
                    .unwrap();
                txn.upsert_to_hnsw("cache_c", b"key", &[9.0, 0.0], b"updated")
                    .unwrap();
                txn.commit().unwrap();
            } else {
                let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                txn.upsert_to_hnsw("cache_c", b"key", &[9.0, 0.0], b"updated")
                    .unwrap();
                txn.commit().unwrap();
            }
            let retained = {
                let after = db.hnsw_cache.read().unwrap();
                ["cache_a", "cache_b"].map(|name| {
                    after
                        .get(name)
                        .is_some_and(|index| Arc::ptr_eq(index, &before[name]))
                })
            };
            for (name, query) in [
                ("cache_a", [0.0, 0.0]),
                ("cache_b", [0.0, 0.0]),
                ("cache_c", [9.0, 0.0]),
            ] {
                let (rows, _) = db.search_hnsw(name, &query, 1, Some(8)).unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].key, b"key");
                assert_eq!(rows[0].distance, 0.0);
            }
            eprintln!(
                "uncached HNSW commit owned={owned}: retained={retained:?}; latest search verified"
            );
            observations.push(retained);
        }
        assert_eq!(observations, vec![[true, true], [true, true]]);
    }

    #[test]
    fn hnsw_cache_key_classification_preserves_storage_boundaries() {
        let db = two_warmed_direct_hnsw_indexes();
        // The encoding does not forbid colons in index names. A node/key write
        // can match more than one cached prefix, so retain conservative matching.
        {
            let mut cache = db.hnsw_cache.write().unwrap();
            let index = Arc::clone(&cache["cache_a"]);
            cache.insert("cache_a:part".to_string(), index);
        }
        let cases: &[(&[u8], Option<&[&str]>)] = &[
            (b"hnsw:meta:cache_c", Some(&[])),
            (b"hnsw:meta:", Some(&[])),
            (b"hnsw:node:cache_c:part:0", Some(&[])),
            (b"hnsw:key:cache_c:\xff:\0", Some(&[])),
            (b"hnsw:key::", Some(&[])),
            (b"hnsw:node:cache_a_extra:0", Some(&[])),
            (b"hnsw:meta:cache_a:part", Some(&["cache_a:part"])),
            (
                b"hnsw:node:cache_a:part:0",
                Some(&["cache_a", "cache_a:part"]),
            ),
            (b"hnsw:key:cache_a:\xff:\0", Some(&["cache_a"])),
            (b"hnsw:node:cache_c", None),
            (b"hnsw:key:cache_c", None),
            (b"hnsw:future-format:cache_c", None),
        ];
        for (key, expected) in cases {
            let changed = db.hnsw_cache_changes(|visitor| {
                visitor(key);
                true
            });
            let expected =
                expected.map(|names| names.iter().map(|name| (*name).to_string()).collect());
            assert_eq!(changed, expected, "key={key:?}");
        }
    }

    #[test]
    fn raw_session_hnsw_write_invalidates_its_cached_graph() {
        let db = two_warmed_direct_hnsw_indexes();
        let before = db.hnsw_cache.read().unwrap().clone();
        let mut txn = Arc::clone(&db)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        txn.session()
            .with_transaction(|raw| {
                let key = b"hnsw:meta:cache_a".to_vec();
                let value = raw.get(&key)?.expect("real persisted HNSW metadata");
                raw.put(key, value)
            })
            .unwrap();
        txn.commit().unwrap();
        {
            let after = db.hnsw_cache.read().unwrap();
            assert!(!after.contains_key("cache_a"));
            assert!(Arc::ptr_eq(&before["cache_b"], &after["cache_b"]));
        }
        let (rows, _) = db.search_hnsw("cache_a", &[0.0, 0.0], 1, Some(8)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, b"key");
        assert_eq!(rows[0].distance, 0.0);
    }

    #[test]
    fn rolled_back_raw_hnsw_write_preserves_cached_graphs() {
        let db = two_warmed_direct_hnsw_indexes();
        let before = db.hnsw_cache.read().unwrap().clone();
        let mut txn = Arc::clone(&db)
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        let session = txn.session();
        let savepoint = session.create_savepoint().unwrap();
        session
            .with_transaction(|raw| {
                raw.put(b"hnsw:unknown:temporary".to_vec(), b"discarded".to_vec())
            })
            .unwrap();
        session.rollback_to_savepoint(savepoint).unwrap();
        txn.put(b"application:kept", b"committed").unwrap();
        txn.commit().unwrap();
        let after = db.hnsw_cache.read().unwrap();
        for name in ["cache_a", "cache_b"] {
            assert!(Arc::ptr_eq(&before[name], &after[name]));
        }
        drop(after);
        let mut read = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(read.get(b"hnsw:unknown:temporary").unwrap(), None);
        assert_eq!(
            read.get(b"application:kept").unwrap(),
            Some(b"committed".to_vec())
        );
        read.rollback().unwrap();
    }

    #[test]
    fn unknown_pending_keys_conservatively_clear_cached_graphs() {
        for complete in [false, true] {
            let db = two_warmed_direct_hnsw_indexes();
            let epoch = db
                .hnsw_cache_epoch
                .load(std::sync::atomic::Ordering::Acquire);
            db.commit_with_hnsw_cache_changes(
                || {
                    let changes = db.hnsw_cache_changes(|visitor| {
                        if complete {
                            visitor(b"hnsw:future-format:cache_a");
                        }
                        complete
                    });
                    assert!(changes.is_none());
                    Ok::<_, ()>(((), changes))
                },
                || {},
            )
            .unwrap();
            assert!(db.hnsw_cache.read().unwrap().is_empty());
            assert_eq!(
                db.hnsw_cache_epoch
                    .load(std::sync::atomic::Ordering::Acquire),
                epoch + 1
            );
        }
    }

    #[test]
    fn empty_hnsw_cache_skips_pending_key_visit() {
        let db = Database::open_in_memory().unwrap();
        let epoch = db
            .hnsw_cache_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let mut committed = false;
        let mut updated = false;
        db.commit_with_hnsw_cache_changes(
            || {
                let changes =
                    db.hnsw_cache_changes(|_| panic!("empty cache must not visit writes"));
                assert_eq!(changes, Some(std::collections::HashSet::new()));
                committed = true;
                Ok::<_, ()>(((), changes))
            },
            || updated = true,
        )
        .unwrap();
        assert!(committed && updated);
        assert_eq!(
            db.hnsw_cache_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            epoch + 1
        );
    }

    #[test]
    fn cold_reader_publication_cannot_cross_selective_commit() {
        use alopex_core::{HnswIndex, KVStore, KVTransaction};
        use std::sync::mpsc;
        use std::time::Duration;

        let db = two_warmed_direct_hnsw_indexes();
        db.hnsw_cache.write().unwrap().clear();
        let (mut old_read, epoch) = {
            let _gate = db.hnsw_cache_gate.read().unwrap();
            (
                db.store.begin(TxnMode::ReadOnly).unwrap(),
                db.hnsw_cache_epoch
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        };
        let old_graph = HnswIndex::load("cache_a", &mut old_read).unwrap();
        old_read.rollback_self().unwrap();
        let mut write = db.store.begin(TxnMode::ReadWrite).unwrap();
        let mut changed_graph = HnswIndex::load("cache_a", &mut write).unwrap();
        changed_graph
            .upsert(b"key", &[9.0, 0.0], b"committed")
            .unwrap();
        changed_graph.save(&mut write).unwrap();
        let (start_tx, start_rx) = mpsc::channel();
        let (progress_tx, progress_rx) = mpsc::channel();
        *db.hnsw_cache_publication_progress.lock().unwrap() = Some(progress_tx);
        let reader_db = Arc::clone(&db);
        let reader = std::thread::spawn(move || -> std::result::Result<(), String> {
            start_rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| error.to_string())?;
            reader_db.hnsw_cache_insert_if_current(epoch, vec![("cache_a".to_string(), old_graph)]);
            Ok(())
        });
        let committed = db.commit_with_hnsw_cache_changes(
            || -> std::result::Result<_, String> {
                let mut visited = false;
                let changes = db.hnsw_cache_changes(|_| {
                    visited = true;
                    true
                });
                if visited || changes != Some(std::collections::HashSet::new()) {
                    return Err("cold cache classification was not empty".to_string());
                }
                start_tx.send(()).map_err(|error| error.to_string())?;
                // A positive signal means the reader either completed publication (old
                // implementation) or encountered the held write gate (fixed implementation).
                // A timeout is an error, never permission to advance a successful test.
                progress_rx
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| error.to_string())?;
                write.commit_self().map_err(|error| error.to_string())?;
                Ok(((), changes))
            },
            || {},
        );
        // The commit helper releases its gate even on timeout/error before we join.
        let reader_result = reader.join();
        *db.hnsw_cache_publication_progress.lock().unwrap() = None;
        committed.expect("writer and reader must reach the controlled ordering");
        reader_result
            .unwrap()
            .expect("reader must complete after gate release");
        let stale_cached = db.hnsw_cache.read().unwrap().contains_key("cache_a");
        let (rows, stats) = db.search_hnsw("cache_a", &[9.0, 0.0], 1, Some(8)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, b"key");
        assert!(stats.nodes_visited > 0);
        eprintln!(
            "cold publication race: stale_cached={stale_cached}, latest_distance={}",
            rows[0].distance
        );
        assert_eq!((stale_cached, rows[0].distance), (false, 0.0));
    }

    fn hnsw_cache_test_fixture() -> (Database, String, String, i32) {
        const DIMENSION: usize = 1024;
        const ROWS: usize = 1025;

        let db = Database::open_in_memory().unwrap();
        let stored_vector = format!("[{}]", vec!["1.0"; DIMENSION].join(", "));
        let query_vector = format!("[{}]", vec!["0.0"; DIMENSION].join(", "));
        db.execute_sql(&format!(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSION}, L2));\
             CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;"
        ))
        .unwrap();
        for start in (0..ROWS).step_by(16) {
            let end = (start + 16).min(ROWS);
            let values = (start..end)
                .map(|id| format!("({id}, {stored_vector})"))
                .collect::<Vec<_>>()
                .join(", ");
            db.execute_sql(&format!(
                "INSERT INTO items (id, embedding) VALUES {values}"
            ))
            .unwrap();
        }
        let query = format!(
            "SELECT id FROM items \
             ORDER BY vector_distance(embedding, {query_vector}, 'l2') ASC LIMIT 1"
        );
        (db, query_vector, query, ROWS as i32)
    }

    #[test]
    fn auto_commit_knn_select_populates_database_hnsw_cache() {
        let (db, _, query, _) = hnsw_cache_test_fixture();
        assert!(db.hnsw_cache.read().unwrap().is_empty());

        db.execute_sql(&query).unwrap();
        let first = db
            .hnsw_cache
            .read()
            .unwrap()
            .get("idx_items_embedding")
            .cloned()
            .expect("a normal SQL kNN SELECT must populate the cache");

        db.execute_sql(&query).unwrap();
        let second = db
            .hnsw_cache
            .read()
            .unwrap()
            .get("idx_items_embedding")
            .cloned()
            .expect("the cached index must remain available for the next normal SELECT");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn committed_sql_write_invalidates_hnsw_cache() {
        let (db, query_vector, query, new_id) = hnsw_cache_test_fixture();
        let mut transaction = db.begin(TxnMode::ReadWrite).unwrap();
        transaction
            .execute_sql(&format!(
                "INSERT INTO items (id, embedding) VALUES ({new_id}, {query_vector})"
            ))
            .unwrap();

        db.execute_sql(&query).unwrap();
        assert!(db
            .hnsw_cache
            .read()
            .unwrap()
            .contains_key("idx_items_embedding"));
        transaction.commit().unwrap();

        let ExecutionResult::Query(result) = db.execute_sql(&query).unwrap() else {
            panic!("kNN SELECT must return a query result");
        };
        assert_eq!(result.rows, vec![vec![SqlValue::Integer(new_id)]]);
    }

    #[test]
    fn hnsw_cache_rejects_entries_copied_before_a_committed_write() {
        let (db, query_vector, query, new_id) = hnsw_cache_test_fixture();
        db.execute_sql(&query).unwrap();
        let (epoch, stale_entries) = db.hnsw_cache_snapshot();

        let mut transaction = db.begin(TxnMode::ReadWrite).unwrap();
        transaction
            .execute_sql(&format!(
                "INSERT INTO items (id, embedding) VALUES ({new_id}, {query_vector})"
            ))
            .unwrap();
        transaction.commit().unwrap();

        // Model a read that started before the commit and completed its HNSW
        // load after the cache transition. The old generation must not be
        // re-published into the current cache.
        db.hnsw_cache.write().unwrap().clear();
        db.hnsw_cache_insert_if_current(epoch, stale_entries);
        assert!(db.hnsw_cache.read().unwrap().is_empty());

        let ExecutionResult::Query(result) = db.execute_sql(&query).unwrap() else {
            panic!("kNN SELECT must return a query result");
        };
        assert_eq!(result.rows, vec![vec![SqlValue::Integer(new_id)]]);
    }

    #[test]
    fn auto_commit_knn_query_does_not_republish_stale_cache_after_write() {
        let (db, query_vector, query, new_id) = hnsw_cache_test_fixture();
        let db = Arc::new(db);
        db.execute_sql(&query).unwrap();

        let after_executor_barrier = Arc::new(Barrier::new(2));
        *db.hnsw_cache_after_executor_barrier.lock().unwrap() =
            Some(Arc::clone(&after_executor_barrier));
        let write_gate_barrier = Arc::new(Barrier::new(2));
        *db.hnsw_cache_write_gate_barrier.lock().unwrap() = Some(Arc::clone(&write_gate_barrier));
        let (write_gate_acquired_tx, write_gate_acquired_rx) = mpsc::channel();
        *db.hnsw_cache_write_gate_acquired.lock().unwrap() = Some(write_gate_acquired_tx);
        let (read_done_tx, read_done_rx) = mpsc::channel();
        let (write_done_tx, write_done_rx) = mpsc::channel();

        let write_gate_acquired_before_reader_release = std::thread::scope(|scope| {
            let reader_db = Arc::clone(&db);
            let reader_query = query.clone();
            scope.spawn(move || {
                reader_db.execute_sql(&reader_query).unwrap();
                read_done_tx.send(()).unwrap();
            });
            after_executor_barrier.wait();

            let writer_db = Arc::clone(&db);
            scope.spawn(move || {
                writer_db
                    .execute_sql(&format!(
                        "INSERT INTO items (id, embedding) VALUES ({new_id}, {query_vector})"
                    ))
                    .unwrap();
                write_done_tx.send(()).unwrap();
            });

            write_gate_barrier.wait();
            write_gate_barrier.wait();
            let write_gate_acquired_before_reader_release = write_gate_acquired_rx
                .recv_timeout(Duration::from_secs(1))
                .is_ok();
            after_executor_barrier.wait();
            read_done_rx.recv().unwrap();
            write_done_rx.recv().unwrap();
            write_gate_acquired_before_reader_release
        });
        *db.hnsw_cache_after_executor_barrier.lock().unwrap() = None;
        *db.hnsw_cache_write_gate_barrier.lock().unwrap() = None;
        *db.hnsw_cache_write_gate_acquired.lock().unwrap() = None;

        let ExecutionResult::Query(result) = db.execute_sql(&query).unwrap() else {
            panic!("kNN SELECT must return a query result");
        };
        assert_eq!(result.rows, vec![vec![SqlValue::Integer(new_id)]]);
        assert!(
            !write_gate_acquired_before_reader_release,
            "a write commit must not overtake an in-flight auto-commit kNN read"
        );
    }
}
