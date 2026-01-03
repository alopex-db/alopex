use alopex_core::kv::KVStore;
use alopex_core::types::TxnMode;
use alopex_core::KVTransaction;
use alopex_sql::catalog::CatalogOverlay;
use alopex_sql::catalog::TxnCatalogView;
use alopex_sql::executor::query::execute_query_streaming;
use alopex_sql::executor::query::RowIterator;
use alopex_sql::executor::{
    build_streaming_pipeline, ColumnInfo, ExecutionConfig, Executor, QueryRowIterator, Row,
};
use alopex_sql::planner::typed_expr::Projection;
use alopex_sql::storage::{SqlValue, TxnBridge};
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

        let requires_write = stmts.iter().any(stmt_requires_write);
        let mode = if requires_write {
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

        let config = ExecutionConfig::default();
        let mut last = alopex_sql::ExecutionResult::Success;
        for stmt in &stmts {
            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt)?
            };

            last = executor
                .execute_in_txn(plan, &config, &mut borrowed)
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
        if requires_write {
            let mut cache = self.hnsw_cache.write().expect("hnsw cache lock poisoned");
            cache.clear();
            let mut vector_cache = self
                .vector_cache
                .write()
                .expect("vector cache lock poisoned");
            *vector_cache = None;
        }
        Ok(last)
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
    /// // Process rows with streaming - transaction stays alive during callback
    /// let result = db.execute_sql_with_rows("SELECT * FROM users;", |mut rows| {
    ///     let mut names = Vec::new();
    ///     while let Ok(Some(row)) = rows.next_row() {
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
        if stmts.len() == 1 && matches!(stmts[0].kind, StatementKind::Select(_)) {
            let stmt = &stmts[0];
            let mode = TxnMode::ReadOnly;

            let mut txn = self.store.begin(mode).map_err(Error::Core)?;
            let mut overlay = CatalogOverlay::new();
            let mut borrowed =
                TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut txn, mode, &mut overlay);

            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay_ref) = borrowed.split_parts();
                plan_stmt(&*catalog, overlay_ref, stmt)?
            };

            let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
            let (mut sql_txn, overlay_ref) = borrowed.split_parts();
            let view = TxnCatalogView::new(&*catalog, overlay_ref);

            // Build streaming pipeline - iterator lifetime tied to sql_txn
            let config = ExecutionConfig::default();
            let (iter, projection, schema) =
                build_streaming_pipeline(&mut sql_txn, &view, &config, plan)
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
    /// use alopex_embedded::{Database, SqlStreamingResult};
    ///
    /// let db = Database::new();
    /// db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").unwrap();
    /// db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');").unwrap();
    ///
    /// let result = db.execute_sql_streaming("SELECT * FROM users;").unwrap();
    /// if let SqlStreamingResult::Query(mut iter) = result {
    ///     while let Ok(Some(row)) = iter.next_row() {
    ///         println!("{:?}", row);
    ///     }
    /// }
    /// ```
    pub fn execute_sql_streaming(&self, sql: &str) -> Result<SqlStreamingResult> {
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(SqlStreamingResult::Success);
        }

        // For streaming, only support single SELECT statement
        if stmts.len() == 1 && matches!(stmts[0].kind, StatementKind::Select(_)) {
            let stmt = &stmts[0];
            let mode = TxnMode::ReadOnly;

            let mut txn = self.store.begin(mode).map_err(Error::Core)?;
            let mut overlay = CatalogOverlay::new();
            let mut borrowed =
                TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(&mut txn, mode, &mut overlay);

            let plan = {
                let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt)?
            };

            let (mut sql_txn, _overlay) = borrowed.split_parts();

            let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
            let view = TxnCatalogView::new(&*catalog, _overlay);
            let config = ExecutionConfig::default();
            let iter = execute_query_streaming(&mut sql_txn, &view, &config, plan)
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
            let mut cache = self
                .db
                .hnsw_cache
                .write()
                .expect("hnsw cache lock poisoned");
            cache.clear();
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
        let mode = txn.mode();

        let mut borrowed =
            TxnBridge::<alopex_core::kv::AnyKV>::wrap_external(txn, mode, &mut self.overlay);
        let mut executor: Executor<_, _> = Executor::new(store, sql_catalog.clone());

        let config = ExecutionConfig::default();
        let mut last = alopex_sql::ExecutionResult::Success;
        for stmt in &stmts {
            let plan = {
                let catalog = sql_catalog.read().expect("catalog lock poisoned");
                let (_, overlay) = borrowed.split_parts();
                plan_stmt(&*catalog, &*overlay, stmt)?
            };

            last = executor
                .execute_in_txn(plan, &config, &mut borrowed)
                .map_err(|e| Error::Sql(alopex_sql::SqlError::from(e)))?;
        }

        Ok(last)
    }
}
