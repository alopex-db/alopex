use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use alopex_core::async_runtime::{BoxFuture, BoxStream, MaybeSend};
use alopex_core::kv::async_kv::AsyncKVTransaction;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::txn::TxnManager;
use alopex_core::types::{Key, TxnId, TxnMode, Value};
use alopex_core::vector::hnsw::{HnswIndex, HnswTransactionState};

use crate::ast::Statement;
use crate::catalog::{Catalog, TableMetadata};
use crate::dialect::AlopexDialect;
use crate::executor::{ExecutionResult, ExecutorError, Result as ExecResult, Row, ddl, dml, query};
use crate::parser::Parser;
use crate::planner::{LogicalPlan, Planner};
use crate::storage::bridge::HnswTxnEntry;
use crate::storage::error::{Result as StorageResult, StorageError};
use crate::storage::{KeyEncoder, RowCodec};

use futures::{Stream, stream::StreamExt};

struct AsyncTxnState<T> {
    txn: Option<T>,
    hnsw_indices: HashMap<String, HnswTxnEntry>,
}

impl<T> AsyncTxnState<T> {
    fn new(txn: T) -> Self {
        Self {
            txn: Some(txn),
            hnsw_indices: HashMap::new(),
        }
    }
}

/// Async transaction bridge wrapping an async KV transaction.
pub struct AsyncTxnBridge<'txn, T>
where
    T: for<'a> AsyncKVTransaction<'a>,
{
    state: Arc<tokio::sync::Mutex<AsyncTxnState<T>>>,
    mode: TxnMode,
    catalog: Option<Arc<RwLock<dyn Catalog + Send + Sync>>>,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn, T> AsyncTxnBridge<'txn, T>
where
    T: for<'a> AsyncKVTransaction<'a>,
{
    /// Create a new async bridge with an explicit transaction mode.
    pub fn new(txn: T, mode: TxnMode) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(AsyncTxnState::new(txn))),
            mode,
            catalog: None,
            _marker: PhantomData,
        }
    }

    /// Create a new async bridge with a catalog handle for SQL execution.
    pub fn with_catalog(
        txn: T,
        mode: TxnMode,
        catalog: Arc<RwLock<dyn Catalog + Send + Sync>>,
    ) -> Self {
        let mut bridge = Self::new(txn, mode);
        bridge.catalog = Some(catalog);
        bridge
    }

    /// Returns the transaction mode.
    pub fn mode(&self) -> TxnMode {
        self.mode
    }

    /// Fetch a row by row id.
    pub fn async_get_row<'a>(
        &'a self,
        table: &'a TableMetadata,
        row_id: u64,
    ) -> BoxFuture<'a, StorageResult<Option<Row>>> {
        Box::pin(async move {
            let guard = self.state.lock().await;
            let txn = guard.txn.as_ref().ok_or(StorageError::TransactionClosed)?;
            let key = KeyEncoder::row_key(table.table_id, row_id);
            let value = txn.async_get(&key).await?;
            match value {
                Some(bytes) => {
                    let row = RowCodec::decode(&bytes)?;
                    Ok(Some(Row::new(row_id, row)))
                }
                None => Ok(None),
            }
        })
    }

    /// Insert or update a row by row id.
    pub fn async_put_row<'a>(
        &'a self,
        table: &'a TableMetadata,
        row: Row,
    ) -> BoxFuture<'a, StorageResult<()>> {
        Box::pin(async move {
            if self.mode != TxnMode::ReadWrite {
                return Err(StorageError::TransactionReadOnly);
            }
            let mut guard = self.state.lock().await;
            let txn = guard.txn.as_mut().ok_or(StorageError::TransactionClosed)?;
            let key = KeyEncoder::row_key(table.table_id, row.row_id);
            let encoded = RowCodec::encode(&row.values);
            txn.async_put(&key, &encoded).await?;
            Ok(())
        })
    }

    /// Scan all rows in a table (streaming).
    pub fn async_scan_table<'a>(
        &'a self,
        table: &'a TableMetadata,
    ) -> BoxStream<'a, StorageResult<Row>>
    where
        T: Send + 'static,
    {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let state = Arc::clone(&self.state);
        let table_id = table.table_id;

        tokio::spawn(async move {
            let (txn, hnsw_indices) = {
                let mut guard = state.lock().await;
                let txn = match guard.txn.take() {
                    Some(txn) => txn,
                    None => {
                        let _ = sender.send(Err(StorageError::TransactionClosed)).await;
                        return;
                    }
                };
                let hnsw = std::mem::take(&mut guard.hnsw_indices);
                (txn, hnsw)
            };

            let prefix = KeyEncoder::table_prefix(table_id);
            let mut stream = txn.async_scan_prefix(&prefix);
            while let Some(entry) = stream.next().await {
                match entry {
                    Ok((key, value)) => {
                        let (decoded_table, row_id) = match KeyEncoder::decode_row_key(&key) {
                            Ok(decoded) => decoded,
                            Err(err) => {
                                let _ = sender.send(Err(err)).await;
                                break;
                            }
                        };
                        if decoded_table != table_id {
                            let _ = sender.send(Err(StorageError::InvalidKeyFormat)).await;
                            break;
                        }
                        let values = match RowCodec::decode(&value) {
                            Ok(row) => row,
                            Err(err) => {
                                let _ = sender.send(Err(err)).await;
                                break;
                            }
                        };
                        if sender.send(Ok(Row::new(row_id, values))).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = sender.send(Err(StorageError::from(err))).await;
                        break;
                    }
                }
            }

            drop(stream);
            let mut guard = state.lock().await;
            guard.txn = Some(txn);
            guard.hnsw_indices = hnsw_indices;
        });

        Box::pin(ReceiverStream { receiver })
    }

    /// Commit the transaction (storage-level).
    pub fn async_commit(self) -> BoxFuture<'txn, StorageResult<()>>
    where
        T: Send + 'static,
    {
        let state = self.state;
        let mode = self.mode;
        Box::pin(async move {
            let (txn, hnsw_indices) = {
                let mut guard = state.lock().await;
                let txn = guard.txn.take().ok_or(StorageError::TransactionClosed)?;
                let hnsw = std::mem::take(&mut guard.hnsw_indices);
                (txn, hnsw)
            };
            let handle = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let mut blocking_txn = BlockingSqlTransaction::new(txn, mode, handle, hnsw_indices);
                blocking_txn.commit_hnsw()?;
                blocking_txn.inner.commit_self().map_err(StorageError::from)
            })
            .await
            .map_err(|_| StorageError::TransactionClosed)?
        })
    }

    /// Roll back the transaction (storage-level).
    pub fn async_rollback(self) -> BoxFuture<'txn, StorageResult<()>>
    where
        T: Send + 'static,
    {
        let state = self.state;
        let mode = self.mode;
        Box::pin(async move {
            let (txn, hnsw_indices) = {
                let mut guard = state.lock().await;
                let txn = guard.txn.take().ok_or(StorageError::TransactionClosed)?;
                let hnsw = std::mem::take(&mut guard.hnsw_indices);
                (txn, hnsw)
            };
            let handle = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let mut blocking_txn = BlockingSqlTransaction::new(txn, mode, handle, hnsw_indices);
                blocking_txn.rollback_hnsw()?;
                blocking_txn
                    .inner
                    .rollback_self()
                    .map_err(StorageError::from)
            })
            .await
            .map_err(|_| StorageError::TransactionClosed)?
        })
    }
}

/// Async SQL transaction trait for executing SQL within a transaction context.
pub trait AsyncSqlTransaction<'txn>: MaybeSend {
    fn async_execute<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, ExecResult<ExecutionResult>>;
    fn async_query<'a>(&'a self, sql: &'a str) -> BoxStream<'a, ExecResult<Row>>;
    fn async_commit(self) -> BoxFuture<'txn, ExecResult<()>>;
    fn async_rollback(self) -> BoxFuture<'txn, ExecResult<()>>;
}

impl<'txn, T> AsyncSqlTransaction<'txn> for AsyncTxnBridge<'txn, T>
where
    T: for<'a> AsyncKVTransaction<'a> + Send + 'static,
{
    fn async_execute<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, ExecResult<ExecutionResult>> {
        let catalog = match self.catalog.clone() {
            Some(catalog) => catalog,
            None => {
                return Box::pin(async move {
                    Err(ExecutorError::InvalidOperation {
                        operation: "async_execute".into(),
                        reason: "catalog not configured".into(),
                    })
                });
            }
        };
        let sql = sql.to_string();
        let state = Arc::clone(&self.state);
        let mode = self.mode;
        Box::pin(async move {
            let (txn, hnsw_indices) = {
                let mut guard = state.lock().await;
                let txn = guard
                    .txn
                    .take()
                    .ok_or(ExecutorError::Storage(StorageError::TransactionClosed))?;
                let hnsw = std::mem::take(&mut guard.hnsw_indices);
                (txn, hnsw)
            };

            let handle = tokio::runtime::Handle::current();
            let join = tokio::task::spawn_blocking(move || {
                let mut blocking_txn = BlockingSqlTransaction::new(txn, mode, handle, hnsw_indices);
                let result = execute_sql_blocking(&mut blocking_txn, &catalog, &sql, mode);
                let (txn, hnsw) = blocking_txn.into_parts();
                (result, txn, hnsw)
            });

            let (result, txn, hnsw_indices) =
                join.await.map_err(|_| ExecutorError::InvalidOperation {
                    operation: "async_execute".into(),
                    reason: "blocking task cancelled".into(),
                })?;

            let mut guard = state.lock().await;
            guard.txn = Some(txn);
            guard.hnsw_indices = hnsw_indices;

            result
        })
    }

    fn async_query<'a>(&'a self, sql: &'a str) -> BoxStream<'a, ExecResult<Row>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let catalog = match self.catalog.clone() {
            Some(catalog) => catalog,
            None => {
                let _ = sender.try_send(Err(ExecutorError::InvalidOperation {
                    operation: "async_query".into(),
                    reason: "catalog not configured".into(),
                }));
                return Box::pin(ReceiverStream { receiver });
            }
        };
        let sql = sql.to_string();
        let state = Arc::clone(&self.state);
        let mode = self.mode;
        let sender_for_task = sender.clone();

        tokio::spawn(async move {
            let (txn, hnsw_indices) = {
                let mut guard = state.lock().await;
                let txn = match guard.txn.take() {
                    Some(txn) => txn,
                    None => {
                        let _ = sender_for_task
                            .send(Err(ExecutorError::Storage(StorageError::TransactionClosed)))
                            .await;
                        return;
                    }
                };
                let hnsw = std::mem::take(&mut guard.hnsw_indices);
                (txn, hnsw)
            };

            let handle = tokio::runtime::Handle::current();
            let sender_blocking = sender.clone();
            let join = tokio::task::spawn_blocking(move || {
                let mut blocking_txn = BlockingSqlTransaction::new(txn, mode, handle, hnsw_indices);
                stream_query_blocking(&mut blocking_txn, &catalog, &sql, sender_blocking);
                blocking_txn.into_parts()
            });

            match join.await {
                Ok((txn, hnsw_indices)) => {
                    let mut guard = state.lock().await;
                    guard.txn = Some(txn);
                    guard.hnsw_indices = hnsw_indices;
                }
                Err(_) => {
                    let _ = sender_for_task
                        .send(Err(ExecutorError::InvalidOperation {
                            operation: "async_query".into(),
                            reason: "blocking task cancelled".into(),
                        }))
                        .await;
                }
            }
        });

        Box::pin(ReceiverStream { receiver })
    }

    fn async_commit(self) -> BoxFuture<'txn, ExecResult<()>> {
        Box::pin(async move {
            AsyncTxnBridge::async_commit(self)
                .await
                .map_err(ExecutorError::from)
        })
    }

    fn async_rollback(self) -> BoxFuture<'txn, ExecResult<()>> {
        Box::pin(async move {
            AsyncTxnBridge::async_rollback(self)
                .await
                .map_err(ExecutorError::from)
        })
    }
}

fn execute_sql_blocking<T>(
    txn: &mut BlockingSqlTransaction<T>,
    catalog: &Arc<RwLock<dyn Catalog + Send + Sync>>,
    sql: &str,
    mode: TxnMode,
) -> ExecResult<ExecutionResult>
where
    T: for<'a> AsyncKVTransaction<'a>,
{
    let statements = parse_sql(sql)?;
    if statements.is_empty() {
        return Err(ExecutorError::InvalidOperation {
            operation: "execute_sql".into(),
            reason: "empty SQL".into(),
        });
    }

    let mut last = ExecutionResult::Success;
    for stmt in statements {
        let plan = {
            let guard = catalog.read().expect("catalog lock poisoned");
            Planner::new(&*guard).plan(&stmt)?
        };

        let op_name = plan.operation_name();
        last = match plan {
            LogicalPlan::CreateTable {
                table,
                if_not_exists,
                with_options,
            } => {
                ensure_write(mode, op_name)?;
                let mut guard = catalog.write().expect("catalog lock poisoned");
                ddl::create_table::execute_create_table(
                    txn,
                    &mut *guard,
                    table,
                    with_options,
                    if_not_exists,
                )?
            }
            LogicalPlan::DropTable { name, if_exists } => {
                ensure_write(mode, op_name)?;
                let mut guard = catalog.write().expect("catalog lock poisoned");
                ddl::drop_table::execute_drop_table(txn, &mut *guard, &name, if_exists)?
            }
            LogicalPlan::CreateIndex {
                index,
                if_not_exists,
            } => {
                ensure_write(mode, op_name)?;
                let mut guard = catalog.write().expect("catalog lock poisoned");
                ddl::create_index::execute_create_index(txn, &mut *guard, index, if_not_exists)?
            }
            LogicalPlan::DropIndex { name, if_exists } => {
                ensure_write(mode, op_name)?;
                let mut guard = catalog.write().expect("catalog lock poisoned");
                ddl::drop_index::execute_drop_index(txn, &mut *guard, &name, if_exists)?
            }
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => {
                ensure_write(mode, op_name)?;
                let guard = catalog.read().expect("catalog lock poisoned");
                dml::execute_insert(txn, &*guard, &table, columns, values)?
            }
            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                ensure_write(mode, op_name)?;
                let guard = catalog.read().expect("catalog lock poisoned");
                dml::execute_update(txn, &*guard, &table, assignments, filter)?
            }
            LogicalPlan::Delete { table, filter } => {
                ensure_write(mode, op_name)?;
                let guard = catalog.read().expect("catalog lock poisoned");
                dml::execute_delete(txn, &*guard, &table, filter)?
            }
            query_plan => {
                let guard = catalog.read().expect("catalog lock poisoned");
                query::execute_query(txn, &*guard, query_plan)?
            }
        };
    }

    Ok(last)
}

fn stream_query_blocking<T>(
    txn: &mut BlockingSqlTransaction<T>,
    catalog: &Arc<RwLock<dyn Catalog + Send + Sync>>,
    sql: &str,
    sender: tokio::sync::mpsc::Sender<ExecResult<Row>>,
) where
    T: for<'a> AsyncKVTransaction<'a>,
{
    let statements = match parse_sql(sql) {
        Ok(stmts) => stmts,
        Err(err) => {
            let _ = sender.blocking_send(Err(err));
            return;
        }
    };
    if statements.len() != 1 {
        let _ = sender.blocking_send(Err(ExecutorError::InvalidOperation {
            operation: "async_query".into(),
            reason: "expected a single statement".into(),
        }));
        return;
    }

    let plan = {
        let guard = catalog.read().expect("catalog lock poisoned");
        match Planner::new(&*guard).plan(&statements[0]) {
            Ok(plan) => plan,
            Err(err) => {
                let _ = sender.blocking_send(Err(err.into()));
                return;
            }
        }
    };

    if !is_query_plan(&plan) {
        let _ = sender.blocking_send(Err(ExecutorError::InvalidOperation {
            operation: "async_query".into(),
            reason: "query API requires SELECT".into(),
        }));
        return;
    }

    let guard = catalog.read().expect("catalog lock poisoned");
    let mut iter = match query::execute_query_streaming(txn, &*guard, plan) {
        Ok(iter) => iter,
        Err(err) => {
            let _ = sender.blocking_send(Err(err));
            return;
        }
    };

    let mut row_id = 0u64;
    loop {
        match iter.next_row() {
            Ok(Some(values)) => {
                row_id = row_id.saturating_add(1);
                if sender.blocking_send(Ok(Row::new(row_id, values))).is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = sender.blocking_send(Err(err));
                break;
            }
        }
    }
}

fn parse_sql(sql: &str) -> ExecResult<Vec<Statement>> {
    Parser::parse_sql(&AlopexDialect, sql).map_err(|err| ExecutorError::InvalidOperation {
        operation: "parse_sql".into(),
        reason: err.to_string(),
    })
}

fn ensure_write(mode: TxnMode, operation: &str) -> ExecResult<()> {
    if mode != TxnMode::ReadWrite {
        return Err(ExecutorError::ReadOnlyTransaction {
            operation: operation.to_string(),
        });
    }
    Ok(())
}

fn is_query_plan(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Scan { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
    )
}

struct ReceiverStream<T> {
    receiver: tokio::sync::mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        self.receiver.poll_recv(cx)
    }
}

struct BlockingSqlTransaction<T> {
    inner: BlockingKVTransaction<T>,
    mode: TxnMode,
    hnsw_indices: HashMap<String, HnswTxnEntry>,
}

impl<T> BlockingSqlTransaction<T>
where
    T: for<'a> AsyncKVTransaction<'a>,
{
    fn new(
        txn: T,
        mode: TxnMode,
        handle: tokio::runtime::Handle,
        hnsw_indices: HashMap<String, HnswTxnEntry>,
    ) -> Self {
        Self {
            inner: BlockingKVTransaction::new(txn, mode, handle),
            mode,
            hnsw_indices,
        }
    }

    fn into_parts(self) -> (T, HashMap<String, HnswTxnEntry>) {
        (self.inner.into_inner(), self.hnsw_indices)
    }

    fn commit_hnsw(&mut self) -> StorageResult<()> {
        for entry in self.hnsw_indices.values_mut() {
            if entry.dirty {
                entry
                    .index
                    .commit_staged(&mut self.inner, &mut entry.state)?;
            }
        }
        self.hnsw_indices.clear();
        Ok(())
    }

    fn rollback_hnsw(&mut self) -> StorageResult<()> {
        for entry in self.hnsw_indices.values_mut() {
            if entry.dirty {
                entry.index.rollback(&mut entry.state)?;
            }
        }
        self.hnsw_indices.clear();
        Ok(())
    }
}

impl<'txn, T> crate::storage::bridge::SqlTxn<'txn, BlockingKVStore<T>> for BlockingSqlTransaction<T>
where
    T: for<'a> AsyncKVTransaction<'a> + 'txn,
{
    fn mode(&self) -> TxnMode {
        self.mode
    }

    fn ensure_write_txn(&self) -> alopex_core::Result<()> {
        if self.mode != TxnMode::ReadWrite {
            return Err(alopex_core::Error::TxnConflict);
        }
        Ok(())
    }

    fn inner_mut(&mut self) -> &mut BlockingKVTransaction<T> {
        &mut self.inner
    }

    fn hnsw_entry(&mut self, name: &str) -> alopex_core::Result<&HnswIndex> {
        if !self.hnsw_indices.contains_key(name) {
            let index = HnswIndex::load(name, &mut self.inner)?;
            self.hnsw_indices.insert(
                name.to_string(),
                HnswTxnEntry {
                    index,
                    state: HnswTransactionState::default(),
                    dirty: false,
                },
            );
        }
        Ok(&self.hnsw_indices.get(name).expect("inserted above").index)
    }

    fn hnsw_entry_mut(&mut self, name: &str) -> alopex_core::Result<&mut HnswTxnEntry> {
        if !self.hnsw_indices.contains_key(name) {
            let index = HnswIndex::load(name, &mut self.inner)?;
            self.hnsw_indices.insert(
                name.to_string(),
                HnswTxnEntry {
                    index,
                    state: HnswTransactionState::default(),
                    dirty: false,
                },
            );
        }
        Ok(self.hnsw_indices.get_mut(name).expect("inserted above"))
    }

    fn flush_hnsw(&mut self) -> StorageResult<()> {
        self.commit_hnsw()
    }

    fn abandon_hnsw(&mut self) -> StorageResult<()> {
        self.rollback_hnsw()
    }

    fn delete_prefix(&mut self, prefix: &[u8]) -> StorageResult<()> {
        const BATCH: usize = 512;
        loop {
            let mut keys = Vec::with_capacity(BATCH);
            {
                let iter = self.inner.scan_prefix(prefix)?;
                for (key, _) in iter.take(BATCH) {
                    keys.push(key);
                }
            }

            if keys.is_empty() {
                break;
            }

            for key in keys {
                self.inner.delete(key)?;
            }
        }

        Ok(())
    }
}

struct BlockingKVStore<T>(PhantomData<fn() -> T>);

struct BlockingTxnManager<T>(PhantomData<fn() -> T>);

impl<'a, T> TxnManager<'a, BlockingKVTransaction<T>> for BlockingTxnManager<T> {
    fn begin(&'a self, _mode: TxnMode) -> alopex_core::Result<BlockingKVTransaction<T>> {
        Err(alopex_core::Error::InvalidFormat(
            "blocking store has no begin".into(),
        ))
    }

    fn commit(&'a self, _txn: BlockingKVTransaction<T>) -> alopex_core::Result<()> {
        Err(alopex_core::Error::InvalidFormat(
            "blocking store has no commit".into(),
        ))
    }

    fn rollback(&'a self, _txn: BlockingKVTransaction<T>) -> alopex_core::Result<()> {
        Err(alopex_core::Error::InvalidFormat(
            "blocking store has no rollback".into(),
        ))
    }
}

impl<T> KVStore for BlockingKVStore<T>
where
    T: for<'a> AsyncKVTransaction<'a>,
{
    type Transaction<'a>
        = BlockingKVTransaction<T>
    where
        Self: 'a;

    type Manager<'a>
        = BlockingTxnManager<T>
    where
        Self: 'a;

    fn txn_manager(&self) -> Self::Manager<'_> {
        BlockingTxnManager(PhantomData)
    }

    fn begin(&self, _mode: TxnMode) -> alopex_core::Result<Self::Transaction<'_>> {
        Err(alopex_core::Error::InvalidFormat(
            "blocking store has no begin".into(),
        ))
    }
}

struct BlockingKVTransaction<T> {
    txn: T,
    handle: tokio::runtime::Handle,
    mode: TxnMode,
    id: TxnId,
    scan_prefix_buf: Vec<u8>,
    scan_range_start: Vec<u8>,
    scan_range_end: Vec<u8>,
}

impl<T> BlockingKVTransaction<T> {
    fn new(txn: T, mode: TxnMode, handle: tokio::runtime::Handle) -> Self {
        Self {
            txn,
            handle,
            mode,
            id: TxnId(0),
            scan_prefix_buf: Vec::new(),
            scan_range_start: Vec::new(),
            scan_range_end: Vec::new(),
        }
    }

    fn into_inner(self) -> T {
        self.txn
    }
}

impl<'a, T> KVTransaction<'a> for BlockingKVTransaction<T>
where
    T: AsyncKVTransaction<'a>,
{
    fn id(&self) -> TxnId {
        self.id
    }

    fn mode(&self) -> TxnMode {
        self.mode
    }

    fn get(&mut self, key: &Key) -> alopex_core::Result<Option<Value>> {
        self.handle.block_on(self.txn.async_get(key))
    }

    fn put(&mut self, key: Key, value: Value) -> alopex_core::Result<()> {
        self.handle.block_on(self.txn.async_put(&key, &value))
    }

    fn delete(&mut self, key: Key) -> alopex_core::Result<()> {
        self.handle.block_on(self.txn.async_delete(&key))
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> alopex_core::Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.scan_prefix_buf.clear();
        self.scan_prefix_buf.extend_from_slice(prefix);
        let stream = self.txn.async_scan_prefix(&self.scan_prefix_buf);
        let iter = BlockingScanIter::new(stream, self.handle.clone());
        Ok(Box::new(iter))
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> alopex_core::Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.scan_range_start.clear();
        self.scan_range_start.extend_from_slice(start);
        self.scan_range_end.clear();
        self.scan_range_end.extend_from_slice(end);
        let start = self.scan_range_start.as_slice();
        let end = self.scan_range_end.as_slice();
        let stream = self.txn.async_scan_prefix(&[]);
        let iter = BlockingScanIter::new(stream, self.handle.clone())
            .filter(move |(key, _)| key.as_slice() >= start && key.as_slice() < end);
        Ok(Box::new(iter))
    }

    fn commit_self(self) -> alopex_core::Result<()> {
        self.handle.block_on(self.txn.async_commit())
    }

    fn rollback_self(self) -> alopex_core::Result<()> {
        self.handle.block_on(self.txn.async_rollback())
    }
}

struct BlockingScanIter<S> {
    stream: S,
    handle: tokio::runtime::Handle,
    done: bool,
}

impl<S> BlockingScanIter<S> {
    fn new(stream: S, handle: tokio::runtime::Handle) -> Self {
        Self {
            stream,
            handle,
            done: false,
        }
    }
}

impl<S> Iterator for BlockingScanIter<S>
where
    S: Stream<Item = alopex_core::Result<(Key, Value)>> + Unpin,
{
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let next_item = self.handle.block_on(self.stream.next());
        match next_item {
            Some(Ok(item)) => Some(item),
            Some(Err(err)) => {
                self.done = true;
                panic!("async scan failed: {err}");
            }
            None => {
                self.done = true;
                None
            }
        }
    }
}
