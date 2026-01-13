//! Tokio-based adapter for the runtime-agnostic async SQL facade.
//!
//! This adapter keeps the synchronous SQL transaction on a blocking worker
//! thread and bridges async calls through a command channel. Public signatures
//! stay runtime-agnostic while the implementation uses tokio internally.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, mpsc as std_mpsc};

use alopex_core::BoxFuture;
use alopex_core::KVTransaction;
use alopex_core::kv::KVStore;
use alopex_core::types::TxnMode;
use futures_core::Stream;
use tokio::sync::{mpsc, oneshot};

use crate::async_api::{AsyncResult, AsyncRowStream, AsyncSqlTransaction, AsyncTxnBridge};
use crate::catalog::{CatalogOverlay, PersistentCatalog, TxnCatalogView};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{ExecutionResult, Row, build_streaming_pipeline};
use crate::planner::Projection;
use crate::storage::TxnBridge;
use crate::{LogicalPlan, SqlError};

/// Tokio adapter for `AsyncTxnBridge` backed by a persistent SQL catalog.
pub struct TokioAsyncTxnBridge<S: KVStore> {
    store: Arc<S>,
    catalog: Arc<RwLock<PersistentCatalog<S>>>,
}

impl<S: KVStore> TokioAsyncTxnBridge<S> {
    /// Create a new tokio async bridge for the given store and catalog.
    pub fn new(store: Arc<S>, catalog: Arc<RwLock<PersistentCatalog<S>>>) -> Self {
        Self { store, catalog }
    }
}

/// Tokio-backed async SQL transaction.
pub struct TokioAsyncSqlTransaction<S: KVStore> {
    command_tx: std_mpsc::Sender<TxnCommand>,
    mode: TxnMode,
    closed: bool,
    _marker: PhantomData<S>,
}

impl<S: KVStore> TokioAsyncSqlTransaction<S> {
    fn new(command_tx: std_mpsc::Sender<TxnCommand>, mode: TxnMode) -> Self {
        Self {
            command_tx,
            mode,
            closed: false,
            _marker: PhantomData,
        }
    }

    fn ensure_open(&self) -> AsyncResult<()> {
        if self.closed {
            return Err(SqlError::from(alopex_core::Error::TxnClosed));
        }
        Ok(())
    }

    fn send_command(&self, cmd: TxnCommand) -> AsyncResult<()> {
        self.command_tx.send(cmd).map_err(|_| SqlError::Execution {
            message: "tokio adapter channel closed".to_string(),
            code: "ALOPEX-E999",
        })
    }
}

impl<S: KVStore + 'static> AsyncTxnBridge for TokioAsyncTxnBridge<S> {
    type Transaction<'a>
        = TokioAsyncSqlTransaction<S>
    where
        Self: 'a;

    fn begin_read<'a>(&'a self) -> BoxFuture<'a, AsyncResult<Self::Transaction<'a>>> {
        let store = Arc::clone(&self.store);
        let catalog = Arc::clone(&self.catalog);
        Box::pin(async move {
            let command_tx = spawn_worker(store, catalog, TxnMode::ReadOnly).await?;
            Ok(TokioAsyncSqlTransaction::<S>::new(
                command_tx,
                TxnMode::ReadOnly,
            ))
        })
    }

    fn begin_write<'a>(&'a self) -> BoxFuture<'a, AsyncResult<Self::Transaction<'a>>> {
        let store = Arc::clone(&self.store);
        let catalog = Arc::clone(&self.catalog);
        Box::pin(async move {
            let command_tx = spawn_worker(store, catalog, TxnMode::ReadWrite).await?;
            Ok(TokioAsyncSqlTransaction::<S>::new(
                command_tx,
                TxnMode::ReadWrite,
            ))
        })
    }
}

impl<S: KVStore> AsyncSqlTransaction for TokioAsyncSqlTransaction<S> {
    fn mode(&self) -> TxnMode {
        self.mode
    }

    fn commit<'a>(&'a mut self) -> BoxFuture<'a, AsyncResult<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let (resp_tx, resp_rx) = oneshot::channel();
            self.send_command(TxnCommand::Commit { response: resp_tx })?;
            let result = resp_rx.await.map_err(|_| SqlError::Execution {
                message: "tokio adapter commit canceled".to_string(),
                code: "ALOPEX-E999",
            })?;
            if result.is_ok() {
                self.closed = true;
            }
            result
        })
    }

    fn rollback<'a>(&'a mut self) -> BoxFuture<'a, AsyncResult<()>> {
        Box::pin(async move {
            self.ensure_open()?;
            let (resp_tx, resp_rx) = oneshot::channel();
            self.send_command(TxnCommand::Rollback { response: resp_tx })?;
            let result = resp_rx.await.map_err(|_| SqlError::Execution {
                message: "tokio adapter rollback canceled".to_string(),
                code: "ALOPEX-E999",
            })?;
            if result.is_ok() {
                self.closed = true;
            }
            result
        })
    }

    fn execute_plan<'a>(
        &'a mut self,
        plan: LogicalPlan,
    ) -> BoxFuture<'a, AsyncResult<ExecutionResult>> {
        Box::pin(async move {
            self.ensure_open()?;
            let (resp_tx, resp_rx) = oneshot::channel();
            self.send_command(TxnCommand::ExecutePlan {
                plan,
                response: resp_tx,
            })?;
            resp_rx.await.map_err(|_| SqlError::Execution {
                message: "tokio adapter execute canceled".to_string(),
                code: "ALOPEX-E999",
            })?
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        plan: LogicalPlan,
    ) -> BoxFuture<'a, AsyncResult<AsyncRowStream<'a>>> {
        Box::pin(async move {
            self.ensure_open()?;
            let (row_tx, row_rx) = mpsc::channel(32);
            let cancel = Arc::new(AtomicBool::new(false));
            let (resp_tx, resp_rx) = oneshot::channel();
            self.send_command(TxnCommand::QueryStream {
                plan,
                rows: row_tx,
                cancel: Arc::clone(&cancel),
                response: resp_tx,
            })?;
            let result = resp_rx.await.map_err(|_| SqlError::Execution {
                message: "tokio adapter stream canceled".to_string(),
                code: "ALOPEX-E999",
            })?;
            result?;
            let stream: AsyncRowStream<'a> = Box::pin(TokioRowStream::new(row_rx, cancel));
            Ok(stream)
        })
    }
}

enum TxnCommand {
    ExecutePlan {
        plan: LogicalPlan,
        response: oneshot::Sender<AsyncResult<ExecutionResult>>,
    },
    QueryStream {
        plan: LogicalPlan,
        rows: mpsc::Sender<AsyncResult<Row>>,
        cancel: Arc<AtomicBool>,
        response: oneshot::Sender<AsyncResult<()>>,
    },
    Commit {
        response: oneshot::Sender<AsyncResult<()>>,
    },
    Rollback {
        response: oneshot::Sender<AsyncResult<()>>,
    },
    #[allow(dead_code)]
    Shutdown,
}

async fn spawn_worker<S: KVStore + 'static>(
    store: Arc<S>,
    catalog: Arc<RwLock<PersistentCatalog<S>>>,
    mode: TxnMode,
) -> AsyncResult<std_mpsc::Sender<TxnCommand>> {
    let (command_tx, command_rx) = std_mpsc::channel();
    let (init_tx, init_rx) = oneshot::channel();

    tokio::task::spawn_blocking(move || {
        let txn = match store.begin(mode) {
            Ok(txn) => txn,
            Err(err) => {
                let _ = init_tx.send(Err(SqlError::from(err)));
                return;
            }
        };
        let mut overlay = CatalogOverlay::new();
        let mut executor = crate::executor::Executor::new(store.clone(), catalog.clone());
        let _ = init_tx.send(Ok(()));
        worker_loop(command_rx, txn, &mut overlay, &mut executor, &catalog, mode);
    });

    init_rx.await.map_err(|_| SqlError::Execution {
        message: "tokio adapter init canceled".to_string(),
        code: "ALOPEX-E999",
    })??;
    Ok(command_tx)
}

fn worker_loop<S: KVStore>(
    command_rx: std_mpsc::Receiver<TxnCommand>,
    txn: S::Transaction<'_>,
    overlay: &mut CatalogOverlay,
    executor: &mut crate::executor::Executor<S, PersistentCatalog<S>>,
    catalog: &Arc<RwLock<PersistentCatalog<S>>>,
    mode: TxnMode,
) {
    let mut txn = Some(txn);
    let mut completed = false;
    for command in command_rx {
        match command {
            TxnCommand::ExecutePlan { plan, response } => {
                let txn = txn.as_mut().expect("transaction already consumed");
                let mut borrowed = TxnBridge::<S>::wrap_external(txn, mode, overlay);
                let result = executor
                    .execute_in_txn(plan, &mut borrowed)
                    .map_err(SqlError::from);
                let _ = response.send(result);
            }
            TxnCommand::QueryStream {
                plan,
                rows,
                cancel,
                response,
            } => {
                let txn = txn.as_mut().expect("transaction already consumed");
                let mut borrowed = TxnBridge::<S>::wrap_external(txn, mode, overlay);
                let (mut sql_txn, overlay) = borrowed.split_parts();
                let (projection, mut row_iter) = {
                    let guard = catalog.read().expect("catalog lock poisoned");
                    let view = TxnCatalogView::new(&*guard, &*overlay);
                    match build_streaming_pipeline(&mut sql_txn, &view, plan) {
                        Ok((iter, projection, _schema)) => (projection, iter),
                        Err(err) => {
                            let _ = response.send(Err(SqlError::from(err)));
                            continue;
                        }
                    }
                };
                if response.send(Ok(())).is_err() {
                    continue;
                }

                loop {
                    if cancel.load(Ordering::Relaxed) {
                        break;
                    }
                    let next = row_iter.next_row();
                    let Some(result) = next else {
                        break;
                    };
                    let item = match result {
                        Ok(row) => match project_row(row, &projection) {
                            Ok(projected) => Ok(projected),
                            Err(err) => {
                                let _ = rows.blocking_send(Err(SqlError::from(err)));
                                break;
                            }
                        },
                        Err(err) => {
                            let _ = rows.blocking_send(Err(SqlError::from(err)));
                            break;
                        }
                    };

                    if rows.blocking_send(item).is_err() {
                        break;
                    }
                }
            }
            TxnCommand::Commit { response } => {
                let txn = txn.take().expect("transaction already consumed");
                let result = commit_txn::<S>(txn, overlay, catalog, mode);
                completed = result.is_ok();
                let _ = response.send(result);
                break;
            }
            TxnCommand::Rollback { response } => {
                let txn = txn.take().expect("transaction already consumed");
                let result = rollback_txn::<S>(txn, overlay);
                completed = result.is_ok();
                let _ = response.send(result);
                break;
            }
            TxnCommand::Shutdown => {
                break;
            }
        }
    }

    if !completed && let Some(txn) = txn {
        let _ = rollback_txn::<S>(txn, overlay);
    }
}

fn commit_txn<S: KVStore>(
    txn: S::Transaction<'_>,
    overlay: &mut CatalogOverlay,
    catalog: &Arc<RwLock<PersistentCatalog<S>>>,
    mode: TxnMode,
) -> AsyncResult<()> {
    txn.commit_self().map_err(SqlError::from)?;
    if mode == TxnMode::ReadWrite {
        let mut catalog = catalog.write().expect("catalog lock poisoned");
        catalog.apply_overlay(std::mem::take(overlay));
    } else {
        PersistentCatalog::<S>::discard_overlay(std::mem::take(overlay));
    }
    Ok(())
}

fn rollback_txn<S: KVStore>(
    txn: S::Transaction<'_>,
    overlay: &mut CatalogOverlay,
) -> AsyncResult<()> {
    txn.rollback_self().map_err(SqlError::from)?;
    PersistentCatalog::<S>::discard_overlay(std::mem::take(overlay));
    Ok(())
}

fn project_row(row: Row, projection: &Projection) -> Result<Row, crate::executor::ExecutorError> {
    match projection {
        Projection::All(_) => Ok(row),
        Projection::Columns(cols) => {
            let row_id = row.row_id;
            let ctx = EvalContext::new(&row.values);
            let mut values = Vec::with_capacity(cols.len());
            for col in cols {
                values.push(evaluate(&col.expr, &ctx)?);
            }
            Ok(Row::new(row_id, values))
        }
    }
}

struct TokioRowStream {
    receiver: mpsc::Receiver<AsyncResult<Row>>,
    cancel: Arc<AtomicBool>,
}

impl TokioRowStream {
    fn new(receiver: mpsc::Receiver<AsyncResult<Row>>, cancel: Arc<AtomicBool>) -> Self {
        Self { receiver, cancel }
    }
}

impl Drop for TokioRowStream {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

impl Stream for TokioRowStream {
    type Item = AsyncResult<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.receiver).poll_recv(cx)
    }
}
