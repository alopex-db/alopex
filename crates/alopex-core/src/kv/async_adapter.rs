//! Async adapters for synchronous KV stores (tokio feature).

use crate::async_runtime::{BoxFuture, BoxStream};
use crate::error::{Error, Result};
use crate::kv::async_kv::{AsyncKVStore, AsyncKVTransaction};
use crate::kv::async_kv::{WorkerTask, WorkerTaskResult};
use crate::kv::{KVStore, KVTransaction, KeySearchCancellation, KeySearchPage, KeySearchRequest};
use crate::types::{Key, TxnMode, Value};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::stream::Stream;
use std::sync::Arc;

/// Async adapter for a synchronous [`KVStore`].
pub struct AsyncKVStoreAdapter<S> {
    store: Arc<S>,
    default_mode: TxnMode,
}

impl<S> AsyncKVStoreAdapter<S> {
    /// Wraps a synchronous store with a default transaction mode.
    pub fn new(store: S) -> Self {
        Self::with_mode(store, TxnMode::ReadWrite)
    }

    /// Wraps a synchronous store with an explicit default transaction mode.
    pub fn with_mode(store: S, default_mode: TxnMode) -> Self {
        Self {
            store: Arc::new(store),
            default_mode,
        }
    }

    /// Wraps a shared synchronous store with an explicit default mode.
    pub fn from_arc(store: Arc<S>, default_mode: TxnMode) -> Self {
        Self {
            store,
            default_mode,
        }
    }
}

impl<S> AsyncKVStore for AsyncKVStoreAdapter<S>
where
    S: KVStore + Send + Sync + 'static,
{
    type Transaction<'a>
        = AsyncKVTransactionAdapter
    where
        S: 'a;

    fn begin_async<'a>(&'a self) -> BoxFuture<'a, Result<Self::Transaction<'a>>> {
        let store = Arc::clone(&self.store);
        let mode = self.default_mode;
        Box::pin(async move {
            let (command_tx, command_rx) = tokio::sync::mpsc::channel(32);
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            tokio::task::spawn_blocking(move || run_worker(store, mode, command_rx, ready_tx));

            match ready_rx.await {
                Ok(Ok(())) => Ok(AsyncKVTransactionAdapter::new(command_tx)),
                Ok(Err(err)) => Err(err),
                Err(_) => Err(Error::TxnClosed),
            }
        })
    }
}

/// Async adapter for a synchronous [`KVTransaction`].
pub struct AsyncKVTransactionAdapter {
    command_tx: tokio::sync::mpsc::Sender<Command>,
}

impl AsyncKVTransactionAdapter {
    /// Wraps a synchronous transaction worker.
    fn new(command_tx: tokio::sync::mpsc::Sender<Command>) -> Self {
        Self { command_tx }
    }
}

impl<'txn> AsyncKVTransaction<'txn> for AsyncKVTransactionAdapter {
    fn supports_worker_tasks(&self) -> bool {
        true
    }

    fn async_worker_task<'a>(
        &'a mut self,
        task: WorkerTask,
    ) -> BoxFuture<'a, Result<WorkerTaskResult>> {
        let commands = self.command_tx.clone();
        Box::pin(async move {
            let (respond_to, response) = tokio::sync::oneshot::channel();
            commands
                .send(Command::WorkerTask { task, respond_to })
                .await
                .map_err(|_| Error::TxnClosed)?;
            response.await.map_err(|_| Error::TxnClosed)?
        })
    }

    fn async_get<'a>(&'a self, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Value>>> {
        let command_tx = self.command_tx.clone();
        let key = key.to_vec();
        Box::pin(async move {
            let (respond_to, response) = tokio::sync::oneshot::channel();
            command_tx
                .send(Command::Get { key, respond_to })
                .await
                .map_err(|_| Error::TxnClosed)?;
            response.await.map_err(|_| Error::TxnClosed)?
        })
    }

    fn async_put<'a>(&'a mut self, key: &'a [u8], value: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        let command_tx = self.command_tx.clone();
        let key = key.to_vec();
        let value = value.to_vec();
        Box::pin(async move {
            let (respond_to, response) = tokio::sync::oneshot::channel();
            command_tx
                .send(Command::Put {
                    key,
                    value,
                    respond_to,
                })
                .await
                .map_err(|_| Error::TxnClosed)?;
            response.await.map_err(|_| Error::TxnClosed)?
        })
    }

    fn async_delete<'a>(&'a mut self, key: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        let command_tx = self.command_tx.clone();
        let key = key.to_vec();
        Box::pin(async move {
            let (respond_to, response) = tokio::sync::oneshot::channel();
            command_tx
                .send(Command::Delete { key, respond_to })
                .await
                .map_err(|_| Error::TxnClosed)?;
            response.await.map_err(|_| Error::TxnClosed)?
        })
    }

    fn async_scan_prefix<'a>(&'a self, prefix: &'a [u8]) -> BoxStream<'a, Result<(Key, Value)>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let command_tx = self.command_tx.clone();
        let prefix = prefix.to_vec();
        let error_sender = sender.clone();

        match command_tx.try_send(Command::ScanPrefix { prefix, sender }) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                let _ = error_sender.try_send(Err(Error::TxnClosed));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
                tokio::spawn(async move {
                    if command_tx.send(command).await.is_err() {
                        let _ = error_sender.send(Err(Error::TxnClosed)).await;
                    }
                });
            }
        }

        Box::pin(ReceiverStream { receiver })
    }

    fn async_search_keys<'a>(
        &'a self,
        request: KeySearchRequest,
    ) -> BoxFuture<'a, Result<KeySearchPage>> {
        self.async_search_keys_with_cancellation(request, KeySearchCancellation::default())
    }

    fn async_search_keys_with_cancellation<'a>(
        &'a self,
        request: KeySearchRequest,
        cancellation: KeySearchCancellation,
    ) -> BoxFuture<'a, Result<KeySearchPage>> {
        let command_tx = self.command_tx.clone();
        Box::pin(async move {
            let (respond_to, response) = tokio::sync::oneshot::channel();
            command_tx
                .send(Command::SearchKeys {
                    request,
                    cancellation,
                    respond_to,
                })
                .await
                .map_err(|_| Error::TxnClosed)?;
            response.await.map_err(|_| Error::TxnClosed)?
        })
    }

    fn async_commit(self) -> BoxFuture<'txn, Result<()>> {
        let command_tx = self.command_tx;
        Box::pin(async move {
            let (respond_to, response) = tokio::sync::oneshot::channel();
            command_tx
                .send(Command::Commit { respond_to })
                .await
                .map_err(|_| Error::TxnClosed)?;
            response.await.map_err(|_| Error::TxnClosed)?
        })
    }

    fn async_rollback(self) -> BoxFuture<'txn, Result<()>> {
        let command_tx = self.command_tx;
        Box::pin(async move {
            let (respond_to, response) = tokio::sync::oneshot::channel();
            command_tx
                .send(Command::Rollback { respond_to })
                .await
                .map_err(|_| Error::TxnClosed)?;
            response.await.map_err(|_| Error::TxnClosed)?
        })
    }
}

enum Command {
    WorkerTask {
        task: WorkerTask,
        respond_to: tokio::sync::oneshot::Sender<Result<WorkerTaskResult>>,
    },
    Get {
        key: Vec<u8>,
        respond_to: tokio::sync::oneshot::Sender<Result<Option<Value>>>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        respond_to: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Delete {
        key: Vec<u8>,
        respond_to: tokio::sync::oneshot::Sender<Result<()>>,
    },
    ScanPrefix {
        prefix: Vec<u8>,
        sender: tokio::sync::mpsc::Sender<Result<(Key, Value)>>,
    },
    SearchKeys {
        request: KeySearchRequest,
        cancellation: KeySearchCancellation,
        respond_to: tokio::sync::oneshot::Sender<Result<KeySearchPage>>,
    },
    Commit {
        respond_to: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Rollback {
        respond_to: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

fn run_worker<S>(
    store: Arc<S>,
    mode: TxnMode,
    mut command_rx: tokio::sync::mpsc::Receiver<Command>,
    ready_tx: tokio::sync::oneshot::Sender<Result<()>>,
) where
    S: KVStore,
{
    let mut txn = match store.begin(mode) {
        Ok(txn) => {
            let _ = ready_tx.send(Ok(()));
            Some(txn)
        }
        Err(err) => {
            let _ = ready_tx.send(Err(err));
            return;
        }
    };
    let mut closed = false;

    while let Some(command) = command_rx.blocking_recv() {
        match command {
            Command::WorkerTask { task, respond_to } => {
                let result = match txn.as_mut() {
                    Some(transaction) => {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task(transaction)))
                            .map_err(|_| Error::TxnClosed)
                    }
                    None => Err(Error::TxnClosed),
                };
                let failed = result.is_err();
                let _ = respond_to.send(result);
                if failed {
                    // A failed callback may have changed the transaction. Close it
                    // and let the worker perform its explicit rollback below.
                    break;
                }
            }
            Command::Get { key, respond_to } => {
                let result = (|| {
                    let txn = txn.as_mut().ok_or(Error::TxnClosed)?;
                    txn.get(&key)
                })();
                let _ = respond_to.send(result);
            }
            Command::Put {
                key,
                value,
                respond_to,
            } => {
                let result = (|| {
                    let txn = txn.as_mut().ok_or(Error::TxnClosed)?;
                    txn.put(key, value)
                })();
                let _ = respond_to.send(result);
            }
            Command::Delete { key, respond_to } => {
                let result = (|| {
                    let txn = txn.as_mut().ok_or(Error::TxnClosed)?;
                    txn.delete(key)
                })();
                let _ = respond_to.send(result);
            }
            Command::ScanPrefix { prefix, sender } => {
                let result = (|| -> Result<()> {
                    let txn = txn.as_mut().ok_or(Error::TxnClosed)?;
                    let iter = txn.scan_prefix(&prefix)?;
                    for item in iter {
                        if sender.blocking_send(Ok(item)).is_err() {
                            return Ok(());
                        }
                    }
                    Ok(())
                })();
                if let Err(err) = result {
                    let _ = sender.blocking_send(Err(err));
                }
            }
            Command::SearchKeys {
                request,
                cancellation,
                respond_to,
            } => {
                let result = (|| {
                    let txn = txn.as_mut().ok_or(Error::TxnClosed)?;
                    txn.search_keys_with_cancellation(&request, &cancellation)
                })();
                let _ = respond_to.send(result);
            }
            Command::Commit { respond_to } => {
                let result = match txn.take() {
                    Some(txn) => txn.commit_self(),
                    None => Err(Error::TxnClosed),
                };
                let _ = respond_to.send(result);
                closed = true;
                break;
            }
            Command::Rollback { respond_to } => {
                let result = match txn.take() {
                    Some(txn) => txn.rollback_self(),
                    None => Err(Error::TxnClosed),
                };
                let _ = respond_to.send(result);
                closed = true;
                break;
            }
        }
    }

    if !closed {
        if let Some(txn) = txn.take() {
            let _ = txn.rollback_self();
        }
    }
}

struct ReceiverStream<T> {
    receiver: tokio::sync::mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RollbackCountingStore(Arc<AtomicUsize>);

    struct RollbackCountingTransaction(Arc<AtomicUsize>);

    impl<'a> KVTransaction<'a> for RollbackCountingTransaction {
        fn id(&self) -> crate::types::TxnId {
            crate::types::TxnId(1)
        }
        fn mode(&self) -> TxnMode {
            TxnMode::ReadWrite
        }
        fn get(&mut self, _key: &Key) -> Result<Option<Value>> {
            Ok(None)
        }
        fn put(&mut self, _key: Key, _value: Value) -> Result<()> {
            Ok(())
        }
        fn delete(&mut self, _key: Key) -> Result<()> {
            Ok(())
        }
        fn scan_prefix(
            &mut self,
            _prefix: &[u8],
        ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
            Ok(Box::new(std::iter::empty()))
        }
        fn scan_range(
            &mut self,
            _start: &[u8],
            _end: &[u8],
        ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
            Ok(Box::new(std::iter::empty()))
        }
        fn commit_self(self) -> Result<()> {
            panic!("failed worker task must not commit")
        }
        fn rollback_self(self) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl<'a> crate::txn::TxnManager<'a, RollbackCountingTransaction> for RollbackCountingStore {
        fn begin(&'a self, mode: TxnMode) -> Result<RollbackCountingTransaction> {
            KVStore::begin(self, mode)
        }
        fn commit(&'a self, txn: RollbackCountingTransaction) -> Result<()> {
            txn.commit_self()
        }
        fn rollback(&'a self, txn: RollbackCountingTransaction) -> Result<()> {
            txn.rollback_self()
        }
    }

    impl KVStore for RollbackCountingStore {
        type Transaction<'a> = RollbackCountingTransaction;
        type Manager<'a> = Self;

        fn txn_manager(&self) -> Self::Manager<'_> {
            Self(Arc::clone(&self.0))
        }
        fn begin(&self, _mode: TxnMode) -> Result<Self::Transaction<'_>> {
            Ok(RollbackCountingTransaction(Arc::clone(&self.0)))
        }
    }

    #[tokio::test]
    async fn worker_task_panic_closes_transaction_and_rolls_back_once() {
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(RollbackCountingStore(Arc::clone(&rollbacks)));
        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        let (ready, started) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            run_worker(store, TxnMode::ReadWrite, receiver, ready)
        });
        started.await.unwrap().unwrap();
        let mut adapter = AsyncKVTransactionAdapter::new(commands);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            adapter.async_worker_task(Box::new(|_| panic!("controlled worker task failure"))),
        )
        .await
        .expect("worker task must report closure");
        assert!(matches!(result, Err(Error::TxnClosed)));
        assert!(matches!(
            adapter.async_get(b"key").await,
            Err(Error::TxnClosed)
        ));
        drop(adapter);
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), worker)
            .await
            .expect("worker must finish");
        assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
        assert!(outcome.is_ok(), "worker must contain the callback panic");
    }

    #[tokio::test]
    async fn prefix_scan_reports_closed_worker_and_finishes() {
        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let adapter = AsyncKVTransactionAdapter::new(commands);
        let mut stream = adapter.async_scan_prefix(b"key:");
        assert!(matches!(stream.next().await, Some(Err(Error::TxnClosed))));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn prefix_scan_waits_for_queue_capacity_and_preserves_rows() {
        let (commands, mut receiver) = tokio::sync::mpsc::channel(1);
        let (respond_to, _response) = tokio::sync::oneshot::channel();
        assert!(commands
            .try_send(Command::Get {
                key: b"existing".to_vec(),
                respond_to,
            })
            .is_ok());
        let adapter = AsyncKVTransactionAdapter::new(commands);
        let mut stream = adapter.async_scan_prefix(b"key:");
        assert!(matches!(receiver.recv().await, Some(Command::Get { .. })));
        let Some(Command::ScanPrefix { prefix, sender }) = receiver.recv().await else {
            panic!("expected queued prefix scan");
        };
        assert_eq!(prefix, b"key:");
        sender
            .send(Ok((b"key:1".to_vec(), b"value".to_vec())))
            .await
            .unwrap();
        drop(sender);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            (b"key:1".to_vec(), b"value".to_vec())
        );
        assert!(stream.next().await.is_none());
    }
}
