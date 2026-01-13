//! Async adapters for synchronous KV stores (tokio feature).

use crate::async_runtime::{BoxFuture, BoxStream};
use crate::error::{Error, Result};
use crate::kv::async_kv::{AsyncKVStore, AsyncKVTransaction};
use crate::kv::{KVStore, KVTransaction};
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

        tokio::spawn(async move {
            if command_tx
                .send(Command::ScanPrefix { prefix, sender })
                .await
                .is_err()
            {
                let _ = error_sender.send(Err(Error::TxnClosed)).await;
            }
        });

        Box::pin(ReceiverStream { receiver })
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
