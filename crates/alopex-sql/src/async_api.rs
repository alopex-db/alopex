//! Runtime-agnostic async facade for `alopex-sql`.
//!
//! This module defines async transaction and streaming query interfaces without
//! exposing any runtime-specific types (e.g. tokio). Adapter implementations
//! (tokio, etc.) are provided separately behind feature flags.

use alopex_core::types::TxnMode;
use alopex_core::{BoxFuture, BoxStream, MaybeSend};

use crate::SqlError;
use crate::executor::{ExecutionResult, Row};
use crate::planner::LogicalPlan;

/// Async result type used by the async facade.
pub type AsyncResult<T> = core::result::Result<T, SqlError>;

/// Stream of rows for async SELECT/scan operations.
pub type AsyncRowStream<'a> = BoxStream<'a, AsyncResult<Row>>;

/// Object-safe async SQL transaction for type erasure.
///
/// This is useful for adapters that need to store heterogeneous transaction
/// implementations behind `Box<dyn ErasedAsyncSqlTransaction>`.
pub trait ErasedAsyncSqlTransaction: MaybeSend {
    fn mode(&self) -> TxnMode;
    fn commit_boxed(self: Box<Self>) -> BoxFuture<'static, AsyncResult<()>>;
    fn rollback_boxed(self: Box<Self>) -> BoxFuture<'static, AsyncResult<()>>;
}

/// Runtime-agnostic async SQL transaction.
pub trait AsyncSqlTransaction: MaybeSend {
    /// Returns the transaction mode.
    fn mode(&self) -> TxnMode;

    /// Commit the transaction.
    fn commit<'a>(&'a mut self) -> BoxFuture<'a, AsyncResult<()>>;

    /// Roll back the transaction.
    fn rollback<'a>(&'a mut self) -> BoxFuture<'a, AsyncResult<()>>;

    /// Execute a logical plan and return a non-streaming result.
    fn execute_plan<'a>(
        &'a mut self,
        plan: LogicalPlan,
    ) -> BoxFuture<'a, AsyncResult<ExecutionResult>>;

    /// Execute a query logical plan and return a stream of rows.
    fn query_stream<'a>(
        &'a mut self,
        plan: LogicalPlan,
    ) -> BoxFuture<'a, AsyncResult<AsyncRowStream<'a>>>;
}

impl<T> ErasedAsyncSqlTransaction for T
where
    T: AsyncSqlTransaction + MaybeSend + 'static,
{
    fn mode(&self) -> TxnMode {
        AsyncSqlTransaction::mode(self)
    }

    fn commit_boxed(self: Box<Self>) -> BoxFuture<'static, AsyncResult<()>> {
        Box::pin(async move {
            let mut this = *self;
            this.commit().await
        })
    }

    fn rollback_boxed(self: Box<Self>) -> BoxFuture<'static, AsyncResult<()>> {
        Box::pin(async move {
            let mut this = *self;
            this.rollback().await
        })
    }
}

/// Runtime-agnostic async transaction bridge for SQL.
pub trait AsyncTxnBridge: MaybeSend {
    /// Concrete transaction type for this bridge.
    ///
    /// The transaction may borrow from `self` (e.g., via a store handle).
    type Transaction<'a>: AsyncSqlTransaction + 'a
    where
        Self: 'a;

    /// Begin a read-only transaction.
    fn begin_read<'a>(&'a self) -> BoxFuture<'a, AsyncResult<Self::Transaction<'a>>>;

    /// Begin a read-write transaction.
    fn begin_write<'a>(&'a self) -> BoxFuture<'a, AsyncResult<Self::Transaction<'a>>>;
}
