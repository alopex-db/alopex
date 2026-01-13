use alopex_core::async_runtime::{BoxFuture, BoxStream, MaybeSend};

use crate::executor::{ExecutionResult, Result, Row};
use crate::storage::AsyncSqlTransaction;

/// Object-safe async SQL transaction for type erasure.
pub trait ErasedAsyncSqlTransaction: MaybeSend + 'static {
    fn execute<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, Result<ExecutionResult>>;
    fn query<'a>(&'a self, sql: &'a str) -> BoxStream<'a, Result<Row>>;
    fn commit_boxed(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
    fn rollback_boxed(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
}

impl<T> ErasedAsyncSqlTransaction for T
where
    T: AsyncSqlTransaction<'static> + MaybeSend + 'static,
{
    fn execute<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, Result<ExecutionResult>> {
        self.async_execute(sql)
    }

    fn query<'a>(&'a self, sql: &'a str) -> BoxStream<'a, Result<Row>> {
        self.async_query(sql)
    }

    fn commit_boxed(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        (*self).async_commit()
    }

    fn rollback_boxed(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        (*self).async_rollback()
    }
}
