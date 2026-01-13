use std::marker::PhantomData;

use alopex_core::async_runtime::{BoxFuture, BoxStream};

use crate::executor::{ExecutionResult, Result, Row};
use crate::storage::AsyncSqlTransaction;

/// Async SQL executor that forwards to an async transaction.
pub struct AsyncExecutor<'txn, T: AsyncSqlTransaction<'txn>> {
    txn: T,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn, T: AsyncSqlTransaction<'txn>> AsyncExecutor<'txn, T> {
    /// Create a new async executor for an existing transaction.
    pub fn new(txn: T) -> Self {
        Self {
            txn,
            _marker: PhantomData,
        }
    }

    /// Execute a SELECT query and stream rows.
    pub fn execute_async<'a>(&'a mut self, sql: &'a str) -> BoxStream<'a, Result<Row>> {
        self.txn.async_query(sql)
    }

    /// Execute a DDL statement.
    pub fn execute_ddl_async<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<ExecutionResult>> {
        self.txn.async_execute(sql)
    }

    /// Execute a DML statement.
    pub fn execute_dml_async<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<ExecutionResult>> {
        self.txn.async_execute(sql)
    }

    /// Return the inner transaction.
    pub fn into_inner(self) -> T {
        self.txn
    }
}
