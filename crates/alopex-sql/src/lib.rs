//! SQL parser and planning components for the Alopex DB SQL dialect.
//!
//! This crate provides:
//!
//! - **Parser**: SQL parsing into an AST via the Nim FFI parser
//! - **Catalog**: Table and index metadata management
//! - **Planner**: AST to logical plan conversion with type checking
//!
//! # Quick Start
//!
//! ```
//! use alopex_sql::{Parser, AlopexDialect};
//! use alopex_sql::catalog::MemoryCatalog;
//! use alopex_sql::planner::Planner;
//!
//! // Parse SQL using the convenience method
//! let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
//! let dialect = AlopexDialect::default();
//! let stmts = Parser::parse_sql(&dialect, sql).unwrap();
//! let stmt = &stmts[0];
//!
//! // Plan with catalog
//! let catalog = MemoryCatalog::new();
//! let planner = Planner::new(&catalog);
//! let plan = planner.plan(stmt).unwrap();
//! ```

pub mod ast;
#[cfg(feature = "async")]
pub mod async_api;
pub mod catalog;
pub mod changefeed_boundary;
pub mod columnar;
pub mod dialect;
pub mod distributed_read;
pub mod error;
pub mod executor;
mod nim_bridge;
mod nim_ffi;
pub mod parser;
pub mod planner;
pub mod scalar;
pub mod storage;
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
pub mod tokio_adapter;
pub mod unified_error;

// AST types
pub use ast::{
    PragmaValue, Statement, StatementKind,
    ddl::*,
    dml::*,
    expr::*,
    span::{Location, Span, Spanned},
};

// Dialect and parser types
pub use dialect::{AlopexDialect, Dialect};
pub use error::{ParserError, Result};
pub use parser::Parser;
pub use unified_error::SqlError;

// Catalog types (re-exported for convenience)
pub use catalog::persistent::{CatalogOverlay, DataSourceFormat, TableType};
pub use catalog::{
    Catalog, ColumnMetadata, Compression, IndexMetadata, MemoryCatalog, RowIdMode, StorageOptions,
    StorageType, TableMetadata,
};

// Planner types (re-exported for convenience)
pub use planner::{
    LogicalPlan, NameResolver, PlannedStatement, Planner, PlannerError, PlanningDiagnostic,
    PlanningDiagnosticSeverity, ProjectedColumn, Projection, ResolvedColumn, ResolvedType,
    RoutingInput, SortExpr, TableReference, TableReferenceAccess, TableReferenceExtractor,
    TableReferenceSource, TypeChecker, TypedAssignment, TypedExpr, TypedExprKind,
    plan_sql_for_routing, plan_statement_for_routing,
};

// Storage types
#[cfg(feature = "tokio")]
pub use storage::ErasedAsyncSqlTransaction;
pub use storage::{
    IndexScanIterator, IndexStorage, KeyEncoder, RangeBoundedScanIterator, RangeReadSnapshot,
    RowCodec, SqlTransaction, SqlValue, StorageError, StorageRangeConstraint,
    StorageRangeConstraintError, TableScanIterator, TableStorage, TxnBridge, TxnContext,
};

// Executor types
#[cfg(feature = "tokio")]
pub use executor::AsyncExecutor;
pub use executor::{
    ColumnInfo, ConstraintViolation, EvaluationError, ExecutionResult, Executor, ExecutorError,
    QueryResult, Row,
};

// Async facade types
#[cfg(feature = "async")]
pub use async_api::{AsyncResult, AsyncRowStream, AsyncSqlTransaction, AsyncTxnBridge};
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
pub use tokio_adapter::{TokioAsyncSqlTransaction, TokioAsyncTxnBridge};

/// `ExecutionResult` の公開 API 名。
pub type SqlResult = ExecutionResult;

#[cfg(test)]
mod integration;
