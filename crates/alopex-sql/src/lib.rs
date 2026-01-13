//! SQL parser and planning components for the Alopex DB SQL dialect.
//!
//! This crate provides:
//!
//! - **Tokenizer**: Lexical analysis of SQL strings
//! - **Parser**: SQL parsing into an AST
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
pub mod catalog;
pub mod columnar;
pub mod dialect;
pub mod error;
pub mod executor;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod tokenizer;
pub mod unified_error;

// AST types
pub use ast::{
    Statement, StatementKind,
    ddl::*,
    dml::*,
    expr::*,
    span::{Location, Span, Spanned},
};

// Dialect and parser types
pub use dialect::{AlopexDialect, Dialect};
pub use error::{ParserError, Result};
pub use parser::Parser;
pub use tokenizer::Tokenizer;
pub use tokenizer::keyword::Keyword;
pub use tokenizer::token::{Token, TokenWithSpan, Word};
pub use unified_error::SqlError;

// Catalog types (re-exported for convenience)
pub use catalog::persistent::{CatalogOverlay, DataSourceFormat, TableType};
pub use catalog::{
    Catalog, ColumnMetadata, Compression, IndexMetadata, MemoryCatalog, RowIdMode, StorageOptions,
    StorageType, TableMetadata,
};

// Planner types (re-exported for convenience)
pub use planner::{
    LogicalPlan, NameResolver, Planner, PlannerError, ProjectedColumn, Projection, ResolvedColumn,
    ResolvedType, SortExpr, TypeChecker, TypedAssignment, TypedExpr, TypedExprKind,
};

// Storage types
#[cfg(feature = "tokio")]
pub use storage::{AsyncSqlTransaction, AsyncTxnBridge, ErasedAsyncSqlTransaction};
pub use storage::{
    IndexScanIterator, IndexStorage, KeyEncoder, RowCodec, SqlTransaction, SqlValue, StorageError,
    TableScanIterator, TableStorage, TxnBridge, TxnContext,
};

// Executor types
#[cfg(feature = "tokio")]
pub use executor::AsyncExecutor;
pub use executor::{
    ColumnInfo, ConstraintViolation, EvaluationError, ExecutionResult, Executor, ExecutorError,
    QueryResult, Row,
};

/// `ExecutionResult` の公開 API 名。
pub type SqlResult = ExecutionResult;

#[cfg(test)]
mod integration;
