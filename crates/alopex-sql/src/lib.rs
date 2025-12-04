//! SQL parser components for the Alopex DB SQL dialect.

pub mod ast;
pub mod dialect;
pub mod error;
pub mod parser;
pub mod tokenizer;

pub use ast::span::{Location, Span};
pub use dialect::AlopexDialect;
pub use error::{ParserError, Result};
