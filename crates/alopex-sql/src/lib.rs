//! SQL parser components for the Alopex DB SQL dialect.

pub mod ast;
pub mod dialect;
pub mod error;
pub mod parser;
pub mod tokenizer;

pub use ast::{
    Statement, StatementKind,
    ddl::*,
    dml::*,
    expr::*,
    span::{Location, Span, Spanned},
};
pub use dialect::AlopexDialect;
pub use error::{ParserError, Result};
pub use tokenizer::Tokenizer;
pub use tokenizer::keyword::Keyword;
pub use tokenizer::token::{Token, TokenWithSpan, Word};
