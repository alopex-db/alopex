use std::fmt;

/// Parser errors for the Alopex SQL dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    /// ALOPEX-P001: An unexpected token was encountered.
    UnexpectedToken {
        line: u64,
        column: u64,
        expected: String,
        found: String,
    },

    /// ALOPEX-P002: A required token was missing.
    ExpectedToken {
        line: u64,
        column: u64,
        expected: String,
        found: String,
    },

    /// ALOPEX-P003: String literal was not closed.
    UnterminatedString { line: u64, column: u64 },

    /// ALOPEX-P004: Number literal is malformed.
    InvalidNumber {
        line: u64,
        column: u64,
        value: String,
    },

    /// ALOPEX-P005: Vector literal is malformed.
    InvalidVector { line: u64, column: u64 },

    /// ALOPEX-P006: Parser exceeded maximum recursion depth.
    RecursionLimitExceeded { depth: usize },
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedToken {
                line,
                column,
                expected,
                found,
            } => write!(
                f,
                "error[ALOPEX-P001]: unexpected token at line {line}, column {column}: expected {expected}, found {found}"
            ),
            Self::ExpectedToken {
                line,
                column,
                expected,
                found,
            } => write!(
                f,
                "error[ALOPEX-P002]: expected {expected} but found {found} at line {line}, column {column}"
            ),
            Self::UnterminatedString { line, column } => write!(
                f,
                "error[ALOPEX-P003]: unterminated string literal starting at line {line}, column {column}"
            ),
            Self::InvalidNumber {
                line,
                column,
                value,
            } => write!(
                f,
                "error[ALOPEX-P004]: invalid number literal '{value}' at line {line}, column {column}"
            ),
            Self::InvalidVector { line, column } => write!(
                f,
                "error[ALOPEX-P005]: invalid vector literal at line {line}, column {column}"
            ),
            Self::RecursionLimitExceeded { depth } => write!(
                f,
                "error[ALOPEX-P006]: recursion limit exceeded (depth: {depth})"
            ),
        }
    }
}

impl std::error::Error for ParserError {}

/// Convenience result type for parser operations.
pub type Result<T> = std::result::Result<T, ParserError>;
