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

    /// ALOPEX-P007: The Nim FFI parser hit an internal invariant violation
    /// (a Nim `Defect`, e.g. `IndexDefect`/`FieldDefect`) rather than a
    /// normal syntax error. This indicates a parser bug, not bad input, and
    /// must stay distinguishable from `UnexpectedToken` so operators do not
    /// mistake it for user error. See `nim-sql-parser/src/alopex_sql_parser.nim`
    /// `alopex_parse_sql`'s `except ... Defect` branch.
    InternalParserDefect { message: String },
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
            Self::InternalParserDefect { message } => write!(
                f,
                "error[ALOPEX-P007]: internal parser defect (this is a parser bug, not invalid SQL): {message}"
            ),
        }
    }
}

impl std::error::Error for ParserError {}

/// Convenience result type for parser operations.
pub type Result<T> = std::result::Result<T, ParserError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_parser_defect_is_labeled_with_its_own_error_code() {
        // ALOPEX-P007 は ALOPEX-P001 (UnexpectedToken) と機械的に区別できな
        // ければならない。運用者がパーサーのバグ (Nim FFI 境界の Defect) と
        // ユーザーの SQL 誤りを混同しないための契約。
        let defect = ParserError::InternalParserDefect {
            message: "field 'children' is not accessible".to_string(),
        };
        let rendered = defect.to_string();
        assert!(rendered.contains("ALOPEX-P007"));
        assert!(rendered.contains("field 'children' is not accessible"));

        let token = ParserError::UnexpectedToken {
            line: 1,
            column: 2,
            expected: "expr".to_string(),
            found: "EOF".to_string(),
        };
        assert!(token.to_string().contains("ALOPEX-P001"));
        assert_ne!(rendered, token.to_string());
    }
}
