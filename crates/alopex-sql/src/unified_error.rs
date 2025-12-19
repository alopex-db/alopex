//! SQL 実行パイプライン（Parse/Plan/Execute）を横断する統一エラー型。
//!
//! 公開 API としては「安定した形」を維持するため、内部エラー型（Parser/Planner/Executor）を
//! そのまま公開せず、`message / code / location` を持つフィールド形式のエラーを提供する。

use std::error::Error as StdError;
use std::fmt;

use crate::catalog::CatalogError;
use crate::error::ParserError;
use crate::executor::ExecutorError;
use crate::planner::PlannerError;
use crate::storage::StorageError;

/// エラー位置情報（1-based、未知の場合は 0）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ErrorLocation {
    pub line: u64,
    pub column: u64,
}

impl ErrorLocation {
    /// 位置情報が有効か判定する。
    pub fn is_known(&self) -> bool {
        self.line > 0 || self.column > 0
    }
}

impl fmt::Display for ErrorLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

/// 統一 SQL エラー型（公開 API）。
#[derive(Debug)]
pub enum SqlError {
    /// パースエラー。
    Parse {
        message: String,
        location: ErrorLocation,
        code: &'static str,
    },

    /// プランニングエラー（型エラー等）。
    Plan {
        message: String,
        location: ErrorLocation,
        code: &'static str,
    },

    /// 実行エラー。
    Execution { message: String, code: &'static str },

    /// ストレージエラー（REQ-4-3: `source` を保持してエラーチェーンを維持）。
    Storage {
        message: String,
        code: &'static str,
        source: Option<alopex_core::Error>,
    },

    /// カタログエラー（テーブル/インデックス等の参照・整合性）。
    Catalog {
        message: String,
        location: ErrorLocation,
        code: &'static str,
    },
}

impl SqlError {
    /// エラーコード（例: `ALOPEX-C001`）。
    pub fn code(&self) -> &'static str {
        match self {
            Self::Parse { code, .. }
            | Self::Plan { code, .. }
            | Self::Execution { code, .. }
            | Self::Storage { code, .. }
            | Self::Catalog { code, .. } => code,
        }
    }

    /// ユーザー向けメッセージ（位置情報は含めない）。
    pub fn message(&self) -> &str {
        match self {
            Self::Parse { message, .. }
            | Self::Plan { message, .. }
            | Self::Execution { message, .. }
            | Self::Storage { message, .. }
            | Self::Catalog { message, .. } => message,
        }
    }

    /// 位置情報（未知の場合は `{ line: 0, column: 0 }`）。
    pub fn location(&self) -> ErrorLocation {
        match self {
            Self::Parse { location, .. }
            | Self::Plan { location, .. }
            | Self::Catalog { location, .. } => *location,
            Self::Execution { .. } | Self::Storage { .. } => ErrorLocation::default(),
        }
    }

    /// span 情報付きメッセージを生成する（位置情報がない場合は位置部分を省略）。
    pub fn message_with_location(&self) -> String {
        let code = self.code();
        let message = self.message();
        let location = self.location();

        match self {
            Self::Storage { .. } => format!("error[{code}]: storage error: {message}"),
            _ if location.is_known() => format!(
                "error[{code}]: {message} at line {}, column {}",
                location.line, location.column
            ),
            _ => format!("error[{code}]: {message}"),
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message_with_location())
    }
}

impl StdError for SqlError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Storage {
                source: Some(source),
                ..
            } => Some(source),
            _ => None,
        }
    }
}

impl From<alopex_core::Error> for SqlError {
    fn from(e: alopex_core::Error) -> Self {
        let code = match e {
            alopex_core::Error::TxnConflict => "ALOPEX-S001",
            alopex_core::Error::TxnClosed => "ALOPEX-S002",
            alopex_core::Error::TxnReadOnly => "ALOPEX-S003",
            _ => "ALOPEX-S999",
        };

        Self::Storage {
            message: e.to_string(),
            code,
            source: Some(e),
        }
    }
}

impl From<ParserError> for SqlError {
    fn from(value: ParserError) -> Self {
        match value {
            ParserError::UnexpectedToken {
                line,
                column,
                expected,
                found,
            } => Self::Parse {
                message: format!("unexpected token: expected {expected}, found {found}"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-P001",
            },
            ParserError::ExpectedToken {
                line,
                column,
                expected,
                found,
            } => Self::Parse {
                message: format!("expected {expected} but found {found}"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-P002",
            },
            ParserError::UnterminatedString { line, column } => Self::Parse {
                message: "unterminated string literal".to_string(),
                location: ErrorLocation { line, column },
                code: "ALOPEX-P003",
            },
            ParserError::InvalidNumber {
                line,
                column,
                value,
            } => Self::Parse {
                message: format!("invalid number literal '{value}'"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-P004",
            },
            ParserError::InvalidVector { line, column } => Self::Parse {
                message: "invalid vector literal".to_string(),
                location: ErrorLocation { line, column },
                code: "ALOPEX-P005",
            },
            ParserError::RecursionLimitExceeded { depth } => Self::Parse {
                message: format!("recursion limit exceeded (depth: {depth})"),
                location: ErrorLocation::default(),
                code: "ALOPEX-P006",
            },
        }
    }
}

impl From<PlannerError> for SqlError {
    fn from(value: PlannerError) -> Self {
        match value {
            PlannerError::TableNotFound { name, line, column } => Self::Catalog {
                message: format!("table '{name}' not found"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-C001",
            },
            PlannerError::TableAlreadyExists { name } => Self::Catalog {
                message: format!("table '{name}' already exists"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C002",
            },
            PlannerError::ColumnNotFound {
                column,
                table,
                line,
                col,
            } => Self::Catalog {
                message: format!("column '{column}' not found in table '{table}'"),
                location: ErrorLocation { line, column: col },
                code: "ALOPEX-C003",
            },
            PlannerError::AmbiguousColumn {
                column,
                tables,
                line,
                col,
            } => Self::Catalog {
                message: format!("ambiguous column '{column}' found in tables: {tables:?}"),
                location: ErrorLocation { line, column: col },
                code: "ALOPEX-C004",
            },
            PlannerError::IndexAlreadyExists { name } => Self::Catalog {
                message: format!("index '{name}' already exists"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C005",
            },
            PlannerError::IndexNotFound { name } => Self::Catalog {
                message: format!("index '{name}' not found"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C006",
            },
            PlannerError::TypeMismatch {
                expected,
                found,
                line,
                column,
            } => Self::Plan {
                message: format!("type mismatch: expected {expected}, found {found}"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-T001",
            },
            PlannerError::InvalidOperator {
                op,
                type_name,
                line,
                column,
            } => Self::Plan {
                message: format!("invalid operator '{op}' for type '{type_name}'"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-T002",
            },
            PlannerError::NullConstraintViolation { column, line, col } => Self::Plan {
                message: format!("null constraint violation for column '{column}'"),
                location: ErrorLocation { line, column: col },
                code: "ALOPEX-T003",
            },
            PlannerError::VectorDimensionMismatch {
                expected,
                found,
                line,
                column,
            } => Self::Plan {
                message: format!("vector dimension mismatch: expected {expected}, found {found}"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-T004",
            },
            PlannerError::InvalidMetric {
                value,
                line,
                column,
            } => Self::Plan {
                message: format!("invalid metric '{value}' (valid: cosine, l2, inner)"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-T005",
            },
            PlannerError::ColumnValueCountMismatch {
                columns,
                values,
                line,
                column,
            } => Self::Plan {
                message: format!("column count ({columns}) does not match value count ({values})"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-T006",
            },
            PlannerError::UnsupportedFeature {
                feature,
                version,
                line,
                column,
            } => Self::Plan {
                message: format!("feature '{feature}' is not supported (expected in {version})"),
                location: ErrorLocation { line, column },
                code: "ALOPEX-F001",
            },
        }
    }
}

impl From<StorageError> for SqlError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::TransactionConflict => Self::Storage {
                message: "transaction conflict".to_string(),
                code: "ALOPEX-S001",
                source: Some(alopex_core::Error::TxnConflict),
            },
            StorageError::TransactionReadOnly => Self::Storage {
                message: "transaction is read-only".to_string(),
                code: "ALOPEX-S003",
                source: Some(alopex_core::Error::TxnReadOnly),
            },
            StorageError::TransactionClosed => Self::Storage {
                message: "transaction is closed".to_string(),
                code: "ALOPEX-S002",
                source: Some(alopex_core::Error::TxnClosed),
            },
            StorageError::KvError(core_error) => Self::from(core_error),
            other => Self::Storage {
                message: other.to_string(),
                code: "ALOPEX-S999",
                source: None,
            },
        }
    }
}

impl From<CatalogError> for SqlError {
    fn from(value: CatalogError) -> Self {
        match value {
            CatalogError::Kv(core_error) => Self::from(StorageError::from(core_error)),
            other => Self::Catalog {
                message: format!("catalog persistence error: {other}"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C999",
            },
        }
    }
}

impl From<ExecutorError> for SqlError {
    fn from(value: ExecutorError) -> Self {
        match value {
            ExecutorError::Planner(planner_error) => Self::from(planner_error),
            ExecutorError::Core(core_error) => Self::from(core_error),
            ExecutorError::Storage(storage_error) => Self::from(storage_error),
            ExecutorError::TransactionConflict => Self::Execution {
                message: "transaction conflict".to_string(),
                code: "ALOPEX-E001",
            },
            ExecutorError::ReadOnlyTransaction { operation } => Self::Execution {
                message: format!("read-only transaction: cannot execute {operation}"),
                code: "ALOPEX-E002",
            },
            ExecutorError::TableNotFound(name) => Self::Catalog {
                message: format!("table '{name}' not found"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C001",
            },
            ExecutorError::TableAlreadyExists(name) => Self::Catalog {
                message: format!("table '{name}' already exists"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C002",
            },
            ExecutorError::IndexNotFound(name) => Self::Catalog {
                message: format!("index '{name}' not found"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C006",
            },
            ExecutorError::IndexAlreadyExists(name) => Self::Catalog {
                message: format!("index '{name}' already exists"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C005",
            },
            ExecutorError::ColumnNotFound(column) => Self::Catalog {
                message: format!("column '{column}' not found"),
                location: ErrorLocation::default(),
                code: "ALOPEX-C003",
            },
            other => Self::Execution {
                message: other.to_string(),
                code: "ALOPEX-E999",
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_parser_error_preserves_location() {
        let parser_error = ParserError::UnexpectedToken {
            line: 12,
            column: 34,
            expected: "SELECT".into(),
            found: "SELEC".into(),
        };

        let unified: SqlError = parser_error.into();
        assert_eq!(unified.code(), "ALOPEX-P001");
        assert_eq!(
            unified.location(),
            ErrorLocation {
                line: 12,
                column: 34
            }
        );
    }

    #[test]
    fn from_planner_error_preserves_code() {
        let planner_error = PlannerError::TableNotFound {
            name: "users".into(),
            line: 1,
            column: 8,
        };

        let unified: SqlError = planner_error.into();
        assert_eq!(unified.code(), "ALOPEX-C001");
        assert_eq!(unified.location(), ErrorLocation { line: 1, column: 8 });
    }

    #[test]
    fn message_with_location_format() {
        let parser_error = ParserError::InvalidNumber {
            line: 3,
            column: 7,
            value: "12x".into(),
        };

        let unified: SqlError = parser_error.into();
        assert_eq!(
            unified.message_with_location(),
            "error[ALOPEX-P004]: invalid number literal '12x' at line 3, column 7"
        );
    }

    #[test]
    fn from_executor_core_error_maps_to_storage_and_preserves_source() {
        let unified: SqlError = ExecutorError::Core(alopex_core::Error::TxnConflict).into();
        assert_eq!(unified.code(), "ALOPEX-S001");
        assert!(unified.source().is_some());
        assert_eq!(
            unified.message_with_location(),
            "error[ALOPEX-S001]: storage error: transaction conflict"
        );
    }

    #[test]
    fn from_executor_core_readonly_maps_to_storage_and_preserves_source() {
        let unified: SqlError = ExecutorError::Core(alopex_core::Error::TxnReadOnly).into();
        assert_eq!(unified.code(), "ALOPEX-S003");
        assert!(unified.source().is_some());
        assert_eq!(
            unified.message_with_location(),
            "error[ALOPEX-S003]: storage error: transaction is read-only"
        );
    }

    #[test]
    fn from_executor_readonly_transaction_maps_to_execution_code() {
        let unified: SqlError = ExecutorError::ReadOnlyTransaction {
            operation: "INSERT".to_string(),
        }
        .into();
        assert_eq!(unified.code(), "ALOPEX-E002");
        assert_eq!(
            unified.message_with_location(),
            "error[ALOPEX-E002]: read-only transaction: cannot execute INSERT"
        );
    }
}
