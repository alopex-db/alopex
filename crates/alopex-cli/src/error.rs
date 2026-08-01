//! Error types for Alopex CLI
//!
//! This module defines CLI-specific error types using thiserror.

use thiserror::Error;

use crate::batch::ExitCode;

/// CLI-specific error type.
///
/// This enum represents all possible errors that can occur during CLI operations.
#[derive(Error, Debug)]
pub enum CliError {
    /// An error from the underlying database layer.
    #[error("Database error: {0}")]
    Database(#[from] alopex_embedded::Error),

    /// An I/O error occurred.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// An invalid argument was provided.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// A specified profile was not found.
    #[error("プロファイル '{0}' が見つかりません。alopex profile list で一覧を確認してください。")]
    #[allow(dead_code)]
    ProfileNotFound(String),

    /// Conflicting CLI options were provided.
    #[error(
        "`--profile` と `--data-dir` は同時に指定できません。どちらか一方を使用してください。"
    )]
    #[allow(dead_code)]
    ConflictingOptions,

    /// A transaction ID is invalid.
    #[error(
        "トランザクション ID '{0}' は無効です。alopex kv txn begin で新しいトランザクションを開始してください。"
    )]
    #[allow(dead_code)]
    InvalidTransactionId(String),

    /// A transaction timed out and was rolled back.
    #[error(
        "トランザクション '{0}' がタイムアウトしました（60秒）。自動的にロールバックされました。"
    )]
    #[allow(dead_code)]
    TransactionTimeout(String),

    /// A query exceeded its deadline.
    #[error("Query timeout: {0}. Suggestion: Use --deadline 120s to increase timeout.")]
    #[allow(dead_code)]
    Timeout(String),

    /// A query was cancelled by the user.
    #[error("Query cancelled by user.")]
    #[allow(dead_code)]
    Cancelled,

    /// No SQL query was provided.
    #[error("SQL クエリを指定してください。引数、-f オプション、または標準入力から入力できます。")]
    #[allow(dead_code)]
    NoQueryProvided,

    /// An unknown index type was specified.
    #[error("未知のインデックスタイプ: '{0}'。許可値: minmax, bloom")]
    #[allow(dead_code)]
    UnknownIndexType(String),

    /// An unknown compression type was specified.
    #[error("未知の圧縮形式: '{0}'。許可値: lz4, zstd, none")]
    #[allow(dead_code)]
    UnknownCompressionType(String),

    /// File format is incompatible with the CLI version.
    #[error("ファイルフォーマット v{file} は CLI v{cli} でサポートされていません。CLI をアップグレードしてください。")]
    #[allow(dead_code)]
    IncompatibleVersion { cli: String, file: String },

    /// An S3-related error occurred.
    #[error("S3 error: {0}")]
    #[allow(dead_code)]
    S3(String),

    /// Credentials are missing or invalid.
    #[error("Credentials error: {0}")]
    Credentials(String),

    /// A parsing error occurred.
    #[error("Parse error: {0}")]
    #[allow(dead_code)]
    Parse(String),

    /// A JSON serialization/deserialization error occurred.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// A server connection error occurred.
    #[error("Server connection error: {0}")]
    #[allow(dead_code)]
    ServerConnection(String),

    /// A server does not support the requested command.
    #[error("Server does not support this command: {0}")]
    #[allow(dead_code)]
    ServerUnsupported(String),

    /// Classified terminal outcome of a distributed SQL read. Its exit code
    /// is fixed by the wire/CLI classification rather than by a generic error.
    #[error("Distributed read {outcome}: {reason}")]
    DistributedReadOutcome {
        outcome: String,
        reason: String,
        exit_code: ExitCode,
    },

    /// A versioned transaction response completed with a non-success or
    /// non-terminal outcome. The canonical outcome was already rendered to
    /// stdout before this error is returned.
    #[error("Transaction {outcome}: {reason}")]
    TransactionOutcome {
        outcome: String,
        reason: String,
        exit_code: ExitCode,
    },

    /// A cluster management request completed with a classified non-success
    /// outcome. The response was already written to stdout before this error
    /// is returned, preserving the operation ID for automation.
    #[error("Cluster management {outcome}: {reason}")]
    ClusterManagementOutcome {
        outcome: String,
        reason: String,
        exit_code: ExitCode,
    },

    /// A CRDT command returned a canonical non-success outcome after writing
    /// its structured result to stdout.
    #[error("CRDT {outcome}: {reason}")]
    CrdtOutcome {
        outcome: String,
        reason: String,
        exit_code: ExitCode,
    },

    /// A changefeed command returned a canonical non-success or pending
    /// outcome after its complete structured response was written to stdout.
    #[error("Changefeed {outcome}: {reason}")]
    ChangefeedOutcome {
        outcome: String,
        reason: String,
        exit_code: ExitCode,
    },
}

/// Type alias for CLI results.
pub type Result<T> = std::result::Result<T, CliError>;

/// Print an error for the top-level command dispatcher.
///
/// If `verbose` is true, prints the debug format (with stack trace info).
/// Otherwise, prints the display format (user-friendly message).
pub fn handle_error(error: &CliError, verbose: bool) {
    if verbose {
        eprintln!("Error: {:?}", error); // Debug format with stack trace
    } else {
        eprintln!("Error: {}", error); // Display format
    }
}

/// Return the process exit class associated with an error. Most CLI failures
/// remain the legacy generic error code; a cluster management response can
/// instead retain its explicit terminal, retryable, pending, or authorization
/// classification.
impl CliError {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::ClusterManagementOutcome { exit_code, .. } => *exit_code,
            Self::CrdtOutcome { exit_code, .. } => *exit_code,
            Self::ChangefeedOutcome { exit_code, .. } => *exit_code,
            Self::DistributedReadOutcome { exit_code, .. } => *exit_code,
            Self::TransactionOutcome { exit_code, .. } => *exit_code,
            _ => ExitCode::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_argument_error() {
        let err = CliError::InvalidArgument("test message".to_string());
        assert_eq!(format!("{}", err), "Invalid argument: test message");
    }

    #[test]
    fn test_s3_error() {
        let err = CliError::S3("bucket not found".to_string());
        assert_eq!(format!("{}", err), "S3 error: bucket not found");
    }

    #[test]
    fn test_credentials_error() {
        let err = CliError::Credentials("AWS_ACCESS_KEY_ID not set".to_string());
        assert_eq!(
            format!("{}", err),
            "Credentials error: AWS_ACCESS_KEY_ID not set"
        );
    }

    #[test]
    fn cluster_management_error_preserves_its_exit_class() {
        let err = CliError::ClusterManagementOutcome {
            outcome: "retryable_failure".to_string(),
            reason: "not_leader".to_string(),
            exit_code: ExitCode::Retryable,
        };

        assert_eq!(err.exit_code(), ExitCode::Retryable);
    }

    #[test]
    fn distributed_read_error_preserves_its_exit_class() {
        let err = CliError::DistributedReadOutcome {
            outcome: "unsupported".to_string(),
            reason: "remote DDL is outside the closed catalog".to_string(),
            exit_code: ExitCode::Unsupported,
        };
        assert_eq!(err.exit_code(), ExitCode::Unsupported);
    }

    #[test]
    fn test_parse_error() {
        let err = CliError::Parse("invalid JSON".to_string());
        assert_eq!(format!("{}", err), "Parse error: invalid JSON");
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: CliError = io_err.into();
        assert!(matches!(err, CliError::Io(_)));
    }
}
