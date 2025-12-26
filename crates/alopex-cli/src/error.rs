//! Error types for Alopex CLI
//!
//! This module defines CLI-specific error types using thiserror.

use thiserror::Error;

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
}

/// Type alias for CLI results.
pub type Result<T> = std::result::Result<T, CliError>;

/// Handle an error by printing it and exiting.
///
/// If `verbose` is true, prints the debug format (with stack trace info).
/// Otherwise, prints the display format (user-friendly message).
pub fn handle_error(error: CliError, verbose: bool) {
    if verbose {
        eprintln!("Error: {:?}", error); // Debug format with stack trace
    } else {
        eprintln!("Error: {}", error); // Display format
    }
    std::process::exit(1);
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
