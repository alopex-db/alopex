//! Offline candidate verification for the v0.8 release boundary.
//!
//! This crate deliberately has no dependency on product crates.  Its only
//! mutable output is the report directory supplied by the caller.

pub mod artifact_verify;
pub mod evidence;
pub mod gate;
pub mod input_bundle;
pub mod inventory;
pub mod manifest;
pub mod policy;
pub mod python_verify;
pub mod report;
pub mod sandbox;
pub mod scope_snapshot;
pub mod workspace;

use std::fmt;

pub type Result<T> = std::result::Result<T, VerificationError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationError {
    pub code: &'static str,
    pub detail: String,
}

impl VerificationError {
    pub fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for VerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for VerificationError {}

pub(crate) fn io_error(context: &str, error: std::io::Error) -> VerificationError {
    VerificationError::new("io_error", format!("{context}: {error}"))
}

pub(crate) fn json_error(context: &str, error: serde_json::Error) -> VerificationError {
    VerificationError::new("invalid_json", format!("{context}: {error}"))
}
