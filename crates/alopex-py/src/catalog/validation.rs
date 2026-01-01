#![allow(dead_code)]

use pyo3::exceptions::PyValueError;
use pyo3::PyResult;

const ALLOWED_SCHEMES: [&str; 5] = ["file", "s3", "gs", "az", "abfs"];

/// Validate that a catalog identifier matches `[A-Za-z_][A-Za-z0-9_]*`.
///
/// Args:
///     name (str): Identifier to validate.
///
/// Returns:
///     None
///
/// Examples:
///     >>> validate_identifier("main")
///
/// Raises:
///     ValueError: If the identifier is empty or contains invalid characters.
pub fn validate_identifier(name: &str) -> PyResult<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(PyValueError::new_err("identifier must not be empty"));
    };
    if !is_identifier_start(first) || !chars.all(is_identifier_part) {
        return Err(PyValueError::new_err(format!(
            "invalid identifier: {} (allowed: [A-Za-z_][A-Za-z0-9_]*)",
            name
        )));
    }
    Ok(())
}

/// Validate that a storage location uses an allowed scheme or local path.
///
/// Args:
///     location (str): Storage location URI/path.
///
/// Returns:
///     None
///
/// Examples:
///     >>> validate_storage_location("file:///tmp/data.parquet")
///     >>> validate_storage_location("/tmp/data.parquet")
///
/// Raises:
///     ValueError: If the location is empty or uses an unsupported scheme.
pub fn validate_storage_location(location: &str) -> PyResult<()> {
    if location.trim().is_empty() {
        return Err(PyValueError::new_err("storage_location must not be empty"));
    }
    if let Some(pos) = location.find("://") {
        let scheme = location[..pos].to_ascii_lowercase();
        if !ALLOWED_SCHEMES.contains(&scheme.as_str()) {
            return Err(PyValueError::new_err(format!(
                "unsupported storage_location scheme: {} (allowed: file://, s3://, gs://, az://, abfs://, or local paths)",
                scheme
            )));
        }
    }
    Ok(())
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_part(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}
