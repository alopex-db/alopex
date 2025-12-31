#![allow(dead_code)]

use pyo3::exceptions::PyValueError;
use pyo3::PyResult;

const ALLOWED_SCHEMES: [&str; 5] = ["file", "s3", "gs", "az", "abfs"];

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
