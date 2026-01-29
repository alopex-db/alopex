use std::collections::HashMap;
use std::env;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use crate::error;

#[allow(dead_code)]
const SENSITIVE_KEYS: &[&str] = &[
    "aws_secret_access_key",
    "aws_access_key_id",
    "account_key",
    "service_account_path",
    "token",
    "password",
    "secret",
];

/// Resolve storage credentials from environment variables.
///
/// Args:
///     storage_location (str): Storage URI or path (s3://, gs://, az://, abfs://).
///
/// Returns:
///     dict[str, str]: Credential key/value pairs for storage options.
///
/// Examples:
///     >>> auto_resolve_credentials("s3://bucket/path")
///
/// Raises:
///     None
pub fn auto_resolve_credentials(storage_location: &str) -> PyResult<HashMap<String, String>> {
    let location = storage_location.trim().to_ascii_lowercase();
    let mut credentials = HashMap::new();

    if location.starts_with("s3://") {
        if let Ok(access_key) = env::var("AWS_ACCESS_KEY_ID") {
            credentials.insert("aws_access_key_id".to_string(), access_key);
        }
        if let Ok(secret_key) = env::var("AWS_SECRET_ACCESS_KEY") {
            credentials.insert("aws_secret_access_key".to_string(), secret_key);
        }
        if let Ok(region) = env::var("AWS_REGION") {
            credentials.insert("aws_region".to_string(), region);
        }
    } else if location.starts_with("gs://") {
        if let Ok(path) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            credentials.insert("service_account_path".to_string(), path);
        }
    } else if location.starts_with("az://") || location.starts_with("abfs://") {
        if let Ok(account) = env::var("AZURE_STORAGE_ACCOUNT") {
            credentials.insert("account_name".to_string(), account);
        }
        if let Ok(key) = env::var("AZURE_STORAGE_KEY") {
            credentials.insert("account_key".to_string(), key);
        }
    }

    Ok(credentials)
}

/// Merge resolved credentials with explicit storage options.
///
/// Args:
///     resolved (dict[str, str]): Resolved credential options.
///     storage_options (dict[str, str] | None): Explicit storage options.
///
/// Returns:
///     dict[str, str]: Merged options with storage_options taking precedence.
///
/// Examples:
///     >>> merge_storage_options({"token": "a"}, {"token": "b"})
///
/// Raises:
///     None
pub fn merge_storage_options(
    mut resolved: HashMap<String, String>,
    storage_options: Option<HashMap<String, String>>,
) -> HashMap<String, String> {
    if let Some(options) = storage_options {
        for (key, value) in options {
            resolved.insert(key, value);
        }
    }
    resolved
}

/// Mask sensitive values in storage options.
///
/// Args:
///     storage_options (dict[str, str]): Storage options to mask.
///
/// Returns:
///     dict[str, str]: New map with sensitive values replaced by "***".
///
/// Examples:
///     >>> mask_sensitive_values({"token": "secret"})
///
/// Raises:
///     None
#[allow(dead_code)]
pub fn mask_sensitive_values(storage_options: &HashMap<String, String>) -> HashMap<String, String> {
    storage_options
        .iter()
        .map(|(key, value)| {
            let key_lower = key.to_ascii_lowercase();
            let masked = if SENSITIVE_KEYS
                .iter()
                .any(|sensitive| key_lower.contains(sensitive))
            {
                "***".to_string()
            } else {
                value.clone()
            };
            (key.clone(), masked)
        })
        .collect()
}

/// Resolve credentials using a provider and merge with storage options.
///
/// Args:
///     credential_provider (str | dict | None): "auto" or explicit options dict.
///     storage_options (dict[str, str] | None): Extra options to merge.
///     storage_location (str): Storage URI or path.
///
/// Returns:
///     dict[str, str]: Resolved and merged storage options.
///
/// Examples:
///     >>> resolve_credentials(py, "auto", None, "s3://bucket/path")
///
/// Raises:
///     AlopexError: If credential_provider is not "auto" or dict.
#[allow(dead_code)]
pub fn resolve_credentials(
    py: Python<'_>,
    credential_provider: &Bound<'_, PyAny>,
    storage_options: Option<HashMap<String, String>>,
    storage_location: &str,
) -> PyResult<HashMap<String, String>> {
    let mut resolved = HashMap::new();
    if credential_provider.is_none() {
        resolved = auto_resolve_credentials(storage_location)?;
    } else if let Ok(dict) = credential_provider.downcast::<PyDict>() {
        for (key, value) in dict {
            let key: String = key.extract()?;
            let value: String = value.extract()?;
            resolved.insert(key, value);
        }
    } else if let Ok(provider) = credential_provider.extract::<String>() {
        if provider == "auto" {
            resolved = auto_resolve_credentials(storage_location)?;
        } else {
            return Err(error::to_py_err(format!(
                "Unsupported credential_provider: {}",
                provider
            )));
        }
    } else {
        return Err(error::to_py_err(
            "credential_provider must be \"auto\" or dict[str, str]",
        ));
    }

    let _ = py;
    Ok(merge_storage_options(resolved, storage_options))
}

/// Resolve credentials for Python callers.
///
/// Args:
///     storage_location (str): Storage URI or path.
///     credential_provider (str | dict | None): "auto" or explicit options dict.
///     storage_options (dict[str, str] | None): Extra options to merge.
///
/// Returns:
///     dict[str, str]: Resolved and merged storage options.
///
/// Examples:
///     >>> from alopex._alopex import catalog as _catalog
///     >>> _catalog._resolve_credentials("s3://bucket/path", "auto")
///
/// Raises:
///     AlopexError: If credential_provider is invalid.
#[pyfunction]
#[pyo3(signature = (storage_location, credential_provider = None, storage_options = None))]
pub fn _resolve_credentials(
    py: Python<'_>,
    storage_location: &str,
    credential_provider: Option<PyObject>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<HashMap<String, String>> {
    let credential_provider = credential_provider.unwrap_or_else(|| py.None());
    let credential_provider = credential_provider.bind(py);
    resolve_credentials(py, credential_provider, storage_options, storage_location)
}
