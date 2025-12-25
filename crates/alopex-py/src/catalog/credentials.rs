use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyAny;

#[allow(dead_code)]
pub fn resolve_credentials(
    _py: Python<'_>,
    _credential_provider: &Bound<'_, PyAny>,
    storage_options: Option<HashMap<String, String>>,
    _storage_location: &str,
) -> PyResult<HashMap<String, String>> {
    Ok(storage_options.unwrap_or_default())
}
