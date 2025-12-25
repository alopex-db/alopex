use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

mod client;
mod credentials;

use crate::catalog::credentials::_resolve_credentials;

#[allow(unused_imports)]
pub use client::{require_polars, PyCatalog};
#[allow(unused_imports)]
pub use credentials::resolve_credentials;

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    client::register(py, m)?;
    m.add_function(wrap_pyfunction!(_resolve_credentials, m)?)?;
    Ok(())
}
