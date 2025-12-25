use pyo3::prelude::*;
use pyo3::types::PyModule;

mod client;
mod credentials;

#[allow(unused_imports)]
pub use client::{require_polars, PyCatalog};
#[allow(unused_imports)]
pub use credentials::resolve_credentials;

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    client::register(py, m)?;
    Ok(())
}
