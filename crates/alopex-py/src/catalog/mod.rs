use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

mod client;
mod credentials;
mod models;
mod validation;

use crate::catalog::credentials::_resolve_credentials;

#[allow(unused_imports)]
pub use client::{require_polars, PyCatalog};
#[allow(unused_imports)]
pub use credentials::resolve_credentials;
#[allow(unused_imports)]
pub use models::{PyCatalogInfo, PyColumnInfo, PyNamespaceInfo, PyTableInfo};
#[allow(unused_imports)]
pub use validation::{validate_identifier, validate_storage_location};

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    client::register(py, m)?;
    m.add_class::<models::PyCatalogInfo>()?;
    m.add_class::<models::PyNamespaceInfo>()?;
    m.add_class::<models::PyTableInfo>()?;
    m.add_class::<models::PyColumnInfo>()?;
    m.add_function(wrap_pyfunction!(_resolve_credentials, m)?)?;
    Ok(())
}
