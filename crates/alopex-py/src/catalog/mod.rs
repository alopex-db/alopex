//! Catalog bindings and helpers for the Python module.
//!
//! Examples:
//!     >>> from alopex import Catalog
//!     >>> Catalog.list_catalogs()

use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

mod client;
mod credentials;
mod models;
mod validation;

use crate::catalog::credentials::_resolve_credentials;

/// Re-export the Catalog API entry point.
#[allow(unused_imports)]
pub use client::{require_polars, PyCatalog};
/// Re-export credential resolution helper.
#[allow(unused_imports)]
pub use credentials::resolve_credentials;
/// Re-export catalog model types.
#[allow(unused_imports)]
pub use models::{PyCatalogInfo, PyColumnInfo, PyNamespaceInfo, PyTableInfo};
/// Re-export input validation helpers.
#[allow(unused_imports)]
pub use validation::{validate_identifier, validate_storage_location};

/// Register catalog bindings under a Python module.
///
/// Args:
///     m (module): Target Python module.
///
/// Returns:
///     None
///
/// Examples:
///     >>> # Internal use from module initialization.
///
/// Raises:
///     AlopexError: If module registration fails.
pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    client::register(py, m)?;
    m.add_class::<models::PyCatalogInfo>()?;
    m.add_class::<models::PyNamespaceInfo>()?;
    m.add_class::<models::PyTableInfo>()?;
    m.add_class::<models::PyColumnInfo>()?;
    m.add_function(wrap_pyfunction!(_resolve_credentials, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
