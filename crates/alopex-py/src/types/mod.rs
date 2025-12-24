use pyo3::prelude::*;
use pyo3::types::PyModule;

mod catalog;
mod config;
mod results;

pub use catalog::{PyCatalogInfo, PyColumnInfo, PyNamespaceInfo, PyTableInfo};
pub use config::{
    PyDatabaseOptions, PyEmbeddedConfig, PyHnswConfig, PyMetric, PyStorageMode, PyTxnMode,
};
pub use results::{PyHnswStats, PyMemoryStats, PySearchResult};

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTxnMode>()?;
    m.add_class::<PyMetric>()?;
    m.add_class::<PyStorageMode>()?;
    m.add_class::<PyHnswConfig>()?;
    m.add_class::<PyEmbeddedConfig>()?;
    m.add_class::<PyDatabaseOptions>()?;
    m.add_class::<PySearchResult>()?;
    m.add_class::<PyHnswStats>()?;
    m.add_class::<PyMemoryStats>()?;
    m.add_class::<PyCatalogInfo>()?;
    m.add_class::<PyNamespaceInfo>()?;
    m.add_class::<PyTableInfo>()?;
    m.add_class::<PyColumnInfo>()?;
    Ok(())
}
