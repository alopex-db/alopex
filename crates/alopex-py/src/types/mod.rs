use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

mod catalog;
pub(crate) mod cluster;
mod config;
mod dataframe;
mod results;

pub use catalog::{PyCatalogInfo, PyColumnInfo, PyNamespaceInfo, PyTableInfo};
pub use config::{
    PyDatabaseOptions, PyEmbeddedConfig, PyHnswConfig, PyMetric, PyStorageMode, PyTxnMode,
};
pub(crate) use dataframe::{streaming_dataframe_err, DataFrameStreamRegistry};
pub use dataframe::{
    PyDataFrame, PyDataFrameStream, PyDatetimeNamespace, PyExpr, PyLazyFrame, PyListNamespace,
    PyStringNamespace,
};
pub(crate) use results::crdt_outcome_to_py;
pub use results::{PyHnswStats, PyMemoryStats, PySearchResult};

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTxnMode>()?;
    m.add_class::<PyMetric>()?;
    m.add_class::<PyStorageMode>()?;
    m.add_class::<PyHnswConfig>()?;
    m.add_class::<PyEmbeddedConfig>()?;
    m.add_class::<PyDatabaseOptions>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyExpr>()?;
    m.add_class::<PyLazyFrame>()?;
    m.add_class::<PyDataFrameStream>()?;
    m.add_class::<PyStringNamespace>()?;
    m.add_class::<PyDatetimeNamespace>()?;
    m.add_class::<PyListNamespace>()?;
    m.add_class::<PySearchResult>()?;
    m.add_class::<PyHnswStats>()?;
    m.add_class::<PyMemoryStats>()?;
    m.add_class::<PyCatalogInfo>()?;
    m.add_class::<PyNamespaceInfo>()?;
    m.add_class::<PyTableInfo>()?;
    m.add_class::<PyColumnInfo>()?;
    m.add_function(wrap_pyfunction!(dataframe::py_col, m)?)?;
    m.add_function(wrap_pyfunction!(dataframe::py_lit, m)?)?;
    m.add_function(wrap_pyfunction!(dataframe::py_concat, m)?)?;
    m.add_function(wrap_pyfunction!(dataframe::py_concat_str, m)?)?;
    Ok(())
}
