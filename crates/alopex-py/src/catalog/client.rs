use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::error;
use crate::types::PyCatalogInfo;

#[pyclass(name = "Catalog")]
pub struct PyCatalog;

#[pymethods]
impl PyCatalog {
    #[staticmethod]
    fn list_catalogs() -> PyResult<Vec<PyCatalogInfo>> {
        Ok(Vec::new())
    }
}

#[allow(dead_code)]
pub fn require_polars(py: Python<'_>) -> PyResult<()> {
    if PyModule::import(py, "polars").is_ok() {
        Ok(())
    } else {
        Err(error::to_py_err(
            "polars が見つかりません。`pip install alopex[polars]` を実行してください",
        ))
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCatalog>()?;
    Ok(())
}
