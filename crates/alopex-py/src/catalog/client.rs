use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::error;
use crate::types::{PyCatalogInfo, PyNamespaceInfo, PyTableInfo};

#[pyclass(name = "Catalog")]
pub struct PyCatalog;

#[pymethods]
impl PyCatalog {
    #[staticmethod]
    fn list_catalogs() -> PyResult<Vec<PyCatalogInfo>> {
        let catalogs = alopex_embedded::Catalog::list_catalogs().map_err(error::embedded_err)?;
        Ok(catalogs.into_iter().map(PyCatalogInfo::from).collect())
    }

    #[staticmethod]
    fn list_namespaces(catalog_name: &str) -> PyResult<Vec<PyNamespaceInfo>> {
        let namespaces =
            alopex_embedded::Catalog::list_namespaces(catalog_name).map_err(error::embedded_err)?;
        Ok(namespaces.into_iter().map(PyNamespaceInfo::from).collect())
    }

    #[staticmethod]
    fn list_tables(catalog_name: &str, namespace: &str) -> PyResult<Vec<PyTableInfo>> {
        let tables = alopex_embedded::Catalog::list_tables(catalog_name, namespace)
            .map_err(error::embedded_err)?;
        Ok(tables.into_iter().map(PyTableInfo::from).collect())
    }

    #[staticmethod]
    fn get_table_info(
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
    ) -> PyResult<PyTableInfo> {
        let table_info =
            alopex_embedded::Catalog::get_table_info(catalog_name, namespace, table_name)
                .map_err(error::embedded_err)?;
        Ok(PyTableInfo::from(table_info))
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
