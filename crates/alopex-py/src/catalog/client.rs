use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyModule};

use crate::catalog::resolve_credentials;
use crate::error;
use crate::types::{PyCatalogInfo, PyColumnInfo, PyNamespaceInfo, PyTableInfo};

#[allow(deprecated)]
fn default_credential_provider() -> PyObject {
    Python::with_gil(|py| "auto".into_py(py))
}

fn to_embedded_columns(columns: Vec<PyColumnInfo>) -> Vec<alopex_embedded::ColumnInfo> {
    columns
        .into_iter()
        .map(|col| alopex_embedded::ColumnInfo {
            name: col.name,
            type_name: col.type_name,
            position: col.position,
            nullable: col.nullable,
            comment: col.comment,
        })
        .collect()
}

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

    #[staticmethod]
    fn create_catalog(name: &str) -> PyResult<()> {
        alopex_embedded::Catalog::create_catalog(name).map_err(error::embedded_err)
    }

    #[staticmethod]
    fn delete_catalog(name: &str) -> PyResult<()> {
        alopex_embedded::Catalog::delete_catalog(name).map_err(error::embedded_err)
    }

    #[staticmethod]
    fn create_namespace(catalog_name: &str, namespace: &str) -> PyResult<()> {
        alopex_embedded::Catalog::create_namespace(catalog_name, namespace)
            .map_err(error::embedded_err)
    }

    #[staticmethod]
    fn delete_namespace(catalog_name: &str, namespace: &str) -> PyResult<()> {
        alopex_embedded::Catalog::delete_namespace(catalog_name, namespace)
            .map_err(error::embedded_err)
    }

    #[staticmethod]
    #[pyo3(signature = (
        catalog_name,
        namespace,
        table_name,
        columns,
        storage_location,
        data_source_format = "parquet"
    ))]
    fn create_table(
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
        columns: Vec<PyColumnInfo>,
        storage_location: String,
        data_source_format: &str,
    ) -> PyResult<()> {
        if data_source_format != "parquet" {
            return Err(error::to_py_err(format!(
                "Unsupported format: {}",
                data_source_format
            )));
        }
        let columns = to_embedded_columns(columns);
        alopex_embedded::Catalog::create_table(
            catalog_name,
            namespace,
            table_name,
            columns,
            Some(storage_location),
            Some(data_source_format.to_string()),
        )
        .map_err(error::embedded_err)
    }

    #[staticmethod]
    fn delete_table(catalog_name: &str, namespace: &str, table_name: &str) -> PyResult<()> {
        alopex_embedded::Catalog::delete_table(catalog_name, namespace, table_name)
            .map_err(error::embedded_err)
    }

    #[staticmethod]
    #[pyo3(signature = (
        catalog_name,
        namespace,
        table_name,
        credential_provider = default_credential_provider(),
        storage_options = None
    ))]
    fn scan_table(
        py: Python<'_>,
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
        credential_provider: PyObject,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<Py<PyAny>> {
        require_polars(py)?;
        let table_info =
            alopex_embedded::Catalog::get_table_info(catalog_name, namespace, table_name)
                .map_err(error::embedded_err)?;
        if table_info.data_source_format.as_deref() != Some("parquet") {
            return Err(error::to_py_err(format!(
                "Unsupported format: {:?}",
                table_info.data_source_format
            )));
        }
        let storage_location = table_info
            .storage_location
            .ok_or_else(|| error::to_py_err("storage_location is required"))?;
        let credential_provider = credential_provider.bind(py);
        let resolved =
            resolve_credentials(py, credential_provider, storage_options, &storage_location)?;
        let polars = PyModule::import(py, "polars")?;
        let scan_parquet = polars.getattr("scan_parquet")?;
        let options = PyDict::new(py);
        for (key, value) in resolved {
            options.set_item(key, value)?;
        }
        let args = (storage_location,);
        let kwargs = if options.is_empty() {
            None
        } else {
            Some(options)
        };
        let lazy_frame = scan_parquet.call(args, kwargs.as_ref())?;
        Ok(lazy_frame.into())
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
