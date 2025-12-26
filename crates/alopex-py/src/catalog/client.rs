use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyModule};

use crate::catalog::resolve_credentials;
use crate::error;
use crate::types::{PyCatalogInfo, PyColumnInfo, PyNamespaceInfo, PyTableInfo};

#[allow(deprecated)]
fn default_credential_provider() -> PyObject {
    Python::with_gil(|py| "auto".into_py(py))
}

fn columns_from_schema(
    schema: &Bound<'_, PyDict>,
) -> PyResult<Vec<alopex_embedded::catalog::ColumnInfo>> {
    let mut columns = Vec::with_capacity(schema.len());
    for (position, (name, dtype)) in schema.iter().enumerate() {
        let name: String = name.extract()?;
        let type_name = dtype.str()?.extract::<String>()?;
        columns.push(alopex_embedded::catalog::ColumnInfo {
            name,
            type_name,
            position,
            nullable: true,
            comment: None,
        });
    }
    Ok(columns)
}

fn schema_from_dataframe(
    df: &Bound<'_, PyAny>,
) -> PyResult<Vec<alopex_embedded::catalog::ColumnInfo>> {
    let schema = df.getattr("schema")?;
    let schema = schema.downcast::<PyDict>()?;
    columns_from_schema(schema)
}

fn to_embedded_columns(columns: Vec<PyColumnInfo>) -> Vec<alopex_embedded::catalog::ColumnInfo> {
    columns
        .into_iter()
        .map(|col| alopex_embedded::catalog::ColumnInfo {
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

    #[staticmethod]
    #[pyo3(signature = (
        df,
        catalog_name,
        namespace,
        table_name,
        delta_mode = "error",
        storage_location = None,
        credential_provider = default_credential_provider()
    ))]
    #[allow(clippy::too_many_arguments)]
    fn write_table(
        py: Python<'_>,
        df: PyObject,
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
        delta_mode: &str,
        storage_location: Option<String>,
        credential_provider: PyObject,
    ) -> PyResult<()> {
        require_polars(py)?;
        let df = df.bind(py);
        let df_type = df.get_type().name()?;
        let df_obj = if df_type == "LazyFrame" {
            df.call_method0("collect")?.unbind()
        } else {
            df.clone().unbind()
        };
        let df_bound = df_obj.bind(py);
        let columns = schema_from_dataframe(df_bound)?;
        let credential_provider = credential_provider.bind(py);

        let table_info =
            match alopex_embedded::Catalog::get_table_info(catalog_name, namespace, table_name) {
                Ok(info) => Some(info),
                Err(alopex_embedded::Error::TableNotFound(_)) => None,
                Err(err) => return Err(error::embedded_err(err)),
            };

        let target_location = match table_info {
            Some(info) => {
                if info.data_source_format.as_deref() != Some("parquet") {
                    return Err(error::to_py_err(format!(
                        "Unsupported format: {:?}",
                        info.data_source_format
                    )));
                }
                match delta_mode {
                    "error" => {
                        return Err(error::to_py_err("table already exists"));
                    }
                    "ignore" => {
                        return Ok(());
                    }
                    "append" | "overwrite" => info.storage_location,
                    other => {
                        return Err(error::to_py_err(format!(
                            "Unsupported delta_mode: {}",
                            other
                        )));
                    }
                }
            }
            None => match delta_mode {
                "ignore" => return Ok(()),
                "error" => {
                    return Err(error::to_py_err("table not found"));
                }
                "append" | "overwrite" => {
                    let location = storage_location
                        .ok_or_else(|| error::to_py_err("storage_location is required"))?;
                    alopex_embedded::Catalog::create_table(
                        catalog_name,
                        namespace,
                        table_name,
                        columns,
                        Some(location.clone()),
                        Some("parquet".to_string()),
                    )
                    .map_err(error::embedded_err)?;
                    Some(location)
                }
                other => {
                    return Err(error::to_py_err(format!(
                        "Unsupported delta_mode: {}",
                        other
                    )));
                }
            },
        };

        let storage_location =
            target_location.ok_or_else(|| error::to_py_err("storage_location is required"))?;
        let resolved = resolve_credentials(py, credential_provider, None, &storage_location)?;
        let polars = PyModule::import(py, "polars")?;

        let mut to_write = df_obj.clone_ref(py);
        if delta_mode == "append" {
            let scan_parquet = polars.getattr("scan_parquet")?;
            let read_options = PyDict::new(py);
            for (key, value) in &resolved {
                read_options.set_item(key, value)?;
            }
            let args = (storage_location.as_str(),);
            let kwargs = if read_options.is_empty() {
                None
            } else {
                Some(read_options)
            };
            let existing_lf = scan_parquet.call(args, kwargs.as_ref())?;
            let existing_df = existing_lf.call_method0("collect")?;
            let concat = polars.getattr("concat")?;
            let list = PyList::new(py, &[existing_df.unbind(), to_write.clone_ref(py)])?;
            let combined = concat.call1((list,))?;
            to_write = combined.unbind();
        }

        let write_options = PyDict::new(py);
        for (key, value) in resolved {
            write_options.set_item(key, value)?;
        }
        let args = (storage_location.as_str(),);
        let kwargs = if write_options.is_empty() {
            None
        } else {
            Some(write_options)
        };
        let df_to_write = to_write.bind(py);
        df_to_write.call_method("write_parquet", args, kwargs.as_ref())?;
        Ok(())
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
