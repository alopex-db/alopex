use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyModule};

use super::{
    validate_identifier, validate_storage_location, PyCatalogInfo, PyColumnInfo, PyNamespaceInfo,
    PyTableInfo,
};
use crate::catalog::resolve_credentials;
use crate::error;

#[allow(deprecated)]
fn default_credential_provider() -> PyObject {
    Python::with_gil(|py| "auto".into_py(py))
}

fn normalize_to_dataframe<'py>(
    _py: Python<'py>,
    df: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let type_name = df.get_type().name()?;
    if type_name == "LazyFrame" {
        df.call_method0("collect")
    } else {
        Ok(df.clone())
    }
}

fn polars_dtype_to_alopex_type(dtype: &str) -> String {
    let dtype = dtype.split('(').next().unwrap_or(dtype);
    match dtype {
        "Int8" | "Int16" | "Int32" => "INTEGER".to_string(),
        "Int64" => "BIGINT".to_string(),
        "Float32" => "FLOAT".to_string(),
        "Float64" => "DOUBLE".to_string(),
        "Utf8" | "String" => "TEXT".to_string(),
        "Binary" => "BLOB".to_string(),
        "Boolean" => "BOOLEAN".to_string(),
        "Datetime" | "Date" | "Time" => "TIMESTAMP".to_string(),
        _ => dtype.to_string(),
    }
}

fn infer_columns_from_dataframe(df: &Bound<'_, PyAny>) -> PyResult<Vec<PyColumnInfo>> {
    let schema = df.getattr("schema")?;
    let schema = schema.downcast::<PyDict>()?;
    let mut columns = Vec::with_capacity(schema.len());
    for (position, (name, dtype)) in schema.iter().enumerate() {
        let name: String = name.extract()?;
        let dtype = dtype.str()?.extract::<String>()?;
        let type_name = polars_dtype_to_alopex_type(&dtype);
        columns.push(PyColumnInfo {
            name,
            type_name,
            position,
            nullable: true,
            comment: None,
        });
    }
    Ok(columns)
}

fn storage_options_to_kwargs(
    py: Python<'_>,
    storage_options: &HashMap<String, String>,
) -> PyResult<Option<Py<PyDict>>> {
    if storage_options.is_empty() {
        return Ok(None);
    }
    let kwargs = PyDict::new(py);
    for (key, value) in storage_options {
        kwargs.set_item(key, value)?;
    }
    Ok(Some(kwargs.unbind()))
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
    fn list_catalogs(py: Python<'_>) -> PyResult<Vec<PyCatalogInfo>> {
        let catalogs = py
            .allow_threads(alopex_embedded::Catalog::list_catalogs)
            .map_err(error::embedded_err)?;
        Ok(catalogs.into_iter().map(PyCatalogInfo::from).collect())
    }

    #[staticmethod]
    fn list_namespaces(py: Python<'_>, catalog_name: &str) -> PyResult<Vec<PyNamespaceInfo>> {
        validate_identifier(catalog_name)?;
        let namespaces = py
            .allow_threads(|| alopex_embedded::Catalog::list_namespaces(catalog_name))
            .map_err(error::embedded_err)?;
        Ok(namespaces.into_iter().map(PyNamespaceInfo::from).collect())
    }

    #[staticmethod]
    fn list_tables(
        py: Python<'_>,
        catalog_name: &str,
        namespace: &str,
    ) -> PyResult<Vec<PyTableInfo>> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        let tables = py
            .allow_threads(|| alopex_embedded::Catalog::list_tables(catalog_name, namespace))
            .map_err(error::embedded_err)?;
        Ok(tables.into_iter().map(PyTableInfo::from).collect())
    }

    #[staticmethod]
    fn get_table_info(
        py: Python<'_>,
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
    ) -> PyResult<PyTableInfo> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        validate_identifier(table_name)?;
        let table_info = py
            .allow_threads(|| {
                alopex_embedded::Catalog::get_table_info(catalog_name, namespace, table_name)
            })
            .map_err(error::embedded_err)?;
        Ok(PyTableInfo::from(table_info))
    }

    #[staticmethod]
    fn create_catalog(py: Python<'_>, name: &str) -> PyResult<()> {
        validate_identifier(name)?;
        py.allow_threads(|| alopex_embedded::Catalog::create_catalog(name))
            .map_err(error::embedded_err)
    }

    #[staticmethod]
    fn delete_catalog(py: Python<'_>, name: &str) -> PyResult<()> {
        validate_identifier(name)?;
        py.allow_threads(|| alopex_embedded::Catalog::delete_catalog(name))
            .map_err(error::embedded_err)
    }

    #[staticmethod]
    fn create_namespace(py: Python<'_>, catalog_name: &str, namespace: &str) -> PyResult<()> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        py.allow_threads(|| alopex_embedded::Catalog::create_namespace(catalog_name, namespace))
            .map_err(|err| match err {
                alopex_embedded::Error::CatalogNotFound(name) => {
                    error::AlopexError::ParentNotFound(name).into()
                }
                other => error::embedded_err(other),
            })
    }

    #[staticmethod]
    fn delete_namespace(py: Python<'_>, catalog_name: &str, namespace: &str) -> PyResult<()> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        py.allow_threads(|| alopex_embedded::Catalog::delete_namespace(catalog_name, namespace))
            .map_err(error::embedded_err)
    }

    #[staticmethod]
    #[pyo3(signature = (
        catalog_name,
        namespace,
        table_name,
        columns,
        storage_location,
        data_source_format = "PARQUET"
    ))]
    fn create_table(
        py: Python<'_>,
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
        columns: Vec<PyColumnInfo>,
        storage_location: String,
        data_source_format: &str,
    ) -> PyResult<()> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        validate_identifier(table_name)?;
        validate_storage_location(&storage_location)?;

        let normalized_format = data_source_format.trim().to_ascii_uppercase();
        if normalized_format != "PARQUET" {
            return Err(error::AlopexError::UnsupportedFormat(normalized_format).into());
        }
        let embedded_format = normalized_format.to_ascii_lowercase();
        let columns = to_embedded_columns(columns);
        py.allow_threads(move || {
            alopex_embedded::Catalog::create_table(
                catalog_name,
                namespace,
                table_name,
                columns,
                Some(storage_location),
                Some(embedded_format),
            )
        })
        .map_err(|err| match err {
            alopex_embedded::Error::CatalogNotFound(name) => {
                error::AlopexError::ParentNotFound(name).into()
            }
            alopex_embedded::Error::NamespaceNotFound(catalog, namespace) => {
                error::AlopexError::ParentNotFound(format!("{}.{}", catalog, namespace)).into()
            }
            other => error::embedded_err(other),
        })
    }

    #[staticmethod]
    fn delete_table(
        py: Python<'_>,
        catalog_name: &str,
        namespace: &str,
        table_name: &str,
    ) -> PyResult<()> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        validate_identifier(table_name)?;
        py.allow_threads(|| {
            alopex_embedded::Catalog::delete_table(catalog_name, namespace, table_name)
        })
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
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        validate_identifier(table_name)?;
        let table_info = py
            .allow_threads(|| {
                alopex_embedded::Catalog::get_table_info(catalog_name, namespace, table_name)
            })
            .map_err(error::embedded_err)?;
        let storage_location = table_info
            .storage_location
            .ok_or_else(|| pyo3::PyErr::from(error::AlopexError::StorageLocationRequired))?;
        validate_storage_location(&storage_location)?;
        let normalized_format = table_info
            .data_source_format
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_uppercase();
        if normalized_format != "PARQUET" {
            return Err(error::AlopexError::UnsupportedFormat(normalized_format).into());
        }
        let credential_provider = credential_provider.bind(py);
        let resolved =
            resolve_credentials(py, credential_provider, storage_options, &storage_location)?;
        let polars = PyModule::import(py, "polars")?;
        let scan_parquet = polars.getattr("scan_parquet")?.unbind();
        let options = PyDict::new(py);
        for (key, value) in resolved {
            options.set_item(key, value)?;
        }
        let kwargs = if options.is_empty() {
            None
        } else {
            Some(options.unbind())
        };
        let storage_location = storage_location.clone();
        let lazy_frame = py.allow_threads(move || {
            Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                let scan_parquet = scan_parquet.bind(py);
                let args = (storage_location.as_str(),);
                let result = if let Some(kwargs_obj) = kwargs.as_ref() {
                    scan_parquet.call(args, Some(kwargs_obj.bind(py)))?
                } else {
                    scan_parquet.call1(args)?
                };
                Ok(result.unbind())
            })
        })?;
        Ok(lazy_frame)
    }

    #[staticmethod]
    #[pyo3(signature = (
        df,
        catalog_name,
        namespace,
        table_name,
        delta_mode = "error",
        storage_location = None,
        credential_provider = default_credential_provider(),
        storage_options = None,
        primary_key = None
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
        storage_options: Option<HashMap<String, String>>,
        primary_key: Option<Vec<String>>,
    ) -> PyResult<()> {
        require_polars(py)?;
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        validate_identifier(table_name)?;
        if let Some(location) = storage_location.as_ref() {
            validate_storage_location(location)?;
        }

        let df = normalize_to_dataframe(py, df.bind(py))?;
        let df_obj = df.unbind();
        let credential_provider = credential_provider.bind(py);

        let table_info = match py.allow_threads(|| {
            alopex_embedded::Catalog::get_table_info(catalog_name, namespace, table_name)
        }) {
            Ok(info) => Some(info),
            Err(alopex_embedded::Error::TableNotFound(_)) => None,
            Err(other) => return Err(error::embedded_err(other)),
        };

        if let Some(info) = table_info.as_ref() {
            let normalized_format = info
                .data_source_format
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_ascii_uppercase();
            if normalized_format != "PARQUET" {
                return Err(error::AlopexError::UnsupportedFormat(normalized_format).into());
            }
            let location = info
                .storage_location
                .as_ref()
                .ok_or(error::AlopexError::StorageLocationRequired)?;
            validate_storage_location(location)?;
        }

        match (table_info.as_ref(), delta_mode) {
            (Some(_), "error") => {
                Err(error::AlopexError::TableExists(table_name.to_string()).into())
            }
            (Some(_), "ignore") => Ok(()),
            (Some(info), "append") => {
                let storage_location = info
                    .storage_location
                    .clone()
                    .ok_or(error::AlopexError::StorageLocationRequired)?;
                let resolved = resolve_credentials(
                    py,
                    credential_provider,
                    storage_options.clone(),
                    &storage_location,
                )?;
                write_parquet_append(py, df_obj.clone_ref(py), storage_location, &resolved)
            }
            (Some(info), "overwrite") => {
                let storage_location = info
                    .storage_location
                    .clone()
                    .ok_or(error::AlopexError::StorageLocationRequired)?;
                let resolved = resolve_credentials(
                    py,
                    credential_provider,
                    storage_options.clone(),
                    &storage_location,
                )?;
                write_parquet_overwrite(py, df_obj.clone_ref(py), storage_location, &resolved)
            }
            (Some(info), "merge") => {
                let primary_key = primary_key
                    .clone()
                    .ok_or(error::AlopexError::PrimaryKeyRequired)?;
                let storage_location = info
                    .storage_location
                    .clone()
                    .ok_or(error::AlopexError::StorageLocationRequired)?;
                let resolved = resolve_credentials(
                    py,
                    credential_provider,
                    storage_options.clone(),
                    &storage_location,
                )?;
                write_table_merge(
                    py,
                    df_obj.clone_ref(py),
                    storage_location,
                    primary_key,
                    &resolved,
                )
            }
            (None, "error") | (None, "ignore") => {
                Err(error::AlopexError::WriteTargetNotFound(table_name.to_string()).into())
            }
            (None, "append" | "overwrite" | "merge") => {
                if delta_mode == "merge" && primary_key.is_none() {
                    return Err(error::AlopexError::PrimaryKeyRequired.into());
                }
                let storage_location = storage_location
                    .clone()
                    .ok_or(error::AlopexError::StorageLocationRequired)?;
                validate_storage_location(&storage_location)?;
                create_table_from_dataframe(
                    py,
                    catalog_name,
                    namespace,
                    table_name,
                    df_obj.bind(py),
                    storage_location.clone(),
                )?;
                let resolved = resolve_credentials(
                    py,
                    credential_provider,
                    storage_options.clone(),
                    &storage_location,
                )?;
                write_parquet_overwrite(py, df_obj.clone_ref(py), storage_location, &resolved)
            }
            (_, other) => Err(error::to_py_err(format!(
                "Unsupported delta_mode: {}",
                other
            ))),
        }
    }
}

#[allow(dead_code)]
pub fn require_polars(py: Python<'_>) -> PyResult<()> {
    if PyModule::import(py, "polars").is_ok() {
        Ok(())
    } else {
        Err(error::AlopexError::PolarsNotInstalled.into())
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCatalog>()?;
    Ok(())
}

fn create_table_from_dataframe(
    py: Python<'_>,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    df: &Bound<'_, PyAny>,
    storage_location: String,
) -> PyResult<()> {
    let columns = infer_columns_from_dataframe(df)?;
    let columns = to_embedded_columns(columns);
    let embedded_format = "parquet".to_string();
    py.allow_threads(move || {
        alopex_embedded::Catalog::create_table(
            catalog_name,
            namespace,
            table_name,
            columns,
            Some(storage_location),
            Some(embedded_format),
        )
    })
    .map_err(|err| match err {
        alopex_embedded::Error::CatalogNotFound(name) => {
            error::AlopexError::ParentNotFound(name).into()
        }
        alopex_embedded::Error::NamespaceNotFound(catalog, namespace) => {
            error::AlopexError::ParentNotFound(format!("{}.{}", catalog, namespace)).into()
        }
        other => error::embedded_err(other),
    })
}

fn write_parquet_append(
    py: Python<'_>,
    df: Py<PyAny>,
    storage_location: String,
    storage_options: &HashMap<String, String>,
) -> PyResult<()> {
    let kwargs = storage_options_to_kwargs(py, storage_options)?;
    py.allow_threads(move || {
        Python::with_gil(|py| -> PyResult<()> {
            let polars = PyModule::import(py, "polars")?;
            let scan_parquet = polars.getattr("scan_parquet")?;
            let args = (storage_location.as_str(),);
            let existing_lf = if let Some(kwargs) = kwargs.as_ref() {
                scan_parquet.call(args, Some(kwargs.bind(py)))?
            } else {
                scan_parquet.call1(args)?
            };
            let existing_df = existing_lf.call_method0("collect")?;
            let concat = polars.getattr("concat")?;
            let list = PyList::new(py, vec![existing_df.unbind(), df.clone_ref(py)])?;
            let combined = concat.call1((list,))?;
            if let Some(kwargs) = kwargs.as_ref() {
                combined.call_method("write_parquet", args, Some(kwargs.bind(py)))?;
            } else {
                combined.call_method1("write_parquet", args)?;
            }
            Ok(())
        })
    })
}

fn write_parquet_overwrite(
    py: Python<'_>,
    df: Py<PyAny>,
    storage_location: String,
    storage_options: &HashMap<String, String>,
) -> PyResult<()> {
    let kwargs = storage_options_to_kwargs(py, storage_options)?;
    py.allow_threads(move || {
        Python::with_gil(|py| -> PyResult<()> {
            let df = df.bind(py);
            let args = (storage_location.as_str(),);
            if let Some(kwargs) = kwargs.as_ref() {
                df.call_method("write_parquet", args, Some(kwargs.bind(py)))?;
            } else {
                df.call_method1("write_parquet", args)?;
            }
            Ok(())
        })
    })
}

fn write_table_merge(
    py: Python<'_>,
    df: Py<PyAny>,
    storage_location: String,
    primary_key: Vec<String>,
    storage_options: &HashMap<String, String>,
) -> PyResult<()> {
    let kwargs = storage_options_to_kwargs(py, storage_options)?;
    py.allow_threads(move || {
        Python::with_gil(|py| -> PyResult<()> {
            let polars = PyModule::import(py, "polars")?;
            let scan_parquet = polars.getattr("scan_parquet")?;
            let args = (storage_location.as_str(),);
            let existing_lf = if let Some(kwargs) = kwargs.as_ref() {
                scan_parquet.call(args, Some(kwargs.bind(py)))?
            } else {
                scan_parquet.call1(args)?
            };
            let existing_df = existing_lf.call_method0("collect")?;
            let new_df = df.bind(py);
            let pk_cols = primary_key
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>();
            let pk_cols = PyList::new(py, pk_cols)?.unbind();
            let join_kwargs = PyDict::new(py);
            join_kwargs.set_item("how", "anti")?;
            let existing_without_updates = existing_df.call_method(
                "join",
                (new_df, pk_cols.bind(py), pk_cols.bind(py)),
                Some(&join_kwargs),
            )?;
            let concat = polars.getattr("concat")?;
            let merged = concat.call1((PyList::new(
                py,
                vec![existing_without_updates.unbind(), df.clone_ref(py)],
            )?,))?;
            if let Some(kwargs) = kwargs.as_ref() {
                merged.call_method("write_parquet", args, Some(kwargs.bind(py)))?;
            } else {
                merged.call_method1("write_parquet", args)?;
            }
            Ok(())
        })
    })
}
