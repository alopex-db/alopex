use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyModule};

use super::{
    validate_identifier, validate_storage_location, PyCatalogInfo, PyColumnInfo, PyNamespaceInfo,
    PyTableInfo,
};
use crate::catalog::resolve_credentials;
use crate::error;

thread_local! {
    static POLARS_SCAN_PARQUET: RefCell<Option<Py<PyAny>>> = const { RefCell::new(None) };
    static SCAN_TABLE_CACHE: RefCell<Option<ScanTableCacheEntry>> = const { RefCell::new(None) };
}

static SCAN_TABLE_CACHE_EPOCH: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct ScanTableCacheEntry {
    catalog: String,
    namespace: String,
    table: String,
    storage_location: String,
    format_upper: String,
    epoch: u64,
}

#[allow(deprecated)]
fn default_credential_provider() -> PyObject {
    Python::with_gil(|py| "auto".into_py(py))
}

fn get_scan_parquet(py: Python<'_>) -> PyResult<Py<PyAny>> {
    POLARS_SCAN_PARQUET.with(|cell| {
        if let Some(func) = cell.borrow().as_ref() {
            return Ok(func.clone_ref(py));
        }
        let polars = PyModule::import(py, "polars")
            .map_err(|_| PyErr::from(error::AlopexError::PolarsNotInstalled))?;
        let func: Py<PyAny> = polars.getattr("scan_parquet")?.into();
        *cell.borrow_mut() = Some(func.clone_ref(py));
        Ok(func)
    })
}

fn load_scan_table_cache(
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
) -> Option<ScanTableCacheEntry> {
    let epoch = SCAN_TABLE_CACHE_EPOCH.load(Ordering::Relaxed);
    SCAN_TABLE_CACHE.with(|cell| {
        cell.borrow().as_ref().and_then(|entry| {
            if entry.epoch == epoch
                && entry.catalog == catalog_name
                && entry.namespace == namespace
                && entry.table == table_name
            {
                Some(entry.clone())
            } else {
                None
            }
        })
    })
}

fn store_scan_table_cache(
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    storage_location: String,
    format_upper: String,
) {
    let epoch = SCAN_TABLE_CACHE_EPOCH.load(Ordering::Relaxed);
    SCAN_TABLE_CACHE.with(|cell| {
        *cell.borrow_mut() = Some(ScanTableCacheEntry {
            catalog: catalog_name.to_string(),
            namespace: namespace.to_string(),
            table: table_name.to_string(),
            storage_location,
            format_upper,
            epoch,
        });
    });
}

fn clear_scan_table_cache() {
    SCAN_TABLE_CACHE_EPOCH.fetch_add(1, Ordering::Relaxed);
    SCAN_TABLE_CACHE.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

fn credential_provider_is_auto(credential_provider: &Bound<'_, PyAny>) -> PyResult<bool> {
    if credential_provider.is_none() {
        return Ok(true);
    }
    if let Ok(provider) = credential_provider.extract::<String>() {
        return Ok(provider == "auto");
    }
    Ok(false)
}

fn is_local_storage_location(location: &str) -> bool {
    let Some(pos) = location.find("://") else {
        return true;
    };
    let scheme = &location[..pos];
    !(scheme.eq_ignore_ascii_case("s3")
        || scheme.eq_ignore_ascii_case("gs")
        || scheme.eq_ignore_ascii_case("az")
        || scheme.eq_ignore_ascii_case("abfs"))
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

/// Catalog API entry point for Unity Catalog-style metadata and DDL operations.
///
/// Examples:
///     >>> from alopex import Catalog
///     >>> Catalog.list_catalogs()
///
/// Raises:
///     AlopexError: Raised by individual methods when an operation fails.
///     ValueError: Raised by methods when identifiers or storage locations are invalid.
///     RuntimeError: Raised by methods when a resource already exists.
#[pyclass(name = "Catalog")]
pub struct PyCatalog;

#[pymethods]
impl PyCatalog {
    /// List catalogs in the metadata store.
    ///
    /// Returns:
    ///     list[CatalogInfo]: Catalog metadata entries.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> catalogs = Catalog.list_catalogs()
    ///     >>> [c.name for c in catalogs]
    ///
    /// Raises:
    ///     AlopexError: If the underlying catalog store returns an error.
    #[staticmethod]
    fn list_catalogs(py: Python<'_>) -> PyResult<Vec<PyCatalogInfo>> {
        let catalogs = py
            .allow_threads(alopex_embedded::Catalog::list_catalogs)
            .map_err(error::embedded_err)?;
        Ok(catalogs.into_iter().map(PyCatalogInfo::from).collect())
    }

    /// List namespaces within a catalog.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///
    /// Returns:
    ///     list[NamespaceInfo]: Namespaces registered in the catalog.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> Catalog.list_namespaces("main")
    ///
    /// Raises:
    ///     ValueError: If catalog_name is invalid or the catalog does not exist.
    ///     AlopexError: For other catalog store errors.
    #[staticmethod]
    fn list_namespaces(py: Python<'_>, catalog_name: &str) -> PyResult<Vec<PyNamespaceInfo>> {
        validate_identifier(catalog_name)?;
        let namespaces = py
            .allow_threads(|| alopex_embedded::Catalog::list_namespaces(catalog_name))
            .map_err(error::embedded_err)?;
        Ok(namespaces.into_iter().map(PyNamespaceInfo::from).collect())
    }

    /// List tables in a catalog namespace.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///
    /// Returns:
    ///     list[TableInfo]: Tables registered in the namespace.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> Catalog.list_tables("main", "default")
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid or the catalog/namespace does not exist.
    ///     AlopexError: For other catalog store errors.
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

    /// Fetch detailed metadata for a table.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///     table_name (str): Table name.
    ///
    /// Returns:
    ///     TableInfo: Metadata for the requested table.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> info = Catalog.get_table_info("main", "default", "users")
    ///     >>> info.storage_location
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid or the table does not exist.
    ///     AlopexError: For other catalog store errors.
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

    /// Create a new catalog.
    ///
    /// Args:
    ///     name (str): Catalog name.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> Catalog.create_catalog("main")
    ///
    /// Raises:
    ///     ValueError: If the name is invalid.
    ///     RuntimeError: If the catalog already exists.
    ///     AlopexError: For other catalog store errors.
    #[staticmethod]
    fn create_catalog(py: Python<'_>, name: &str) -> PyResult<()> {
        validate_identifier(name)?;
        clear_scan_table_cache();
        py.allow_threads(|| alopex_embedded::Catalog::create_catalog(name))
            .map_err(error::embedded_err)
    }

    /// Delete an existing catalog.
    ///
    /// Args:
    ///     name (str): Catalog name.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> Catalog.delete_catalog("main")
    ///
    /// Raises:
    ///     ValueError: If the name is invalid or the catalog does not exist.
    ///     AlopexError: For other catalog store errors.
    #[staticmethod]
    fn delete_catalog(py: Python<'_>, name: &str) -> PyResult<()> {
        validate_identifier(name)?;
        clear_scan_table_cache();
        py.allow_threads(|| alopex_embedded::Catalog::delete_catalog(name))
            .map_err(error::embedded_err)
    }

    /// Create a namespace within a catalog.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> Catalog.create_namespace("main", "default")
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid or the parent catalog does not exist.
    ///     RuntimeError: If the namespace already exists.
    ///     AlopexError: For other catalog store errors.
    #[staticmethod]
    fn create_namespace(py: Python<'_>, catalog_name: &str, namespace: &str) -> PyResult<()> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        clear_scan_table_cache();
        py.allow_threads(|| alopex_embedded::Catalog::create_namespace(catalog_name, namespace))
            .map_err(|err| match err {
                alopex_embedded::Error::CatalogNotFound(name) => {
                    error::AlopexError::ParentNotFound(name).into()
                }
                other => error::embedded_err(other),
            })
    }

    /// Delete a namespace from a catalog.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> Catalog.delete_namespace("main", "default")
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid or the namespace does not exist.
    ///     AlopexError: For other catalog store errors.
    #[staticmethod]
    fn delete_namespace(py: Python<'_>, catalog_name: &str, namespace: &str) -> PyResult<()> {
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        clear_scan_table_cache();
        py.allow_threads(|| alopex_embedded::Catalog::delete_namespace(catalog_name, namespace))
            .map_err(error::embedded_err)
    }

    /// Create a table in a catalog namespace.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///     table_name (str): Table name.
    ///     columns (list[ColumnInfo]): Column definitions.
    ///     storage_location (str): Table storage location (file://, s3://, gs://, az://, abfs://).
    ///     data_source_format (str): Data source format (default: "PARQUET").
    ///
    /// Examples:
    ///     >>> from alopex import Catalog, ColumnInfo
    ///     >>> Catalog.create_table(
    ///     ...     "main",
    ///     ...     "default",
    ///     ...     "users",
    ///     ...     [ColumnInfo("id", "INTEGER", 0, False)],
    ///     ...     "/tmp/users.parquet",
    ///     ... )
    ///
    /// Raises:
    ///     ValueError: If identifiers or storage_location are invalid, or parent resources are missing.
    ///     AlopexError: If the format is unsupported or the table already exists.
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
        clear_scan_table_cache();

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

    /// Delete a table from a catalog namespace.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///     table_name (str): Table name.
    ///
    /// Examples:
    ///     >>> from alopex import Catalog
    ///     >>> Catalog.delete_table("main", "default", "users")
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid or the table does not exist.
    ///     AlopexError: For other catalog store errors.
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
        clear_scan_table_cache();
        py.allow_threads(|| {
            alopex_embedded::Catalog::delete_table(catalog_name, namespace, table_name)
        })
        .map_err(error::embedded_err)
    }

    /// Lazily scan a table as a Polars LazyFrame.
    ///
    /// Args:
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///     table_name (str): Table name.
    ///     credential_provider (str | dict | None): "auto" (default) or storage options dict.
    ///     storage_options (dict | None): Additional storage options to merge.
    ///
    /// Returns:
    ///     polars.LazyFrame: Lazy scan of the table data.
    ///
    /// Examples:
    ///     >>> import polars as pl
    ///     >>> from alopex import Catalog
    ///     >>> lf = Catalog.scan_table("main", "default", "users")
    ///     >>> isinstance(lf, pl.LazyFrame)
    ///
    /// Raises:
    ///     ValueError: If identifiers are invalid or the table does not exist.
    ///     AlopexError: If polars is unavailable, the format is unsupported, or storage is missing.
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
        validate_identifier(catalog_name)?;
        validate_identifier(namespace)?;
        validate_identifier(table_name)?;
        let (storage_location, format_upper) =
            if let Some(entry) = load_scan_table_cache(catalog_name, namespace, table_name) {
                (entry.storage_location, entry.format_upper)
            } else {
                let table_info =
                    alopex_embedded::Catalog::get_table_info(catalog_name, namespace, table_name)
                        .map_err(error::embedded_err)?;
                let storage_location = table_info.storage_location.ok_or_else(|| {
                    pyo3::PyErr::from(error::AlopexError::StorageLocationRequired)
                })?;
                validate_storage_location(&storage_location)?;
                let format_upper = table_info
                    .data_source_format
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_uppercase();
                store_scan_table_cache(
                    catalog_name,
                    namespace,
                    table_name,
                    storage_location.clone(),
                    format_upper.clone(),
                );
                (storage_location, format_upper)
            };
        if format_upper != "PARQUET" {
            return Err(error::AlopexError::UnsupportedFormat(format_upper).into());
        }
        let credential_provider = credential_provider.bind(py);
        let use_fast_path = storage_options.is_none()
            && credential_provider_is_auto(credential_provider)?
            && is_local_storage_location(&storage_location);
        let scan_parquet = get_scan_parquet(py)?;
        let scan_parquet = scan_parquet.bind(py);
        let args = (storage_location.as_str(),);
        let result = if use_fast_path {
            scan_parquet.call1(args)?
        } else {
            let resolved =
                resolve_credentials(py, credential_provider, storage_options, &storage_location)?;
            let kwargs = storage_options_to_kwargs(py, &resolved)?;
            if let Some(kwargs_obj) = kwargs.as_ref() {
                scan_parquet.call(args, Some(kwargs_obj.bind(py)))?
            } else {
                scan_parquet.call1(args)?
            }
        };
        Ok(result.unbind())
    }

    /// Write a DataFrame into a catalog table.
    ///
    /// Args:
    ///     df (polars.DataFrame | polars.LazyFrame): Data to write.
    ///     catalog_name (str): Catalog name.
    ///     namespace (str): Namespace name.
    ///     table_name (str): Table name.
    ///     delta_mode (str): "error", "ignore", "append", "overwrite", or "merge".
    ///     storage_location (str | None): Required when creating a new table.
    ///     credential_provider (str | dict | None): "auto" (default) or storage options dict.
    ///     storage_options (dict | None): Additional storage options to merge.
    ///     primary_key (list[str] | None): Required when delta_mode="merge".
    ///
    /// Examples:
    ///     >>> import polars as pl
    ///     >>> from alopex import Catalog
    ///     >>> df = pl.DataFrame({"id": [1]})
    ///     >>> Catalog.write_table(
    ///     ...     df,
    ///     ...     "main",
    ///     ...     "default",
    ///     ...     "users",
    ///     ...     delta_mode="append",
    ///     ...     storage_location="/tmp/users.parquet",
    ///     ... )
    ///
    /// Raises:
    ///     ValueError: If identifiers or storage_location are invalid.
    ///     AlopexError: If polars is unavailable, storage is missing, or merge keys are required.
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

        // Fast path: use cache for overwrite/append on existing table (avoids DB lookup)
        if matches!(delta_mode, "overwrite" | "append") {
            if let Some(cached) = load_scan_table_cache(catalog_name, namespace, table_name) {
                if cached.format_upper != "PARQUET" {
                    return Err(error::AlopexError::UnsupportedFormat(cached.format_upper).into());
                }
                let resolved = resolve_credentials(
                    py,
                    credential_provider,
                    storage_options.clone(),
                    &cached.storage_location,
                )?;
                return if delta_mode == "overwrite" {
                    write_parquet_overwrite(
                        py,
                        df_obj.clone_ref(py),
                        cached.storage_location,
                        &resolved,
                    )
                } else {
                    write_parquet_append(
                        py,
                        df_obj.clone_ref(py),
                        cached.storage_location,
                        &resolved,
                    )
                };
            }
        }

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
            // Store in cache for subsequent operations
            store_scan_table_cache(
                catalog_name,
                namespace,
                table_name,
                location.clone(),
                normalized_format,
            );
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
                // Cache newly created table for subsequent operations
                store_scan_table_cache(
                    catalog_name,
                    namespace,
                    table_name,
                    storage_location.clone(),
                    "PARQUET".to_string(),
                );
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

/// Ensure that Polars is available for catalog operations.
///
/// Args:
///     py (Python): GIL token.
///
/// Returns:
///     None
///
/// Examples:
///     >>> from alopex import Catalog
///     >>> # Catalog.scan_table() will fail if polars is missing.
///
/// Raises:
///     AlopexError: If polars is not installed.
#[allow(dead_code)]
pub fn require_polars(py: Python<'_>) -> PyResult<()> {
    if PyModule::import(py, "polars").is_ok() {
        Ok(())
    } else {
        Err(error::AlopexError::PolarsNotInstalled.into())
    }
}

/// Register catalog types in a Python module.
///
/// Args:
///     m (module): Python module to receive Catalog bindings.
///
/// Returns:
///     None
///
/// Examples:
///     >>> # Internal use from module initialization.
///
/// Raises:
///     AlopexError: If registration fails.
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
    clear_scan_table_cache();
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
            join_kwargs.set_item("on", pk_cols.bind(py))?;
            join_kwargs.set_item("how", "anti")?;
            let existing_without_updates =
                existing_df.call_method("join", (new_df,), Some(&join_kwargs))?;
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
