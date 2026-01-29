use pyo3::prelude::*;

/// Catalog metadata returned by Catalog APIs.
///
/// Attributes:
///     name (str): Catalog name.
///     comment (str | None): Optional catalog comment.
///     storage_root (str | None): Optional storage root URI.
///
/// Examples:
///     >>> from alopex import CatalogInfo
///     >>> CatalogInfo("main")
#[pyclass(name = "CatalogInfo")]
#[derive(Clone, Debug)]
pub struct PyCatalogInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub comment: Option<String>,
    #[pyo3(get)]
    pub storage_root: Option<String>,
}

#[pymethods]
impl PyCatalogInfo {
    /// Create a CatalogInfo instance.
    ///
    /// Args:
    ///     name (str): Catalog name.
    ///     comment (str | None): Optional catalog comment.
    ///     storage_root (str | None): Optional storage root URI.
    ///
    /// Returns:
    ///     CatalogInfo: New instance.
    ///
    /// Examples:
    ///     >>> from alopex import CatalogInfo
    ///     >>> CatalogInfo("main", comment="primary")
    ///
    /// Raises:
    ///     None
    #[new]
    #[pyo3(signature = (name, comment = None, storage_root = None))]
    fn new(name: String, comment: Option<String>, storage_root: Option<String>) -> Self {
        Self {
            name,
            comment,
            storage_root,
        }
    }
}

impl From<alopex_embedded::catalog::CatalogInfo> for PyCatalogInfo {
    fn from(info: alopex_embedded::catalog::CatalogInfo) -> Self {
        Self {
            name: info.name,
            comment: info.comment,
            storage_root: info.storage_root,
        }
    }
}

/// Namespace metadata returned by Catalog APIs.
///
/// Attributes:
///     name (str): Namespace name.
///     catalog_name (str): Parent catalog name.
///     comment (str | None): Optional namespace comment.
///     storage_root (str | None): Optional storage root URI.
///
/// Examples:
///     >>> from alopex import NamespaceInfo
///     >>> NamespaceInfo("default", "main")
#[pyclass(name = "NamespaceInfo")]
#[derive(Clone, Debug)]
pub struct PyNamespaceInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub catalog_name: String,
    #[pyo3(get)]
    pub comment: Option<String>,
    #[pyo3(get)]
    pub storage_root: Option<String>,
}

#[pymethods]
impl PyNamespaceInfo {
    /// Create a NamespaceInfo instance.
    ///
    /// Args:
    ///     name (str): Namespace name.
    ///     catalog_name (str): Parent catalog name.
    ///     comment (str | None): Optional namespace comment.
    ///     storage_root (str | None): Optional storage root URI.
    ///
    /// Returns:
    ///     NamespaceInfo: New instance.
    ///
    /// Examples:
    ///     >>> from alopex import NamespaceInfo
    ///     >>> NamespaceInfo("default", "main")
    ///
    /// Raises:
    ///     None
    #[new]
    #[pyo3(signature = (name, catalog_name, comment = None, storage_root = None))]
    fn new(
        name: String,
        catalog_name: String,
        comment: Option<String>,
        storage_root: Option<String>,
    ) -> Self {
        Self {
            name,
            catalog_name,
            comment,
            storage_root,
        }
    }
}

impl From<alopex_embedded::catalog::NamespaceInfo> for PyNamespaceInfo {
    fn from(info: alopex_embedded::catalog::NamespaceInfo) -> Self {
        Self {
            name: info.name,
            catalog_name: info.catalog_name,
            comment: info.comment,
            storage_root: info.storage_root,
        }
    }
}

/// Column metadata used in table definitions.
///
/// Attributes:
///     name (str): Column name.
///     type_name (str): Data type name (e.g., "INTEGER", "TEXT").
///     position (int): Column position (zero-based).
///     nullable (bool): Whether the column allows nulls.
///     comment (str | None): Optional column comment.
///
/// Examples:
///     >>> from alopex import ColumnInfo
///     >>> ColumnInfo("id", "INTEGER", 0, False)
#[pyclass(name = "ColumnInfo")]
#[derive(Clone, Debug)]
pub struct PyColumnInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub type_name: String,
    #[pyo3(get)]
    pub position: usize,
    #[pyo3(get)]
    pub nullable: bool,
    #[pyo3(get)]
    pub comment: Option<String>,
}

#[pymethods]
impl PyColumnInfo {
    /// Create a ColumnInfo instance.
    ///
    /// Args:
    ///     name (str): Column name.
    ///     type_name (str): Data type name.
    ///     position (int): Column position (default: 0).
    ///     nullable (bool): Whether the column allows nulls (default: True).
    ///     comment (str | None): Optional column comment.
    ///
    /// Returns:
    ///     ColumnInfo: New instance.
    ///
    /// Examples:
    ///     >>> from alopex import ColumnInfo
    ///     >>> ColumnInfo("id", "INTEGER", 0, False)
    ///
    /// Raises:
    ///     None
    #[new]
    #[pyo3(signature = (name, type_name, position = 0, nullable = true, comment = None))]
    fn new(
        name: String,
        type_name: String,
        position: usize,
        nullable: bool,
        comment: Option<String>,
    ) -> Self {
        Self {
            name,
            type_name,
            position,
            nullable,
            comment,
        }
    }
}

impl From<alopex_embedded::catalog::ColumnInfo> for PyColumnInfo {
    fn from(info: alopex_embedded::catalog::ColumnInfo) -> Self {
        Self {
            name: info.name,
            type_name: info.type_name,
            position: info.position,
            nullable: info.nullable,
            comment: info.comment,
        }
    }
}

/// Table metadata returned by Catalog APIs.
///
/// Attributes:
///     name (str): Table name.
///     catalog_name (str): Parent catalog name.
///     namespace_name (str): Parent namespace name.
///     table_type (str): Table type (default: "MANAGED").
///     storage_location (str | None): Storage location URI/path.
///     data_source_format (str | None): Data source format (e.g., "PARQUET").
///     columns (list[ColumnInfo]): Column definitions.
///     primary_key (list[str] | None): Optional primary key column names.
///     comment (str | None): Optional table comment.
///
/// Examples:
///     >>> from alopex import TableInfo
///     >>> TableInfo("users", "main", "default")
#[pyclass(name = "TableInfo")]
#[derive(Clone, Debug)]
pub struct PyTableInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub catalog_name: String,
    #[pyo3(get)]
    pub namespace_name: String,
    #[pyo3(get)]
    pub table_type: String,
    #[pyo3(get)]
    pub storage_location: Option<String>,
    #[pyo3(get)]
    pub data_source_format: Option<String>,
    #[pyo3(get)]
    pub columns: Vec<PyColumnInfo>,
    #[pyo3(get)]
    pub primary_key: Option<Vec<String>>,
    #[pyo3(get)]
    pub comment: Option<String>,
}

#[pymethods]
impl PyTableInfo {
    /// Create a TableInfo instance.
    ///
    /// Args:
    ///     name (str): Table name.
    ///     catalog_name (str): Parent catalog name.
    ///     namespace_name (str): Parent namespace name.
    ///     table_type (str): Table type (default: "MANAGED").
    ///     storage_location (str | None): Storage location URI/path.
    ///     data_source_format (str | None): Data source format (e.g., "PARQUET").
    ///     columns (list[ColumnInfo]): Column definitions.
    ///     primary_key (list[str] | None): Optional primary key column names.
    ///     comment (str | None): Optional table comment.
    ///
    /// Returns:
    ///     TableInfo: New instance.
    ///
    /// Examples:
    ///     >>> from alopex import TableInfo
    ///     >>> TableInfo("users", "main", "default")
    ///
    /// Raises:
    ///     None
    #[new]
    #[pyo3(signature = (
        name,
        catalog_name,
        namespace_name,
        table_type = "MANAGED".to_string(),
        storage_location = None,
        data_source_format = None,
        columns = Vec::new(),
        primary_key = None,
        comment = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        name: String,
        catalog_name: String,
        namespace_name: String,
        table_type: String,
        storage_location: Option<String>,
        data_source_format: Option<String>,
        columns: Vec<PyColumnInfo>,
        primary_key: Option<Vec<String>>,
        comment: Option<String>,
    ) -> Self {
        Self {
            name,
            catalog_name,
            namespace_name,
            table_type,
            storage_location,
            data_source_format,
            columns,
            primary_key,
            comment,
        }
    }
}

impl From<alopex_embedded::catalog::TableInfo> for PyTableInfo {
    fn from(info: alopex_embedded::catalog::TableInfo) -> Self {
        Self {
            name: info.name,
            catalog_name: info.catalog_name,
            namespace_name: info.namespace_name,
            table_type: "MANAGED".to_string(),
            storage_location: info.storage_location,
            data_source_format: info.data_source_format.map(|fmt| fmt.to_uppercase()),
            columns: info.columns.into_iter().map(PyColumnInfo::from).collect(),
            primary_key: None,
            comment: None,
        }
    }
}
