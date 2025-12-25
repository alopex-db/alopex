use pyo3::prelude::*;

#[pyclass(name = "CatalogInfo")]
#[derive(Clone, Debug)]
pub struct PyCatalogInfo {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub comment: Option<String>,
    #[pyo3(get, set)]
    pub storage_root: Option<String>,
}

#[pymethods]
impl PyCatalogInfo {
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

impl From<alopex_embedded::CatalogInfo> for PyCatalogInfo {
    fn from(info: alopex_embedded::CatalogInfo) -> Self {
        Self {
            name: info.name,
            comment: info.comment,
            storage_root: info.storage_root,
        }
    }
}

#[pyclass(name = "NamespaceInfo")]
#[derive(Clone, Debug)]
pub struct PyNamespaceInfo {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub catalog_name: String,
    #[pyo3(get, set)]
    pub comment: Option<String>,
    #[pyo3(get, set)]
    pub storage_root: Option<String>,
}

#[pymethods]
impl PyNamespaceInfo {
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

impl From<alopex_embedded::NamespaceInfo> for PyNamespaceInfo {
    fn from(info: alopex_embedded::NamespaceInfo) -> Self {
        Self {
            name: info.name,
            catalog_name: info.catalog_name,
            comment: info.comment,
            storage_root: info.storage_root,
        }
    }
}

#[pyclass(name = "ColumnInfo")]
#[derive(Clone, Debug)]
pub struct PyColumnInfo {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub type_name: String,
    #[pyo3(get, set)]
    pub position: usize,
    #[pyo3(get, set)]
    pub nullable: bool,
    #[pyo3(get, set)]
    pub comment: Option<String>,
}

#[pymethods]
impl PyColumnInfo {
    #[new]
    #[pyo3(signature = (name, type_name, position, nullable, comment = None))]
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

impl From<alopex_embedded::ColumnInfo> for PyColumnInfo {
    fn from(info: alopex_embedded::ColumnInfo) -> Self {
        Self {
            name: info.name,
            type_name: info.type_name,
            position: info.position,
            nullable: info.nullable,
            comment: info.comment,
        }
    }
}

#[pyclass(name = "TableInfo")]
#[derive(Clone, Debug)]
pub struct PyTableInfo {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub catalog_name: String,
    #[pyo3(get, set)]
    pub namespace_name: String,
    #[pyo3(get, set)]
    pub storage_location: Option<String>,
    #[pyo3(get, set)]
    pub data_source_format: Option<String>,
    #[pyo3(get, set)]
    pub columns: Vec<PyColumnInfo>,
}

#[pymethods]
impl PyTableInfo {
    #[new]
    #[pyo3(signature = (
        name,
        catalog_name,
        namespace_name,
        storage_location = None,
        data_source_format = None,
        columns = Vec::new()
    ))]
    fn new(
        name: String,
        catalog_name: String,
        namespace_name: String,
        storage_location: Option<String>,
        data_source_format: Option<String>,
        columns: Vec<PyColumnInfo>,
    ) -> Self {
        Self {
            name,
            catalog_name,
            namespace_name,
            storage_location,
            data_source_format,
            columns,
        }
    }
}

impl From<alopex_embedded::TableInfo> for PyTableInfo {
    fn from(info: alopex_embedded::TableInfo) -> Self {
        Self {
            name: info.name,
            catalog_name: info.catalog_name,
            namespace_name: info.namespace_name,
            storage_location: info.storage_location,
            data_source_format: info.data_source_format,
            columns: info.columns.into_iter().map(PyColumnInfo::from).collect(),
        }
    }
}
