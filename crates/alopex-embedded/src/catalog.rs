//! Unity Catalog-like metadata store for embedded usage.

use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

use crate::{Error, Result};

/// Catalog metadata.
#[derive(Clone, Debug)]
pub struct CatalogInfo {
    /// Catalog name.
    pub name: String,
    /// Optional catalog comment.
    pub comment: Option<String>,
    /// Optional catalog storage root.
    pub storage_root: Option<String>,
}

/// Namespace metadata.
#[derive(Clone, Debug)]
pub struct NamespaceInfo {
    /// Namespace name.
    pub name: String,
    /// Owning catalog name.
    pub catalog_name: String,
    /// Optional namespace comment.
    pub comment: Option<String>,
    /// Optional namespace storage root.
    pub storage_root: Option<String>,
}

/// Column metadata.
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Column type name.
    pub type_name: String,
    /// Column position in the schema.
    pub position: usize,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Optional column comment.
    pub comment: Option<String>,
}

/// Table metadata.
#[derive(Clone, Debug)]
pub struct TableInfo {
    /// Table name.
    pub name: String,
    /// Owning catalog name.
    pub catalog_name: String,
    /// Owning namespace name.
    pub namespace_name: String,
    /// Optional storage location.
    pub storage_location: Option<String>,
    /// Optional data source format.
    pub data_source_format: Option<String>,
    /// Column definitions.
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Default)]
struct CatalogStore {
    catalogs: BTreeMap<String, CatalogEntry>,
}

#[derive(Debug)]
struct CatalogEntry {
    info: CatalogInfo,
    namespaces: BTreeMap<String, NamespaceEntry>,
}

#[derive(Debug)]
struct NamespaceEntry {
    info: NamespaceInfo,
    tables: BTreeMap<String, TableInfo>,
}

fn catalog_store() -> &'static RwLock<CatalogStore> {
    static STORE: OnceLock<RwLock<CatalogStore>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(CatalogStore::default()))
}

/// Catalog metadata access entrypoint.
pub struct Catalog;

impl Catalog {
    /// List all catalogs.
    pub fn list_catalogs() -> Result<Vec<CatalogInfo>> {
        let guard = catalog_store()
            .read()
            .map_err(|_| Error::CatalogLockPoisoned)?;
        Ok(guard
            .catalogs
            .values()
            .map(|entry| entry.info.clone())
            .collect())
    }

    /// List all namespaces within a catalog.
    pub fn list_namespaces(catalog_name: &str) -> Result<Vec<NamespaceInfo>> {
        let guard = catalog_store()
            .read()
            .map_err(|_| Error::CatalogLockPoisoned)?;
        let catalog = guard
            .catalogs
            .get(catalog_name)
            .ok_or_else(|| Error::CatalogNotFound(catalog_name.to_string()))?;
        Ok(catalog
            .namespaces
            .values()
            .map(|entry| entry.info.clone())
            .collect())
    }

    /// List all tables within a namespace.
    pub fn list_tables(catalog_name: &str, namespace_name: &str) -> Result<Vec<TableInfo>> {
        let guard = catalog_store()
            .read()
            .map_err(|_| Error::CatalogLockPoisoned)?;
        let catalog = guard
            .catalogs
            .get(catalog_name)
            .ok_or_else(|| Error::CatalogNotFound(catalog_name.to_string()))?;
        let namespace =
            catalog
                .namespaces
                .get(namespace_name)
                .ok_or_else(|| Error::NamespaceNotFound {
                    catalog: catalog_name.to_string(),
                    namespace: namespace_name.to_string(),
                })?;
        Ok(namespace.tables.values().cloned().collect::<Vec<_>>())
    }

    /// Get a table metadata entry.
    pub fn get_table_info(
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let guard = catalog_store()
            .read()
            .map_err(|_| Error::CatalogLockPoisoned)?;
        let catalog = guard
            .catalogs
            .get(catalog_name)
            .ok_or_else(|| Error::CatalogNotFound(catalog_name.to_string()))?;
        let namespace =
            catalog
                .namespaces
                .get(namespace_name)
                .ok_or_else(|| Error::NamespaceNotFound {
                    catalog: catalog_name.to_string(),
                    namespace: namespace_name.to_string(),
                })?;
        namespace.tables.get(table_name).cloned().ok_or_else(|| {
            Error::TableNotFound(format!(
                "{}.{}.{}",
                catalog_name, namespace_name, table_name
            ))
        })
    }
}
