//! Catalog API 向けの公開型定義。

use std::collections::HashMap;

use alopex_core::{KVStore, KVTransaction};
use alopex_sql::ast::ddl::{DataType, IndexMethod, VectorMetric};
use alopex_sql::catalog::persistent::{CatalogMeta, NamespaceMeta, TableFqn};
use alopex_sql::catalog::{
    Catalog, CatalogOverlay, ColumnMetadata, Compression, IndexMetadata, StorageOptions,
    StorageType, TableMetadata,
};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{DataSourceFormat, TableType};

use crate::{Database, Error, Result, Transaction, TxnMode};

/// Catalog 情報（公開 API 返却用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogInfo {
    /// Catalog 名。
    pub name: String,
    /// コメント。
    pub comment: Option<String>,
    /// ストレージルート。
    pub storage_root: Option<String>,
}

impl From<CatalogMeta> for CatalogInfo {
    fn from(value: CatalogMeta) -> Self {
        Self {
            name: value.name,
            comment: value.comment,
            storage_root: value.storage_root,
        }
    }
}

/// Namespace 情報（公開 API 返却用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInfo {
    /// Namespace 名。
    pub name: String,
    /// 所属 Catalog 名。
    pub catalog_name: String,
    /// コメント。
    pub comment: Option<String>,
    /// ストレージルート。
    pub storage_root: Option<String>,
}

impl From<NamespaceMeta> for NamespaceInfo {
    fn from(value: NamespaceMeta) -> Self {
        Self {
            name: value.name,
            catalog_name: value.catalog_name,
            comment: value.comment,
            storage_root: value.storage_root,
        }
    }
}

/// カラム情報（公開 API 返却用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// カラム名。
    pub name: String,
    /// データ型（例: "INTEGER", "TEXT", "VECTOR(128, COSINE)"）。
    pub data_type: String,
    /// NULL 許可。
    pub nullable: bool,
    /// 主キーの一部かどうか。
    pub is_primary_key: bool,
    /// コメント。
    pub comment: Option<String>,
}

/// ストレージ設定情報（TableInfo 返却用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageInfo {
    /// ストレージ種別（"row" | "columnar"）。
    pub storage_type: String,
    /// 圧縮方式（"none" | "lz4" | "zstd"）。
    pub compression: String,
}

impl Default for StorageInfo {
    fn default() -> Self {
        Self {
            storage_type: "row".to_string(),
            compression: "none".to_string(),
        }
    }
}

impl From<&StorageOptions> for StorageInfo {
    fn from(value: &StorageOptions) -> Self {
        let storage_type = match value.storage_type {
            StorageType::Row => "row",
            StorageType::Columnar => "columnar",
        };
        let compression = match value.compression {
            Compression::None => "none",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        };
        Self {
            storage_type: storage_type.to_string(),
            compression: compression.to_string(),
        }
    }
}

/// テーブル情報（公開 API 返却用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// テーブル名。
    pub name: String,
    /// 所属 Catalog 名。
    pub catalog_name: String,
    /// 所属 Namespace 名。
    pub namespace_name: String,
    /// テーブル ID。
    pub table_id: u32,
    /// テーブル種別。
    pub table_type: TableType,
    /// カラム情報。
    pub columns: Vec<ColumnInfo>,
    /// 主キー。
    pub primary_key: Option<Vec<String>>,
    /// ストレージロケーション。
    pub storage_location: Option<String>,
    /// データソース形式。
    pub data_source_format: DataSourceFormat,
    /// ストレージ設定。
    pub storage_options: StorageInfo,
    /// コメント。
    pub comment: Option<String>,
    /// カスタムプロパティ。
    pub properties: HashMap<String, String>,
}

impl From<&TableMetadata> for TableInfo {
    fn from(value: &TableMetadata) -> Self {
        let primary_key = value.primary_key.clone();
        let columns = value
            .columns
            .iter()
            .map(|column| ColumnInfo {
                name: column.name.clone(),
                data_type: resolved_type_to_string(&column.data_type),
                nullable: !column.not_null,
                is_primary_key: column.primary_key
                    || primary_key
                        .as_ref()
                        .map(|keys| keys.iter().any(|name| name == &column.name))
                        .unwrap_or(false),
                comment: None,
            })
            .collect();
        let storage_options = if value.storage_options == StorageOptions::default() {
            StorageInfo::default()
        } else {
            StorageInfo::from(&value.storage_options)
        };

        Self {
            name: value.name.clone(),
            catalog_name: value.catalog_name.clone(),
            namespace_name: value.namespace_name.clone(),
            table_id: value.table_id,
            table_type: value.table_type,
            columns,
            primary_key,
            storage_location: value.storage_location.clone(),
            data_source_format: value.data_source_format,
            storage_options,
            comment: value.comment.clone(),
            properties: value.properties.clone(),
        }
    }
}

impl From<TableMetadata> for TableInfo {
    fn from(value: TableMetadata) -> Self {
        Self::from(&value)
    }
}

/// インデックス情報（公開 API 返却用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    /// インデックス名。
    pub name: String,
    /// インデックス ID。
    pub index_id: u32,
    /// 所属 Catalog 名。
    pub catalog_name: String,
    /// 所属 Namespace 名。
    pub namespace_name: String,
    /// 対象テーブル名。
    pub table_name: String,
    /// 対象カラム名。
    pub columns: Vec<String>,
    /// インデックス方式（"btree" | "hnsw"）。
    pub method: String,
    /// ユニーク制約。
    pub is_unique: bool,
}

impl From<&IndexMetadata> for IndexInfo {
    fn from(value: &IndexMetadata) -> Self {
        let method = match value.method {
            Some(IndexMethod::BTree) | None => "btree",
            Some(IndexMethod::Hnsw) => "hnsw",
        };
        Self {
            name: value.name.clone(),
            index_id: value.index_id,
            catalog_name: value.catalog_name.clone(),
            namespace_name: value.namespace_name.clone(),
            table_name: value.table.clone(),
            columns: value.columns.clone(),
            method: method.to_string(),
            is_unique: value.unique,
        }
    }
}

impl From<IndexMetadata> for IndexInfo {
    fn from(value: IndexMetadata) -> Self {
        Self::from(&value)
    }
}

/// Catalog 作成リクエスト。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCatalogRequest {
    /// Catalog 名。
    pub name: String,
    /// コメント。
    pub comment: Option<String>,
    /// ストレージルート。
    pub storage_root: Option<String>,
}

impl CreateCatalogRequest {
    /// 必須フィールドを指定して作成する。
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            comment: None,
            storage_root: None,
        }
    }

    /// コメントを指定する。
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// ストレージルートを指定する。
    pub fn with_storage_root(mut self, storage_root: impl Into<String>) -> Self {
        self.storage_root = Some(storage_root.into());
        self
    }

    /// 必須フィールドを検証して返す。
    pub fn build(self) -> Result<Self> {
        validate_required(&self.name, "catalog 名")?;
        Ok(self)
    }
}

/// Namespace 作成リクエスト。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNamespaceRequest {
    /// 所属 Catalog 名。
    pub catalog_name: String,
    /// Namespace 名。
    pub name: String,
    /// コメント。
    pub comment: Option<String>,
    /// ストレージルート。
    pub storage_root: Option<String>,
}

impl CreateNamespaceRequest {
    /// 必須フィールドを指定して作成する。
    pub fn new(catalog_name: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            name: name.into(),
            comment: None,
            storage_root: None,
        }
    }

    /// コメントを指定する。
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// ストレージルートを指定する。
    pub fn with_storage_root(mut self, storage_root: impl Into<String>) -> Self {
        self.storage_root = Some(storage_root.into());
        self
    }

    /// 必須フィールドを検証して返す。
    pub fn build(self) -> Result<Self> {
        validate_required(&self.catalog_name, "catalog 名")?;
        validate_required(&self.name, "namespace 名")?;
        Ok(self)
    }
}

/// テーブル作成リクエスト。
#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    /// Catalog 名（既定: "default"）。
    pub catalog_name: String,
    /// Namespace 名（既定: "default"）。
    pub namespace_name: String,
    /// テーブル名。
    pub name: String,
    /// スキーマ。
    pub schema: Option<Vec<ColumnDefinition>>,
    /// テーブル種別（既定: Managed）。
    pub table_type: TableType,
    /// データソース形式（None の場合は Alopex）。
    pub data_source_format: Option<DataSourceFormat>,
    /// 主キー。
    pub primary_key: Option<Vec<String>>,
    /// ストレージルート。
    pub storage_root: Option<String>,
    /// ストレージオプション。
    pub storage_options: Option<StorageOptions>,
    /// コメント。
    pub comment: Option<String>,
    /// カスタムプロパティ（None の場合は空の HashMap）。
    pub properties: Option<HashMap<String, String>>,
}

impl CreateTableRequest {
    /// 必須フィールドを指定して作成する。
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            catalog_name: "default".to_string(),
            namespace_name: "default".to_string(),
            name: name.into(),
            schema: None,
            table_type: TableType::Managed,
            data_source_format: None,
            primary_key: None,
            storage_root: None,
            storage_options: None,
            comment: None,
            properties: None,
        }
    }

    /// Catalog 名を指定する。
    pub fn with_catalog_name(mut self, catalog_name: impl Into<String>) -> Self {
        self.catalog_name = catalog_name.into();
        self
    }

    /// Namespace 名を指定する。
    pub fn with_namespace_name(mut self, namespace_name: impl Into<String>) -> Self {
        self.namespace_name = namespace_name.into();
        self
    }

    /// スキーマを指定する。
    pub fn with_schema(mut self, schema: Vec<ColumnDefinition>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// テーブル種別を指定する。
    pub fn with_table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    /// データソース形式を指定する。
    pub fn with_data_source_format(mut self, data_source_format: DataSourceFormat) -> Self {
        self.data_source_format = Some(data_source_format);
        self
    }

    /// 主キーを指定する。
    pub fn with_primary_key(mut self, primary_key: Vec<String>) -> Self {
        self.primary_key = Some(primary_key);
        self
    }

    /// ストレージルートを指定する。
    pub fn with_storage_root(mut self, storage_root: impl Into<String>) -> Self {
        self.storage_root = Some(storage_root.into());
        self
    }

    /// ストレージオプションを指定する。
    pub fn with_storage_options(mut self, storage_options: StorageOptions) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    /// コメントを指定する。
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// カスタムプロパティを指定する。
    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = Some(properties);
        self
    }

    /// 必須フィールドを検証して返す。
    pub fn build(mut self) -> Result<Self> {
        validate_required(&self.catalog_name, "catalog 名")?;
        validate_required(&self.namespace_name, "namespace 名")?;
        validate_required(&self.name, "table 名")?;

        if self.table_type == TableType::Managed && self.schema.is_none() {
            return Err(Error::SchemaRequired);
        }
        if self.table_type == TableType::External && self.storage_root.is_none() {
            return Err(Error::StorageRootRequired);
        }

        if self.data_source_format.is_none() {
            self.data_source_format = Some(DataSourceFormat::Alopex);
        }
        if self.properties.is_none() {
            self.properties = Some(HashMap::new());
        }
        Ok(self)
    }
}

/// カラム定義。
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// カラム名。
    pub name: String,
    /// データ型。
    pub data_type: DataType,
    /// NULL 許可（既定: true）。
    pub nullable: bool,
    /// コメント。
    pub comment: Option<String>,
}

impl ColumnDefinition {
    /// 必須フィールドを指定して作成する。
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            comment: None,
        }
    }

    /// NULL 許可を指定する。
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// コメントを指定する。
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }
}

impl Database {
    /// Catalog 一覧を取得する。
    pub fn list_catalogs(&self) -> Result<Vec<CatalogInfo>> {
        let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
        Ok(catalog
            .list_catalogs()
            .into_iter()
            .map(CatalogInfo::from)
            .collect())
    }

    /// Catalog を取得する。
    pub fn get_catalog(&self, name: &str) -> Result<CatalogInfo> {
        let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
        let meta = catalog
            .get_catalog(name)
            .ok_or_else(|| Error::CatalogNotFound(name.to_string()))?;
        Ok(meta.into())
    }

    /// Namespace 一覧を取得する。
    pub fn list_namespaces(&self, catalog_name: &str) -> Result<Vec<NamespaceInfo>> {
        let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
        ensure_catalog_exists(&*catalog, catalog_name)?;
        Ok(catalog
            .list_namespaces(catalog_name)
            .into_iter()
            .map(NamespaceInfo::from)
            .collect())
    }

    /// Namespace を取得する。
    pub fn get_namespace(&self, catalog_name: &str, namespace_name: &str) -> Result<NamespaceInfo> {
        let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
        ensure_catalog_exists(&*catalog, catalog_name)?;
        let meta = catalog
            .get_namespace(catalog_name, namespace_name)
            .ok_or_else(|| {
                Error::NamespaceNotFound(catalog_name.to_string(), namespace_name.to_string())
            })?;
        Ok(meta.into())
    }

    /// テーブル一覧を取得する。
    pub fn list_tables(&self, catalog_name: &str, namespace_name: &str) -> Result<Vec<TableInfo>> {
        let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
        ensure_namespace_exists(&*catalog, catalog_name, namespace_name)?;
        let namespace = catalog
            .get_namespace(catalog_name, namespace_name)
            .ok_or_else(|| {
                Error::NamespaceNotFound(catalog_name.to_string(), namespace_name.to_string())
            })?;

        let overlay = CatalogOverlay::new();
        let tables = catalog.list_tables_in_txn(catalog_name, namespace_name, &overlay);
        Ok(tables
            .into_iter()
            .map(|table| {
                let info = TableInfo::from(table);
                apply_storage_location(info, namespace.storage_root.as_deref())
            })
            .collect())
    }

    /// デフォルト catalog/namespace のテーブル一覧を取得する。
    pub fn list_tables_simple(&self) -> Result<Vec<TableInfo>> {
        self.list_tables("default", "default")
    }

    /// テーブル情報を取得する。
    pub fn get_table_info(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
        ensure_namespace_exists(&*catalog, catalog_name, namespace_name)?;
        let namespace = catalog
            .get_namespace(catalog_name, namespace_name)
            .ok_or_else(|| {
                Error::NamespaceNotFound(catalog_name.to_string(), namespace_name.to_string())
            })?;

        let overlay = CatalogOverlay::new();
        let tables = catalog.list_tables_in_txn(catalog_name, namespace_name, &overlay);
        let table = tables
            .into_iter()
            .find(|table| table.name == table_name)
            .ok_or_else(|| {
                Error::TableNotFound(table_full_name(catalog_name, namespace_name, table_name))
            })?;

        let info = TableInfo::from(table);
        Ok(apply_storage_location(
            info,
            namespace.storage_root.as_deref(),
        ))
    }

    /// デフォルト catalog/namespace のテーブル情報を取得する。
    pub fn get_table_info_simple(&self, table_name: &str) -> Result<TableInfo> {
        self.get_table_info("default", "default", table_name)
    }

    /// インデックス一覧を取得する。
    pub fn list_indexes(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<Vec<IndexInfo>> {
        let catalog = self.sql_catalog.read().expect("catalog lock poisoned");
        ensure_namespace_exists(&*catalog, catalog_name, namespace_name)?;
        ensure_table_exists(&*catalog, catalog_name, namespace_name, table_name)?;

        let overlay = CatalogOverlay::new();
        let fqn = TableFqn::new(catalog_name, namespace_name, table_name);
        let indexes = catalog.list_indexes_in_txn(&fqn, &overlay);
        Ok(indexes.into_iter().map(IndexInfo::from).collect())
    }

    /// デフォルト catalog/namespace のインデックス一覧を取得する。
    pub fn list_indexes_simple(&self, table_name: &str) -> Result<Vec<IndexInfo>> {
        self.list_indexes("default", "default", table_name)
    }

    /// インデックス情報を取得する。
    pub fn get_index_info(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<IndexInfo> {
        let indexes = self.list_indexes(catalog_name, namespace_name, table_name)?;
        indexes
            .into_iter()
            .find(|index| index.name == index_name)
            .ok_or_else(|| {
                Error::IndexNotFound(index_full_name(
                    catalog_name,
                    namespace_name,
                    table_name,
                    index_name,
                ))
            })
    }

    /// デフォルト catalog/namespace のインデックス情報を取得する。
    pub fn get_index_info_simple(&self, table_name: &str, index_name: &str) -> Result<IndexInfo> {
        self.get_index_info("default", "default", table_name, index_name)
    }

    /// Catalog を作成する。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::{CreateCatalogRequest, Database};
    ///
    /// let db = Database::new();
    /// let catalog = db.create_catalog(CreateCatalogRequest::new("main")).unwrap();
    /// assert_eq!(catalog.name, "main");
    /// ```
    pub fn create_catalog(&self, request: CreateCatalogRequest) -> Result<CatalogInfo> {
        let request = request.build()?;
        let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");
        if catalog.get_catalog(&request.name).is_some() {
            return Err(Error::CatalogAlreadyExists(request.name));
        }
        let meta = CatalogMeta {
            name: request.name,
            comment: request.comment,
            storage_root: request.storage_root,
        };
        catalog
            .create_catalog(meta.clone())
            .map_err(|err| Error::Sql(err.into()))?;
        Ok(meta.into())
    }

    /// Catalog を削除する。
    pub fn delete_catalog(&self, name: &str, force: bool) -> Result<()> {
        if name == "default" {
            return Err(Error::CannotDeleteDefault("catalog".to_string()));
        }
        let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");
        ensure_catalog_exists(&*catalog, name)?;

        if !force {
            let namespaces = catalog.list_namespaces(name);
            let has_non_default = namespaces.iter().any(|ns| ns.name != "default");
            let has_tables = namespaces.iter().any(|ns| {
                let overlay = CatalogOverlay::new();
                !catalog
                    .list_tables_in_txn(name, &ns.name, &overlay)
                    .is_empty()
            });
            if has_non_default || has_tables {
                return Err(Error::CatalogNotEmpty(name.to_string()));
            }
        }

        catalog
            .delete_catalog(name)
            .map_err(|err| Error::Sql(err.into()))?;
        Ok(())
    }

    /// Namespace を作成する。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::{CreateCatalogRequest, CreateNamespaceRequest, Database};
    ///
    /// let db = Database::new();
    /// db.create_catalog(CreateCatalogRequest::new("main")).unwrap();
    /// let namespace = db
    ///     .create_namespace(CreateNamespaceRequest::new("main", "analytics"))
    ///     .unwrap();
    /// assert_eq!(namespace.catalog_name, "main");
    /// assert_eq!(namespace.name, "analytics");
    /// ```
    pub fn create_namespace(&self, request: CreateNamespaceRequest) -> Result<NamespaceInfo> {
        let request = request.build()?;
        let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");
        let catalog_meta = catalog
            .get_catalog(&request.catalog_name)
            .ok_or_else(|| Error::CatalogNotFound(request.catalog_name.clone()))?;
        if catalog
            .get_namespace(&request.catalog_name, &request.name)
            .is_some()
        {
            return Err(Error::NamespaceAlreadyExists(
                request.catalog_name,
                request.name,
            ));
        }

        let storage_root = request
            .storage_root
            .or_else(|| catalog_meta.storage_root.clone());
        let meta = NamespaceMeta {
            name: request.name,
            catalog_name: request.catalog_name,
            comment: request.comment,
            storage_root,
        };
        catalog
            .create_namespace(meta.clone())
            .map_err(|err| Error::Sql(err.into()))?;
        Ok(meta.into())
    }

    /// Namespace を削除する。
    pub fn delete_namespace(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        force: bool,
    ) -> Result<()> {
        if namespace_name == "default" {
            return Err(Error::CannotDeleteDefault("namespace".to_string()));
        }
        let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");
        ensure_namespace_exists(&*catalog, catalog_name, namespace_name)?;

        let overlay = CatalogOverlay::new();
        let tables = catalog.list_tables_in_txn(catalog_name, namespace_name, &overlay);
        if !force && !tables.is_empty() {
            return Err(Error::NamespaceNotEmpty(
                catalog_name.to_string(),
                namespace_name.to_string(),
            ));
        }

        if force {
            let store = catalog.store().clone();
            let mut txn = store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
            for table in &tables {
                catalog
                    .persist_drop_table(&mut txn, &table.name)
                    .map_err(|err| Error::Sql(err.into()))?;
            }
            txn.commit_self().map_err(Error::Core)?;

            let mut overlay = CatalogOverlay::new();
            for table in tables {
                overlay.drop_table(&TableFqn::from(&table));
            }
            catalog.apply_overlay(overlay);
        }

        catalog
            .delete_namespace(catalog_name, namespace_name)
            .map_err(|err| Error::Sql(err.into()))?;
        Ok(())
    }

    /// テーブルを作成する。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::{
    ///     ColumnDefinition, CreateCatalogRequest, CreateNamespaceRequest, CreateTableRequest,
    ///     Database,
    /// };
    /// use alopex_sql::ast::ddl::DataType;
    ///
    /// let db = Database::new();
    /// db.create_catalog(CreateCatalogRequest::new("default")).unwrap();
    /// db.create_namespace(CreateNamespaceRequest::new("default", "default"))
    ///     .unwrap();
    ///
    /// let schema = vec![ColumnDefinition::new("id", DataType::Integer)];
    /// let table = db
    ///     .create_table(CreateTableRequest::new("users").with_schema(schema))
    ///     .unwrap();
    /// assert_eq!(table.name, "users");
    /// ```
    pub fn create_table(&self, request: CreateTableRequest) -> Result<TableInfo> {
        let request = request.build()?;
        let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");

        ensure_namespace_exists(&*catalog, &request.catalog_name, &request.namespace_name)?;
        ensure_table_absent(
            &*catalog,
            &request.catalog_name,
            &request.namespace_name,
            &request.name,
        )?;

        if request.table_type == TableType::Managed && request.storage_root.is_some() {
            eprintln!("警告: managed テーブルの storage_root は無視されます");
        }

        let table_id = catalog.next_table_id();
        let primary_key = request.primary_key.clone();
        let columns = build_columns(request.schema.clone(), primary_key.as_ref())?;

        let storage_options = request.storage_options.unwrap_or_else(|| StorageOptions {
            compression: Compression::None,
            ..StorageOptions::default()
        });

        let namespace = catalog.get_namespace(&request.catalog_name, &request.namespace_name);
        let storage_location = resolve_storage_location(
            &request.table_type,
            request.storage_root.as_deref(),
            namespace.as_ref(),
            &request.name,
        )?;

        let mut table = TableMetadata::new(&request.name, columns).with_table_id(table_id);
        table.catalog_name = request.catalog_name.clone();
        table.namespace_name = request.namespace_name.clone();
        table.primary_key = primary_key;
        table.storage_options = storage_options;
        table.table_type = request.table_type;
        table.data_source_format = request
            .data_source_format
            .unwrap_or(DataSourceFormat::Alopex);
        table.storage_location = storage_location;
        table.comment = request.comment;
        table.properties = request.properties.unwrap_or_default();

        let store = catalog.store().clone();
        let mut txn = store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        catalog
            .persist_create_table(&mut txn, &table)
            .map_err(|err| Error::Sql(err.into()))?;
        txn.commit_self().map_err(Error::Core)?;

        let mut overlay = CatalogOverlay::new();
        overlay.add_table(TableFqn::from(&table), table.clone());
        catalog.apply_overlay(overlay);

        let info = TableInfo::from(table);
        let namespace_root = namespace.and_then(|ns| ns.storage_root);
        Ok(apply_storage_location(info, namespace_root.as_deref()))
    }

    /// デフォルト catalog/namespace のテーブルを作成する。
    pub fn create_table_simple(
        &self,
        name: &str,
        schema: Vec<ColumnDefinition>,
    ) -> Result<TableInfo> {
        self.create_table(CreateTableRequest::new(name).with_schema(schema))
    }

    /// テーブルを削除する。
    pub fn delete_table(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let mut catalog = self.sql_catalog.write().expect("catalog lock poisoned");
        ensure_namespace_exists(&*catalog, catalog_name, namespace_name)?;
        let table = find_table_metadata(&*catalog, catalog_name, namespace_name, table_name)?
            .ok_or_else(|| {
                Error::TableNotFound(table_full_name(catalog_name, namespace_name, table_name))
            })?;

        let store = catalog.store().clone();
        let mut txn = store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        catalog
            .persist_drop_table(&mut txn, &table.name)
            .map_err(|err| Error::Sql(err.into()))?;
        txn.commit_self().map_err(Error::Core)?;

        let mut overlay = CatalogOverlay::new();
        overlay.drop_table(&TableFqn::from(&table));
        catalog.apply_overlay(overlay);
        Ok(())
    }

    /// デフォルト catalog/namespace のテーブルを削除する。
    pub fn delete_table_simple(&self, name: &str) -> Result<()> {
        self.delete_table("default", "default", name)
    }
}

impl<'a> Transaction<'a> {
    /// Catalog 一覧を取得する（オーバーレイ反映）。
    pub fn list_catalogs(&self) -> Result<Vec<CatalogInfo>> {
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        Ok(catalog
            .list_catalogs_in_txn(self.catalog_overlay())
            .into_iter()
            .map(CatalogInfo::from)
            .collect())
    }

    /// Catalog を取得する（オーバーレイ反映）。
    pub fn get_catalog(&self, name: &str) -> Result<CatalogInfo> {
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        let meta = catalog
            .get_catalog_in_txn(name, self.catalog_overlay())
            .ok_or_else(|| Error::CatalogNotFound(name.to_string()))?;
        Ok(meta.clone().into())
    }

    /// Namespace 一覧を取得する（オーバーレイ反映）。
    pub fn list_namespaces(&self, catalog_name: &str) -> Result<Vec<NamespaceInfo>> {
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        ensure_catalog_exists_in_txn(&*catalog, self.catalog_overlay(), catalog_name)?;
        Ok(catalog
            .list_namespaces_in_txn(catalog_name, self.catalog_overlay())
            .into_iter()
            .map(NamespaceInfo::from)
            .collect())
    }

    /// Namespace を取得する（オーバーレイ反映）。
    pub fn get_namespace(&self, catalog_name: &str, namespace_name: &str) -> Result<NamespaceInfo> {
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        ensure_catalog_exists_in_txn(&*catalog, self.catalog_overlay(), catalog_name)?;
        let meta = catalog
            .get_namespace_in_txn(catalog_name, namespace_name, self.catalog_overlay())
            .ok_or_else(|| {
                Error::NamespaceNotFound(catalog_name.to_string(), namespace_name.to_string())
            })?;
        Ok(meta.clone().into())
    }

    /// テーブル一覧を取得する（オーバーレイ反映）。
    pub fn list_tables(&self, catalog_name: &str, namespace_name: &str) -> Result<Vec<TableInfo>> {
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        ensure_namespace_exists_in_txn(
            &*catalog,
            self.catalog_overlay(),
            catalog_name,
            namespace_name,
        )?;
        let namespace = catalog
            .get_namespace_in_txn(catalog_name, namespace_name, self.catalog_overlay())
            .cloned()
            .ok_or_else(|| {
                Error::NamespaceNotFound(catalog_name.to_string(), namespace_name.to_string())
            })?;
        let tables =
            catalog.list_tables_in_txn(catalog_name, namespace_name, self.catalog_overlay());
        Ok(tables
            .into_iter()
            .map(|table| {
                let info = TableInfo::from(table);
                apply_storage_location(info, namespace.storage_root.as_deref())
            })
            .collect())
    }

    /// テーブル情報を取得する（オーバーレイ反映）。
    pub fn get_table_info(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        ensure_namespace_exists_in_txn(
            &*catalog,
            self.catalog_overlay(),
            catalog_name,
            namespace_name,
        )?;
        let namespace = catalog
            .get_namespace_in_txn(catalog_name, namespace_name, self.catalog_overlay())
            .cloned()
            .ok_or_else(|| {
                Error::NamespaceNotFound(catalog_name.to_string(), namespace_name.to_string())
            })?;

        let tables =
            catalog.list_tables_in_txn(catalog_name, namespace_name, self.catalog_overlay());
        let table = tables
            .into_iter()
            .find(|table| table.name == table_name)
            .ok_or_else(|| {
                Error::TableNotFound(table_full_name(catalog_name, namespace_name, table_name))
            })?;
        let info = TableInfo::from(table);
        Ok(apply_storage_location(
            info,
            namespace.storage_root.as_deref(),
        ))
    }

    /// Catalog を作成する（オーバーレイ反映）。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::{CreateCatalogRequest, Database, TxnMode};
    ///
    /// let db = Database::new();
    /// let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    /// let catalog = txn.create_catalog(CreateCatalogRequest::new("main")).unwrap();
    /// assert_eq!(catalog.name, "main");
    /// ```
    pub fn create_catalog(&mut self, request: CreateCatalogRequest) -> Result<CatalogInfo> {
        ensure_write_mode(self)?;
        let request = request.build()?;
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        if catalog
            .get_catalog_in_txn(&request.name, self.catalog_overlay())
            .is_some()
        {
            return Err(Error::CatalogAlreadyExists(request.name));
        }

        let meta = CatalogMeta {
            name: request.name,
            comment: request.comment,
            storage_root: request.storage_root,
        };
        self.catalog_overlay_mut().add_catalog(meta.clone());
        Ok(meta.into())
    }

    /// Catalog を削除する（オーバーレイ反映）。
    pub fn delete_catalog(&mut self, name: &str, force: bool) -> Result<()> {
        ensure_write_mode(self)?;
        if name == "default" {
            return Err(Error::CannotDeleteDefault("catalog".to_string()));
        }
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        ensure_catalog_exists_in_txn(&*catalog, self.catalog_overlay(), name)?;

        if !force {
            let namespaces = catalog.list_namespaces_in_txn(name, self.catalog_overlay());
            let has_non_default = namespaces.iter().any(|ns| ns.name != "default");
            let has_tables = namespaces.iter().any(|ns| {
                !catalog
                    .list_tables_in_txn(name, &ns.name, self.catalog_overlay())
                    .is_empty()
            });
            if has_non_default || has_tables {
                return Err(Error::CatalogNotEmpty(name.to_string()));
            }
        }

        if force {
            self.catalog_overlay_mut().drop_cascade_catalog(name);
        } else {
            self.catalog_overlay_mut().drop_catalog(name);
        }
        Ok(())
    }

    /// Namespace を作成する（オーバーレイ反映）。
    pub fn create_namespace(&mut self, request: CreateNamespaceRequest) -> Result<NamespaceInfo> {
        ensure_write_mode(self)?;
        let request = request.build()?;
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        let catalog_meta = catalog
            .get_catalog_in_txn(&request.catalog_name, self.catalog_overlay())
            .ok_or_else(|| Error::CatalogNotFound(request.catalog_name.clone()))?;
        if catalog
            .get_namespace_in_txn(&request.catalog_name, &request.name, self.catalog_overlay())
            .is_some()
        {
            return Err(Error::NamespaceAlreadyExists(
                request.catalog_name,
                request.name,
            ));
        }

        let storage_root = request
            .storage_root
            .or_else(|| catalog_meta.storage_root.clone());
        let meta = NamespaceMeta {
            name: request.name,
            catalog_name: request.catalog_name,
            comment: request.comment,
            storage_root,
        };
        self.catalog_overlay_mut().add_namespace(meta.clone());
        Ok(meta.into())
    }

    /// Namespace を削除する（オーバーレイ反映）。
    pub fn delete_namespace(
        &mut self,
        catalog_name: &str,
        namespace_name: &str,
        force: bool,
    ) -> Result<()> {
        ensure_write_mode(self)?;
        if namespace_name == "default" {
            return Err(Error::CannotDeleteDefault("namespace".to_string()));
        }
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        ensure_namespace_exists_in_txn(
            &*catalog,
            self.catalog_overlay(),
            catalog_name,
            namespace_name,
        )?;

        let tables =
            catalog.list_tables_in_txn(catalog_name, namespace_name, self.catalog_overlay());
        if !force && !tables.is_empty() {
            return Err(Error::NamespaceNotEmpty(
                catalog_name.to_string(),
                namespace_name.to_string(),
            ));
        }

        if force {
            self.catalog_overlay_mut()
                .drop_cascade_namespace(catalog_name, namespace_name);
        } else {
            self.catalog_overlay_mut()
                .drop_namespace(catalog_name, namespace_name);
        }
        Ok(())
    }

    /// テーブルを作成する（オーバーレイ反映）。
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_embedded::{
    ///     ColumnDefinition, CreateCatalogRequest, CreateNamespaceRequest, CreateTableRequest,
    ///     Database, TxnMode,
    /// };
    /// use alopex_sql::ast::ddl::DataType;
    ///
    /// let db = Database::new();
    /// let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    /// txn.create_catalog(CreateCatalogRequest::new("main")).unwrap();
    /// txn.create_namespace(CreateNamespaceRequest::new("main", "default"))
    ///     .unwrap();
    ///
    /// let schema = vec![ColumnDefinition::new("id", DataType::Integer)];
    /// let table = txn
    ///     .create_table(
    ///         CreateTableRequest::new("events")
    ///             .with_catalog_name("main")
    ///             .with_namespace_name("default")
    ///             .with_schema(schema),
    ///     )
    ///     .unwrap();
    /// assert_eq!(table.name, "events");
    /// ```
    pub fn create_table(&mut self, request: CreateTableRequest) -> Result<TableInfo> {
        ensure_write_mode(self)?;
        let request = request.build()?;

        let mut catalog = self.db.sql_catalog.write().expect("catalog lock poisoned");
        ensure_namespace_exists_in_txn(
            &*catalog,
            self.catalog_overlay(),
            &request.catalog_name,
            &request.namespace_name,
        )?;
        ensure_table_absent_in_txn(
            &*catalog,
            self.catalog_overlay(),
            &request.catalog_name,
            &request.namespace_name,
            &request.name,
        )?;

        if request.table_type == TableType::Managed && request.storage_root.is_some() {
            eprintln!("警告: managed テーブルの storage_root は無視されます");
        }

        let table_id = catalog.next_table_id();
        let primary_key = request.primary_key.clone();
        let columns = build_columns(request.schema.clone(), primary_key.as_ref())?;

        let storage_options = request.storage_options.unwrap_or_else(|| StorageOptions {
            compression: Compression::None,
            ..StorageOptions::default()
        });

        let namespace = catalog
            .get_namespace_in_txn(
                &request.catalog_name,
                &request.namespace_name,
                self.catalog_overlay(),
            )
            .cloned();
        let storage_location = resolve_storage_location(
            &request.table_type,
            request.storage_root.as_deref(),
            namespace.as_ref(),
            &request.name,
        )?;

        let mut table = TableMetadata::new(&request.name, columns).with_table_id(table_id);
        table.catalog_name = request.catalog_name.clone();
        table.namespace_name = request.namespace_name.clone();
        table.primary_key = primary_key;
        table.storage_options = storage_options;
        table.table_type = request.table_type;
        table.data_source_format = request
            .data_source_format
            .unwrap_or(DataSourceFormat::Alopex);
        table.storage_location = storage_location;
        table.comment = request.comment;
        table.properties = request.properties.unwrap_or_default();

        self.catalog_overlay_mut()
            .add_table(TableFqn::from(&table), table.clone());
        let info = TableInfo::from(table);
        let namespace_root = namespace.and_then(|ns| ns.storage_root);
        Ok(apply_storage_location(info, namespace_root.as_deref()))
    }

    /// テーブルを削除する（オーバーレイ反映）。
    pub fn delete_table(
        &mut self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<()> {
        ensure_write_mode(self)?;
        let catalog = self.db.sql_catalog.read().expect("catalog lock poisoned");
        ensure_namespace_exists_in_txn(
            &*catalog,
            self.catalog_overlay(),
            catalog_name,
            namespace_name,
        )?;
        let table = find_table_metadata_in_txn(
            &*catalog,
            self.catalog_overlay(),
            catalog_name,
            namespace_name,
            table_name,
        )?
        .ok_or_else(|| {
            Error::TableNotFound(table_full_name(catalog_name, namespace_name, table_name))
        })?;

        self.catalog_overlay_mut()
            .drop_table(&TableFqn::from(&table));
        Ok(())
    }
}

fn validate_required(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "{label}が未指定です"
        ))));
    }
    Ok(())
}

fn ensure_catalog_exists<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    name: &str,
) -> Result<()> {
    if catalog.get_catalog(name).is_none() {
        return Err(Error::CatalogNotFound(name.to_string()));
    }
    Ok(())
}

fn ensure_catalog_exists_in_txn<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    overlay: &CatalogOverlay,
    name: &str,
) -> Result<()> {
    if catalog.get_catalog_in_txn(name, overlay).is_none() {
        return Err(Error::CatalogNotFound(name.to_string()));
    }
    Ok(())
}

fn ensure_namespace_exists<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<()> {
    ensure_catalog_exists(catalog, catalog_name)?;
    if catalog
        .get_namespace(catalog_name, namespace_name)
        .is_none()
    {
        return Err(Error::NamespaceNotFound(
            catalog_name.to_string(),
            namespace_name.to_string(),
        ));
    }
    Ok(())
}

fn ensure_namespace_exists_in_txn<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    overlay: &CatalogOverlay,
    catalog_name: &str,
    namespace_name: &str,
) -> Result<()> {
    ensure_catalog_exists_in_txn(catalog, overlay, catalog_name)?;
    if catalog
        .get_namespace_in_txn(catalog_name, namespace_name, overlay)
        .is_none()
    {
        return Err(Error::NamespaceNotFound(
            catalog_name.to_string(),
            namespace_name.to_string(),
        ));
    }
    Ok(())
}

fn ensure_table_exists<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<()> {
    let Some(table) = find_table_metadata(catalog, catalog_name, namespace_name, table_name)?
    else {
        return Err(Error::TableNotFound(table_full_name(
            catalog_name,
            namespace_name,
            table_name,
        )));
    };
    let _ = table;
    Ok(())
}

fn ensure_table_absent<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<()> {
    if find_table_metadata(catalog, catalog_name, namespace_name, table_name)?.is_some() {
        return Err(Error::TableAlreadyExists(table_full_name(
            catalog_name,
            namespace_name,
            table_name,
        )));
    }
    Ok(())
}

fn ensure_table_absent_in_txn<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    overlay: &CatalogOverlay,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<()> {
    if find_table_metadata_in_txn(catalog, overlay, catalog_name, namespace_name, table_name)?
        .is_some()
    {
        return Err(Error::TableAlreadyExists(table_full_name(
            catalog_name,
            namespace_name,
            table_name,
        )));
    }
    Ok(())
}

fn find_table_metadata<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<Option<TableMetadata>> {
    let overlay = CatalogOverlay::new();
    let tables = catalog.list_tables_in_txn(catalog_name, namespace_name, &overlay);
    Ok(tables.into_iter().find(|table| table.name == table_name))
}

fn find_table_metadata_in_txn<S: alopex_core::kv::KVStore>(
    catalog: &alopex_sql::catalog::PersistentCatalog<S>,
    overlay: &CatalogOverlay,
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
) -> Result<Option<TableMetadata>> {
    let tables = catalog.list_tables_in_txn(catalog_name, namespace_name, overlay);
    Ok(tables.into_iter().find(|table| table.name == table_name))
}

fn table_full_name(catalog_name: &str, namespace_name: &str, table_name: &str) -> String {
    format!("{catalog_name}.{namespace_name}.{table_name}")
}

fn index_full_name(
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
    index_name: &str,
) -> String {
    format!("{catalog_name}.{namespace_name}.{table_name}.{index_name}")
}

fn apply_storage_location(mut info: TableInfo, namespace_root: Option<&str>) -> TableInfo {
    if info.storage_location.is_none() && info.table_type == TableType::Managed {
        if let Some(root) = namespace_root {
            info.storage_location = Some(format!("{root}/{}", info.name));
        }
    }
    info
}

fn resolve_storage_location(
    table_type: &TableType,
    request_storage_root: Option<&str>,
    namespace: Option<&NamespaceMeta>,
    table_name: &str,
) -> Result<Option<String>> {
    match table_type {
        TableType::Managed => Ok(namespace
            .and_then(|ns| ns.storage_root.as_deref())
            .map(|root| format!("{root}/{table_name}"))),
        TableType::External => {
            let storage_root = request_storage_root
                .map(|root| root.to_string())
                .ok_or(Error::StorageRootRequired)?;
            Ok(Some(storage_root))
        }
    }
}

fn build_columns(
    schema: Option<Vec<ColumnDefinition>>,
    primary_key: Option<&Vec<String>>,
) -> Result<Vec<ColumnMetadata>> {
    let Some(schema) = schema else {
        return Ok(Vec::new());
    };

    let mut columns = Vec::with_capacity(schema.len());
    for definition in schema {
        validate_required(&definition.name, "column 名")?;
        let mut column = ColumnMetadata::new(
            definition.name.clone(),
            ResolvedType::from_ast(&definition.data_type),
        )
        .with_not_null(!definition.nullable);
        if primary_key
            .map(|keys| keys.iter().any(|key| key == &definition.name))
            .unwrap_or(false)
        {
            column = column.with_primary_key(true).with_not_null(true);
        }
        columns.push(column);
    }

    if let Some(keys) = primary_key {
        let missing: Vec<String> = keys
            .iter()
            .filter(|key| !columns.iter().any(|col| col.name == **key))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
                "主キーが見つかりません: {}",
                missing.join(", ")
            ))));
        }
    }

    Ok(columns)
}

fn ensure_write_mode(txn: &Transaction<'_>) -> Result<()> {
    let mode = txn.txn_mode()?;
    if mode != TxnMode::ReadWrite {
        return Err(Error::TxnReadOnly);
    }
    Ok(())
}

fn resolved_type_to_string(resolved_type: &ResolvedType) -> String {
    match resolved_type {
        ResolvedType::Integer => "INTEGER".to_string(),
        ResolvedType::BigInt => "BIGINT".to_string(),
        ResolvedType::Float => "FLOAT".to_string(),
        ResolvedType::Double => "DOUBLE".to_string(),
        ResolvedType::Text => "TEXT".to_string(),
        ResolvedType::Blob => "BLOB".to_string(),
        ResolvedType::Boolean => "BOOLEAN".to_string(),
        ResolvedType::Timestamp => "TIMESTAMP".to_string(),
        ResolvedType::Vector { dimension, metric } => {
            let metric = match metric {
                VectorMetric::Cosine => "COSINE",
                VectorMetric::L2 => "L2",
                VectorMetric::Inner => "INNER",
            };
            format!("VECTOR({dimension}, {metric})")
        }
        ResolvedType::Null => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, TxnMode};
    use alopex_sql::catalog::{ColumnMetadata, RowIdMode};
    use alopex_sql::ExecutionResult;

    #[test]
    fn storage_info_default_is_row_none() {
        let info = StorageInfo::default();
        assert_eq!(info.storage_type, "row");
        assert_eq!(info.compression, "none");
    }

    #[test]
    fn column_definition_defaults_to_nullable() {
        let column = ColumnDefinition::new("id", DataType::Integer);
        assert!(column.nullable);
        assert!(column.comment.is_none());

        let column = column.with_nullable(false).with_comment("ID");
        assert!(!column.nullable);
        assert_eq!(column.comment.as_deref(), Some("ID"));
    }

    #[test]
    fn create_catalog_request_builder_validates_name() {
        let err = CreateCatalogRequest::new("").build().unwrap_err();
        assert!(matches!(err, Error::Core(_)));

        let request = CreateCatalogRequest::new("main")
            .with_comment("メイン")
            .with_storage_root("/data")
            .build()
            .unwrap();
        assert_eq!(request.name, "main");
        assert_eq!(request.comment.as_deref(), Some("メイン"));
        assert_eq!(request.storage_root.as_deref(), Some("/data"));
    }

    #[test]
    fn create_namespace_request_builder_validates_fields() {
        let err = CreateNamespaceRequest::new("", "default")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Core(_)));

        let request = CreateNamespaceRequest::new("main", "analytics")
            .with_comment("分析")
            .build()
            .unwrap();
        assert_eq!(request.catalog_name, "main");
        assert_eq!(request.name, "analytics");
        assert_eq!(request.comment.as_deref(), Some("分析"));
    }

    #[test]
    fn create_table_request_defaults_and_validation() {
        let schema = vec![ColumnDefinition::new("id", DataType::Integer)];

        let request = CreateTableRequest::new("users")
            .with_schema(schema.clone())
            .build()
            .unwrap();
        assert_eq!(request.catalog_name, "default");
        assert_eq!(request.namespace_name, "default");
        assert_eq!(request.table_type, TableType::Managed);
        assert_eq!(request.data_source_format, Some(DataSourceFormat::Alopex));
        assert_eq!(request.properties.as_ref().unwrap().len(), 0);

        let err = CreateTableRequest::new("users").build().unwrap_err();
        assert!(matches!(err, Error::SchemaRequired));

        let err = CreateTableRequest::new("ext")
            .with_table_type(TableType::External)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::StorageRootRequired));

        let request = CreateTableRequest::new("ext")
            .with_table_type(TableType::External)
            .with_storage_root("/external")
            .build()
            .unwrap();
        assert_eq!(request.storage_root.as_deref(), Some("/external"));
        assert_eq!(request.data_source_format, Some(DataSourceFormat::Alopex));
        assert!(request.properties.as_ref().unwrap().is_empty());
    }

    #[test]
    fn table_info_converts_from_metadata() {
        let mut table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_table_id(42);
        table.catalog_name = "main".to_string();
        table.namespace_name = "default".to_string();
        table.primary_key = Some(vec!["id".to_string()]);
        table.storage_options = StorageOptions {
            storage_type: StorageType::Columnar,
            compression: Compression::Zstd,
            row_group_size: 1024,
            row_id_mode: RowIdMode::Direct,
        };

        let info = TableInfo::from(table);
        assert_eq!(info.name, "users");
        assert_eq!(info.table_id, 42);
        assert_eq!(info.catalog_name, "main");
        assert_eq!(info.namespace_name, "default");
        assert_eq!(info.columns.len(), 2);
        assert_eq!(info.columns[0].data_type, "INTEGER");
        assert!(info.columns[0].is_primary_key);
        assert_eq!(info.storage_options.storage_type, "columnar");
        assert_eq!(info.storage_options.compression, "zstd");
    }

    #[test]
    fn table_info_defaults_storage_options_to_row_none() {
        let table = TableMetadata::new(
            "logs",
            vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        );
        let info = TableInfo::from(table);
        assert_eq!(info.storage_options.storage_type, "row");
        assert_eq!(info.storage_options.compression, "none");
    }

    #[test]
    fn index_info_converts_from_metadata() {
        let mut index = IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
            .with_unique(true)
            .with_method(IndexMethod::Hnsw);
        index.catalog_name = "main".to_string();
        index.namespace_name = "default".to_string();

        let info = IndexInfo::from(index);
        assert_eq!(info.name, "idx_users_id");
        assert_eq!(info.table_name, "users");
        assert_eq!(info.method, "hnsw");
        assert!(info.is_unique);
    }

    fn ensure_default_catalog_and_namespace(db: &Database) {
        let _ = db.create_catalog(CreateCatalogRequest::new("default"));
        let _ = db.create_namespace(CreateNamespaceRequest::new("default", "default"));
    }

    #[test]
    fn database_catalog_and_namespace_crud() {
        let db = Database::new();

        let catalog = db
            .create_catalog(CreateCatalogRequest::new("main"))
            .unwrap();
        assert_eq!(catalog.name, "main");

        let namespace = db
            .create_namespace(CreateNamespaceRequest::new("main", "analytics"))
            .unwrap();
        assert_eq!(namespace.catalog_name, "main");
        assert_eq!(namespace.name, "analytics");

        let list = db.list_namespaces("main").unwrap();
        assert_eq!(list.len(), 1);

        let err = db.delete_catalog("main", false).unwrap_err();
        assert!(matches!(err, Error::CatalogNotEmpty(_)));

        db.delete_catalog("main", true).unwrap();

        let err = db.get_catalog("main").unwrap_err();
        assert!(matches!(err, Error::CatalogNotFound(_)));
    }

    #[test]
    fn cannot_delete_default_catalog_or_namespace() {
        let db = Database::new();
        ensure_default_catalog_and_namespace(&db);

        let err = db.delete_catalog("default", true).unwrap_err();
        assert!(matches!(err, Error::CannotDeleteDefault(_)));

        let err = db.delete_namespace("default", "default", true).unwrap_err();
        assert!(matches!(err, Error::CannotDeleteDefault(_)));

        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        let err = txn.delete_catalog("default", true).unwrap_err();
        assert!(matches!(err, Error::CannotDeleteDefault(_)));

        let err = txn
            .delete_namespace("default", "default", true)
            .unwrap_err();
        assert!(matches!(err, Error::CannotDeleteDefault(_)));
    }

    #[test]
    fn database_table_crud_and_simple_helpers() {
        let db = Database::new();
        ensure_default_catalog_and_namespace(&db);

        let schema = vec![ColumnDefinition::new("id", DataType::Integer)];
        let info = db.create_table_simple("users", schema).unwrap();
        assert_eq!(info.catalog_name, "default");
        assert_eq!(info.namespace_name, "default");
        assert_eq!(info.table_type, TableType::Managed);
        assert_eq!(info.data_source_format, DataSourceFormat::Alopex);
        assert_eq!(info.storage_options.storage_type, "row");
        assert_eq!(info.storage_options.compression, "none");

        let tables = db.list_tables_simple().unwrap();
        assert_eq!(tables.len(), 1);

        let info = db.get_table_info_simple("users").unwrap();
        assert_eq!(info.name, "users");

        let err = db
            .create_table_simple(
                "users",
                vec![ColumnDefinition::new("id", DataType::Integer)],
            )
            .unwrap_err();
        assert!(matches!(err, Error::TableAlreadyExists(_)));

        db.delete_table_simple("users").unwrap();
        assert!(db.list_tables_simple().unwrap().is_empty());
    }

    #[test]
    fn database_index_read_helpers() {
        let db = Database::new();
        ensure_default_catalog_and_namespace(&db);

        let schema = vec![ColumnDefinition::new("id", DataType::Integer)];
        db.create_table_simple("users", schema).unwrap();

        let result = db
            .execute_sql("CREATE INDEX idx_users_id ON users (id);")
            .unwrap();
        assert!(matches!(result, ExecutionResult::Success));

        let indexes = db.list_indexes_simple("users").unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "idx_users_id");
        assert_eq!(indexes[0].method, "btree");

        let index = db.get_index_info_simple("users", "idx_users_id").unwrap();
        assert_eq!(index.table_name, "users");
    }

    #[test]
    fn transaction_overlay_visibility_and_commit() {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();

        txn.create_catalog(CreateCatalogRequest::new("main"))
            .unwrap();
        txn.create_namespace(CreateNamespaceRequest::new("main", "default"))
            .unwrap();

        let schema = vec![ColumnDefinition::new("id", DataType::Integer)];
        txn.create_table(
            CreateTableRequest::new("events")
                .with_catalog_name("main")
                .with_namespace_name("default")
                .with_schema(schema),
        )
        .unwrap();

        let tables = txn.list_tables("main", "default").unwrap();
        assert_eq!(tables.len(), 1);

        txn.commit().unwrap();

        let info = db.get_table_info("main", "default", "events").unwrap();
        assert_eq!(info.name, "events");
    }

    #[test]
    fn transaction_commit_persists_overlay_to_store() {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();

        txn.create_catalog(CreateCatalogRequest::new("main"))
            .unwrap();
        txn.create_namespace(CreateNamespaceRequest::new("main", "default"))
            .unwrap();

        let schema = vec![ColumnDefinition::new("id", DataType::Integer)];
        txn.create_table(
            CreateTableRequest::new("events")
                .with_catalog_name("main")
                .with_namespace_name("default")
                .with_schema(schema),
        )
        .unwrap();

        txn.commit().unwrap();

        let reloaded = alopex_sql::catalog::PersistentCatalog::load(db.store.clone()).unwrap();
        assert!(reloaded.get_catalog("main").is_some());
        assert!(reloaded.get_namespace("main", "default").is_some());
        assert!(reloaded.table_exists("events"));
    }

    #[test]
    fn transaction_rollback_discards_overlay() {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();

        txn.create_catalog(CreateCatalogRequest::new("main"))
            .unwrap();
        txn.create_namespace(CreateNamespaceRequest::new("main", "default"))
            .unwrap();

        let schema = vec![ColumnDefinition::new("id", DataType::Integer)];
        txn.create_table(
            CreateTableRequest::new("staging")
                .with_catalog_name("main")
                .with_namespace_name("default")
                .with_schema(schema),
        )
        .unwrap();

        txn.rollback().unwrap();

        let err = db.get_table_info("main", "default", "staging").unwrap_err();
        assert!(matches!(err, Error::CatalogNotFound(_)));
    }

    #[test]
    fn transaction_readonly_rejects_ddl() {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadOnly).unwrap();
        let err = txn
            .create_catalog(CreateCatalogRequest::new("main"))
            .unwrap_err();
        assert!(matches!(err, Error::TxnReadOnly));
    }
}
