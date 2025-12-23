//! Catalog API 向けの公開型定義。

use std::collections::HashMap;

use alopex_sql::ast::ddl::{DataType, IndexMethod, VectorMetric};
use alopex_sql::catalog::{Compression, IndexMetadata, StorageOptions, StorageType, TableMetadata};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{DataSourceFormat, TableType};

use crate::{Error, Result};

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
            table_type: TableType::Managed,
            columns,
            primary_key,
            storage_location: None,
            data_source_format: DataSourceFormat::Alopex,
            storage_options,
            comment: None,
            properties: HashMap::new(),
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

fn validate_required(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "{label}が未指定です"
        ))));
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
    use alopex_sql::catalog::{ColumnMetadata, RowIdMode};

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
}
