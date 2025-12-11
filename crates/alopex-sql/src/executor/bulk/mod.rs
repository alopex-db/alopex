//! COPY / Bulk Load 実装。
//!
//! 現段階では CSV/Parquet を簡易的に読み込み、テーブルスキーマに従って
//! `SqlValue` へ変換する。Columnar ストレージも Row ストレージと同じ経路で
//! 取り込み、将来の columnar エンジン実装で差し替え可能な構造にしている。

use std::fs;
use std::path::{Path, PathBuf};

use alopex_core::kv::KVStore;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{Catalog, IndexMetadata, TableMetadata};
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::planner::types::ResolvedType;
use crate::storage::{SqlTransaction, SqlValue, StorageError};

mod csv;
mod parquet;

pub use csv::CsvReader;
pub use parquet::ParquetReader;

/// ファイル形式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Parquet,
}

/// COPY オプション。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyOptions {
    /// CSV ヘッダ行の有無。
    pub header: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self { header: false }
    }
}

/// COPY セキュリティ設定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySecurityConfig {
    /// 許可するベースディレクトリ一覧（None なら無制限）。
    pub allowed_base_dirs: Option<Vec<PathBuf>>,
    /// シンボリックリンクを許可するか。
    pub allow_symlinks: bool,
}

impl Default for CopySecurityConfig {
    fn default() -> Self {
        Self {
            allowed_base_dirs: None,
            allow_symlinks: false,
        }
    }
}

/// 入力スキーマのフィールド。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyField {
    pub name: Option<String>,
    pub data_type: Option<ResolvedType>,
}

/// 入力スキーマ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySchema {
    pub fields: Vec<CopyField>,
}

impl CopySchema {
    pub fn from_table(table: &TableMetadata) -> Self {
        let fields = table
            .columns
            .iter()
            .map(|c| CopyField {
                name: Some(c.name.clone()),
                data_type: Some(c.data_type.clone()),
            })
            .collect();
        Self { fields }
    }
}

/// バッチリーダー。
pub trait BulkReader {
    /// 入力スキーマを返す。
    fn schema(&self) -> &CopySchema;
    /// 最大 `max_rows` 行のバッチを返す。終端で None。
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<SqlValue>>>>;
}

/// COPY 文を実行する。
pub fn execute_copy<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    table_name: &str,
    file_path: &str,
    format: FileFormat,
    options: CopyOptions,
    config: &CopySecurityConfig,
) -> Result<ExecutionResult> {
    let table_meta = catalog
        .get_table(table_name)
        .cloned()
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    validate_file_path(file_path, config)?;

    if !Path::new(file_path).exists() {
        return Err(ExecutorError::FileNotFound(file_path.to_string()));
    }

    let reader: Box<dyn BulkReader> = match format {
        FileFormat::Parquet => {
            Box::new(ParquetReader::open(file_path, &table_meta, options.header)?)
        }
        FileFormat::Csv => Box::new(CsvReader::open(file_path, &table_meta, options.header)?),
    };

    validate_schema(reader.schema(), &table_meta)?;

    let rows_loaded = match table_meta.storage_options.storage_type {
        crate::catalog::StorageType::Columnar => {
            bulk_load_columnar(txn, catalog, &table_meta, reader)?
        }
        crate::catalog::StorageType::Row => bulk_load_row(txn, catalog, &table_meta, reader)?,
    };

    Ok(ExecutionResult::RowsAffected(rows_loaded))
}

/// パスセキュリティ検証。
pub fn validate_file_path(file_path: &str, config: &CopySecurityConfig) -> Result<()> {
    let path = Path::new(file_path);

    // 先に存在確認を行い、設計どおり FileNotFound を優先する。
    if !path.exists() {
        return Err(ExecutorError::FileNotFound(file_path.into()));
    }

    let canonical = path
        .canonicalize()
        .map_err(|e| ExecutorError::PathValidationFailed {
            path: file_path.into(),
            reason: format!("failed to canonicalize: {e}"),
        })?;

    if let Some(base_dirs) = &config.allowed_base_dirs {
        let allowed = base_dirs.iter().any(|base| canonical.starts_with(base));
        if !allowed {
            return Err(ExecutorError::PathValidationFailed {
                path: file_path.into(),
                reason: format!("path not in allowed directories: {:?}", base_dirs),
            });
        }
    }

    if !config.allow_symlinks && path.is_symlink() {
        return Err(ExecutorError::PathValidationFailed {
            path: file_path.into(),
            reason: "symbolic links not allowed".into(),
        });
    }

    let metadata = fs::metadata(&canonical).map_err(|e| ExecutorError::PathValidationFailed {
        path: file_path.into(),
        reason: format!("cannot access file: {e}"),
    })?;

    if !metadata.is_file() {
        return Err(ExecutorError::PathValidationFailed {
            path: file_path.into(),
            reason: "path is not a regular file".into(),
        });
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o444 == 0 {
            return Err(ExecutorError::PathValidationFailed {
                path: file_path.into(),
                reason: "file is not readable".into(),
            });
        }
    }

    Ok(())
}

/// スキーマ整合性検証。
pub fn validate_schema(schema: &CopySchema, table_meta: &TableMetadata) -> Result<()> {
    if schema.fields.len() != table_meta.columns.len() {
        return Err(ExecutorError::SchemaMismatch {
            expected: table_meta.columns.len(),
            actual: schema.fields.len(),
            reason: "column count mismatch".into(),
        });
    }

    for (idx, (field, col)) in schema
        .fields
        .iter()
        .zip(table_meta.columns.iter())
        .enumerate()
    {
        if let Some(dt) = &field.data_type {
            if !is_type_compatible(dt, &col.data_type) {
                return Err(ExecutorError::SchemaMismatch {
                    expected: table_meta.columns.len(),
                    actual: schema.fields.len(),
                    reason: format!(
                        "type mismatch for column '{}': expected {:?}, got {:?}",
                        col.name, col.data_type, dt
                    ),
                });
            }
        }
        if let Some(name) = &field.name {
            if name != &col.name {
                return Err(ExecutorError::SchemaMismatch {
                    expected: table_meta.columns.len(),
                    actual: schema.fields.len(),
                    reason: format!(
                        "column name mismatch at position {}: expected '{}', got '{}'",
                        idx, col.name, name
                    ),
                });
            }
        }
    }

    Ok(())
}

/// Row ストレージへの書き込み。
fn bulk_load_row<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    table: &TableMetadata,
    mut reader: Box<dyn BulkReader>,
) -> Result<u64> {
    let indexes: Vec<IndexMetadata> = catalog
        .get_indexes_for_table(&table.name)
        .into_iter()
        .cloned()
        .collect();
    let (hnsw_indexes, btree_indexes): (Vec<_>, Vec<_>) = indexes
        .into_iter()
        .partition(|idx| matches!(idx.method, Some(IndexMethod::Hnsw)));

    let mut staged: Vec<(u64, Vec<SqlValue>)> = Vec::new();
    {
        let mut storage = txn.table_storage(table);
        while let Some(batch) = reader.next_batch(1024)? {
            for row in batch {
                if row.len() != table.column_count() {
                    return Err(ExecutorError::BulkLoad(format!(
                        "row has {} columns, expected {}",
                        row.len(),
                        table.column_count()
                    )));
                }
                let row_id = storage
                    .next_row_id()
                    .map_err(|e| map_storage_error(table, e))?;
                storage
                    .insert(row_id, &row)
                    .map_err(|e| map_storage_error(table, e))?;
                staged.push((row_id, row));
            }
        }
    }

    populate_indexes(txn, &btree_indexes, &staged)?;
    populate_hnsw_indexes(txn, table, &hnsw_indexes, &staged)?;

    Ok(staged.len() as u64)
}

/// Columnar ストレージへの書き込み（現状は Row と同経路で処理）。
fn bulk_load_columnar<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &C,
    table: &TableMetadata,
    reader: Box<dyn BulkReader>,
) -> Result<u64> {
    // Columnar エンジン未実装のため、Row と同じ処理で取り込む。
    bulk_load_row(txn, catalog, table, reader)
}

/// テキストをテーブル型に合わせて `SqlValue` へ変換する。
pub(crate) fn parse_value(raw: &str, ty: &ResolvedType) -> Result<SqlValue> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(SqlValue::Null);
    }

    match ty {
        ResolvedType::Integer => trimmed
            .parse::<i32>()
            .map(SqlValue::Integer)
            .map_err(|e| parse_error(trimmed, ty, e)),
        ResolvedType::BigInt => trimmed
            .parse::<i64>()
            .map(SqlValue::BigInt)
            .map_err(|e| parse_error(trimmed, ty, e)),
        ResolvedType::Float => trimmed
            .parse::<f32>()
            .map(SqlValue::Float)
            .map_err(|e| parse_error(trimmed, ty, e)),
        ResolvedType::Double => trimmed
            .parse::<f64>()
            .map(SqlValue::Double)
            .map_err(|e| parse_error(trimmed, ty, e)),
        ResolvedType::Boolean => {
            let parsed = trimmed
                .parse::<bool>()
                .or_else(|_| match trimmed {
                    "1" => Ok(true),
                    "0" => Ok(false),
                    _ => Err(()),
                })
                .map_err(|_| {
                    ExecutorError::BulkLoad(format!(
                        "failed to parse value '{trimmed}' as {}: invalid boolean",
                        ty.type_name()
                    ))
                })?;
            Ok(SqlValue::Boolean(parsed))
        }
        ResolvedType::Timestamp => trimmed
            .parse::<i64>()
            .map(SqlValue::Timestamp)
            .map_err(|e| parse_error(trimmed, ty, e)),
        ResolvedType::Text => Ok(SqlValue::Text(trimmed.to_string())),
        ResolvedType::Blob => Ok(SqlValue::Blob(trimmed.as_bytes().to_vec())),
        ResolvedType::Vector { dimension, .. } => {
            let body = trimmed.trim_matches(['[', ']']);
            if body.is_empty() {
                return Err(ExecutorError::BulkLoad(
                    "vector literal cannot be empty".into(),
                ));
            }
            let mut values = Vec::new();
            for part in body.split(',') {
                let v = part
                    .trim()
                    .parse::<f32>()
                    .map_err(|e| ExecutorError::BulkLoad(format!("invalid vector value: {e}")))?;
                values.push(v);
            }
            if values.len() as u32 != *dimension {
                return Err(ExecutorError::BulkLoad(format!(
                    "vector dimension mismatch: expected {}, got {}",
                    dimension,
                    values.len()
                )));
            }
            Ok(SqlValue::Vector(values))
        }
        ResolvedType::Null => Ok(SqlValue::Null),
    }
}

fn parse_error(trimmed: &str, ty: &ResolvedType, err: impl std::fmt::Display) -> ExecutorError {
    ExecutorError::BulkLoad(format!(
        "failed to parse value '{trimmed}' as {}: {err}",
        ty.type_name()
    ))
}

fn is_type_compatible(file_type: &ResolvedType, table_type: &ResolvedType) -> bool {
    match (file_type, table_type) {
        (
            ResolvedType::Vector {
                dimension: f_dim,
                metric: f_metric,
            },
            ResolvedType::Vector {
                dimension: t_dim,
                metric: t_metric,
            },
        ) => f_dim == t_dim && f_metric == t_metric,
        (ft, tt) => ft == tt || ft.can_cast_to(tt),
    }
}

fn map_storage_error(table: &TableMetadata, err: StorageError) -> ExecutorError {
    match err {
        StorageError::NullConstraintViolation { column } => {
            ExecutorError::ConstraintViolation(crate::executor::ConstraintViolation::NotNull {
                column,
            })
        }
        StorageError::PrimaryKeyViolation { .. } => {
            ExecutorError::ConstraintViolation(crate::executor::ConstraintViolation::PrimaryKey {
                columns: table.primary_key.clone().unwrap_or_default(),
                value: None,
            })
        }
        StorageError::TransactionConflict => ExecutorError::TransactionConflict,
        other => ExecutorError::Storage(other),
    }
}

fn map_index_error(index: &IndexMetadata, err: StorageError) -> ExecutorError {
    match err {
        StorageError::UniqueViolation { .. } => {
            if index.name.starts_with("__pk_") {
                ExecutorError::ConstraintViolation(
                    crate::executor::ConstraintViolation::PrimaryKey {
                        columns: index.columns.clone(),
                        value: None,
                    },
                )
            } else {
                ExecutorError::ConstraintViolation(crate::executor::ConstraintViolation::Unique {
                    index_name: index.name.clone(),
                    columns: index.columns.clone(),
                    value: None,
                })
            }
        }
        StorageError::NullConstraintViolation { column } => {
            ExecutorError::ConstraintViolation(crate::executor::ConstraintViolation::NotNull {
                column,
            })
        }
        StorageError::TransactionConflict => ExecutorError::TransactionConflict,
        other => ExecutorError::Storage(other),
    }
}

fn populate_indexes<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    indexes: &[IndexMetadata],
    rows: &[(u64, Vec<SqlValue>)],
) -> Result<()> {
    for index in indexes {
        let mut storage =
            txn.index_storage(index.index_id, index.unique, index.column_indices.clone());
        for (row_id, row) in rows {
            if should_skip_unique_index_for_null(index, row) {
                continue;
            }
            storage
                .insert(row, *row_id)
                .map_err(|e| map_index_error(index, e))?;
        }
    }
    Ok(())
}

fn populate_hnsw_indexes<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    table: &TableMetadata,
    indexes: &[IndexMetadata],
    rows: &[(u64, Vec<SqlValue>)],
) -> Result<()> {
    for index in indexes {
        for (row_id, row) in rows {
            HnswBridge::on_insert(txn, table, index, *row_id, row)?;
        }
    }
    Ok(())
}

fn should_skip_unique_index_for_null(index: &IndexMetadata, row: &[SqlValue]) -> bool {
    index.unique
        && index
            .column_indices
            .iter()
            .any(|&idx| row.get(idx).is_none_or(SqlValue::is_null))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, MemoryCatalog, StorageType};
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use ::parquet::arrow::ArrowWriter;
    use alopex_core::kv::memory::MemoryKV;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;

    fn bridge() -> (TxnBridge<MemoryKV>, MemoryCatalog) {
        (
            TxnBridge::new(Arc::new(MemoryKV::new())),
            MemoryCatalog::new(),
        )
    }

    fn create_table(
        bridge: &TxnBridge<MemoryKV>,
        catalog: &mut MemoryCatalog,
        storage: StorageType,
    ) {
        let mut table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".into()]);
        table.storage_options.storage_type = storage;

        let mut txn = bridge.begin_write().unwrap();
        execute_create_table(&mut txn, catalog, table, vec![], false).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn validate_file_path_rejects_symlink_and_directory() {
        let dir = std::env::temp_dir();
        let dir_path = dir.join("alopex_copy_dir");
        std::fs::create_dir_all(&dir_path).unwrap();

        let config = CopySecurityConfig {
            allowed_base_dirs: Some(vec![dir.clone()]),
            allow_symlinks: false,
        };

        // Directory is rejected.
        let err = validate_file_path(dir_path.to_str().unwrap(), &config).unwrap_err();
        assert!(matches!(err, ExecutorError::PathValidationFailed { .. }));

        // Symlink is rejected on unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let file_path = dir.join("alopex_copy_file.txt");
            fs::write(&file_path, "1,alice\n").unwrap();
            let link = dir.join("alopex_copy_link.txt");
            let _ = fs::remove_file(&link);
            symlink(&file_path, &link).unwrap();
            let err = validate_file_path(link.to_str().unwrap(), &config).unwrap_err();
            assert!(matches!(err, ExecutorError::PathValidationFailed { .. }));
        }
    }

    #[test]
    fn validate_schema_checks_names_and_types() {
        let (bridge, mut catalog) = bridge();
        create_table(&bridge, &mut catalog, StorageType::Row);
        let table = catalog.get_table("users").unwrap();

        let schema = CopySchema {
            fields: vec![
                CopyField {
                    name: Some("users".into()),
                    data_type: Some(ResolvedType::Integer),
                },
                CopyField {
                    name: Some("name".into()),
                    data_type: Some(ResolvedType::Text),
                },
            ],
        };

        let err = validate_schema(&schema, table).unwrap_err();
        assert!(matches!(err, ExecutorError::SchemaMismatch { .. }));
    }

    #[test]
    fn execute_copy_csv_inserts_rows() {
        let dir = std::env::temp_dir();
        let file_path = dir.join("alopex_copy_test.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "id,name").unwrap();
        writeln!(file, "1,alice").unwrap();
        writeln!(file, "2,bob").unwrap();

        let (bridge, mut catalog) = bridge();
        create_table(&bridge, &mut catalog, StorageType::Row);

        let mut txn = bridge.begin_write().unwrap();
        let result = execute_copy(
            &mut txn,
            &catalog,
            "users",
            file_path.to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        txn.commit().unwrap();
        assert_eq!(result, ExecutionResult::RowsAffected(2));

        // Verify rows inserted.
        let table = catalog.get_table("users").unwrap().clone();
        let mut read_txn = bridge.begin_read().unwrap();
        let mut storage = read_txn.table_storage(&table);
        let rows: Vec<_> = storage.scan().unwrap().map(|r| r.unwrap().1).collect();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![SqlValue::Integer(1), SqlValue::Text("alice".into())]));
    }

    #[test]
    fn execute_copy_parquet_reads_schema_and_rows() {
        let dir = std::env::temp_dir();
        let file_path = dir.join("alopex_copy_test.parquet");
        write_parquet_sample(&file_path, 2);

        let (bridge, mut catalog) = bridge();
        create_table(&bridge, &mut catalog, StorageType::Row);

        let mut txn = bridge.begin_write().unwrap();
        let result = execute_copy(
            &mut txn,
            &catalog,
            "users",
            file_path.to_str().unwrap(),
            FileFormat::Parquet,
            CopyOptions::default(),
            &CopySecurityConfig::default(),
        )
        .unwrap();
        txn.commit().unwrap();
        assert_eq!(result, ExecutionResult::RowsAffected(2));

        // スキーマは Parquet から取得するため、テーブル側と不一致なら validate_schema が弾く。
        let table = catalog.get_table("users").unwrap().clone();
        let mut read_txn = bridge.begin_read().unwrap();
        let mut storage = read_txn.table_storage(&table);
        let rows: Vec<_> = storage.scan().unwrap().map(|r| r.unwrap().1).collect();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![SqlValue::Integer(1), SqlValue::Text("user0".into())]));
    }

    #[test]
    fn parquet_reader_streams_batches() {
        let dir = std::env::temp_dir();
        let file_path = dir.join("alopex_copy_stream.parquet");
        write_parquet_sample(&file_path, 1500);

        let (bridge, mut catalog) = bridge();
        create_table(&bridge, &mut catalog, StorageType::Row);
        let table = catalog.get_table("users").unwrap().clone();

        let mut reader = ParquetReader::open(file_path.to_str().unwrap(), &table, false).unwrap();
        let mut batches = 0;
        let mut total = 0;
        while let Some(batch) = reader.next_batch(512).unwrap() {
            total += batch.len();
            batches += 1;
        }
        assert!(
            batches >= 2,
            "複数バッチを期待しましたが {batches} バッチでした"
        );
        assert_eq!(total, 1500);
    }

    fn write_parquet_sample(path: &Path, count: usize) {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, false),
        ]));

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        let chunk_size = 700;
        let mut start = 0;
        while start < count {
            let end = (start + chunk_size).min(count);
            let ids: Vec<i32> = ((start + 1) as i32..=end as i32).collect();
            let names: Vec<String> = (start..end).map(|i| format!("user{i}")).collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids)) as Arc<_>,
                    Arc::new(StringArray::from(names)) as Arc<_>,
                ],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            start = end;
        }

        writer.close().unwrap();
    }
}
