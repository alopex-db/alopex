//! COPY / Bulk Load 実装。
//!
//! 現段階では CSV/Parquet を簡易的に読み込み、テーブルスキーマに従って
//! `SqlValue` へ変換する。Columnar ストレージも Row ストレージと同じ経路で
//! 取り込み、将来の columnar エンジン実装で差し替え可能な構造にしている。

use std::fs;
use std::path::{Path, PathBuf};

use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::encoding_v2::Bitmap;
use alopex_core::columnar::kvs_bridge::key_layout;
use alopex_core::columnar::segment_v2::{
    ColumnSchema, ColumnSegmentV2, RecordBatch, Schema, SegmentConfigV2, SegmentWriterV2,
};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::storage::compression::CompressionV2;
use alopex_core::storage::format::bincode_config;
use bincode::config::Options;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{
    Catalog, ColumnMetadata, Compression, IndexMetadata, RowIdMode, TableMetadata,
};
use crate::columnar::statistics::compute_row_group_statistics;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CopyOptions {
    /// CSV ヘッダ行の有無。
    pub header: bool,
}

/// COPY セキュリティ設定。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CopySecurityConfig {
    /// 許可するベースディレクトリ一覧（None なら無制限）。
    pub allowed_base_dirs: Option<Vec<PathBuf>>,
    /// シンボリックリンクを許可するか。
    pub allow_symlinks: bool,
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
        if let Some(dt) = &field.data_type
            && !is_type_compatible(dt, &col.data_type)
        {
            return Err(ExecutorError::SchemaMismatch {
                expected: table_meta.columns.len(),
                actual: schema.fields.len(),
                reason: format!(
                    "type mismatch for column '{}': expected {:?}, got {:?}",
                    col.name, col.data_type, dt
                ),
            });
        }
        if let Some(name) = &field.name
            && name != &col.name
        {
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
    mut reader: Box<dyn BulkReader>,
) -> Result<u64> {
    let _ = catalog; // reserved for future index integration

    let row_group_size = table.storage_options.row_group_size.max(1) as usize;
    let compression = map_compression(table.storage_options.compression);
    let mut writer = SegmentWriterV2::new(SegmentConfigV2 {
        row_group_size: row_group_size as u64,
        compression,
        ..Default::default()
    });
    let schema = build_segment_schema(table)?;

    let mut row_group_stats = Vec::new();
    let mut total_rows = 0u64;
    while let Some(batch) = reader.next_batch(row_group_size)? {
        if batch.is_empty() {
            continue;
        }
        let stats = compute_row_group_statistics(&batch);
        let record_batch = build_record_batch(&schema, table, &batch)?;
        writer
            .write_batch(record_batch)
            .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
        row_group_stats.push(stats);
        total_rows += batch.len() as u64;
    }

    if total_rows == 0 {
        return Ok(0);
    }

    let segment = writer
        .finish()
        .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
    let _segment_id = persist_segment(txn, table, segment, &row_group_stats)?;

    Ok(total_rows)
}

fn map_compression(compression: Compression) -> CompressionV2 {
    let desired = match compression {
        Compression::None => CompressionV2::None,
        Compression::Lz4 => CompressionV2::Lz4,
        Compression::Zstd => CompressionV2::Zstd { level: 3 },
    };

    if desired.is_available() {
        desired
    } else {
        CompressionV2::None
    }
}

fn build_segment_schema(table: &TableMetadata) -> Result<Schema> {
    let mut columns = Vec::with_capacity(table.column_count());
    for col in &table.columns {
        let logical_type = logical_type_for(&col.data_type)?;
        columns.push(ColumnSchema {
            name: col.name.clone(),
            logical_type,
            nullable: !col.not_null,
            fixed_len: fixed_len_for(&col.data_type),
        });
    }
    Ok(Schema { columns })
}

fn logical_type_for(ty: &ResolvedType) -> Result<LogicalType> {
    match ty {
        ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Timestamp => {
            Ok(LogicalType::Int64)
        }
        ResolvedType::Vector { dimension, .. } => {
            Ok(LogicalType::Fixed(dimension.checked_mul(4).ok_or_else(|| {
                ExecutorError::Columnar("vector dimension overflow when computing fixed len".into())
            })? as u16))
        }
        ResolvedType::Float => Ok(LogicalType::Float32),
        ResolvedType::Double => Ok(LogicalType::Float64),
        ResolvedType::Boolean => Ok(LogicalType::Bool),
        ResolvedType::Text | ResolvedType::Blob => Ok(LogicalType::Binary),
        ResolvedType::Null => Err(ExecutorError::Columnar(
            "NULL column type is not supported for columnar storage".into(),
        )),
    }
}

fn fixed_len_for(ty: &ResolvedType) -> Option<u32> {
    match ty {
        ResolvedType::Vector { dimension, .. } => Some(dimension.saturating_mul(4)),
        _ => None,
    }
}

fn build_record_batch(
    schema: &Schema,
    table: &TableMetadata,
    rows: &[Vec<SqlValue>],
) -> Result<RecordBatch> {
    for row in rows {
        if row.len() != table.column_count() {
            return Err(ExecutorError::BulkLoad(format!(
                "row has {} columns, expected {}",
                row.len(),
                table.column_count()
            )));
        }
    }

    let mut columns = Vec::with_capacity(table.column_count());
    let mut bitmaps = Vec::with_capacity(table.column_count());
    for (idx, col_meta) in table.columns.iter().enumerate() {
        let (col, bitmap) = build_column(idx, col_meta, rows)?;
        columns.push(col);
        bitmaps.push(bitmap);
    }

    Ok(RecordBatch::new(schema.clone(), columns, bitmaps))
}

fn validity_bitmap(validity: &[bool]) -> Option<Bitmap> {
    if validity.iter().all(|v| *v) {
        None
    } else {
        Some(Bitmap::from_bools(validity))
    }
}

fn build_column(
    col_idx: usize,
    col_meta: &ColumnMetadata,
    rows: &[Vec<SqlValue>],
) -> Result<(Column, Option<Bitmap>)> {
    match &col_meta.data_type {
        ResolvedType::Integer => {
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(0);
                    }
                    SqlValue::Integer(v) => {
                        validity.push(true);
                        values.push(*v as i64);
                    }
                    SqlValue::BigInt(v) => {
                        validity.push(true);
                        values.push(*v);
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected Integer, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((Column::Int64(values), validity_bitmap(&validity)))
        }
        ResolvedType::BigInt | ResolvedType::Timestamp => {
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(0);
                    }
                    SqlValue::BigInt(v) | SqlValue::Timestamp(v) => {
                        validity.push(true);
                        values.push(*v);
                    }
                    SqlValue::Integer(v) => {
                        validity.push(true);
                        values.push(*v as i64);
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected BigInt/Timestamp, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((Column::Int64(values), validity_bitmap(&validity)))
        }
        ResolvedType::Float => {
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(0.0);
                    }
                    SqlValue::Float(v) => {
                        validity.push(true);
                        values.push(*v);
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected Float, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((Column::Float32(values), validity_bitmap(&validity)))
        }
        ResolvedType::Double => {
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(0.0);
                    }
                    SqlValue::Double(v) => {
                        validity.push(true);
                        values.push(*v);
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected Double, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((Column::Float64(values), validity_bitmap(&validity)))
        }
        ResolvedType::Boolean => {
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(false);
                    }
                    SqlValue::Boolean(v) => {
                        validity.push(true);
                        values.push(*v);
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected Boolean, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((Column::Bool(values), validity_bitmap(&validity)))
        }
        ResolvedType::Text => {
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(Vec::new());
                    }
                    SqlValue::Text(v) => {
                        validity.push(true);
                        values.push(v.as_bytes().to_vec());
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected Text, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((Column::Binary(values), validity_bitmap(&validity)))
        }
        ResolvedType::Blob => {
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(Vec::new());
                    }
                    SqlValue::Blob(v) => {
                        validity.push(true);
                        values.push(v.clone());
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected Blob, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((Column::Binary(values), validity_bitmap(&validity)))
        }
        ResolvedType::Vector { dimension, .. } => {
            let fixed_len = dimension.saturating_mul(4) as usize;
            let mut validity = Vec::with_capacity(rows.len());
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .ok_or_else(|| ExecutorError::BulkLoad("row too short".into()))?
                {
                    SqlValue::Null => {
                        validity.push(false);
                        values.push(vec![0u8; fixed_len]);
                    }
                    SqlValue::Vector(v) => {
                        if v.len() as u32 != *dimension {
                            return Err(ExecutorError::BulkLoad(format!(
                                "vector dimension mismatch for column '{}': expected {}, got {}",
                                col_meta.name,
                                dimension,
                                v.len()
                            )));
                        }
                        validity.push(true);
                        let mut buf = Vec::with_capacity(fixed_len);
                        for f in v {
                            buf.extend_from_slice(&f.to_le_bytes());
                        }
                        values.push(buf);
                    }
                    other => {
                        return Err(ExecutorError::BulkLoad(format!(
                            "type mismatch for column '{}': expected Vector, got {}",
                            col_meta.name,
                            other.type_name()
                        )));
                    }
                }
            }
            Ok((
                Column::Fixed {
                    len: fixed_len,
                    values,
                },
                validity_bitmap(&validity),
            ))
        }
        ResolvedType::Null => Err(ExecutorError::Columnar(
            "NULL column type is not supported for columnar storage".into(),
        )),
    }
}

fn persist_segment<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    table: &TableMetadata,
    mut segment: ColumnSegmentV2,
    row_group_stats: &[crate::columnar::statistics::RowGroupStatistics],
) -> Result<u64> {
    if row_group_stats.len() != segment.meta.row_groups.len() {
        return Err(ExecutorError::Columnar(
            "row group statistics length mismatch".into(),
        ));
    }

    let table_id = table.table_id;
    let index_key = key_layout::segment_index_key(table_id);
    let existing = txn.inner_mut().get(&index_key)?;
    let mut index: Vec<u64> = if let Some(bytes) = existing {
        bincode_config()
            .deserialize(&bytes)
            .map_err(|e| ExecutorError::Columnar(e.to_string()))?
    } else {
        Vec::new()
    };
    let segment_id = index
        .last()
        .copied()
        .map(|id| id.saturating_add(1))
        .unwrap_or(0);

    let mut row_group_stats = row_group_stats.to_vec();
    if table.storage_options.row_id_mode == RowIdMode::Direct {
        let total_rows = usize::try_from(segment.meta.num_rows)
            .map_err(|_| ExecutorError::Columnar("segment row count exceeds usize::MAX".into()))?;
        segment.row_ids = (0..total_rows)
            .map(|idx| {
                alopex_core::columnar::segment_v2::encode_row_id(segment_id, idx as u64)
                    .map_err(|e| ExecutorError::Columnar(e.to_string()))
            })
            .collect::<Result<Vec<u64>>>()?;

        for (idx, meta) in segment.meta.row_groups.iter().enumerate() {
            let start = usize::try_from(meta.row_start)
                .map_err(|_| ExecutorError::Columnar("row_start exceeds usize::MAX".into()))?;
            let count = usize::try_from(meta.row_count)
                .map_err(|_| ExecutorError::Columnar("row_count exceeds usize::MAX".into()))?;
            if count == 0 {
                continue;
            }
            let end = start
                .checked_add(count)
                .ok_or_else(|| ExecutorError::Columnar("row_id range overflow".into()))?;
            if end > segment.row_ids.len() {
                return Err(ExecutorError::Columnar(
                    "row_ids length is smaller than row_group range".into(),
                ));
            }
            row_group_stats[idx].row_id_min = segment.row_ids.get(start).copied();
            row_group_stats[idx].row_id_max = segment.row_ids.get(end - 1).copied();
        }
    } else {
        segment.row_ids.clear();
    }

    let segment_bytes = bincode_config()
        .serialize(&segment)
        .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
    txn.inner_mut().put(
        key_layout::column_segment_key(table_id, segment_id, 0),
        segment_bytes,
    )?;

    let meta_bytes = bincode_config()
        .serialize(&segment.meta)
        .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
    txn.inner_mut()
        .put(key_layout::statistics_key(table_id, segment_id), meta_bytes)?;

    let rg_bytes = bincode_config()
        .serialize(&row_group_stats)
        .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
    txn.inner_mut().put(
        key_layout::row_group_stats_key(table_id, segment_id),
        rg_bytes,
    )?;

    index.push(segment_id);
    let index_bytes = bincode_config()
        .serialize(&index)
        .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
    txn.inner_mut().put(index_key, index_bytes)?;
    Ok(segment_id)
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
                .or(match trimmed {
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
