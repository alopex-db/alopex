use alopex_core::Error as CoreError;
use alopex_core::kv::KVStore;
use alopex_core::vector::hnsw::{HnswConfig, HnswIndex};
use alopex_core::vector::{Metric, validate_dimensions};

use crate::ast::ddl::VectorMetric;
use crate::catalog::{ColumnMetadata, IndexMetadata, TableMetadata};
use crate::executor::{ExecutorError, Result};
use crate::planner::types::ResolvedType;
use crate::storage::{SqlTxn, SqlValue};

/// SQL と HNSW の橋渡しを行うユーティリティ。
pub struct HnswBridge;

impl HnswBridge {
    /// HNSW インデックスを作成し、既存行を取り込む。
    pub fn create_index<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
        txn: &mut T,
        table: &TableMetadata,
        index: &IndexMetadata,
    ) -> Result<()> {
        txn.ensure_write_txn().map_err(ExecutorError::from)?;
        let (column, col_idx) = vector_column(table, index)?;
        let config = build_config(index, column)?;
        config.validate().map_err(ExecutorError::from)?;

        let mut hnsw = HnswIndex::create(&index.name, config).map_err(ExecutorError::from)?;

        {
            let mut storage = txn.table_storage(table);
            for entry in storage.range_scan(0, u64::MAX)? {
                let (row_id, row) = entry.map_err(ExecutorError::Storage)?;
                let vector = required_vector(&row_id, &table.name, column, &row[col_idx])?;
                hnsw.upsert(&row_id.to_be_bytes(), &vector, &[])
                    .map_err(ExecutorError::from)?;
            }
        }

        hnsw.save(txn.inner_mut()).map_err(ExecutorError::from)
    }

    /// HNSW インデックスを削除する。
    pub fn drop_index<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
        txn: &mut T,
        index: &IndexMetadata,
        if_exists: bool,
    ) -> Result<()> {
        txn.ensure_write_txn().map_err(ExecutorError::from)?;
        match HnswIndex::load(&index.name, txn.inner_mut()) {
            Ok(index) => index.drop(txn.inner_mut()).map_err(ExecutorError::from),
            Err(CoreError::IndexNotFound { .. }) if if_exists => Ok(()),
            Err(err) => Err(ExecutorError::from(err)),
        }
    }

    /// INSERT/UPSERT 時のインデックス更新。
    pub fn on_insert<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
        txn: &mut T,
        table: &TableMetadata,
        index: &IndexMetadata,
        row_id: u64,
        row: &[SqlValue],
    ) -> Result<()> {
        txn.ensure_write_txn().map_err(ExecutorError::from)?;
        let (column, col_idx) = vector_column(table, index)?;
        let vector = required_vector(&row_id, &table.name, column, &row[col_idx])?;
        let entry = txn
            .hnsw_entry_mut(&index.name)
            .map_err(ExecutorError::from)?;
        entry
            .index
            .upsert_staged(&row_id.to_be_bytes(), &vector, &[], &mut entry.state)
            .map_err(ExecutorError::from)?;
        entry.dirty = true;
        Ok(())
    }

    /// DELETE 時のインデックス更新。
    pub fn on_delete<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
        txn: &mut T,
        index: &IndexMetadata,
        row_id: u64,
    ) -> Result<()> {
        txn.ensure_write_txn().map_err(ExecutorError::from)?;
        let entry = txn
            .hnsw_entry_mut(&index.name)
            .map_err(ExecutorError::from)?;
        entry
            .index
            .delete_staged(&row_id.to_be_bytes(), &mut entry.state)
            .map_err(ExecutorError::from)?;
        entry.dirty = true;
        Ok(())
    }

    /// UPDATE 時のインデックス更新。
    pub fn on_update<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
        txn: &mut T,
        table: &TableMetadata,
        index: &IndexMetadata,
        row_id: u64,
        old_row: &[SqlValue],
        new_row: &[SqlValue],
    ) -> Result<()> {
        txn.ensure_write_txn().map_err(ExecutorError::from)?;
        let (column, col_idx) = vector_column(table, index)?;
        if old_row
            .get(col_idx)
            .zip(new_row.get(col_idx))
            .is_some_and(|(old, new)| old == new)
        {
            return Ok(());
        }

        let vector = required_vector(&row_id, &table.name, column, &new_row[col_idx])?;
        let entry = txn
            .hnsw_entry_mut(&index.name)
            .map_err(ExecutorError::from)?;
        entry
            .index
            .upsert_staged(&row_id.to_be_bytes(), &vector, &[], &mut entry.state)
            .map_err(ExecutorError::from)?;
        entry.dirty = true;
        Ok(())
    }

    /// kNN 検索を実行する。
    #[allow(dead_code)]
    pub fn search_knn<'txn, S: KVStore + 'txn>(
        txn: &mut impl SqlTxn<'txn, S>,
        index_name: &str,
        query: &[f32],
        k: usize,
        ef_search: Option<usize>,
    ) -> Result<Vec<(u64, f32)>> {
        let index = txn.hnsw_entry(index_name).map_err(ExecutorError::from)?;
        let (results, _) = index
            .search(query, k, ef_search)
            .map_err(ExecutorError::from)?;
        results
            .into_iter()
            .map(|res| {
                let key: [u8; 8] =
                    res.key
                        .as_slice()
                        .try_into()
                        .map_err(|_| ExecutorError::InvalidOperation {
                            operation: "HNSW search".into(),
                            reason: format!("RowID フォーマットが不正です: {:?}", res.key),
                        })?;
                Ok((u64::from_be_bytes(key), res.distance))
            })
            .collect()
    }

    /// HNSW インデックスの存在確認。
    #[allow(dead_code)]
    pub fn index_exists<'txn, S: KVStore + 'txn>(
        txn: &mut impl SqlTxn<'txn, S>,
        index_name: &str,
    ) -> Result<bool> {
        match txn.hnsw_entry(index_name) {
            Ok(_) => Ok(true),
            Err(CoreError::IndexNotFound { .. }) => Ok(false),
            Err(err) => Err(ExecutorError::from(err)),
        }
    }
}

fn metric_from_vector(metric: &VectorMetric) -> Metric {
    match metric {
        VectorMetric::Cosine => Metric::Cosine,
        VectorMetric::L2 => Metric::L2,
        VectorMetric::Inner => Metric::InnerProduct,
    }
}

fn build_config(index: &IndexMetadata, column: &ColumnMetadata) -> Result<HnswConfig> {
    let (dimension, metric) = match &column.data_type {
        ResolvedType::Vector { dimension, metric } => {
            (*dimension as usize, metric_from_vector(metric))
        }
        _ => {
            return Err(ExecutorError::Core(CoreError::InvalidColumnType {
                column: column.name.clone(),
                expected: "VECTOR".into(),
            }));
        }
    };

    let mut config = HnswConfig::default()
        .with_dimension(dimension)
        .with_metric(metric);

    for (key, value) in &index.options {
        match key.to_ascii_lowercase().as_str() {
            "m" => {
                let parsed: usize = value.parse().map_err(|_| {
                    ExecutorError::Core(CoreError::InvalidParameter {
                        param: "m".into(),
                        reason: format!("整数値に変換できません: {value}"),
                    })
                })?;
                config.m = parsed;
            }
            "ef_construction" => {
                let parsed: usize = value.parse().map_err(|_| {
                    ExecutorError::Core(CoreError::InvalidParameter {
                        param: "ef_construction".into(),
                        reason: format!("整数値に変換できません: {value}"),
                    })
                })?;
                config.ef_construction = parsed;
            }
            other => {
                return Err(ExecutorError::Core(CoreError::UnknownOption {
                    key: other.to_string(),
                }));
            }
        }
    }

    Ok(config)
}

fn extract_vector(value: &SqlValue, column: &ColumnMetadata) -> Result<Option<Vec<f32>>> {
    match value {
        SqlValue::Vector(v) => {
            validate_dimensions(
                match &column.data_type {
                    ResolvedType::Vector { dimension, .. } => *dimension as usize,
                    _ => 0,
                },
                v.len(),
            )
            .map_err(ExecutorError::from)?;
            Ok(Some(v.clone()))
        }
        SqlValue::Null => Ok(None),
        other => Err(ExecutorError::Core(CoreError::InvalidColumnType {
            column: column.name.clone(),
            expected: format!("VECTOR (got {})", other.type_name()),
        })),
    }
}

fn required_vector(
    row_id: &u64,
    table: &str,
    column: &ColumnMetadata,
    value: &SqlValue,
) -> Result<Vec<f32>> {
    match extract_vector(value, column)? {
        Some(vec) => Ok(vec),
        None => Err(ExecutorError::InvalidOperation {
            operation: "HNSW index".into(),
            reason: format!(
                "テーブル {table} の HNSW インデックス対象カラム {} に NULL が含まれています (RowID={row_id})",
                column.name
            ),
        }),
    }
}

fn vector_column<'a>(
    table: &'a TableMetadata,
    index: &IndexMetadata,
) -> Result<(&'a ColumnMetadata, usize)> {
    if index.column_indices.len() != 1 {
        return Err(ExecutorError::InvalidOperation {
            operation: "HNSW index".into(),
            reason: "HNSW インデックスは単一の VECTOR カラムのみサポートします".into(),
        });
    }
    let col_idx = index.column_indices[0];
    let column = table.columns.get(col_idx).ok_or_else(|| {
        ExecutorError::ColumnNotFound(index.columns.first().cloned().unwrap_or_default())
    })?;
    if let ResolvedType::Vector { .. } = column.data_type {
        Ok((column, col_idx))
    } else {
        Err(ExecutorError::Core(CoreError::InvalidColumnType {
            column: column.name.clone(),
            expected: "VECTOR".into(),
        }))
    }
}
