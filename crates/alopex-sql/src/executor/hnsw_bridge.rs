use alopex_core::Error as CoreError;
use alopex_core::kv::KVStore;
use alopex_core::vector::hnsw::{HnswConfig, HnswIndex, SearchStats};
use alopex_core::vector::{Metric, validate_dimensions};

use crate::ast::ddl::VectorMetric;
use crate::catalog::{ColumnMetadata, IndexMetadata, TableMetadata};
use crate::executor::{ExecutorError, Result};
use crate::planner::types::ResolvedType;
use crate::storage::{SqlTxn, SqlValue};

/// SQL と HNSW の橋渡しを行うユーティリティ。
pub struct HnswBridge;

impl HnswBridge {
    /// Returns the index-default search breadth, if configured.
    pub(crate) fn search_ef(index: &IndexMetadata) -> Result<Option<usize>> {
        index
            .options
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("ef_search"))
            .map(|(_, value)| parse_ef_search(value))
            .transpose()
    }

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

        let entries = {
            let mut storage = txn.table_storage(table);
            let mut entries = Vec::new();
            for entry in storage.range_scan(0, u64::MAX)? {
                let (row_id, row) = entry.map_err(ExecutorError::Storage)?;
                if let Some(vector) = extract_vector(&row[col_idx], column)? {
                    entries.push((row_id.to_be_bytes().to_vec(), vector, Vec::new()));
                }
            }
            entries
        };
        let entries: Vec<_> = entries
            .iter()
            .map(|(key, vector, metadata)| (key.as_slice(), vector.as_slice(), metadata.as_slice()))
            .collect();
        hnsw.upsert_batch(&entries).map_err(ExecutorError::from)?;

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

    /// Batch INSERT/UPSERT index update. Validation happens before the graph changes.
    pub fn on_insert_batch<'txn, S: KVStore + 'txn, T: SqlTxn<'txn, S>>(
        txn: &mut T,
        table: &TableMetadata,
        index: &IndexMetadata,
        rows: &[(u64, Vec<SqlValue>)],
    ) -> Result<()> {
        txn.ensure_write_txn().map_err(ExecutorError::from)?;
        let (column, col_idx) = vector_column(table, index)?;
        let mut owned_entries = Vec::with_capacity(rows.len());
        for (row_id, row) in rows {
            if let Some(vector) = extract_vector(&row[col_idx], column)? {
                owned_entries.push((row_id.to_be_bytes().to_vec(), vector, Vec::new()));
            }
        }
        let entries: Vec<_> = owned_entries
            .iter()
            .map(|(key, vector, metadata)| (key.as_slice(), vector.as_slice(), metadata.as_slice()))
            .collect();
        let entry = txn
            .hnsw_entry_mut(&index.name)
            .map_err(ExecutorError::from)?;
        entry
            .index
            .upsert_staged_batch(&entries, &mut entry.state)
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

        let vector = extract_vector(&new_row[col_idx], column)?;
        let entry = txn
            .hnsw_entry_mut(&index.name)
            .map_err(ExecutorError::from)?;
        match vector {
            Some(vector) => entry
                .index
                .upsert_staged(&row_id.to_be_bytes(), &vector, &[], &mut entry.state)
                .map_err(ExecutorError::from)?,
            None => {
                entry
                    .index
                    .delete_staged(&row_id.to_be_bytes(), &mut entry.state)
                    .map_err(ExecutorError::from)?;
            }
        }
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
    ) -> Result<(Vec<(u64, f32)>, SearchStats)> {
        let index = txn.hnsw_entry(index_name).map_err(ExecutorError::from)?;
        let (results, stats) = index
            .search(query, k, ef_search)
            .map_err(ExecutorError::from)?;
        let results =
            results
                .into_iter()
                .map(|res| {
                    let key: [u8; 8] = res.key.as_slice().try_into().map_err(|_| {
                        ExecutorError::InvalidOperation {
                            operation: "HNSW search".into(),
                            reason: format!("RowID フォーマットが不正です: {:?}", res.key),
                        }
                    })?;
                    Ok((u64::from_be_bytes(key), res.distance))
                })
                .collect::<Result<Vec<_>>>()?;
        Ok((results, stats))
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
            "ef_search" => {
                parse_ef_search(value)?;
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

fn parse_ef_search(value: &str) -> Result<usize> {
    let parsed: usize = value.parse().map_err(|_| {
        ExecutorError::Core(CoreError::InvalidParameter {
            param: "ef_search".into(),
            reason: format!("整数値に変換できません: {value}"),
        })
    })?;
    if parsed == 0 {
        return Err(ExecutorError::Core(CoreError::InvalidParameter {
            param: "ef_search".into(),
            reason: "must be greater than zero".into(),
        }));
    }
    Ok(parsed)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_ef_search_is_parsed_and_validated() {
        let index = IndexMetadata::new(1, "idx", "items", vec!["embedding".into()])
            .with_option("EF_SEARCH", "256");
        assert_eq!(HnswBridge::search_ef(&index).unwrap(), Some(256));

        let invalid = IndexMetadata::new(1, "idx", "items", vec!["embedding".into()])
            .with_option("ef_search", "0");
        assert!(HnswBridge::search_ef(&invalid).is_err());
    }

    #[test]
    fn null_vector_is_skipped() {
        let column = ColumnMetadata::new(
            "embedding",
            ResolvedType::Vector {
                dimension: 2,
                metric: VectorMetric::L2,
            },
        );
        assert_eq!(extract_vector(&SqlValue::Null, &column).unwrap(), None);
    }
}
