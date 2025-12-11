use std::fs;

use crate::catalog::TableMetadata;
use crate::executor::{ExecutorError, Result};
use crate::storage::SqlValue;

use super::{BulkReader, CopySchema, parse_value};

/// 簡易 Parquet リーダー（現段階では行指向テキストを読み込む代替実装）。
///
/// 本来の Parquet パーサを導入するまでの暫定措置として、1 行 1 レコードの
/// カンマ区切りテキストを読み取り、テーブル定義に従って型変換する。
pub struct ParquetReader {
    schema: CopySchema,
    rows: Vec<Vec<SqlValue>>,
    position: usize,
}

impl ParquetReader {
    pub fn open(path: &str, table_meta: &TableMetadata, header: bool) -> Result<Self> {
        let content = fs::read_to_string(path).map_err(|e| {
            ExecutorError::BulkLoad(format!("failed to read Parquet placeholder file: {e}"))
        })?;

        let mut lines = content.lines();
        if header {
            // 無視するが、パーサの挙動を CSV と揃えるためにスキップ。
            let _ = lines.next();
        }

        let fields = CopySchema::from_table(table_meta).fields;

        let mut rows = Vec::new();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() != table_meta.column_count() {
                return Err(ExecutorError::BulkLoad(format!(
                    "column count mismatch in row: expected {}, got {}",
                    table_meta.column_count(),
                    parts.len()
                )));
            }

            let mut parsed = Vec::with_capacity(parts.len());
            for (idx, raw) in parts.iter().enumerate() {
                let value = parse_value(raw, &table_meta.columns[idx].data_type)?;
                parsed.push(value);
            }
            rows.push(parsed);
        }

        Ok(Self {
            schema: CopySchema { fields },
            rows,
            position: 0,
        })
    }
}

impl BulkReader for ParquetReader {
    fn schema(&self) -> &CopySchema {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<SqlValue>>>> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }
        let end = (self.position + max_rows).min(self.rows.len());
        let batch = self.rows[self.position..end].to_vec();
        self.position = end;
        Ok(Some(batch))
    }
}
