use std::fs;

use crate::catalog::TableMetadata;
use crate::executor::{ExecutorError, Result};
use crate::storage::SqlValue;

use super::{BulkReader, CopyField, CopySchema, parse_value};

/// 簡易 CSV リーダー。
pub struct CsvReader {
    schema: CopySchema,
    rows: Vec<Vec<SqlValue>>,
    position: usize,
}

impl CsvReader {
    pub fn open(path: &str, table_meta: &TableMetadata, header: bool) -> Result<Self> {
        let content = fs::read_to_string(path)
            .map_err(|e| ExecutorError::BulkLoad(format!("failed to read CSV: {e}")))?;

        let mut lines = content.lines();
        let header_names: Option<Vec<String>> = if header {
            lines
                .next()
                .map(|h| h.split(',').map(|s| s.trim().to_string()).collect())
        } else {
            None
        };

        let schema_fields = table_meta
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| CopyField {
                name: header_names
                    .as_ref()
                    .and_then(|names| names.get(idx))
                    .map(|s| s.to_string()),
                data_type: Some(col.data_type.clone()),
            })
            .collect();

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
            schema: CopySchema {
                fields: schema_fields,
            },
            rows,
            position: 0,
        })
    }
}

impl BulkReader for CsvReader {
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
