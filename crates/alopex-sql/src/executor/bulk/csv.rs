use std::fs::File;
use std::io::{Cursor, Read};

use crate::catalog::TableMetadata;
use crate::executor::{ExecutorError, Result};
use crate::storage::SqlValue;

use super::{BulkReader, CopyField, CopySchema, parse_value};

/// 簡易 CSV リーダー。
pub struct CsvReader {
    schema: CopySchema,
    reader: ::csv::Reader<Box<dyn Read>>,
    types: Vec<crate::planner::types::ResolvedType>,
}

impl CsvReader {
    pub fn open(path: &str, table_meta: &TableMetadata, header: bool) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| ExecutorError::BulkLoad(format!("failed to open CSV: {e}")))?;
        Self::from_reader(file, table_meta, header)
    }

    pub fn from_content(content: String, table_meta: &TableMetadata, header: bool) -> Result<Self> {
        Self::from_reader(Cursor::new(content.into_bytes()), table_meta, header)
    }

    pub fn from_reader(
        reader: impl Read + 'static,
        table_meta: &TableMetadata,
        header: bool,
    ) -> Result<Self> {
        let mut reader = ::csv::ReaderBuilder::new()
            .has_headers(header)
            .flexible(true)
            .from_reader(Box::new(reader) as Box<dyn Read>);
        let header_names = if header {
            Some(
                reader
                    .headers()
                    .map_err(|error| ExecutorError::BulkLoad(error.to_string()))?
                    .iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
            )
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
        Ok(Self {
            schema: CopySchema {
                fields: schema_fields,
            },
            reader,
            types: table_meta
                .columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect(),
        })
    }
}

impl BulkReader for CsvReader {
    fn schema(&self) -> &CopySchema {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<SqlValue>>>> {
        let mut batch = Vec::with_capacity(max_rows);
        for record in self.reader.records().take(max_rows) {
            let record = record.map_err(|error| ExecutorError::BulkLoad(error.to_string()))?;
            let mut parts = record.iter().map(str::to_string).collect::<Vec<_>>();
            if parts.len() != self.types.len()
                && matches!(
                    self.types.last(),
                    Some(crate::planner::types::ResolvedType::Vector { .. })
                )
                && parts.len() > self.types.len()
            {
                let tail = parts.split_off(self.types.len().saturating_sub(1));
                parts.push(tail.join(","));
            }
            if parts.len() != self.types.len() {
                return Err(ExecutorError::BulkLoad(format!(
                    "column count mismatch in row: expected {}, got {}",
                    self.types.len(),
                    parts.len()
                )));
            }
            batch.push(
                parts
                    .iter()
                    .zip(&self.types)
                    .map(|(raw, data_type)| parse_value(raw, data_type))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok((!batch.is_empty()).then_some(batch))
    }
}
