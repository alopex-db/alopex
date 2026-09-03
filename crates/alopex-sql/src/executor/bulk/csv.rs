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

        let records = parse_csv_records(&content)?;
        let header_names: Option<Vec<String>> = if header {
            records
                .first()
                .map(|row| row.iter().map(|s| s.trim().to_string()).collect())
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
        let start = if header { 1 } else { 0 };
        for record in records.into_iter().skip(start) {
            if record.iter().all(|field| field.trim().is_empty()) {
                continue;
            }
            let mut parts = record;
            if parts.len() != table_meta.column_count() {
                // 特例: 最終列が VECTOR の場合は余りを結合して帳尻を合わせる（埋め込みにカンマが含まれるため）。
                if let Some(last_ty) = table_meta.columns.last().map(|c| &c.data_type)
                    && matches!(last_ty, crate::planner::types::ResolvedType::Vector { .. })
                    && parts.len() > table_meta.column_count()
                {
                    let head_count = table_meta.column_count().saturating_sub(1);
                    let tail = parts.split_off(head_count);
                    let merged = tail.join(",");
                    parts.push(merged);
                }
            }
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

fn parse_csv_records(content: &str) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = content.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && matches!(chars.peek(), Some('"')) {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                row.push(field);
                field = String::new();
            }
            '\n' if !in_quotes => {
                row.push(field);
                if !row.iter().all(|value| value.trim().is_empty()) {
                    rows.push(row);
                }
                row = Vec::new();
                field = String::new();
            }
            '\r' if !in_quotes => {
                if matches!(chars.peek(), Some('\n')) {
                    chars.next();
                }
                row.push(field);
                if !row.iter().all(|value| value.trim().is_empty()) {
                    rows.push(row);
                }
                row = Vec::new();
                field = String::new();
            }
            _ => field.push(ch),
        }
    }

    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        if !row.iter().all(|value| value.trim().is_empty()) {
            rows.push(row);
        }
    }

    Ok(rows)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_reader_handles_quoted_commas_and_escaped_quotes() {
        let table = crate::catalog::TableMetadata::new(
            "people",
            vec![
                crate::catalog::ColumnMetadata::new(
                    "id",
                    crate::planner::types::ResolvedType::Integer,
                ),
                crate::catalog::ColumnMetadata::new(
                    "name",
                    crate::planner::types::ResolvedType::Text,
                ),
            ],
        );

        let tmp = std::env::temp_dir().join("alopex_csv_quoted.csv");
        std::fs::write(
            &tmp,
            "id,name\n1,\"alice, jr\"\n2,\"bob \"\"the builder\"\"\"\n",
        )
        .unwrap();

        let reader = CsvReader::open(tmp.to_str().unwrap(), &table, true).unwrap();
        let rows = reader.rows;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], SqlValue::Text("alice, jr".into()));
        assert_eq!(rows[1][1], SqlValue::Text("bob \"the builder\"".into()));
        let _ = std::fs::remove_file(tmp);
    }
}
