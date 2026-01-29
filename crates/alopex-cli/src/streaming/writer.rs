//! StreamingWriter - Streaming output controller
//!
//! Manages streaming output with buffer limits for non-streaming formats.

use std::io::Write;

use crate::error::{CliError, Result};
use crate::models::{Column, Row};
use crate::output::formatter::Formatter;
/// Default buffer limit for non-streaming formats (table).
pub const DEFAULT_BUFFER_LIMIT: usize = 10 * 1024 * 1024;

/// Status returned by write operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStatus {
    /// Row was written successfully, continue writing.
    Continue,
    /// Limit reached, no more rows will be written.
    LimitReached,
}

/// Streaming writer for output.
///
/// Controls streaming output with the following behaviors:
/// - **Streaming formats** (json, jsonl, csv, tsv): Output rows immediately.
/// - **Non-streaming formats** (table): Buffer rows up to `buffer_limit`.
///
/// # Output Boundary
///
/// - **Output started**: When `output_started == true` (header has been written).
/// - **Buffer overflow**: Returns an error prompting `--output json|csv|tsv` or `--limit`.
pub struct StreamingWriter<W> {
    /// The underlying writer.
    writer: W,
    /// The formatter to use for output.
    formatter: Box<dyn Formatter>,
    /// Column definitions (schema).
    columns: Vec<Column>,
    /// Optional row limit.
    limit: Option<usize>,
    /// Buffer limit for non-streaming formats.
    buffer_limit: usize,
    /// Buffer for non-streaming formats (table).
    buffer: Vec<Row>,
    /// Approximate buffered size in bytes.
    buffer_bytes: usize,
    /// Number of rows written (for limit checking).
    written_count: usize,
    /// Whether header has been output (output started).
    output_started: bool,
    /// Whether quiet mode is enabled (suppress warnings).
    quiet: bool,
}

impl<W: Write> StreamingWriter<W> {
    /// Create a new StreamingWriter.
    ///
    /// # Arguments
    ///
    /// * `writer` - The output writer (e.g., stdout).
    /// * `formatter` - The formatter to use for output.
    /// * `columns` - Column definitions for the output.
    /// * `limit` - Optional row limit.
    pub fn new(
        writer: W,
        formatter: Box<dyn Formatter>,
        columns: Vec<Column>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            writer,
            formatter,
            columns,
            limit,
            buffer_limit: DEFAULT_BUFFER_LIMIT,
            buffer: Vec::new(),
            buffer_bytes: 0,
            written_count: 0,
            output_started: false,
            quiet: false,
        }
    }

    /// Create a new StreamingWriter with a custom buffer limit.
    #[allow(dead_code)]
    pub fn with_buffer_limit(mut self, buffer_limit: usize) -> Self {
        self.buffer_limit = buffer_limit;
        self
    }

    /// Enable quiet mode (suppress warnings).
    pub fn with_quiet(mut self, quiet: bool) -> Self {
        self.quiet = quiet;
        self
    }

    /// Check if quiet mode is enabled.
    ///
    /// When quiet mode is enabled, status-only output (OK messages) should be suppressed.
    pub fn is_quiet(&self) -> bool {
        self.quiet
    }

    /// Prepare the writer for output.
    ///
    /// For streaming formats (json, jsonl, csv, tsv), immediately outputs the header.
    /// For non-streaming formats (table), defers header output.
    ///
    /// # Arguments
    ///
    /// * `row_count_hint` - Optional estimated row count (unused for buffer sizing).
    pub fn prepare(&mut self, row_count_hint: Option<usize>) -> Result<()> {
        let _ = row_count_hint;

        // For streaming formats, output header immediately
        if self.formatter.supports_streaming() {
            self.formatter
                .write_header(&mut self.writer, &self.columns)?;
            self.output_started = true;
        }

        Ok(())
    }

    /// Write a row to the output.
    ///
    /// For streaming formats, the row is output immediately.
    /// For non-streaming formats, the row is buffered.
    ///
    /// # Returns
    ///
    /// * `WriteStatus::Continue` - Row was written, continue writing.
    /// * `WriteStatus::LimitReached` - Row limit reached, stop writing.
    ///
    /// # Note
    ///
    /// The row is taken by ownership to avoid unnecessary cloning when buffering.
    pub fn write_row(&mut self, row: Row) -> Result<WriteStatus> {
        // Check limit
        if let Some(limit) = self.limit {
            if self.written_count >= limit {
                return Ok(WriteStatus::LimitReached);
            }
        }

        if self.formatter.supports_streaming() {
            // Streaming format: output immediately
            self.formatter.write_row(&mut self.writer, &row)?;
            self.written_count += 1;
        } else {
            // Non-streaming format: buffer the row
            let row_bytes = estimate_row_bytes(&row);
            self.buffer_bytes = self.buffer_bytes.saturating_add(row_bytes);
            if self.buffer_bytes > self.buffer_limit {
                return Err(CliError::InvalidArgument(
                    "Buffer limit exceeded (~10MB). \
                     Use --output json|csv|tsv or --limit to reduce results."
                        .into(),
                ));
            }
            self.buffer.push(row);
            self.written_count += 1;
        }

        Ok(WriteStatus::Continue)
    }

    /// Finish output, flushing any buffered rows and writing the footer.
    pub fn finish(&mut self) -> Result<()> {
        // For non-streaming formats, output header if not yet done
        if !self.output_started {
            self.formatter
                .write_header(&mut self.writer, &self.columns)?;
            self.output_started = true;

            // Flush any buffered rows
            for row in self.buffer.drain(..) {
                self.formatter.write_row(&mut self.writer, &row)?;
            }
        }

        // Write footer
        self.formatter.write_footer(&mut self.writer)?;

        Ok(())
    }

    /// Returns the number of rows written.
    #[allow(dead_code)]
    pub fn written_count(&self) -> usize {
        self.written_count
    }

    /// Returns whether output has started.
    #[allow(dead_code)]
    pub fn output_started(&self) -> bool {
        self.output_started
    }

    /// Returns approximate buffered bytes.
    #[allow(dead_code)]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer_bytes
    }
}

fn estimate_row_bytes(row: &Row) -> usize {
    row.columns
        .iter()
        .map(estimate_value_bytes)
        .sum::<usize>()
        .saturating_add(row.columns.len() * 8)
}

fn estimate_value_bytes(value: &crate::models::Value) -> usize {
    match value {
        crate::models::Value::Null => 4,
        crate::models::Value::Bool(_) => 1,
        crate::models::Value::Int(_) => 8,
        crate::models::Value::Float(_) => 8,
        crate::models::Value::Text(text) => text.len(),
        crate::models::Value::Bytes(bytes) => bytes.len(),
        crate::models::Value::Vector(values) => values.len() * 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CliError;
    use crate::models::{DataType, Value};
    use crate::output::csv::CsvFormatter;
    use crate::output::json::JsonFormatter;
    use crate::output::jsonl::JsonlFormatter;
    use crate::output::table::TableFormatter;

    fn test_columns() -> Vec<Column> {
        vec![
            Column::new("id", DataType::Int),
            Column::new("name", DataType::Text),
        ]
    }

    fn test_row(id: i64, name: &str) -> Row {
        Row::new(vec![Value::Int(id), Value::Text(name.to_string())])
    }

    #[test]
    fn test_streaming_format_immediate_output() {
        let mut output = Vec::new();
        let formatter = Box::new(JsonlFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(&mut output, formatter, columns, None);

        writer.prepare(None).unwrap();
        assert!(writer.output_started());

        let status = writer.write_row(test_row(1, "Alice")).unwrap();
        assert_eq!(status, WriteStatus::Continue);
        assert_eq!(writer.written_count(), 1);

        writer.finish().unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("\"id\":1"));
        assert!(result.contains("\"name\":\"Alice\""));
    }

    #[test]
    fn test_non_streaming_format_buffered_output() {
        let mut output = Vec::new();
        let formatter = Box::new(TableFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(&mut output, formatter, columns, None);

        writer.prepare(None).unwrap();
        assert!(!writer.output_started()); // Header not output yet

        let status = writer.write_row(test_row(1, "Alice")).unwrap();
        assert_eq!(status, WriteStatus::Continue);
        assert!(!writer.output_started()); // Still buffering

        writer.finish().unwrap();
        assert!(writer.output_started()); // Now output started

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("id"));
        assert!(result.contains("Alice"));
    }

    #[test]
    fn test_limit_enforcement() {
        let mut output = Vec::new();
        let formatter = Box::new(CsvFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(&mut output, formatter, columns, Some(2));

        writer.prepare(None).unwrap();

        assert_eq!(
            writer.write_row(test_row(1, "Alice")).unwrap(),
            WriteStatus::Continue
        );
        assert_eq!(
            writer.write_row(test_row(2, "Bob")).unwrap(),
            WriteStatus::Continue
        );
        assert_eq!(
            writer.write_row(test_row(3, "Charlie")).unwrap(),
            WriteStatus::LimitReached
        );

        assert_eq!(writer.written_count(), 2);

        writer.finish().unwrap();
    }

    #[test]
    fn test_buffer_overflow_errors() {
        let mut output = Vec::new();
        let formatter = Box::new(TableFormatter::new());
        let columns = test_columns();

        let mut writer =
            StreamingWriter::new(&mut output, formatter, columns, None).with_buffer_limit(40); // Very small buffer

        writer.prepare(None).unwrap();
        assert!(!writer.output_started());

        // Add rows to buffer
        assert_eq!(
            writer.write_row(test_row(1, "Alice")).unwrap(),
            WriteStatus::Continue
        );
        let err = writer.write_row(test_row(2, "Bob")).unwrap_err();
        assert!(matches!(err, CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_empty_output() {
        let mut output = Vec::new();
        let formatter = Box::new(JsonFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(&mut output, formatter, columns, None);

        writer.prepare(None).unwrap();
        writer.finish().unwrap();

        let result = String::from_utf8(output).unwrap();
        // JSON array format: should be valid empty array
        assert!(result.contains('['));
        assert!(result.contains(']'));
    }

    #[test]
    fn test_csv_streaming() {
        let mut output = Vec::new();
        let formatter = Box::new(CsvFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(&mut output, formatter, columns, None);

        writer.prepare(None).unwrap();
        assert!(writer.output_started()); // CSV is streaming

        writer.write_row(test_row(1, "Alice")).unwrap();
        writer.write_row(test_row(2, "Bob")).unwrap();
        writer.finish().unwrap();

        let result = String::from_utf8(output).unwrap();
        assert_eq!(result, "id,name\n1,Alice\n2,Bob\n");
    }

    #[test]
    fn test_written_count() {
        let mut output = Vec::new();
        let formatter = Box::new(CsvFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(&mut output, formatter, columns, None);

        writer.prepare(None).unwrap();

        assert_eq!(writer.written_count(), 0);
        writer.write_row(test_row(1, "Alice")).unwrap();
        assert_eq!(writer.written_count(), 1);
        writer.write_row(test_row(2, "Bob")).unwrap();
        assert_eq!(writer.written_count(), 2);

        writer.finish().unwrap();
    }

    #[test]
    fn test_table_with_small_data() {
        let mut output = Vec::new();
        let formatter = Box::new(TableFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(&mut output, formatter, columns, None);

        writer.prepare(None).unwrap();
        assert!(!writer.output_started()); // Table is non-streaming

        writer.write_row(test_row(1, "Alice")).unwrap();
        writer.write_row(test_row(2, "Bob")).unwrap();
        writer.finish().unwrap();

        let result = String::from_utf8(output).unwrap();
        // Table output should contain the data
        assert!(result.contains("id"));
        assert!(result.contains("name"));
        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
    }

    #[test]
    fn test_streaming_large_row_count_does_not_buffer() {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = test_columns();

        let mut writer = StreamingWriter::new(std::io::sink(), formatter, columns, None);

        writer.prepare(None).unwrap();
        for i in 0..12_000 {
            writer.write_row(test_row(i, "row")).unwrap();
        }

        assert_eq!(writer.written_count(), 12_000);
        assert!(writer.buffer.is_empty());

        writer.finish().unwrap();
    }
}
