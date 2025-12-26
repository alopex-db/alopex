//! Formatter trait and factory function
//!
//! Defines the common interface for all output formatters.

use std::io::Write;

use crate::cli::OutputFormat;
use crate::error::Result;
use crate::models::{Column, Row};

use super::csv::CsvFormatter;
use super::json::JsonFormatter;
use super::jsonl::JsonlFormatter;
use super::table::TableFormatter;
use super::tsv::TsvFormatter;

/// Trait for output formatters.
///
/// All formatters must be Send + Sync to allow use in multi-threaded contexts.
pub trait Formatter: Send + Sync {
    /// Write the header (column names) to the output.
    ///
    /// For streaming formats (jsonl, csv, tsv), this is called immediately.
    /// For buffered formats (table, json), this is called after buffer evaluation.
    fn write_header(&mut self, writer: &mut dyn Write, columns: &[Column]) -> Result<()>;

    /// Write a single row to the output.
    fn write_row(&mut self, writer: &mut dyn Write, row: &Row) -> Result<()>;

    /// Write the footer to the output.
    ///
    /// For json format, this closes the array. For others, this may be a no-op.
    fn write_footer(&mut self, writer: &mut dyn Write) -> Result<()>;

    /// Returns true if this formatter supports streaming output.
    ///
    /// Streaming formats (jsonl, csv, tsv) write rows immediately.
    /// Non-streaming formats (table, json) buffer rows before output.
    fn supports_streaming(&self) -> bool;
}

/// Create a formatter for the specified output format.
pub fn create_formatter(format: OutputFormat) -> Box<dyn Formatter> {
    match format {
        OutputFormat::Table => Box::new(TableFormatter::new()),
        OutputFormat::Json => Box::new(JsonFormatter::new()),
        OutputFormat::Jsonl => Box::new(JsonlFormatter::new()),
        OutputFormat::Csv => Box::new(CsvFormatter::new()),
        OutputFormat::Tsv => Box::new(TsvFormatter::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_formatter_table() {
        let formatter = create_formatter(OutputFormat::Table);
        assert!(!formatter.supports_streaming());
    }

    #[test]
    fn test_create_formatter_json() {
        let formatter = create_formatter(OutputFormat::Json);
        assert!(!formatter.supports_streaming());
    }

    #[test]
    fn test_create_formatter_jsonl() {
        let formatter = create_formatter(OutputFormat::Jsonl);
        assert!(formatter.supports_streaming());
    }

    #[test]
    fn test_create_formatter_csv() {
        let formatter = create_formatter(OutputFormat::Csv);
        assert!(formatter.supports_streaming());
    }

    #[test]
    fn test_create_formatter_tsv() {
        let formatter = create_formatter(OutputFormat::Tsv);
        assert!(formatter.supports_streaming());
    }
}
