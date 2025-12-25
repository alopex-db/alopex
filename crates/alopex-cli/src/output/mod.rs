//! Output formatters
//!
//! This module provides formatters for different output formats:
//! - table: Human-readable table format (comfy-table)
//! - json: JSON array format
//! - jsonl: JSON Lines format (streaming)
//! - csv: CSV format (RFC 4180)
//! - tsv: TSV format

pub mod csv;
pub mod formatter;
pub mod json;
pub mod jsonl;
pub mod table;
pub mod tsv;

pub use formatter::create_formatter;

// Re-export for public API (may be used by external consumers)
#[allow(unused_imports)]
pub use formatter::Formatter;
