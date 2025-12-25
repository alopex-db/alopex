//! Columnar Command - Columnar segment operations
//!
//! Supports: scan, stats, list (segment-based operations)

use std::io::Write;

use alopex_embedded::Database;

use crate::cli::ColumnarCommand;
use crate::error::Result;
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::Formatter;
use crate::streaming::{StreamingWriter, WriteStatus};

/// Execute a Columnar command with segment-based operations.
///
/// # Arguments
///
/// * `db` - The database instance.
/// * `cmd` - The Columnar subcommand to execute.
/// * `writer` - The output writer.
/// * `formatter` - The formatter to use.
/// * `limit` - Optional row limit.
/// * `quiet` - Whether to suppress informational output.
pub fn execute_with_formatter<W: Write>(
    db: &Database,
    cmd: ColumnarCommand,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    match cmd {
        ColumnarCommand::Scan { segment, progress } => {
            let columns = columnar_scan_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            execute_scan(db, &segment, progress, &mut streaming_writer)
        }
        ColumnarCommand::Stats { segment } => {
            let columns = columnar_stats_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            execute_stats(db, &segment, &mut streaming_writer)
        }
        ColumnarCommand::List => {
            let columns = columnar_list_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            execute_list(db, &mut streaming_writer)
        }
    }
}

/// Execute a columnar scan command on a specific segment.
///
/// FR-7 Compliance: Uses streaming API to avoid materializing all rows upfront.
/// Rows are yielded one at a time from `ColumnarRowIterator`.
fn execute_scan<W: Write>(
    db: &Database,
    segment_id: &str,
    progress: bool,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    // Show progress indicator (respect --quiet flag)
    let show_progress = progress && !writer.is_quiet();
    if show_progress {
        eprint!("Scanning segment '{}'... ", segment_id);
    }

    // FR-7: Use streaming API to avoid materializing all rows upfront
    let iter = db.scan_columnar_segment_streaming(segment_id)?;

    // FR-7: Use None for row count hint to support true streaming output
    writer.prepare(None)?;

    // FR-7: Stream rows one at a time, counting as we go
    let mut row_count = 0usize;
    for row_data in iter {
        let values: Vec<Value> = row_data.into_iter().map(sql_value_to_value).collect();
        let row = Row::new(values);

        match writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
        row_count += 1;
    }

    writer.finish()?;

    // Complete progress indicator with final count (after streaming is done)
    if show_progress {
        eprintln!("done ({} rows).", row_count);
    }

    Ok(())
}

/// Execute a columnar stats command for a specific segment.
fn execute_stats<W: Write>(
    db: &Database,
    segment_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    // Try to get segment statistics
    let stats = db.get_columnar_segment_stats(segment_id)?;

    writer.prepare(Some(4))?;

    // Output stats as rows
    let stats_rows = vec![
        ("segment_id", Value::Text(segment_id.to_string())),
        ("row_count", Value::Int(stats.row_count as i64)),
        ("column_count", Value::Int(stats.column_count as i64)),
        ("size_bytes", Value::Int(stats.size_bytes as i64)),
    ];

    for (key, value) in stats_rows {
        let row = Row::new(vec![Value::Text(key.to_string()), value]);
        writer.write_row(row)?;
    }

    writer.finish()?;
    Ok(())
}

/// Execute a columnar list command to list all segments.
fn execute_list<W: Write>(db: &Database, writer: &mut StreamingWriter<W>) -> Result<()> {
    // List all columnar segments
    let segments = db.list_columnar_segments()?;

    writer.prepare(Some(segments.len()))?;

    for segment_id in segments {
        let row = Row::new(vec![Value::Text(segment_id)]);

        match writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
    }

    writer.finish()?;
    Ok(())
}

/// Convert alopex_sql::SqlValue to our Value type.
fn sql_value_to_value(sql_value: alopex_sql::SqlValue) -> Value {
    use alopex_sql::SqlValue;

    match sql_value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::Int(i as i64),
        SqlValue::BigInt(i) => Value::Int(i),
        SqlValue::Float(f) => Value::Float(f as f64),
        SqlValue::Double(f) => Value::Float(f),
        SqlValue::Text(s) => Value::Text(s),
        SqlValue::Blob(b) => Value::Bytes(b),
        SqlValue::Boolean(b) => Value::Bool(b),
        SqlValue::Timestamp(ts) => Value::Text(format!("{}", ts)),
        SqlValue::Vector(v) => Value::Vector(v),
    }
}

/// Create columns for columnar scan output.
pub fn columnar_scan_columns() -> Vec<Column> {
    // Generic columns - actual columns will depend on segment schema
    vec![
        Column::new("column1", DataType::Text),
        Column::new("column2", DataType::Text),
    ]
}

/// Create columns for columnar stats output.
pub fn columnar_stats_columns() -> Vec<Column> {
    vec![
        Column::new("property", DataType::Text),
        Column::new("value", DataType::Text),
    ]
}

/// Create columns for columnar list output.
pub fn columnar_list_columns() -> Vec<Column> {
    vec![Column::new("segment_id", DataType::Text)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::jsonl::JsonlFormatter;

    fn create_test_db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn create_stats_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = columnar_stats_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn create_list_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = columnar_list_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    #[test]
    fn test_columnar_list_empty() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_list_writer(&mut output);
            // This may return an error or empty list depending on implementation
            let _ = execute_list(&db, &mut writer);
        }
    }

    #[test]
    fn test_columnar_stats_nonexistent() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_stats_writer(&mut output);
            let result = execute_stats(&db, "nonexistent_segment", &mut writer);
            // Should return an error for nonexistent segment
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_sql_value_conversion() {
        use alopex_sql::SqlValue;

        assert!(matches!(sql_value_to_value(SqlValue::Null), Value::Null));
        assert!(matches!(
            sql_value_to_value(SqlValue::Integer(42)),
            Value::Int(42)
        ));
        assert!(matches!(
            sql_value_to_value(SqlValue::Boolean(true)),
            Value::Bool(true)
        ));
        assert!(matches!(
            sql_value_to_value(SqlValue::Text("hello".to_string())),
            Value::Text(s) if s == "hello"
        ));
    }
}
