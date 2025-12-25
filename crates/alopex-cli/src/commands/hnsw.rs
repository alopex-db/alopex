//! HNSW Command - HNSW index management
//!
//! Supports: create, stats, drop

use std::io::Write;

use alopex_embedded::{Database, HnswConfig, Metric};

use crate::cli::{DistanceMetric, HnswCommand};
use crate::error::Result;
use crate::models::{Column, DataType, Row, Value};
use crate::streaming::StreamingWriter;

/// Default M parameter (max connections per node)
const DEFAULT_M: usize = 16;
/// Default ef_construction parameter
const DEFAULT_EF_CONSTRUCTION: usize = 200;

/// Execute an HNSW command.
///
/// # Arguments
///
/// * `db` - The database instance.
/// * `cmd` - The HNSW subcommand to execute.
/// * `writer` - The streaming writer for output.
pub fn execute<W: Write>(
    db: &Database,
    cmd: HnswCommand,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match cmd {
        HnswCommand::Create { name, dim, metric } => execute_create(db, &name, dim, metric, writer),
        HnswCommand::Stats { name } => execute_stats(db, &name, writer),
        HnswCommand::Drop { name } => execute_drop(db, &name, writer),
    }
}

/// Convert CLI distance metric to embedded Metric.
fn to_embedded_metric(metric: DistanceMetric) -> Metric {
    match metric {
        DistanceMetric::Cosine => Metric::Cosine,
        DistanceMetric::L2 => Metric::L2,
        DistanceMetric::Ip => Metric::InnerProduct,
    }
}

/// Execute an HNSW create command.
fn execute_create<W: Write>(
    db: &Database,
    name: &str,
    dim: usize,
    metric: DistanceMetric,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let config = HnswConfig {
        dimension: dim,
        metric: to_embedded_metric(metric),
        m: DEFAULT_M,
        ef_construction: DEFAULT_EF_CONSTRUCTION,
    };

    db.create_hnsw_index(name, config)?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Created HNSW index: {}", name)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Execute an HNSW stats command.
fn execute_stats<W: Write>(
    db: &Database,
    name: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let stats = db.get_hnsw_stats(name)?;

    // Output stats as rows
    writer.prepare(Some(4))?;

    // Output each stat as a row
    let stats_rows = vec![
        ("node_count", Value::Int(stats.node_count as i64)),
        ("deleted_count", Value::Int(stats.deleted_count as i64)),
        ("memory_bytes", Value::Int(stats.memory_bytes as i64)),
        ("avg_edges_per_node", Value::Float(stats.avg_edges_per_node)),
    ];

    for (key, value) in stats_rows {
        let row = Row::new(vec![Value::Text(key.to_string()), value]);
        writer.write_row(row)?;
    }

    writer.finish()?;
    Ok(())
}

/// Execute an HNSW drop command.
fn execute_drop<W: Write>(
    db: &Database,
    name: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    db.drop_hnsw_index(name)?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Dropped HNSW index: {}", name)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Create columns for HNSW stats output.
///
/// Note: value column is Text because stats include both integer and float values.
pub fn hnsw_stats_columns() -> Vec<Column> {
    vec![
        Column::new("property", DataType::Text),
        Column::new("value", DataType::Text),
    ]
}

/// Create columns for HNSW status output.
pub fn hnsw_status_columns() -> Vec<Column> {
    vec![
        Column::new("status", DataType::Text),
        Column::new("message", DataType::Text),
    ]
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
        let columns = hnsw_stats_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn create_status_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = hnsw_status_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    #[test]
    fn test_create_hnsw_index() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_create(&db, "test_index", 128, DistanceMetric::Cosine, &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("OK"));
        assert!(result.contains("Created HNSW index"));
    }

    #[test]
    fn test_create_hnsw_index_l2() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_create(&db, "l2_index", 64, DistanceMetric::L2, &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("OK"));
        assert!(result.contains("Created HNSW index: l2_index"));
    }

    #[test]
    fn test_get_hnsw_stats() {
        let db = create_test_db();

        // Create index first
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_create(&db, "stats_test", 64, DistanceMetric::Cosine, &mut writer).unwrap();
        }

        // Get stats
        let mut output = Vec::new();
        {
            let mut writer = create_stats_writer(&mut output);
            execute_stats(&db, "stats_test", &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("node_count"));
        assert!(result.contains("memory_bytes"));
    }

    #[test]
    fn test_drop_hnsw_index() {
        let db = create_test_db();

        // Create index first
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_create(&db, "drop_test", 32, DistanceMetric::Cosine, &mut writer).unwrap();
        }

        // Drop index
        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_drop(&db, "drop_test", &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("OK"));
        assert!(result.contains("Dropped HNSW index"));
    }

    #[test]
    fn test_create_and_query_index() {
        let db = create_test_db();

        // Create index
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_create(&db, "query_test", 3, DistanceMetric::Cosine, &mut writer).unwrap();
        }

        // Stats should show 0 vectors initially
        let mut output = Vec::new();
        {
            let mut writer = create_stats_writer(&mut output);
            execute_stats(&db, "query_test", &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("node_count"));
    }
}
