//! Vector Command - Vector similarity operations
//!
//! Supports: search, upsert, delete (single key/vector operations)

use std::io::Write;

use alopex_embedded::{Database, TxnMode};
use serde::{Deserialize, Serialize};

use crate::batch::BatchMode;
use crate::cli::VectorCommand;
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::Formatter;
use crate::progress::ProgressIndicator;
use crate::streaming::{StreamingWriter, WriteStatus};

#[derive(Debug, Serialize)]
struct RemoteVectorSearchRequest {
    index: String,
    query: Vec<f32>,
    k: usize,
}

#[derive(Debug, Serialize)]
struct RemoteVectorUpsertRequest {
    index: String,
    key: Vec<u8>,
    vector: Vec<f32>,
}

#[derive(Debug, Serialize)]
struct RemoteVectorDeleteRequest {
    index: String,
    key: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct RemoteVectorSearchResult {
    key: Vec<u8>,
    distance: f32,
    #[allow(dead_code)]
    metadata: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct RemoteVectorSearchResponse {
    results: Vec<RemoteVectorSearchResult>,
}

#[derive(Debug, Deserialize)]
struct RemoteVectorStatusResponse {
    success: bool,
}

/// Execute a Vector command.
///
/// # Arguments
///
/// * `db` - The database instance.
/// * `cmd` - The Vector subcommand to execute.
/// * `writer` - The streaming writer for output.
pub fn execute<W: Write>(
    db: &Database,
    cmd: VectorCommand,
    batch_mode: &BatchMode,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match cmd {
        VectorCommand::Search {
            index,
            query,
            k,
            progress,
        } => execute_search(db, &index, &query, k, progress, batch_mode, writer),
        VectorCommand::Upsert { index, key, vector } => {
            execute_upsert(db, &index, &key, &vector, writer)
        }
        VectorCommand::Delete { index, key } => execute_delete(db, &index, &key, writer),
    }
}

/// Execute a Vector command against a remote server.
pub async fn execute_remote_with_formatter<W: Write>(
    client: &HttpClient,
    cmd: &VectorCommand,
    batch_mode: &BatchMode,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    match cmd {
        VectorCommand::Search {
            index,
            query,
            k,
            progress,
        } => {
            execute_remote_search(
                client, index, query, *k, *progress, batch_mode, writer, formatter, limit, quiet,
            )
            .await
        }
        VectorCommand::Upsert { index, key, vector } => {
            execute_remote_upsert(client, index, key, vector, writer, formatter, limit, quiet).await
        }
        VectorCommand::Delete { index, key } => {
            execute_remote_delete(client, index, key, writer, formatter, limit, quiet).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_remote_search<W: Write>(
    client: &HttpClient,
    index: &str,
    query_json: &str,
    k: usize,
    progress: bool,
    batch_mode: &BatchMode,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let query_vector: Vec<f32> = serde_json::from_str(query_json)
        .map_err(|e| CliError::InvalidArgument(format!("Invalid vector JSON: {}", e)))?;

    let mut progress_indicator = ProgressIndicator::new(
        batch_mode,
        progress,
        quiet,
        format!("Searching index '{}' for {} nearest neighbors...", index, k),
    );

    let request = RemoteVectorSearchRequest {
        index: index.to_string(),
        query: query_vector,
        k,
    };
    let response: RemoteVectorSearchResponse = client
        .post_json("hnsw/search", &request)
        .await
        .map_err(map_client_error)?;

    progress_indicator.finish_with_message(format!("found {} results.", response.results.len()));

    let columns = vector_search_columns();
    let mut streaming_writer =
        StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
    streaming_writer.prepare(Some(response.results.len()))?;
    for result in response.results {
        let key_display = match std::str::from_utf8(&result.key) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(result.key),
        };
        let row = Row::new(vec![key_display, Value::Float(result.distance as f64)]);
        match streaming_writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
    }
    streaming_writer.finish()
}

#[allow(clippy::too_many_arguments)]
async fn execute_remote_upsert<W: Write>(
    client: &HttpClient,
    index: &str,
    key: &str,
    vector_json: &str,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let vector: Vec<f32> = serde_json::from_str(vector_json)
        .map_err(|e| CliError::InvalidArgument(format!("Invalid vector JSON: {}", e)))?;
    let request = RemoteVectorUpsertRequest {
        index: index.to_string(),
        key: key.as_bytes().to_vec(),
        vector,
    };
    let response: RemoteVectorStatusResponse = client
        .post_json("hnsw/upsert", &request)
        .await
        .map_err(map_client_error)?;
    if response.success {
        if quiet {
            return Ok(());
        }
        let columns = vector_status_columns();
        let mut streaming_writer =
            StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
        streaming_writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Vector '{}' upserted", key)),
        ]);
        streaming_writer.write_row(row)?;
        streaming_writer.finish()
    } else {
        Err(CliError::InvalidArgument(
            "Failed to upsert vector".to_string(),
        ))
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_remote_delete<W: Write>(
    client: &HttpClient,
    index: &str,
    key: &str,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let request = RemoteVectorDeleteRequest {
        index: index.to_string(),
        key: key.as_bytes().to_vec(),
    };
    let response: RemoteVectorStatusResponse = client
        .post_json("hnsw/delete", &request)
        .await
        .map_err(map_client_error)?;
    if response.success {
        if quiet {
            return Ok(());
        }
        let columns = vector_status_columns();
        let mut streaming_writer =
            StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
        streaming_writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Vector '{}' deleted", key)),
        ]);
        streaming_writer.write_row(row)?;
        streaming_writer.finish()
    } else {
        Err(CliError::InvalidArgument(
            "Failed to delete vector".to_string(),
        ))
    }
}

fn map_client_error(err: ClientError) -> CliError {
    match err {
        ClientError::Request { source, .. } => {
            CliError::ServerConnection(format!("request failed: {source}"))
        }
        ClientError::InvalidUrl(message) => CliError::InvalidArgument(message),
        ClientError::Build(message) => CliError::InvalidArgument(message),
        ClientError::Auth(err) => CliError::InvalidArgument(err.to_string()),
        ClientError::HttpStatus { status, body } => {
            CliError::InvalidArgument(format!("Server error: HTTP {} - {}", status.as_u16(), body))
        }
    }
}

/// Execute a vector search command.
fn execute_search<W: Write>(
    db: &Database,
    index: &str,
    query_json: &str,
    k: usize,
    progress: bool,
    batch_mode: &BatchMode,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    // Parse vector from JSON array
    let query_vector: Vec<f32> = serde_json::from_str(query_json)
        .map_err(|e| CliError::InvalidArgument(format!("Invalid vector JSON: {}", e)))?;

    let mut progress_indicator = ProgressIndicator::new(
        batch_mode,
        progress,
        writer.is_quiet(),
        format!("Searching index '{}' for {} nearest neighbors...", index, k),
    );

    // Perform search
    let (results, _stats) = db.search_hnsw(index, &query_vector, k, None)?;

    progress_indicator.finish_with_message(format!("found {} results.", results.len()));

    // Prepare writer with hint
    writer.prepare(Some(results.len()))?;

    for result in results {
        // Convert key to displayable string
        let key_display = match std::str::from_utf8(&result.key) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(result.key),
        };

        let row = Row::new(vec![key_display, Value::Float(result.distance as f64)]);

        match writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
    }

    writer.finish()?;
    Ok(())
}

/// Execute a single vector upsert command.
fn execute_upsert<W: Write>(
    db: &Database,
    index: &str,
    key: &str,
    vector_json: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    // Parse vector from JSON array
    let vector: Vec<f32> = serde_json::from_str(vector_json)
        .map_err(|e| CliError::InvalidArgument(format!("Invalid vector JSON: {}", e)))?;

    // Begin transaction
    let mut txn = db.begin(TxnMode::ReadWrite)?;

    txn.upsert_to_hnsw(index, key.as_bytes(), &vector, b"")?;

    txn.commit()?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Vector '{}' upserted", key)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Execute a single vector delete command.
fn execute_delete<W: Write>(
    db: &Database,
    index: &str,
    key: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    // Begin transaction
    let mut txn = db.begin(TxnMode::ReadWrite)?;

    txn.delete_from_hnsw(index, key.as_bytes())?;

    txn.commit()?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Vector '{}' deleted", key)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Create columns for vector search output.
pub fn vector_search_columns() -> Vec<Column> {
    vec![
        Column::new("id", DataType::Text),
        Column::new("distance", DataType::Float),
    ]
}

/// Create columns for vector status output.
pub fn vector_status_columns() -> Vec<Column> {
    vec![
        Column::new("status", DataType::Text),
        Column::new("message", DataType::Text),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchModeSource;
    use crate::output::jsonl::JsonlFormatter;
    use alopex_embedded::HnswConfig;

    fn create_test_db() -> Database {
        Database::new()
    }

    fn create_search_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = vector_search_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn default_batch_mode() -> BatchMode {
        BatchMode {
            is_batch: false,
            is_tty: true,
            source: BatchModeSource::Default,
        }
    }

    fn create_status_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = vector_status_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn setup_hnsw_index(db: &Database, name: &str) {
        let config = HnswConfig::default()
            .with_dimension(3)
            .with_metric(alopex_embedded::Metric::L2)
            .with_m(8)
            .with_ef_construction(32);
        db.create_hnsw_index(name, config).unwrap();
    }

    #[test]
    fn test_upsert_single_vector() {
        let db = create_test_db();
        setup_hnsw_index(&db, "test_index");

        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_upsert(&db, "test_index", "v1", "[1.0, 0.0, 0.0]", &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("OK"));
        assert!(result.contains("Vector 'v1' upserted"));
    }

    #[test]
    fn test_search_vectors() {
        use alopex_embedded::TxnMode;

        let db = create_test_db();
        setup_hnsw_index(&db, "search_test");

        // Upsert vectors in a single transaction
        // (multiple sequential transactions have a known bug with checksum mismatch)
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.upsert_to_hnsw("search_test", b"v1", &[1.0_f32, 0.0, 0.0], b"")
                .unwrap();
            txn.upsert_to_hnsw("search_test", b"v2", &[0.0_f32, 1.0, 0.0], b"")
                .unwrap();
            txn.upsert_to_hnsw("search_test", b"v3", &[0.0_f32, 0.0, 1.0], b"")
                .unwrap();
            txn.commit().unwrap();
        }

        // Search for similar vectors
        let query = "[1.0, 0.0, 0.0]";
        let mut output = Vec::new();
        {
            let mut writer = create_search_writer(&mut output);
            execute_search(
                &db,
                "search_test",
                query,
                2,
                false,
                &default_batch_mode(),
                &mut writer,
            )
            .unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("v1")); // Should find v1 as most similar
    }

    #[test]
    fn test_delete_single_vector() {
        use alopex_embedded::TxnMode;

        let db = create_test_db();
        setup_hnsw_index(&db, "delete_test");

        // Upsert vectors in a single transaction
        // (multiple sequential transactions have a known bug with checksum mismatch)
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.upsert_to_hnsw("delete_test", b"v1", &[1.0_f32, 0.0, 0.0], b"")
                .unwrap();
            txn.upsert_to_hnsw("delete_test", b"v2", &[0.0_f32, 1.0, 0.0], b"")
                .unwrap();
            txn.commit().unwrap();
        }

        // Delete one vector via CLI command
        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_delete(&db, "delete_test", "v1", &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("OK"));
        assert!(result.contains("Vector 'v1' deleted"));
    }

    #[test]
    fn test_invalid_vector_json() {
        let db = create_test_db();
        setup_hnsw_index(&db, "invalid_test");

        let mut output = Vec::new();
        let mut writer = create_status_writer(&mut output);

        let result = execute_upsert(&db, "invalid_test", "v1", "not valid json", &mut writer);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CliError::InvalidArgument(_)));
    }

    /// Direct test using EXACTLY the same pattern as hnsw_integration_tests.rs
    /// to verify that multiple upserts in a single transaction work correctly.
    #[test]
    fn test_direct_multi_txn_hnsw() {
        use alopex_embedded::TxnMode;

        let db = Database::new();
        // Use same config as hnsw_integration_tests.rs: dimension 2, L2, m=8, ef=32
        let config = HnswConfig::default()
            .with_dimension(2)
            .with_metric(alopex_embedded::Metric::L2)
            .with_m(8)
            .with_ef_construction(32);
        db.create_hnsw_index("direct_test", config).unwrap();

        // Single transaction with multiple upserts (exactly like hnsw_integration_tests.rs)
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_to_hnsw("direct_test", b"a", &[0.0_f32, 0.0], b"ma")
            .unwrap();
        txn.upsert_to_hnsw("direct_test", b"b", &[1.0_f32, 0.0], b"mb")
            .unwrap();
        txn.commit().unwrap();

        // Search should find "a" as nearest to [0.1, 0]
        let (results, _) = db
            .search_hnsw("direct_test", &[0.1_f32, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"a");
    }
}
