//! Columnar Command - Columnar segment operations
//!
//! Supports: scan, stats, list (segment-based operations)

use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use alopex_core::columnar::encoding::{Column as ColumnData, LogicalType};
use alopex_core::columnar::segment_v2::{
    ColumnSchema, RecordBatch, Schema, SegmentConfigV2, SegmentWriterV2,
};
use alopex_core::storage::compression::CompressionV2;
use alopex_core::storage::format::bincode_config;
use alopex_embedded::{ColumnarIndexType, Database};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow_schema::DataType as ArrowDataType;
use bincode::config::Options;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::batch::BatchMode;
use crate::cli::{ColumnarCommand, IndexCommand};
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::Formatter;
use crate::progress::ProgressIndicator;
use crate::streaming::{StreamingWriter, WriteStatus};

#[derive(Debug, Serialize)]
struct RemoteColumnarScanRequest {
    segment_id: String,
}

#[derive(Debug, Serialize)]
struct RemoteColumnarStatsRequest {
    segment_id: String,
}

#[derive(Debug, Serialize)]
struct RemoteColumnarIndexCreateRequest {
    segment_id: String,
    column: String,
    index_type: String,
}

#[derive(Debug, Serialize)]
struct RemoteColumnarIndexListRequest {
    segment_id: String,
}

#[derive(Debug, Serialize)]
struct RemoteColumnarIndexDropRequest {
    segment_id: String,
    column: String,
}

#[derive(Debug, Serialize)]
struct RemoteColumnarIngestRequest {
    table: String,
    compression: String,
    segment: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct RemoteColumnarScanResponse {
    rows: Vec<Vec<alopex_sql::SqlValue>>,
}

#[derive(Debug, Deserialize)]
struct RemoteColumnarStatsResponse {
    row_count: usize,
    column_count: usize,
    size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct RemoteColumnarListResponse {
    segments: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RemoteColumnarIndexInfo {
    column: String,
    index_type: String,
}

#[derive(Debug, Deserialize)]
struct RemoteColumnarIndexListResponse {
    indexes: Vec<RemoteColumnarIndexInfo>,
}

#[derive(Debug, Deserialize)]
struct RemoteColumnarIngestResponse {
    row_count: u64,
    segment_id: String,
    size_bytes: u64,
    compression: String,
    elapsed_ms: u64,
}

#[derive(Debug, Deserialize)]
struct RemoteColumnarStatusResponse {
    success: bool,
}

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
    batch_mode: &BatchMode,
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
            execute_scan(db, &segment, progress, batch_mode, &mut streaming_writer)
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
        ColumnarCommand::Ingest {
            file,
            table,
            delimiter,
            header,
            compression,
            row_group_size,
        } => {
            let columns = columnar_ingest_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            let options = IngestOptions {
                file: &file,
                table: &table,
                delimiter,
                header,
                compression: compression.as_str(),
                row_group_size,
            };
            execute_ingest(db, options, &mut streaming_writer)
        }
        ColumnarCommand::Index(command) => {
            let columns = match &command {
                IndexCommand::List { .. } => columnar_index_list_columns(),
                IndexCommand::Create { .. } | IndexCommand::Drop { .. } => {
                    columnar_status_columns()
                }
            };
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            execute_index_command(db, command, &mut streaming_writer)
        }
    }
}

async fn execute_remote_ingest<W: Write>(
    client: &HttpClient,
    options: IngestOptions<'_>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let extension = options
        .file
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let batch = match extension.as_str() {
        "csv" => parse_csv(options.file, options.delimiter, options.header)?,
        "parquet" | "pq" => parse_parquet(options.file)?,
        _ => {
            return Err(CliError::InvalidArgument(format!(
                "Unsupported file format: {}",
                options.file.display()
            )))
        }
    };

    let compression_type = parse_compression_arg(options.compression)?;
    let mut config = SegmentConfigV2::default();
    if let Some(size) = options.row_group_size {
        config.row_group_size = size as u64;
    }
    config.compression = map_compression(compression_type);

    let mut segment_writer = SegmentWriterV2::new(config);
    segment_writer
        .write_batch(batch)
        .map_err(|err| CliError::InvalidArgument(err.to_string()))?;
    let segment = segment_writer
        .finish()
        .map_err(|err| CliError::InvalidArgument(err.to_string()))?;
    let segment_bytes = bincode_config()
        .serialize(&segment)
        .map_err(|err| CliError::InvalidArgument(err.to_string()))?;

    let request = RemoteColumnarIngestRequest {
        table: options.table.to_string(),
        compression: compression_as_str(compression_type).to_string(),
        segment: segment_bytes,
    };
    let response: RemoteColumnarIngestResponse = client
        .post_json("columnar/ingest", &request)
        .await
        .map_err(map_client_error)?;

    writer.prepare(Some(1))?;
    let row = Row::new(vec![
        Value::Int(response.row_count as i64),
        Value::Text(response.segment_id),
        Value::Int(response.size_bytes as i64),
        Value::Text(response.compression),
        Value::Int(response.elapsed_ms as i64),
    ]);
    writer.write_row(row)?;
    writer.finish()?;
    Ok(())
}

/// Execute a Columnar command against a remote server.
pub async fn execute_remote_with_formatter<W: Write>(
    client: &HttpClient,
    cmd: &ColumnarCommand,
    batch_mode: &BatchMode,
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
            execute_remote_scan(
                client,
                segment,
                *progress,
                batch_mode,
                &mut streaming_writer,
            )
            .await
        }
        ColumnarCommand::Stats { segment } => {
            let columns = columnar_stats_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            execute_remote_stats(client, segment, &mut streaming_writer).await
        }
        ColumnarCommand::List => {
            let columns = columnar_list_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            execute_remote_list(client, &mut streaming_writer).await
        }
        ColumnarCommand::Ingest {
            file,
            table,
            delimiter,
            header,
            compression,
            row_group_size,
        } => {
            let columns = columnar_ingest_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            let options = IngestOptions {
                file,
                table,
                delimiter: *delimiter,
                header: *header,
                compression: compression.as_str(),
                row_group_size: *row_group_size,
            };
            execute_remote_ingest(client, options, &mut streaming_writer).await
        }
        ColumnarCommand::Index(command) => {
            let columns = match &command {
                IndexCommand::List { .. } => columnar_index_list_columns(),
                IndexCommand::Create { .. } | IndexCommand::Drop { .. } => {
                    columnar_status_columns()
                }
            };
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            execute_remote_index_command(client, command, &mut streaming_writer).await
        }
    }
}

async fn execute_remote_scan<W: Write>(
    client: &HttpClient,
    segment_id: &str,
    progress: bool,
    batch_mode: &BatchMode,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut progress_indicator = ProgressIndicator::new(
        batch_mode,
        progress,
        writer.is_quiet(),
        format!("Scanning segment '{}'...", segment_id),
    );
    let request = RemoteColumnarScanRequest {
        segment_id: segment_id.to_string(),
    };
    let response: RemoteColumnarScanResponse = client
        .post_json("columnar/scan", &request)
        .await
        .map_err(map_client_error)?;
    writer.prepare(Some(response.rows.len()))?;
    let mut row_count = 0usize;
    for row in response.rows {
        let values: Vec<Value> = row.into_iter().map(sql_value_to_value).collect();
        let row = Row::new(values);
        match writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
        row_count += 1;
    }
    writer.finish()?;
    progress_indicator.finish_with_message(format!("done ({} rows).", row_count));
    Ok(())
}

async fn execute_remote_stats<W: Write>(
    client: &HttpClient,
    segment_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let request = RemoteColumnarStatsRequest {
        segment_id: segment_id.to_string(),
    };
    let response: RemoteColumnarStatsResponse = client
        .post_json("columnar/stats", &request)
        .await
        .map_err(map_client_error)?;
    writer.prepare(Some(4))?;
    let stats_rows = vec![
        ("segment_id", Value::Text(segment_id.to_string())),
        ("row_count", Value::Int(response.row_count as i64)),
        ("column_count", Value::Int(response.column_count as i64)),
        ("size_bytes", Value::Int(response.size_bytes as i64)),
    ];
    for (key, value) in stats_rows {
        let row = Row::new(vec![Value::Text(key.to_string()), value]);
        writer.write_row(row)?;
    }
    writer.finish()?;
    Ok(())
}

async fn execute_remote_list<W: Write>(
    client: &HttpClient,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let response: RemoteColumnarListResponse = client
        .post_json("columnar/list", &serde_json::json!({}))
        .await
        .map_err(map_client_error)?;
    writer.prepare(Some(response.segments.len()))?;
    for segment_id in response.segments {
        let row = Row::new(vec![Value::Text(segment_id)]);
        match writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
    }
    writer.finish()?;
    Ok(())
}

async fn execute_remote_index_command<W: Write>(
    client: &HttpClient,
    command: &IndexCommand,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match command {
        IndexCommand::Create {
            segment,
            column,
            index_type,
        } => {
            let parsed = parse_index_type_arg(index_type)?;
            let request = RemoteColumnarIndexCreateRequest {
                segment_id: segment.clone(),
                column: column.clone(),
                index_type: parsed.as_str().to_string(),
            };
            let response: RemoteColumnarStatusResponse = client
                .post_json("columnar/index/create", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                write_status_if_needed(
                    writer,
                    &format!("Created columnar index: {}:{}", segment, column),
                )
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to create columnar index".to_string(),
                ))
            }
        }
        IndexCommand::List { segment } => {
            let request = RemoteColumnarIndexListRequest {
                segment_id: segment.clone(),
            };
            let response: RemoteColumnarIndexListResponse = client
                .post_json("columnar/index/list", &request)
                .await
                .map_err(map_client_error)?;
            writer.prepare(Some(response.indexes.len()))?;
            for entry in response.indexes {
                let row = Row::new(vec![
                    Value::Text(entry.column),
                    Value::Text(entry.index_type),
                ]);
                match writer.write_row(row)? {
                    WriteStatus::LimitReached => break,
                    WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
                }
            }
            writer.finish()?;
            Ok(())
        }
        IndexCommand::Drop { segment, column } => {
            let request = RemoteColumnarIndexDropRequest {
                segment_id: segment.clone(),
                column: column.clone(),
            };
            let response: RemoteColumnarStatusResponse = client
                .post_json("columnar/index/drop", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                write_status_if_needed(
                    writer,
                    &format!("Dropped columnar index: {}:{}", segment, column),
                )
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to drop columnar index".to_string(),
                ))
            }
        }
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

/// Execute a columnar scan command on a specific segment.
///
/// FR-7 Compliance: Uses streaming API to avoid materializing all rows upfront.
/// Rows are yielded one at a time from `ColumnarRowIterator`.
fn execute_scan<W: Write>(
    db: &Database,
    segment_id: &str,
    progress: bool,
    batch_mode: &BatchMode,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut progress_indicator = ProgressIndicator::new(
        batch_mode,
        progress,
        writer.is_quiet(),
        format!("Scanning segment '{}'...", segment_id),
    );

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

    progress_indicator.finish_with_message(format!("done ({} rows).", row_count));

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

#[derive(Debug, Serialize)]
struct IngestResult {
    row_count: usize,
    segment_id: String,
    size_bytes: u64,
    compression: String,
    elapsed_ms: u64,
}

struct IngestOptions<'a> {
    file: &'a Path,
    table: &'a str,
    delimiter: char,
    header: bool,
    compression: &'a str,
    row_group_size: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompressionType {
    Lz4,
    Zstd,
    None,
}

enum ColumnBuilder {
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Bool(Vec<bool>),
    Binary(Vec<Vec<u8>>),
}

impl ColumnBuilder {
    fn from_arrow_type(dt: &ArrowDataType) -> Result<(Self, LogicalType)> {
        match dt {
            ArrowDataType::Int32 | ArrowDataType::Int64 => {
                Ok((Self::Int64(Vec::new()), LogicalType::Int64))
            }
            ArrowDataType::Float32 => Ok((Self::Float32(Vec::new()), LogicalType::Float32)),
            ArrowDataType::Float64 => Ok((Self::Float64(Vec::new()), LogicalType::Float64)),
            ArrowDataType::Boolean => Ok((Self::Bool(Vec::new()), LogicalType::Bool)),
            ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8 => Ok((Self::Binary(Vec::new()), LogicalType::Binary)),
            other => Err(CliError::InvalidArgument(format!(
                "Unsupported Parquet type: {other:?}"
            ))),
        }
    }

    fn append_array(&mut self, array: &dyn Array, dt: &ArrowDataType) -> Result<()> {
        match (self, dt) {
            (ColumnBuilder::Int64(values), ArrowDataType::Int32) => {
                let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    CliError::InvalidArgument("Failed to read Int32 column".to_string())
                })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(0);
                    } else {
                        values.push(arr.value(idx) as i64);
                    }
                }
            }
            (ColumnBuilder::Int64(values), ArrowDataType::Int64) => {
                let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    CliError::InvalidArgument("Failed to read Int64 column".to_string())
                })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(0);
                    } else {
                        values.push(arr.value(idx));
                    }
                }
            }
            (ColumnBuilder::Float32(values), ArrowDataType::Float32) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        CliError::InvalidArgument("Failed to read Float32 column".to_string())
                    })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(0.0);
                    } else {
                        values.push(arr.value(idx));
                    }
                }
            }
            (ColumnBuilder::Float64(values), ArrowDataType::Float64) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        CliError::InvalidArgument("Failed to read Float64 column".to_string())
                    })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(0.0);
                    } else {
                        values.push(arr.value(idx));
                    }
                }
            }
            (ColumnBuilder::Bool(values), ArrowDataType::Boolean) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        CliError::InvalidArgument("Failed to read Boolean column".to_string())
                    })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(false);
                    } else {
                        values.push(arr.value(idx));
                    }
                }
            }
            (ColumnBuilder::Binary(values), ArrowDataType::Binary) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        CliError::InvalidArgument("Failed to read Binary column".to_string())
                    })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Vec::new());
                    } else {
                        values.push(arr.value(idx).to_vec());
                    }
                }
            }
            (ColumnBuilder::Binary(values), ArrowDataType::LargeBinary) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        CliError::InvalidArgument("Failed to read LargeBinary column".to_string())
                    })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Vec::new());
                    } else {
                        values.push(arr.value(idx).to_vec());
                    }
                }
            }
            (ColumnBuilder::Binary(values), ArrowDataType::Utf8) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        CliError::InvalidArgument("Failed to read Utf8 column".to_string())
                    })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Vec::new());
                    } else {
                        values.push(arr.value(idx).as_bytes().to_vec());
                    }
                }
            }
            (ColumnBuilder::Binary(values), ArrowDataType::LargeUtf8) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        CliError::InvalidArgument("Failed to read LargeUtf8 column".to_string())
                    })?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Vec::new());
                    } else {
                        values.push(arr.value(idx).as_bytes().to_vec());
                    }
                }
            }
            _ => {
                return Err(CliError::InvalidArgument(
                    "Parquet schema mismatch detected".to_string(),
                ))
            }
        }
        Ok(())
    }

    fn finish(self) -> ColumnData {
        match self {
            ColumnBuilder::Int64(values) => ColumnData::Int64(values),
            ColumnBuilder::Float32(values) => ColumnData::Float32(values),
            ColumnBuilder::Float64(values) => ColumnData::Float64(values),
            ColumnBuilder::Bool(values) => ColumnData::Bool(values),
            ColumnBuilder::Binary(values) => ColumnData::Binary(values),
        }
    }
}

fn execute_ingest<W: Write>(
    db: &Database,
    options: IngestOptions<'_>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let start = Instant::now();
    let extension = options
        .file
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let batch = match extension.as_str() {
        "csv" => parse_csv(options.file, options.delimiter, options.header)?,
        "parquet" | "pq" => parse_parquet(options.file)?,
        _ => {
            return Err(CliError::InvalidArgument(format!(
                "Unsupported file format: {}",
                options.file.display()
            )))
        }
    };

    let compression_type = parse_compression_arg(options.compression)?;
    let row_count = batch.num_rows();
    let mut config = SegmentConfigV2::default();
    if let Some(size) = options.row_group_size {
        config.row_group_size = size as u64;
    }
    config.compression = map_compression(compression_type);

    let seg_id = db.write_columnar_segment_with_config(options.table, batch, config)?;
    let segment_id = format!("{}:{}", table_id(options.table)?, seg_id);
    let stats = db.get_columnar_segment_stats(&segment_id)?;
    let result = IngestResult {
        row_count,
        segment_id,
        size_bytes: stats.size_bytes as u64,
        compression: compression_as_str(compression_type).to_string(),
        elapsed_ms: start.elapsed().as_millis() as u64,
    };

    writer.prepare(Some(1))?;
    let row = Row::new(vec![
        Value::Int(result.row_count as i64),
        Value::Text(result.segment_id),
        Value::Int(result.size_bytes as i64),
        Value::Text(result.compression),
        Value::Int(result.elapsed_ms as i64),
    ]);
    writer.write_row(row)?;
    writer.finish()?;
    Ok(())
}

fn execute_index_command<W: Write>(
    db: &Database,
    cmd: IndexCommand,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match cmd {
        IndexCommand::Create {
            segment,
            column,
            index_type,
        } => {
            let index_type = parse_index_type_arg(&index_type)?;
            db.create_columnar_index(&segment, &column, index_type)?;
            write_status_if_needed(
                writer,
                &format!("Created index {} on {}", index_type.as_str(), column),
            )
        }
        IndexCommand::List { segment } => {
            let entries = db.list_columnar_indexes(&segment)?;

            writer.prepare(Some(entries.len()))?;
            for entry in entries {
                let row = Row::new(vec![
                    Value::Text(entry.column),
                    Value::Text(entry.index_type.as_str().to_string()),
                ]);
                match writer.write_row(row)? {
                    WriteStatus::LimitReached => break,
                    WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
                }
            }
            writer.finish()?;
            Ok(())
        }
        IndexCommand::Drop { segment, column } => {
            db.drop_columnar_index(&segment, &column)?;
            write_status_if_needed(writer, &format!("Dropped index on {}", column))
        }
    }
}

fn parse_csv(path: &Path, delimiter: char, has_header: bool) -> Result<RecordBatch> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .has_headers(has_header)
        .from_path(path)
        .map_err(|err| {
            CliError::InvalidArgument(format!(
                "Failed to open CSV file '{}': {}",
                path.display(),
                err
            ))
        })?;

    let mut column_names: Vec<String> = if has_header {
        reader
            .headers()
            .map_err(|err| {
                CliError::InvalidArgument(format!(
                    "Failed to read CSV header '{}': {}",
                    path.display(),
                    err
                ))
            })?
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        Vec::new()
    };

    let mut columns: Vec<Vec<Vec<u8>>> = Vec::new();

    for record in reader.records() {
        let record = record.map_err(|err| {
            CliError::InvalidArgument(format!(
                "Failed to read CSV record '{}': {}",
                path.display(),
                err
            ))
        })?;
        if columns.is_empty() {
            if !has_header {
                column_names = (0..record.len()).map(|i| format!("col_{}", i)).collect();
            }
            columns = vec![Vec::new(); column_names.len()];
        }
        if record.len() != columns.len() {
            return Err(CliError::InvalidArgument(format!(
                "CSV column count mismatch: expected {}, got {}",
                columns.len(),
                record.len()
            )));
        }
        for (idx, field) in record.iter().enumerate() {
            columns[idx].push(field.as_bytes().to_vec());
        }
    }

    if columns.is_empty() {
        if has_header && !column_names.is_empty() {
            columns = vec![Vec::new(); column_names.len()];
        } else {
            return Err(CliError::InvalidArgument(format!(
                "CSV file '{}' is empty",
                path.display()
            )));
        }
    }

    let schema = Schema {
        columns: column_names
            .into_iter()
            .map(|name| ColumnSchema {
                name,
                logical_type: LogicalType::Binary,
                nullable: false,
                fixed_len: None,
            })
            .collect(),
    };
    let columns = columns
        .into_iter()
        .map(ColumnData::Binary)
        .collect::<Vec<_>>();
    let null_bitmaps = vec![None; columns.len()];
    Ok(RecordBatch::new(schema, columns, null_bitmaps))
}

fn parse_parquet(path: &Path) -> Result<RecordBatch> {
    let file = std::fs::File::open(path).map_err(|err| {
        CliError::InvalidArgument(format!(
            "Failed to open Parquet file '{}': {}",
            path.display(),
            err
        ))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        CliError::InvalidArgument(format!(
            "Failed to read Parquet metadata '{}': {}",
            path.display(),
            err
        ))
    })?;

    let arrow_schema = builder.schema().clone();
    let mut schemas = Vec::with_capacity(arrow_schema.fields().len());
    let mut builders = Vec::with_capacity(arrow_schema.fields().len());
    for field in arrow_schema.fields() {
        let (builder, logical_type) = ColumnBuilder::from_arrow_type(field.data_type())?;
        schemas.push(ColumnSchema {
            name: field.name().clone(),
            logical_type,
            nullable: field.is_nullable(),
            fixed_len: None,
        });
        builders.push(builder);
    }

    let reader = builder.with_batch_size(1024).build().map_err(|err| {
        CliError::InvalidArgument(format!(
            "Failed to create Parquet reader '{}': {}",
            path.display(),
            err
        ))
    })?;

    for batch in reader {
        let batch = batch.map_err(|err| {
            CliError::InvalidArgument(format!(
                "Failed to read Parquet batch '{}': {}",
                path.display(),
                err
            ))
        })?;
        for (col_idx, array) in batch.columns().iter().enumerate() {
            let dt = arrow_schema.field(col_idx).data_type();
            builders[col_idx].append_array(array.as_ref(), dt)?;
        }
    }

    let columns = builders.into_iter().map(|b| b.finish()).collect::<Vec<_>>();
    let null_bitmaps = vec![None; columns.len()];
    let schema = Schema { columns: schemas };
    Ok(RecordBatch::new(schema, columns, null_bitmaps))
}

fn parse_compression_arg(raw: &str) -> Result<CompressionType> {
    match raw {
        "lz4" => Ok(CompressionType::Lz4),
        "zstd" => Ok(CompressionType::Zstd),
        "none" => Ok(CompressionType::None),
        other => Err(CliError::UnknownCompressionType(other.to_string())),
    }
}

fn parse_index_type_arg(raw: &str) -> Result<ColumnarIndexType> {
    match raw {
        "minmax" => Ok(ColumnarIndexType::Minmax),
        "bloom" => Ok(ColumnarIndexType::Bloom),
        other => Err(CliError::UnknownIndexType(other.to_string())),
    }
}

fn map_compression(compression: CompressionType) -> CompressionV2 {
    match compression {
        CompressionType::Lz4 => CompressionV2::Lz4,
        CompressionType::Zstd => CompressionV2::Zstd { level: 3 },
        CompressionType::None => CompressionV2::None,
    }
}

fn compression_as_str(compression: CompressionType) -> &'static str {
    match compression {
        CompressionType::Lz4 => "lz4",
        CompressionType::Zstd => "zstd",
        CompressionType::None => "none",
    }
}

fn table_id(table: &str) -> Result<u32> {
    if table.is_empty() {
        return Err(CliError::InvalidArgument(
            "Table name is required".to_string(),
        ));
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table.hash(&mut hasher);
    Ok((hasher.finish() & 0xffff_ffff) as u32)
}

fn write_status_if_needed<W: Write>(writer: &mut StreamingWriter<W>, message: &str) -> Result<()> {
    if writer.is_quiet() {
        return Ok(());
    }

    writer.prepare(Some(1))?;
    let row = Row::new(vec![
        Value::Text("OK".to_string()),
        Value::Text(message.to_string()),
    ]);
    writer.write_row(row)?;
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

/// Create columns for columnar ingest output.
pub fn columnar_ingest_columns() -> Vec<Column> {
    vec![
        Column::new("row_count", DataType::Int),
        Column::new("segment_id", DataType::Text),
        Column::new("size_bytes", DataType::Int),
        Column::new("compression", DataType::Text),
        Column::new("elapsed_ms", DataType::Int),
    ]
}

/// Create columns for columnar index list output.
pub fn columnar_index_list_columns() -> Vec<Column> {
    vec![
        Column::new("column", DataType::Text),
        Column::new("index_type", DataType::Text),
    ]
}

/// Create columns for columnar status output.
pub fn columnar_status_columns() -> Vec<Column> {
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

    #[test]
    fn test_parse_compression_arg_valid() {
        assert!(matches!(
            parse_compression_arg("lz4").unwrap(),
            CompressionType::Lz4
        ));
        assert!(matches!(
            parse_compression_arg("zstd").unwrap(),
            CompressionType::Zstd
        ));
        assert!(matches!(
            parse_compression_arg("none").unwrap(),
            CompressionType::None
        ));
    }

    #[test]
    fn test_parse_compression_arg_invalid() {
        let err = parse_compression_arg("snappy").unwrap_err();
        assert!(matches!(
            err,
            CliError::UnknownCompressionType(value) if value == "snappy"
        ));
    }

    #[test]
    fn test_parse_index_type_arg_valid() {
        assert!(matches!(
            parse_index_type_arg("minmax").unwrap(),
            ColumnarIndexType::Minmax
        ));
        assert!(matches!(
            parse_index_type_arg("bloom").unwrap(),
            ColumnarIndexType::Bloom
        ));
    }

    #[test]
    fn test_parse_index_type_arg_invalid() {
        let err = parse_index_type_arg("bitmap").unwrap_err();
        assert!(matches!(
            err,
            CliError::UnknownIndexType(value) if value == "bitmap"
        ));
    }
}
