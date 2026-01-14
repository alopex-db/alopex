//! Connect command - remote server connection over HTTP
//!
//! Provides an interactive SQL prompt backed by the server HTTP API.

use std::io::{self, BufRead, Read, Write};

use alopex_sql::{AlopexDialect, Parser, StatementKind};
use reqwest::blocking::Client;
use reqwest::Url;
use serde::Deserialize;

use crate::batch::BatchMode;
use crate::cli::OutputFormat;
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::create_formatter;
use crate::streaming::{StreamingWriter, WriteStatus};

#[derive(Deserialize)]
struct ColumnInfo {
    name: String,
    data_type: String,
}

#[derive(Deserialize)]
struct SqlResponse {
    columns: Vec<ColumnInfo>,
    rows: Vec<Vec<alopex_sql::SqlValue>>,
    affected_rows: Option<u64>,
}

#[derive(Deserialize)]
struct StreamError {
    code: String,
    message: String,
    correlation_id: String,
}

#[derive(Deserialize)]
struct StreamItem {
    row: Option<Vec<alopex_sql::SqlValue>>,
    error: Option<StreamError>,
    done: bool,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
    correlation_id: String,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error: ErrorBody,
}

pub fn execute_connect(
    server_url: &str,
    api_key: Option<&str>,
    batch_mode: &BatchMode,
    output: OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let client = Client::new();
    let sql_url = build_sql_url(server_url)?;

    if batch_mode.is_tty {
        let stdin = io::stdin();
        let mut stdout = io::stdout();
        loop {
            if !quiet {
                print!("alopex> ");
                stdout.flush()?;
            }
            let mut line = String::new();
            if stdin.lock().read_line(&mut line)? == 0 {
                break;
            }
            let sql = line.trim();
            if sql.is_empty() {
                continue;
            }
            if matches!(sql, "exit" | "quit" | "\\q") {
                break;
            }
            run_sql(&client, sql_url.clone(), api_key, sql, output, limit, quiet)?;
        }
        Ok(())
    } else {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        let sql = input.trim();
        if sql.is_empty() {
            return Err(CliError::NoQueryProvided);
        }
        run_sql(&client, sql_url, api_key, sql, output, limit, quiet)
    }
}

fn build_sql_url(server_url: &str) -> Result<Url> {
    let mut url = Url::parse(server_url)
        .map_err(|err| CliError::InvalidArgument(format!("Invalid server URL: {err}")))?;
    let mut path = url.path().to_string();
    if !path.ends_with('/') {
        path.push('/');
    }
    url.set_path(&path);
    url.join("sql")
        .map_err(|err| CliError::InvalidArgument(format!("Invalid server URL: {err}")))
}

fn run_sql(
    client: &Client,
    url: Url,
    api_key: Option<&str>,
    sql: &str,
    output: OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let formatter = create_formatter(output);
    let is_select = is_select_sql(sql)?;
    let streaming = is_select && formatter.supports_streaming();

    let mut request = client.post(url).json(&serde_json::json!({
        "sql": sql,
        "streaming": streaming
    }));
    if let Some(key) = api_key {
        request = request.header("x-api-key", key);
    }

    let response = request
        .send()
        .map_err(|err| CliError::InvalidArgument(format!("Connection error: {err}")))?;
    if !response.status().is_success() {
        return Err(parse_error_response(response));
    }

    if streaming {
        return handle_streaming_response(response, formatter, limit, quiet);
    }

    let payload: SqlResponse = response
        .json()
        .map_err(|err| CliError::InvalidArgument(format!("Invalid response: {err}")))?;
    handle_sql_response(payload, formatter, limit, quiet)
}

fn handle_sql_response(
    payload: SqlResponse,
    formatter: Box<dyn crate::output::formatter::Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    if payload.columns.is_empty() {
        if quiet {
            return Ok(());
        }
        let message = match payload.affected_rows {
            Some(count) => format!("{count} row(s) affected"),
            None => "Operation completed successfully".to_string(),
        };
        return write_status(formatter, limit, message);
    }

    let columns: Vec<Column> = payload
        .columns
        .iter()
        .map(|col| Column::new(&col.name, data_type_from_string(&col.data_type)))
        .collect();
    let mut writer =
        StreamingWriter::new(io::stdout(), formatter, columns, limit).with_quiet(quiet);
    writer.prepare(Some(payload.rows.len()))?;
    for row in payload.rows {
        let values = row.into_iter().map(sql_value_to_value).collect();
        match writer.write_row(Row::new(values))? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
    }
    writer.finish()
}

fn handle_streaming_response(
    response: reqwest::blocking::Response,
    formatter: Box<dyn crate::output::formatter::Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let mut reader = io::BufReader::new(response);
    let mut line = String::new();
    let mut writer: Option<StreamingWriter<std::io::Stdout>> = None;
    let mut formatter = Some(formatter);
    let mut saw_row = false;

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        let item: StreamItem = serde_json::from_str(trimmed)?;
        if let Some(error) = item.error {
            return Err(CliError::InvalidArgument(format!(
                "Server error {}: {} (correlation_id={})",
                error.code, error.message, error.correlation_id
            )));
        }
        if let Some(row) = item.row {
            if writer.is_none() {
                let columns = row
                    .iter()
                    .enumerate()
                    .map(|(idx, value)| {
                        Column::new(format!("col{}", idx + 1), data_type_from_value(value))
                    })
                    .collect::<Vec<_>>();
                let formatter = formatter.take().ok_or_else(|| {
                    CliError::InvalidArgument("stream formatter missing".to_string())
                })?;
                let mut new_writer =
                    StreamingWriter::new(io::stdout(), formatter, columns, limit).with_quiet(quiet);
                new_writer.prepare(None)?;
                writer = Some(new_writer);
            }
            saw_row = true;
            let values = row.into_iter().map(sql_value_to_value).collect();
            if let Some(writer) = writer.as_mut() {
                match writer.write_row(Row::new(values))? {
                    WriteStatus::LimitReached => break,
                    WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
                }
            }
        }
        if item.done {
            break;
        }
    }

    if let Some(writer) = writer.as_mut() {
        writer.finish()?;
    } else if !quiet && !saw_row {
        println!("OK (0 rows)");
    }
    Ok(())
}

fn write_status(
    formatter: Box<dyn crate::output::formatter::Formatter>,
    limit: Option<usize>,
    message: String,
) -> Result<()> {
    let columns = vec![
        Column::new("status", DataType::Text),
        Column::new("message", DataType::Text),
    ];
    let mut writer = StreamingWriter::new(io::stdout(), formatter, columns, limit);
    writer.prepare(Some(1))?;
    let row = Row::new(vec![Value::Text("OK".to_string()), Value::Text(message)]);
    writer.write_row(row)?;
    writer.finish()
}

fn data_type_from_string(value: &str) -> DataType {
    let upper = value.to_ascii_uppercase();
    if upper.starts_with("INT") || upper.starts_with("BIGINT") {
        DataType::Int
    } else if upper.starts_with("FLOAT") || upper.starts_with("DOUBLE") {
        DataType::Float
    } else if upper.starts_with("BLOB") {
        DataType::Bytes
    } else if upper.starts_with("BOOLEAN") {
        DataType::Bool
    } else if upper.starts_with("VECTOR") {
        DataType::Vector
    } else {
        DataType::Text
    }
}

fn data_type_from_value(value: &alopex_sql::SqlValue) -> DataType {
    match value {
        alopex_sql::SqlValue::Null => DataType::Text,
        alopex_sql::SqlValue::Integer(_) | alopex_sql::SqlValue::BigInt(_) => DataType::Int,
        alopex_sql::SqlValue::Float(_) | alopex_sql::SqlValue::Double(_) => DataType::Float,
        alopex_sql::SqlValue::Text(_) => DataType::Text,
        alopex_sql::SqlValue::Blob(_) => DataType::Bytes,
        alopex_sql::SqlValue::Boolean(_) => DataType::Bool,
        alopex_sql::SqlValue::Timestamp(_) => DataType::Text,
        alopex_sql::SqlValue::Vector(_) => DataType::Vector,
    }
}

fn sql_value_to_value(value: alopex_sql::SqlValue) -> Value {
    match value {
        alopex_sql::SqlValue::Null => Value::Null,
        alopex_sql::SqlValue::Integer(v) => Value::Int(i64::from(v)),
        alopex_sql::SqlValue::BigInt(v) => Value::Int(v),
        alopex_sql::SqlValue::Float(v) => Value::Float(f64::from(v)),
        alopex_sql::SqlValue::Double(v) => Value::Float(v),
        alopex_sql::SqlValue::Text(v) => Value::Text(v),
        alopex_sql::SqlValue::Blob(v) => Value::Bytes(v),
        alopex_sql::SqlValue::Boolean(v) => Value::Bool(v),
        alopex_sql::SqlValue::Timestamp(v) => Value::Text(v.to_string()),
        alopex_sql::SqlValue::Vector(v) => Value::Vector(v),
    }
}

fn parse_error_response(response: reqwest::blocking::Response) -> CliError {
    let status = response.status();
    let text = response.text().unwrap_or_default();
    if let Ok(error) = serde_json::from_str::<ErrorResponse>(&text) {
        return CliError::InvalidArgument(format!(
            "Server error {}: {} (correlation_id={})",
            error.error.code, error.error.message, error.error.correlation_id
        ));
    }
    CliError::InvalidArgument(format!("Server error: HTTP {} - {}", status, text))
}

fn is_select_sql(sql: &str) -> Result<bool> {
    let dialect = AlopexDialect;
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| CliError::Parse(format!("{e}")))?;
    Ok(statements.len() == 1
        && matches!(
            statements.first().map(|s| &s.kind),
            Some(StatementKind::Select(_))
        ))
}
