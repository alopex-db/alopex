//! KV Command - Key-Value operations
//!
//! Supports: get, put, delete, list

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use alopex_embedded::{
    Database, KeyPattern, KeySearchPage, KeySearchRequest, TransactionManager as Transaction,
    TxnMode,
};
use serde::{Deserialize, Serialize};

use crate::batch::BatchMode;
use crate::cli::{KvCommand, KvSearchMode, KvTxnCommand, OutputFormat};
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::Formatter;
use crate::output::RowCollector;
use crate::streaming::{StreamingWriter, WriteStatus};
use crate::tui::admin::{AdminBackend, AdminContext, AdminTarget, AuthCapabilities};
use crate::tui::renderer::render_output;

const DEFAULT_TXN_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Serialize)]
struct RemoteKvGetRequest {
    key: String,
}

#[derive(Debug, Serialize)]
struct RemoteKvPutRequest {
    key: String,
    value: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct RemoteKvDeleteRequest {
    key: String,
}

#[derive(Debug, Serialize)]
struct RemoteKvListRequest {
    prefix: Option<String>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnBeginRequest {
    timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnGetRequest {
    txn_id: String,
    key: String,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnPutRequest {
    txn_id: String,
    key: String,
    value: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnDeleteRequest {
    txn_id: String,
    key: String,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnCommitRequest {
    txn_id: String,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnRollbackRequest {
    txn_id: String,
}

#[derive(Debug, Deserialize)]
struct RemoteKvGetResponse {
    value: Option<Vec<u8>>,
}

#[derive(Debug, Deserialize)]
struct RemoteKvListEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct RemoteKvListResponse {
    entries: Vec<RemoteKvListEntry>,
}

#[derive(Debug, Deserialize)]
struct RemoteKvStatusResponse {
    success: bool,
}

#[derive(Debug, Deserialize)]
struct RemoteKvTxnBeginResponse {
    txn_id: String,
}

/// Execute a KV command.
///
/// # Arguments
///
/// * `db` - The database instance.
/// * `cmd` - The KV subcommand to execute.
/// * `writer` - The streaming writer for output.
pub fn execute<W: Write>(
    db: &Database,
    cmd: KvCommand,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match cmd {
        KvCommand::Get { key } => execute_get(db, &key, writer),
        KvCommand::Put { key, value } => execute_put(db, &key, &value, writer),
        KvCommand::Delete { key } => execute_delete(db, &key, writer),
        KvCommand::List { prefix } => execute_list(db, prefix.as_deref(), writer),
        KvCommand::Search {
            mode,
            pattern,
            pattern_hex,
            cursor_hex,
            page_size,
            scan_budget,
            max_bytes,
        } => execute_search(
            db,
            mode,
            &pattern,
            pattern_hex,
            cursor_hex.as_deref(),
            page_size,
            scan_budget,
            max_bytes,
            writer,
        ),
        KvCommand::Txn(cmd) => execute_txn_command(db, cmd, writer),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn execute_tui(
    db: &Database,
    cmd: KvCommand,
    batch_mode: &BatchMode,
    output_format: OutputFormat,
    columns: Vec<Column>,
    limit: Option<usize>,
    quiet: bool,
    connection_label: impl Into<String>,
    data_dir: Option<PathBuf>,
) -> Result<()> {
    let connection_label = connection_label.into();
    let context_message = Some(kv_command_context(&cmd));
    let admin_label = connection_label.clone();
    let admin_data_dir = data_dir.clone();
    let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> = Some(Box::new(move || {
        let connection_label = admin_label.clone();
        let data_dir = admin_data_dir.clone();
        crate::tui::admin::run_admin_ui(AdminContext {
            connection_label,
            auth: AuthCapabilities::full(),
            backend: AdminBackend::Local {
                db,
                batch_mode,
                output_format,
                limit,
                quiet,
                data_dir,
            },
            initial_target: Some(AdminTarget::Kv),
        })
    }));
    let collector = RowCollector::new();
    let formatter = Box::new(collector.formatter());
    let mut sink = std::io::sink();
    let mut writer =
        StreamingWriter::new(&mut sink, formatter, columns.clone(), limit).with_quiet(quiet);
    execute(db, cmd, &mut writer)?;
    let warning = collector.truncation_warning();
    render_output(
        columns,
        collector.rows(),
        connection_label,
        context_message,
        true,
        warning,
        output_format,
        admin_launcher,
    )
}

/// Execute a KV command against a remote server.
pub async fn execute_remote_with_formatter<W: Write>(
    client: &HttpClient,
    cmd: &KvCommand,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    match cmd {
        KvCommand::Get { key } => {
            let request = RemoteKvGetRequest { key: key.clone() };
            let response: RemoteKvGetResponse = client
                .post_json("kv/get", &request)
                .await
                .map_err(map_client_error)?;
            let Some(value) = response.value else {
                return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
            };

            let columns = kv_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            streaming_writer.prepare(Some(1))?;
            let row = Row::new(vec![Value::Text(key.clone()), bytes_to_value(value)]);
            streaming_writer.write_row(row)?;
            streaming_writer.finish()
        }
        KvCommand::Put { key, value } => {
            let request = RemoteKvPutRequest {
                key: key.clone(),
                value: value.as_bytes().to_vec(),
            };
            let response: RemoteKvStatusResponse = client
                .post_json("kv/put", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                if quiet {
                    return Ok(());
                }
                let columns = kv_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                streaming_writer.prepare(Some(1))?;
                let row = Row::new(vec![
                    Value::Text("OK".to_string()),
                    Value::Text(format!("Set key: {}", key)),
                ]);
                streaming_writer.write_row(row)?;
                streaming_writer.finish()
            } else {
                Err(CliError::InvalidArgument("Failed to set key".to_string()))
            }
        }
        KvCommand::Delete { key } => {
            let request = RemoteKvDeleteRequest { key: key.clone() };
            let response: RemoteKvStatusResponse = client
                .post_json("kv/delete", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                if quiet {
                    return Ok(());
                }
                let columns = kv_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                streaming_writer.prepare(Some(1))?;
                let row = Row::new(vec![
                    Value::Text("OK".to_string()),
                    Value::Text(format!("Deleted key: {}", key)),
                ]);
                streaming_writer.write_row(row)?;
                streaming_writer.finish()
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to delete key".to_string(),
                ))
            }
        }
        KvCommand::List { prefix } => {
            let request = RemoteKvListRequest {
                prefix: prefix.clone(),
            };
            let response: RemoteKvListResponse = client
                .post_json("kv/list", &request)
                .await
                .map_err(map_client_error)?;
            let columns = kv_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            streaming_writer.prepare(Some(response.entries.len()))?;
            for entry in response.entries {
                let row = Row::new(vec![bytes_to_value(entry.key), bytes_to_value(entry.value)]);
                match streaming_writer.write_row(row)? {
                    WriteStatus::LimitReached => break,
                    WriteStatus::Continue => {}
                }
            }
            streaming_writer.finish()
        }
        KvCommand::Search {
            mode,
            pattern,
            pattern_hex,
            cursor_hex,
            page_size,
            scan_budget,
            max_bytes,
        } => {
            let request = search_request(
                *mode,
                pattern,
                *pattern_hex,
                cursor_hex.as_deref(),
                limit.map_or(*page_size, |limit| (*page_size).min(limit)),
                *scan_budget,
                *max_bytes,
            )?;
            let page: KeySearchPage = client
                .post_json("kv/search", &request)
                .await
                .map_err(map_client_error)?;
            let columns = kv_search_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            write_search_page(page, &mut streaming_writer)
        }
        KvCommand::Txn(txn_cmd) => {
            execute_remote_txn_command(client, txn_cmd, writer, formatter, limit, quiet).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_remote_tui<'a>(
    client: &HttpClient,
    cmd: &KvCommand,
    columns: Vec<Column>,
    output_format: OutputFormat,
    limit: Option<usize>,
    quiet: bool,
    connection_label: impl Into<String>,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
) -> Result<()> {
    let collector = RowCollector::new();
    let formatter = Box::new(collector.formatter());
    let mut sink = std::io::sink();
    execute_remote_with_formatter(client, cmd, &mut sink, formatter, limit, quiet).await?;
    let warning = collector.truncation_warning();
    render_output(
        columns,
        collector.rows(),
        connection_label,
        Some(kv_command_context(cmd)),
        true,
        warning,
        output_format,
        admin_launcher,
    )
}

async fn execute_remote_txn_command<W: Write>(
    client: &HttpClient,
    cmd: &KvTxnCommand,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    match cmd {
        KvTxnCommand::Begin { timeout_secs } => {
            let request = RemoteKvTxnBeginRequest {
                timeout_secs: *timeout_secs,
            };
            let response: RemoteKvTxnBeginResponse = client
                .post_json("kv/txn/begin", &request)
                .await
                .map_err(map_client_error)?;
            let columns = kv_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            streaming_writer.prepare(Some(1))?;
            let row = Row::new(vec![
                Value::Text("txn_id".to_string()),
                Value::Text(response.txn_id),
            ]);
            streaming_writer.write_row(row)?;
            streaming_writer.finish()
        }
        KvTxnCommand::Get { key, txn_id } => {
            let request = RemoteKvTxnGetRequest {
                txn_id: txn_id.clone(),
                key: key.clone(),
            };
            let response: RemoteKvGetResponse = client
                .post_json("kv/txn/get", &request)
                .await
                .map_err(map_client_error)?;
            let Some(value) = response.value else {
                return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
            };
            let columns = kv_columns();
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            streaming_writer.prepare(Some(1))?;
            let row = Row::new(vec![Value::Text(key.clone()), bytes_to_value(value)]);
            streaming_writer.write_row(row)?;
            streaming_writer.finish()
        }
        KvTxnCommand::Put { key, value, txn_id } => {
            let request = RemoteKvTxnPutRequest {
                txn_id: txn_id.clone(),
                key: key.clone(),
                value: value.as_bytes().to_vec(),
            };
            let response: RemoteKvStatusResponse = client
                .post_json("kv/txn/put", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                let columns = kv_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                write_status_if_needed(&mut streaming_writer, &format!("Staged key: {}", key))
            } else {
                Err(CliError::InvalidArgument("Failed to stage key".to_string()))
            }
        }
        KvTxnCommand::Delete { key, txn_id } => {
            let request = RemoteKvTxnDeleteRequest {
                txn_id: txn_id.clone(),
                key: key.clone(),
            };
            let response: RemoteKvStatusResponse = client
                .post_json("kv/txn/delete", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                let columns = kv_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                write_status_if_needed(&mut streaming_writer, &format!("Staged delete: {}", key))
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to stage delete".to_string(),
                ))
            }
        }
        KvTxnCommand::Commit { txn_id } => {
            let request = RemoteKvTxnCommitRequest {
                txn_id: txn_id.clone(),
            };
            let response: RemoteKvStatusResponse = client
                .post_json("kv/txn/commit", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                let columns = kv_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                write_status_if_needed(
                    &mut streaming_writer,
                    &format!("Committed transaction: {}", txn_id),
                )
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to commit transaction".to_string(),
                ))
            }
        }
        KvTxnCommand::Rollback { txn_id } => {
            let request = RemoteKvTxnRollbackRequest {
                txn_id: txn_id.clone(),
            };
            let response: RemoteKvStatusResponse = client
                .post_json("kv/txn/rollback", &request)
                .await
                .map_err(map_client_error)?;
            if response.success {
                let columns = kv_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                write_status_if_needed(
                    &mut streaming_writer,
                    &format!("Rolled back transaction: {}", txn_id),
                )
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to rollback transaction".to_string(),
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

fn kv_command_context(cmd: &KvCommand) -> String {
    match cmd {
        KvCommand::Get { key } => format!("kv get {key}"),
        KvCommand::Put { key, .. } => format!("kv put {key}"),
        KvCommand::Delete { key } => format!("kv delete {key}"),
        KvCommand::List { prefix } => match prefix {
            Some(prefix) => format!("kv list --prefix {prefix}"),
            None => "kv list".to_string(),
        },
        KvCommand::Search { mode, pattern, .. } => {
            format!("kv search --mode {mode:?} {pattern}")
        }
        KvCommand::Txn(command) => match command {
            KvTxnCommand::Begin { timeout_secs } => match timeout_secs {
                Some(secs) => format!("kv txn begin --timeout-secs {secs}"),
                None => "kv txn begin".to_string(),
            },
            KvTxnCommand::Get { key, txn_id } => {
                format!("kv txn get {key} --txn-id {txn_id}")
            }
            KvTxnCommand::Put { key, txn_id, .. } => {
                format!("kv txn put {key} --txn-id {txn_id}")
            }
            KvTxnCommand::Delete { key, txn_id } => {
                format!("kv txn delete {key} --txn-id {txn_id}")
            }
            KvTxnCommand::Commit { txn_id } => format!("kv txn commit --txn-id {txn_id}"),
            KvTxnCommand::Rollback { txn_id } => format!("kv txn rollback --txn-id {txn_id}"),
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_search<W: Write>(
    db: &Database,
    mode: KvSearchMode,
    pattern: &str,
    pattern_hex: bool,
    cursor_hex: Option<&str>,
    page_size: usize,
    scan_budget: usize,
    max_bytes: usize,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let page_size = writer
        .remaining_limit()
        .map_or(page_size, |limit| page_size.min(limit));
    let request = search_request(
        mode,
        pattern,
        pattern_hex,
        cursor_hex,
        page_size,
        scan_budget,
        max_bytes,
    )?;
    let mut txn = db.begin(TxnMode::ReadOnly)?;
    let page = txn.search_keys(&request)?;
    txn.commit()?;
    write_search_page(page, writer)
}

fn search_request(
    mode: KvSearchMode,
    pattern: &str,
    pattern_hex: bool,
    cursor_hex: Option<&str>,
    page_size: usize,
    scan_budget: usize,
    max_bytes: usize,
) -> Result<KeySearchRequest> {
    let pattern = match (mode, pattern_hex) {
        (KvSearchMode::Glob, false) => KeyPattern::glob(pattern.as_bytes()),
        (KvSearchMode::Glob, true) => KeyPattern::glob(decode_hex("pattern", pattern)?),
        (KvSearchMode::Regex, false) => KeyPattern::regex(pattern),
        (KvSearchMode::Regex, true) => {
            return Err(CliError::InvalidArgument(
                "--pattern-hex is only valid with --mode glob".into(),
            ));
        }
    };
    let mut request =
        KeySearchRequest::new(pattern, page_size, scan_budget).with_max_bytes(max_bytes);
    if let Some(cursor) = cursor_hex {
        request.cursor = Some(decode_hex("cursor", cursor)?);
    }
    Ok(request)
}

fn write_search_page<W: Write>(page: KeySearchPage, writer: &mut StreamingWriter<W>) -> Result<()> {
    writer.prepare(Some(page.entries.len()))?;
    let cursor = page
        .next_cursor
        .as_deref()
        .map(encode_hex)
        .map(Value::Text)
        .unwrap_or(Value::Null);
    for entry in page.entries {
        let row = Row::new(vec![
            bytes_to_value(entry.key),
            bytes_to_value(entry.value),
            cursor.clone(),
        ]);
        if writer.write_row(row)? == WriteStatus::LimitReached {
            break;
        }
    }
    writer.finish()
}

fn decode_hex(name: &str, value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(CliError::InvalidArgument(format!(
            "{name} hex must contain an even number of digits"
        )));
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex input is UTF-8");
            u8::from_str_radix(text, 16).map_err(|_| {
                CliError::InvalidArgument(format!("{name} contains invalid hex: {text}"))
            })
        })
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn bytes_to_value(bytes: Vec<u8>) -> Value {
    match std::str::from_utf8(&bytes) {
        Ok(s) => Value::Text(s.to_string()),
        Err(_) => Value::Bytes(bytes),
    }
}

fn map_txn_error(txn_id: &str, err: alopex_embedded::Error) -> CliError {
    match err {
        alopex_embedded::Error::InvalidTransactionId(_) => {
            CliError::InvalidTransactionId(txn_id.to_string())
        }
        other => CliError::Database(other),
    }
}

fn map_txn_result<T>(
    txn_id: &str,
    result: std::result::Result<T, alopex_embedded::Error>,
) -> Result<T> {
    result.map_err(|err| map_txn_error(txn_id, err))
}

fn ensure_txn_not_expired(db: &Database, txn_id: &str) -> Result<()> {
    let expired = map_txn_result(txn_id, Transaction::is_expired(db, txn_id))?;
    if expired {
        let _ = Transaction::rollback(db, txn_id);
        return Err(CliError::TransactionTimeout(txn_id.to_string()));
    }
    Ok(())
}

fn execute_txn_command<W: Write>(
    db: &Database,
    cmd: KvTxnCommand,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match cmd {
        KvTxnCommand::Begin { timeout_secs } => execute_txn_begin(db, timeout_secs, writer),
        KvTxnCommand::Get { key, txn_id } => execute_txn_get(db, &key, &txn_id, writer),
        KvTxnCommand::Put { key, value, txn_id } => {
            execute_txn_put(db, &key, &value, &txn_id, writer)
        }
        KvTxnCommand::Delete { key, txn_id } => execute_txn_delete(db, &key, &txn_id, writer),
        KvTxnCommand::Commit { txn_id } => execute_txn_commit(db, &txn_id, writer),
        KvTxnCommand::Rollback { txn_id } => execute_txn_rollback(db, &txn_id, writer),
    }
}

fn execute_txn_begin<W: Write>(
    db: &Database,
    timeout_secs: Option<u64>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let timeout = Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_TXN_TIMEOUT_SECS));
    let txn_id = Transaction::begin_with_timeout(db, timeout)?;

    writer.prepare(Some(1))?;
    let row = Row::new(vec![Value::Text("txn_id".to_string()), Value::Text(txn_id)]);
    writer.write_row(row)?;
    writer.finish()?;
    Ok(())
}

fn execute_txn_get<W: Write>(
    db: &Database,
    key: &str,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    ensure_txn_not_expired(db, txn_id)?;
    let value = map_txn_result(txn_id, Transaction::get(db, txn_id, key.as_bytes()))?;
    write_kv_value(key, value, writer)
}

fn execute_txn_put<W: Write>(
    db: &Database,
    key: &str,
    value: &str,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    ensure_txn_not_expired(db, txn_id)?;
    map_txn_result(
        txn_id,
        Transaction::put(db, txn_id, key.as_bytes(), value.as_bytes()),
    )?;

    write_status_if_needed(writer, &format!("Staged key: {}", key))
}

fn execute_txn_delete<W: Write>(
    db: &Database,
    key: &str,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    ensure_txn_not_expired(db, txn_id)?;
    map_txn_result(txn_id, Transaction::delete(db, txn_id, key.as_bytes()))?;

    write_status_if_needed(writer, &format!("Staged delete: {}", key))
}

fn execute_txn_commit<W: Write>(
    db: &Database,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    ensure_txn_not_expired(db, txn_id)?;
    map_txn_result(txn_id, Transaction::commit(db, txn_id))?;

    write_status_if_needed(writer, &format!("Committed transaction: {}", txn_id))
}

fn execute_txn_rollback<W: Write>(
    db: &Database,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    map_txn_result(txn_id, Transaction::rollback(db, txn_id))?;

    write_status_if_needed(writer, &format!("Rolled back transaction: {}", txn_id))
}

fn write_kv_value<W: Write>(
    key: &str,
    value: Option<Vec<u8>>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let Some(value) = value else {
        return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
    };

    let value_display = match std::str::from_utf8(&value) {
        Ok(s) => Value::Text(s.to_string()),
        Err(_) => Value::Bytes(value),
    };
    writer.prepare(Some(1))?;
    let row = Row::new(vec![Value::Text(key.to_string()), value_display]);
    writer.write_row(row)?;
    writer.finish()?;
    Ok(())
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

/// Execute a KV get command.
fn execute_get<W: Write>(db: &Database, key: &str, writer: &mut StreamingWriter<W>) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadOnly)?;
    let result = txn.get(key.as_bytes())?;
    txn.commit()?;

    // Prepare writer with hint of 1 row
    writer.prepare(Some(1))?;

    match result {
        Some(value) => {
            // Try to interpret value as UTF-8 text, fallback to hex
            let value_display = match std::str::from_utf8(&value) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Bytes(value),
            };

            let row = Row::new(vec![Value::Text(key.to_string()), value_display]);
            writer.write_row(row)?;
        }
        None => {
            return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
        }
    }

    writer.finish()?;
    Ok(())
}

/// Execute a KV put command.
fn execute_put<W: Write>(
    db: &Database,
    key: &str,
    value: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadWrite)?;
    txn.put(key.as_bytes(), value.as_bytes())?;
    txn.commit()?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Stored key: {}", key)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Execute a KV delete command.
fn execute_delete<W: Write>(
    db: &Database,
    key: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadWrite)?;

    // Check if key exists first
    let exists = txn.get(key.as_bytes())?.is_some();

    if !exists {
        txn.rollback()?;
        return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
    }

    txn.delete(key.as_bytes())?;
    txn.commit()?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Deleted key: {}", key)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Execute a KV list command.
fn execute_list<W: Write>(
    db: &Database,
    prefix: Option<&str>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadOnly)?;

    let prefix_bytes = prefix.map(|p| p.as_bytes()).unwrap_or(b"");
    let iter = txn.scan_prefix(prefix_bytes)?;

    // Prepare writer before streaming (no hint since we don't know count ahead of time)
    writer.prepare(None)?;

    // Stream results directly without collecting to Vec
    for (key, value) in iter {
        // Try to interpret key and value as UTF-8 text
        let key_display = match std::str::from_utf8(&key) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(key),
        };
        let value_display = match std::str::from_utf8(&value) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(value),
        };

        let row = Row::new(vec![key_display, value_display]);
        match writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue => {}
        }
    }

    txn.commit()?;
    writer.finish()?;
    Ok(())
}

/// Create columns for KV output.
pub fn kv_columns() -> Vec<Column> {
    vec![
        Column::new("key", DataType::Text),
        Column::new("value", DataType::Text),
    ]
}

/// Create columns for a bounded KV search page.
pub fn kv_search_columns() -> Vec<Column> {
    vec![
        Column::new("key", DataType::Text),
        Column::new("value", DataType::Text),
        Column::new("next_cursor_hex", DataType::Text),
    ]
}

/// Create columns for KV status output.
pub fn kv_status_columns() -> Vec<Column> {
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

    fn create_test_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = kv_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn create_status_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = kv_status_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn create_search_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        StreamingWriter::new(
            output,
            Box::new(JsonlFormatter::new()),
            kv_search_columns(),
            None,
        )
    }

    #[test]
    fn test_put_and_get() {
        let db = create_test_db();

        // Put
        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_put(&db, "test_key", "test_value", &mut writer).unwrap();
        }

        // Get
        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_get(&db, "test_key", &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("test_key"));
        assert!(result.contains("test_value"));
    }

    #[test]
    fn test_get_not_found() {
        let db = create_test_db();

        let mut output = Vec::new();
        let mut writer = create_test_writer(&mut output);

        let result = execute_get(&db, "nonexistent", &mut writer);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_delete() {
        let db = create_test_db();

        // Put first
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_put(&db, "to_delete", "value", &mut writer).unwrap();
        }

        // Delete
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_delete(&db, "to_delete", &mut writer).unwrap();
        }

        // Verify deletion
        {
            let mut output = Vec::new();
            let mut writer = create_test_writer(&mut output);
            let result = execute_get(&db, "to_delete", &mut writer);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_delete_not_found() {
        let db = create_test_db();

        let mut output = Vec::new();
        let mut writer = create_status_writer(&mut output);

        let result = execute_delete(&db, "nonexistent", &mut writer);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_list_all() {
        let db = create_test_db();

        // Put some keys
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.put(b"key1", b"value1").unwrap();
            txn.put(b"key2", b"value2").unwrap();
            txn.put(b"key3", b"value3").unwrap();
            txn.commit().unwrap();
        }

        // List all
        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_list(&db, None, &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("key1"));
        assert!(result.contains("key2"));
        assert!(result.contains("key3"));
    }

    #[test]
    fn test_list_with_prefix() {
        let db = create_test_db();

        // Put some keys
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.put(b"user:1", b"Alice").unwrap();
            txn.put(b"user:2", b"Bob").unwrap();
            txn.put(b"item:1", b"Widget").unwrap();
            txn.commit().unwrap();
        }

        // List with prefix
        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_list(&db, Some("user:"), &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("user:1"));
        assert!(result.contains("user:2"));
        assert!(!result.contains("item:1"));
    }

    #[test]
    fn test_list_empty() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_list(&db, None, &mut writer).unwrap();
        }

        // Should complete without error even with no results
        let result = String::from_utf8(output).unwrap();
        // Empty output is fine
        assert!(result.is_empty() || result.lines().count() == 0);
    }

    #[test]
    fn test_search_accepts_binary_glob_and_emits_cursor() {
        let db = create_test_db();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(&[0xff, b'/', b'a'], b"a").unwrap();
        txn.put(&[0xff, b'/', b'b'], b"b").unwrap();
        txn.commit().unwrap();

        let mut output = Vec::new();
        let mut writer = create_search_writer(&mut output);
        execute_search(
            &db,
            KvSearchMode::Glob,
            "ff2f2a",
            true,
            None,
            1,
            10,
            1024,
            &mut writer,
        )
        .unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("ff2f61"), "missing binary cursor: {output}");
    }

    #[test]
    fn test_search_output_limit_becomes_the_page_boundary() {
        let db = create_test_db();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        for key in [b"a1".as_slice(), b"a2", b"a3"] {
            txn.put(key, key).unwrap();
        }
        txn.commit().unwrap();

        let mut output = Vec::new();
        let mut writer = StreamingWriter::new(
            &mut output,
            Box::new(JsonlFormatter::new()),
            kv_search_columns(),
            Some(1),
        );
        execute_search(
            &db,
            KvSearchMode::Glob,
            "a*",
            false,
            None,
            3,
            10,
            1024,
            &mut writer,
        )
        .unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("6131"));
        assert!(!output.contains("6132"));
    }
}
