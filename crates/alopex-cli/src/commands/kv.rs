//! KV Command - Key-Value operations
//!
//! Supports: get, put, delete, list

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use alopex_embedded::{Database, TransactionManager as Transaction, TxnMode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::batch::{BatchMode, TransactionCliOutcome};
use crate::cli::{KvCommand, KvTxnCommand, OutputFormat};
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
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnGetRequest {
    txn_id: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnPutRequest {
    txn_id: String,
    key: String,
    value: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnDeleteRequest {
    txn_id: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnCommitRequest {
    txn_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RemoteKvTxnRollbackRequest {
    txn_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RemoteKvGetResponse {
    value: Option<Vec<u8>>,
    #[serde(default)]
    transaction: Option<JsonValue>,
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
    #[serde(default)]
    transaction: Option<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct RemoteKvTxnBeginResponse {
    txn_id: String,
    #[serde(default)]
    transaction: Option<JsonValue>,
}

enum RemoteTransactionResponse<T> {
    Success(T),
    Failure { status: u16, document: JsonValue },
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
        KvTxnCommand::Begin {
            timeout_secs,
            request_id,
        } => {
            let request = RemoteKvTxnBeginRequest {
                timeout_secs: *timeout_secs,
                request_id: request_id.clone(),
            };
            let response = match post_transaction_json::<_, RemoteKvTxnBeginResponse>(
                client,
                "kv/txn/begin",
                &request,
            )
            .await?
            {
                RemoteTransactionResponse::Success(response) => response,
                RemoteTransactionResponse::Failure { status, document } => {
                    return render_remote_transaction_failure(
                        writer, formatter, limit, quiet, status, &document,
                    );
                }
            };
            let mut columns = kv_columns();
            append_transaction_column(&mut columns, response.transaction.as_ref());
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            streaming_writer.prepare(Some(1))?;
            let mut values = vec![
                Value::Text("txn_id".to_string()),
                Value::Text(response.txn_id),
            ];
            append_transaction_value(&mut values, response.transaction.as_ref());
            streaming_writer.write_row(Row::new(values))?;
            streaming_writer.finish()?;
            classify_transaction_response(response.transaction.as_ref(), 200)
        }
        KvTxnCommand::Get {
            key,
            txn_id,
            request_id,
        } => {
            let request = RemoteKvTxnGetRequest {
                txn_id: txn_id.clone(),
                key: key.clone(),
                request_id: request_id.clone(),
            };
            let response = match post_transaction_json::<_, RemoteKvGetResponse>(
                client,
                "kv/txn/get",
                &request,
            )
            .await?
            {
                RemoteTransactionResponse::Success(response) => response,
                RemoteTransactionResponse::Failure { status, document } => {
                    return render_remote_transaction_failure(
                        writer, formatter, limit, quiet, status, &document,
                    );
                }
            };
            write_remote_kv_value(
                writer,
                formatter,
                limit,
                quiet,
                key,
                response.value,
                response.transaction.as_ref(),
            )?;
            classify_transaction_response(response.transaction.as_ref(), 200)
        }
        KvTxnCommand::Put {
            key,
            value,
            txn_id,
            request_id,
        } => {
            let request = RemoteKvTxnPutRequest {
                txn_id: txn_id.clone(),
                key: key.clone(),
                value: value.as_bytes().to_vec(),
                request_id: request_id.clone(),
            };
            let response = match post_transaction_json::<_, RemoteKvStatusResponse>(
                client,
                "kv/txn/put",
                &request,
            )
            .await?
            {
                RemoteTransactionResponse::Success(response) => response,
                RemoteTransactionResponse::Failure { status, document } => {
                    return render_remote_transaction_failure(
                        writer, formatter, limit, quiet, status, &document,
                    );
                }
            };
            if response.success {
                write_remote_transaction_status(
                    writer,
                    formatter,
                    limit,
                    quiet,
                    "OK",
                    &format!("Staged key: {}", key),
                    response.transaction.as_ref(),
                )?;
                classify_transaction_response(response.transaction.as_ref(), 200)
            } else {
                Err(CliError::InvalidArgument("Failed to stage key".to_string()))
            }
        }
        KvTxnCommand::Delete {
            key,
            txn_id,
            request_id,
        } => {
            let request = RemoteKvTxnDeleteRequest {
                txn_id: txn_id.clone(),
                key: key.clone(),
                request_id: request_id.clone(),
            };
            let response = match post_transaction_json::<_, RemoteKvStatusResponse>(
                client,
                "kv/txn/delete",
                &request,
            )
            .await?
            {
                RemoteTransactionResponse::Success(response) => response,
                RemoteTransactionResponse::Failure { status, document } => {
                    return render_remote_transaction_failure(
                        writer, formatter, limit, quiet, status, &document,
                    );
                }
            };
            if response.success {
                write_remote_transaction_status(
                    writer,
                    formatter,
                    limit,
                    quiet,
                    "OK",
                    &format!("Staged delete: {}", key),
                    response.transaction.as_ref(),
                )?;
                classify_transaction_response(response.transaction.as_ref(), 200)
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to stage delete".to_string(),
                ))
            }
        }
        KvTxnCommand::Commit { txn_id, request_id } => {
            let request = RemoteKvTxnCommitRequest {
                txn_id: txn_id.clone(),
                request_id: request_id.clone(),
            };
            let response = match post_transaction_json::<_, RemoteKvStatusResponse>(
                client,
                "kv/txn/commit",
                &request,
            )
            .await
            {
                Ok(RemoteTransactionResponse::Success(response)) => response,
                Ok(RemoteTransactionResponse::Failure { status, document }) => {
                    return render_remote_transaction_failure(
                        writer, formatter, limit, quiet, status, &document,
                    );
                }
                Err(error) => return Err(error),
            };
            if response.success {
                write_remote_transaction_status(
                    writer,
                    formatter,
                    limit,
                    quiet,
                    "OK",
                    &format!("Committed transaction: {}", txn_id),
                    response.transaction.as_ref(),
                )?;
                classify_transaction_response(response.transaction.as_ref(), 200)
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to commit transaction".to_string(),
                ))
            }
        }
        KvTxnCommand::Rollback { txn_id, request_id } => {
            let request = RemoteKvTxnRollbackRequest {
                txn_id: txn_id.clone(),
                request_id: request_id.clone(),
            };
            let response = match post_transaction_json::<_, RemoteKvStatusResponse>(
                client,
                "kv/txn/rollback",
                &request,
            )
            .await
            {
                Ok(RemoteTransactionResponse::Success(response)) => response,
                Ok(RemoteTransactionResponse::Failure { status, document }) => {
                    return render_remote_transaction_failure(
                        writer, formatter, limit, quiet, status, &document,
                    );
                }
                Err(error) => return Err(error),
            };
            if response.success {
                write_remote_transaction_status(
                    writer,
                    formatter,
                    limit,
                    quiet,
                    "OK",
                    &format!("Rolled back transaction: {}", txn_id),
                    response.transaction.as_ref(),
                )?;
                classify_transaction_response(response.transaction.as_ref(), 200)
            } else {
                Err(CliError::InvalidArgument(
                    "Failed to rollback transaction".to_string(),
                ))
            }
        }
    }
}

async fn post_transaction_json<B: Serialize, T: DeserializeOwned>(
    client: &HttpClient,
    path: &str,
    request: &B,
) -> Result<RemoteTransactionResponse<T>> {
    let raw = client
        .post_json_raw(path, request)
        .await
        .map_err(map_client_error)?;
    let status = raw.status.as_u16();
    let document = serde_json::from_str(&raw.body).unwrap_or(JsonValue::String(raw.body));
    if (200..300).contains(&status) {
        return serde_json::from_value(document)
            .map(RemoteTransactionResponse::Success)
            .map_err(CliError::Json);
    }
    Ok(RemoteTransactionResponse::Failure { status, document })
}

fn render_remote_transaction_failure<W: Write>(
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
    status: u16,
    document: &JsonValue,
) -> Result<()> {
    if let Some(transaction) = document.get("transaction") {
        let message = transaction_reason(document, transaction, status);
        write_remote_transaction_status(
            writer,
            formatter,
            limit,
            quiet,
            "ERROR",
            &message,
            Some(transaction),
        )?;
    }
    Err(transaction_response_error(status, document))
}

fn transaction_response_error(status: u16, document: &JsonValue) -> CliError {
    let Some(transaction) = document.get("transaction") else {
        return CliError::InvalidArgument(format!("Server error: HTTP {status} - {document}"));
    };
    let outcome = TransactionCliOutcome::from_transaction(Some(transaction), status);
    CliError::TransactionOutcome {
        outcome: outcome.as_str().to_string(),
        reason: transaction_reason(document, transaction, status),
        exit_code: outcome.exit_code(),
    }
}

fn classify_transaction_response(transaction: Option<&JsonValue>, status: u16) -> Result<()> {
    let Some(transaction) = transaction else {
        return Ok(());
    };
    let outcome = TransactionCliOutcome::from_transaction(Some(transaction), status);
    if outcome == TransactionCliOutcome::Success {
        return Ok(());
    }
    let reason = transaction
        .get("reason_code")
        .and_then(JsonValue::as_str)
        .unwrap_or("transaction outcome is not committed")
        .to_string();
    Err(CliError::TransactionOutcome {
        outcome: outcome.as_str().to_string(),
        reason,
        exit_code: outcome.exit_code(),
    })
}

fn transaction_reason(document: &JsonValue, transaction: &JsonValue, status: u16) -> String {
    transaction
        .get("reason_code")
        .and_then(JsonValue::as_str)
        .or_else(|| {
            document
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(JsonValue::as_str)
        })
        .map(str::to_string)
        .unwrap_or_else(|| format!("HTTP {status}"))
}

fn append_transaction_column(columns: &mut Vec<Column>, transaction: Option<&JsonValue>) {
    if transaction.is_some() {
        columns.push(Column::new("Transaction", DataType::Text));
    }
}

fn append_transaction_value(values: &mut Vec<Value>, transaction: Option<&JsonValue>) {
    if let Some(transaction) = transaction {
        let canonical = serde_json::to_string(transaction)
            .unwrap_or_else(|_| "{\"outcome\":\"serialization_error\"}".to_string());
        values.push(Value::Text(canonical));
    }
}

fn write_remote_kv_value<W: Write>(
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
    key: &str,
    value: Option<Vec<u8>>,
    transaction: Option<&JsonValue>,
) -> Result<()> {
    let Some(value) = value else {
        return Err(CliError::InvalidArgument(format!("Key not found: {key}")));
    };
    let mut columns = kv_columns();
    append_transaction_column(&mut columns, transaction);
    let mut streaming_writer =
        StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
    streaming_writer.prepare(Some(1))?;
    let mut values = vec![Value::Text(key.to_string()), bytes_to_value(value)];
    append_transaction_value(&mut values, transaction);
    streaming_writer.write_row(Row::new(values))?;
    streaming_writer.finish()
}

fn write_remote_transaction_status<W: Write>(
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
    status: &str,
    message: &str,
    transaction: Option<&JsonValue>,
) -> Result<()> {
    if quiet {
        return Ok(());
    }
    let mut columns = kv_status_columns();
    append_transaction_column(&mut columns, transaction);
    let mut streaming_writer = StreamingWriter::new(writer, formatter, columns, limit);
    streaming_writer.prepare(Some(1))?;
    let mut values = vec![
        Value::Text(status.to_string()),
        Value::Text(message.to_string()),
    ];
    append_transaction_value(&mut values, transaction);
    streaming_writer.write_row(Row::new(values))?;
    streaming_writer.finish()
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
            let document = serde_json::from_str::<JsonValue>(&body).ok();
            let transaction = document
                .as_ref()
                .and_then(|document| document.get("transaction"));
            if let Some(transaction) = transaction {
                let outcome =
                    TransactionCliOutcome::from_transaction(Some(transaction), status.as_u16());
                let reason = document
                    .as_ref()
                    .and_then(|document| {
                        document
                            .get("error")
                            .and_then(|error| error.get("message"))
                            .and_then(JsonValue::as_str)
                    })
                    .or_else(|| transaction.get("reason_code").and_then(JsonValue::as_str))
                    .unwrap_or("transaction request failed")
                    .to_string();
                CliError::TransactionOutcome {
                    outcome: outcome.as_str().to_string(),
                    reason,
                    exit_code: outcome.exit_code(),
                }
            } else {
                CliError::InvalidArgument(format!(
                    "Server error: HTTP {} - {}",
                    status.as_u16(),
                    body
                ))
            }
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
        KvCommand::Txn(command) => match command {
            KvTxnCommand::Begin { timeout_secs, .. } => match timeout_secs {
                Some(secs) => format!("kv txn begin --timeout-secs {secs}"),
                None => "kv txn begin".to_string(),
            },
            KvTxnCommand::Get { key, txn_id, .. } => {
                format!("kv txn get {key} --txn-id {txn_id}")
            }
            KvTxnCommand::Put { key, txn_id, .. } => {
                format!("kv txn put {key} --txn-id {txn_id}")
            }
            KvTxnCommand::Delete { key, txn_id, .. } => {
                format!("kv txn delete {key} --txn-id {txn_id}")
            }
            KvTxnCommand::Commit { txn_id, .. } => format!("kv txn commit --txn-id {txn_id}"),
            KvTxnCommand::Rollback { txn_id, .. } => {
                format!("kv txn rollback --txn-id {txn_id}")
            }
        },
    }
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
        KvTxnCommand::Begin { timeout_secs, .. } => execute_txn_begin(db, timeout_secs, writer),
        KvTxnCommand::Get { key, txn_id, .. } => execute_txn_get(db, &key, &txn_id, writer),
        KvTxnCommand::Put {
            key, value, txn_id, ..
        } => execute_txn_put(db, &key, &value, &txn_id, writer),
        KvTxnCommand::Delete { key, txn_id, .. } => execute_txn_delete(db, &key, &txn_id, writer),
        KvTxnCommand::Commit { txn_id, .. } => execute_txn_commit(db, &txn_id, writer),
        KvTxnCommand::Rollback { txn_id, .. } => execute_txn_rollback(db, &txn_id, writer),
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
    fn transaction_request_serialization_and_nonterminal_classification_are_stable() {
        let request = RemoteKvTxnCommitRequest {
            txn_id: "txn-1".to_string(),
            request_id: Some("commit-retry-1".to_string()),
        };
        let document = serde_json::to_value(request).unwrap();
        assert_eq!(document["txn_id"], "txn-1");
        assert_eq!(document["request_id"], "commit-retry-1");

        let pending = serde_json::json!({
            "outcome_version": "v0.9",
            "transaction_id": "txn-1",
            "request_id": "commit-retry-1",
            "participating_ranges": [],
            "state": "running",
            "failure_class": null,
            "reason_code": "local_kv_transaction_write",
            "routing": {"kind": "single_range"},
            "retryable": false,
            "idempotency": {
                "operation_id": "txn-1",
                "request_id": "commit-retry-1",
                "state": "running",
            },
        });
        let error = classify_transaction_response(Some(&pending), 200).unwrap_err();
        assert!(matches!(
            error,
            CliError::TransactionOutcome {
                exit_code: crate::batch::ExitCode::Warning,
                ..
            }
        ));

        let mut columns = kv_status_columns();
        append_transaction_column(&mut columns, Some(&pending));
        assert_eq!(columns.last().unwrap().name, "Transaction");
    }
}
