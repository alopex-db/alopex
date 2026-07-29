//! CRDT command handlers.
//!
//! This module is a thin adapter over the shared Phase 2 operation envelope
//! and outcome. It does not implement a Counter merge rule: the embedded and
//! server boundaries both delegate to the same durable projection.

use std::io::Write;

use alopex_cluster::crdt::{CrdtOperationEnvelope, CrdtOutcome, CrdtPayload};
use alopex_cluster::{CrdtOperationKind, RangeIdentity};
use alopex_embedded::Database;
use serde::Serialize;

use crate::batch::ExitCode;
use crate::cli::{CounterCommand, CrdtCommand, OutputFormat, SetCommand};
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::create_formatter;

/// Execute a CRDT command against an embedded database.
pub fn execute_local<W: Write>(
    db: &Database,
    command: CrdtCommand,
    writer: &mut W,
    output_format: OutputFormat,
    quiet: bool,
) -> Result<()> {
    match command {
        CrdtCommand::Counter { command } => {
            let command = command.ok_or_else(|| {
                CliError::InvalidArgument("Missing CRDT Counter subcommand".to_string())
            })?;
            match command {
                CounterCommand::Create {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    initial_value,
                    actor,
                } => {
                    let range = RangeIdentity::new(
                        cluster_id,
                        table_id,
                        range_id,
                        None,
                        None,
                        schema_version,
                        data_epoch,
                    );
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        range,
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::CounterCreate,
                        CrdtPayload::Counter {
                            initial_value: Some(initial_value),
                            delta: None,
                        },
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.create_counter(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                CounterCommand::Read {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    actor,
                } => {
                    let range = RangeIdentity::new(
                        cluster_id,
                        table_id,
                        range_id,
                        None,
                        None,
                        schema_version,
                        data_epoch,
                    );
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        range,
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::CounterRead,
                        CrdtPayload::None,
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.read_counter(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                CounterCommand::Increment {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    delta,
                    actor,
                } => {
                    let range = RangeIdentity::new(
                        cluster_id,
                        table_id,
                        range_id,
                        None,
                        None,
                        schema_version,
                        data_epoch,
                    );
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        range,
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::CounterIncrement,
                        CrdtPayload::Counter {
                            initial_value: None,
                            delta: Some(delta),
                        },
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.increment_counter(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                CounterCommand::Decrement {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    delta,
                    actor,
                } => {
                    let range = RangeIdentity::new(
                        cluster_id,
                        table_id,
                        range_id,
                        None,
                        None,
                        schema_version,
                        data_epoch,
                    );
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        range,
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::CounterDecrement,
                        CrdtPayload::Counter {
                            initial_value: None,
                            delta: Some(delta),
                        },
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.decrement_counter(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
            }
        }
        CrdtCommand::Set { command } => {
            let command = command.ok_or_else(|| {
                CliError::InvalidArgument("Missing CRDT Set subcommand".to_string())
            })?;
            match command {
                SetCommand::Create {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    actor,
                } => {
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        RangeIdentity::new(
                            cluster_id,
                            table_id,
                            range_id,
                            None,
                            None,
                            schema_version,
                            data_epoch,
                        ),
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::SetCreate,
                        CrdtPayload::None,
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.create_set(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                SetCommand::Add {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    member,
                    actor,
                } => {
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        RangeIdentity::new(
                            cluster_id,
                            table_id,
                            range_id,
                            None,
                            None,
                            schema_version,
                            data_epoch,
                        ),
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::SetAdd,
                        CrdtPayload::Set {
                            member: Some(member),
                        },
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.add_set(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                SetCommand::Remove {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    member,
                    actor,
                } => {
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        RangeIdentity::new(
                            cluster_id,
                            table_id,
                            range_id,
                            None,
                            None,
                            schema_version,
                            data_epoch,
                        ),
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::SetRemove,
                        CrdtPayload::Set {
                            member: Some(member),
                        },
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.remove_set(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                SetCommand::Read {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    actor,
                } => {
                    let envelope = CrdtOperationEnvelope::new(
                        object_id,
                        RangeIdentity::new(
                            cluster_id,
                            table_id,
                            range_id,
                            None,
                            None,
                            schema_version,
                            data_epoch,
                        ),
                        actor,
                        request_id,
                        operation_id,
                        update_version,
                        CrdtOperationKind::SetRead,
                        CrdtPayload::None,
                    )
                    .map_err(|error| CliError::InvalidArgument(error.to_string()))?;
                    let outcome = db.read_set(envelope)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
            }
        }
    }
}

/// Execute a CRDT command through the existing authenticated HTTP client.
///
/// The server derives actor identity from its transport authentication context;
/// `--actor` is deliberately not serialized on this path.
pub async fn execute_remote<W: Write>(
    client: &HttpClient,
    command: &CrdtCommand,
    writer: &mut W,
    output_format: OutputFormat,
    quiet: bool,
) -> Result<()> {
    match command {
        CrdtCommand::Counter { command } => {
            let command = command.as_ref().ok_or_else(|| {
                CliError::InvalidArgument("Missing CRDT Counter subcommand".to_string())
            })?;
            match command {
                CounterCommand::Create {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    initial_value,
                    ..
                } => {
                    let request = RemoteCounterCreateRequest {
                        object_id,
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                        initial_value: *initial_value,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json("api/crdt/counters", &request)
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                CounterCommand::Read {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    ..
                } => {
                    let request = RemoteCounterReadRequest {
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json(&format!("api/crdt/counters/{object_id}/read"), &request)
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                CounterCommand::Increment {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    delta,
                    ..
                } => {
                    let request = RemoteCounterIncrementRequest {
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                        delta: *delta,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json(
                            &format!("api/crdt/counters/{object_id}/increment"),
                            &request,
                        )
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                CounterCommand::Decrement {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    delta,
                    ..
                } => {
                    let request = RemoteCounterDecrementRequest {
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                        delta: *delta,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json(
                            &format!("api/crdt/counters/{object_id}/decrement"),
                            &request,
                        )
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
            }
        }
        CrdtCommand::Set { command } => {
            let command = command.as_ref().ok_or_else(|| {
                CliError::InvalidArgument("Missing CRDT Set subcommand".to_string())
            })?;
            match command {
                SetCommand::Create {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    ..
                } => {
                    let request = RemoteSetCreateRequest {
                        object_id,
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json("api/crdt/sets", &request)
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                SetCommand::Add {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    member,
                    ..
                } => {
                    let request = RemoteSetAddRequest {
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                        member,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json(&format!("api/crdt/sets/{object_id}/add"), &request)
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                SetCommand::Remove {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    member,
                    ..
                } => {
                    let request = RemoteSetRemoveRequest {
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                        member,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json(&format!("api/crdt/sets/{object_id}/remove"), &request)
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
                SetCommand::Read {
                    object_id,
                    cluster_id,
                    table_id,
                    range_id,
                    schema_version,
                    data_epoch,
                    request_id,
                    operation_id,
                    update_version,
                    ..
                } => {
                    let request = RemoteSetReadRequest {
                        range: RangeIdentity::new(
                            cluster_id.clone(),
                            *table_id,
                            range_id.clone(),
                            None,
                            None,
                            *schema_version,
                            *data_epoch,
                        ),
                        request_id,
                        operation_id,
                        update_version: *update_version,
                    };
                    let outcome: CrdtOutcome = client
                        .post_json(&format!("api/crdt/sets/{object_id}/read"), &request)
                        .await
                        .map_err(map_client_error)?;
                    render_and_classify(&outcome, writer, output_format, quiet)
                }
            }
        }
    }
}

#[derive(Serialize)]
struct RemoteCounterCreateRequest<'a> {
    object_id: &'a str,
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
    initial_value: i64,
}

#[derive(Serialize)]
struct RemoteSetCreateRequest<'a> {
    object_id: &'a str,
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
}

#[derive(Serialize)]
struct RemoteSetReadRequest<'a> {
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
}

#[derive(Serialize)]
struct RemoteSetAddRequest<'a> {
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
    member: &'a str,
}

#[derive(Serialize)]
struct RemoteSetRemoveRequest<'a> {
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
    member: &'a str,
}

#[derive(Serialize)]
struct RemoteCounterReadRequest<'a> {
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
}

#[derive(Serialize)]
struct RemoteCounterIncrementRequest<'a> {
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
    delta: i64,
}

#[derive(Serialize)]
struct RemoteCounterDecrementRequest<'a> {
    range: RangeIdentity,
    request_id: &'a str,
    operation_id: &'a str,
    update_version: u64,
    delta: i64,
}

fn render_and_classify<W: Write>(
    outcome: &CrdtOutcome,
    writer: &mut W,
    output_format: OutputFormat,
    quiet: bool,
) -> Result<()> {
    if !quiet {
        render_outcome(outcome, writer, output_format)?;
    }

    let status = outcome.surface_status();
    if status.cli_exit_code == 0 {
        return Ok(());
    }

    Err(CliError::CrdtOutcome {
        outcome: serde_json::to_string(&outcome.common().state)?,
        reason: outcome.common().routing.reason_code.clone(),
        exit_code: exit_code(status.cli_exit_code),
    })
}

fn render_outcome<W: Write>(
    outcome: &CrdtOutcome,
    writer: &mut W,
    output_format: OutputFormat,
) -> Result<()> {
    match output_format {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut *writer, &[outcome])?;
            writeln!(writer)?;
            Ok(())
        }
        OutputFormat::Jsonl => {
            serde_json::to_writer(&mut *writer, outcome)?;
            writeln!(writer)?;
            Ok(())
        }
        OutputFormat::Table | OutputFormat::Csv | OutputFormat::Tsv => {
            let columns = outcome_columns();
            let row = outcome_row(outcome)?;
            let mut formatter = create_formatter(output_format);
            formatter.write_header(writer, &columns)?;
            formatter.write_row(writer, &row)?;
            formatter.write_footer(writer)
        }
    }
}

fn outcome_columns() -> Vec<Column> {
    [
        ("object_type", DataType::Text),
        ("object_id", DataType::Text),
        ("range", DataType::Text),
        ("state_epoch", DataType::Int),
        ("actor", DataType::Text),
        ("request_id", DataType::Text),
        ("operation_id", DataType::Text),
        ("state", DataType::Text),
        ("failure_class", DataType::Text),
        ("routing", DataType::Text),
        ("retryable", DataType::Bool),
        ("idempotency", DataType::Text),
        ("value", DataType::Text),
        ("value_unavailable", DataType::Text),
        ("membership_unavailable", DataType::Text),
    ]
    .into_iter()
    .map(|(name, data_type)| Column::new(name, data_type))
    .collect()
}

fn outcome_row(outcome: &CrdtOutcome) -> Result<Row> {
    let document = serde_json::to_value(outcome)?;
    Ok(Row::new(vec![
        text_field(&document, "object_type"),
        text_field(&document, "object_id"),
        json_field(&document, "range"),
        integer_field(&document, "state_epoch"),
        text_field(&document, "actor"),
        text_field(&document, "request_id"),
        text_field(&document, "operation_id"),
        text_field(&document, "state"),
        optional_text_field(&document, "failure_class"),
        json_field(&document, "routing"),
        Value::Bool(document["retryable"].as_bool().unwrap_or(false)),
        json_field(&document, "idempotency"),
        optional_json_field(&document, "value"),
        optional_text_field(&document, "value_unavailable"),
        optional_text_field(&document, "membership_unavailable"),
    ]))
}

fn text_field(document: &serde_json::Value, field: &str) -> Value {
    document
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(|value| Value::Text(value.to_string()))
        .unwrap_or(Value::Null)
}

fn optional_text_field(document: &serde_json::Value, field: &str) -> Value {
    text_field(document, field)
}

fn integer_field(document: &serde_json::Value, field: &str) -> Value {
    document
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .map(Value::Int)
        .unwrap_or(Value::Null)
}

fn json_field(document: &serde_json::Value, field: &str) -> Value {
    Value::Text(document[field].to_string())
}

fn optional_json_field(document: &serde_json::Value, field: &str) -> Value {
    document
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| Value::Text(value.to_string()))
        .unwrap_or(Value::Null)
}

fn exit_code(code: i32) -> ExitCode {
    match code {
        0 => ExitCode::Success,
        1 => ExitCode::Error,
        2 => ExitCode::Warning,
        3 => ExitCode::Retryable,
        4 => ExitCode::Authorization,
        5 => ExitCode::Unsupported,
        130 => ExitCode::Interrupted,
        _ => ExitCode::Error,
    }
}

fn map_client_error(error: ClientError) -> CliError {
    match error {
        ClientError::Request { source, .. } => {
            CliError::ServerConnection(format!("request failed: {source}"))
        }
        ClientError::InvalidUrl(message) | ClientError::Build(message) => {
            CliError::InvalidArgument(message)
        }
        ClientError::Auth(error) => CliError::InvalidArgument(error.to_string()),
        ClientError::HttpStatus { status, body } => {
            CliError::ServerConnection(format!("server error {status}: {body}"))
        }
    }
}
