//! Changefeed output and exit-code adaptation.
//!
//! The server's JSON and JSONL documents remain authoritative.  Every
//! presentation format carries a lossless `canonical_json` field so a human
//! table or delimited export never erases event, checkpoint, failure,
//! operation, replay, or request identity fields.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use serde_json::Value as JsonValue;

use crate::batch::ChangefeedCliOutcome;
use crate::cli::OutputFormat;
use crate::commands::changefeed::ChangefeedResponse;
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};

use super::create_formatter;

/// Render a complete canonical response using the command-local output
/// override (when present), then return the stable shell outcome. Rendering
/// happens before a non-success is returned, preserving structured evidence
/// for automation and operators.
pub fn render_and_classify<W: Write>(
    writer: &mut W,
    response: &ChangefeedResponse,
    fallback_format: OutputFormat,
) -> Result<()> {
    let format = response.format.unwrap_or(fallback_format);
    render_documents(writer, &response.documents, format, response.follow)?;

    let outcome = ChangefeedCliOutcome::from_changefeed_response(
        &response.documents,
        response.status.as_u16(),
    );
    if outcome == ChangefeedCliOutcome::Success {
        return Ok(());
    }

    Err(CliError::ChangefeedOutcome {
        outcome: outcome.as_str().to_string(),
        reason: diagnostic_reason(&response.documents, response.status.as_u16()),
        exit_code: outcome.exit_code(),
    })
}

fn render_documents<W: Write>(
    writer: &mut W,
    documents: &[JsonValue],
    format: OutputFormat,
    follow: bool,
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut *writer, documents)?;
            writeln!(writer)?;
        }
        OutputFormat::Jsonl => {
            for document in documents {
                serde_json::to_writer(&mut *writer, document)?;
                writeln!(writer)?;
                if follow {
                    writer.flush()?;
                }
            }
        }
        OutputFormat::Table | OutputFormat::Csv | OutputFormat::Tsv => {
            let (columns, rows) = tabular_rows(documents)?;
            let mut formatter = create_formatter(format);
            formatter.write_header(writer, &columns)?;
            for row in &rows {
                formatter.write_row(writer, row)?;
            }
            formatter.write_footer(writer)?;
        }
    }
    Ok(())
}

/// Convert canonical documents to stable columnar rows while retaining an
/// exact JSON copy in every row. Nested values are additionally expanded with
/// dotted paths for human and spreadsheet use; arrays remain compact JSON so
/// their ordering and structure are preserved.
fn tabular_rows(documents: &[JsonValue]) -> Result<(Vec<Column>, Vec<Row>)> {
    let flattened = documents
        .iter()
        .map(flatten_document)
        .collect::<Result<Vec<_>>>()?;
    let mut names = BTreeSet::from(["canonical_json".to_string()]);
    for fields in &flattened {
        names.extend(fields.keys().cloned());
    }
    let names = names.into_iter().collect::<Vec<_>>();
    let columns = names
        .iter()
        .map(|name| Column::new(name, DataType::Text))
        .collect::<Vec<_>>();
    let rows = flattened
        .iter()
        .map(|fields| {
            Row::new(
                names
                    .iter()
                    .map(|name| {
                        fields
                            .get(name)
                            .cloned()
                            .flatten()
                            .map(Value::Text)
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            )
        })
        .collect();
    Ok((columns, rows))
}

fn flatten_document(document: &JsonValue) -> Result<BTreeMap<String, Option<String>>> {
    let mut fields = BTreeMap::new();
    fields.insert(
        "canonical_json".to_string(),
        Some(serde_json::to_string(document)?),
    );
    flatten_value(document, None, &mut fields)?;
    Ok(fields)
}

fn flatten_value(
    value: &JsonValue,
    prefix: Option<&str>,
    fields: &mut BTreeMap<String, Option<String>>,
) -> Result<()> {
    match value {
        JsonValue::Object(object) => {
            if object.is_empty() {
                insert_flattened_value(prefix, Some("{}".to_string()), fields);
                return Ok(());
            }
            for (key, value) in object {
                let key = match prefix {
                    None if key == "canonical_json" => "field.canonical_json".to_string(),
                    None => key.clone(),
                    Some(prefix) => format!("{prefix}.{key}"),
                };
                flatten_value(value, Some(&key), fields)?;
            }
        }
        JsonValue::Array(_) => {
            insert_flattened_value(prefix, Some(serde_json::to_string(value)?), fields)
        }
        JsonValue::String(value) => insert_flattened_value(prefix, Some(value.clone()), fields),
        JsonValue::Number(value) => insert_flattened_value(prefix, Some(value.to_string()), fields),
        JsonValue::Bool(value) => insert_flattened_value(prefix, Some(value.to_string()), fields),
        JsonValue::Null => insert_flattened_value(prefix, None, fields),
    }
    Ok(())
}

fn insert_flattened_value(
    prefix: Option<&str>,
    value: Option<String>,
    fields: &mut BTreeMap<String, Option<String>>,
) {
    if let Some(prefix) = prefix {
        fields.insert(prefix.to_string(), value);
    }
}

fn diagnostic_reason(documents: &[JsonValue], http_status: u16) -> String {
    documents
        .iter()
        .find_map(|document| {
            document
                .get("reason_code")
                .and_then(JsonValue::as_str)
                .or_else(|| document.get("operation_state").and_then(JsonValue::as_str))
        })
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("HTTP {http_status}"))
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;
    use serde_json::json;

    use super::*;

    fn canonical_document() -> JsonValue {
        json!({
            "operation_state": "committed",
            "failure_class": null,
            "reason_code": null,
            "retryable": false,
            "operation_id": "operation-a",
            "request_id": "request-a",
            "idempotency": {
                "operation_id": "operation-a",
                "request_id": "request-a",
                "replay_id": "replay-a",
                "state": "committed"
            },
            "result": {
                "result_type": "ack",
                "result": {
                    "ack_state": "committed",
                    "committed_checkpoint": {"cursor": "checkpoint-a"}
                }
            },
            "event": {
                "event_id": "event-a",
                "checkpoint": "checkpoint-a",
                "payload": {"kind": "insert"}
            }
        })
    }

    fn response(format: OutputFormat) -> ChangefeedResponse {
        ChangefeedResponse {
            status: StatusCode::OK,
            documents: vec![canonical_document()],
            follow: false,
            format: Some(format),
        }
    }

    #[test]
    fn every_tabular_row_retains_the_lossless_canonical_document() {
        let document = canonical_document();
        let (columns, rows) = tabular_rows(std::slice::from_ref(&document)).expect("rows");
        let canonical_index = columns
            .iter()
            .position(|column| column.name == "canonical_json")
            .expect("canonical JSON column");
        let Value::Text(canonical) = &rows[0].columns[canonical_index] else {
            panic!("canonical JSON is text");
        };
        assert_eq!(
            serde_json::from_str::<JsonValue>(canonical).unwrap(),
            document
        );
        for field in [
            "operation_id",
            "request_id",
            "idempotency.replay_id",
            "result.result.ack_state",
            "event.event_id",
            "event.checkpoint",
            "failure_class",
            "reason_code",
        ] {
            assert!(columns.iter().any(|column| column.name == field), "{field}");
        }
    }

    #[test]
    fn every_output_format_retains_canonical_fields() {
        for format in [
            OutputFormat::Table,
            OutputFormat::Json,
            OutputFormat::Csv,
            OutputFormat::Tsv,
            OutputFormat::Jsonl,
        ] {
            let mut output = Vec::new();
            render_and_classify(&mut output, &response(format), OutputFormat::Table)
                .expect("committed response");
            let output = String::from_utf8(output).expect("UTF-8 output");
            match format {
                OutputFormat::Json => {
                    assert_eq!(
                        serde_json::from_str::<JsonValue>(&output).unwrap(),
                        json!([canonical_document()])
                    );
                }
                OutputFormat::Jsonl => {
                    assert_eq!(
                        serde_json::from_str::<JsonValue>(output.trim()).unwrap(),
                        canonical_document()
                    );
                }
                OutputFormat::Table | OutputFormat::Csv | OutputFormat::Tsv => {
                    for field in [
                        "canonical_json",
                        "operation_id",
                        "request_id",
                        "idempotency.replay_id",
                        "result.result.ack_state",
                        "event.event_id",
                    ] {
                        assert!(output.contains(field), "{format:?} missing {field}");
                    }
                }
            }
        }
    }

    #[test]
    fn canonical_outcomes_use_the_documented_exit_matrix() {
        let document = canonical_document();
        assert_eq!(
            ChangefeedCliOutcome::from_changefeed_response(&[document], 200).exit_code(),
            crate::batch::ExitCode::Success
        );

        let cases = [
            (json!({"operation_state": "accepted"}), 202, 2),
            (
                json!({"operation_state": "retryable_failure", "failure_class": "timeout", "reason_code": "backpressure", "retryable": true}),
                408,
                3,
            ),
            (
                json!({"operation_state": "terminal_failure", "failure_class": "unauthorized", "reason_code": "denied", "retryable": false}),
                401,
                4,
            ),
            (
                json!({"operation_state": "terminal_failure", "failure_class": "invalid_request", "reason_code": "change_kind_unsupported", "routing": {"kind": "unsupported"}, "retryable": false}),
                501,
                5,
            ),
            (
                json!({"operation_state": "terminal_failure", "failure_class": "stale_metadata", "reason_code": "retention_expired", "retryable": false}),
                409,
                1,
            ),
            (
                json!({"operation_state": "terminal_failure", "failure_class": "invalid_request", "reason_code": "resource_limit", "retryable": false}),
                400,
                1,
            ),
            (json!({"operation_state": "cancelled"}), 408, 130),
        ];
        for (document, status, expected) in cases {
            assert_eq!(
                ChangefeedCliOutcome::from_changefeed_response(&[document], status)
                    .exit_code()
                    .as_i32(),
                expected
            );
        }
    }

    #[test]
    fn every_format_returns_the_same_canonical_non_success_exit_class() {
        let document = json!({
            "operation_state": "terminal_failure",
            "failure_class": "stale_metadata",
            "reason_code": "retention_expired",
            "retryable": false,
            "operation_id": "operation-retained",
            "request_id": "request-retained",
            "idempotency": {"replay_id": "replay-retained"}
        });
        for format in [
            OutputFormat::Table,
            OutputFormat::Json,
            OutputFormat::Csv,
            OutputFormat::Tsv,
            OutputFormat::Jsonl,
        ] {
            let response = ChangefeedResponse {
                status: StatusCode::CONFLICT,
                documents: vec![document.clone()],
                follow: false,
                format: Some(format),
            };
            let mut output = Vec::new();
            let error = render_and_classify(&mut output, &response, OutputFormat::Table)
                .expect_err("canonical terminal response");
            assert_eq!(error.exit_code().as_i32(), 1, "{format:?}");
            let output = String::from_utf8(output).expect("UTF-8 output");
            assert!(output.contains("retention_expired"), "{format:?}");
            assert!(output.contains("request-retained"), "{format:?}");
            assert!(output.contains("replay-retained"), "{format:?}");
        }
    }
}
