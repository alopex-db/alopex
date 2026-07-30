//! Changefeed CLI command invocation.
//!
//! This module owns only grammar-to-HTTP translation and the preservation of
//! canonical response documents. Format rendering and exit-code classification
//! deliberately live in the later changefeed output adapter.

use std::io::Write;

use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;

use crate::cli::{
    ChangefeedCheckpointRequest, ChangefeedCommand, ChangefeedDeliveryRequest,
    ChangefeedLifecycleRequest,
};
use crate::client::http::{ClientError, HttpClient, RawHttpResponse};
use crate::error::{CliError, Result};
use crate::profile::config::{AuthType, ServerConfig};

/// Canonical response documents returned by one lifecycle invocation.
///
/// `documents` contains one JSON document for unary responses and one per
/// JSONL line for the stream operation. Output formatting and exit-code
/// classification are introduced by the dedicated output adapter task.
#[derive(Debug)]
pub struct ChangefeedResponse {
    /// Preserved for the output adapter's documented exit-code classification
    /// in task 3.13. Task 3.12 deliberately does not reinterpret a canonical
    /// non-success response as a transport failure.
    #[allow(dead_code, reason = "task 3.13 owns exit-code classification")]
    pub status: StatusCode,
    pub documents: Vec<Value>,
    /// A follow stream must make each decoded record visible promptly.
    pub follow: bool,
}

/// Derive the actor assertion required by the current server contract from
/// profile-owned authentication. The caller cannot select an arbitrary actor.
pub fn authenticated_actor(config: &ServerConfig) -> &'static str {
    match config.auth.unwrap_or(AuthType::None) {
        AuthType::None => "anonymous",
        AuthType::Token | AuthType::Basic | AuthType::MTls => "dev",
    }
}

/// Invoke one changefeed lifecycle operation through the existing authenticated
/// HTTP client. Input validation occurs before any HTTP request; server-side
/// capability and range-metadata rejections remain structured response bodies.
pub async fn invoke_remote(
    client: &HttpClient,
    command: &ChangefeedCommand,
    actor: &str,
) -> Result<ChangefeedResponse> {
    let (operation, raw, follow) = match command {
        ChangefeedCommand::Create {
            table,
            range,
            tenant,
            request_id,
            deadline,
            ..
        } => {
            require_non_empty(tenant, "tenant")?;
            require_non_empty(request_id, "request_id")?;
            let table = non_empty_option(table, "table")?;
            let range_id = non_empty_option(range, "range")?;
            if table.is_some() == range_id.is_some() {
                return Err(CliError::InvalidArgument(
                    "exactly one of --table or --range is required".to_string(),
                ));
            }
            let request = CreateRequest {
                request_id,
                tenant,
                actor,
                table,
                range_id,
                retention: RetentionRequest {
                    deadline_epoch: *deadline,
                },
                change_kinds: Vec::new(),
            };
            (
                "create",
                client
                    .post_json_raw("v1/changefeeds", &request)
                    .await
                    .map_err(map_client_error)?,
                false,
            )
        }
        ChangefeedCommand::Subscribe {
            feed_id,
            request_id,
            generation,
            epoch,
            ..
        } => {
            require_non_empty(feed_id, "feed_id")?;
            require_non_empty(request_id, "request_id")?;
            let request = SubscribeRequest {
                request_id,
                expected_generation: *generation,
                expected_epoch: *epoch,
            };
            (
                "subscribe",
                client
                    .post_json_raw(&feed_path(feed_id, "subscribe")?, &request)
                    .await
                    .map_err(map_client_error)?,
                false,
            )
        }
        ChangefeedCommand::Poll { request } => (
            "poll",
            delivery_request(client, request, "events").await?,
            false,
        ),
        ChangefeedCommand::Stream { request, follow } => (
            "stream",
            delivery_request(client, request, "stream").await?,
            *follow,
        ),
        ChangefeedCommand::Ack { request, ack_id } => {
            validate_checkpoint_request(request)?;
            require_non_empty(ack_id, "ack_id")?;
            let payload = AckRequest {
                request_id: &request.request_id,
                ack_id,
                checkpoint: &request.checkpoint,
            };
            (
                "ack",
                client
                    .post_json_raw(&feed_path(&request.feed_id, "ack")?, &payload)
                    .await
                    .map_err(map_client_error)?,
                false,
            )
        }
        ChangefeedCommand::Resume { request } => {
            validate_checkpoint_request(request)?;
            let payload = ResumeRequest {
                request_id: &request.request_id,
                checkpoint: &request.checkpoint,
            };
            (
                "resume",
                client
                    .post_json_raw(&feed_path(&request.feed_id, "resume")?, &payload)
                    .await
                    .map_err(map_client_error)?,
                false,
            )
        }
        ChangefeedCommand::Cancel { request } => (
            "cancel",
            lifecycle_request(client, request, "cancel").await?,
            false,
        ),
        ChangefeedCommand::Close { request } => (
            "close",
            lifecycle_request(client, request, "close").await?,
            false,
        ),
    };

    let documents = parse_documents(operation, &raw)?;
    Ok(ChangefeedResponse {
        status: raw.status,
        documents,
        follow,
    })
}

/// Temporary canonical JSON handoff until the dedicated output adapter maps
/// all requested formats. It intentionally emits the server documents without
/// table/CSV/exit-code reinterpretation.
pub fn write_canonical_json<W: Write>(writer: &mut W, response: &ChangefeedResponse) -> Result<()> {
    for document in &response.documents {
        serde_json::to_writer(&mut *writer, document)?;
        writeln!(writer)?;
        if response.follow {
            writer.flush()?;
        }
    }
    Ok(())
}

async fn delivery_request(
    client: &HttpClient,
    request: &ChangefeedDeliveryRequest,
    operation: &str,
) -> Result<RawHttpResponse> {
    require_non_empty(&request.feed_id, "feed_id")?;
    require_non_empty(&request.request_id, "request_id")?;
    if request.max_events == 0 || request.deadline == 0 {
        return Err(CliError::InvalidArgument(
            "--max-events and --deadline must both be greater than zero".to_string(),
        ));
    }
    let query = [
        ("request_id", request.request_id.clone()),
        ("max_events", request.max_events.to_string()),
        ("deadline_epoch", request.deadline.to_string()),
    ];
    client
        .get_raw_with_query(&feed_path(&request.feed_id, operation)?, &query)
        .await
        .map_err(map_client_error)
}

async fn lifecycle_request(
    client: &HttpClient,
    request: &ChangefeedLifecycleRequest,
    operation: &str,
) -> Result<RawHttpResponse> {
    require_non_empty(&request.feed_id, "feed_id")?;
    require_non_empty(&request.request_id, "request_id")?;
    let payload = LifecycleRequest {
        request_id: &request.request_id,
    };
    client
        .post_json_raw(&feed_path(&request.feed_id, operation)?, &payload)
        .await
        .map_err(map_client_error)
}

fn validate_checkpoint_request(request: &ChangefeedCheckpointRequest) -> Result<()> {
    require_non_empty(&request.feed_id, "feed_id")?;
    require_non_empty(&request.request_id, "request_id")?;
    require_non_empty(&request.checkpoint, "checkpoint")
}

fn require_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(CliError::InvalidArgument(format!(
            "--{field} must not be empty"
        )));
    }
    Ok(())
}

fn non_empty_option<'a>(value: &'a Option<String>, field: &str) -> Result<Option<&'a str>> {
    match value {
        Some(value) => {
            require_non_empty(value, field)?;
            Ok(Some(value.trim()))
        }
        None => Ok(None),
    }
}

fn feed_path(feed_id: &str, operation: &str) -> Result<String> {
    require_non_empty(feed_id, "feed_id")?;
    let encoded = url::form_urlencoded::byte_serialize(feed_id.as_bytes()).collect::<String>();
    Ok(format!("v1/changefeeds/{encoded}/{operation}"))
}

fn parse_documents(operation: &str, raw: &RawHttpResponse) -> Result<Vec<Value>> {
    if operation == "stream"
        && raw
            .content_type
            .as_deref()
            .is_some_and(|content_type| content_type.starts_with("application/x-ndjson"))
    {
        let mut documents = Vec::new();
        for line in raw.body.lines().filter(|line| !line.trim().is_empty()) {
            documents.push(serde_json::from_str(line).map_err(|error| {
                CliError::ServerConnection(format!("invalid changefeed JSONL response: {error}"))
            })?);
        }
        if documents.is_empty() {
            return Err(CliError::ServerConnection(
                "changefeed stream returned an empty JSONL response".to_string(),
            ));
        }
        return Ok(documents);
    }

    Ok(vec![serde_json::from_str(&raw.body).map_err(|error| {
        CliError::ServerConnection(format!("invalid changefeed JSON response: {error}"))
    })?])
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

#[derive(Serialize)]
struct CreateRequest<'a> {
    request_id: &'a str,
    tenant: &'a str,
    actor: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_id: Option<&'a str>,
    retention: RetentionRequest,
    change_kinds: Vec<String>,
}

#[derive(Serialize)]
struct RetentionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline_epoch: Option<u64>,
}

#[derive(Serialize)]
struct SubscribeRequest<'a> {
    request_id: &'a str,
    expected_generation: u64,
    expected_epoch: u64,
}

#[derive(Serialize)]
struct AckRequest<'a> {
    request_id: &'a str,
    ack_id: &'a str,
    checkpoint: &'a str,
}

#[derive(Serialize)]
struct ResumeRequest<'a> {
    request_id: &'a str,
    checkpoint: &'a str,
}

#[derive(Serialize)]
struct LifecycleRequest<'a> {
    request_id: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_identity_is_encoded_as_one_path_segment() {
        assert_eq!(
            feed_path("feed/a?b", "close").expect("path"),
            "v1/changefeeds/feed%2Fa%3Fb/close"
        );
    }

    #[test]
    fn jsonl_stream_preserves_each_canonical_document() {
        let raw = RawHttpResponse {
            status: StatusCode::OK,
            content_type: Some("application/x-ndjson".to_string()),
            body: "{\"event\":{\"id\":1}}\n{\"state\":\"accepted\"}\n".to_string(),
        };

        let documents = parse_documents("stream", &raw).expect("JSONL response");
        assert_eq!(documents.len(), 2);
        assert_eq!(documents[0]["event"]["id"], 1);
        assert_eq!(documents[1]["state"], "accepted");
    }

    #[test]
    fn missing_checkpoint_is_rejected_before_http_invocation() {
        let request = ChangefeedCheckpointRequest {
            feed_id: "feed-a".to_string(),
            request_id: "request-a".to_string(),
            checkpoint: " ".to_string(),
            format: None,
        };

        assert!(validate_checkpoint_request(&request).is_err());
    }
}
