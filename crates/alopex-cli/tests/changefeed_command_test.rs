use std::collections::BTreeMap;
use std::sync::Arc;

use alopex_cli::cli::{
    ChangefeedCheckpointRequest, ChangefeedCommand, ChangefeedDeliveryRequest,
    ChangefeedLifecycleRequest, OutputFormat,
};
use alopex_cli::client::http::HttpClient;
use alopex_cli::commands::changefeed::{authenticated_actor, invoke_remote};
use alopex_cli::profile::config::ServerConfig;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex};

#[derive(Default)]
struct ObservedRequests {
    entries: Mutex<Vec<(String, Value)>>,
}

async fn create(
    State(observed): State<Arc<ObservedRequests>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    observed
        .entries
        .lock()
        .await
        .push(("create".to_string(), body));
    Json(json!({"operation": "create", "state": "accepted"}))
}

async fn post_lifecycle(
    State(observed): State<Arc<ObservedRequests>>,
    Path((_feed_id, operation)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Response {
    observed
        .entries
        .lock()
        .await
        .push((operation.clone(), body));
    if operation == "close" {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "failure_class": "prerequisite_missing",
                "reason_code": "durable_unavailable"
            })),
        )
            .into_response();
    }
    Json(json!({"operation": operation, "state": "accepted"})).into_response()
}

async fn get_lifecycle(
    State(observed): State<Arc<ObservedRequests>>,
    Path((_feed_id, operation)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    observed
        .entries
        .lock()
        .await
        .push((operation.clone(), json!(query)));
    if operation == "stream" {
        return (
            [(header::CONTENT_TYPE, "application/x-ndjson")],
            "{\"event\":{\"event_id\":\"event-1\"}}\n{\"state\":\"accepted\"}\n",
        )
            .into_response();
    }
    Json(json!({"operation": operation, "events": []})).into_response()
}

async fn spawn_server(router: Router) -> (String, oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let address = listener.local_addr().expect("address");
    let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    (format!("http://{address}"), shutdown_tx)
}

fn delivery_request(request_id: &str) -> ChangefeedDeliveryRequest {
    ChangefeedDeliveryRequest {
        feed_id: "feed-a".to_string(),
        request_id: request_id.to_string(),
        max_events: 2,
        deadline: 77,
        format: Some(OutputFormat::Jsonl),
    }
}

fn checkpoint_request(request_id: &str) -> ChangefeedCheckpointRequest {
    ChangefeedCheckpointRequest {
        feed_id: "feed-a".to_string(),
        request_id: request_id.to_string(),
        checkpoint: "checkpoint-a".to_string(),
        format: Some(OutputFormat::Json),
    }
}

fn lifecycle_request(request_id: &str) -> ChangefeedLifecycleRequest {
    ChangefeedLifecycleRequest {
        feed_id: "feed-a".to_string(),
        request_id: request_id.to_string(),
        format: None,
    }
}

#[tokio::test]
async fn changefeed_cli_invokes_all_eight_http_contracts_and_retains_rejections() {
    let observed = Arc::new(ObservedRequests::default());
    let router = Router::new()
        .route("/v1/changefeeds", post(create))
        .route(
            "/v1/changefeeds/{feed_id}/{operation}",
            post(post_lifecycle),
        )
        .route("/v1/changefeeds/{feed_id}/{operation}", get(get_lifecycle))
        .with_state(observed.clone());
    let (url, shutdown) = spawn_server(router).await;
    let config = ServerConfig {
        url,
        insecure: true,
        auth: None,
        token: None,
        username: None,
        password_command: None,
        cert_path: None,
        key_path: None,
    };
    let client = HttpClient::new(&config).expect("client");
    let actor = authenticated_actor(&config);

    let create = invoke_remote(
        &client,
        &ChangefeedCommand::Create {
            table: Some("orders".to_string()),
            range: None,
            tenant: "tenant-a".to_string(),
            request_id: "create-1".to_string(),
            deadline: Some(99),
            format: Some(OutputFormat::Json),
        },
        actor,
    )
    .await
    .expect("create");
    assert_eq!(create.status, StatusCode::OK);
    assert_eq!(create.documents[0]["operation"], "create");

    let subscribe = invoke_remote(
        &client,
        &ChangefeedCommand::Subscribe {
            feed_id: "feed-a".to_string(),
            request_id: "subscribe-1".to_string(),
            generation: 4,
            epoch: 9,
            format: None,
        },
        actor,
    )
    .await
    .expect("subscribe");
    assert_eq!(subscribe.documents[0]["operation"], "subscribe");

    let poll = invoke_remote(
        &client,
        &ChangefeedCommand::Poll {
            request: delivery_request("poll-1"),
        },
        actor,
    )
    .await
    .expect("poll");
    assert_eq!(poll.documents[0]["operation"], "events");

    let stream = invoke_remote(
        &client,
        &ChangefeedCommand::Stream {
            request: delivery_request("stream-1"),
            follow: true,
        },
        actor,
    )
    .await
    .expect("stream");
    assert!(stream.follow);
    assert_eq!(stream.documents.len(), 2);

    let ack = invoke_remote(
        &client,
        &ChangefeedCommand::Ack {
            request: checkpoint_request("ack-1"),
            ack_id: "ack-record-1".to_string(),
        },
        actor,
    )
    .await
    .expect("ack");
    assert_eq!(ack.documents[0]["operation"], "ack");

    let resume = invoke_remote(
        &client,
        &ChangefeedCommand::Resume {
            request: checkpoint_request("resume-1"),
        },
        actor,
    )
    .await
    .expect("resume");
    assert_eq!(resume.documents[0]["operation"], "resume");

    let cancel = invoke_remote(
        &client,
        &ChangefeedCommand::Cancel {
            request: lifecycle_request("cancel-1"),
        },
        actor,
    )
    .await
    .expect("cancel");
    assert_eq!(cancel.documents[0]["operation"], "cancel");

    let close = invoke_remote(
        &client,
        &ChangefeedCommand::Close {
            request: lifecycle_request("close-1"),
        },
        actor,
    )
    .await
    .expect("structured close rejection");
    assert_eq!(close.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(close.documents[0]["failure_class"], "prerequisite_missing");

    let entries = observed.entries.lock().await;
    let operations = entries
        .iter()
        .map(|(operation, _)| operation.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        operations,
        [
            "create",
            "subscribe",
            "events",
            "stream",
            "ack",
            "resume",
            "cancel",
            "close"
        ]
    );
    assert_eq!(entries[0].1["actor"], "anonymous");
    assert_eq!(entries[0].1["table"], "orders");
    assert_eq!(entries[2].1["request_id"], "poll-1");
    assert_eq!(entries[2].1["max_events"], "2");
    assert_eq!(entries[2].1["deadline_epoch"], "77");
    assert_eq!(entries[4].1["ack_id"], "ack-record-1");

    let _ = shutdown.send(());
}
