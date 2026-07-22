use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use alopex_cluster::RequestId;
use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::ops::distributed_read::{
    PeerCancellation, ReadExecutionOutcome, ReadExecutionOwner, ReadExecutionPlanSummary,
};
use alopex_server::Server;
use alopex_sql::distributed_read::{
    AssemblerRow, AssemblyPlan, DistributedReadBudget, GlobalResultAssembler, RangeAssemblerInput,
    RangeAssemblerPayload, RangeTerminal, ResultPresentation, RowMergePlan,
};
use alopex_sql::executor::ColumnInfo;
use alopex_sql::planner::ResolvedType;
use alopex_sql::storage::SqlValue;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

fn state() -> (Arc<alopex_server::server::ServerState>, tempfile::TempDir) {
    let temp = tempdir().unwrap();
    let server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::Dev {
            api_key: "test-key".into(),
        },
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .unwrap();
    (server.state, temp)
}

fn plan() -> ReadExecutionPlanSummary {
    ReadExecutionPlanSummary {
        requested_mode: "strong".into(),
        effective_mode: "strong".into(),
        metadata_version: 9,
        ranges: vec!["range-a".into()],
        freshness: "current_committed_prefix".into(),
        retry_count: 0,
        failover_count: 0,
    }
}

fn prepared_result() -> alopex_sql::distributed_read::PreparedResult {
    let columns = vec![ColumnInfo::new("name", ResolvedType::Text)];
    let mut assembler = GlobalResultAssembler::new(
        vec!["range-a".into()],
        AssemblyPlan::Rows(RowMergePlan {
            presentation: ResultPresentation {
                columns: columns.clone(),
                distinct: false,
                order: Vec::new(),
                final_order_key_indexes: Vec::new(),
                offset: 0,
                limit: None,
            },
        }),
        DistributedReadBudget::default(),
    )
    .unwrap();
    assembler
        .push_range(RangeAssemblerInput {
            range_id: "range-a".into(),
            columns,
            payloads: vec![RangeAssemblerPayload::Rows(vec![AssemblerRow {
                values: vec![SqlValue::Text("visible-after-prepare".into())],
                order_keys: Vec::new(),
                row_key: 1,
            }])],
            terminal: RangeTerminal::Completed {
                cleanup_acknowledged: true,
            },
        })
        .unwrap();
    assembler.prepare().unwrap()
}

fn request(method: &str, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("x-api-key", "test-key")
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

#[tokio::test]
async fn prepared_route_emits_metadata_rows_and_terminal_summary_only_after_prepare() {
    let (state, _temp) = state();
    let execution_id = RequestId::new("read-ready");
    state
        .distributed_read_registry
        .register_with_id(
            execution_id.clone(),
            ReadExecutionOwner::new("dev", None).unwrap(),
            plan(),
            Vec::new(),
        )
        .unwrap();
    assert!(matches!(
        state
            .distributed_read_registry
            .open_prepared(&execution_id, Some("dev")),
        Err(alopex_server::ServerError::Conflict(_))
    ));
    state
        .distributed_read_registry
        .publish_prepared(&execution_id, prepared_result())
        .unwrap();

    let response = http::router(state)
        .oneshot(request("GET", "/v1/sql/reads/read-ready", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let items = String::from_utf8(body.to_vec())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["type"], "prepared");
    assert_eq!(items[1]["type"], "row");
    assert_eq!(
        items[1]["values"][0],
        json!({"Text": "visible-after-prepare"})
    );
    assert_eq!(items[2]["type"], "terminal");
    assert_eq!(items[2]["summary"]["outcome"], "success");
    assert_eq!(items[2]["summary"]["execution_id"], "read-ready");
}

#[tokio::test]
async fn cancel_route_is_profile_authenticated_and_dispatches_cleanup_once() {
    let (state, _temp) = state();
    let execution_id = RequestId::new("read-cancel");
    let deliveries = Arc::new(AtomicUsize::new(0));
    let observed = deliveries.clone();
    let callback: PeerCancellation = Arc::new(move |id| {
        assert_eq!(id.as_str(), "read-cancel");
        observed.fetch_add(1, Ordering::SeqCst);
    });
    state
        .distributed_read_registry
        .register_with_id(
            execution_id.clone(),
            ReadExecutionOwner::new("dev", None).unwrap(),
            plan(),
            vec![callback],
        )
        .unwrap();

    let router = http::router(state.clone());
    let response = router
        .oneshot(request(
            "POST",
            "/v1/sql/reads/read-cancel/cancel",
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["summary"]["outcome"], "cancelled");
    assert_eq!(value["peer_cleanup_deliveries"], 1);
    assert_eq!(deliveries.load(Ordering::SeqCst), 1);
    assert_eq!(
        state
            .distributed_read_registry
            .summary(&execution_id, Some("dev"))
            .unwrap()
            .outcome,
        ReadExecutionOutcome::Cancelled
    );

    let response = http::router(state)
        .oneshot(request(
            "POST",
            "/v1/sql/reads/read-cancel/cancel",
            Body::empty(),
        ))
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["peer_cleanup_deliveries"], 0);
    assert_eq!(deliveries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicit_remote_start_returns_capability_unavailable_without_local_execution() {
    let (state, _temp) = state();
    let response = http::router(state)
        .oneshot(request(
            "POST",
            "/v1/sql/reads",
            Body::from(
                json!({ "sql": "SELECT * FROM does_not_exist", "read_mode": "strong" }).to_string(),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["error"]["code"], "CAPABILITY_UNAVAILABLE");
    assert!(value["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not executed locally"));
}
