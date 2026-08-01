//! Cross-surface register and HTTP outcome-shape parity for Phase 4.

#[path = "../../../tests/f4_surface_matrix.rs"]
mod f4_surface_matrix;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::{http, Server};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

async fn send(router: axum::Router, body: Value) -> (StatusCode, Value) {
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/kv/txn/begin")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (
        status,
        serde_json::from_slice(&body).expect("JSON response"),
    )
}

fn assert_common_outcome_shape(outcome: &Value) {
    let object = outcome.as_object().expect("transaction outcome object");
    for field in f4_surface_matrix::COMMON_OUTCOME_FIELDS {
        assert!(
            object.contains_key(*field),
            "missing common outcome field: {field}"
        );
    }
    assert_eq!(outcome["outcome_version"], "v0.9");
    assert_eq!(outcome["isolation"], "snapshot");
    assert!(outcome["idempotency"].is_object());
    assert!(outcome["routing"].is_object());
}

#[test]
fn closed_sql_and_api_registers_have_no_missing_or_duplicate_phase4_rows() {
    let sql_rows = alopex_sql::transaction_sql_statement_matrix();
    let actual_sql = sql_rows
        .iter()
        .map(|row| row.id.to_owned())
        .collect::<Vec<_>>();
    assert_eq!(actual_sql, f4_surface_matrix::sql_transaction_ids());
    assert_eq!(
        sql_rows
            .iter()
            .filter(|row| row.status == alopex_sql::TransactionSqlStatus::Distributed)
            .count(),
        27
    );
    assert_eq!(
        sql_rows
            .iter()
            .filter(|row| row.status == alopex_sql::TransactionSqlStatus::PreExecutionReject)
            .count(),
        19
    );
    assert_eq!(
        sql_rows
            .iter()
            .filter(|row| row.status == alopex_sql::TransactionSqlStatus::SingleRange)
            .count(),
        2
    );
    assert_eq!(
        sql_rows
            .iter()
            .filter(|row| row.status == alopex_sql::TransactionSqlStatus::LocalOnly)
            .count(),
        3
    );

    let api_rows = f4_surface_matrix::api_surface_rows();
    assert_eq!(api_rows.len(), 92);
    let api_ids = api_rows
        .iter()
        .map(|row| row.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(api_ids.len(), 92, "API register must not have duplicates");
    assert_eq!(f4_surface_matrix::inherited_surface_ids().len(), 98);
}

#[tokio::test]
async fn http_local_and_explicit_distributed_preflight_share_the_common_outcome_contract() {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().join("server-data"),
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server");
    let router = http::router(server.state);

    let (status, local) = send(
        router.clone(),
        json!({ "request_id": "f4-parity-local-begin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // The legacy KV transaction adapter returns its transaction identifier at
    // the top level; v0.9 adds the common outcome under `transaction` rather
    // than replacing that response shape with a generic `success` field.
    assert!(local["txn_id"].is_string());
    assert_common_outcome_shape(&local["transaction"]);
    assert_eq!(local["transaction"]["routing"]["kind"], "single_range");
    assert_eq!(local["transaction"]["state"], "running");

    let (status, unsupported) = send(
        router,
        json!({
            "request_id": "f4-parity-distributed-begin",
            "require_distributed": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_common_outcome_shape(&unsupported["transaction"]);
    assert_eq!(unsupported["transaction"]["state"], "rejected");
    assert_eq!(
        unsupported["transaction"]["failure_class"],
        "invalid_request"
    );
    assert_eq!(unsupported["transaction"]["routing"]["kind"], "unsupported");
}
