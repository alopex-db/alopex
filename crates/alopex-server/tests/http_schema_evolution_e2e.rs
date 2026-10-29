#![cfg(not(target_arch = "wasm32"))]

use alopex_server::server::ServerState;
use alopex_server::{http, Server, ServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn config(dir: &tempfile::TempDir) -> ServerConfig {
    ServerConfig {
        data_dir: dir.path().join("data"),
        audit_log_enabled: false,
        tracing_enabled: false,
        metrics_enabled: false,
        ..ServerConfig::default()
    }
}
fn server() -> (tempfile::TempDir, Arc<ServerState>, axum::Router) {
    let dir = tempfile::tempdir().unwrap();
    let state = Server::new(config(&dir)).unwrap().state.clone();
    let app = http::router(state.clone());
    (dir, state, app)
}
async fn request(app: &axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn sql(app: &axum::Router, text: &str, session: Option<&str>) -> Value {
    let (status, result) = request(app, "/sql", json!({"sql":text,"session_id":session})).await;
    assert_eq!(status, StatusCode::OK, "{text}: {result}");
    result
}
async fn begin(app: &axum::Router) -> String {
    let (status, body) = request(app, "/session/begin", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["session_id"].as_str().unwrap().to_owned()
}
async fn finish(app: &axum::Router, session: &str, action: &str) {
    let (status, body) = request(app, &format!("/session/{session}/{action}"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
async fn seed(app: &axum::Router) {
    for text in [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)",
        "CREATE INDEX value_idx ON items (value)",
        "INSERT INTO items VALUES (1, 7)",
    ] {
        sql(app, text, None).await;
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn alter_commit_reopens_rows_schema_indexes_and_view() {
    let (dir, state, app) = server();
    seed(&app).await;
    let session = begin(&app).await;
    for text in [
        "ALTER TABLE items ADD COLUMN label TEXT DEFAULT 'seed'",
        "ALTER TABLE items RENAME COLUMN value TO renamed_value",
        "ALTER TABLE items RENAME TO renamed",
        "CREATE VIEW visible AS SELECT id, renamed_value, label FROM renamed",
    ] {
        sql(&app, text, Some(&session)).await;
    }
    finish(&app, &session, "commit").await;
    drop(app);
    drop(state);
    let reopened = Server::new(config(&dir)).unwrap().state.clone();
    let app = http::router(reopened.clone());
    assert_eq!(
        sql(&app, "SELECT id, renamed_value, label FROM visible", None).await["rows"],
        json!([[{"Integer":1},{"Integer":7},{"Text":"seed"}]])
    );
    assert_eq!(
        sql(&app, "SELECT id FROM renamed WHERE renamed_value = 7", None).await["rows"],
        json!([[{"Integer":1}]])
    );
    let catalog = reopened.catalog.read().unwrap();
    assert!(!catalog.table_exists("items"));
    let index = catalog.get_index("value_idx").unwrap();
    assert_eq!(index.table, "renamed");
    assert_eq!(index.columns, ["renamed_value"]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn repeated_alter_rollback_restores_original_schema_and_index_identity() {
    let (_dir, state, app) = server();
    seed(&app).await;
    let index_id = state
        .catalog
        .read()
        .unwrap()
        .get_index("value_idx")
        .unwrap()
        .index_id;
    let session = begin(&app).await;
    for text in [
        "ALTER TABLE items ADD COLUMN extra_first TEXT",
        "ALTER TABLE items ADD COLUMN extra_second INTEGER",
        "ALTER TABLE items RENAME COLUMN value TO renamed_value",
        "ALTER TABLE items RENAME TO renamed",
    ] {
        sql(&app, text, Some(&session)).await;
    }
    finish(&app, &session, "rollback").await;
    assert_eq!(
        sql(&app, "SELECT * FROM items WHERE value = 7", None).await["rows"],
        json!([[{"Integer":1},{"Integer":7}]])
    );
    let catalog = state.catalog.read().unwrap();
    assert!(!catalog.table_exists("renamed"));
    assert_eq!(
        catalog.get_table("items").unwrap().column_names(),
        vec!["id", "value"]
    );
    let index = catalog.get_index("value_idx").unwrap();
    assert_eq!(index.index_id, index_id);
    assert_eq!(index.table, "items");
    assert_eq!(index.columns, ["value"]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn failed_alter_batch_restores_prior_statement_and_releases_gates() {
    let (_dir, state, app) = server();
    seed(&app).await;
    let (status, result) = request(&app, "/sql", json!({"sql":"ALTER TABLE items ADD COLUMN marker TEXT; ALTER TABLE items DROP COLUMN value"})).await;
    assert_ne!(status, StatusCode::OK, "{result}");
    assert!(
        result.to_string().contains("referenced by index"),
        "{result}"
    );
    assert_eq!(
        state
            .catalog
            .read()
            .unwrap()
            .get_table("items")
            .unwrap()
            .column_names(),
        vec!["id", "value"]
    );
    assert_eq!(
        sql(&app, "SELECT * FROM items WHERE value = 7", None).await["rows"],
        json!([[{"Integer":1},{"Integer":7}]])
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn mixed_alter_index_batch_rollback_restores_original_index() {
    let (_dir, state, app) = server();
    seed(&app).await;
    let session = begin(&app).await;
    sql(&app, "DROP INDEX value_idx; ALTER TABLE items ADD COLUMN marker TEXT; CREATE INDEX replacement_idx ON items (value)", Some(&session)).await;
    finish(&app, &session, "rollback").await;
    {
        let catalog = state.catalog.read().unwrap();
        assert!(catalog.index_exists("value_idx"));
        assert!(!catalog.index_exists("replacement_idx"));
        assert_eq!(
            catalog.get_table("items").unwrap().column_names(),
            vec!["id", "value"]
        );
    }
    assert_eq!(
        sql(&app, "SELECT id FROM items WHERE value = 7", None).await["rows"],
        json!([[{"Integer":1}]])
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn create_alter_view_rollback_removes_all_uncommitted_metadata() {
    let (_dir, state, app) = server();
    let session = begin(&app).await;
    for text in [
        "CREATE TABLE items (id INTEGER)",
        "ALTER TABLE items ADD COLUMN value TEXT",
        "CREATE VIEW visible AS SELECT id, value FROM items",
    ] {
        sql(&app, text, Some(&session)).await;
    }
    finish(&app, &session, "rollback").await;
    let catalog = state.catalog.read().unwrap();
    assert!(!catalog.table_exists("items"));
    assert!(!catalog.table_exists("visible"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn view_dependencies_and_drop_rollback_are_preserved() {
    let (_dir, _state, app) = server();
    seed(&app).await;
    sql(
        &app,
        "CREATE VIEW visible AS SELECT id, value FROM items",
        None,
    )
    .await;
    for text in [
        "DROP TABLE items",
        "ALTER TABLE items ADD COLUMN marker TEXT",
    ] {
        let (status, result) = request(&app, "/sql", json!({"sql":text})).await;
        assert_ne!(status, StatusCode::OK);
        assert!(
            result.to_string().contains("referenced by view"),
            "{result}"
        );
    }
    let session = begin(&app).await;
    sql(&app, "DROP VIEW visible", Some(&session)).await;
    finish(&app, &session, "rollback").await;
    assert_eq!(
        sql(&app, "SELECT * FROM visible", None).await["rows"],
        json!([[{"Integer":1},{"Integer":7}]])
    );
    sql(&app, "DROP VIEW visible", None).await;
    sql(&app, "DROP VIEW IF EXISTS visible", None).await;
    assert_ne!(
        request(&app, "/sql", json!({"sql":"SELECT * FROM visible"}))
            .await
            .0,
        StatusCode::OK
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn truncate_rollback_and_commit_preserve_schema_and_index() {
    let (_dir, state, app) = server();
    seed(&app).await;
    let session = begin(&app).await;
    sql(&app, "TRUNCATE TABLE items", Some(&session)).await;
    assert_eq!(
        sql(&app, "SELECT id FROM items WHERE value = 7", Some(&session)).await["rows"],
        json!([])
    );
    finish(&app, &session, "rollback").await;
    assert_eq!(
        sql(&app, "SELECT id FROM items WHERE value = 7", None).await["rows"],
        json!([[{"Integer":1}]])
    );
    sql(&app, "TRUNCATE TABLE items", None).await;
    assert_eq!(
        sql(&app, "SELECT id FROM items WHERE value = 7", None).await["rows"],
        json!([])
    );
    assert!(state.catalog.read().unwrap().index_exists("value_idx"));
    sql(&app, "INSERT INTO items VALUES (1, 9)", None).await;
    assert_eq!(
        sql(&app, "SELECT id FROM items WHERE value = 9", None).await["rows"],
        json!([[{"Integer":1}]])
    );
}
