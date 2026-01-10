use std::sync::Arc;

use axum::extract::{Extension, Path};
use axum::response::Response;
use serde::Serialize;

use crate::error::{Result, ServerError};
use crate::http::{error_response, json_response, RequestContext};
use crate::server::ServerState;
use crate::session::SessionId;

#[derive(Serialize)]
struct SessionBeginResponse {
    session_id: String,
    expires_at: String,
}

#[derive(Serialize)]
struct SessionActionResponse {
    success: bool,
}

pub async fn begin(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    match begin_session(state.clone()).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn commit(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<String>,
) -> Response {
    match session_action(state.clone(), &id, Action::Commit).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn rollback(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<String>,
) -> Response {
    match session_action(state.clone(), &id, Action::Rollback).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

async fn begin_session(state: Arc<ServerState>) -> Result<SessionBeginResponse> {
    let session_id = state.session_manager.create_session().await?;
    state.session_manager.begin_transaction(&session_id).await?;
    let snapshot = state.session_manager.get_session(&session_id).await?;
    let expires_at = chrono::DateTime::<chrono::Utc>::from(snapshot.expires_at);
    Ok(SessionBeginResponse {
        session_id: session_id.to_string(),
        expires_at: expires_at.to_rfc3339(),
    })
}

enum Action {
    Commit,
    Rollback,
}

async fn session_action(
    state: Arc<ServerState>,
    id: &str,
    action: Action,
) -> Result<SessionActionResponse> {
    let session_id = id
        .parse::<SessionId>()
        .map_err(|_| ServerError::BadRequest("invalid session id".into()))?;
    match action {
        Action::Commit => state.session_manager.commit(&session_id).await?,
        Action::Rollback => state.session_manager.rollback(&session_id).await?,
    }
    Ok(SessionActionResponse { success: true })
}
