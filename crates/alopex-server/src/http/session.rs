use std::sync::{Arc, OnceLock};

use alopex_cluster::{FailureClass, OperationState, RequestId, RoutingOutcomeKind};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::error::{Result, ServerError};
use crate::http::sql::sync_catalog_to_store;
use crate::http::{
    mark_transaction_outcome_response, transaction_error_response, transaction_failure_outcome,
    transaction_json_response, transaction_metadata_version, transaction_request_id,
    HttpTransactionOutcome, RequestContext,
};
use crate::ops::distributed_read::ReadExecutionOwner;
use crate::server::ServerState;
use crate::session::SessionId;

#[derive(Serialize)]
struct SessionBeginResponse {
    session_id: String,
    expires_at: String,
    transaction: HttpTransactionOutcome,
}

#[derive(Serialize)]
struct SessionActionResponse {
    success: bool,
    transaction: HttpTransactionOutcome,
}

/// Optional, additive request identity accepted by the legacy body-less
/// session endpoints.  An omitted body remains valid for v0.8 clients.
#[derive(Debug, Default, Deserialize)]
pub struct SessionRequest {
    #[serde(default)]
    pub request_id: Option<RequestId>,
}

// A local SQL session is deliberately process-local, whereas a caller can
// retry an HTTP request across a reconnect or process restart.  This compact,
// durable tombstone ledger prevents such a replay from creating a second
// session or applying commit/rollback twice.  It does not masquerade as the
// distributed coordinator ledger: a record is replayable only while its
// original local session is still live; otherwise the retained tombstone
// returns the established session-expired boundary.
const SESSION_IDEMPOTENCY_PREFIX: &[u8] = b"__alopex_internal/http-session-idempotency/v1/";
static SESSION_IDEMPOTENCY_LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSessionResponse {
    status: u16,
    body: Value,
    session_id: Option<String>,
    requires_active_transaction: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionLedgerRecord {
    request_id: String,
    fingerprint: String,
    /// Stable operation identity is retained independently from the replayable
    /// response. A durable invalidation tombstone must keep reporting the
    /// original local session rather than a later correlation-derived ID.
    #[serde(default)]
    transaction_id: Option<String>,
    duplicate_count: u64,
    /// `None` is a durable invalidation tombstone.  It is written before an
    /// operation and retained when the in-memory session cannot be proven
    /// live, so retries never manufacture a new local session.
    response: Option<StoredSessionResponse>,
}

enum SessionLedgerClaim {
    Execute(SessionLedgerReservation),
    Replay(StoredSessionResponse),
    Expired(Option<String>),
    Conflict,
}

struct SessionLedgerReservation {
    key: Vec<u8>,
    request_id: RequestId,
    fingerprint: String,
    _guard: OwnedMutexGuard<()>,
}

fn session_idempotency_lock() -> Arc<Mutex<()>> {
    SESSION_IDEMPOTENCY_LOCK
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
}

fn session_ledger_key(request_id: &RequestId) -> Vec<u8> {
    let digest = Sha256::digest(request_id.as_str().as_bytes());
    let mut key = SESSION_IDEMPOTENCY_PREFIX.to_vec();
    key.extend_from_slice(format!("{digest:x}").as_bytes());
    key
}

fn session_request_fingerprint(
    ctx: &RequestContext,
    operation: &str,
    session_id: Option<&str>,
) -> String {
    let canonical = format!(
        "actor={:?}\nroute=/session/{operation}\ntarget={}",
        ctx.actor,
        session_id.unwrap_or("<begin>")
    );
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

fn read_session_ledger(state: &ServerState, key: &[u8]) -> Result<Option<SessionLedgerRecord>> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let key = key.to_vec();
    let value = txn.get(&key)?;
    txn.rollback_self()?;
    value
        .map(|value| {
            serde_json::from_slice(&value).map_err(|error| {
                ServerError::Internal(format!("invalid session idempotency ledger: {error}"))
            })
        })
        .transpose()
}

fn write_session_ledger(
    state: &ServerState,
    key: Vec<u8>,
    record: &SessionLedgerRecord,
) -> Result<()> {
    let value = serde_json::to_vec(record).map_err(|error| {
        ServerError::Internal(format!("encode session idempotency ledger: {error}"))
    })?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(key, value)?;
    txn.commit_self()?;
    Ok(())
}

fn record_duplicate_count(body: &mut Value, duplicate_count: u64) {
    if let Some(transaction) = body.get_mut("transaction") {
        if let Some(idempotency) = transaction.get_mut("idempotency") {
            idempotency["duplicate_count"] = json!(duplicate_count);
        }
    }
}

async fn stored_response_is_live(state: &ServerState, response: &StoredSessionResponse) -> bool {
    let Some(session_id) = &response.session_id else {
        return false;
    };
    let Ok(session_id) = session_id.parse::<SessionId>() else {
        return false;
    };
    match state.session_manager.get_session(&session_id).await {
        Ok(snapshot) => !response.requires_active_transaction || snapshot.has_transaction,
        Err(_) => false,
    }
}

async fn claim_session_request(
    state: &ServerState,
    request_id: RequestId,
    fingerprint: String,
) -> Result<SessionLedgerClaim> {
    let guard = session_idempotency_lock().lock_owned().await;
    let key = session_ledger_key(&request_id);
    let Some(mut record) = read_session_ledger(state, &key)? else {
        write_session_ledger(
            state,
            key.clone(),
            &SessionLedgerRecord {
                request_id: request_id.as_str().to_owned(),
                fingerprint: fingerprint.clone(),
                transaction_id: None,
                duplicate_count: 0,
                response: None,
            },
        )?;
        return Ok(SessionLedgerClaim::Execute(SessionLedgerReservation {
            key,
            request_id,
            fingerprint,
            _guard: guard,
        }));
    };

    if record.request_id != request_id.as_str() || record.fingerprint != fingerprint {
        return Ok(SessionLedgerClaim::Conflict);
    }

    let Some(mut response) = record.response.take() else {
        return Ok(SessionLedgerClaim::Expired(record.transaction_id.clone()));
    };
    if !stored_response_is_live(state, &response).await {
        // Persist an invalidation tombstone before releasing the local lock.
        // A restarted server must not turn the same request ID into a fresh
        // begin/commit/rollback operation.
        let transaction_id = record
            .transaction_id
            .clone()
            .or_else(|| response.session_id.clone());
        write_session_ledger(state, key, &record)?;
        return Ok(SessionLedgerClaim::Expired(transaction_id));
    }

    record.duplicate_count = record
        .duplicate_count
        .checked_add(1)
        .ok_or_else(|| ServerError::Internal("session idempotency duplicate overflow".into()))?;
    record_duplicate_count(&mut response.body, record.duplicate_count);
    record.response = Some(response.clone());
    write_session_ledger(state, key, &record)?;
    Ok(SessionLedgerClaim::Replay(response))
}

impl SessionLedgerReservation {
    fn complete(
        self,
        state: &ServerState,
        transaction_id: Option<String>,
        response: StoredSessionResponse,
    ) -> Result<()> {
        write_session_ledger(
            state,
            self.key,
            &SessionLedgerRecord {
                request_id: self.request_id.as_str().to_owned(),
                fingerprint: self.fingerprint,
                transaction_id,
                duplicate_count: 0,
                response: Some(response),
            },
        )
    }
}

fn stored_response(
    status: StatusCode,
    body: Value,
    session_id: Option<String>,
    requires_active_transaction: bool,
) -> StoredSessionResponse {
    StoredSessionResponse {
        status: status.as_u16(),
        body,
        session_id,
        requires_active_transaction,
    }
}

fn stored_response_into_http(response: StoredSessionResponse) -> Response {
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut reply = (status, Json(response.body)).into_response();
    mark_transaction_outcome_response(&mut reply);
    reply
}

fn stored_success<T: Serialize>(
    response: &T,
    transaction: &HttpTransactionOutcome,
    session_id: Option<String>,
    requires_active_transaction: bool,
    max_response_size: usize,
    ctx: &RequestContext,
) -> Result<StoredSessionResponse> {
    let body = serde_json::to_value(response).map_err(|error| {
        ServerError::Internal(format!("encode session idempotency response: {error}"))
    })?;
    if serde_json::to_vec(&body)
        .map_err(|error| {
            ServerError::Internal(format!("encode session idempotency response: {error}"))
        })?
        .len()
        > max_response_size
    {
        // The session operation has already run. Persist and replay the same
        // legacy 413 envelope together with its actual outcome instead of
        // letting retry identities bypass the normal response-size contract.
        return Ok(stored_transaction_error(
            &ServerError::PayloadTooLarge("response size exceeds limit".into()),
            transaction,
            ctx,
            session_id,
            requires_active_transaction,
        ));
    }
    Ok(stored_response(
        StatusCode::OK,
        body,
        session_id,
        requires_active_transaction,
    ))
}

fn stored_transaction_error(
    error: &ServerError,
    outcome: &HttpTransactionOutcome,
    ctx: &RequestContext,
    session_id: Option<String>,
    requires_active_transaction: bool,
) -> StoredSessionResponse {
    stored_response(
        error.status_code(),
        json!({
            "error": {
                "code": error.error_code(),
                "message": error.to_string(),
                "correlation_id": ctx.correlation_id,
            },
            "transaction": outcome,
        }),
        session_id,
        requires_active_transaction,
    )
}

fn session_expired_response(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    ctx: &RequestContext,
) -> Response {
    let error = ServerError::SessionExpired(
        "local session replay is unavailable after restart or terminal transition".into(),
    );
    let outcome = transaction_failure_outcome(state, transaction_id, request_id, &error);
    transaction_error_response(error, outcome, ctx)
}

fn session_idempotency_conflict_response(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    ctx: &RequestContext,
) -> Response {
    let outcome = HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        OperationState::Rejected,
        Some(FailureClass::Conflict),
        Some("idempotency_conflict".to_owned()),
        RoutingOutcomeKind::LocalOnly,
        "idempotency_conflict",
        false,
    );
    transaction_error_response(
        ServerError::Conflict(
            "request_id was already used for a different session operation".into(),
        ),
        outcome,
        ctx,
    )
}

pub async fn begin(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    request: Option<Json<SessionRequest>>,
) -> Response {
    let supplied_request_id = request.and_then(|Json(request)| request.request_id);
    let provisional_transaction_id = format!("http-session:{}", ctx.correlation_id);
    let preflight_request_id = match transaction_request_id(
        supplied_request_id.clone(),
        &provisional_transaction_id,
        "begin",
    ) {
        Ok(request_id) => request_id,
        Err(error) => {
            let outcome = transaction_failure_outcome(
                &state,
                provisional_transaction_id,
                RequestId::new(format!("{}:begin", ctx.correlation_id)),
                &error,
            );
            return transaction_error_response(error, outcome, &ctx);
        }
    };

    // A caller-supplied begin identity is claimed before `create_session`.
    // Body-less v0.8 calls retain their historical behavior because there is
    // no retry-stable identity available before the random session id exists.
    if let Some(request_id) = supplied_request_id.clone() {
        let fingerprint = session_request_fingerprint(&ctx, "begin", None);
        match claim_session_request(&state, request_id.clone(), fingerprint).await {
            Ok(SessionLedgerClaim::Replay(response)) => return stored_response_into_http(response),
            Ok(SessionLedgerClaim::Expired(session_id)) => {
                return session_expired_response(
                    &state,
                    session_id.unwrap_or(provisional_transaction_id),
                    request_id,
                    &ctx,
                );
            }
            Ok(SessionLedgerClaim::Conflict) => {
                return session_idempotency_conflict_response(
                    &state,
                    provisional_transaction_id,
                    request_id,
                    &ctx,
                );
            }
            Ok(SessionLedgerClaim::Execute(reservation)) => {
                let stored = match begin_session(state.clone(), Some(request_id.clone())).await {
                    Ok(response) => {
                        match stored_success(
                            &response,
                            &response.transaction,
                            Some(response.session_id.clone()),
                            true,
                            state.config.max_response_size,
                            &ctx,
                        ) {
                            Ok(stored) => stored,
                            Err(error) => {
                                let outcome = transaction_failure_outcome(
                                    &state,
                                    provisional_transaction_id.clone(),
                                    request_id.clone(),
                                    &error,
                                );
                                stored_transaction_error(&error, &outcome, &ctx, None, false)
                            }
                        }
                    }
                    Err(error) => {
                        let outcome = transaction_failure_outcome(
                            &state,
                            provisional_transaction_id.clone(),
                            request_id.clone(),
                            &error,
                        );
                        stored_transaction_error(&error, &outcome, &ctx, None, false)
                    }
                };
                let reply = stored.clone();
                let transaction_id = stored.session_id.clone();
                if reservation
                    .complete(&state, transaction_id, stored)
                    .is_err()
                {
                    // The pending record was committed before any side effect.
                    // Do not report an unverifiable local session success when
                    // its durable replay/tombstone record could not be stored.
                    return session_expired_response(
                        &state,
                        provisional_transaction_id,
                        request_id,
                        &ctx,
                    );
                }
                return stored_response_into_http(reply);
            }
            Err(error) => {
                let outcome = transaction_failure_outcome(
                    &state,
                    provisional_transaction_id,
                    request_id,
                    &error,
                );
                return transaction_error_response(error, outcome, &ctx);
            }
        }
    }

    match begin_session(state.clone(), supplied_request_id).await {
        Ok(response) => {
            let transaction = response.transaction.clone();
            transaction_json_response(response, transaction, state.config.max_response_size, &ctx)
        }
        Err(error) => {
            let outcome = transaction_failure_outcome(
                &state,
                provisional_transaction_id,
                preflight_request_id,
                &error,
            );
            transaction_error_response(error, outcome, &ctx)
        }
    }
}

pub async fn commit(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<String>,
    request: Option<Json<SessionRequest>>,
) -> Response {
    session_action_response(state, ctx, id, Action::Commit, request).await
}

async fn session_action_response(
    state: Arc<ServerState>,
    ctx: RequestContext,
    id: String,
    action: Action,
    request: Option<Json<SessionRequest>>,
) -> Response {
    let request_id = request.and_then(|Json(request)| request.request_id);
    let operation = action.operation_name();
    let request_id = match transaction_request_id(request_id, &id, operation) {
        Ok(request_id) => request_id,
        Err(error) => {
            let outcome = transaction_failure_outcome(
                &state,
                id,
                RequestId::new(format!("{}:{operation}", ctx.correlation_id)),
                &error,
            );
            return transaction_error_response(error, outcome, &ctx);
        }
    };

    let fingerprint = session_request_fingerprint(&ctx, operation, Some(&id));
    match claim_session_request(&state, request_id.clone(), fingerprint).await {
        Ok(SessionLedgerClaim::Replay(response)) => stored_response_into_http(response),
        Ok(SessionLedgerClaim::Expired(_)) => {
            session_expired_response(&state, id, request_id, &ctx)
        }
        Ok(SessionLedgerClaim::Conflict) => {
            session_idempotency_conflict_response(&state, id, request_id, &ctx)
        }
        Ok(SessionLedgerClaim::Execute(reservation)) => {
            let stored = match session_action(state.clone(), &id, action, request_id.clone()).await
            {
                Ok(response) => match stored_success(
                    &response,
                    &response.transaction,
                    Some(id.clone()),
                    false,
                    state.config.max_response_size,
                    &ctx,
                ) {
                    Ok(stored) => stored,
                    Err(error) => {
                        let outcome = transaction_failure_outcome(
                            &state,
                            id.clone(),
                            request_id.clone(),
                            &error,
                        );
                        stored_transaction_error(&error, &outcome, &ctx, Some(id.clone()), false)
                    }
                },
                Err(error) => {
                    let outcome =
                        transaction_failure_outcome(&state, id.clone(), request_id.clone(), &error);
                    stored_transaction_error(&error, &outcome, &ctx, Some(id.clone()), false)
                }
            };
            let reply = stored.clone();
            if reservation
                .complete(&state, Some(id.clone()), stored)
                .is_err()
            {
                return session_expired_response(&state, id, request_id, &ctx);
            }
            stored_response_into_http(reply)
        }
        Err(error) => {
            let outcome = transaction_failure_outcome(&state, id, request_id, &error);
            transaction_error_response(error, outcome, &ctx)
        }
    }
}

pub async fn rollback(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<String>,
    request: Option<Json<SessionRequest>>,
) -> Response {
    session_action_response(state, ctx, id, Action::Rollback, request).await
}

/// Bind a distributed-read registration to the authenticated HTTP profile and
/// (when supplied) to a live SQL session. The session is correlation state;
/// the profile remains the authorization authority for later stream and cancel
/// requests, so a guessed session ID cannot broaden access.
pub async fn distributed_read_owner(
    state: &ServerState,
    ctx: &RequestContext,
    session_id: Option<&str>,
) -> Result<ReadExecutionOwner> {
    let profile = ctx.actor.clone().ok_or_else(|| {
        ServerError::Unauthorized("distributed read requires an authenticated profile".into())
    })?;
    let session_id = match session_id {
        Some(id) => {
            let session_id = id
                .parse::<SessionId>()
                .map_err(|_| ServerError::BadRequest("invalid session id".into()))?;
            // Preserve the existing session expiry/not-found classification.
            state.session_manager.get_session(&session_id).await?;
            Some(session_id)
        }
        None => None,
    };
    ReadExecutionOwner::new(profile, session_id)
}

async fn begin_session(
    state: Arc<ServerState>,
    supplied_request_id: Option<RequestId>,
) -> Result<SessionBeginResponse> {
    let session_id = state.session_manager.create_session().await?;
    state.session_manager.begin_transaction(&session_id).await?;
    let snapshot = state.session_manager.get_session(&session_id).await?;
    let expires_at = chrono::DateTime::<chrono::Utc>::from(snapshot.expires_at);
    let transaction_id = session_id.to_string();
    let request_id = transaction_request_id(supplied_request_id, &transaction_id, "begin")?;
    Ok(SessionBeginResponse {
        session_id: transaction_id.clone(),
        expires_at: expires_at.to_rfc3339(),
        transaction: local_session_outcome(
            &state,
            transaction_id,
            request_id,
            OperationState::Running,
            "session_started",
        ),
    })
}

#[derive(Clone, Copy)]
enum Action {
    Commit,
    Rollback,
}

impl Action {
    fn operation_name(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Rollback => "rollback",
        }
    }

    fn outcome_state(self) -> OperationState {
        match self {
            Self::Commit => OperationState::Committed,
            Self::Rollback => OperationState::Cancelled,
        }
    }

    fn outcome_reason(self) -> &'static str {
        match self {
            Self::Commit => "local_session_committed",
            Self::Rollback => "local_session_rolled_back",
        }
    }
}

async fn session_action(
    state: Arc<ServerState>,
    id: &str,
    action: Action,
    request_id: RequestId,
) -> Result<SessionActionResponse> {
    let session_id = id
        .parse::<SessionId>()
        .map_err(|_| ServerError::BadRequest("invalid session id".into()))?;
    match action {
        Action::Commit => {
            let effects = state.session_manager.commit(&session_id).await?;
            if !effects.is_empty() {
                state.apply_table_lifecycle_effects(effects)?;
                sync_catalog_to_store(&state)?;
            }
        }
        Action::Rollback => {
            let effects = state.session_manager.rollback(&session_id).await?;
            state.apply_catalog_rollback_effects(effects)?;
        }
    }
    Ok(SessionActionResponse {
        success: true,
        transaction: local_session_outcome(
            &state,
            id,
            request_id,
            action.outcome_state(),
            action.outcome_reason(),
        ),
    })
}

fn local_session_outcome(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    operation_state: OperationState,
    reason_code: impl Into<String>,
) -> HttpTransactionOutcome {
    let reason_code = reason_code.into();
    HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        operation_state,
        None,
        None,
        RoutingOutcomeKind::LocalOnly,
        reason_code,
        false,
    )
}
