use std::sync::{Arc, OnceLock};

use alopex_cluster::{FailureClass, OperationState, RequestId, RoutingOutcomeKind};
use alopex_core::kv::KVTransaction;
use alopex_core::types::TxnMode;
use alopex_core::KVStore;
use axum::extract::Extension;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::error::{Result, ServerError};
use crate::http::{
    error_response, json_response, mark_transaction_outcome_response, transaction_error_response,
    transaction_failure_outcome, transaction_metadata_version, transaction_request_id,
    HttpTransactionOutcome, RequestContext,
};
use crate::server::ServerState;

#[derive(Debug, Deserialize)]
pub struct KvGetRequest {
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct KvPutRequest {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct KvDeleteRequest {
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct KvListRequest {
    pub prefix: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct KvTxnBeginRequest {
    pub timeout_secs: Option<u64>,
    /// Additive v0.9 replay identity. Omitted legacy requests remain valid.
    #[serde(default)]
    pub request_id: Option<RequestId>,
    /// This endpoint remains single-range-only. An explicit distributed
    /// request is rejected before the local transaction is created.
    #[serde(default)]
    pub require_distributed: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct KvTxnRequest {
    pub txn_id: String,
    pub key: Option<String>,
    pub value: Option<Vec<u8>>,
    /// Additive v0.9 replay identity. Omitted legacy requests derive one from
    /// the durable transaction identity and operation name.
    #[serde(default)]
    pub request_id: Option<RequestId>,
    /// The raw KV transaction API has no multi-range coordinator. Explicit
    /// distributed execution therefore never falls back to this local path.
    #[serde(default)]
    pub require_distributed: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct KvGetResponse {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<HttpTransactionOutcome>,
}

#[derive(Debug, Serialize)]
pub struct KvListEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Serialize)]
pub struct KvListResponse {
    pub entries: Vec<KvListEntry>,
}

#[derive(Debug, Serialize)]
pub struct KvStatusResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<HttpTransactionOutcome>,
}

#[derive(Debug, Serialize)]
pub struct KvTxnBeginResponse {
    pub txn_id: String,
    pub transaction: HttpTransactionOutcome,
}

// The KV transaction state and staged writes are durable, unlike HTTP SQL
// sessions. A durable response ledger can therefore replay both non-terminal
// and terminal outcomes after restart without creating a new transaction or
// applying a commit twice.
// Server control records must never be addressable by public raw KV routes.
// `INTERNAL_KV_PREFIX` is checked by every raw and transaction-key operation
// below, so a client cannot erase or forge an idempotency result.
const INTERNAL_KV_PREFIX: &[u8] = b"__alopex_internal/";
const KV_TXN_IDEMPOTENCY_PREFIX: &[u8] =
    b"__alopex_internal/http-kv-transaction-idempotency/v1/request/";
const KV_TXN_IDEMPOTENCY_ORDINAL_PREFIX: &[u8] =
    b"__alopex_internal/http-kv-transaction-idempotency/v1/ordinal/";
static KV_TXN_IDEMPOTENCY_LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredKvTxnResponse {
    status: u16,
    body: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct KvTxnLedgerRecord {
    request_id: String,
    operation: String,
    fingerprint: String,
    transaction_id: String,
    duplicate_count: u64,
    /// A process failure can occur after reservation and before a durable
    /// outcome. Replaying that uncertain request must be recovery-pending,
    /// never a second local apply.
    response: Option<StoredKvTxnResponse>,
}

enum KvTxnLedgerClaim {
    Execute(KvTxnLedgerReservation),
    Replay(StoredKvTxnResponse),
    RecoveryPending(String),
    Conflict,
}

struct KvTxnLedgerReservation {
    key: Vec<u8>,
    request_id: RequestId,
    operation: &'static str,
    fingerprint: String,
    _guard: OwnedMutexGuard<()>,
}

fn kv_txn_idempotency_lock() -> Arc<Mutex<()>> {
    KV_TXN_IDEMPOTENCY_LOCK
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
}

fn kv_txn_ledger_key(request_id: &RequestId) -> Vec<u8> {
    // `request_id` is shared by every KV transaction operation.  The record
    // carries its original operation and fingerprint, so reusing the same ID
    // for a different operation is a conflict rather than a second apply.
    let digest = Sha256::digest(request_id.as_str().as_bytes());
    let mut key = KV_TXN_IDEMPOTENCY_PREFIX.to_vec();
    key.extend_from_slice(format!("{digest:x}").as_bytes());
    key
}

fn kv_txn_ordinal_key(transaction_id: &str, operation: &str) -> Vec<u8> {
    let digest = Sha256::digest(format!("{transaction_id}\n{operation}").as_bytes());
    let mut key = KV_TXN_IDEMPOTENCY_ORDINAL_PREFIX.to_vec();
    key.extend_from_slice(format!("{digest:x}").as_bytes());
    key
}

fn is_internal_kv_key(key: &[u8]) -> bool {
    key.starts_with(INTERNAL_KV_PREFIX)
}

fn ensure_public_kv_key(key: &str) -> Result<()> {
    if is_internal_kv_key(key.as_bytes()) {
        return Err(ServerError::BadRequest(
            "reserved internal KV namespace is not publicly accessible".into(),
        ));
    }
    Ok(())
}

fn ensure_public_kv_list_prefix(prefix: &str) -> Result<()> {
    if prefix.as_bytes().starts_with(INTERNAL_KV_PREFIX) {
        return Err(ServerError::BadRequest(
            "reserved internal KV namespace is not publicly accessible".into(),
        ));
    }
    Ok(())
}

fn kv_fingerprint_field(output: &mut Vec<u8>, name: &str, value: &[u8]) {
    output.extend_from_slice(&(name.len() as u64).to_be_bytes());
    output.extend_from_slice(name.as_bytes());
    output.extend_from_slice(&(value.len() as u64).to_be_bytes());
    output.extend_from_slice(value);
}

fn kv_fingerprint_optional_field(output: &mut Vec<u8>, name: &str, value: Option<&[u8]>) {
    // A missing JSON field and an explicitly empty value are distinct request
    // payloads.  Retaining that distinction is required before an existing
    // request ID may be replayed instead of rejected as a conflict.
    kv_fingerprint_field(
        output,
        &format!("{name}_present"),
        if value.is_some() { b"true" } else { b"false" },
    );
    if let Some(value) = value {
        kv_fingerprint_field(output, name, value);
    }
}

fn kv_txn_request_fingerprint(
    ctx: &RequestContext,
    operation: &str,
    transaction_id: &str,
    timeout_secs: Option<u64>,
    key: Option<&str>,
    value: Option<&[u8]>,
    require_distributed: Option<bool>,
) -> String {
    let mut canonical = Vec::new();
    kv_fingerprint_field(
        &mut canonical,
        "actor",
        ctx.actor.as_deref().unwrap_or_default().as_bytes(),
    );
    kv_fingerprint_field(&mut canonical, "operation", operation.as_bytes());
    kv_fingerprint_field(&mut canonical, "transaction_id", transaction_id.as_bytes());
    kv_fingerprint_field(
        &mut canonical,
        "timeout_secs",
        timeout_secs
            .map(|timeout| timeout.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    kv_fingerprint_optional_field(&mut canonical, "key", key.map(str::as_bytes));
    kv_fingerprint_optional_field(&mut canonical, "value", value);
    kv_fingerprint_field(
        &mut canonical,
        "require_distributed",
        format!("{require_distributed:?}").as_bytes(),
    );
    format!("{:x}", Sha256::digest(canonical))
}

fn read_kv_txn_ledger(state: &ServerState, key: &[u8]) -> Result<Option<KvTxnLedgerRecord>> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let value = txn.get(&key.to_vec())?;
    txn.rollback_self()?;
    value
        .map(|value| {
            serde_json::from_slice(&value).map_err(|error| {
                ServerError::Internal(format!(
                    "invalid KV transaction idempotency ledger: {error}"
                ))
            })
        })
        .transpose()
}

fn write_kv_txn_ledger(
    state: &ServerState,
    key: Vec<u8>,
    record: &KvTxnLedgerRecord,
) -> Result<()> {
    let value = serde_json::to_vec(record).map_err(|error| {
        ServerError::Internal(format!("encode KV transaction idempotency ledger: {error}"))
    })?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(key, value)?;
    txn.commit_self()?;
    Ok(())
}

fn next_kv_txn_operation_ordinal(
    state: &ServerState,
    transaction_id: &str,
    operation: &str,
) -> Result<u64> {
    let key = kv_txn_ordinal_key(transaction_id, operation);
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let ordinal = match txn.get(&key)? {
        Some(value) => {
            let bytes: [u8; 8] = value.as_slice().try_into().map_err(|_| {
                ServerError::Internal("invalid KV transaction request ordinal".into())
            })?;
            u64::from_be_bytes(bytes)
        }
        None => 0,
    };
    let next = ordinal
        .checked_add(1)
        .ok_or_else(|| ServerError::Internal("KV transaction request ordinal overflow".into()))?;
    txn.put(key, next.to_be_bytes().to_vec())?;
    txn.commit_self()?;
    Ok(next)
}

async fn kv_txn_request_id(
    state: &ServerState,
    supplied: Option<RequestId>,
    transaction_id: &str,
    operation: &'static str,
) -> Result<RequestId> {
    if supplied.is_some() {
        return transaction_request_id(supplied, transaction_id, operation);
    }

    // Legacy payloads omit request_id.  Assign a durable statement ordinal
    // so successive v0.8 operations of the same kind remain distinct.  The
    // returned outcome exposes this ID; a retry can submit it explicitly to
    // obtain the stored first result.
    let _guard = kv_txn_idempotency_lock().lock_owned().await;
    let ordinal = next_kv_txn_operation_ordinal(state, transaction_id, operation)?;
    Ok(RequestId::new(format!(
        "{transaction_id}:{operation}:{ordinal}"
    )))
}

fn record_duplicate_count(body: &mut Value, duplicate_count: u64) {
    if let Some(transaction) = body.get_mut("transaction") {
        if let Some(idempotency) = transaction.get_mut("idempotency") {
            idempotency["duplicate_count"] = json!(duplicate_count);
        }
    }
}

fn record_current_correlation_id(body: &mut Value, ctx: &RequestContext) {
    if let Some(error) = body.get_mut("error") {
        if let Some(error) = error.as_object_mut() {
            error.insert(
                "correlation_id".to_owned(),
                Value::String(ctx.correlation_id.clone()),
            );
        }
    }
}

async fn claim_kv_txn_request(
    state: &ServerState,
    ctx: &RequestContext,
    request_id: RequestId,
    operation: &'static str,
    fingerprint: String,
    transaction_id: String,
) -> Result<KvTxnLedgerClaim> {
    let guard = kv_txn_idempotency_lock().lock_owned().await;
    let key = kv_txn_ledger_key(&request_id);
    let Some(mut record) = read_kv_txn_ledger(state, &key)? else {
        write_kv_txn_ledger(
            state,
            key.clone(),
            &KvTxnLedgerRecord {
                request_id: request_id.as_str().to_owned(),
                operation: operation.to_owned(),
                fingerprint: fingerprint.clone(),
                transaction_id,
                duplicate_count: 0,
                response: None,
            },
        )?;
        return Ok(KvTxnLedgerClaim::Execute(KvTxnLedgerReservation {
            key,
            request_id,
            operation,
            fingerprint,
            _guard: guard,
        }));
    };

    if record.request_id != request_id.as_str()
        || record.operation != operation
        || record.fingerprint != fingerprint
    {
        return Ok(KvTxnLedgerClaim::Conflict);
    }
    let Some(mut response) = record.response.take() else {
        return Ok(KvTxnLedgerClaim::RecoveryPending(record.transaction_id));
    };
    record.duplicate_count = record
        .duplicate_count
        .checked_add(1)
        .ok_or_else(|| ServerError::Internal("KV transaction duplicate count overflow".into()))?;
    record_duplicate_count(&mut response.body, record.duplicate_count);
    // The outcome remains the stored first result, but the legacy error
    // envelope's correlation ID always belongs to the request being served.
    record_current_correlation_id(&mut response.body, ctx);
    record.response = Some(response.clone());
    write_kv_txn_ledger(state, key, &record)?;
    Ok(KvTxnLedgerClaim::Replay(response))
}

impl KvTxnLedgerReservation {
    fn complete(
        self,
        state: &ServerState,
        transaction_id: String,
        response: StoredKvTxnResponse,
    ) -> Result<()> {
        write_kv_txn_ledger(
            state,
            self.key,
            &KvTxnLedgerRecord {
                request_id: self.request_id.as_str().to_owned(),
                operation: self.operation.to_owned(),
                fingerprint: self.fingerprint,
                transaction_id,
                duplicate_count: 0,
                response: Some(response),
            },
        )
    }
}

fn stored_kv_txn_response_into_http(response: StoredKvTxnResponse) -> Response {
    let status = axum::http::StatusCode::from_u16(response.status)
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let mut reply = (status, Json(response.body)).into_response();
    mark_transaction_outcome_response(&mut reply);
    reply
}

fn stored_kv_txn_error(
    error: &ServerError,
    transaction: &HttpTransactionOutcome,
    ctx: &RequestContext,
) -> StoredKvTxnResponse {
    StoredKvTxnResponse {
        status: error.status_code().as_u16(),
        body: json!({
            "error": {
                "code": error.error_code(),
                "message": error.to_string(),
                "correlation_id": ctx.correlation_id,
            },
            "transaction": transaction,
        }),
    }
}

fn stored_kv_txn_success(
    body: Value,
    transaction: &HttpTransactionOutcome,
    max_response_size: usize,
    ctx: &RequestContext,
) -> Result<StoredKvTxnResponse> {
    if serde_json::to_vec(&body)
        .map_err(|error| ServerError::Internal(format!("encode KV transaction response: {error}")))?
        .len()
        > max_response_size
    {
        return Ok(stored_kv_txn_error(
            &ServerError::PayloadTooLarge("response size exceeds limit".into()),
            transaction,
            ctx,
        ));
    }
    Ok(StoredKvTxnResponse {
        status: axum::http::StatusCode::OK.as_u16(),
        body,
    })
}

fn kv_txn_outcome(
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
        Some(reason_code.clone()),
        RoutingOutcomeKind::SingleRange,
        reason_code,
        false,
    )
}

fn kv_txn_failure_response(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    error: ServerError,
    ctx: &RequestContext,
) -> Response {
    let outcome = transaction_failure_outcome(state, transaction_id, request_id, &error);
    transaction_error_response(error, outcome, ctx)
}

fn kv_txn_idempotency_conflict_response(
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
        RoutingOutcomeKind::SingleRange,
        "idempotency_conflict",
        false,
    );
    transaction_error_response(
        ServerError::Conflict(
            "request_id was already used for a different KV transaction operation".into(),
        ),
        outcome,
        ctx,
    )
}

fn kv_txn_recovery_pending_response(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    ctx: &RequestContext,
) -> Response {
    let outcome = HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        OperationState::RecoveryPending,
        Some(FailureClass::Internal),
        Some("idempotency_recovery_pending".to_owned()),
        RoutingOutcomeKind::Unavailable,
        "idempotency_recovery_pending",
        false,
    );
    transaction_error_response(
        ServerError::Internal(
            "KV transaction request outcome is unavailable; recovery is required".into(),
        ),
        outcome,
        ctx,
    )
}

fn kv_txn_persistence_pending_response(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    ctx: &RequestContext,
) -> Response {
    let outcome = HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        OperationState::RecoveryPending,
        Some(FailureClass::Internal),
        Some("idempotency_persistence_failed".to_owned()),
        RoutingOutcomeKind::Unavailable,
        "idempotency_persistence_failed",
        false,
    );
    transaction_error_response(
        ServerError::Internal(
            "KV transaction completed but its idempotency outcome could not be persisted".into(),
        ),
        outcome,
        ctx,
    )
}

fn complete_kv_txn_response(
    reservation: KvTxnLedgerReservation,
    state: &ServerState,
    transaction_id: String,
    request_id: RequestId,
    response: StoredKvTxnResponse,
    ctx: &RequestContext,
) -> Response {
    if reservation
        .complete(state, transaction_id.clone(), response.clone())
        .is_err()
    {
        return kv_txn_persistence_pending_response(state, transaction_id, request_id, ctx);
    }
    stored_kv_txn_response_into_http(response)
}

enum KvTxnAction {
    Get,
    Put,
    Delete,
    Commit,
    Rollback,
}

impl KvTxnAction {
    fn operation(&self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::Commit => "commit",
            Self::Rollback => "rollback",
        }
    }

    fn outcome_state(&self) -> OperationState {
        match self {
            Self::Get | Self::Put | Self::Delete => OperationState::Running,
            Self::Commit => OperationState::Committed,
            Self::Rollback => OperationState::Cancelled,
        }
    }

    fn reason_code(&self) -> &'static str {
        match self {
            Self::Get => "local_kv_transaction_read",
            Self::Put => "local_kv_transaction_write",
            Self::Delete => "local_kv_transaction_delete",
            Self::Commit => "local_kv_transaction_committed",
            Self::Rollback => "local_kv_transaction_rolled_back",
        }
    }

    fn execute(&self, state: Arc<ServerState>, request: KvTxnRequest) -> Result<Value> {
        match self {
            Self::Get => serde_json::to_value(txn_get_impl(state, request)?).map_err(|error| {
                ServerError::Internal(format!("encode KV transaction get response: {error}"))
            }),
            Self::Put => serde_json::to_value(txn_put_impl(state, request)?).map_err(|error| {
                ServerError::Internal(format!("encode KV transaction put response: {error}"))
            }),
            Self::Delete => {
                serde_json::to_value(txn_delete_impl(state, request)?).map_err(|error| {
                    ServerError::Internal(format!("encode KV transaction delete response: {error}"))
                })
            }
            Self::Commit => {
                serde_json::to_value(txn_commit_impl(state, request)?).map_err(|error| {
                    ServerError::Internal(format!("encode KV transaction commit response: {error}"))
                })
            }
            Self::Rollback => {
                serde_json::to_value(txn_rollback_impl(state, request)?).map_err(|error| {
                    ServerError::Internal(format!(
                        "encode KV transaction rollback response: {error}"
                    ))
                })
            }
        }
    }
}

async fn prepare_kv_txn_request(
    state: &ServerState,
    ctx: &RequestContext,
    transaction_id: &str,
    request_id: RequestId,
    operation: &'static str,
    fingerprint: String,
) -> std::result::Result<KvTxnLedgerReservation, Response> {
    match claim_kv_txn_request(
        state,
        ctx,
        request_id.clone(),
        operation,
        fingerprint,
        transaction_id.to_owned(),
    )
    .await
    {
        Ok(KvTxnLedgerClaim::Execute(reservation)) => Ok(reservation),
        Ok(KvTxnLedgerClaim::Replay(response)) => Err(stored_kv_txn_response_into_http(response)),
        Ok(KvTxnLedgerClaim::RecoveryPending(transaction_id)) => Err(
            kv_txn_recovery_pending_response(state, transaction_id, request_id, ctx),
        ),
        Ok(KvTxnLedgerClaim::Conflict) => Err(kv_txn_idempotency_conflict_response(
            state,
            transaction_id,
            request_id,
            ctx,
        )),
        Err(error) => Err(kv_txn_failure_response(
            state,
            transaction_id,
            request_id,
            error,
            ctx,
        )),
    }
}

pub async fn get(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvGetRequest>,
) -> Response {
    match get_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn put(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvPutRequest>,
) -> Response {
    match put_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn delete(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvDeleteRequest>,
) -> Response {
    match delete_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn list(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvListRequest>,
) -> Response {
    match list_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn txn_begin(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnBeginRequest>,
) -> Response {
    // `generate_txn_id` has no storage effect. Generating it before the
    // ledger claim lets an omitted v0.8 request receive the documented
    // transaction-id-derived request id without creating a transaction first.
    let transaction_id = generate_txn_id();
    let request_id =
        match kv_txn_request_id(&state, request.request_id.clone(), &transaction_id, "begin").await
        {
            Ok(request_id) => request_id,
            Err(error) => {
                return kv_txn_failure_response(
                    &state,
                    transaction_id,
                    RequestId::new(format!("{}:begin", ctx.correlation_id)),
                    error,
                    &ctx,
                );
            }
        };
    let fingerprint = kv_txn_request_fingerprint(
        &ctx,
        "begin",
        // A begin request creates the transaction ID.  Its replay
        // fingerprint must therefore be independent of that generated output
        // so the same client-provided request ID can retrieve the first
        // response after a retry or restart.
        request_id.as_str(),
        request.timeout_secs,
        None,
        None,
        request.require_distributed,
    );
    let reservation = match prepare_kv_txn_request(
        &state,
        &ctx,
        &transaction_id,
        request_id.clone(),
        "begin",
        fingerprint,
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(response) => return response,
    };
    if request.require_distributed.unwrap_or(false) {
        let error = ServerError::NotImplemented(
            "HTTP KV transactions are single-range-only; distributed execution is unsupported"
                .into(),
        );
        let outcome =
            transaction_failure_outcome(&state, transaction_id.clone(), request_id.clone(), &error);
        return complete_kv_txn_response(
            reservation,
            &state,
            transaction_id,
            request_id,
            stored_kv_txn_error(&error, &outcome, &ctx),
            &ctx,
        );
    }
    match txn_begin_impl(
        state.clone(),
        request,
        transaction_id.clone(),
        request_id.clone(),
    ) {
        Ok(response) => {
            let transaction = response.transaction.clone();
            let body = match serde_json::to_value(response) {
                Ok(body) => body,
                Err(_) => {
                    return kv_txn_persistence_pending_response(
                        &state,
                        transaction_id,
                        request_id,
                        &ctx,
                    )
                }
            };
            let stored = match stored_kv_txn_success(
                body,
                &transaction,
                state.config.max_response_size,
                &ctx,
            ) {
                Ok(stored) => stored,
                Err(_) => {
                    return kv_txn_persistence_pending_response(
                        &state,
                        transaction_id,
                        request_id,
                        &ctx,
                    )
                }
            };
            complete_kv_txn_response(
                reservation,
                &state,
                transaction_id,
                request_id,
                stored,
                &ctx,
            )
        }
        Err(error) => {
            let outcome = transaction_failure_outcome(
                &state,
                transaction_id.clone(),
                request_id.clone(),
                &error,
            );
            complete_kv_txn_response(
                reservation,
                &state,
                transaction_id,
                request_id,
                stored_kv_txn_error(&error, &outcome, &ctx),
                &ctx,
            )
        }
    }
}

pub async fn txn_get(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    txn_action(state, ctx, request, KvTxnAction::Get).await
}

pub async fn txn_put(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    txn_action(state, ctx, request, KvTxnAction::Put).await
}

pub async fn txn_delete(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    txn_action(state, ctx, request, KvTxnAction::Delete).await
}

pub async fn txn_commit(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    txn_action(state, ctx, request, KvTxnAction::Commit).await
}

pub async fn txn_rollback(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    txn_action(state, ctx, request, KvTxnAction::Rollback).await
}

async fn txn_action(
    state: Arc<ServerState>,
    ctx: RequestContext,
    request: KvTxnRequest,
    action: KvTxnAction,
) -> Response {
    let transaction_id = request.txn_id.clone();
    let operation = action.operation();
    let request_id = match kv_txn_request_id(
        &state,
        request.request_id.clone(),
        &transaction_id,
        operation,
    )
    .await
    {
        Ok(request_id) => request_id,
        Err(error) => {
            return kv_txn_failure_response(
                &state,
                transaction_id,
                RequestId::new(format!("{}:{operation}", ctx.correlation_id)),
                error,
                &ctx,
            );
        }
    };
    let fingerprint = kv_txn_request_fingerprint(
        &ctx,
        operation,
        &transaction_id,
        None,
        request.key.as_deref(),
        request.value.as_deref(),
        request.require_distributed,
    );
    let reservation = match prepare_kv_txn_request(
        &state,
        &ctx,
        &transaction_id,
        request_id.clone(),
        operation,
        fingerprint,
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(response) => return response,
    };
    if request.require_distributed.unwrap_or(false) {
        let error = ServerError::NotImplemented(
            "HTTP KV transactions are single-range-only; distributed execution is unsupported"
                .into(),
        );
        let outcome =
            transaction_failure_outcome(&state, transaction_id.clone(), request_id.clone(), &error);
        return complete_kv_txn_response(
            reservation,
            &state,
            transaction_id,
            request_id,
            stored_kv_txn_error(&error, &outcome, &ctx),
            &ctx,
        );
    }
    match action.execute(state.clone(), request) {
        Ok(mut body) => {
            let outcome = kv_txn_outcome(
                &state,
                transaction_id.clone(),
                request_id.clone(),
                action.outcome_state(),
                action.reason_code(),
            );
            let Some(object) = body.as_object_mut() else {
                return kv_txn_persistence_pending_response(
                    &state,
                    transaction_id,
                    request_id,
                    &ctx,
                );
            };
            let transaction = match serde_json::to_value(&outcome) {
                Ok(transaction) => transaction,
                Err(_) => {
                    return kv_txn_persistence_pending_response(
                        &state,
                        transaction_id,
                        request_id,
                        &ctx,
                    )
                }
            };
            object.insert("transaction".to_owned(), transaction);
            let stored =
                match stored_kv_txn_success(body, &outcome, state.config.max_response_size, &ctx) {
                    Ok(stored) => stored,
                    Err(_) => {
                        return kv_txn_persistence_pending_response(
                            &state,
                            transaction_id,
                            request_id,
                            &ctx,
                        )
                    }
                };
            complete_kv_txn_response(
                reservation,
                &state,
                transaction_id,
                request_id,
                stored,
                &ctx,
            )
        }
        Err(error) => {
            let outcome = transaction_failure_outcome(
                &state,
                transaction_id.clone(),
                request_id.clone(),
                &error,
            );
            complete_kv_txn_response(
                reservation,
                &state,
                transaction_id,
                request_id,
                stored_kv_txn_error(&error, &outcome, &ctx),
                &ctx,
            )
        }
    }
}

fn get_impl(state: Arc<ServerState>, request: KvGetRequest) -> Result<KvGetResponse> {
    ensure_public_kv_key(&request.key)?;
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let key_bytes = request.key.into_bytes();
    let value = txn.get(&key_bytes)?;
    txn.commit_self()?;
    Ok(KvGetResponse {
        key: key_bytes,
        value,
        transaction: None,
    })
}

fn put_impl(state: Arc<ServerState>, request: KvPutRequest) -> Result<KvStatusResponse> {
    state.lifecycle_state.check_write_allowed()?;
    ensure_public_kv_key(&request.key)?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(request.key.into_bytes(), request.value)?;
    txn.commit_self()?;
    Ok(KvStatusResponse {
        success: true,
        transaction: None,
    })
}

fn delete_impl(state: Arc<ServerState>, request: KvDeleteRequest) -> Result<KvStatusResponse> {
    state.lifecycle_state.check_write_allowed()?;
    ensure_public_kv_key(&request.key)?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.delete(request.key.into_bytes())?;
    txn.commit_self()?;
    Ok(KvStatusResponse {
        success: true,
        transaction: None,
    })
}

fn list_impl(state: Arc<ServerState>, request: KvListRequest) -> Result<KvListResponse> {
    let prefix = request.prefix.unwrap_or_default();
    ensure_public_kv_list_prefix(&prefix)?;
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let mut entries = Vec::new();
    for (key, value) in txn.scan_prefix(prefix.as_bytes())? {
        if is_internal_kv_key(&key) {
            continue;
        }
        entries.push(KvListEntry { key, value });
    }
    txn.commit_self()?;
    Ok(KvListResponse { entries })
}

fn txn_begin_impl(
    state: Arc<ServerState>,
    request: KvTxnBeginRequest,
    txn_id: String,
    request_id: RequestId,
) -> Result<KvTxnBeginResponse> {
    state.lifecycle_state.check_write_allowed()?;
    let timeout_secs = request.timeout_secs.unwrap_or(DEFAULT_TXN_TIMEOUT_SECS);
    let meta = TxnMeta {
        started_at_secs: current_timestamp_secs(),
        timeout_secs,
    };
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(txn_meta_key(&txn_id), encode_meta(meta))?;
    txn.commit_self()?;
    Ok(KvTxnBeginResponse {
        transaction: kv_txn_outcome(
            &state,
            txn_id.clone(),
            request_id,
            OperationState::Running,
            "local_kv_transaction_started",
        ),
        txn_id,
    })
}

fn txn_get_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvGetResponse> {
    let key = request
        .key
        .ok_or_else(|| ServerError::BadRequest("key is required".into()))?;
    ensure_public_kv_key(&key)?;
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let meta = load_meta(&mut txn, &request.txn_id)?;
    if is_expired_from_meta(meta, current_timestamp_secs()) {
        txn.commit_self()?;
        rollback_transaction(state.clone(), &request.txn_id)?;
        return Err(ServerError::SessionExpired("transaction expired".into()));
    }
    let value = if let Some(raw) = txn.get(&txn_write_key(&request.txn_id, key.as_bytes()))? {
        match decode_write(&request.txn_id, &raw)? {
            TxnWrite::Put(value) => Some(value),
            TxnWrite::Delete => None,
        }
    } else {
        txn.get(&key.as_bytes().to_vec())?
    };
    txn.commit_self()?;
    Ok(KvGetResponse {
        key: key.into_bytes(),
        value,
        transaction: None,
    })
}

fn txn_put_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    state.lifecycle_state.check_write_allowed()?;
    let key = request
        .key
        .ok_or_else(|| ServerError::BadRequest("key is required".into()))?;
    ensure_public_kv_key(&key)?;
    let value = request
        .value
        .ok_or_else(|| ServerError::BadRequest("value is required".into()))?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let meta = load_meta(&mut txn, &request.txn_id)?;
    if is_expired_from_meta(meta, current_timestamp_secs()) {
        txn.rollback_self()?;
        rollback_transaction(state.clone(), &request.txn_id)?;
        return Err(ServerError::SessionExpired("transaction expired".into()));
    }
    txn.put(
        txn_write_key(&request.txn_id, key.as_bytes()),
        encode_write(TxnWrite::Put(value)),
    )?;
    txn.commit_self()?;
    Ok(KvStatusResponse {
        success: true,
        transaction: None,
    })
}

fn txn_delete_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    state.lifecycle_state.check_write_allowed()?;
    let key = request
        .key
        .ok_or_else(|| ServerError::BadRequest("key is required".into()))?;
    ensure_public_kv_key(&key)?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let meta = load_meta(&mut txn, &request.txn_id)?;
    if is_expired_from_meta(meta, current_timestamp_secs()) {
        txn.rollback_self()?;
        rollback_transaction(state.clone(), &request.txn_id)?;
        return Err(ServerError::SessionExpired("transaction expired".into()));
    }
    txn.put(
        txn_write_key(&request.txn_id, key.as_bytes()),
        encode_write(TxnWrite::Delete),
    )?;
    txn.commit_self()?;
    Ok(KvStatusResponse {
        success: true,
        transaction: None,
    })
}

fn txn_commit_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    state.lifecycle_state.check_write_allowed()?;
    commit_transaction(state, &request.txn_id)?;
    Ok(KvStatusResponse {
        success: true,
        transaction: None,
    })
}

fn txn_rollback_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    state.lifecycle_state.check_write_allowed()?;
    rollback_transaction(state, &request.txn_id)?;
    Ok(KvStatusResponse {
        success: true,
        transaction: None,
    })
}

fn commit_transaction(state: Arc<ServerState>, txn_id: &str) -> Result<()> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let meta = load_meta(&mut txn, txn_id)?;
    if is_expired_from_meta(meta, current_timestamp_secs()) {
        txn.rollback_self()?;
        rollback_transaction(state, txn_id)?;
        return Err(ServerError::SessionExpired("transaction expired".into()));
    }
    let prefix = txn_write_prefix(txn_id);
    let staged: Vec<(Vec<u8>, Vec<u8>)> = txn.scan_prefix(&prefix)?.collect();
    for (staged_key, raw) in &staged {
        let user_key = extract_user_key(txn_id, staged_key)?;
        if is_internal_kv_key(&user_key) {
            return Err(ServerError::BadRequest(
                "reserved internal KV namespace is not publicly accessible".into(),
            ));
        }
        match decode_write(txn_id, raw)? {
            TxnWrite::Put(value) => {
                txn.put(user_key, value)?;
            }
            TxnWrite::Delete => {
                txn.delete(user_key)?;
            }
        }
    }
    for (staged_key, _) in staged {
        txn.delete(staged_key)?;
    }
    txn.delete(txn_meta_key(txn_id))?;
    txn.commit_self()?;
    Ok(())
}

fn rollback_transaction(state: Arc<ServerState>, txn_id: &str) -> Result<()> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let _ = load_meta(&mut txn, txn_id)?;
    let prefix = txn_write_prefix(txn_id);
    let staged: Vec<(Vec<u8>, Vec<u8>)> = txn.scan_prefix(&prefix)?.collect();
    for (staged_key, _) in staged {
        txn.delete(staged_key)?;
    }
    txn.delete(txn_meta_key(txn_id))?;
    txn.commit_self()?;
    Ok(())
}

const DEFAULT_TXN_TIMEOUT_SECS: u64 = 60;
const TXN_META_PREFIX: &[u8] = b"__alopex_txn_meta__:";
const TXN_WRITE_PREFIX: &[u8] = b"__alopex_txn_write__:";
const TXN_WRITE_DELETE: u8 = 0;
const TXN_WRITE_PUT: u8 = 1;

#[derive(Debug, Clone, Copy)]
struct TxnMeta {
    started_at_secs: u64,
    timeout_secs: u64,
}

enum TxnWrite {
    Put(Vec<u8>),
    Delete,
}

fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_txn_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("txn-{}-{}", nanos, std::process::id())
}

fn txn_meta_key(txn_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(TXN_META_PREFIX.len() + txn_id.len());
    key.extend_from_slice(TXN_META_PREFIX);
    key.extend_from_slice(txn_id.as_bytes());
    key
}

fn txn_write_prefix(txn_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(TXN_WRITE_PREFIX.len() + txn_id.len() + 1);
    key.extend_from_slice(TXN_WRITE_PREFIX);
    key.extend_from_slice(txn_id.as_bytes());
    key.push(b':');
    key
}

fn txn_write_key(txn_id: &str, key: &[u8]) -> Vec<u8> {
    let mut full = txn_write_prefix(txn_id);
    full.extend_from_slice(key);
    full
}

fn encode_meta(meta: TxnMeta) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&meta.started_at_secs.to_le_bytes());
    payload.extend_from_slice(&meta.timeout_secs.to_le_bytes());
    payload
}

fn decode_meta(txn_id: &str, raw: &[u8]) -> Result<TxnMeta> {
    if raw.len() < 16 {
        return Err(ServerError::BadRequest(format!(
            "transaction metadata invalid: {}",
            txn_id
        )));
    }
    let started_at_secs = u64::from_le_bytes(raw[0..8].try_into().unwrap());
    let timeout_secs = u64::from_le_bytes(raw[8..16].try_into().unwrap());
    Ok(TxnMeta {
        started_at_secs,
        timeout_secs,
    })
}

fn load_meta(
    txn: &mut alopex_core::kv::any::AnyKVTransaction<'_>,
    txn_id: &str,
) -> Result<TxnMeta> {
    let Some(raw) = txn.get(&txn_meta_key(txn_id))? else {
        return Err(ServerError::NotFound("transaction not found".into()));
    };
    decode_meta(txn_id, &raw)
}

fn is_expired_from_meta(meta: TxnMeta, now_secs: u64) -> bool {
    now_secs.saturating_sub(meta.started_at_secs) >= meta.timeout_secs
}

fn encode_write(entry: TxnWrite) -> Vec<u8> {
    match entry {
        TxnWrite::Put(value) => {
            let mut payload = Vec::with_capacity(1 + value.len());
            payload.push(TXN_WRITE_PUT);
            payload.extend_from_slice(&value);
            payload
        }
        TxnWrite::Delete => vec![TXN_WRITE_DELETE],
    }
}

fn decode_write(txn_id: &str, raw: &[u8]) -> Result<TxnWrite> {
    let Some((&tag, rest)) = raw.split_first() else {
        return Err(ServerError::BadRequest(format!(
            "transaction write entry invalid: {}",
            txn_id
        )));
    };
    match tag {
        TXN_WRITE_PUT => Ok(TxnWrite::Put(rest.to_vec())),
        TXN_WRITE_DELETE => Ok(TxnWrite::Delete),
        _ => Err(ServerError::BadRequest(format!(
            "transaction write entry invalid: {}",
            txn_id
        ))),
    }
}

fn extract_user_key(txn_id: &str, staged_key: &[u8]) -> Result<Vec<u8>> {
    let prefix = txn_write_prefix(txn_id);
    if !staged_key.starts_with(&prefix) {
        return Err(ServerError::BadRequest(format!(
            "transaction write key invalid: {}",
            txn_id
        )));
    }
    Ok(staged_key[prefix.len()..].to_vec())
}
