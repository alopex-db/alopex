use std::sync::Arc;

use alopex_core::kv::KVTransaction;
use alopex_core::types::TxnMode;
use alopex_core::KVStore;
use axum::extract::Extension;
use axum::response::Response;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, ServerError};
use crate::http::{error_response, json_response, RequestContext};
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

#[derive(Debug, Deserialize)]
pub struct KvTxnBeginRequest {
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct KvTxnRequest {
    pub txn_id: String,
    pub key: Option<String>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize)]
pub struct KvGetResponse {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
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
}

#[derive(Debug, Serialize)]
pub struct KvTxnBeginResponse {
    pub txn_id: String,
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
    match txn_begin_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn txn_get(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    match txn_get_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn txn_put(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    match txn_put_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn txn_delete(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    match txn_delete_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn txn_commit(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    match txn_commit_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn txn_rollback(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<KvTxnRequest>,
) -> Response {
    match txn_rollback_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

fn get_impl(state: Arc<ServerState>, request: KvGetRequest) -> Result<KvGetResponse> {
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let key_bytes = request.key.into_bytes();
    let value = txn.get(&key_bytes)?;
    txn.commit_self()?;
    Ok(KvGetResponse {
        key: key_bytes,
        value,
    })
}

fn put_impl(state: Arc<ServerState>, request: KvPutRequest) -> Result<KvStatusResponse> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(request.key.into_bytes(), request.value)?;
    txn.commit_self()?;
    Ok(KvStatusResponse { success: true })
}

fn delete_impl(state: Arc<ServerState>, request: KvDeleteRequest) -> Result<KvStatusResponse> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.delete(request.key.into_bytes())?;
    txn.commit_self()?;
    Ok(KvStatusResponse { success: true })
}

fn list_impl(state: Arc<ServerState>, request: KvListRequest) -> Result<KvListResponse> {
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let prefix = request.prefix.unwrap_or_default();
    let mut entries = Vec::new();
    for (key, value) in txn.scan_prefix(prefix.as_bytes())? {
        entries.push(KvListEntry { key, value });
    }
    txn.commit_self()?;
    Ok(KvListResponse { entries })
}

fn txn_begin_impl(
    state: Arc<ServerState>,
    request: KvTxnBeginRequest,
) -> Result<KvTxnBeginResponse> {
    let timeout_secs = request.timeout_secs.unwrap_or(DEFAULT_TXN_TIMEOUT_SECS);
    let meta = TxnMeta {
        started_at_secs: current_timestamp_secs(),
        timeout_secs,
    };
    let txn_id = generate_txn_id();
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(txn_meta_key(&txn_id), encode_meta(meta))?;
    txn.commit_self()?;
    Ok(KvTxnBeginResponse { txn_id })
}

fn txn_get_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvGetResponse> {
    let key = request
        .key
        .ok_or_else(|| ServerError::BadRequest("key is required".into()))?;
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
    })
}

fn txn_put_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    let key = request
        .key
        .ok_or_else(|| ServerError::BadRequest("key is required".into()))?;
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
    Ok(KvStatusResponse { success: true })
}

fn txn_delete_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    let key = request
        .key
        .ok_or_else(|| ServerError::BadRequest("key is required".into()))?;
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
    Ok(KvStatusResponse { success: true })
}

fn txn_commit_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    commit_transaction(state, &request.txn_id)?;
    Ok(KvStatusResponse { success: true })
}

fn txn_rollback_impl(state: Arc<ServerState>, request: KvTxnRequest) -> Result<KvStatusResponse> {
    rollback_transaction(state, &request.txn_id)?;
    Ok(KvStatusResponse { success: true })
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
