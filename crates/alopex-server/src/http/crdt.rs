use std::sync::Arc;

use alopex_cluster::crdt::{
    CrdtLifecycleAction, CrdtOutcome, CrdtPolicyInput, CrdtPreExecutionPolicy, CrdtRangeFreshness,
};
use alopex_cluster::{
    CrdtCounterError, CrdtCounterProjection, CrdtOperationEnvelope, CrdtOperationKind, CrdtPayload,
    FailureClass, IdempotencyResult, NodeId, OperationState, RangeIdentity, RequestId,
    RoutingOutcome, RoutingOutcomeKind,
};
use alopex_core::kv::any::{AnyKVManager, AnyKVTransaction};
use alopex_core::kv::{AnyKV, KVStore};
use alopex_core::TxnMode;
use axum::extract::{Extension, Path};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::http::RequestContext;
use crate::server::ServerState;

/// Typed wire request for the first public Counter operation.  The actor is
/// intentionally absent: it is derived from the authenticated transport
/// context rather than accepted from caller-controlled JSON.
#[derive(Clone, Debug, Deserialize)]
pub struct CounterCreateRequest {
    pub object_id: String,
    pub range: RangeIdentity,
    pub request_id: RequestId,
    pub operation_id: String,
    pub update_version: u64,
    pub initial_value: i64,
}

/// HTTP JSON request for a Counter read. The object identity is carried by the
/// route path while the full Phase 1 range identity remains explicit rather
/// than being inferred from a caller-local default.
#[derive(Clone, Debug, Deserialize)]
pub struct CounterReadRequest {
    pub range: RangeIdentity,
    pub request_id: RequestId,
    pub operation_id: String,
    pub update_version: u64,
}

/// A borrowing store adapter keeps the server Counter projection on the
/// existing server `AnyKV` transaction and WAL boundary.
struct ServerCrdtStore<'store>(&'store AnyKV);

impl<'store> KVStore for ServerCrdtStore<'store> {
    type Transaction<'txn>
        = AnyKVTransaction<'txn>
    where
        Self: 'txn;
    type Manager<'txn>
        = AnyKVManager<'txn>
    where
        Self: 'txn;

    fn txn_manager(&self) -> Self::Manager<'_> {
        self.0.txn_manager()
    }

    fn begin(&self, mode: TxnMode) -> alopex_core::Result<Self::Transaction<'_>> {
        self.0.begin(mode)
    }
}

pub async fn create_counter(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<CounterCreateRequest>,
) -> Response {
    let outcome = create_counter_outcome(&state, &context, request);
    crdt_response(outcome, state.config.max_response_size)
}

pub async fn read_counter(
    Path(object_id): Path<String>,
    Extension(state): Extension<Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<CounterReadRequest>,
) -> Response {
    let outcome = read_counter_outcome(&state, &context, object_id, request);
    crdt_response(outcome, state.config.max_response_size)
}

pub(crate) fn create_counter_outcome(
    state: &ServerState,
    context: &RequestContext,
    request: CounterCreateRequest,
) -> CrdtOutcome {
    let actor = match state.auth.authorize_crdt(context.actor.as_deref()) {
        Ok(subject) => subject.as_str().to_owned(),
        Err(_) => {
            let envelope =
                request.unchecked_envelope(context.actor.as_deref().unwrap_or("unauthenticated"));
            return counter_rejection(
                &envelope,
                OperationState::Rejected,
                FailureClass::Unauthorized,
                RoutingOutcome::new(
                    RoutingOutcomeKind::Blocked,
                    Some(envelope.range.clone()),
                    0,
                    "authorization_denied",
                ),
                false,
            );
        }
    };
    let unchecked = request.unchecked_envelope(actor.as_str());
    let envelope = match request.into_envelope(actor) {
        Ok(envelope) => envelope,
        Err(_) => {
            return counter_rejection(
                &unchecked,
                OperationState::Rejected,
                FailureClass::InvalidRequest,
                RoutingOutcome::new(
                    RoutingOutcomeKind::Blocked,
                    Some(unchecked.range.clone()),
                    0,
                    "counter_create_request_invalid",
                ),
                false,
            );
        }
    };

    let (single_node, metadata_version) = match state.cluster_manager.read() {
        Ok(manager) => (
            manager.status_snapshot().mode == alopex_cluster::ClusterMode::SingleNode,
            manager.identity().update_epoch,
        ),
        Err(_) => {
            return counter_rejection(
                &envelope,
                OperationState::Rejected,
                FailureClass::Internal,
                RoutingOutcome::new(
                    RoutingOutcomeKind::Blocked,
                    Some(envelope.range.clone()),
                    0,
                    "cluster_manager_unavailable",
                ),
                false,
            );
        }
    };
    let routing = RoutingOutcome::new(
        RoutingOutcomeKind::LocalOnly,
        Some(envelope.range.clone()),
        metadata_version,
        "single_node_valid_lease",
    );
    let policy = CrdtPreExecutionPolicy::evaluate(&CrdtPolicyInput {
        lifecycle: CrdtLifecycleAction::Create,
        authorized: true,
        range_freshness: CrdtRangeFreshness::Current,
        // The current server has no replica exchange adapter.  It may execute
        // only the explicitly local single-node profile; cluster-aware calls
        // fail closed before the ledger instead of reporting local durability
        // as replica convergence.
        chirps_ready: single_node,
        node_available: state.lifecycle_state.check_write_allowed().is_ok(),
        resource_available: true,
        timed_out: false,
        routing,
    });
    if !policy.permit_ledger {
        return counter_rejection(
            &envelope,
            policy.state,
            policy
                .failure_class
                .expect("rejected CRDT policy has a failure class"),
            policy.routing,
            policy.retryable,
        );
    }

    let projection = CrdtCounterProjection::new(ServerCrdtStore(state.store.as_ref()));
    match projection.apply(&envelope, envelope.state_epoch) {
        Ok(result) => {
            let common = envelope.common_fields(
                result.ledger.first_state,
                result.ledger.first_failure_class,
                policy.routing,
                false,
                result.ledger.idempotency_result(),
            );
            CrdtOutcome::counter(common, result.value)
        }
        Err(error) => counter_projection_failure(&envelope, metadata_version, error),
    }
}

pub(crate) fn read_counter_outcome(
    state: &ServerState,
    context: &RequestContext,
    object_id: String,
    request: CounterReadRequest,
) -> CrdtOutcome {
    let actor = match state.auth.authorize_crdt(context.actor.as_deref()) {
        Ok(subject) => subject.as_str().to_owned(),
        Err(_) => {
            let envelope = request.unchecked_envelope(
                object_id,
                context.actor.as_deref().unwrap_or("unauthenticated"),
            );
            return counter_rejection(
                &envelope,
                OperationState::Rejected,
                FailureClass::Unauthorized,
                RoutingOutcome::new(
                    RoutingOutcomeKind::Blocked,
                    Some(envelope.range.clone()),
                    0,
                    "authorization_denied",
                ),
                false,
            );
        }
    };
    let unchecked = request.unchecked_envelope(object_id.clone(), actor.as_str());
    let envelope = match request.into_envelope(object_id, actor) {
        Ok(envelope) => envelope,
        Err(_) => {
            return counter_rejection(
                &unchecked,
                OperationState::Rejected,
                FailureClass::InvalidRequest,
                RoutingOutcome::new(
                    RoutingOutcomeKind::Blocked,
                    Some(unchecked.range.clone()),
                    0,
                    "counter_read_request_invalid",
                ),
                false,
            );
        }
    };

    let (single_node, metadata_version) = match state.cluster_manager.read() {
        Ok(manager) => (
            manager.status_snapshot().mode == alopex_cluster::ClusterMode::SingleNode,
            manager.identity().update_epoch,
        ),
        Err(_) => {
            return counter_rejection(
                &envelope,
                OperationState::Rejected,
                FailureClass::Internal,
                RoutingOutcome::new(
                    RoutingOutcomeKind::Blocked,
                    Some(envelope.range.clone()),
                    0,
                    "cluster_manager_unavailable",
                ),
                false,
            );
        }
    };
    let routing = RoutingOutcome::new(
        RoutingOutcomeKind::LocalOnly,
        Some(envelope.range.clone()),
        metadata_version,
        "single_node_valid_lease",
    );
    let policy = CrdtPreExecutionPolicy::evaluate(&CrdtPolicyInput {
        lifecycle: CrdtLifecycleAction::Read,
        authorized: true,
        range_freshness: CrdtRangeFreshness::Current,
        chirps_ready: single_node,
        node_available: true,
        resource_available: true,
        timed_out: false,
        routing,
    });
    if !policy.permit_ledger {
        return counter_rejection(
            &envelope,
            policy.state,
            policy
                .failure_class
                .expect("rejected CRDT policy has a failure class"),
            policy.routing,
            policy.retryable,
        );
    }

    let projection = CrdtCounterProjection::new(ServerCrdtStore(state.store.as_ref()));
    match projection.read(&envelope) {
        Ok(value) => {
            let common = envelope.common_fields(
                OperationState::Committed,
                None,
                policy.routing,
                false,
                IdempotencyResult {
                    operation_id: envelope.operation_id.clone(),
                    request_id: envelope.request_id.clone(),
                    first_outcome: "counter_read".to_string(),
                    state: OperationState::Committed,
                    duplicate_count: 0,
                },
            );
            CrdtOutcome::counter(common, value)
        }
        Err(CrdtCounterError::MissingProjection { .. }) => counter_rejection(
            &envelope,
            OperationState::Rejected,
            FailureClass::PrerequisiteMissing,
            RoutingOutcome::new(
                RoutingOutcomeKind::Blocked,
                Some(envelope.range.clone()),
                metadata_version,
                "counter_not_found",
            ),
            false,
        ),
        Err(error) => counter_projection_failure(&envelope, metadata_version, error),
    }
}

impl CounterCreateRequest {
    fn into_envelope(self, actor: impl Into<NodeId>) -> Result<CrdtOperationEnvelope, ()> {
        CrdtOperationEnvelope::new(
            self.object_id,
            self.range,
            actor,
            self.request_id,
            self.operation_id,
            self.update_version,
            CrdtOperationKind::CounterCreate,
            CrdtPayload::Counter {
                initial_value: Some(self.initial_value),
                delta: None,
            },
        )
        .map_err(|_| ())
    }

    fn unchecked_envelope(&self, actor: impl Into<NodeId>) -> CrdtOperationEnvelope {
        CrdtOperationEnvelope {
            object_type: alopex_cluster::CrdtObjectType::Counter,
            object_id: self.object_id.clone(),
            range: self.range.clone(),
            state_epoch: self.range.data_epoch,
            actor: actor.into(),
            request_id: self.request_id.clone(),
            operation_id: self.operation_id.clone(),
            update_version: self.update_version,
            operation: CrdtOperationKind::CounterCreate,
            payload: CrdtPayload::Counter {
                initial_value: Some(self.initial_value),
                delta: None,
            },
        }
    }
}

impl CounterReadRequest {
    fn into_envelope(
        self,
        object_id: String,
        actor: impl Into<NodeId>,
    ) -> Result<CrdtOperationEnvelope, ()> {
        CrdtOperationEnvelope::new(
            object_id,
            self.range,
            actor,
            self.request_id,
            self.operation_id,
            self.update_version,
            CrdtOperationKind::CounterRead,
            CrdtPayload::None,
        )
        .map_err(|_| ())
    }

    fn unchecked_envelope(
        &self,
        object_id: String,
        actor: impl Into<NodeId>,
    ) -> CrdtOperationEnvelope {
        CrdtOperationEnvelope {
            object_type: alopex_cluster::CrdtObjectType::Counter,
            object_id,
            range: self.range.clone(),
            state_epoch: self.range.data_epoch,
            actor: actor.into(),
            request_id: self.request_id.clone(),
            operation_id: self.operation_id.clone(),
            update_version: self.update_version,
            operation: CrdtOperationKind::CounterRead,
            payload: CrdtPayload::None,
        }
    }
}

fn counter_projection_failure(
    envelope: &CrdtOperationEnvelope,
    metadata_version: u64,
    error: CrdtCounterError,
) -> CrdtOutcome {
    let (failure_class, reason) = match error {
        CrdtCounterError::AlreadyExists { .. } => {
            (FailureClass::Conflict, "counter_already_exists")
        }
        CrdtCounterError::InvalidCounterPayload | CrdtCounterError::WrongOperation { .. } => {
            (FailureClass::InvalidRequest, "counter_payload_invalid")
        }
        CrdtCounterError::ArithmeticOverflow => {
            (FailureClass::InvalidRequest, "counter_value_out_of_range")
        }
        CrdtCounterError::MissingProjection { .. }
        | CrdtCounterError::Ledger(_)
        | CrdtCounterError::Storage(_)
        | CrdtCounterError::Encode(_)
        | CrdtCounterError::Decode(_) => (FailureClass::Internal, "counter_projection_failed"),
    };
    counter_rejection(
        envelope,
        OperationState::Rejected,
        failure_class,
        RoutingOutcome::new(
            RoutingOutcomeKind::Blocked,
            Some(envelope.range.clone()),
            metadata_version,
            reason,
        ),
        false,
    )
}

fn counter_rejection(
    envelope: &CrdtOperationEnvelope,
    state: OperationState,
    failure_class: FailureClass,
    routing: RoutingOutcome,
    retryable: bool,
) -> CrdtOutcome {
    let reason = routing.reason_code.clone();
    let common = envelope.common_fields(
        state,
        Some(failure_class),
        routing,
        retryable,
        IdempotencyResult {
            operation_id: envelope.operation_id.clone(),
            request_id: envelope.request_id.clone(),
            first_outcome: reason.clone(),
            state,
            duplicate_count: 0,
        },
    );
    CrdtOutcome::counter_unavailable(common, reason)
}

fn crdt_response(outcome: CrdtOutcome, max_response_size: usize) -> Response {
    let status = axum::http::StatusCode::from_u16(outcome.surface_status().http_status)
        .expect("CRDT surface status is a valid HTTP status");
    match outcome.canonical_bytes() {
        Ok(bytes) if bytes.len() <= max_response_size => (status, Json(outcome)).into_response(),
        Ok(_) => (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "error": "crdt_response_too_large",
            })),
        )
            .into_response(),
        Err(_) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "crdt_response_serialization_failed",
            })),
        )
            .into_response(),
    }
}
