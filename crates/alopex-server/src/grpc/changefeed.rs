//! Typed gRPC projection of the shared changefeed lifecycle façade.
//!
//! This module owns only protobuf validation and wire/status translation.
//! Authorization, Durable preflight, range resolution, and every lifecycle
//! transition stay in `http::changefeed`, which is transport-neutral despite
//! its historical module location.

use prost::Message;
use tonic::{Code, Response, Status};

use alopex_cluster::{
    changefeed::{
        AckResult, ChangeEventEnvelope, ChangeOperationType, ChangePayload, ChangefeedOutcome,
        ChangefeedResult, Checkpoint, FeedDelivery, FeedIdentity, RetentionWindow,
    },
    IdempotencyResult, RangeIdentity, RoutingOutcome,
};

use crate::{
    http::{
        changefeed::{
            self as changefeed_core, AckChangefeedRequest, CreateChangefeedRequest, DeliveryQuery,
            LifecycleRequest, ResumeChangefeedRequest, SubscribeChangefeedRequest,
        },
        RequestContext,
    },
    server::ServerState,
};

use super::{proto, GrpcContext};

const CONTRACT_VERSION: u32 = 1;

pub(crate) type ChangefeedStream = tokio_stream::Iter<
    std::vec::IntoIter<std::result::Result<proto::ChangefeedStreamItemV1, Status>>,
>;

pub(crate) fn create(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::CreateChangefeedRequestV1,
) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
    ensure_contract_version(request.contract_version)?;
    let table = target_table(&request);
    let range_id = target_range(&request);
    let request = CreateChangefeedRequest {
        request_id: request.request_id,
        tenant: request.tenant,
        actor: request.actor,
        table,
        range_id,
        retention: retention_from_proto(request.retention),
        change_kinds: request
            .change_kinds
            .into_iter()
            .map(change_kind_from_proto)
            .collect::<std::result::Result<_, _>>()?,
    };
    outcome_response(
        changefeed_core::create_changefeed(state, &http_context(context), request)
            .map_err(|error| super::map_status(error, &context.correlation_id))?,
        context,
    )
}

pub(crate) fn subscribe(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::SubscribeChangefeedRequestV1,
) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
    ensure_contract_version(request.contract_version)?;
    outcome_response(
        changefeed_core::subscribe_changefeed(
            state,
            &http_context(context),
            &request.feed_id,
            SubscribeChangefeedRequest {
                request_id: request.request_id,
                expected_generation: request.expected_generation,
                expected_epoch: request.expected_epoch,
            },
        )
        .map_err(|error| super::map_status(error, &context.correlation_id))?,
        context,
    )
}

pub(crate) fn poll(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::DeliveryChangefeedRequestV1,
) -> std::result::Result<Response<proto::ChangefeedDeliveryV1>, Status> {
    ensure_contract_version(request.contract_version)?;
    delivery_response(
        changefeed_core::deliver_changefeed(
            state,
            &http_context(context),
            &request.feed_id,
            delivery_query(&request),
            alopex_cluster::changefeed::ChangefeedAction::Poll,
        )
        .map_err(|error| super::map_status(error, &context.correlation_id))?,
        context,
    )
}

pub(crate) fn stream(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::DeliveryChangefeedRequestV1,
) -> std::result::Result<Response<ChangefeedStream>, Status> {
    ensure_contract_version(request.contract_version)?;
    let delivery = changefeed_core::deliver_changefeed(
        state,
        &http_context(context),
        &request.feed_id,
        delivery_query(&request),
        alopex_cluster::changefeed::ChangefeedAction::Stream,
    )
    .map_err(|error| super::map_status(error, &context.correlation_id))?;
    let outcome = outcome_to_proto(&delivery.outcome, &context.correlation_id);
    if delivery.outcome.surface_status().grpc_code != "OK" {
        return Err(outcome_status(&delivery.outcome, outcome));
    }

    let mut items = delivery
        .events
        .iter()
        .map(|event| {
            Ok(proto::ChangefeedStreamItemV1 {
                contract_version: CONTRACT_VERSION,
                correlation_id: context.correlation_id.clone(),
                item_type: "event".to_owned(),
                event: Some(event_to_proto(event)),
                outcome: None,
            })
        })
        .collect::<Vec<_>>();
    items.push(Ok(proto::ChangefeedStreamItemV1 {
        contract_version: CONTRACT_VERSION,
        correlation_id: context.correlation_id.clone(),
        item_type: "outcome".to_owned(),
        event: None,
        outcome: Some(outcome),
    }));
    Ok(Response::new(tokio_stream::iter(items)))
}

pub(crate) fn ack(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::AckChangefeedRequestV1,
) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
    ensure_contract_version(request.contract_version)?;
    outcome_response(
        changefeed_core::ack_changefeed(
            state,
            &http_context(context),
            &request.feed_id,
            AckChangefeedRequest {
                request_id: request.request_id,
                ack_id: request.ack_id,
                checkpoint: request.checkpoint,
            },
        )
        .map_err(|error| super::map_status(error, &context.correlation_id))?,
        context,
    )
}

pub(crate) fn resume(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::ResumeChangefeedRequestV1,
) -> std::result::Result<Response<proto::ChangefeedDeliveryV1>, Status> {
    ensure_contract_version(request.contract_version)?;
    delivery_response(
        changefeed_core::resume_changefeed(
            state,
            &http_context(context),
            &request.feed_id,
            ResumeChangefeedRequest {
                request_id: request.request_id,
                checkpoint: request.checkpoint,
            },
        )
        .map_err(|error| super::map_status(error, &context.correlation_id))?,
        context,
    )
}

pub(crate) fn cancel(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::LifecycleChangefeedRequestV1,
) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
    lifecycle_response(
        state,
        context,
        request,
        alopex_cluster::changefeed::ChangefeedAction::Cancel,
    )
}

pub(crate) fn close(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::LifecycleChangefeedRequestV1,
) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
    lifecycle_response(
        state,
        context,
        request,
        alopex_cluster::changefeed::ChangefeedAction::Close,
    )
}

fn lifecycle_response(
    state: &ServerState,
    context: &GrpcContext,
    request: proto::LifecycleChangefeedRequestV1,
    action: alopex_cluster::changefeed::ChangefeedAction,
) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
    ensure_contract_version(request.contract_version)?;
    outcome_response(
        changefeed_core::close_changefeed(
            state,
            &http_context(context),
            &request.feed_id,
            LifecycleRequest {
                request_id: request.request_id,
            },
            action,
        )
        .map_err(|error| super::map_status(error, &context.correlation_id))?,
        context,
    )
}

fn http_context(context: &GrpcContext) -> RequestContext {
    RequestContext {
        correlation_id: context.correlation_id.clone(),
        actor: context.actor.clone(),
    }
}

fn ensure_contract_version(version: u32) -> std::result::Result<(), Status> {
    if version == CONTRACT_VERSION {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "unsupported changefeed contract_version {version}; expected {CONTRACT_VERSION}"
        )))
    }
}

fn target_table(request: &proto::CreateChangefeedRequestV1) -> Option<String> {
    match &request.target {
        Some(proto::create_changefeed_request_v1::Target::Table(table)) => Some(table.clone()),
        _ => None,
    }
}

fn target_range(request: &proto::CreateChangefeedRequestV1) -> Option<String> {
    match &request.target {
        Some(proto::create_changefeed_request_v1::Target::RangeId(range_id)) => {
            Some(range_id.clone())
        }
        _ => None,
    }
}

fn retention_from_proto(retention: Option<proto::ChangefeedRetentionV1>) -> RetentionWindow {
    let retention = retention.unwrap_or_default();
    RetentionWindow {
        deadline_epoch: retention
            .has_deadline_epoch
            .then_some(retention.deadline_epoch),
        retained_through_position: retention
            .has_retained_through_position
            .then_some(retention.retained_through_position),
    }
}

fn delivery_query(request: &proto::DeliveryChangefeedRequestV1) -> DeliveryQuery {
    DeliveryQuery {
        request_id: request.request_id.clone(),
        max_events: request.max_events as usize,
        deadline_epoch: request.deadline_epoch,
    }
}

fn change_kind_from_proto(value: i32) -> std::result::Result<ChangeOperationType, Status> {
    match proto::ChangefeedOperationTypeV1::try_from(value) {
        Ok(proto::ChangefeedOperationTypeV1::Insert) => Ok(ChangeOperationType::Insert),
        Ok(proto::ChangefeedOperationTypeV1::Update) => Ok(ChangeOperationType::Update),
        Ok(proto::ChangefeedOperationTypeV1::Delete) => Ok(ChangeOperationType::Delete),
        Ok(proto::ChangefeedOperationTypeV1::Schema) => Ok(ChangeOperationType::Schema),
        Ok(proto::ChangefeedOperationTypeV1::Tombstone) => Ok(ChangeOperationType::Tombstone),
        Ok(proto::ChangefeedOperationTypeV1::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("change_kind must be specified"))
        }
    }
}

fn outcome_response(
    outcome: ChangefeedOutcome,
    context: &GrpcContext,
) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
    let wire = outcome_to_proto(&outcome, &context.correlation_id);
    if outcome.surface_status().grpc_code == "OK" {
        Ok(Response::new(wire))
    } else {
        Err(outcome_status(&outcome, wire))
    }
}

fn delivery_response(
    delivery: FeedDelivery,
    context: &GrpcContext,
) -> std::result::Result<Response<proto::ChangefeedDeliveryV1>, Status> {
    let outcome = outcome_to_proto(&delivery.outcome, &context.correlation_id);
    if delivery.outcome.surface_status().grpc_code != "OK" {
        return Err(outcome_status(&delivery.outcome, outcome));
    }
    Ok(Response::new(proto::ChangefeedDeliveryV1 {
        contract_version: CONTRACT_VERSION,
        outcome: Some(outcome),
        events: delivery.events.iter().map(event_to_proto).collect(),
        correlation_id: context.correlation_id.clone(),
    }))
}

fn outcome_status(outcome: &ChangefeedOutcome, wire: proto::ChangefeedOutcomeV1) -> Status {
    Status::with_details(
        grpc_code(outcome.surface_status().grpc_code),
        format!(
            "changefeed request failed (correlation_id={})",
            wire.correlation_id
        ),
        wire.encode_to_vec().into(),
    )
}

fn grpc_code(code: &str) -> Code {
    match code {
        "UNAUTHENTICATED" => Code::Unauthenticated,
        "ABORTED" => Code::Aborted,
        "FAILED_PRECONDITION" => Code::FailedPrecondition,
        "UNAVAILABLE" => Code::Unavailable,
        "DEADLINE_EXCEEDED" => Code::DeadlineExceeded,
        "INVALID_ARGUMENT" => Code::InvalidArgument,
        "UNIMPLEMENTED" => Code::Unimplemented,
        "CANCELLED" => Code::Cancelled,
        _ => Code::Internal,
    }
}

fn outcome_to_proto(
    outcome: &ChangefeedOutcome,
    correlation_id: &str,
) -> proto::ChangefeedOutcomeV1 {
    let (result_type, feed_result, event, ack) = match &outcome.result {
        ChangefeedResult::Feed => (
            "feed".to_owned(),
            Some(proto::ChangefeedFeedResultV1 {}),
            None,
            None,
        ),
        ChangefeedResult::Event(event) => {
            ("event".to_owned(), None, Some(event_to_proto(event)), None)
        }
        ChangefeedResult::Ack(ack) => ("ack".to_owned(), None, None, Some(ack_to_proto(ack))),
    };
    proto::ChangefeedOutcomeV1 {
        contract_version: CONTRACT_VERSION,
        feed: Some(feed_to_proto(&outcome.feed)),
        operation_id: outcome.operation_id.clone(),
        request_id: outcome.request_id.as_str().to_owned(),
        operation_state: enum_wire(&outcome.operation_state),
        failure_class: outcome
            .failure_class
            .as_ref()
            .map(enum_wire)
            .unwrap_or_default(),
        reason_code: outcome.reason_code.clone().unwrap_or_default(),
        routing: Some(routing_to_proto(&outcome.routing)),
        retryable: outcome.retryable,
        idempotency: Some(idempotency_to_proto(&outcome.idempotency)),
        result_type,
        feed_result,
        event,
        ack,
        correlation_id: correlation_id.to_owned(),
    }
}

fn feed_to_proto(feed: &FeedIdentity) -> proto::ChangefeedFeedIdentityV1 {
    proto::ChangefeedFeedIdentityV1 {
        feed_id: feed.feed_id.clone(),
        range: Some(range_identity_to_proto(&feed.range)),
        generation: feed.generation,
        placement: Some(proto::ChangefeedPlacementV1 {
            owner_node: feed.placement.owner_node.as_str().to_owned(),
            replica_nodes: feed
                .placement
                .replica_nodes
                .iter()
                .map(|node| node.as_str().to_owned())
                .collect(),
            role: enum_wire(&feed.placement.role),
            readiness: enum_wire(&feed.placement.readiness),
            placement_epoch: feed.placement.placement_epoch,
        }),
        ordering_scope: enum_wire(&feed.ordering_scope),
        retention: Some(retention_to_proto(&feed.retention)),
        status: enum_wire(&feed.status),
    }
}

fn routing_to_proto(routing: &RoutingOutcome) -> proto::ChangefeedRoutingOutcomeV1 {
    proto::ChangefeedRoutingOutcomeV1 {
        kind: enum_wire(&routing.kind),
        range: routing.range_identity.as_ref().map(range_identity_to_proto),
        has_range: routing.range_identity.is_some(),
        metadata_version: routing.metadata_version,
        reason_code: routing.reason_code.clone(),
    }
}

fn idempotency_to_proto(idempotency: &IdempotencyResult) -> proto::ChangefeedIdempotencyResultV1 {
    proto::ChangefeedIdempotencyResultV1 {
        operation_id: idempotency.operation_id.clone(),
        request_id: idempotency.request_id.as_str().to_owned(),
        first_outcome: idempotency.first_outcome.clone(),
        state: enum_wire(&idempotency.state),
        duplicate_count: idempotency.duplicate_count,
    }
}

fn checkpoint_to_proto(checkpoint: &Checkpoint) -> proto::ChangefeedCheckpointV1 {
    proto::ChangefeedCheckpointV1 {
        feed_id: checkpoint.feed_id.clone(),
        range_id: checkpoint.range_id.as_str().to_owned(),
        generation: checkpoint.generation,
        commit_position: checkpoint.commit_position,
        payload_ordinal: checkpoint.payload_ordinal,
        epoch: checkpoint.epoch,
        retention_deadline: checkpoint.retention_deadline.unwrap_or_default(),
        has_retention_deadline: checkpoint.retention_deadline.is_some(),
    }
}

fn ack_to_proto(ack: &AckResult) -> proto::ChangefeedAckResultV1 {
    proto::ChangefeedAckResultV1 {
        ack_id: ack.ack_id.clone(),
        ack_state: enum_wire(&ack.ack_state),
        committed_checkpoint: ack.committed_checkpoint.as_ref().map(checkpoint_to_proto),
        has_committed_checkpoint: ack.committed_checkpoint.is_some(),
        next_resume_position: ack.next_resume_position.as_ref().map(checkpoint_to_proto),
        has_next_resume_position: ack.next_resume_position.is_some(),
        operation_state: enum_wire(&ack.operation_state),
        failure_class: ack
            .failure_class
            .as_ref()
            .map(enum_wire)
            .unwrap_or_default(),
        reason_code: ack.reason_code.clone().unwrap_or_default(),
        retryable: ack.retryable,
        idempotency: Some(idempotency_to_proto(&ack.idempotency)),
    }
}

fn event_to_proto(event: &ChangeEventEnvelope) -> proto::ChangefeedEventV1 {
    let ChangePayload {
        payload,
        payload_unavailable,
    } = &event.payload;
    proto::ChangefeedEventV1 {
        event_id: event.event_id.clone(),
        feed_id: event.feed_id.clone(),
        range: Some(range_identity_to_proto(&event.range)),
        generation: event.generation,
        operation_id: event.operation_id.clone(),
        request_id: event.request_id.as_str().to_owned(),
        commit_position: event.commit_position,
        payload_ordinal: event.payload_ordinal,
        operation_type: operation_type_to_proto(event.operation_type),
        key_or_hash: event.key_or_hash.clone(),
        payload: payload.clone().unwrap_or_default(),
        has_payload: payload.is_some(),
        payload_unavailable: payload_unavailable.clone().unwrap_or_default(),
        checkpoint: Some(checkpoint_to_proto(&event.checkpoint)),
        operation_state: enum_wire(&event.operation_state),
        failure_class: event
            .failure_class
            .as_ref()
            .map(enum_wire)
            .unwrap_or_default(),
        reason_code: event.reason_code.clone().unwrap_or_default(),
        routing: Some(routing_to_proto(&event.routing)),
        retryable: event.retryable,
        idempotency: Some(idempotency_to_proto(&event.idempotency)),
    }
}

fn operation_type_to_proto(operation_type: ChangeOperationType) -> i32 {
    let operation_type = match operation_type {
        ChangeOperationType::Insert => proto::ChangefeedOperationTypeV1::Insert,
        ChangeOperationType::Update => proto::ChangefeedOperationTypeV1::Update,
        ChangeOperationType::Delete => proto::ChangefeedOperationTypeV1::Delete,
        ChangeOperationType::Schema => proto::ChangefeedOperationTypeV1::Schema,
        ChangeOperationType::Tombstone => proto::ChangefeedOperationTypeV1::Tombstone,
    };
    operation_type as i32
}

fn retention_to_proto(retention: &RetentionWindow) -> proto::ChangefeedRetentionV1 {
    proto::ChangefeedRetentionV1 {
        deadline_epoch: retention.deadline_epoch.unwrap_or_default(),
        has_deadline_epoch: retention.deadline_epoch.is_some(),
        retained_through_position: retention.retained_through_position.unwrap_or_default(),
        has_retained_through_position: retention.retained_through_position.is_some(),
    }
}

fn range_identity_to_proto(range: &RangeIdentity) -> proto::CrdtRangeIdentity {
    proto::CrdtRangeIdentity {
        cluster_id: range.cluster_id.as_str().to_owned(),
        table_id: range.table_id,
        range_id: range.range_id.as_str().to_owned(),
        lower_bound: range.lower_bound.clone().unwrap_or_default(),
        has_lower_bound: range.lower_bound.is_some(),
        upper_bound: range.upper_bound.clone().unwrap_or_default(),
        has_upper_bound: range.upper_bound.is_some(),
        schema_version: range.schema_version,
        data_epoch: range.data_epoch,
    }
}

fn enum_wire(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .expect("changefeed enum serializes")
        .as_str()
        .expect("changefeed enum serializes as string")
        .to_owned()
}
