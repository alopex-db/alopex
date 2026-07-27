use sha2::{Digest, Sha256};

use crate::{
    FailureClass, IdempotencyResult, NodeId, OperationState, RangeIdentity, RequestId,
    RoutingOutcome,
};

/// The only object types exposed by the Phase 2 CRDT contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrdtObjectType {
    Counter,
    Set,
}

/// The canonical ten-operation public register shared by every adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrdtOperationKind {
    CounterCreate,
    CounterRead,
    CounterIncrement,
    CounterDecrement,
    SetCreate,
    SetRead,
    SetAdd,
    SetRemove,
    SetContains,
    SetList,
}

impl CrdtOperationKind {
    pub const fn object_type(self) -> CrdtObjectType {
        match self {
            Self::CounterCreate
            | Self::CounterRead
            | Self::CounterIncrement
            | Self::CounterDecrement => CrdtObjectType::Counter,
            Self::SetCreate
            | Self::SetRead
            | Self::SetAdd
            | Self::SetRemove
            | Self::SetContains
            | Self::SetList => CrdtObjectType::Set,
        }
    }
}

/// Operation-specific data represented without adapter-specific wire types.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "payload_type", rename_all = "snake_case")]
pub enum CrdtPayload {
    None,
    Counter {
        #[serde(default)]
        initial_value: Option<i64>,
        #[serde(default)]
        delta: Option<i64>,
    },
    Set {
        #[serde(default)]
        member: Option<String>,
    },
}

/// A mutation/read identity that can be serialized identically by every
/// adapter before it reaches persistence or replica exchange.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrdtOperationEnvelope {
    pub object_type: CrdtObjectType,
    pub object_id: String,
    pub range: RangeIdentity,
    /// F1 data epoch captured when the operation was evaluated.
    pub state_epoch: u64,
    pub actor: NodeId,
    pub request_id: RequestId,
    pub operation_id: String,
    /// F1-epoch-scoped ordering value. Counter and Set projections define its
    /// operation-specific interpretation in their own modules.
    pub update_version: u64,
    pub operation: CrdtOperationKind,
    pub payload: CrdtPayload,
}

impl CrdtOperationEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        object_id: impl Into<String>,
        range: RangeIdentity,
        actor: impl Into<NodeId>,
        request_id: impl Into<RequestId>,
        operation_id: impl Into<String>,
        update_version: u64,
        operation: CrdtOperationKind,
        payload: CrdtPayload,
    ) -> Result<Self, CrdtEnvelopeError> {
        let object_id = object_id.into();
        let operation_id = operation_id.into();
        if object_id.is_empty() {
            return Err(CrdtEnvelopeError::EmptyObjectId);
        }
        if operation_id.is_empty() {
            return Err(CrdtEnvelopeError::EmptyOperationId);
        }
        if !payload_matches(operation, &payload) {
            return Err(CrdtEnvelopeError::InvalidPayload { operation });
        }

        Ok(Self {
            object_type: operation.object_type(),
            object_id,
            state_epoch: range.data_epoch,
            range,
            actor: actor.into(),
            request_id: request_id.into(),
            operation_id,
            update_version,
            operation,
            payload,
        })
    }

    /// Stable bytes for durable identity and cross-surface
    /// fixtures. This contract has no unordered map fields.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, CrdtEnvelopeError> {
        serde_json::to_vec(self).map_err(CrdtEnvelopeError::Serialize)
    }

    /// Digest of the full canonical operation identity and payload.
    pub fn canonical_digest(&self) -> Result<String, CrdtEnvelopeError> {
        Ok(sha256_hex(self.canonical_bytes()?))
    }

    /// Digest of the operation payload only. The durable ledger combines this
    /// with the separately persisted operation identity and scope.
    pub fn payload_digest(&self) -> Result<String, CrdtEnvelopeError> {
        let payload = serde_json::to_vec(&self.payload).map_err(CrdtEnvelopeError::Serialize)?;
        Ok(sha256_hex(payload))
    }

    /// Builds the public common fields while preserving the exact Phase 1
    /// state, failure, routing and idempotency contracts.
    pub fn common_fields(
        &self,
        state: OperationState,
        failure_class: Option<FailureClass>,
        routing: RoutingOutcome,
        retryable: bool,
        idempotency: IdempotencyResult,
    ) -> CrdtCommonFields {
        CrdtCommonFields {
            object_type: self.object_type,
            object_id: self.object_id.clone(),
            range: self.range.clone(),
            state_epoch: self.state_epoch,
            actor: self.actor.clone(),
            request_id: self.request_id.clone(),
            operation_id: self.operation_id.clone(),
            state,
            failure_class,
            routing,
            retryable,
            idempotency,
        }
    }
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Exact Phase 1 common fields carried by Counter and Set outcomes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrdtCommonFields {
    pub object_type: CrdtObjectType,
    pub object_id: String,
    pub range: RangeIdentity,
    pub state_epoch: u64,
    pub actor: NodeId,
    pub request_id: RequestId,
    pub operation_id: String,
    pub state: OperationState,
    #[serde(default)]
    pub failure_class: Option<FailureClass>,
    pub routing: RoutingOutcome,
    pub retryable: bool,
    pub idempotency: IdempotencyResult,
}

#[derive(Debug, thiserror::Error)]
pub enum CrdtEnvelopeError {
    #[error("CRDT object_id must not be empty")]
    EmptyObjectId,
    #[error("CRDT operation_id must not be empty")]
    EmptyOperationId,
    #[error("payload is invalid for {operation:?}")]
    InvalidPayload { operation: CrdtOperationKind },
    #[error("failed to serialize CRDT envelope: {0}")]
    Serialize(serde_json::Error),
}

fn payload_matches(operation: CrdtOperationKind, payload: &CrdtPayload) -> bool {
    match operation {
        CrdtOperationKind::CounterCreate => matches!(
            payload,
            CrdtPayload::Counter {
                initial_value: Some(_),
                delta: None,
            }
        ),
        CrdtOperationKind::CounterIncrement | CrdtOperationKind::CounterDecrement => matches!(
            payload,
            CrdtPayload::Counter {
                initial_value: None,
                delta: Some(_),
            }
        ),
        CrdtOperationKind::SetAdd
        | CrdtOperationKind::SetRemove
        | CrdtOperationKind::SetContains => matches!(
            payload,
            CrdtPayload::Set {
                member: Some(member)
            } if !member.is_empty()
        ),
        CrdtOperationKind::CounterRead
        | CrdtOperationKind::SetCreate
        | CrdtOperationKind::SetRead
        | CrdtOperationKind::SetList => matches!(payload, CrdtPayload::None),
    }
}

#[cfg(test)]
mod tests {
    use super::{CrdtOperationEnvelope, CrdtOperationKind, CrdtPayload};
    use crate::{
        IdempotencyResult, OperationState, RangeIdentity, RoutingOutcome, RoutingOutcomeKind,
    };

    fn range() -> RangeIdentity {
        RangeIdentity::new("cluster-a", 7, "range-a", None, None, 1, 9)
    }

    #[test]
    fn envelope_serialization_and_payload_digest_are_deterministic() {
        let envelope = CrdtOperationEnvelope::new(
            "counter-a",
            range(),
            "node-a",
            "request-a",
            "operation-a",
            12,
            CrdtOperationKind::CounterCreate,
            CrdtPayload::Counter {
                initial_value: Some(-3),
                delta: None,
            },
        )
        .expect("valid envelope");

        assert_eq!(
            envelope.canonical_bytes().unwrap(),
            envelope.canonical_bytes().unwrap()
        );
        assert_eq!(
            envelope.canonical_digest().unwrap(),
            envelope.canonical_digest().unwrap()
        );
        assert_eq!(
            envelope.payload_digest().unwrap(),
            envelope.payload_digest().unwrap()
        );

        let encoded = String::from_utf8(envelope.canonical_bytes().unwrap()).unwrap();
        assert!(encoded.contains("\"object_type\":\"counter\""));
        assert!(encoded.contains("\"operation\":\"counter_create\""));
        assert!(encoded.contains("\"state_epoch\":9"));

        let replay_with_new_identity = CrdtOperationEnvelope::new(
            "counter-a",
            range(),
            "node-b",
            "request-b",
            "operation-b",
            12,
            CrdtOperationKind::CounterCreate,
            CrdtPayload::Counter {
                initial_value: Some(-3),
                delta: None,
            },
        )
        .expect("valid envelope");
        assert_ne!(
            envelope.canonical_digest().unwrap(),
            replay_with_new_identity.canonical_digest().unwrap()
        );
        assert_eq!(
            envelope.payload_digest().unwrap(),
            replay_with_new_identity.payload_digest().unwrap()
        );
    }

    #[test]
    fn common_fields_reuse_phase_one_outcome_types_without_new_states() {
        let envelope = CrdtOperationEnvelope::new(
            "set-a",
            range(),
            "node-a",
            "request-a",
            "operation-a",
            13,
            CrdtOperationKind::SetAdd,
            CrdtPayload::Set {
                member: Some("member-a".to_string()),
            },
        )
        .expect("valid envelope");
        let routing = RoutingOutcome::new(
            RoutingOutcomeKind::SingleRange,
            Some(range()),
            9,
            "lease_valid",
        );
        let fields = envelope.common_fields(
            OperationState::Accepted,
            None,
            routing,
            true,
            IdempotencyResult {
                operation_id: "operation-a".to_string(),
                request_id: "request-a".into(),
                first_outcome: "accepted".to_string(),
                state: OperationState::Accepted,
                duplicate_count: 0,
            },
        );

        let encoded = serde_json::to_string(&fields).expect("serialize common fields");
        assert!(encoded.contains("\"state\":\"accepted\""));
        assert!(encoded.contains("\"kind\":\"single_range\""));
        assert!(encoded.contains("\"duplicate_count\":0"));
    }

    #[test]
    fn envelope_rejects_missing_identity_or_wrong_payload_before_persistence() {
        let empty_object = CrdtOperationEnvelope::new(
            "",
            range(),
            "node-a",
            "request-a",
            "operation-a",
            0,
            CrdtOperationKind::SetCreate,
            CrdtPayload::None,
        );
        assert!(empty_object.is_err());

        let wrong_payload = CrdtOperationEnvelope::new(
            "counter-a",
            range(),
            "node-a",
            "request-a",
            "operation-a",
            0,
            CrdtOperationKind::CounterRead,
            CrdtPayload::Counter {
                initial_value: Some(1),
                delta: None,
            },
        );
        assert!(wrong_payload.is_err());
    }
}
