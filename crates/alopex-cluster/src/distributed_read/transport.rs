//! Typed transport contracts for remote range reads.

use alopex_core::ReadAtPoint;
use alopex_sql::StorageRangeConstraint;
use alopex_sql::distributed_read::{
    REMOTE_READ_CATALOG_VERSION, RemoteReadDescriptor, RemoteReadShape,
};
use alopex_sql::executor::Row;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{RangeId, RequestId, VerifiedPeerIdentity};

use super::{
    DelegationAuthorizationError, DelegationValidationContext, LocalReadAuthorizationRecheck,
    ReadDelegationCredential, ReadDelegationVerifier, ReadOperationScope, verify_and_recheck,
};

/// Request metadata that must exactly agree with the signed delegation before a worker opens storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteReadAuthorizationEnvelope {
    pub credential: ReadDelegationCredential,
    pub range_id: RangeId,
    pub table_id: u32,
    pub operation: ReadOperationScope,
    pub request_id: RequestId,
    pub query_digest: String,
    pub read_fence_digest: String,
    pub read_at: ReadAtPoint,
}

/// Fenced payload accepted by a range worker after authenticated transport.
///
/// The descriptor is deliberately a closed catalog descriptor rather than a
/// serialized logical plan. The worker validates its canonical digest against
/// the signed authorization envelope before opening any storage snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRangeReadRequest {
    pub authorization: RemoteReadAuthorizationEnvelope,
    pub descriptor: RemoteReadDescriptor,
    pub constraint: StorageRangeConstraint,
    /// Absolute worker-clock deadline in milliseconds.
    pub deadline_ms: u64,
}

/// A successful cleanup acknowledgement. An [`RangeReadEnd`] is never emitted
/// without this acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupAcknowledgement {
    pub request_id: RequestId,
}

/// A range-bounded batch, retained by the worker until the terminal cleanup
/// succeeds so an error cannot surface a partial successful result.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeReadBatch {
    pub request_id: RequestId,
    pub rows: Vec<Row>,
}

/// Typed terminal payload for a completed range worker request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeReadEnd {
    pub request_id: RequestId,
    pub row_count: u64,
    pub cleanup: CleanupAcknowledgement,
}

/// Request validation performed by a worker before it opens a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemoteRangeReadRequestError {
    #[error("remote read deadline has elapsed")]
    DeadlineElapsed,
    #[error("remote read descriptor is not a recognized v0.8 catalog entry: {0}")]
    DescriptorRejected(String),
    #[error("remote read descriptor digest does not match the authorized query")]
    QueryDigestMismatch,
    #[error("remote read range constraint does not match the authorization envelope")]
    ConstraintMismatch,
    #[error("remote read range fence digest does not match the authorization envelope")]
    FenceDigestMismatch,
    #[error("could not encode canonical remote read request data: {0}")]
    Encoding(String),
}

impl RemoteRangeReadRequest {
    /// Validates the closed descriptor, deadline, range/table/read-at fence,
    /// and canonical descriptor/fence digests before a backend can open.
    pub fn validate_before_open(&self, now_ms: u64) -> Result<(), RemoteRangeReadRequestError> {
        if now_ms >= self.deadline_ms {
            return Err(RemoteRangeReadRequestError::DeadlineElapsed);
        }
        validate_descriptor(&self.descriptor)?;
        if self.authorization.range_id.as_str() != self.constraint.range_id()
            || self.authorization.table_id != self.constraint.table_id()
            || self.authorization.read_at != self.constraint.snapshot().read_at()
        {
            return Err(RemoteRangeReadRequestError::ConstraintMismatch);
        }
        if descriptor_digest(&self.descriptor)? != self.authorization.query_digest {
            return Err(RemoteRangeReadRequestError::QueryDigestMismatch);
        }
        if range_fence_digest(&self.constraint)? != self.authorization.read_fence_digest {
            return Err(RemoteRangeReadRequestError::FenceDigestMismatch);
        }
        Ok(())
    }
}

/// Computes the stable digest that the gateway binds into its delegation
/// credential for a closed catalog descriptor.
pub fn descriptor_digest(
    descriptor: &RemoteReadDescriptor,
) -> Result<String, RemoteRangeReadRequestError> {
    canonical_digest(descriptor)
}

/// Computes the stable digest that binds a worker request to exactly one
/// generation, SQL table, row-key interval, schema manifest, and read point.
pub fn range_fence_digest(
    constraint: &StorageRangeConstraint,
) -> Result<String, RemoteRangeReadRequestError> {
    #[derive(Serialize)]
    struct Fence<'a> {
        range_id: &'a str,
        generation: u64,
        table_id: u32,
        lower_inclusive: &'a [u8],
        upper_exclusive: &'a [u8],
        schema_manifest_id: &'a str,
        read_at: ReadAtPoint,
    }
    let (lower_inclusive, upper_exclusive) = constraint.encoded_bounds();
    canonical_digest(&Fence {
        range_id: constraint.range_id(),
        generation: constraint.generation(),
        table_id: constraint.table_id(),
        lower_inclusive,
        upper_exclusive,
        schema_manifest_id: constraint.snapshot().schema_manifest_id(),
        read_at: constraint.snapshot().read_at(),
    })
}

fn canonical_digest(value: &impl Serialize) -> Result<String, RemoteRangeReadRequestError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| RemoteRangeReadRequestError::Encoding(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn validate_descriptor(
    descriptor: &RemoteReadDescriptor,
) -> Result<(), RemoteRangeReadRequestError> {
    if descriptor.catalog_version != REMOTE_READ_CATALOG_VERSION {
        return Err(RemoteRangeReadRequestError::DescriptorRejected(
            "catalog version mismatch".into(),
        ));
    }
    if descriptor.table.trim().is_empty() {
        return Err(RemoteRangeReadRequestError::DescriptorRejected(
            "empty table identity".into(),
        ));
    }
    if matches!(&descriptor.shape, RemoteReadShape::Aggregate { aggregates } if aggregates.is_empty())
    {
        return Err(RemoteRangeReadRequestError::DescriptorRejected(
            "aggregate descriptor has no aggregate identity".into(),
        ));
    }
    Ok(())
}

/// Performs all credential checks before a range worker receives a query payload.
pub fn authorize_remote_read(
    peer: VerifiedPeerIdentity,
    envelope: &RemoteReadAuthorizationEnvelope,
    now_ms: u64,
    verifier: &dyn ReadDelegationVerifier,
    local_authorizer: &dyn LocalReadAuthorizationRecheck,
) -> Result<(), DelegationAuthorizationError> {
    verify_and_recheck(
        &envelope.credential,
        &DelegationValidationContext {
            peer,
            range_id: envelope.range_id.clone(),
            table_id: envelope.table_id,
            operation: envelope.operation,
            request_id: envelope.request_id.clone(),
            query_digest: envelope.query_digest.clone(),
            read_fence_digest: envelope.read_fence_digest.clone(),
            read_at: envelope.read_at,
            now_ms,
        },
        verifier,
        local_authorizer,
    )
}
