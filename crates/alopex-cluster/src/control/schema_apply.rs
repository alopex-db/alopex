//! Adapter from verified local catalog evidence to committed schema rollout.

use crate::{
    MetadataActor, MetadataCommand, MetadataCommandEnvelope, MetadataConsensusStore, RequestId,
    SchemaApplyEvidence, SchemaControlError, SchemaControlResult, SchemaControlService,
};

/// The authenticated, idempotent envelope used to publish local catalog apply
/// evidence.  The control validator additionally requires `actor.node_id` to
/// equal `evidence.member`, preventing one member from reporting another
/// member as Applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaApplyEvidenceRequest {
    pub request_id: RequestId,
    pub request_fingerprint: String,
    pub actor: MetadataActor,
    pub expected_version: Option<u64>,
    pub evidence: SchemaApplyEvidence,
}

/// Turns already verified local catalog facts into the one allowed committed
/// `RecordSchemaApply` metadata command.  It does not apply SQL or own a
/// remote connection.
#[derive(Debug, Default)]
pub struct SchemaApplyEvidenceAdapter;

impl SchemaApplyEvidenceAdapter {
    pub fn submit<S: MetadataConsensusStore>(
        &self,
        control: &mut SchemaControlService<S>,
        request: SchemaApplyEvidenceRequest,
    ) -> Result<SchemaControlResult, SchemaControlError> {
        control.submit(MetadataCommandEnvelope {
            request_id: request.request_id,
            request_fingerprint: request.request_fingerprint,
            actor: request.actor,
            expected_version: request.expected_version,
            command: MetadataCommand::RecordSchemaApply {
                evidence: request.evidence,
            },
        })
    }
}
