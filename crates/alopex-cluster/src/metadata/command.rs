//! Admission validation for metadata-management commands.

use super::model::{
    ClusterReadPolicy, CommittedMetadata, ManagementOutcome, MemberRecord, RangeReplicaEvidence,
    RangeReplicaLifecycle, RangeRoutingDefinition, SchemaApplyEvidence, SchemaApplyState,
    SchemaManifest,
};
use crate::{
    NodeId, PlacementMetadata, RangeId, RequestId, SchemaManifestId, StableDiagnosticCode,
};
use sha2::Digest;
use std::{collections::BTreeSet, error::Error, fmt};

/// Permission required for one public metadata-management area.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthorizationScope {
    Membership,
    Range,
    Placement,
    ReadPolicy,
    Schema,
}

/// Authenticated management actor and its allowed metadata scopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataActor {
    pub node_id: NodeId,
    pub allowed_scopes: BTreeSet<AuthorizationScope>,
}

impl MetadataActor {
    pub fn authorized_for(node_id: impl Into<NodeId>, scope: AuthorizationScope) -> Self {
        Self {
            node_id: node_id.into(),
            allowed_scopes: BTreeSet::from([scope]),
        }
    }

    pub fn allows(&self, scope: AuthorizationScope) -> bool {
        self.allowed_scopes.contains(&scope)
    }
}

/// Supported management mutations. No variant carries user SQL DDL or data
/// movement; range transfer and catalog application have their own later
/// adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCommand {
    AdmitMember {
        member: MemberRecord,
    },
    ReplaceMember {
        retired_node_id: NodeId,
        replacement: MemberRecord,
    },
    RegisterRange {
        definition: RangeRoutingDefinition,
    },
    UpdateRange {
        definition: RangeRoutingDefinition,
    },
    RetireRange {
        range_id: RangeId,
    },
    SetPlacement {
        range_id: RangeId,
        placement: PlacementMetadata,
    },
    RecordRangeReplica {
        evidence: RangeReplicaEvidence,
    },
    SetReadPolicy {
        policy: ClusterReadPolicy,
    },
    SetSchemaOwner {
        owner: NodeId,
    },
    ProposeSchemaManifest {
        manifest: SchemaManifest,
    },
    CommitSchemaManifest {
        manifest_id: SchemaManifestId,
    },
    RecordSchemaApply {
        evidence: SchemaApplyEvidence,
    },
}

impl MetadataCommand {
    fn required_scope(&self) -> AuthorizationScope {
        match self {
            Self::AdmitMember { .. } | Self::ReplaceMember { .. } => AuthorizationScope::Membership,
            Self::RegisterRange { .. }
            | Self::UpdateRange { .. }
            | Self::RetireRange { .. }
            | Self::RecordRangeReplica { .. } => AuthorizationScope::Range,
            Self::SetPlacement { .. } => AuthorizationScope::Placement,
            Self::SetReadPolicy { .. } => AuthorizationScope::ReadPolicy,
            Self::SetSchemaOwner { .. }
            | Self::ProposeSchemaManifest { .. }
            | Self::CommitSchemaManifest { .. }
            | Self::RecordSchemaApply { .. } => AuthorizationScope::Schema,
        }
    }
}

/// A command request whose id/fingerprint protect against accidental duplicate
/// state changes and conflicting reuse of one request ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCommandEnvelope {
    pub request_id: RequestId,
    pub request_fingerprint: String,
    pub actor: MetadataActor,
    pub expected_version: Option<u64>,
    pub command: MetadataCommand,
}

/// A command that passed all pre-consensus checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedMetadataCommand(MetadataCommandEnvelope);

impl ValidatedMetadataCommand {
    pub fn envelope(&self) -> &MetadataCommandEnvelope {
        &self.0
    }
}

/// Either a new command eligible for consensus proposal or the immutable
/// outcome of an idempotent retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationDecision {
    Apply(Box<ValidatedMetadataCommand>),
    Idempotent(ManagementOutcome),
}

/// A stable pre-proposal rejection. Callers map this to terminal or retryable
/// management output without exposing consensus internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataValidationError {
    pub code: StableDiagnosticCode,
    pub message: String,
}

impl MetadataValidationError {
    fn new(code: StableDiagnosticCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for MetadataValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl Error for MetadataValidationError {}

/// Validates only immutable metadata intent. It neither applies a mutation nor
/// initiates Raft; the consensus adapter consumes [`ValidatedMetadataCommand`]
/// in the next task.
#[derive(Debug, Default)]
pub struct MetadataCommandValidator;

impl MetadataCommandValidator {
    pub fn validate(
        &self,
        metadata: &CommittedMetadata,
        envelope: MetadataCommandEnvelope,
    ) -> Result<ValidationDecision, MetadataValidationError> {
        validate_request_identity(&envelope)?;
        if let Some(existing) = metadata
            .operation(&envelope.request_id)
            .or_else(|| metadata.membership_operation(&envelope.request_id))
        {
            return idempotent_or_conflict(existing, &envelope.request_fingerprint);
        }
        if let Some(expected_version) = envelope.expected_version
            && expected_version != metadata.state_version()
        {
            return Err(MetadataValidationError::new(
                StableDiagnosticCode::StaleMetadataVersion,
                format!(
                    "expected metadata version {expected_version}, current version is {}",
                    metadata.state_version()
                ),
            ));
        }

        let required_scope = envelope.command.required_scope();
        if !envelope.actor.allows(required_scope) {
            return Err(MetadataValidationError::new(
                StableDiagnosticCode::Unauthorized,
                format!("actor is not authorized for {required_scope:?} metadata management"),
            ));
        }

        validate_command(metadata, &envelope)?;
        Ok(ValidationDecision::Apply(Box::new(
            ValidatedMetadataCommand(envelope),
        )))
    }
}

fn validate_request_identity(
    envelope: &MetadataCommandEnvelope,
) -> Result<(), MetadataValidationError> {
    if envelope.request_id.as_str().trim().is_empty()
        || envelope.request_fingerprint.trim().is_empty()
    {
        return Err(MetadataValidationError::new(
            StableDiagnosticCode::RequestConflict,
            "request_id and request_fingerprint must be non-empty",
        ));
    }
    Ok(())
}

fn idempotent_or_conflict(
    existing: &ManagementOutcome,
    request_fingerprint: &str,
) -> Result<ValidationDecision, MetadataValidationError> {
    if existing.request_fingerprint == request_fingerprint {
        Ok(ValidationDecision::Idempotent(existing.clone()))
    } else {
        Err(MetadataValidationError::new(
            StableDiagnosticCode::RequestConflict,
            "request_id was already committed with a different request fingerprint",
        ))
    }
}

fn validate_command(
    metadata: &CommittedMetadata,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), MetadataValidationError> {
    match &envelope.command {
        MetadataCommand::AdmitMember { member } => {
            let node_id = &member.identity.node_id;
            if member.identity.cluster_id.as_ref() != Some(metadata.cluster_id()) {
                return Err(invalid(
                    "member cluster_id does not match committed cluster",
                ));
            }
            if metadata.members().contains_key(node_id) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "member node_id is already committed",
                ));
            }
        }
        MetadataCommand::ReplaceMember {
            retired_node_id,
            replacement,
        } => {
            if !metadata.members().contains_key(retired_node_id) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "replacement target is not a committed member",
                ));
            }
            if metadata
                .members()
                .contains_key(&replacement.identity.node_id)
            {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "replacement node_id is already committed",
                ));
            }
        }
        MetadataCommand::RegisterRange { definition } => {
            if metadata.ranges().contains_key(&definition.range_id) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "range_id is already committed; use UpdateRange with the expected version",
                ));
            }
            validate_range_definition(definition)?;
            let mut candidate = ranges_for_table(metadata, definition);
            candidate.push(definition);
            validate_complete_non_overlapping_coverage(&candidate)?;
        }
        MetadataCommand::UpdateRange { definition } => {
            if !metadata.ranges().contains_key(&definition.range_id) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "range_id is not committed; use RegisterRange",
                ));
            }
            validate_range_definition(definition)?;
            let mut candidate = ranges_for_table(metadata, definition);
            candidate.retain(|range| range.range_id != definition.range_id);
            candidate.push(definition);
            validate_complete_non_overlapping_coverage(&candidate)?;
        }
        MetadataCommand::RetireRange { range_id } => {
            if !metadata.ranges().contains_key(range_id) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "range_id is not committed",
                ));
            }
        }
        MetadataCommand::SetPlacement {
            range_id,
            placement,
        } => {
            let range = metadata.ranges().get(range_id).ok_or_else(|| {
                MetadataValidationError::new(
                    StableDiagnosticCode::InvalidRange,
                    "placement requires a committed range definition",
                )
            })?;
            if placement.table_ref != range.table_ref || placement.table_id != range.table_id {
                return Err(invalid(
                    "placement table does not match the committed range",
                ));
            }
            if !placement
                .ranges
                .iter()
                .any(|item| &item.range_id == range_id)
            {
                return Err(invalid(
                    "placement must retain the referenced logical range",
                ));
            }
        }
        MetadataCommand::RecordRangeReplica { evidence } => {
            let range = metadata.ranges().get(&evidence.range_id).ok_or_else(|| {
                MetadataValidationError::new(
                    StableDiagnosticCode::InvalidRange,
                    "replica evidence requires a committed range definition",
                )
            })?;
            if evidence.generation != range.generation {
                return Err(invalid(
                    "replica evidence generation does not match the range",
                ));
            }
            if evidence.lifecycle == RangeReplicaLifecycle::Ready {
                let coverage = evidence.coverage.as_ref().ok_or_else(|| {
                    MetadataValidationError::new(
                        StableDiagnosticCode::RangeCoverageIncomplete,
                        "Ready replica evidence requires a coverage proof",
                    )
                })?;
                if coverage.generation != range.generation
                    || coverage.lower_inclusive != range.lower_inclusive
                    || coverage.upper_exclusive != range.upper_exclusive
                    || coverage.data_epoch != evidence.data_epoch
                    || coverage.index_epoch != evidence.index_epoch
                {
                    return Err(MetadataValidationError::new(
                        StableDiagnosticCode::RangeCoverageIncomplete,
                        "replica coverage proof does not match the committed range and epochs",
                    ));
                }
            }
        }
        MetadataCommand::SetReadPolicy { .. } => {}
        MetadataCommand::SetSchemaOwner { owner } => {
            if !metadata.members().contains_key(owner) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "schema owner must be a committed member",
                ));
            }
        }
        MetadataCommand::ProposeSchemaManifest { manifest } => {
            require_schema_owner(metadata, envelope)?;
            if manifest.owner != envelope.actor.node_id {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::SchemaOwnerRequired,
                    "schema manifest owner must match the authenticated metadata actor",
                ));
            }
            if metadata.schema_manifests().contains_key(&manifest.id) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "schema manifest id is already committed",
                ));
            }
            if manifest.parent_id != metadata.schema().active_manifest {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "schema manifest parent must match the current active manifest",
                ));
            }
            validate_schema_manifest(manifest)?;
        }
        MetadataCommand::CommitSchemaManifest { manifest_id } => {
            require_schema_owner(metadata, envelope)?;
            let manifest = metadata
                .schema_manifests()
                .get(manifest_id)
                .ok_or_else(|| {
                    MetadataValidationError::new(
                        StableDiagnosticCode::RequestConflict,
                        "cannot commit a schema manifest that was not proposed",
                    )
                })?;
            if manifest.parent_id != metadata.schema().active_manifest {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "schema manifest parent no longer matches the current active manifest",
                ));
            }
            validate_schema_manifest(manifest)?;
        }
        MetadataCommand::RecordSchemaApply { evidence } => {
            let member = metadata.members().get(&evidence.member).ok_or_else(|| {
                MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "schema apply evidence references an unknown member",
                )
            })?;
            if member.lifecycle != super::model::MemberLifecycle::Active {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "schema apply evidence requires an active member",
                ));
            }
            if envelope.actor.node_id != evidence.member {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::Unauthorized,
                    "schema apply evidence must be submitted by the authenticated member",
                ));
            }
            let manifest = metadata
                .schema_manifests()
                .get(&evidence.manifest_id)
                .ok_or_else(|| {
                    MetadataValidationError::new(
                        StableDiagnosticCode::RequestConflict,
                        "schema apply evidence references an unknown manifest",
                    )
                })?;
            if metadata.schema().active_manifest.as_ref() != Some(&evidence.manifest_id) {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "schema apply evidence must reference the active manifest",
                ));
            }
            validate_schema_apply_evidence(manifest, evidence)?;
        }
    }
    Ok(())
}

fn validate_schema_manifest(manifest: &SchemaManifest) -> Result<(), MetadataValidationError> {
    if manifest.id.as_str().trim().is_empty()
        || manifest.catalog_delta_format.trim().is_empty()
        || manifest.catalog_delta.is_empty()
        || manifest.checksum.trim().is_empty()
    {
        return Err(MetadataValidationError::new(
            StableDiagnosticCode::RequestConflict,
            "schema manifest identity, catalog delta format, payload, and checksum must be present",
        ));
    }
    if manifest.compatibility.minimum_catalog_version
        > manifest.compatibility.maximum_catalog_version
    {
        return Err(MetadataValidationError::new(
            StableDiagnosticCode::RequestConflict,
            "schema manifest compatibility range is invalid",
        ));
    }
    let actual = format!("{:x}", sha2::Sha256::digest(&manifest.catalog_delta));
    if actual != manifest.checksum {
        return Err(MetadataValidationError::new(
            StableDiagnosticCode::RequestConflict,
            "schema manifest checksum does not match the immutable catalog delta",
        ));
    }
    Ok(())
}

fn validate_schema_apply_evidence(
    manifest: &SchemaManifest,
    evidence: &SchemaApplyEvidence,
) -> Result<(), MetadataValidationError> {
    match evidence.state {
        SchemaApplyState::Applied => {
            if evidence.catalog_version != Some(manifest.schema_version)
                || evidence.checksum.as_deref() != Some(manifest.checksum.as_str())
                || !evidence.compatibility_verified
            {
                return Err(MetadataValidationError::new(
                    StableDiagnosticCode::RequestConflict,
                    "Applied schema evidence requires matching catalog version, checksum, and compatibility verification",
                ));
            }
        }
        SchemaApplyState::Incompatible if evidence.compatibility_verified => {
            return Err(MetadataValidationError::new(
                StableDiagnosticCode::RequestConflict,
                "incompatible schema evidence cannot claim compatibility verification",
            ));
        }
        SchemaApplyState::Pending | SchemaApplyState::Applying
            if evidence.catalog_version.is_some()
                || evidence.checksum.is_some()
                || evidence.compatibility_verified =>
        {
            return Err(MetadataValidationError::new(
                StableDiagnosticCode::RequestConflict,
                "pending or applying schema evidence cannot claim completed verification",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn require_schema_owner(
    metadata: &CommittedMetadata,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), MetadataValidationError> {
    if metadata.schema().owner.as_ref() == Some(&envelope.actor.node_id) {
        Ok(())
    } else {
        Err(MetadataValidationError::new(
            StableDiagnosticCode::SchemaOwnerRequired,
            "only the committed schema owner may propose or commit a schema manifest",
        ))
    }
}

fn validate_range_definition(
    definition: &RangeRoutingDefinition,
) -> Result<(), MetadataValidationError> {
    if definition.range_id.as_str().trim().is_empty()
        || definition.table_ref.as_str().trim().is_empty()
    {
        return Err(invalid("range_id and table_ref must be non-empty"));
    }
    if let (Some(lower), Some(upper)) = (&definition.lower_inclusive, &definition.upper_exclusive)
        && lower >= upper
    {
        return Err(invalid(
            "range lower_inclusive must sort before upper_exclusive",
        ));
    }
    Ok(())
}

fn ranges_for_table<'a>(
    metadata: &'a CommittedMetadata,
    definition: &RangeRoutingDefinition,
) -> Vec<&'a RangeRoutingDefinition> {
    metadata
        .ranges()
        .values()
        .filter(|range| {
            range.table_ref == definition.table_ref && range.table_id == definition.table_id
        })
        .collect()
}

fn validate_complete_non_overlapping_coverage(
    ranges: &[&RangeRoutingDefinition],
) -> Result<(), MetadataValidationError> {
    if ranges.is_empty() {
        return Err(MetadataValidationError::new(
            StableDiagnosticCode::RangeCoverageIncomplete,
            "an active table requires at least one range",
        ));
    }
    let mut sorted = ranges.to_vec();
    sorted.sort_by(|left, right| left.lower_inclusive.cmp(&right.lower_inclusive));
    if sorted[0].lower_inclusive.is_some()
        || sorted
            .last()
            .is_some_and(|range| range.upper_exclusive.is_some())
    {
        return Err(MetadataValidationError::new(
            StableDiagnosticCode::RangeCoverageIncomplete,
            "active ranges must cover from the table minimum through its end",
        ));
    }
    for pair in sorted.windows(2) {
        let previous_upper = pair[0].upper_exclusive.as_ref().ok_or_else(|| {
            MetadataValidationError::new(
                StableDiagnosticCode::InvalidRange,
                "only the final active range may have an unbounded upper edge",
            )
        })?;
        let next_lower = pair[1].lower_inclusive.as_ref().ok_or_else(|| {
            MetadataValidationError::new(
                StableDiagnosticCode::InvalidRange,
                "only the first active range may have an unbounded lower edge",
            )
        })?;
        if previous_upper != next_lower {
            return Err(MetadataValidationError::new(
                StableDiagnosticCode::InvalidRange,
                "active ranges overlap or leave a gap in canonical row-key coverage",
            ));
        }
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> MetadataValidationError {
    MetadataValidationError::new(StableDiagnosticCode::InvalidRange, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterId, MemberIdentity, NodeRole};

    fn actor(scope: AuthorizationScope) -> MetadataActor {
        MetadataActor::authorized_for("node-a", scope)
    }

    fn envelope(
        request_id: &str,
        fingerprint: &str,
        actor: MetadataActor,
        command: MetadataCommand,
    ) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope {
            request_id: RequestId::new(request_id),
            request_fingerprint: fingerprint.to_string(),
            actor,
            expected_version: Some(0),
            command,
        }
    }

    fn full_range(id: &str) -> RangeRoutingDefinition {
        RangeRoutingDefinition {
            range_id: RangeId::new(id),
            table_ref: crate::TableRef::new("default.public.users"),
            table_id: 7,
            lower_inclusive: None,
            upper_exclusive: None,
            generation: 1,
        }
    }

    #[test]
    fn unauthorized_mutation_is_rejected_before_proposal() {
        let metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let result = MetadataCommandValidator.validate(
            &metadata,
            envelope(
                "request-1",
                "digest-1",
                actor(AuthorizationScope::ReadPolicy),
                MetadataCommand::RegisterRange {
                    definition: full_range("range-a"),
                },
            ),
        );

        assert_eq!(result.unwrap_err().code, StableDiagnosticCode::Unauthorized);
    }

    #[test]
    fn stale_expected_version_is_rejected_before_proposal() {
        let metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let mut request = envelope(
            "request-1",
            "digest-1",
            actor(AuthorizationScope::Range),
            MetadataCommand::RegisterRange {
                definition: full_range("range-a"),
            },
        );
        request.expected_version = Some(1);

        assert_eq!(
            MetadataCommandValidator
                .validate(&metadata, request)
                .unwrap_err()
                .code,
            StableDiagnosticCode::StaleMetadataVersion
        );
    }

    #[test]
    fn matching_retry_returns_existing_outcome_but_conflicting_reuse_is_rejected() {
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        metadata.record_operation_for_apply(ManagementOutcome::pending("request-1", "digest-1"));
        let validator = MetadataCommandValidator;
        let command = MetadataCommand::SetReadPolicy {
            policy: ClusterReadPolicy::default(),
        };

        assert!(matches!(
            validator
                .validate(
                    &metadata,
                    envelope(
                        "request-1",
                        "digest-1",
                        actor(AuthorizationScope::ReadPolicy),
                        command.clone()
                    ),
                )
                .unwrap(),
            ValidationDecision::Idempotent(_)
        ));
        assert_eq!(
            validator
                .validate(
                    &metadata,
                    envelope(
                        "request-1",
                        "different",
                        actor(AuthorizationScope::ReadPolicy),
                        command
                    ),
                )
                .unwrap_err()
                .code,
            StableDiagnosticCode::RequestConflict
        );
    }

    #[test]
    fn incomplete_or_invalid_range_coverage_cannot_enter_committed_state() {
        let metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let mut partial = full_range("range-a");
        partial.lower_inclusive = Some(vec![1]);
        partial.upper_exclusive = Some(vec![2]);
        let validator = MetadataCommandValidator;

        assert_eq!(
            validator
                .validate(
                    &metadata,
                    envelope(
                        "request-1",
                        "digest-1",
                        actor(AuthorizationScope::Range),
                        MetadataCommand::RegisterRange {
                            definition: partial
                        },
                    ),
                )
                .unwrap_err()
                .code,
            StableDiagnosticCode::RangeCoverageIncomplete
        );

        let mut invalid = full_range("range-b");
        invalid.lower_inclusive = Some(vec![2]);
        invalid.upper_exclusive = Some(vec![1]);
        assert_eq!(
            validator
                .validate(
                    &metadata,
                    envelope(
                        "request-2",
                        "digest-2",
                        actor(AuthorizationScope::Range),
                        MetadataCommand::RegisterRange {
                            definition: invalid
                        },
                    ),
                )
                .unwrap_err()
                .code,
            StableDiagnosticCode::InvalidRange
        );
    }

    #[test]
    fn overlapping_ranges_cannot_enter_committed_state() {
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        metadata.record_range_for_apply(full_range("range-a"));

        assert_eq!(
            MetadataCommandValidator
                .validate(
                    &metadata,
                    envelope(
                        "request-2",
                        "digest-2",
                        actor(AuthorizationScope::Range),
                        MetadataCommand::RegisterRange {
                            definition: full_range("range-b")
                        },
                    ),
                )
                .unwrap_err()
                .code,
            StableDiagnosticCode::InvalidRange
        );
    }

    #[test]
    fn member_admission_requires_the_committed_cluster_identity() {
        let metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let member = MemberRecord::new(
            MemberIdentity {
                node_id: NodeId::new("node-b"),
                cluster_id: Some(ClusterId::new("cluster-other")),
                advertised_endpoint: None,
                role: NodeRole::Worker,
            },
            super::super::model::MemberLifecycle::Admitted,
        );

        assert_eq!(
            MetadataCommandValidator
                .validate(
                    &metadata,
                    envelope(
                        "request-1",
                        "digest-1",
                        actor(AuthorizationScope::Membership),
                        MetadataCommand::AdmitMember { member },
                    ),
                )
                .unwrap_err()
                .code,
            StableDiagnosticCode::InvalidRange
        );
    }
}
