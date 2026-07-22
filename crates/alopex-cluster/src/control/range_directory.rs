//! Committed replica readiness and coverage proof evaluation.

use super::range_transfer::RangeTransferAck;
use crate::{
    CommittedMetadata, NodeId, RangeId, RangeReplicaEvidence, RangeReplicaLifecycle,
    RangeRoutingDefinition, SchemaManifestId,
};
use alopex_core::CanonicalRowKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeReplicaReadinessState {
    Ready,
    Provisioning,
    CoverageInvalid,
    SchemaStale,
    EpochStale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeReplicaReadiness {
    pub range_id: RangeId,
    pub node_id: crate::NodeId,
    pub state: RangeReplicaReadinessState,
}

/// Read-only directory derived exclusively from committed range and replica
/// evidence. Gossip/reachability observations are never accepted as readiness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeReplicaDirectory {
    entries: Vec<RangeReplicaReadiness>,
}

impl RangeReplicaDirectory {
    pub fn from_committed(metadata: &CommittedMetadata) -> Self {
        let active_manifest = metadata.schema().active_manifest.as_ref();
        let entries = metadata
            .ranges()
            .iter()
            .flat_map(|(range_id, definition)| {
                metadata
                    .range_replicas()
                    .get(range_id)
                    .into_iter()
                    .flat_map(move |replicas| {
                        replicas
                            .values()
                            .map(move |evidence| RangeReplicaReadiness {
                                range_id: range_id.clone(),
                                node_id: evidence.node_id.clone(),
                                state: readiness(definition, evidence, active_manifest),
                            })
                    })
            })
            .collect();
        Self { entries }
    }

    pub fn entries(&self) -> &[RangeReplicaReadiness] {
        &self.entries
    }

    pub fn routing_eligible(&self, range_id: &RangeId) -> Vec<&RangeReplicaReadiness> {
        self.entries
            .iter()
            .filter(|entry| {
                entry.range_id == *range_id && entry.state == RangeReplicaReadinessState::Ready
            })
            .collect()
    }

    pub fn can_retire_replica(&self, range_id: &RangeId, node_id: &crate::NodeId) -> bool {
        self.routing_eligible(range_id)
            .iter()
            .any(|entry| entry.node_id != *node_id)
    }

    /// Builds readiness evidence only from a target acknowledgement that was
    /// persisted after verified snapshot-plus-suffix apply.
    pub fn evidence_after_verified_transfer(
        definition: &RangeRoutingDefinition,
        node_id: NodeId,
        schema_manifest_id: Option<SchemaManifestId>,
        ack: &RangeTransferAck,
    ) -> Option<RangeReplicaEvidence> {
        if ack.range_id() != &definition.range_id
            || ack.generation() != definition.generation
            || ack.schema_manifest_id() != schema_manifest_id.as_ref()
        {
            return None;
        }
        Some(RangeReplicaEvidence {
            range_id: definition.range_id.clone(),
            node_id,
            generation: definition.generation,
            schema_manifest_id,
            data_epoch: ack.final_epoch(),
            index_epoch: ack.final_epoch(),
            lifecycle: RangeReplicaLifecycle::Ready,
            coverage: Some(crate::RangeCoverageProof {
                generation: definition.generation,
                lower_inclusive: definition.lower_inclusive.clone(),
                upper_exclusive: definition.upper_exclusive.clone(),
                data_epoch: ack.final_epoch(),
                index_epoch: ack.final_epoch(),
                content_hash: ack.content_hash().to_string(),
            }),
        })
    }
}

fn readiness(
    definition: &RangeRoutingDefinition,
    evidence: &RangeReplicaEvidence,
    active_manifest: Option<&crate::SchemaManifestId>,
) -> RangeReplicaReadinessState {
    if evidence.lifecycle != RangeReplicaLifecycle::Ready {
        return RangeReplicaReadinessState::Provisioning;
    }
    if evidence.generation != definition.generation {
        return RangeReplicaReadinessState::EpochStale;
    }
    if active_manifest.is_some() && evidence.schema_manifest_id.as_ref() != active_manifest {
        return RangeReplicaReadinessState::SchemaStale;
    }
    let Some(coverage) = &evidence.coverage else {
        return RangeReplicaReadinessState::CoverageInvalid;
    };
    if coverage.generation != definition.generation
        || coverage.data_epoch != evidence.data_epoch
        || coverage.index_epoch != evidence.index_epoch
        || coverage.content_hash.trim().is_empty()
        || coverage.lower_inclusive != definition.lower_inclusive
        || coverage.upper_exclusive != definition.upper_exclusive
        || !valid_bounds(definition)
    {
        return RangeReplicaReadinessState::CoverageInvalid;
    }
    RangeReplicaReadinessState::Ready
}

fn valid_bounds(definition: &RangeRoutingDefinition) -> bool {
    let decode = |bound: &Option<Vec<u8>>| {
        bound
            .as_deref()
            .map(CanonicalRowKey::decode)
            .transpose()
            .ok()
            .flatten()
    };
    let lower = decode(&definition.lower_inclusive);
    let upper = decode(&definition.upper_exclusive);
    if definition.lower_inclusive.is_some() && lower.is_none()
        || definition.upper_exclusive.is_some() && upper.is_none()
    {
        return false;
    }
    let table_matches =
        |key: Option<CanonicalRowKey>| key.is_none_or(|key| key.table_id() == definition.table_id);
    table_matches(lower)
        && table_matches(upper)
        && match (lower, upper) {
            (Some(lower), Some(upper)) => lower < upper,
            _ => true,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterId, RangeCoverageProof, RangeReplicaEvidence, SchemaManifestId};

    fn metadata() -> (CommittedMetadata, RangeRoutingDefinition) {
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let definition = RangeRoutingDefinition {
            range_id: RangeId::new("range-a"),
            table_ref: crate::TableRef::new("default.public.users"),
            table_id: 7,
            lower_inclusive: Some(CanonicalRowKey::new(7, 10).encode()),
            upper_exclusive: Some(CanonicalRowKey::new(7, 20).encode()),
            generation: 2,
        };
        metadata.record_range_for_apply(definition.clone());
        (metadata, definition)
    }

    fn ready_evidence(definition: &RangeRoutingDefinition, node_id: &str) -> RangeReplicaEvidence {
        RangeReplicaEvidence {
            range_id: definition.range_id.clone(),
            node_id: crate::NodeId::new(node_id),
            generation: 2,
            schema_manifest_id: Some(SchemaManifestId::new("schema-1")),
            data_epoch: 8,
            index_epoch: 9,
            lifecycle: RangeReplicaLifecycle::Ready,
            coverage: Some(RangeCoverageProof {
                generation: 2,
                lower_inclusive: definition.lower_inclusive.clone(),
                upper_exclusive: definition.upper_exclusive.clone(),
                data_epoch: 8,
                index_epoch: 9,
                content_hash: "hash".to_string(),
            }),
        }
    }

    #[test]
    fn ready_requires_matching_generation_bounds_and_epochs() {
        let (mut metadata, definition) = metadata();
        let mut evidence = ready_evidence(&definition, "node-a");
        evidence.coverage.as_mut().unwrap().index_epoch = 10;
        metadata.record_replica_for_apply(evidence);

        let directory = RangeReplicaDirectory::from_committed(&metadata);
        assert_eq!(
            directory.entries()[0].state,
            RangeReplicaReadinessState::CoverageInvalid
        );
    }

    #[test]
    fn only_ready_replacement_allows_replica_retirement() {
        let (mut metadata, definition) = metadata();
        let mut first = ready_evidence(&definition, "node-a");
        first.schema_manifest_id = None;
        let mut second = ready_evidence(&definition, "node-b");
        second.schema_manifest_id = None;
        metadata.record_replica_for_apply(first);
        metadata.record_replica_for_apply(second);
        let directory = RangeReplicaDirectory::from_committed(&metadata);

        assert_eq!(directory.routing_eligible(&definition.range_id).len(), 2);
        assert!(directory.can_retire_replica(&definition.range_id, &crate::NodeId::new("node-a")));
    }

    #[test]
    fn stale_generation_is_not_routing_eligible() {
        let (mut metadata, definition) = metadata();
        let mut evidence = ready_evidence(&definition, "node-a");
        evidence.schema_manifest_id = None;
        evidence.generation = 1;
        metadata.record_replica_for_apply(evidence);

        let directory = RangeReplicaDirectory::from_committed(&metadata);
        assert_eq!(
            directory.entries()[0].state,
            RangeReplicaReadinessState::EpochStale
        );
        assert!(directory.routing_eligible(&definition.range_id).is_empty());
    }
}
