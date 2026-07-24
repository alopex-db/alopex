//! Committed replica readiness and coverage proof evaluation.

use super::range_transfer::RangeTransferAck;
use crate::{
    CommittedMetadata, NodeId, RangeId, RangeReplicaEvidence, RangeReplicaLifecycle,
    RangeRoutingDefinition, SchemaManifestId,
};
use alopex_core::CanonicalRowKey;
use std::{collections::BTreeMap, error::Error, fmt};

/// Deterministic range-directory validation failures. These are returned
/// before a metadata proposal is created, so a rejected split/merge cannot
/// alter the committed view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeDirectoryError {
    EmptyRange { range_id: RangeId },
    InvalidBounds { range_id: RangeId },
    TableMismatch { range_id: RangeId },
    Gap { previous: RangeId, next: RangeId },
    Overlap { previous: RangeId, next: RangeId },
    ExpectedVersion { expected: u64, actual: u64 },
    RangeNotFound { range_id: RangeId },
    RangeAlreadyExists { range_id: RangeId },
    SplitKeyOutsideRange { range_id: RangeId },
    MergeNotAdjacent { left: RangeId, right: RangeId },
    MergeTableMismatch { left: RangeId, right: RangeId },
}

impl fmt::Display for RangeDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRange { range_id } => write!(formatter, "range {range_id:?} is empty"),
            Self::InvalidBounds { range_id } => {
                write!(formatter, "range {range_id:?} has invalid bounds")
            }
            Self::TableMismatch { range_id } => {
                write!(formatter, "range {range_id:?} has a table mismatch")
            }
            Self::Gap { previous, next } => {
                write!(formatter, "gap between {previous:?} and {next:?}")
            }
            Self::Overlap { previous, next } => {
                write!(formatter, "overlap between {previous:?} and {next:?}")
            }
            Self::ExpectedVersion { expected, actual } => {
                write!(
                    formatter,
                    "expected metadata version {expected}, actual {actual}"
                )
            }
            Self::RangeNotFound { range_id } => {
                write!(formatter, "range {range_id:?} is not committed")
            }
            Self::RangeAlreadyExists { range_id } => {
                write!(formatter, "range {range_id:?} already exists")
            }
            Self::SplitKeyOutsideRange { range_id } => {
                write!(formatter, "split key is outside {range_id:?}")
            }
            Self::MergeNotAdjacent { left, right } => {
                write!(formatter, "ranges {left:?} and {right:?} are not adjacent")
            }
            Self::MergeTableMismatch { left, right } => write!(
                formatter,
                "ranges {left:?} and {right:?} use different tables"
            ),
        }
    }
}

impl Error for RangeDirectoryError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeTransitionKind {
    Registered,
    Split,
    Merged,
}

/// Observable predecessor/successor relation for a committed range change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTransition {
    pub kind: RangeTransitionKind,
    pub predecessors: Vec<RangeId>,
    pub successors: Vec<RangeId>,
    pub generation: u64,
    pub metadata_version: u64,
}

/// In-memory validator/projection for one committed table range directory.
/// The directory never becomes authoritative by itself; callers apply the
/// returned transition through the metadata consensus boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDirectory {
    metadata_version: u64,
    ranges: BTreeMap<RangeId, RangeRoutingDefinition>,
}

impl RangeDirectory {
    pub fn empty(metadata_version: u64) -> Self {
        Self {
            metadata_version,
            ranges: BTreeMap::new(),
        }
    }

    pub fn from_committed(metadata: &CommittedMetadata) -> Result<Self, RangeDirectoryError> {
        Self::from_definitions(
            metadata.state_version(),
            metadata.ranges().values().cloned(),
        )
    }

    pub fn from_definitions(
        metadata_version: u64,
        definitions: impl IntoIterator<Item = RangeRoutingDefinition>,
    ) -> Result<Self, RangeDirectoryError> {
        let ranges = definitions
            .into_iter()
            .map(|definition| (definition.range_id.clone(), definition))
            .collect();
        let directory = Self {
            metadata_version,
            ranges,
        };
        directory.validate_current()?;
        Ok(directory)
    }

    pub fn metadata_version(&self) -> u64 {
        self.metadata_version
    }

    pub fn ranges(&self) -> &BTreeMap<RangeId, RangeRoutingDefinition> {
        &self.ranges
    }

    pub fn lookup(&self, row_key: &[u8]) -> Option<&RangeRoutingDefinition> {
        self.ranges
            .values()
            .find(|definition| definition.contains(row_key))
    }

    pub fn register(
        &mut self,
        definition: RangeRoutingDefinition,
        expected_version: u64,
    ) -> Result<RangeTransition, RangeDirectoryError> {
        self.check_version(expected_version)?;
        if self.ranges.contains_key(&definition.range_id) {
            return Err(RangeDirectoryError::RangeAlreadyExists {
                range_id: definition.range_id,
            });
        }
        let id = definition.range_id.clone();
        let generation = definition.generation;
        let mut candidate = self.ranges.clone();
        candidate.insert(id.clone(), definition);
        validate_definitions(&candidate)?;
        self.ranges = candidate;
        self.metadata_version = self.metadata_version.saturating_add(1);
        Ok(RangeTransition {
            kind: RangeTransitionKind::Registered,
            predecessors: Vec::new(),
            successors: vec![id],
            generation,
            metadata_version: self.metadata_version,
        })
    }

    pub fn split(
        &mut self,
        parent_id: &RangeId,
        split_key: Vec<u8>,
        left_id: RangeId,
        right_id: RangeId,
        expected_version: u64,
    ) -> Result<RangeTransition, RangeDirectoryError> {
        self.check_version(expected_version)?;
        let parent = self.ranges.get(parent_id).cloned().ok_or_else(|| {
            RangeDirectoryError::RangeNotFound {
                range_id: parent_id.clone(),
            }
        })?;
        if left_id == right_id
            || self.ranges.contains_key(&left_id)
            || self.ranges.contains_key(&right_id)
        {
            return Err(RangeDirectoryError::RangeAlreadyExists { range_id: left_id });
        }
        let split = CanonicalRowKey::decode(&split_key)
            .ok()
            .filter(|key| key.table_id() == parent.table_id);
        if split.is_none()
            || parent
                .lower_inclusive
                .as_ref()
                .is_some_and(|lower| split_key.as_slice() <= lower.as_slice())
            || parent
                .upper_exclusive
                .as_ref()
                .is_some_and(|upper| split_key.as_slice() >= upper.as_slice())
        {
            return Err(RangeDirectoryError::SplitKeyOutsideRange {
                range_id: parent_id.clone(),
            });
        }
        let generation = parent.generation.saturating_add(1);
        let mut left = parent.clone();
        left.range_id = left_id.clone();
        left.upper_exclusive = Some(split_key.clone());
        left.generation = generation;
        let mut right = parent;
        right.range_id = right_id.clone();
        right.lower_inclusive = Some(split_key);
        right.generation = generation;
        let mut candidate = self.ranges.clone();
        candidate.remove(parent_id);
        candidate.insert(left_id.clone(), left);
        candidate.insert(right_id.clone(), right);
        validate_definitions(&candidate)?;
        self.ranges = candidate;
        self.metadata_version = self.metadata_version.saturating_add(1);
        Ok(RangeTransition {
            kind: RangeTransitionKind::Split,
            predecessors: vec![parent_id.clone()],
            successors: vec![left_id, right_id],
            generation,
            metadata_version: self.metadata_version,
        })
    }

    pub fn merge(
        &mut self,
        left_id: &RangeId,
        right_id: &RangeId,
        merged_id: RangeId,
        expected_version: u64,
    ) -> Result<RangeTransition, RangeDirectoryError> {
        self.check_version(expected_version)?;
        let left = self.ranges.get(left_id).cloned().ok_or_else(|| {
            RangeDirectoryError::RangeNotFound {
                range_id: left_id.clone(),
            }
        })?;
        let right = self.ranges.get(right_id).cloned().ok_or_else(|| {
            RangeDirectoryError::RangeNotFound {
                range_id: right_id.clone(),
            }
        })?;
        if left.table_id != right.table_id || left.table_ref != right.table_ref {
            return Err(RangeDirectoryError::MergeTableMismatch {
                left: left_id.clone(),
                right: right_id.clone(),
            });
        }
        if left.upper_exclusive != right.lower_inclusive {
            return Err(RangeDirectoryError::MergeNotAdjacent {
                left: left_id.clone(),
                right: right_id.clone(),
            });
        }
        if self.ranges.contains_key(&merged_id) {
            return Err(RangeDirectoryError::RangeAlreadyExists {
                range_id: merged_id,
            });
        }
        let generation = left.generation.max(right.generation).saturating_add(1);
        let merged = RangeRoutingDefinition {
            range_id: merged_id.clone(),
            table_ref: left.table_ref,
            table_id: left.table_id,
            lower_inclusive: left.lower_inclusive,
            upper_exclusive: right.upper_exclusive,
            generation,
        };
        let mut candidate = self.ranges.clone();
        candidate.remove(left_id);
        candidate.remove(right_id);
        candidate.insert(merged_id.clone(), merged);
        validate_definitions(&candidate)?;
        self.ranges = candidate;
        self.metadata_version = self.metadata_version.saturating_add(1);
        Ok(RangeTransition {
            kind: RangeTransitionKind::Merged,
            predecessors: vec![left_id.clone(), right_id.clone()],
            successors: vec![merged_id],
            generation,
            metadata_version: self.metadata_version,
        })
    }

    fn check_version(&self, expected: u64) -> Result<(), RangeDirectoryError> {
        if expected != self.metadata_version {
            return Err(RangeDirectoryError::ExpectedVersion {
                expected,
                actual: self.metadata_version,
            });
        }
        Ok(())
    }

    fn validate_current(&self) -> Result<(), RangeDirectoryError> {
        validate_definitions(&self.ranges)
    }
}

fn validate_definitions(
    definitions: &BTreeMap<RangeId, RangeRoutingDefinition>,
) -> Result<(), RangeDirectoryError> {
    let mut ordered: Vec<&RangeRoutingDefinition> = definitions.values().collect();
    ordered.sort_by(|left, right| lower_cmp(&left.lower_inclusive, &right.lower_inclusive));
    for definition in &ordered {
        if !valid_bounds(definition) {
            return Err(RangeDirectoryError::InvalidBounds {
                range_id: definition.range_id.clone(),
            });
        }
    }
    for pair in ordered.windows(2) {
        let previous = pair[0];
        let next = pair[1];
        if previous.table_id != next.table_id || previous.table_ref != next.table_ref {
            return Err(RangeDirectoryError::TableMismatch {
                range_id: next.range_id.clone(),
            });
        }
        match (&previous.upper_exclusive, &next.lower_inclusive) {
            (Some(upper), Some(lower)) if upper == lower => {}
            (Some(upper), Some(lower)) if upper > lower => {
                return Err(RangeDirectoryError::Overlap {
                    previous: previous.range_id.clone(),
                    next: next.range_id.clone(),
                });
            }
            (Some(_), Some(_)) => {
                return Err(RangeDirectoryError::Gap {
                    previous: previous.range_id.clone(),
                    next: next.range_id.clone(),
                });
            }
            _ => {
                return Err(RangeDirectoryError::Overlap {
                    previous: previous.range_id.clone(),
                    next: next.range_id.clone(),
                });
            }
        }
    }
    if let Some(first) = ordered.first()
        && first.lower_inclusive.is_some()
    {
        return Err(RangeDirectoryError::Gap {
            previous: first.range_id.clone(),
            next: first.range_id.clone(),
        });
    }
    if let Some(last) = ordered.last()
        && last.upper_exclusive.is_some()
    {
        return Err(RangeDirectoryError::Gap {
            previous: last.range_id.clone(),
            next: last.range_id.clone(),
        });
    }
    Ok(())
}

fn lower_cmp(left: &Option<Vec<u8>>, right: &Option<Vec<u8>>) -> std::cmp::Ordering {
    match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => left.cmp(right),
    }
}

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

    #[test]
    fn split_and_merge_preserve_complete_half_open_coverage() {
        let mut directory = RangeDirectory::empty(4);
        let root = RangeRoutingDefinition {
            range_id: RangeId::new("root"),
            table_ref: crate::TableRef::new("default.public.users"),
            table_id: 7,
            lower_inclusive: None,
            upper_exclusive: None,
            generation: 1,
        };
        directory.register(root, 4).expect("register root");
        let split_key = CanonicalRowKey::new(7, 10).encode();
        let transition = directory
            .split(
                &RangeId::new("root"),
                split_key.clone(),
                RangeId::new("left"),
                RangeId::new("right"),
                5,
            )
            .expect("split root");
        assert_eq!(transition.predecessors, vec![RangeId::new("root")]);
        assert_eq!(
            directory
                .lookup(&CanonicalRowKey::new(7, 1).encode())
                .unwrap()
                .range_id,
            RangeId::new("left")
        );
        assert_eq!(
            directory
                .lookup(&CanonicalRowKey::new(7, 20).encode())
                .unwrap()
                .range_id,
            RangeId::new("right")
        );

        let merged = directory
            .merge(
                &RangeId::new("left"),
                &RangeId::new("right"),
                RangeId::new("merged"),
                6,
            )
            .expect("merge adjacent ranges");
        assert_eq!(merged.successors, vec![RangeId::new("merged")]);
        assert!(
            directory
                .lookup(&CanonicalRowKey::new(7, 20).encode())
                .is_some()
        );
    }

    #[test]
    fn rejects_gap_overlap_and_stale_expected_version() {
        let first = RangeRoutingDefinition {
            range_id: RangeId::new("first"),
            table_ref: crate::TableRef::new("default.public.users"),
            table_id: 7,
            lower_inclusive: None,
            upper_exclusive: Some(CanonicalRowKey::new(7, 10).encode()),
            generation: 1,
        };
        let second = RangeRoutingDefinition {
            range_id: RangeId::new("second"),
            table_ref: crate::TableRef::new("default.public.users"),
            table_id: 7,
            lower_inclusive: Some(CanonicalRowKey::new(7, 20).encode()),
            upper_exclusive: None,
            generation: 1,
        };
        assert!(matches!(
            RangeDirectory::from_definitions(0, [first.clone(), second]),
            Err(RangeDirectoryError::Gap { .. })
        ));
        let overlap = RangeRoutingDefinition {
            range_id: RangeId::new("overlap"),
            table_ref: first.table_ref.clone(),
            table_id: first.table_id,
            lower_inclusive: Some(CanonicalRowKey::new(7, 5).encode()),
            upper_exclusive: None,
            generation: 1,
        };
        assert!(matches!(
            RangeDirectory::from_definitions(0, [first.clone(), overlap]),
            Err(RangeDirectoryError::Overlap { .. })
        ));
        let mut directory = RangeDirectory::empty(0);
        let root = RangeRoutingDefinition {
            range_id: RangeId::new("root"),
            table_ref: first.table_ref,
            table_id: first.table_id,
            lower_inclusive: None,
            upper_exclusive: None,
            generation: 1,
        };
        directory.register(root, 0).expect("register root");
        assert!(matches!(
            directory.register(
                RangeRoutingDefinition {
                    range_id: RangeId::new("stale"),
                    table_ref: crate::TableRef::new("default.public.users"),
                    table_id: 7,
                    lower_inclusive: None,
                    upper_exclusive: None,
                    generation: 1,
                },
                0,
            ),
            Err(RangeDirectoryError::ExpectedVersion { .. })
        ));
    }
}
