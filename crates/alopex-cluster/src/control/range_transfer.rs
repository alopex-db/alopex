//! Verified snapshot-plus-journal range transfer and recovery apply.

use crate::{
    ClusterFrameHandler, ClusterFrameHandlerError, ClusterFrameKind, ClusterId, NodeId, RangeId,
    RangeRoutingDefinition, RequestId, SchemaManifestId, VerifiedClusterFrame,
    VerifiedPeerIdentity,
};
use alopex_core::kv::{
    KVStore, KVTransaction, RangeChangePayload, RangeChangeRecord, stage_range_change,
};
use alopex_core::{CanonicalRowKey, RowKeyRange};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

const TRANSFER_PROGRESS_PREFIX: &[u8] = b"\x00alopex/range-transfer/progress/";

/// Immutable source declaration for one range transfer cut and suffix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeTransferManifest {
    pub transfer_id: String,
    pub range_id: RangeId,
    pub generation: u64,
    pub schema_manifest_id: Option<SchemaManifestId>,
    pub source_node_id: NodeId,
    pub base_epoch: u64,
    pub final_epoch: u64,
    /// Chunk ordinal to canonical SHA-256 content hash.
    pub chunk_hashes: BTreeMap<u64, String>,
    /// B-tree secondary-index ID to owning primary table ID.
    pub index_tables: BTreeMap<u32, u32>,
}

/// One immutable snapshot item. Snapshot import never accepts deletes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeSnapshotEntry {
    Row {
        row_key: Vec<u8>,
        encoded_row: Vec<u8>,
    },
    Index {
        index_id: u32,
        index_key: Vec<u8>,
        row_key: Vec<u8>,
    },
}

/// Hash-addressed chunk from a snapshot cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeSnapshotChunk {
    pub transfer_id: String,
    pub ordinal: u64,
    pub entries: Vec<RangeSnapshotEntry>,
    pub content_hash: String,
}

impl RangeSnapshotChunk {
    /// Computes the canonical content hash for entries before transmission.
    pub fn content_hash(entries: &[RangeSnapshotEntry]) -> Result<String, RangeTransferError> {
        hash_value(entries)
    }
}

/// Hash-addressed contiguous journal item sent after the snapshot cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeChangeEnvelope {
    pub transfer_id: String,
    pub record: RangeChangeRecord,
    pub content_hash: String,
}

impl RangeChangeEnvelope {
    /// Computes the canonical content hash for a range-change record.
    pub fn content_hash(record: &RangeChangeRecord) -> Result<String, RangeTransferError> {
        hash_value(record)
    }
}

/// An application frame reserved for range transfer; it is never a Raft RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeTransferWireFrame {
    pub target_node_id: NodeId,
    pub message: RangeTransferWireMessage,
}

/// Range-transfer protocol message carried by an authenticated application frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "body", rename_all = "snake_case")]
pub enum RangeTransferWireMessage {
    Manifest(RangeTransferManifest),
    SnapshotChunk(RangeSnapshotChunk),
    Change(RangeChangeEnvelope),
}

/// Committed target-side assumptions that a source must match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTransferExpectation {
    pub cluster_id: ClusterId,
    pub local_node_id: NodeId,
    pub definition: RangeRoutingDefinition,
    pub schema_manifest_id: Option<SchemaManifestId>,
}

/// Receiver used by the authenticated transport adapter.
pub trait VerifiedRangeTransferReceiver: Send {
    fn receive_verified(
        &mut self,
        peer: &VerifiedPeerIdentity,
        frame: RangeTransferWireFrame,
    ) -> Result<(), RangeTransferError>;
}

/// Dispatcher handler that decodes only verified `RangeTransfer` frames.
pub struct RangeTransferFrameHandler {
    local_node_id: NodeId,
    receiver: Box<dyn VerifiedRangeTransferReceiver>,
}

impl RangeTransferFrameHandler {
    pub fn new(local_node_id: NodeId, receiver: Box<dyn VerifiedRangeTransferReceiver>) -> Self {
        Self {
            local_node_id,
            receiver,
        }
    }
}

impl ClusterFrameHandler for RangeTransferFrameHandler {
    fn handle(&mut self, frame: VerifiedClusterFrame) -> Result<(), ClusterFrameHandlerError> {
        if frame.kind() != ClusterFrameKind::RangeTransfer {
            return Err(ClusterFrameHandlerError::new(
                "range-transfer handler received a non-transfer frame",
            ));
        }
        let wire: RangeTransferWireFrame =
            serde_json::from_slice(frame.payload()).map_err(|error| {
                ClusterFrameHandlerError::new(format!("invalid range-transfer frame: {error}"))
            })?;
        if wire.target_node_id != self.local_node_id {
            return Err(ClusterFrameHandlerError::new(
                "range-transfer frame is addressed to another node",
            ));
        }
        self.receiver
            .receive_verified(frame.peer(), wire)
            .map_err(|error| ClusterFrameHandlerError::new(error.to_string()))
    }
}

/// Durable, post-commit acknowledgement for a fully applied transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeTransferAck {
    transfer_id: String,
    range_id: RangeId,
    generation: u64,
    schema_manifest_id: Option<SchemaManifestId>,
    final_epoch: u64,
    content_hash: String,
}

impl RangeTransferAck {
    pub fn transfer_id(&self) -> &str {
        &self.transfer_id
    }

    pub fn range_id(&self) -> &RangeId {
        &self.range_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn schema_manifest_id(&self) -> Option<&SchemaManifestId> {
        self.schema_manifest_id.as_ref()
    }

    pub fn final_epoch(&self) -> u64 {
        self.final_epoch
    }

    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }
}

/// Reconnect position derived only from a durable target-side acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTransferResumePoint {
    pub next_chunk_ordinal: u64,
    pub next_epoch: u64,
}

/// Successful target apply is distinguishable from an idempotent reconnect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeTransferApplyOutcome {
    Applied(RangeTransferAck),
    AlreadyAcknowledged(RangeTransferAck),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeTransferPhase {
    Prepared,
    Copying,
    Verified,
    Published,
    Aborted,
}

/// Durable operation checkpoint for a move. The source remains the serving
/// owner until `publish` succeeds; reconnecting with the same request returns
/// this exact record instead of applying another move.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeTransferCheckpoint {
    pub request_id: RequestId,
    pub transfer_id: String,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub phase: RangeTransferPhase,
    pub copied_chunks: u64,
    pub verified_epoch: Option<u64>,
    pub serving_owner: NodeId,
}

#[derive(Debug, Default)]
pub struct RangeTransferCoordinator {
    checkpoints: BTreeMap<RequestId, RangeTransferCheckpoint>,
}

impl RangeTransferCoordinator {
    pub fn prepare(
        &mut self,
        request_id: impl Into<RequestId>,
        transfer_id: impl Into<String>,
        source_node_id: impl Into<NodeId>,
        target_node_id: impl Into<NodeId>,
    ) -> Result<RangeTransferCheckpoint, RangeTransferError> {
        let request_id = request_id.into();
        let transfer_id = transfer_id.into();
        let source_node_id = source_node_id.into();
        let target_node_id = target_node_id.into();
        if let Some(existing) = self.checkpoints.get(&request_id) {
            if existing.transfer_id == transfer_id
                && existing.source_node_id == source_node_id
                && existing.target_node_id == target_node_id
            {
                return Ok(existing.clone());
            }
            return Err(RangeTransferError::ProgressConflict);
        }
        let checkpoint = RangeTransferCheckpoint {
            request_id: request_id.clone(),
            transfer_id,
            source_node_id: source_node_id.clone(),
            target_node_id,
            phase: RangeTransferPhase::Prepared,
            copied_chunks: 0,
            verified_epoch: None,
            serving_owner: source_node_id,
        };
        self.checkpoints.insert(request_id, checkpoint.clone());
        Ok(checkpoint)
    }

    pub fn copy_chunk(
        &mut self,
        request_id: &RequestId,
    ) -> Result<RangeTransferCheckpoint, RangeTransferError> {
        let checkpoint = self
            .checkpoints
            .get_mut(request_id)
            .ok_or(RangeTransferError::ManifestMissing)?;
        match checkpoint.phase {
            RangeTransferPhase::Prepared | RangeTransferPhase::Copying => {
                checkpoint.phase = RangeTransferPhase::Copying;
                checkpoint.copied_chunks = checkpoint.copied_chunks.saturating_add(1);
            }
            RangeTransferPhase::Verified | RangeTransferPhase::Published => {}
            RangeTransferPhase::Aborted => return Err(RangeTransferError::ProgressConflict),
        }
        Ok(checkpoint.clone())
    }

    pub fn verify(
        &mut self,
        request_id: &RequestId,
        final_epoch: u64,
    ) -> Result<RangeTransferCheckpoint, RangeTransferError> {
        let checkpoint = self
            .checkpoints
            .get_mut(request_id)
            .ok_or(RangeTransferError::ManifestMissing)?;
        match checkpoint.phase {
            RangeTransferPhase::Prepared | RangeTransferPhase::Copying => {
                checkpoint.phase = RangeTransferPhase::Verified;
                checkpoint.verified_epoch = Some(final_epoch);
            }
            RangeTransferPhase::Verified | RangeTransferPhase::Published => {}
            RangeTransferPhase::Aborted => return Err(RangeTransferError::ProgressConflict),
        }
        Ok(checkpoint.clone())
    }

    pub fn publish(
        &mut self,
        request_id: &RequestId,
    ) -> Result<RangeTransferCheckpoint, RangeTransferError> {
        let checkpoint = self
            .checkpoints
            .get_mut(request_id)
            .ok_or(RangeTransferError::ManifestMissing)?;
        match checkpoint.phase {
            RangeTransferPhase::Verified | RangeTransferPhase::Published => {
                checkpoint.phase = RangeTransferPhase::Published;
                checkpoint.serving_owner = checkpoint.target_node_id.clone();
            }
            RangeTransferPhase::Prepared | RangeTransferPhase::Copying => {
                return Err(RangeTransferError::ManifestMissing);
            }
            RangeTransferPhase::Aborted => return Err(RangeTransferError::ProgressConflict),
        }
        Ok(checkpoint.clone())
    }

    pub fn abort(
        &mut self,
        request_id: &RequestId,
    ) -> Result<RangeTransferCheckpoint, RangeTransferError> {
        let checkpoint = self
            .checkpoints
            .get_mut(request_id)
            .ok_or(RangeTransferError::ManifestMissing)?;
        match checkpoint.phase {
            RangeTransferPhase::Published => return Ok(checkpoint.clone()),
            RangeTransferPhase::Aborted => {}
            RangeTransferPhase::Prepared
            | RangeTransferPhase::Copying
            | RangeTransferPhase::Verified => checkpoint.phase = RangeTransferPhase::Aborted,
        }
        Ok(checkpoint.clone())
    }

    pub fn checkpoint(&self, request_id: &RequestId) -> Option<&RangeTransferCheckpoint> {
        self.checkpoints.get(request_id)
    }
}

/// Classified transfer rejection. No variant reports a partially ready range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeTransferError {
    PeerClusterMismatch,
    PeerNodeMismatch,
    TargetNodeMismatch,
    ManifestMissing,
    TransferIdMismatch,
    RangeMismatch,
    GenerationMismatch,
    SchemaMismatch,
    ChunkHashMismatch { ordinal: u64 },
    ChunkMissing { ordinal: u64 },
    RecordHashMismatch { epoch: u64 },
    NonContiguousEpoch { expected: u64, actual: u64 },
    InvalidRangeData(String),
    ProgressConflict,
    Storage(String),
    Encoding(String),
}

impl fmt::Display for RangeTransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerClusterMismatch => {
                formatter.write_str("verified peer belongs to another cluster")
            }
            Self::PeerNodeMismatch => {
                formatter.write_str("verified peer does not match manifest source node")
            }
            Self::TargetNodeMismatch => formatter.write_str("range transfer targets another node"),
            Self::ManifestMissing => {
                formatter.write_str("range transfer manifest has not been accepted")
            }
            Self::TransferIdMismatch => {
                formatter.write_str("range transfer message belongs to another transfer")
            }
            Self::RangeMismatch => {
                formatter.write_str("range transfer range differs from committed metadata")
            }
            Self::GenerationMismatch => {
                formatter.write_str("range transfer generation differs from committed metadata")
            }
            Self::SchemaMismatch => formatter
                .write_str("range transfer schema manifest differs from committed metadata"),
            Self::ChunkHashMismatch { ordinal } => {
                write!(formatter, "range snapshot chunk hash mismatch: {ordinal}")
            }
            Self::ChunkMissing { ordinal } => {
                write!(formatter, "range snapshot chunk is missing: {ordinal}")
            }
            Self::RecordHashMismatch { epoch } => {
                write!(formatter, "range journal record hash mismatch: {epoch}")
            }
            Self::NonContiguousEpoch { expected, actual } => write!(
                formatter,
                "range journal epoch is not contiguous: expected {expected}, got {actual}"
            ),
            Self::InvalidRangeData(message) => write!(formatter, "invalid range data: {message}"),
            Self::ProgressConflict => {
                formatter.write_str("durable progress belongs to a different transfer")
            }
            Self::Storage(message) => {
                write!(formatter, "range transfer storage failure: {message}")
            }
            Self::Encoding(message) => {
                write!(formatter, "range transfer encoding failure: {message}")
            }
        }
    }
}

impl Error for RangeTransferError {}

/// In-memory assembly state; only a complete verified assembly may be applied.
pub struct RangeTransferSession {
    expectation: RangeTransferExpectation,
    manifest: Option<RangeTransferManifest>,
    chunks: BTreeMap<u64, RangeSnapshotChunk>,
    changes: BTreeMap<u64, RangeChangeEnvelope>,
}

impl RangeTransferSession {
    pub fn new(expectation: RangeTransferExpectation) -> Self {
        Self {
            expectation,
            manifest: None,
            chunks: BTreeMap::new(),
            changes: BTreeMap::new(),
        }
    }

    /// Accepts a message only after transport authentication has bound its peer.
    pub fn receive_verified(
        &mut self,
        peer: &VerifiedPeerIdentity,
        frame: RangeTransferWireFrame,
    ) -> Result<(), RangeTransferError> {
        if peer.cluster_id() != &self.expectation.cluster_id {
            return Err(RangeTransferError::PeerClusterMismatch);
        }
        if frame.target_node_id != self.expectation.local_node_id {
            return Err(RangeTransferError::TargetNodeMismatch);
        }
        match frame.message {
            RangeTransferWireMessage::Manifest(manifest) => self.accept_manifest(peer, manifest),
            RangeTransferWireMessage::SnapshotChunk(chunk) => self.accept_chunk(peer, chunk),
            RangeTransferWireMessage::Change(change) => self.accept_change(peer, change),
        }
    }

    /// Applies the complete verified snapshot plus contiguous suffix atomically,
    /// then returns an acknowledgement only after durable commit succeeds.
    pub fn apply<S: KVStore>(
        &self,
        store: &S,
    ) -> Result<RangeTransferApplyOutcome, RangeTransferError> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or(RangeTransferError::ManifestMissing)?;
        if let Some(ack) = load_durable_ack(store, manifest)? {
            return if ack.transfer_id == manifest.transfer_id
                && ack.content_hash == transfer_hash(self, manifest)?
            {
                Ok(RangeTransferApplyOutcome::AlreadyAcknowledged(ack))
            } else {
                Err(RangeTransferError::ProgressConflict)
            };
        }
        self.validate_complete(manifest)?;

        let mut transaction = store
            .begin(alopex_core::TxnMode::ReadWrite)
            .map_err(storage_error)?;
        clear_range(
            &mut transaction,
            &self.expectation.definition,
            &manifest.index_tables,
        )?;
        for chunk in self.chunks.values() {
            for entry in &chunk.entries {
                apply_snapshot_entry(
                    &mut transaction,
                    &self.expectation.definition,
                    &manifest.index_tables,
                    entry,
                )?;
            }
        }
        for change in self.changes.values() {
            apply_change_record(
                &mut transaction,
                &self.expectation.definition,
                &manifest.index_tables,
                &change.record,
            )?;
            stage_range_change(&mut transaction, &change.record).map_err(storage_error)?;
        }
        let ack = RangeTransferAck {
            transfer_id: manifest.transfer_id.clone(),
            range_id: manifest.range_id.clone(),
            generation: manifest.generation,
            schema_manifest_id: manifest.schema_manifest_id.clone(),
            final_epoch: manifest.final_epoch,
            content_hash: transfer_hash(self, manifest)?,
        };
        let progress = serde_json::to_vec(&ack)
            .map_err(|error| RangeTransferError::Encoding(error.to_string()))?;
        transaction
            .put(progress_key(manifest), progress)
            .map_err(storage_error)?;
        transaction.commit_self().map_err(storage_error)?;
        Ok(RangeTransferApplyOutcome::Applied(ack))
    }

    /// Returns a reconnect position only when a matching durable ack exists.
    pub fn resume_point<S: KVStore>(
        &self,
        store: &S,
    ) -> Result<Option<RangeTransferResumePoint>, RangeTransferError> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or(RangeTransferError::ManifestMissing)?;
        let Some(ack) = load_durable_ack(store, manifest)? else {
            return Ok(None);
        };
        if ack.transfer_id != manifest.transfer_id
            || ack.content_hash != transfer_hash(self, manifest)?
        {
            return Err(RangeTransferError::ProgressConflict);
        }
        Ok(Some(RangeTransferResumePoint {
            next_chunk_ordinal: manifest.chunk_hashes.len() as u64,
            next_epoch: ack.final_epoch.saturating_add(1),
        }))
    }

    fn accept_manifest(
        &mut self,
        peer: &VerifiedPeerIdentity,
        manifest: RangeTransferManifest,
    ) -> Result<(), RangeTransferError> {
        self.validate_manifest(peer, &manifest)?;
        if let Some(existing) = &self.manifest {
            if existing != &manifest {
                return Err(RangeTransferError::TransferIdMismatch);
            }
            return Ok(());
        }
        self.manifest = Some(manifest);
        Ok(())
    }

    fn accept_chunk(
        &mut self,
        peer: &VerifiedPeerIdentity,
        chunk: RangeSnapshotChunk,
    ) -> Result<(), RangeTransferError> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or(RangeTransferError::ManifestMissing)?;
        self.verify_source(peer, manifest)?;
        if chunk.transfer_id != manifest.transfer_id {
            return Err(RangeTransferError::TransferIdMismatch);
        }
        let expected =
            manifest
                .chunk_hashes
                .get(&chunk.ordinal)
                .ok_or(RangeTransferError::ChunkMissing {
                    ordinal: chunk.ordinal,
                })?;
        if &RangeSnapshotChunk::content_hash(&chunk.entries)? != expected
            || &chunk.content_hash != expected
        {
            return Err(RangeTransferError::ChunkHashMismatch {
                ordinal: chunk.ordinal,
            });
        }
        for entry in &chunk.entries {
            validate_snapshot_entry(&self.expectation.definition, &manifest.index_tables, entry)?;
        }
        if let Some(existing) = self.chunks.get(&chunk.ordinal) {
            if existing != &chunk {
                return Err(RangeTransferError::ChunkHashMismatch {
                    ordinal: chunk.ordinal,
                });
            }
        } else {
            self.chunks.insert(chunk.ordinal, chunk);
        }
        Ok(())
    }

    fn accept_change(
        &mut self,
        peer: &VerifiedPeerIdentity,
        change: RangeChangeEnvelope,
    ) -> Result<(), RangeTransferError> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or(RangeTransferError::ManifestMissing)?;
        self.verify_source(peer, manifest)?;
        if change.transfer_id != manifest.transfer_id {
            return Err(RangeTransferError::TransferIdMismatch);
        }
        if change.record.range_id != manifest.range_id.as_str()
            || change.record.generation != manifest.generation
        {
            return Err(RangeTransferError::RangeMismatch);
        }
        if RangeChangeEnvelope::content_hash(&change.record)? != change.content_hash {
            return Err(RangeTransferError::RecordHashMismatch {
                epoch: change.record.epoch,
            });
        }
        validate_change_record(
            &self.expectation.definition,
            &manifest.index_tables,
            &change.record,
        )?;
        if let Some(existing) = self.changes.get(&change.record.epoch) {
            if existing != &change {
                return Err(RangeTransferError::RecordHashMismatch {
                    epoch: change.record.epoch,
                });
            }
        } else {
            self.changes.insert(change.record.epoch, change);
        }
        Ok(())
    }

    fn validate_manifest(
        &self,
        peer: &VerifiedPeerIdentity,
        manifest: &RangeTransferManifest,
    ) -> Result<(), RangeTransferError> {
        if manifest.range_id != self.expectation.definition.range_id {
            return Err(RangeTransferError::RangeMismatch);
        }
        if manifest.generation != self.expectation.definition.generation {
            return Err(RangeTransferError::GenerationMismatch);
        }
        if manifest.schema_manifest_id != self.expectation.schema_manifest_id {
            return Err(RangeTransferError::SchemaMismatch);
        }
        if manifest.final_epoch < manifest.base_epoch {
            return Err(RangeTransferError::InvalidRangeData(
                "final epoch precedes snapshot cut".to_string(),
            ));
        }
        for (expected, actual) in manifest.chunk_hashes.keys().enumerate() {
            if *actual != expected as u64 {
                return Err(RangeTransferError::InvalidRangeData(
                    "snapshot chunk ordinals must start at zero and be contiguous".to_string(),
                ));
            }
        }
        self.verify_source(peer, manifest)
    }

    fn verify_source(
        &self,
        peer: &VerifiedPeerIdentity,
        manifest: &RangeTransferManifest,
    ) -> Result<(), RangeTransferError> {
        if peer.node_id() != &manifest.source_node_id {
            return Err(RangeTransferError::PeerNodeMismatch);
        }
        Ok(())
    }

    fn validate_complete(
        &self,
        manifest: &RangeTransferManifest,
    ) -> Result<(), RangeTransferError> {
        for ordinal in manifest.chunk_hashes.keys() {
            if !self.chunks.contains_key(ordinal) {
                return Err(RangeTransferError::ChunkMissing { ordinal: *ordinal });
            }
        }
        let mut expected = manifest.base_epoch;
        for change in self.changes.values() {
            let next = expected.saturating_add(1);
            if change.record.epoch != next
                || change.record.predecessor_epoch
                    != if expected == 0 { None } else { Some(expected) }
            {
                return Err(RangeTransferError::NonContiguousEpoch {
                    expected: next,
                    actual: change.record.epoch,
                });
            }
            expected = change.record.epoch;
        }
        if expected != manifest.final_epoch {
            return Err(RangeTransferError::NonContiguousEpoch {
                expected: manifest.final_epoch,
                actual: expected,
            });
        }
        Ok(())
    }
}

impl VerifiedRangeTransferReceiver for RangeTransferSession {
    fn receive_verified(
        &mut self,
        peer: &VerifiedPeerIdentity,
        frame: RangeTransferWireFrame,
    ) -> Result<(), RangeTransferError> {
        self.receive_verified(peer, frame)
    }
}

fn load_durable_ack<S: KVStore>(
    store: &S,
    manifest: &RangeTransferManifest,
) -> Result<Option<RangeTransferAck>, RangeTransferError> {
    let mut transaction = store
        .begin(alopex_core::TxnMode::ReadOnly)
        .map_err(storage_error)?;
    let value = transaction
        .get(&progress_key(manifest))
        .map_err(storage_error)?;
    transaction.commit_self().map_err(storage_error)?;
    value
        .map(|raw| {
            serde_json::from_slice(&raw)
                .map_err(|error| RangeTransferError::Encoding(error.to_string()))
        })
        .transpose()
}

fn progress_key(manifest: &RangeTransferManifest) -> Vec<u8> {
    let mut key = TRANSFER_PROGRESS_PREFIX.to_vec();
    key.extend_from_slice(manifest.range_id.as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(&manifest.generation.to_be_bytes());
    key
}

fn transfer_hash(
    session: &RangeTransferSession,
    manifest: &RangeTransferManifest,
) -> Result<String, RangeTransferError> {
    hash_value(&(manifest, &session.chunks, &session.changes))
}

fn hash_value<T: Serialize + ?Sized>(value: &T) -> Result<String, RangeTransferError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| RangeTransferError::Encoding(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn storage_error(error: alopex_core::Error) -> RangeTransferError {
    RangeTransferError::Storage(error.to_string())
}

fn range_contains(
    definition: &RangeRoutingDefinition,
    row_key: &[u8],
) -> Result<(), RangeTransferError> {
    let row = CanonicalRowKey::decode(row_key)
        .map_err(|error| RangeTransferError::InvalidRangeData(error.to_string()))?;
    if row.table_id() != definition.table_id || !definition.contains(row_key) {
        return Err(RangeTransferError::InvalidRangeData(
            "row key is outside the committed range".to_string(),
        ));
    }
    Ok(())
}

fn validate_index_reference(
    definition: &RangeRoutingDefinition,
    index_tables: &BTreeMap<u32, u32>,
    index_id: u32,
    index_key: &[u8],
    row_key: &[u8],
) -> Result<(), RangeTransferError> {
    range_contains(definition, row_key)?;
    if index_tables.get(&index_id) != Some(&definition.table_id)
        || index_key.len() < 13
        || index_key[0] != 0x02
        || u32::from_be_bytes(index_key[1..5].try_into().expect("four bytes")) != index_id
    {
        return Err(RangeTransferError::InvalidRangeData(
            "secondary-index reference is not bound to the committed table".to_string(),
        ));
    }
    let row_id = u64::from_be_bytes(
        index_key[index_key.len() - 8..]
            .try_into()
            .expect("eight bytes"),
    );
    if CanonicalRowKey::new(definition.table_id, row_id).encode() != row_key {
        return Err(RangeTransferError::InvalidRangeData(
            "secondary-index key does not reference its declared row key".to_string(),
        ));
    }
    Ok(())
}

fn validate_snapshot_entry(
    definition: &RangeRoutingDefinition,
    index_tables: &BTreeMap<u32, u32>,
    entry: &RangeSnapshotEntry,
) -> Result<(), RangeTransferError> {
    match entry {
        RangeSnapshotEntry::Row { row_key, .. } => range_contains(definition, row_key),
        RangeSnapshotEntry::Index {
            index_id,
            index_key,
            row_key,
        } => validate_index_reference(definition, index_tables, *index_id, index_key, row_key),
    }
}

fn validate_change_record(
    definition: &RangeRoutingDefinition,
    index_tables: &BTreeMap<u32, u32>,
    record: &RangeChangeRecord,
) -> Result<(), RangeTransferError> {
    for payload in &record.payload {
        match payload {
            RangeChangePayload::UpsertRow { row_key, .. }
            | RangeChangePayload::DeleteRow { row_key, .. } => range_contains(definition, row_key)?,
            RangeChangePayload::UpsertIndex {
                index_id,
                index_key,
                row_key,
            }
            | RangeChangePayload::DeleteIndex {
                index_id,
                index_key,
                row_key,
            } => {
                validate_index_reference(definition, index_tables, *index_id, index_key, row_key)?;
            }
        }
    }
    Ok(())
}

fn clear_range<'txn, T: KVTransaction<'txn>>(
    transaction: &mut T,
    definition: &RangeRoutingDefinition,
    index_tables: &BTreeMap<u32, u32>,
) -> Result<(), RangeTransferError> {
    let lower = definition.lower_inclusive.clone().unwrap_or_else(|| {
        RowKeyRange::full_table(definition.table_id)
            .encoded_bounds()
            .lower_inclusive
    });
    let upper = definition.upper_exclusive.clone().unwrap_or_else(|| {
        RowKeyRange::full_table(definition.table_id)
            .encoded_bounds()
            .upper_exclusive
    });
    let row_keys = transaction
        .scan_range(&lower, &upper)
        .map_err(storage_error)?
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    for key in row_keys {
        transaction.delete(key).map_err(storage_error)?;
    }
    for (index_id, table_id) in index_tables {
        if *table_id != definition.table_id {
            continue;
        }
        let mut prefix = vec![0x02];
        prefix.extend_from_slice(&index_id.to_be_bytes());
        let index_keys = transaction
            .scan_prefix(&prefix)
            .map_err(storage_error)?
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        for key in index_keys {
            if key.len() < 13 {
                return Err(RangeTransferError::InvalidRangeData(
                    "stored secondary-index key is malformed".to_string(),
                ));
            }
            let row_id = u64::from_be_bytes(key[key.len() - 8..].try_into().expect("eight bytes"));
            if range_contains(
                definition,
                &CanonicalRowKey::new(*table_id, row_id).encode(),
            )
            .is_ok()
            {
                transaction.delete(key).map_err(storage_error)?;
            }
        }
    }
    Ok(())
}

fn apply_snapshot_entry<'txn, T: KVTransaction<'txn>>(
    transaction: &mut T,
    definition: &RangeRoutingDefinition,
    index_tables: &BTreeMap<u32, u32>,
    entry: &RangeSnapshotEntry,
) -> Result<(), RangeTransferError> {
    validate_snapshot_entry(definition, index_tables, entry)?;
    match entry {
        RangeSnapshotEntry::Row {
            row_key,
            encoded_row,
        } => transaction
            .put(row_key.clone(), encoded_row.clone())
            .map_err(storage_error),
        RangeSnapshotEntry::Index { index_key, .. } => transaction
            .put(index_key.clone(), Vec::new())
            .map_err(storage_error),
    }
}

fn apply_change_record<'txn, T: KVTransaction<'txn>>(
    transaction: &mut T,
    definition: &RangeRoutingDefinition,
    index_tables: &BTreeMap<u32, u32>,
    record: &RangeChangeRecord,
) -> Result<(), RangeTransferError> {
    validate_change_record(definition, index_tables, record)?;
    for payload in &record.payload {
        match payload {
            RangeChangePayload::UpsertRow {
                row_key,
                encoded_row,
            } => transaction
                .put(row_key.clone(), encoded_row.clone())
                .map_err(storage_error)?,
            RangeChangePayload::DeleteRow { row_key, .. } => {
                transaction.delete(row_key.clone()).map_err(storage_error)?
            }
            RangeChangePayload::UpsertIndex { index_key, .. } => transaction
                .put(index_key.clone(), Vec::new())
                .map_err(storage_error)?,
            RangeChangePayload::DeleteIndex { index_key, .. } => transaction
                .delete(index_key.clone())
                .map_err(storage_error)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::{Arc, Mutex};

    fn expectation() -> RangeTransferExpectation {
        RangeTransferExpectation {
            cluster_id: ClusterId::new("cluster-a"),
            local_node_id: NodeId::new("target"),
            definition: RangeRoutingDefinition {
                range_id: RangeId::new("range-a"),
                table_ref: crate::TableRef::new("default.public.users"),
                table_id: 7,
                lower_inclusive: Some(CanonicalRowKey::new(7, 10).encode()),
                upper_exclusive: Some(CanonicalRowKey::new(7, 20).encode()),
                generation: 2,
            },
            schema_manifest_id: Some(SchemaManifestId::new("schema-1")),
        }
    }

    fn manifest() -> RangeTransferManifest {
        let entries = vec![
            RangeSnapshotEntry::Row {
                row_key: CanonicalRowKey::new(7, 10).encode(),
                encoded_row: b"snapshot".to_vec(),
            },
            RangeSnapshotEntry::Index {
                index_id: 4,
                index_key: vec![0x02, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 10],
                row_key: CanonicalRowKey::new(7, 10).encode(),
            },
        ];
        let mut chunk_hashes = BTreeMap::new();
        chunk_hashes.insert(0, RangeSnapshotChunk::content_hash(&entries).unwrap());
        RangeTransferManifest {
            transfer_id: "transfer-a".to_string(),
            range_id: RangeId::new("range-a"),
            generation: 2,
            schema_manifest_id: Some(SchemaManifestId::new("schema-1")),
            source_node_id: NodeId::new("source"),
            base_epoch: 4,
            final_epoch: 5,
            chunk_hashes,
            index_tables: [(4, 7)].into(),
        }
    }

    fn peer() -> VerifiedPeerIdentity {
        VerifiedPeerIdentity::new("source", "cluster-a")
    }

    fn receive_complete(session: &mut RangeTransferSession) {
        let manifest = manifest();
        session
            .receive_verified(
                &peer(),
                RangeTransferWireFrame {
                    target_node_id: NodeId::new("target"),
                    message: RangeTransferWireMessage::Manifest(manifest.clone()),
                },
            )
            .unwrap();
        let entries = vec![
            RangeSnapshotEntry::Row {
                row_key: CanonicalRowKey::new(7, 10).encode(),
                encoded_row: b"snapshot".to_vec(),
            },
            RangeSnapshotEntry::Index {
                index_id: 4,
                index_key: vec![0x02, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 10],
                row_key: CanonicalRowKey::new(7, 10).encode(),
            },
        ];
        session
            .receive_verified(
                &peer(),
                RangeTransferWireFrame {
                    target_node_id: NodeId::new("target"),
                    message: RangeTransferWireMessage::SnapshotChunk(RangeSnapshotChunk {
                        transfer_id: manifest.transfer_id.clone(),
                        ordinal: 0,
                        content_hash: RangeSnapshotChunk::content_hash(&entries).unwrap(),
                        entries,
                    }),
                },
            )
            .unwrap();
        let record = RangeChangeRecord {
            range_id: "range-a".to_string(),
            generation: 2,
            epoch: 5,
            predecessor_epoch: Some(4),
            replay_id: "replay-5".to_string(),
            payload: vec![RangeChangePayload::UpsertRow {
                row_key: CanonicalRowKey::new(7, 11).encode(),
                encoded_row: b"suffix".to_vec(),
            }],
        };
        session
            .receive_verified(
                &peer(),
                RangeTransferWireFrame {
                    target_node_id: NodeId::new("target"),
                    message: RangeTransferWireMessage::Change(RangeChangeEnvelope {
                        transfer_id: manifest.transfer_id,
                        content_hash: RangeChangeEnvelope::content_hash(&record).unwrap(),
                        record,
                    }),
                },
            )
            .unwrap();
    }

    #[test]
    fn verified_complete_transfer_applies_snapshot_suffix_and_durable_ack() {
        let mut session = RangeTransferSession::new(expectation());
        receive_complete(&mut session);
        let store = MemoryKV::new();
        let outcome = session.apply(&store).unwrap();
        let RangeTransferApplyOutcome::Applied(ack) = outcome else {
            panic!("expected initial apply")
        };
        assert_eq!(ack.final_epoch(), 5);
        let mut reader = store.begin(alopex_core::TxnMode::ReadOnly).unwrap();
        assert_eq!(
            reader.get(&CanonicalRowKey::new(7, 10).encode()).unwrap(),
            Some(b"snapshot".to_vec())
        );
        assert_eq!(
            reader.get(&CanonicalRowKey::new(7, 11).encode()).unwrap(),
            Some(b"suffix".to_vec())
        );
        reader.commit_self().unwrap();
        let mut recovered = RangeTransferSession::new(expectation());
        receive_complete(&mut recovered);
        assert_eq!(
            recovered.resume_point(&store).unwrap(),
            Some(RangeTransferResumePoint {
                next_chunk_ordinal: 1,
                next_epoch: 6,
            })
        );
        assert!(matches!(
            recovered.apply(&store).unwrap(),
            RangeTransferApplyOutcome::AlreadyAcknowledged(_)
        ));
    }

    #[test]
    fn unauthenticated_or_mismatched_transfer_never_mutates_target() {
        let mut session = RangeTransferSession::new(expectation());
        let error = session
            .receive_verified(
                &VerifiedPeerIdentity::new("attacker", "cluster-a"),
                RangeTransferWireFrame {
                    target_node_id: NodeId::new("target"),
                    message: RangeTransferWireMessage::Manifest(manifest()),
                },
            )
            .unwrap_err();
        assert_eq!(error, RangeTransferError::PeerNodeMismatch);
        let store = MemoryKV::new();
        assert_eq!(
            session.apply(&store).unwrap_err(),
            RangeTransferError::ManifestMissing
        );
    }

    #[test]
    fn gap_or_schema_mismatch_is_rejected_before_apply() {
        let mut session = RangeTransferSession::new(expectation());
        let mut bad = manifest();
        bad.schema_manifest_id = Some(SchemaManifestId::new("schema-other"));
        assert_eq!(
            session
                .receive_verified(
                    &peer(),
                    RangeTransferWireFrame {
                        target_node_id: NodeId::new("target"),
                        message: RangeTransferWireMessage::Manifest(bad),
                    },
                )
                .unwrap_err(),
            RangeTransferError::SchemaMismatch
        );

        let mut session = RangeTransferSession::new(expectation());
        receive_complete(&mut session);
        session.changes.clear();
        let store = MemoryKV::new();
        assert!(matches!(
            session.apply(&store).unwrap_err(),
            RangeTransferError::NonContiguousEpoch { .. }
        ));
    }

    #[test]
    fn coordinator_is_idempotent_and_keeps_old_owner_until_publish() {
        let mut coordinator = RangeTransferCoordinator::default();
        let request_id = RequestId::new("request-1");
        let prepared = coordinator
            .prepare(request_id.clone(), "transfer-1", "node-a", "node-b")
            .expect("prepare");
        assert_eq!(prepared.phase, RangeTransferPhase::Prepared);
        assert_eq!(prepared.serving_owner, NodeId::new("node-a"));
        assert_eq!(
            coordinator
                .prepare(request_id.clone(), "transfer-1", "node-a", "node-b")
                .unwrap(),
            prepared
        );

        coordinator.copy_chunk(&request_id).expect("copy");
        let verified = coordinator.verify(&request_id, 8).expect("verify");
        assert_eq!(verified.phase, RangeTransferPhase::Verified);
        assert_eq!(verified.serving_owner, NodeId::new("node-a"));
        let published = coordinator.publish(&request_id).expect("publish");
        assert_eq!(published.phase, RangeTransferPhase::Published);
        assert_eq!(published.serving_owner, NodeId::new("node-b"));
        assert_eq!(coordinator.publish(&request_id).unwrap(), published);
    }

    #[test]
    fn coordinator_abort_preserves_source_owner_and_rejects_reuse_conflict() {
        let mut coordinator = RangeTransferCoordinator::default();
        let request_id = RequestId::new("request-2");
        coordinator
            .prepare(request_id.clone(), "transfer-2", "node-a", "node-b")
            .expect("prepare");
        let aborted = coordinator.abort(&request_id).expect("abort");
        assert_eq!(aborted.phase, RangeTransferPhase::Aborted);
        assert_eq!(aborted.serving_owner, NodeId::new("node-a"));
        assert!(matches!(
            coordinator.prepare(request_id, "other-transfer", "node-a", "node-b"),
            Err(RangeTransferError::ProgressConflict)
        ));
    }

    #[test]
    fn failed_target_commit_leaves_no_rows_or_durable_ack() {
        let mut session = RangeTransferSession::new(expectation());
        receive_complete(&mut session);
        let store = MemoryKV::new_with_limit(Some(0));
        assert!(matches!(
            session.apply(&store),
            Err(RangeTransferError::Storage(_))
        ));
        let mut reader = store.begin(alopex_core::TxnMode::ReadOnly).unwrap();
        assert!(
            reader
                .get(&CanonicalRowKey::new(7, 10).encode())
                .unwrap()
                .is_none()
        );
        reader.commit_self().unwrap();
        assert!(session.resume_point(&store).unwrap().is_none());
    }

    #[test]
    fn only_a_durable_ack_can_be_converted_to_ready_evidence() {
        let mut session = RangeTransferSession::new(expectation());
        receive_complete(&mut session);
        let store = MemoryKV::new();
        let RangeTransferApplyOutcome::Applied(ack) = session.apply(&store).unwrap() else {
            panic!("expected initial apply")
        };
        let definition = expectation().definition;
        let evidence = crate::RangeReplicaDirectory::evidence_after_verified_transfer(
            &definition,
            NodeId::new("target"),
            Some(SchemaManifestId::new("schema-1")),
            &ack,
        )
        .unwrap();
        assert_eq!(evidence.lifecycle, crate::RangeReplicaLifecycle::Ready);
        assert_eq!(evidence.data_epoch, 5);
        assert!(
            crate::RangeReplicaDirectory::evidence_after_verified_transfer(
                &definition,
                NodeId::new("target"),
                Some(SchemaManifestId::new("schema-other")),
                &ack,
            )
            .is_none()
        );
    }

    struct RecordingReceiver(Arc<Mutex<Vec<NodeId>>>);

    impl VerifiedRangeTransferReceiver for RecordingReceiver {
        fn receive_verified(
            &mut self,
            peer: &VerifiedPeerIdentity,
            _frame: RangeTransferWireFrame,
        ) -> Result<(), RangeTransferError> {
            self.0
                .lock()
                .expect("test mutex")
                .push(peer.node_id().clone());
            Ok(())
        }
    }

    struct TestAuthenticator;

    impl crate::ClusterPeerAuthenticator for TestAuthenticator {
        fn authenticate(
            &self,
            inbound: &crate::InboundClusterFrame,
        ) -> Result<VerifiedPeerIdentity, crate::PeerAuthenticationError> {
            Ok(VerifiedPeerIdentity::new(
                inbound.claimed_node_id.clone(),
                inbound.claimed_cluster_id.clone(),
            ))
        }
    }

    #[test]
    fn range_transfer_uses_the_verified_dispatcher_destination_not_raft() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut dispatcher = crate::ClusterFrameDispatcher::new();
        dispatcher
            .register_handler(
                ClusterFrameKind::RangeTransfer,
                Box::new(RangeTransferFrameHandler::new(
                    NodeId::new("target"),
                    Box::new(RecordingReceiver(received.clone())),
                )),
            )
            .unwrap();
        let wire = RangeTransferWireFrame {
            target_node_id: NodeId::new("target"),
            message: RangeTransferWireMessage::Manifest(manifest()),
        };
        dispatcher
            .authenticate_and_dispatch(
                &TestAuthenticator,
                crate::InboundClusterFrame {
                    claimed_node_id: NodeId::new("source"),
                    claimed_cluster_id: ClusterId::new("cluster-a"),
                    kind: ClusterFrameKind::RangeTransfer,
                    payload: serde_json::to_vec(&wire).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(
            received.lock().expect("test mutex").as_slice(),
            &[NodeId::new("source")]
        );
        assert_eq!(dispatcher.handler_count(), 1);
    }
}
