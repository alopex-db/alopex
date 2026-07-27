use alopex_core::{KVStore, KVTransaction, TxnMode};

use crate::{FailureClass, IdempotencyResult, NodeId, OperationState, RequestId};

use super::{CrdtEnvelopeError, CrdtObjectType, CrdtOperationEnvelope, CrdtOperationKind};

const RECORD_PREFIX: &[u8] = b"alopex/crdt/ledger/v1/record/";
const OPERATION_PREFIX: &[u8] = b"alopex/crdt/ledger/v1/operation/";

/// Durable identity retained for a Counter or Set operation, including after
/// payload compaction.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrdtLedgerIdentity {
    pub cluster_id: String,
    pub range_id: String,
    pub object_id: String,
    pub operation_id: String,
    pub actor: NodeId,
    pub object_type: CrdtObjectType,
    pub operation: CrdtOperationKind,
    pub state_epoch: u64,
    pub update_version: u64,
    pub payload_digest: String,
}

impl CrdtLedgerIdentity {
    fn from_envelope(envelope: &CrdtOperationEnvelope) -> Result<Self, CrdtLedgerError> {
        Ok(Self {
            cluster_id: envelope.range.cluster_id.as_str().to_string(),
            range_id: envelope.range.range_id.as_str().to_string(),
            object_id: envelope.object_id.clone(),
            operation_id: envelope.operation_id.clone(),
            actor: envelope.actor.clone(),
            object_type: envelope.object_type,
            operation: envelope.operation,
            state_epoch: envelope.state_epoch,
            update_version: envelope.update_version,
            payload_digest: envelope.payload_digest()?,
        })
    }
}

/// Persisted first outcome and retention evidence for an operation identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrdtLedgerRecord {
    pub identity: CrdtLedgerIdentity,
    pub first_request_id: RequestId,
    pub first_outcome: String,
    pub first_state: OperationState,
    #[serde(default)]
    pub first_failure_class: Option<FailureClass>,
    pub duplicate_count: u64,
    pub retention_until_epoch: u64,
    /// A tombstone retains all deduplication evidence while the original
    /// payload is no longer retained by the ledger.
    #[serde(default)]
    pub tombstoned: bool,
}

impl CrdtLedgerRecord {
    /// Produces the exact existing Phase 1 idempotency shape for adapter
    /// outcome construction.
    pub fn idempotency_result(&self) -> IdempotencyResult {
        IdempotencyResult {
            operation_id: self.identity.operation_id.clone(),
            request_id: self.first_request_id.clone(),
            first_outcome: self.first_outcome.clone(),
            state: self.first_state,
            duplicate_count: self.duplicate_count,
        }
    }
}

/// Result of admitting an envelope into the durable ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrdtLedgerAdmission {
    First(CrdtLedgerRecord),
    Duplicate(CrdtLedgerRecord),
}

impl CrdtLedgerAdmission {
    pub fn record(&self) -> &CrdtLedgerRecord {
        match self {
            Self::First(record) | Self::Duplicate(record) => record,
        }
    }

    pub const fn is_duplicate(&self) -> bool {
        matches!(self, Self::Duplicate(_))
    }
}

/// Durable operation ledger backed by the same KV/WAL transaction boundary as
/// later Counter and Set projections.
pub struct CrdtOperationLedger<S> {
    store: S,
}

impl<S> CrdtOperationLedger<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S: KVStore> CrdtOperationLedger<S> {
    /// Admits one first operation or returns its recorded first outcome for a
    /// valid replay. It commits both the scoped record and operation index in
    /// one KV/WAL transaction.
    pub fn admit(
        &self,
        envelope: &CrdtOperationEnvelope,
        first_outcome: impl Into<String>,
        first_state: OperationState,
        first_failure_class: Option<FailureClass>,
        retention_until_epoch: u64,
    ) -> Result<CrdtLedgerAdmission, CrdtLedgerError> {
        let mut transaction = self.store.begin(TxnMode::ReadWrite)?;
        let admission = self.admit_in_transaction(
            &mut transaction,
            envelope,
            first_outcome,
            first_state,
            first_failure_class,
            retention_until_epoch,
        )?;
        transaction.commit_self()?;
        Ok(admission)
    }

    /// Admits within a caller-owned transaction so Counter and Set projections
    /// can share the exact WAL commit point with the ledger entry.
    pub fn admit_in_transaction<'a, T: KVTransaction<'a>>(
        &self,
        transaction: &mut T,
        envelope: &CrdtOperationEnvelope,
        first_outcome: impl Into<String>,
        first_state: OperationState,
        first_failure_class: Option<FailureClass>,
        retention_until_epoch: u64,
    ) -> Result<CrdtLedgerAdmission, CrdtLedgerError> {
        let identity = CrdtLedgerIdentity::from_envelope(envelope)?;
        let operation_key = operation_key(&identity.operation_id);
        if let Some(encoded) = transaction.get(&operation_key)? {
            let mut record = decode_record(&encoded)?;
            if record.identity != identity {
                return Err(CrdtLedgerError::Conflict {
                    operation_id: identity.operation_id,
                });
            }
            record.duplicate_count = record
                .duplicate_count
                .checked_add(1)
                .ok_or(CrdtLedgerError::DuplicateCountOverflow)?;
            write_record(transaction, &record)?;
            return Ok(CrdtLedgerAdmission::Duplicate(record));
        }

        let record = CrdtLedgerRecord {
            identity,
            first_request_id: envelope.request_id.clone(),
            first_outcome: first_outcome.into(),
            first_state,
            first_failure_class,
            duplicate_count: 0,
            retention_until_epoch,
            tombstoned: false,
        };
        write_record(transaction, &record)?;
        Ok(CrdtLedgerAdmission::First(record))
    }

    /// Retrieves a retained operation record by operation identity.
    pub fn read(&self, operation_id: &str) -> Result<Option<CrdtLedgerRecord>, CrdtLedgerError> {
        let mut transaction = self.store.begin(TxnMode::ReadOnly)?;
        let result = transaction
            .get(&operation_key(operation_id))?
            .map(|encoded| decode_record(&encoded))
            .transpose()?;
        transaction.rollback_self()?;
        Ok(result)
    }

    /// Retains deduplication evidence while marking the payload as compacted.
    /// The record remains replayable through `retention_until_epoch`.
    pub fn tombstone(
        &self,
        operation_id: &str,
        retention_until_epoch: u64,
    ) -> Result<CrdtLedgerRecord, CrdtLedgerError> {
        let mut transaction = self.store.begin(TxnMode::ReadWrite)?;
        let encoded = transaction
            .get(&operation_key(operation_id))?
            .ok_or_else(|| CrdtLedgerError::MissingOperation {
                operation_id: operation_id.to_string(),
            })?;
        let mut record = decode_record(&encoded)?;
        record.tombstoned = true;
        record.retention_until_epoch = record.retention_until_epoch.max(retention_until_epoch);
        write_record(&mut transaction, &record)?;
        transaction.commit_self()?;
        Ok(record)
    }
}

fn write_record<'a, T: KVTransaction<'a>>(
    transaction: &mut T,
    record: &CrdtLedgerRecord,
) -> Result<(), CrdtLedgerError> {
    let encoded = serde_json::to_vec(record)?;
    transaction.put(scoped_key(&record.identity), encoded.clone())?;
    transaction.put(operation_key(&record.identity.operation_id), encoded)?;
    Ok(())
}

fn decode_record(encoded: &[u8]) -> Result<CrdtLedgerRecord, CrdtLedgerError> {
    serde_json::from_slice(encoded).map_err(CrdtLedgerError::Decode)
}

fn scoped_key(identity: &CrdtLedgerIdentity) -> Vec<u8> {
    let mut key = RECORD_PREFIX.to_vec();
    for component in [
        identity.cluster_id.as_str(),
        identity.range_id.as_str(),
        identity.object_id.as_str(),
        identity.operation_id.as_str(),
    ] {
        append_component(&mut key, component);
    }
    key
}

fn operation_key(operation_id: &str) -> Vec<u8> {
    let mut key = OPERATION_PREFIX.to_vec();
    append_component(&mut key, operation_id);
    key
}

fn append_component(key: &mut Vec<u8>, component: &str) {
    let length = u32::try_from(component.len()).expect("operation identity component exceeds u32");
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(component.as_bytes());
}

#[derive(Debug, thiserror::Error)]
pub enum CrdtLedgerError {
    #[error("CRDT envelope is invalid: {0}")]
    Envelope(#[from] CrdtEnvelopeError),
    #[error("CRDT ledger storage failure: {0}")]
    Storage(#[from] alopex_core::Error),
    #[error("failed to encode CRDT ledger record: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("failed to decode CRDT ledger record: {0}")]
    Decode(serde_json::Error),
    #[error(
        "operation `{operation_id}` conflicts with a retained CRDT operation identity or payload"
    )]
    Conflict { operation_id: String },
    #[error("operation `{operation_id}` is not retained by the CRDT ledger")]
    MissingOperation { operation_id: String },
    #[error("CRDT ledger duplicate count overflowed")]
    DuplicateCountOverflow,
}

impl CrdtLedgerError {
    pub const fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }
}
