use std::collections::BTreeMap;

use alopex_core::{KVStore, KVTransaction, TxnMode};
use unicode_normalization::is_nfc;

use crate::OperationState;

use super::{
    CrdtLedgerAdmission, CrdtLedgerError, CrdtLedgerRecord, CrdtOperationEnvelope,
    CrdtOperationKind, CrdtPayload, ledger::admit_in_transaction,
};

const SET_PREFIX: &[u8] = b"alopex/crdt/set/v1/";

/// Explicit limits advertised by the Set capability rather than hidden
/// per-replica constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SetProjectionLimits {
    pub max_member_bytes: usize,
    pub max_object_bytes: usize,
}

impl Default for SetProjectionLimits {
    fn default() -> Self {
        Self {
            max_member_bytes: 1024 * 1024,
            max_object_bytes: 16 * 1024 * 1024,
        }
    }
}

/// The winning add or remove for one normalized Set member.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SetMemberVersion {
    pub update_version: u64,
    pub operation_id: String,
    pub present: bool,
}

impl SetMemberVersion {
    fn wins_over(&self, current: &Self) -> bool {
        (self.update_version, self.operation_id.as_str())
            > (current.update_version, current.operation_id.as_str())
    }
}

/// Durable deterministic projection of a Set's member winners and accepted
/// operation versions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SetProjectionState {
    pub state_epoch: u64,
    pub members: BTreeMap<String, SetMemberVersion>,
    pub accepted_operation_versions: BTreeMap<String, u64>,
}

impl SetProjectionState {
    fn new(state_epoch: u64, operation_id: String, update_version: u64) -> Self {
        Self {
            state_epoch,
            members: BTreeMap::new(),
            accepted_operation_versions: BTreeMap::from([(operation_id, update_version)]),
        }
    }

    fn apply_member(&mut self, member: String, winner: SetMemberVersion) {
        if self
            .members
            .get(&member)
            .is_none_or(|current| winner.wins_over(current))
        {
            self.members.insert(member, winner);
        }
    }

    fn as_value(&self) -> SetValue {
        let members = self
            .members
            .iter()
            .filter_map(|(member, winner)| winner.present.then_some(member.clone()))
            .collect();
        SetValue {
            members,
            member_versions: self.members.clone(),
            accepted_operation_versions: self.accepted_operation_versions.clone(),
        }
    }
}

/// Canonically ordered Set membership and all per-member winner evidence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SetValue {
    pub members: Vec<String>,
    pub member_versions: BTreeMap<String, SetMemberVersion>,
    pub accepted_operation_versions: BTreeMap<String, u64>,
}

/// Result of a Set create/add/remove projection operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetApplyResult {
    pub value: SetValue,
    pub ledger: CrdtLedgerRecord,
    pub duplicate: bool,
}

/// Set projection backed by the same Core KV/WAL transaction as the CRDT
/// operation ledger.
pub struct CrdtSetProjection<S> {
    store: S,
    limits: SetProjectionLimits,
}

impl<S> CrdtSetProjection<S> {
    pub fn new(store: S) -> Self {
        Self::with_limits(store, SetProjectionLimits::default())
    }

    pub fn with_limits(store: S, limits: SetProjectionLimits) -> Self {
        Self { store, limits }
    }

    pub fn limits(&self) -> SetProjectionLimits {
        self.limits
    }

    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S: KVStore> CrdtSetProjection<S> {
    /// Applies a create, add, or remove. Validation and capacity checks finish
    /// before ledger admission; the winning projection and ledger record then
    /// share one durable commit boundary.
    pub fn apply(
        &self,
        envelope: &CrdtOperationEnvelope,
        retention_until_epoch: u64,
    ) -> Result<SetApplyResult, CrdtSetError> {
        let member = self.validate_mutation(envelope)?;
        let mut transaction = self.store.begin(TxnMode::ReadWrite)?;
        let key = set_key(envelope);
        let current = transaction
            .get(&key)?
            .map(|encoded| decode_state(&encoded))
            .transpose()?;
        self.preflight(envelope, member.as_deref(), current.as_ref())?;

        let admission = admit_in_transaction(
            &mut transaction,
            envelope,
            "set_committed",
            OperationState::Committed,
            None,
            retention_until_epoch,
        )?;
        if admission.is_duplicate() {
            let state = current.ok_or(CrdtSetError::MissingProjection {
                object_id: envelope.object_id.clone(),
            })?;
            transaction.commit_self()?;
            return Ok(SetApplyResult {
                value: state.as_value(),
                ledger: admission.record().clone(),
                duplicate: true,
            });
        }

        let state = match envelope.operation {
            CrdtOperationKind::SetCreate => {
                if current.is_some() {
                    return Err(CrdtSetError::AlreadyExists {
                        object_id: envelope.object_id.clone(),
                    });
                }
                SetProjectionState::new(
                    envelope.state_epoch,
                    envelope.operation_id.clone(),
                    envelope.update_version,
                )
            }
            CrdtOperationKind::SetAdd | CrdtOperationKind::SetRemove => {
                let mut state = current.expect("preflight required existing Set projection");
                let member = member.expect("preflight required Set member");
                let winner = SetMemberVersion {
                    update_version: envelope.update_version,
                    operation_id: envelope.operation_id.clone(),
                    present: envelope.operation == CrdtOperationKind::SetAdd,
                };
                state.apply_member(member, winner);
                state
                    .accepted_operation_versions
                    .insert(envelope.operation_id.clone(), envelope.update_version);
                state
            }
            _ => unreachable!("preflight accepted only Set mutations"),
        };

        let value = state.as_value();
        transaction.put(key, serde_json::to_vec(&state)?)?;
        transaction.commit_self()?;
        Ok(SetApplyResult {
            value,
            ledger: match admission {
                CrdtLedgerAdmission::First(record) => record,
                CrdtLedgerAdmission::Duplicate(_) => unreachable!("handled before mutation"),
            },
            duplicate: false,
        })
    }

    /// Reads the durable Set value without creating a ledger operation.
    pub fn read(&self, envelope: &CrdtOperationEnvelope) -> Result<SetValue, CrdtSetError> {
        if envelope.operation != CrdtOperationKind::SetRead {
            return Err(CrdtSetError::WrongOperation {
                operation: envelope.operation,
            });
        }
        let mut transaction = self.store.begin(TxnMode::ReadOnly)?;
        let state = transaction
            .get(&set_key(envelope))?
            .map(|encoded| decode_state(&encoded))
            .transpose()?
            .ok_or(CrdtSetError::MissingProjection {
                object_id: envelope.object_id.clone(),
            })?;
        self.validate_epoch(envelope, &state)?;
        transaction.rollback_self()?;
        Ok(state.as_value())
    }

    fn validate_mutation(
        &self,
        envelope: &CrdtOperationEnvelope,
    ) -> Result<Option<String>, CrdtSetError> {
        match envelope.operation {
            CrdtOperationKind::SetCreate => match &envelope.payload {
                CrdtPayload::None => Ok(None),
                _ => Err(CrdtSetError::InvalidSetPayload),
            },
            CrdtOperationKind::SetAdd | CrdtOperationKind::SetRemove => {
                if !is_canonical_lowercase_uuid(&envelope.operation_id) {
                    return Err(CrdtSetError::NonCanonicalOperationId);
                }
                let member = match &envelope.payload {
                    CrdtPayload::Set {
                        member: Some(member),
                    } => member,
                    _ => return Err(CrdtSetError::InvalidSetPayload),
                };
                if member.is_empty() || !is_nfc(member) {
                    return Err(CrdtSetError::NonCanonicalMember);
                }
                if member.len() > self.limits.max_member_bytes {
                    return Err(CrdtSetError::ResourceLimit {
                        limit: self.limits.max_member_bytes,
                    });
                }
                Ok(Some(member.clone()))
            }
            _ => Err(CrdtSetError::WrongOperation {
                operation: envelope.operation,
            }),
        }
    }

    fn preflight(
        &self,
        envelope: &CrdtOperationEnvelope,
        member: Option<&str>,
        current: Option<&SetProjectionState>,
    ) -> Result<(), CrdtSetError> {
        match envelope.operation {
            CrdtOperationKind::SetCreate => {}
            CrdtOperationKind::SetAdd | CrdtOperationKind::SetRemove => {
                let state = current.ok_or(CrdtSetError::MissingProjection {
                    object_id: envelope.object_id.clone(),
                })?;
                self.validate_epoch(envelope, state)?;
                let member = member.expect("mutation validation supplied a member");
                let winner = SetMemberVersion {
                    update_version: envelope.update_version,
                    operation_id: envelope.operation_id.clone(),
                    present: envelope.operation == CrdtOperationKind::SetAdd,
                };
                if winner.present && member_would_be_added(state, member, &winner) {
                    validate_object_limit(state, member, self.limits)?;
                }
            }
            _ => unreachable!("mutation validation rejects non-mutation operations"),
        }
        Ok(())
    }

    fn validate_epoch(
        &self,
        envelope: &CrdtOperationEnvelope,
        state: &SetProjectionState,
    ) -> Result<(), CrdtSetError> {
        if state.state_epoch != envelope.state_epoch {
            return Err(CrdtSetError::EpochMismatch {
                expected: state.state_epoch,
                actual: envelope.state_epoch,
            });
        }
        Ok(())
    }
}

fn member_would_be_added(
    state: &SetProjectionState,
    member: &str,
    candidate: &SetMemberVersion,
) -> bool {
    state
        .members
        .get(member)
        .is_none_or(|current| candidate.wins_over(current) && !current.present)
}

fn validate_object_limit(
    state: &SetProjectionState,
    member: &str,
    limits: SetProjectionLimits,
) -> Result<(), CrdtSetError> {
    let membership_bytes = state
        .members
        .iter()
        .filter(|(_, winner)| winner.present)
        .map(|(member, _)| member.len() + std::mem::size_of::<u32>())
        .try_fold(member.len() + std::mem::size_of::<u32>(), |total, bytes| {
            total.checked_add(bytes)
        })
        .ok_or(CrdtSetError::ResourceLimit {
            limit: limits.max_object_bytes,
        })?;
    if membership_bytes > limits.max_object_bytes {
        return Err(CrdtSetError::ResourceLimit {
            limit: limits.max_object_bytes,
        });
    }
    Ok(())
}

fn decode_state(encoded: &[u8]) -> Result<SetProjectionState, CrdtSetError> {
    serde_json::from_slice(encoded).map_err(CrdtSetError::Decode)
}

fn set_key(envelope: &CrdtOperationEnvelope) -> Vec<u8> {
    let mut key = SET_PREFIX.to_vec();
    for component in [
        envelope.range.cluster_id.as_str(),
        envelope.range.range_id.as_str(),
        envelope.object_id.as_str(),
    ] {
        let length = u32::try_from(component.len()).expect("Set key component exceeds u32");
        key.extend_from_slice(&length.to_be_bytes());
        key.extend_from_slice(component.as_bytes());
    }
    key
}

fn is_canonical_lowercase_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        })
}

#[derive(Debug, thiserror::Error)]
pub enum CrdtSetError {
    #[error("CRDT Set ledger failure: {0}")]
    Ledger(#[from] CrdtLedgerError),
    #[error("CRDT Set storage failure: {0}")]
    Storage(#[from] alopex_core::Error),
    #[error("failed to encode CRDT Set projection: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("failed to decode CRDT Set projection: {0}")]
    Decode(serde_json::Error),
    #[error("Set `{object_id}` does not exist")]
    MissingProjection { object_id: String },
    #[error("Set `{object_id}` already exists")]
    AlreadyExists { object_id: String },
    #[error("operation {operation:?} is not a Set projection operation")]
    WrongOperation { operation: CrdtOperationKind },
    #[error("Set operation payload is invalid")]
    InvalidSetPayload,
    #[error("Set member must be non-empty canonical NFC text")]
    NonCanonicalMember,
    #[error("Set conflict tie-breaker requires a canonical lowercase UUID operation_id")]
    NonCanonicalOperationId,
    #[error("Set resource limit exceeded (limit={limit} bytes)")]
    ResourceLimit { limit: usize },
    #[error("Set state epoch mismatch: expected {expected}, received {actual}")]
    EpochMismatch { expected: u64, actual: u64 },
}
