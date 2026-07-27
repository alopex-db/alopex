use std::collections::BTreeMap;

use alopex_core::{KVStore, KVTransaction, TxnMode};

use crate::OperationState;

use super::{
    CrdtLedgerAdmission, CrdtLedgerError, CrdtLedgerRecord, CrdtOperationEnvelope,
    CrdtOperationKind, CrdtPayload, ledger::admit_in_transaction,
};

const COUNTER_PREFIX: &[u8] = b"alopex/crdt/counter/v1/";

/// Deterministic durable projection of one Counter's accepted unique updates.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CounterProjectionState {
    pub initial_value: i64,
    pub accepted_deltas: BTreeMap<String, i64>,
    pub accepted_operation_versions: BTreeMap<String, u64>,
}

impl CounterProjectionState {
    fn new(initial_value: i64, operation_id: String, update_version: u64) -> Self {
        Self {
            initial_value,
            accepted_deltas: BTreeMap::new(),
            accepted_operation_versions: BTreeMap::from([(operation_id, update_version)]),
        }
    }

    /// Sums the initial value and every accepted unique delta using checked
    /// signed arithmetic.
    pub fn value(&self) -> Result<i64, CrdtCounterError> {
        self.accepted_deltas
            .values()
            .try_fold(self.initial_value, |value, delta| {
                value
                    .checked_add(*delta)
                    .ok_or(CrdtCounterError::ArithmeticOverflow)
            })
    }

    pub fn accepted_delta_total(&self) -> Result<i64, CrdtCounterError> {
        self.accepted_deltas
            .values()
            .try_fold(0_i64, |total, delta| {
                total
                    .checked_add(*delta)
                    .ok_or(CrdtCounterError::ArithmeticOverflow)
            })
    }

    fn apply_delta(
        &mut self,
        operation_id: String,
        update_version: u64,
        delta: i64,
    ) -> Result<(), CrdtCounterError> {
        self.value()?
            .checked_add(delta)
            .ok_or(CrdtCounterError::ArithmeticOverflow)?;
        self.accepted_deltas.insert(operation_id.clone(), delta);
        self.accepted_operation_versions
            .insert(operation_id, update_version);
        Ok(())
    }

    fn as_value(&self) -> Result<CounterValue, CrdtCounterError> {
        Ok(CounterValue {
            initial_value: self.initial_value,
            accepted_delta_total: self.accepted_delta_total()?,
            value: self.value()?,
            accepted_operation_versions: self.accepted_operation_versions.clone(),
        })
    }
}

/// Observable deterministic Counter value and accepted-operation evidence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CounterValue {
    pub initial_value: i64,
    pub accepted_delta_total: i64,
    pub value: i64,
    pub accepted_operation_versions: BTreeMap<String, u64>,
}

/// Result of a Counter create/increment/decrement projection operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterApplyResult {
    pub value: CounterValue,
    pub ledger: CrdtLedgerRecord,
    pub duplicate: bool,
}

/// Counter projection backed by the same Core KV/WAL store as the CRDT ledger.
pub struct CrdtCounterProjection<S> {
    store: S,
}

impl<S> CrdtCounterProjection<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S: KVStore> CrdtCounterProjection<S> {
    /// Applies a create, increment, or decrement exactly once. The ledger and
    /// projection are committed by one KV/WAL transaction.
    pub fn apply(
        &self,
        envelope: &CrdtOperationEnvelope,
        retention_until_epoch: u64,
    ) -> Result<CounterApplyResult, CrdtCounterError> {
        let mut transaction = self.store.begin(TxnMode::ReadWrite)?;
        let key = counter_key(envelope);
        let current = transaction
            .get(&key)?
            .map(|encoded| decode_state(&encoded))
            .transpose()?;

        let admission = admit_in_transaction(
            &mut transaction,
            envelope,
            "counter_committed",
            OperationState::Committed,
            None,
            retention_until_epoch,
        )?;
        if admission.is_duplicate() {
            let state = current.ok_or(CrdtCounterError::MissingProjection {
                object_id: envelope.object_id.clone(),
            })?;
            let value = state.as_value()?;
            transaction.commit_self()?;
            return Ok(CounterApplyResult {
                value,
                ledger: admission.record().clone(),
                duplicate: true,
            });
        }

        let state = match envelope.operation {
            CrdtOperationKind::CounterCreate => {
                if current.is_some() {
                    return Err(CrdtCounterError::AlreadyExists {
                        object_id: envelope.object_id.clone(),
                    });
                }
                let initial_value = match &envelope.payload {
                    CrdtPayload::Counter {
                        initial_value: Some(value),
                        delta: None,
                    } => *value,
                    _ => return Err(CrdtCounterError::InvalidCounterPayload),
                };
                CounterProjectionState::new(
                    initial_value,
                    envelope.operation_id.clone(),
                    envelope.update_version,
                )
            }
            CrdtOperationKind::CounterIncrement | CrdtOperationKind::CounterDecrement => {
                let mut state = current.ok_or(CrdtCounterError::MissingProjection {
                    object_id: envelope.object_id.clone(),
                })?;
                let raw_delta = match &envelope.payload {
                    CrdtPayload::Counter {
                        initial_value: None,
                        delta: Some(delta),
                    } => *delta,
                    _ => return Err(CrdtCounterError::InvalidCounterPayload),
                };
                let delta = match envelope.operation {
                    CrdtOperationKind::CounterIncrement => raw_delta,
                    CrdtOperationKind::CounterDecrement => raw_delta
                        .checked_neg()
                        .ok_or(CrdtCounterError::ArithmeticOverflow)?,
                    _ => unreachable!("counter mutation was checked above"),
                };
                state.apply_delta(
                    envelope.operation_id.clone(),
                    envelope.update_version,
                    delta,
                )?;
                state
            }
            _ => {
                return Err(CrdtCounterError::WrongOperation {
                    operation: envelope.operation,
                });
            }
        };

        let value = state.as_value()?;
        transaction.put(key, serde_json::to_vec(&state)?)?;
        transaction.commit_self()?;
        Ok(CounterApplyResult {
            value,
            ledger: match admission {
                CrdtLedgerAdmission::First(record) => record,
                CrdtLedgerAdmission::Duplicate(_) => unreachable!("handled before mutation"),
            },
            duplicate: false,
        })
    }

    /// Reads the durable Counter projection without adding a ledger mutation.
    pub fn read(&self, envelope: &CrdtOperationEnvelope) -> Result<CounterValue, CrdtCounterError> {
        if envelope.operation != CrdtOperationKind::CounterRead {
            return Err(CrdtCounterError::WrongOperation {
                operation: envelope.operation,
            });
        }
        let mut transaction = self.store.begin(TxnMode::ReadOnly)?;
        let state = transaction
            .get(&counter_key(envelope))?
            .map(|encoded| decode_state(&encoded))
            .transpose()?
            .ok_or(CrdtCounterError::MissingProjection {
                object_id: envelope.object_id.clone(),
            })?;
        transaction.rollback_self()?;
        state.as_value()
    }
}

fn decode_state(encoded: &[u8]) -> Result<CounterProjectionState, CrdtCounterError> {
    serde_json::from_slice(encoded).map_err(CrdtCounterError::Decode)
}

fn counter_key(envelope: &CrdtOperationEnvelope) -> Vec<u8> {
    let mut key = COUNTER_PREFIX.to_vec();
    for component in [
        envelope.range.cluster_id.as_str(),
        envelope.range.range_id.as_str(),
        envelope.object_id.as_str(),
    ] {
        let length = u32::try_from(component.len()).expect("counter key component exceeds u32");
        key.extend_from_slice(&length.to_be_bytes());
        key.extend_from_slice(component.as_bytes());
    }
    key
}

#[derive(Debug, thiserror::Error)]
pub enum CrdtCounterError {
    #[error("CRDT counter ledger failure: {0}")]
    Ledger(#[from] CrdtLedgerError),
    #[error("CRDT counter storage failure: {0}")]
    Storage(#[from] alopex_core::Error),
    #[error("failed to encode CRDT counter projection: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("failed to decode CRDT counter projection: {0}")]
    Decode(serde_json::Error),
    #[error("counter `{object_id}` does not exist")]
    MissingProjection { object_id: String },
    #[error("counter `{object_id}` already exists")]
    AlreadyExists { object_id: String },
    #[error("operation {operation:?} is not a Counter projection operation")]
    WrongOperation { operation: CrdtOperationKind },
    #[error("Counter operation payload is invalid")]
    InvalidCounterPayload,
    #[error("Counter arithmetic exceeds the signed 64-bit range")]
    ArithmeticOverflow,
}
