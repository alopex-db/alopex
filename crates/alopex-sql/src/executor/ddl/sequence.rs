//! Transactional sequence metadata and allocation primitives.

use alopex_core::kv::{KVStore, KVTransaction};
use serde::{Deserialize, Serialize};

use crate::ast::ddl::{AlterSequence, CreateSequence, DropSequence, SequenceOptions};
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::SqlTxn;

const PREFIX: &[u8] = b"__sequence__/";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SequenceState {
    next_value: i64,
    increment: i64,
    min_value: i64,
    max_value: i64,
    cycle: bool,
}

fn key(name: &str) -> Vec<u8> {
    let mut value = PREFIX.to_vec();
    value.extend_from_slice(name.as_bytes());
    value
}

fn state_from_options(options: &SequenceOptions) -> Result<SequenceState> {
    let increment = options.increment.unwrap_or(1);
    if increment == 0 {
        return Err(ExecutorError::InvalidOperation {
            operation: "CREATE SEQUENCE".into(),
            reason: "INCREMENT must not be zero".into(),
        });
    }
    let (default_min, default_max) = if increment > 0 {
        (1, i64::MAX)
    } else {
        (i64::MIN, -1)
    };
    let min_value = options.min_value.unwrap_or(default_min);
    let max_value = options.max_value.unwrap_or(default_max);
    if min_value >= max_value {
        return Err(ExecutorError::InvalidOperation {
            operation: "CREATE SEQUENCE".into(),
            reason: "MINVALUE must be less than MAXVALUE".into(),
        });
    }
    let start = options
        .restart
        .or(options.start)
        .unwrap_or(if increment > 0 { 1 } else { -1 });
    if start < min_value || start > max_value {
        return Err(ExecutorError::InvalidOperation {
            operation: "CREATE SEQUENCE".into(),
            reason: "START value is outside sequence bounds".into(),
        });
    }
    Ok(SequenceState {
        next_value: start,
        increment,
        min_value,
        max_value,
        cycle: options.cycle.unwrap_or(false),
    })
}

fn encode(state: &SequenceState) -> Result<Vec<u8>> {
    bincode::serialize(state).map_err(|error| ExecutorError::InvalidOperation {
        operation: "sequence".into(),
        reason: error.to_string(),
    })
}

fn decode(bytes: &[u8]) -> Result<SequenceState> {
    bincode::deserialize(bytes).map_err(|error| ExecutorError::InvalidOperation {
        operation: "sequence".into(),
        reason: error.to_string(),
    })
}

pub fn create<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    statement: CreateSequence,
) -> Result<ExecutionResult> {
    txn.ensure_write_txn()?;
    let sequence_key = key(&statement.name);
    if txn.inner_mut().get(&sequence_key)?.is_some() {
        return if statement.if_not_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::InvalidOperation {
                operation: "CREATE SEQUENCE".into(),
                reason: format!("sequence already exists: {}", statement.name),
            })
        };
    }
    txn.inner_mut().put(
        sequence_key,
        encode(&state_from_options(&statement.options)?)?,
    )?;
    Ok(ExecutionResult::Success)
}

pub fn alter<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    statement: AlterSequence,
) -> Result<ExecutionResult> {
    txn.ensure_write_txn()?;
    let sequence_key = key(&statement.name);
    let Some(bytes) = txn.inner_mut().get(&sequence_key)? else {
        return if statement.if_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::InvalidOperation {
                operation: "ALTER SEQUENCE".into(),
                reason: format!("sequence not found: {}", statement.name),
            })
        };
    };
    let mut state = decode(&bytes)?;
    if statement.options.restart.is_some()
        || statement.options.start.is_some()
        || statement.options.increment.is_some()
        || statement.options.min_value.is_some()
        || statement.options.max_value.is_some()
        || statement.options.cycle.is_some()
    {
        let _ = state_from_options(&statement.options)?;
        state.next_value = statement
            .options
            .restart
            .or(statement.options.start)
            .unwrap_or(state.next_value);
        state.increment = statement.options.increment.unwrap_or(state.increment);
        state.min_value = statement.options.min_value.unwrap_or(state.min_value);
        state.max_value = statement.options.max_value.unwrap_or(state.max_value);
        state.cycle = statement.options.cycle.unwrap_or(state.cycle);
    }
    txn.inner_mut().put(sequence_key, encode(&state)?)?;
    Ok(ExecutionResult::Success)
}

/// Allocate and persist the next value for a sequence in the current transaction.
#[allow(dead_code)]
pub fn next_value<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    name: &str,
) -> Result<i64> {
    txn.ensure_write_txn()?;
    let sequence_key = key(name);
    let bytes =
        txn.inner_mut()
            .get(&sequence_key)?
            .ok_or_else(|| ExecutorError::InvalidOperation {
                operation: "nextval".into(),
                reason: format!("sequence not found: {name}"),
            })?;
    let mut state = decode(&bytes)?;
    let value = state.next_value;
    state.next_value = match value.checked_add(state.increment) {
        Some(next) if next >= state.min_value && next <= state.max_value => next,
        Some(_) if state.cycle => {
            if state.increment > 0 {
                state.min_value
            } else {
                state.max_value
            }
        }
        _ => {
            return Err(ExecutorError::InvalidOperation {
                operation: "nextval".into(),
                reason: "sequence exhausted".into(),
            });
        }
    };
    txn.inner_mut().put(sequence_key, encode(&state)?)?;
    Ok(value)
}

pub fn drop<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    statement: DropSequence,
) -> Result<ExecutionResult> {
    txn.ensure_write_txn()?;
    let sequence_key = key(&statement.name);
    if txn.inner_mut().get(&sequence_key)?.is_none() {
        return if statement.if_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::InvalidOperation {
                operation: "DROP SEQUENCE".into(),
                reason: format!("sequence not found: {}", statement.name),
            })
        };
    }
    txn.inner_mut().delete(sequence_key)?;
    Ok(ExecutionResult::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use alopex_core::kv::memory::MemoryKV;

    use crate::ast::Span;
    use crate::storage::TxnBridge;

    #[test]
    fn validates_increment_and_bounds() {
        let state = state_from_options(&SequenceOptions {
            start: Some(10),
            increment: Some(2),
            min_value: Some(1),
            max_value: Some(20),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(state.next_value, 10);
        assert_eq!(state.increment, 2);
        assert!(
            state_from_options(&SequenceOptions {
                increment: Some(0),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn next_value_is_monotonic_and_transactional() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut txn = bridge.begin_write().unwrap();
        create(
            &mut txn,
            CreateSequence {
                if_not_exists: false,
                name: "ids".into(),
                options: SequenceOptions {
                    start: Some(10),
                    increment: Some(2),
                    ..Default::default()
                },
                span: Span::default(),
            },
        )
        .unwrap();
        assert_eq!(next_value(&mut txn, "ids").unwrap(), 10);
        assert_eq!(next_value(&mut txn, "ids").unwrap(), 12);
        txn.commit().unwrap();
    }
}
