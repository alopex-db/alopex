//! Transactional sequence metadata and allocation primitives.

use alopex_core::kv::{KVStore, KVTransaction};
use serde::{Deserialize, Serialize};

use crate::ast::ddl::{AlterSequence, CreateSequence, DropSequence, SequenceOptions};
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::SqlTxn;

const PREFIX: &[u8] = b"__sequence__/";

/// Public snapshot of a persisted sequence definition and allocation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceInfo {
    pub name: String,
    pub start_value: i64,
    pub next_value: i64,
    pub last_value: Option<i64>,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cache: u64,
    pub cycle: bool,
    pub owned_by: Option<String>,
}

/// Build the reserved sequence name used by SERIAL and IDENTITY columns.
pub fn generated_name(table: &str, column: &str) -> String {
    format!("__alopex_auto__{table}__{column}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SequenceState {
    start_value: i64,
    next_value: i64,
    last_value: Option<i64>,
    increment: i64,
    min_value: i64,
    max_value: i64,
    cache: u64,
    cycle: bool,
    owned_by: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LegacySequenceState {
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
    if options.cache == Some(0) {
        return Err(ExecutorError::InvalidOperation {
            operation: "CREATE SEQUENCE".into(),
            reason: "CACHE must be greater than zero".into(),
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
        start_value: start,
        next_value: start,
        last_value: None,
        increment,
        min_value,
        max_value,
        cache: options.cache.unwrap_or(1),
        cycle: options.cycle.unwrap_or(false),
        owned_by: options.owned_by.clone(),
    })
}

fn encode(state: &SequenceState) -> Result<Vec<u8>> {
    bincode::serialize(state).map_err(|error| ExecutorError::InvalidOperation {
        operation: "sequence".into(),
        reason: error.to_string(),
    })
}

fn decode(bytes: &[u8]) -> Result<SequenceState> {
    if let Ok(state) = bincode::deserialize(bytes) {
        return Ok(state);
    }
    bincode::deserialize::<LegacySequenceState>(bytes)
        .map(|state| SequenceState {
            start_value: state.next_value,
            next_value: state.next_value,
            last_value: None,
            increment: state.increment,
            min_value: state.min_value,
            max_value: state.max_value,
            cache: 1,
            cycle: state.cycle,
            owned_by: None,
        })
        .map_err(|error| ExecutorError::InvalidOperation {
            operation: "sequence".into(),
            reason: error.to_string(),
        })
}

pub fn list<'txn, S: KVStore + 'txn>(txn: &mut impl SqlTxn<'txn, S>) -> Result<Vec<SequenceInfo>> {
    let mut sequences = txn
        .inner_mut()
        .scan_prefix(PREFIX)?
        .map(|(key, value)| {
            let name = std::str::from_utf8(&key[PREFIX.len()..])
                .map_err(|error| ExecutorError::InvalidOperation {
                    operation: "sequence introspection".into(),
                    reason: error.to_string(),
                })?
                .to_string();
            let state = decode(&value)?;
            Ok(SequenceInfo {
                name,
                start_value: state.start_value,
                next_value: state.next_value,
                last_value: state.last_value,
                increment: state.increment,
                min_value: state.min_value,
                max_value: state.max_value,
                cache: state.cache,
                cycle: state.cycle,
                owned_by: state.owned_by,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    sequences.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(sequences)
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

/// Create an implicit sequence for a SERIAL/IDENTITY column.
pub fn create_generated<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    name: String,
    owned_by: String,
    mut options: SequenceOptions,
) -> Result<()> {
    options.owned_by = Some(owned_by);
    let _ = create(
        txn,
        CreateSequence {
            if_not_exists: false,
            name,
            options,
            span: crate::ast::Span::default(),
        },
    )?;
    Ok(())
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
    let options = statement.options;
    let increment = options.increment.unwrap_or(state.increment);
    let min_value = options.min_value.unwrap_or(state.min_value);
    let max_value = options.max_value.unwrap_or(state.max_value);
    if increment == 0 || min_value >= max_value || options.cache == Some(0) {
        return Err(ExecutorError::InvalidOperation {
            operation: "ALTER SEQUENCE".into(),
            reason: "sequence increment, bounds, or cache is invalid".into(),
        });
    }
    let start_value = options.start.unwrap_or(state.start_value);
    let next_value = if options.restart_default {
        start_value
    } else {
        options.restart.unwrap_or(state.next_value)
    };
    if start_value < min_value
        || start_value > max_value
        || next_value < min_value
        || next_value > max_value
    {
        return Err(ExecutorError::InvalidOperation {
            operation: "ALTER SEQUENCE".into(),
            reason: "sequence value is outside sequence bounds".into(),
        });
    }
    state.start_value = start_value;
    state.next_value = next_value;
    if options.restart.is_some() || options.restart_default {
        state.last_value = None;
    }
    state.increment = increment;
    state.min_value = min_value;
    state.max_value = max_value;
    state.cache = options.cache.unwrap_or(state.cache);
    state.cycle = options.cycle.unwrap_or(state.cycle);
    state.owned_by = options.owned_by.or(state.owned_by);
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
    state.last_value = Some(value);
    txn.inner_mut().put(sequence_key, encode(&state)?)?;
    Ok(value)
}

/// Return the value most recently allocated from a sequence.
pub fn current_value<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    name: &str,
) -> Result<i64> {
    let bytes =
        txn.inner_mut()
            .get(&key(name))?
            .ok_or_else(|| ExecutorError::InvalidOperation {
                operation: "currval".into(),
                reason: format!("sequence not found: {name}"),
            })?;
    decode(&bytes)?
        .last_value
        .ok_or_else(|| ExecutorError::InvalidOperation {
            operation: "currval".into(),
            reason: format!("nextval has not been called for sequence: {name}"),
        })
}

/// Drop implicit and explicit sequences owned by a table.
pub fn drop_owned_by<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table: &str,
) -> Result<()> {
    let mut scan = txn.inner_mut().scan_prefix(PREFIX)?;
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for entry in scan.by_ref() {
        let (key, value) = entry;
        let state = decode(&value)?;
        let generated = std::str::from_utf8(&key).ok().is_some_and(|name| {
            name.starts_with(&format!("__sequence__/{}", generated_name(table, "")))
        });
        let explicit = state
            .owned_by
            .as_deref()
            .and_then(|owner| owner.split_once('.'))
            .is_some_and(|(owner_table, _)| owner_table.eq_ignore_ascii_case(table));
        if generated || explicit {
            keys.push(key);
        }
    }
    std::mem::drop(scan);
    for key in keys {
        txn.inner_mut().delete(key)?;
    }
    Ok(())
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
    fn decodes_v0810_sequence_state() {
        #[derive(Serialize)]
        struct V0810SequenceState {
            next_value: i64,
            increment: i64,
            min_value: i64,
            max_value: i64,
            cycle: bool,
        }

        let bytes = bincode::serialize(&V0810SequenceState {
            next_value: 7,
            increment: 2,
            min_value: 1,
            max_value: 99,
            cycle: true,
        })
        .unwrap();
        let state = decode(&bytes).unwrap();

        assert_eq!(state.start_value, 7);
        assert_eq!(state.next_value, 7);
        assert_eq!(state.cache, 1);
        assert_eq!(state.last_value, None);
        assert_eq!(state.owned_by, None);
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
