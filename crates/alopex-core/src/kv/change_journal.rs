//! Atomic local range-change journal staging for replication.

use crate::kv::KVTransaction;
use crate::types::{Key, Value};
use crate::Result;
use serde::{Deserialize, Serialize};

const JOURNAL_PREFIX: &[u8] = b"\x00alopex/range-change/";

/// Immutable data-plane payload captured with a local durable commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeChangePayload {
    /// A primary row image written at a canonical row key.
    UpsertRow {
        /// Canonical primary row key.
        row_key: Vec<u8>,
        /// Encoded durable row image.
        encoded_row: Vec<u8>,
    },
    /// A primary-row tombstone.
    DeleteRow {
        /// Canonical primary row key.
        row_key: Vec<u8>,
        /// Durable tombstone payload.
        tombstone: Vec<u8>,
    },
    /// A secondary-index entry referring to a canonical row key.
    UpsertIndex {
        /// Secondary-index identity.
        index_id: u32,
        /// Encoded secondary-index key.
        index_key: Vec<u8>,
        /// Referenced canonical primary row key.
        row_key: Vec<u8>,
    },
    /// A removed secondary-index entry.
    DeleteIndex {
        /// Secondary-index identity.
        index_id: u32,
        /// Encoded secondary-index key.
        index_key: Vec<u8>,
        /// Referenced canonical primary row key.
        row_key: Vec<u8>,
    },
}

/// Ordered immutable change record with replay identity and epoch predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeChangeRecord {
    /// Logical range identity.
    pub range_id: String,
    /// Immutable range generation.
    pub generation: u64,
    /// Applied data epoch.
    pub epoch: u64,
    /// Required previous epoch for contiguous replay.
    pub predecessor_epoch: Option<u64>,
    /// Idempotent replay identity.
    pub replay_id: String,
    /// Ordered row/index/tombstone mutations.
    pub payload: Vec<RangeChangePayload>,
}

/// Whether a concrete backend can durably stage replication changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeChangeJournalCapability {
    /// The backend can atomically stage journal and data mutations.
    Supported,
    /// The backend must not advertise replication readiness.
    Unavailable,
}

/// Stages the immutable journal record in the caller's current write
/// transaction. The caller must stage its row/index mutations in that same
/// transaction before invoking `commit_self`.
pub fn stage_range_change<'a, T: KVTransaction<'a>>(
    transaction: &mut T,
    record: &RangeChangeRecord,
) -> Result<Key> {
    let key = journal_key(record);
    let value = bincode::serialize(record).expect("RangeChangeRecord serialization is infallible");
    transaction.put(key.clone(), value)?;
    Ok(key)
}

/// Decodes a persisted journal entry read from the local store.
pub fn decode_range_change(value: &Value) -> Result<RangeChangeRecord> {
    bincode::deserialize(value).map_err(|error| {
        crate::Error::InvalidFormat(format!("invalid range-change journal: {error}"))
    })
}

/// Produces the stable outbox key for a single range epoch/replay identity.
pub fn journal_key(record: &RangeChangeRecord) -> Key {
    let mut key = JOURNAL_PREFIX.to_vec();
    key.extend_from_slice(record.range_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&record.generation.to_be_bytes());
    key.extend_from_slice(&record.epoch.to_be_bytes());
    key.push(0);
    key.extend_from_slice(record.replay_id.as_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::memory::MemoryKV;
    use crate::kv::{KVStore, KVTransaction};
    use crate::types::TxnMode;

    fn record() -> RangeChangeRecord {
        RangeChangeRecord {
            range_id: "range-a".into(),
            generation: 1,
            epoch: 2,
            predecessor_epoch: Some(1),
            replay_id: "replay-1".into(),
            payload: vec![RangeChangePayload::UpsertRow {
                row_key: b"row-a".to_vec(),
                encoded_row: b"value-a".to_vec(),
            }],
        }
    }

    #[test]
    fn journal_and_data_share_one_local_commit_boundary() {
        let store = MemoryKV::new();
        let record = record();
        let key = journal_key(&record);
        let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
        transaction
            .put(b"row-a".to_vec(), b"value-a".to_vec())
            .unwrap();
        stage_range_change(&mut transaction, &record).unwrap();
        transaction.commit_self().unwrap();

        let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(
            reader.get(&b"row-a".to_vec()).unwrap(),
            Some(b"value-a".to_vec())
        );
        assert_eq!(
            decode_range_change(&reader.get(&key).unwrap().unwrap()).unwrap(),
            record
        );
    }

    #[test]
    fn rollback_exposes_neither_data_nor_journal_entry() {
        let store = MemoryKV::new();
        let record = record();
        let key = journal_key(&record);
        let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
        transaction
            .put(b"row-a".to_vec(), b"value-a".to_vec())
            .unwrap();
        stage_range_change(&mut transaction, &record).unwrap();
        transaction.rollback_self().unwrap();

        let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
        assert!(reader.get(&b"row-a".to_vec()).unwrap().is_none());
        assert!(reader.get(&key).unwrap().is_none());
    }

    #[test]
    fn failed_commit_exposes_neither_data_nor_journal_entry() {
        let store = MemoryKV::new_with_limit(Some(0));
        let record = record();
        let key = journal_key(&record);
        let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
        transaction
            .put(b"row-a".to_vec(), b"value-a".to_vec())
            .unwrap();
        stage_range_change(&mut transaction, &record).unwrap();
        assert!(transaction.commit_self().is_err());

        let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
        assert!(reader.get(&b"row-a".to_vec()).unwrap().is_none());
        assert!(reader.get(&key).unwrap().is_none());
    }
}
