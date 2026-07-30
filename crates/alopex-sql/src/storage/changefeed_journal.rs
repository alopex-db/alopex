//! Read-only, committed input for the Phase 3 changefeed adapter.
//!
//! `LocalRangeChangeJournal` stages a record in the same transaction as SQL
//! data. This module intentionally opens a fresh read-only transaction before
//! exposing a record to an adapter, so staged, rolled-back, and failed commits
//! cannot be mistaken for published feed input.

use alopex_core::kv::change_journal::RangeChangeEventInput;
use alopex_core::kv::{
    KVStore, KVTransaction, RangeChangeRecord, decode_range_change, journal_key,
};
use alopex_core::types::TxnMode;

/// An immutable SQL change record observed from a fresh committed snapshot.
/// `schema_version=None` is an explicit absence of catalog-delta evidence, not
/// a claim that the change is a schema event or that it has a known schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedSqlChangeJournal {
    pub record: RangeChangeRecord,
    pub payloads: Vec<RangeChangeEventInput>,
    pub schema_version: Option<u64>,
    pub data_epoch: u64,
}

impl CommittedSqlChangeJournal {
    /// Reads and verifies the exact record after its enclosing SQL/KV commit.
    /// The expected record must be found byte-for-byte equivalent at its
    /// durable outbox key; otherwise it is not valid changefeed input.
    pub fn load<S: KVStore>(
        store: &S,
        expected: &RangeChangeRecord,
        schema_version: Option<u64>,
    ) -> Result<Self, ChangefeedJournalError> {
        let key = journal_key(expected);
        let mut transaction = store.begin(TxnMode::ReadOnly)?;
        let persisted = transaction.get(&key);
        let close_result = transaction.rollback_self();
        let persisted = persisted?;
        close_result?;
        let Some(value) = persisted else {
            return Err(ChangefeedJournalError::NotCommitted {
                range_id: expected.range_id.clone(),
                generation: expected.generation,
                epoch: expected.epoch,
            });
        };
        let record = decode_range_change(&value)?;
        if record != *expected {
            return Err(ChangefeedJournalError::RecordMismatch);
        }
        let payloads = record.changefeed_inputs()?;
        Ok(Self {
            data_epoch: record.epoch,
            record,
            payloads,
            schema_version,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChangefeedJournalError {
    #[error(
        "range-change record is not visible from a committed snapshot: range={range_id} generation={generation} epoch={epoch}"
    )]
    NotCommitted {
        range_id: String,
        generation: u64,
        epoch: u64,
    },
    #[error("committed range-change record does not match the expected immutable record")]
    RecordMismatch,
    #[error(transparent)]
    Core(#[from] alopex_core::Error),
}

#[cfg(test)]
mod tests {
    use alopex_core::kv::memory::MemoryKV;
    use alopex_core::kv::{KVStore, KVTransaction, RangeChangePayload, stage_range_change};
    use alopex_core::types::TxnMode;

    use super::{ChangefeedJournalError, CommittedSqlChangeJournal};
    use alopex_core::kv::RangeChangeRecord;
    use alopex_core::kv::change_journal::RangeChangeEventInput;

    fn record() -> RangeChangeRecord {
        RangeChangeRecord {
            range_id: "range-a".to_string(),
            generation: 3,
            epoch: 8,
            predecessor_epoch: Some(7),
            replay_id: "replay-8".to_string(),
            payload: vec![
                RangeChangePayload::UpsertRow {
                    row_key: b"row-a".to_vec(),
                    encoded_row: b"post-image".to_vec(),
                },
                RangeChangePayload::DeleteRow {
                    row_key: b"row-b".to_vec(),
                    tombstone: b"pre-image".to_vec(),
                },
                RangeChangePayload::DeleteIndex {
                    index_id: 7,
                    index_key: b"index-a".to_vec(),
                    row_key: b"row-a".to_vec(),
                },
            ],
        }
    }

    #[test]
    fn only_a_fresh_committed_snapshot_is_changefeed_input() {
        let store = MemoryKV::new();
        let record = record();
        assert!(matches!(
            CommittedSqlChangeJournal::load(&store, &record, None),
            Err(ChangefeedJournalError::NotCommitted { .. })
        ));

        let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
        stage_range_change(&mut transaction, &record).unwrap();
        transaction.commit_self().unwrap();

        let committed = CommittedSqlChangeJournal::load(&store, &record, None).unwrap();
        assert_eq!(committed.record, record);
        assert_eq!(committed.data_epoch, 8);
        assert_eq!(committed.schema_version, None);
        assert!(matches!(
            &committed.payloads[0],
            RangeChangeEventInput::UnclassifiedUpsertRow {
                payload_ordinal: 0,
                ..
            }
        ));
        assert!(matches!(
            &committed.payloads[1],
            RangeChangeEventInput::DeleteRow {
                payload_ordinal: 1,
                tombstone,
                ..
            } if tombstone == b"pre-image"
        ));
        assert!(matches!(
            &committed.payloads[2],
            RangeChangeEventInput::DeleteIndex {
                payload_ordinal: 2,
                index_id: 7,
                ..
            }
        ));
    }

    #[test]
    fn failed_commit_never_becomes_changefeed_input() {
        let store = MemoryKV::new_with_limit(Some(0));
        let record = record();
        let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
        stage_range_change(&mut transaction, &record).unwrap();
        assert!(transaction.commit_self().is_err());

        assert!(matches!(
            CommittedSqlChangeJournal::load(&store, &record, Some(4)),
            Err(ChangefeedJournalError::NotCommitted { .. })
        ));
    }
}
