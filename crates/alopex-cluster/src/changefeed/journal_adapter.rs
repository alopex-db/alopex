use alopex_core::kv::RangeChangeRecord;
use alopex_core::kv::change_journal::RangeChangeEventInput;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{FailureClass, IdempotencyResult, OperationState, RoutingOutcome};

use super::{
    ChangeEventEnvelope, ChangeOperationType, ChangePayload, ChangefeedModelError, Checkpoint,
    CursorError, EventIdentity, FeedIdentity,
};

/// Converts committed, storage-neutral range-change records into canonical
/// public event envelopes. It has no persistence or transport responsibility.
#[derive(Debug, Default)]
pub struct JournalEventAdapter;

impl JournalEventAdapter {
    /// Adapts every source payload only when the record matches the committed
    /// feed range/generation/data epoch. A non-adaptable payload fails the
    /// entire record so a caller cannot silently deliver a partial commit.
    pub fn adapt(
        &self,
        feed: &FeedIdentity,
        routing: &RoutingOutcome,
        record: &RangeChangeRecord,
    ) -> Result<Vec<ChangeEventEnvelope>, JournalAdapterError> {
        feed.validate().map_err(JournalAdapterError::Model)?;
        validate_source(feed, record)?;
        record
            .changefeed_inputs()
            .map_err(JournalAdapterError::Core)?
            .iter()
            .map(|input| self.adapt_input(feed, routing, record, input))
            .collect()
    }

    /// Adapts one source payload while retaining its stable ordinal. This is
    /// public for a delivery coordinator which has already recorded a complete
    /// rejection for any sibling payload in the same source commit.
    pub fn adapt_input(
        &self,
        feed: &FeedIdentity,
        routing: &RoutingOutcome,
        record: &RangeChangeRecord,
        input: &RangeChangeEventInput,
    ) -> Result<ChangeEventEnvelope, JournalAdapterError> {
        validate_source(feed, record)?;
        match input {
            RangeChangeEventInput::UnclassifiedUpsertRow {
                payload_ordinal, ..
            } => Err(JournalAdapterError::PayloadUnavailable {
                payload_ordinal: *payload_ordinal,
                reason_code: "operation_type_unattributable",
            }),
            RangeChangeEventInput::UpsertIndex {
                payload_ordinal, ..
            } => Err(JournalAdapterError::PayloadUnavailable {
                payload_ordinal: *payload_ordinal,
                reason_code: "index_operation_unattributable",
            }),
            RangeChangeEventInput::DeleteRow {
                payload_ordinal,
                row_key,
                tombstone,
            } => self.event(
                feed,
                routing,
                record,
                *payload_ordinal,
                ChangeOperationType::Delete,
                row_key,
                ChangePayload::available(tombstone.clone()),
            ),
            RangeChangeEventInput::DeleteIndex {
                payload_ordinal,
                index_id,
                index_key,
                row_key,
            } => {
                let payload = serde_json::to_vec(&IndexTombstonePayload {
                    payload_type: "index_tombstone",
                    index_id: *index_id,
                    index_key,
                    row_key,
                })
                .map_err(JournalAdapterError::Serialize)?;
                self.event(
                    feed,
                    routing,
                    record,
                    *payload_ordinal,
                    ChangeOperationType::Tombstone,
                    index_key,
                    ChangePayload::available(payload),
                )
            }
        }
    }

    /// Schema/catalog records have no source journal evidence in the current
    /// contract and must be rejected before a transport can advertise them.
    pub const fn reject_schema(&self) -> JournalAdapterError {
        JournalAdapterError::UnsupportedChangeKind {
            reason_code: "schema_unsupported",
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn event(
        &self,
        feed: &FeedIdentity,
        routing: &RoutingOutcome,
        record: &RangeChangeRecord,
        payload_ordinal: u32,
        operation_type: ChangeOperationType,
        key: &[u8],
        payload: ChangePayload,
    ) -> Result<ChangeEventEnvelope, JournalAdapterError> {
        let event_id = EventIdentity::new(
            feed.range.cluster_id.clone(),
            record.range_id.clone(),
            record.generation,
            record.epoch,
            record.replay_id.clone(),
            payload_ordinal,
        )
        .map_err(JournalAdapterError::Cursor)?
        .event_id()
        .map_err(JournalAdapterError::Cursor)?;
        let checkpoint = Checkpoint::new(
            feed.feed_id.clone(),
            record.range_id.clone(),
            record.generation,
            record.epoch,
            payload_ordinal,
            record.epoch,
            feed.retention.deadline_epoch,
        )
        .map_err(JournalAdapterError::Model)?;
        let request_id = crate::RequestId::from(record.replay_id.clone());
        ChangeEventEnvelope::new(
            event_id,
            feed.feed_id.clone(),
            feed.range.clone(),
            record.generation,
            record.replay_id.clone(),
            request_id.clone(),
            record.epoch,
            payload_ordinal,
            operation_type,
            hex_digest(key),
            payload,
            checkpoint,
            OperationState::Committed,
            None,
            None,
            routing.clone(),
            false,
            IdempotencyResult {
                operation_id: record.replay_id.clone(),
                request_id,
                first_outcome: "committed".to_string(),
                state: OperationState::Committed,
                duplicate_count: 0,
            },
        )
        .map_err(JournalAdapterError::Model)
    }
}

fn validate_source(
    feed: &FeedIdentity,
    record: &RangeChangeRecord,
) -> Result<(), JournalAdapterError> {
    if feed.range.range_id.as_str() != record.range_id {
        return Err(JournalAdapterError::SourceMismatch { field: "range_id" });
    }
    if feed.generation != record.generation {
        return Err(JournalAdapterError::SourceMismatch {
            field: "generation",
        });
    }
    if feed.range.data_epoch != record.epoch {
        return Err(JournalAdapterError::SourceMismatch {
            field: "data_epoch",
        });
    }
    Ok(())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Serialize)]
struct IndexTombstonePayload<'a> {
    payload_type: &'static str,
    index_id: u32,
    index_key: &'a [u8],
    row_key: &'a [u8],
}

#[derive(Debug, thiserror::Error)]
pub enum JournalAdapterError {
    #[error("journal source does not match the feed {field}")]
    SourceMismatch { field: &'static str },
    #[error("journal payload {payload_ordinal} is unavailable: {reason_code}")]
    PayloadUnavailable {
        payload_ordinal: u32,
        reason_code: &'static str,
    },
    #[error("journal change kind is unsupported: {reason_code}")]
    UnsupportedChangeKind { reason_code: &'static str },
    #[error("cannot serialize auxiliary index tombstone: {0}")]
    Serialize(serde_json::Error),
    #[error(transparent)]
    Core(alopex_core::Error),
    #[error(transparent)]
    Cursor(CursorError),
    #[error(transparent)]
    Model(ChangefeedModelError),
}

impl JournalAdapterError {
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            Self::SourceMismatch { .. } => FailureClass::StaleMetadata,
            Self::PayloadUnavailable { .. } | Self::UnsupportedChangeKind { .. } => {
                FailureClass::InvalidRequest
            }
            Self::Serialize(_) | Self::Core(_) | Self::Cursor(_) | Self::Model(_) => {
                FailureClass::Internal
            }
        }
    }

    pub const fn reason_code(&self) -> &'static str {
        match self {
            Self::SourceMismatch { .. } => "journal_source_mismatch",
            Self::PayloadUnavailable { reason_code, .. }
            | Self::UnsupportedChangeKind { reason_code } => reason_code,
            Self::Serialize(_) | Self::Core(_) | Self::Cursor(_) | Self::Model(_) => {
                "journal_adapter_internal"
            }
        }
    }
}
