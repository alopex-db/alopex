//! Bounded adapter for provisioned V08 columnar segment cursors.
//!
//! The concrete V08/KVS implementation lives in `alopex-embedded`, which
//! depends on this crate.  Keeping the cursor trait in `physical::streaming`
//! avoids a reverse dependency while retaining the same ownership and terminal
//! rules as CSV and Parquet sources.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::physical::budget::{ResourceReservation, ResourceScope};
use crate::physical::{
    record_batch_memory_size, BatchOpenContext, BatchSource, BatchSourceFactory,
    ColumnarSegmentCursor, ColumnarSegmentProvider, SourceLimit, StreamBatch,
};
use crate::{DataFrameError, Result};

/// Re-consumable bounded source factory over an adapter-provided V08 cursor.
#[derive(Clone)]
pub struct ColumnarSegmentBatchSourceFactory {
    provider: Arc<dyn ColumnarSegmentProvider>,
}

impl fmt::Debug for ColumnarSegmentBatchSourceFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColumnarSegmentBatchSourceFactory")
            .field("source_name", &self.provider.source_name())
            .finish_non_exhaustive()
    }
}

impl ColumnarSegmentBatchSourceFactory {
    /// Creates a factory from a read-only V08 segment provider.
    pub fn new(provider: Arc<dyn ColumnarSegmentProvider>) -> Self {
        Self { provider }
    }
}

impl BatchSourceFactory for ColumnarSegmentBatchSourceFactory {
    fn source_name(&self) -> &'static str {
        self.provider.source_name()
    }

    fn schema(&self) -> Result<SchemaRef> {
        Err(DataFrameError::streaming_unsupported(
            "columnar_segment_scan",
            "schema_is_available_after_bounded_open",
        ))
    }

    fn source_limits(&self) -> Vec<SourceLimit> {
        self.provider.source_limits()
    }

    fn open(&self, context: BatchOpenContext) -> Result<Box<dyn BatchSource>> {
        // Opening a V08 cursor deserializes bounded header/schema/directory
        // objects. Reserve the complete configured limit before that opaque
        // allocation, then retain only the cursor's conservative footprint.
        let mut metadata_reservation = context
            .budget
            .reserve(ResourceScope::Source, context.options.memory_limit_bytes)?;
        let cursor = self.provider.open(context.options.memory_limit_bytes)?;
        let retained_metadata = cursor.metadata_footprint_upper_bound();
        if retained_metadata > context.options.memory_limit_bytes {
            return Err(DataFrameError::resource_limit_exceeded(
                context.options.memory_limit_bytes,
                retained_metadata,
                context.budget.max_in_flight_batches(),
                context.budget.usage().reserved_batches,
                ResourceScope::Source,
            ));
        }
        metadata_reservation.shrink_to(retained_metadata);
        let schema = cursor.schema();

        Ok(Box::new(ColumnarSegmentBatchSource {
            context,
            cursor,
            schema,
            metadata_reservation: Some(metadata_reservation),
        }))
    }
}

struct ColumnarSegmentBatchSource {
    context: BatchOpenContext,
    cursor: Box<dyn ColumnarSegmentCursor>,
    schema: SchemaRef,
    metadata_reservation: Option<ResourceReservation>,
}

impl BatchSource for ColumnarSegmentBatchSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<StreamBatch>> {
        let Some(plan) = self.cursor.next_batch_plan()? else {
            return Ok(None);
        };
        let reservation_upper_bound = plan.reservation_upper_bound();
        let reservation = self
            .context
            .budget
            .reserve_batch(ResourceScope::Decode, reservation_upper_bound)?;
        let Some(batch) = self.cursor.next_batch()? else {
            drop(reservation);
            return Err(DataFrameError::schema_mismatch(
                "V08 cursor returned no batch after declaring a batch plan",
            ));
        };

        let actual_arrow_bytes = record_batch_memory_size(&batch);
        if actual_arrow_bytes > plan.arrow_allocation_upper_bound {
            drop(reservation);
            return Err(DataFrameError::resource_limit_exceeded(
                self.context.budget.memory_limit_bytes(),
                self.context
                    .budget
                    .usage()
                    .reserved_bytes
                    .saturating_add(actual_arrow_bytes),
                self.context.budget.max_in_flight_batches(),
                self.context.budget.usage().reserved_batches,
                ResourceScope::Decode,
            ));
        }

        Ok(Some(StreamBatch::new(batch, reservation)))
    }

    fn close(&mut self) -> Result<()> {
        let result = self.cursor.close();
        self.metadata_reservation.take();
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::physical::{
        ColumnarSegmentBatchPlan, ColumnarSegmentCursor, ColumnarSegmentProvider, DataFrameStream,
        PlanSubject, SourceLimit,
    };
    use crate::{DataFrameError, Result};

    #[derive(Clone)]
    struct TestProvider {
        batches: Vec<RecordBatch>,
        plan: ColumnarSegmentBatchPlan,
        metadata_bytes: u64,
        next_calls: Arc<AtomicUsize>,
        close_calls: Arc<AtomicUsize>,
    }

    impl ColumnarSegmentProvider for TestProvider {
        fn source_limits(&self) -> Vec<SourceLimit> {
            vec![SourceLimit {
                source: PlanSubject::ColumnarSegmentScan,
                code: "requires_v08_chunked_layout",
                description: "legacy V2 segment blobs are not streaming sources",
            }]
        }

        fn open(&self, _metadata_limit_bytes: u64) -> Result<Box<dyn ColumnarSegmentCursor>> {
            Ok(Box::new(TestCursor {
                batches: self.batches.clone().into(),
                plan: self.plan,
                metadata_bytes: self.metadata_bytes,
                next_calls: self.next_calls.clone(),
                close_calls: self.close_calls.clone(),
            }))
        }
    }

    struct TestCursor {
        batches: VecDeque<RecordBatch>,
        plan: ColumnarSegmentBatchPlan,
        metadata_bytes: u64,
        next_calls: Arc<AtomicUsize>,
        close_calls: Arc<AtomicUsize>,
    }

    impl ColumnarSegmentCursor for TestCursor {
        fn schema(&self) -> SchemaRef {
            schema()
        }

        fn metadata_footprint_upper_bound(&self) -> u64 {
            self.metadata_bytes
        }

        fn next_batch_plan(&self) -> Result<Option<ColumnarSegmentBatchPlan>> {
            Ok((!self.batches.is_empty()).then_some(self.plan))
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            self.next_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.batches.pop_front())
        }

        fn close(&mut self) -> Result<()> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn batch(value: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
        )
        .unwrap()
    }

    fn options(limit: u64) -> crate::physical::budget::StreamOptions {
        crate::physical::budget::StreamOptions::new(
            limit,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
    }

    #[test]
    fn source_reserves_before_cursor_read_and_releases_on_exhaustion() {
        let next_calls = Arc::new(AtomicUsize::new(0));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let provider = TestProvider {
            batches: vec![batch(1), batch(2)],
            plan: ColumnarSegmentBatchPlan {
                decoded_bytes: 128,
                arrow_allocation_upper_bound: 1024,
            },
            metadata_bytes: 256,
            next_calls: next_calls.clone(),
            close_calls: close_calls.clone(),
        };
        let factory = ColumnarSegmentBatchSourceFactory::new(Arc::new(provider));
        let mut stream = DataFrameStream::from_factory(&factory, options(4096)).unwrap();

        assert_eq!(next_calls.load(Ordering::SeqCst), 0);
        assert_eq!(stream.next_batch().unwrap().unwrap().height(), 1);
        assert_eq!(stream.next_batch().unwrap().unwrap().height(), 1);
        assert!(stream.next_batch().unwrap().is_none());
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(stream.budget().usage().reserved_bytes, 0);
    }

    #[test]
    fn oversized_row_group_is_rejected_before_cursor_read() {
        let next_calls = Arc::new(AtomicUsize::new(0));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let provider = TestProvider {
            batches: vec![batch(1)],
            plan: ColumnarSegmentBatchPlan {
                decoded_bytes: 2048,
                arrow_allocation_upper_bound: 2048,
            },
            metadata_bytes: 128,
            next_calls: next_calls.clone(),
            close_calls: close_calls.clone(),
        };
        let factory = ColumnarSegmentBatchSourceFactory::new(Arc::new(provider));
        let mut stream = DataFrameStream::from_factory(&factory, options(1024)).unwrap();

        assert!(matches!(
            stream.next_batch(),
            Err(DataFrameError::StreamFailed {
                code: "resource_limit_exceeded",
                ..
            })
        ));
        assert_eq!(next_calls.load(Ordering::SeqCst), 0);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }
}
