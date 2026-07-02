//! Partition-aware DataFrame scan primitives.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use crate::columnar::encoding::Column;
use crate::columnar::segment_v2::{RecordBatch, SegmentReaderV2};
use crate::{Error, Result};

/// A typed value used to compare partition keys.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PartitionValue {
    /// SQL-style NULL key component.
    Null,
    /// Signed 64-bit integer key component.
    Int64(i64),
    /// 32-bit floating-point key component, stored by raw bits for total equality.
    Float32(u32),
    /// 64-bit floating-point key component, stored by raw bits for total equality.
    Float64(u64),
    /// Boolean key component.
    Bool(bool),
    /// Variable-length binary key component.
    Binary(Vec<u8>),
    /// Fixed-length binary key component.
    Fixed(Vec<u8>),
}

/// A composite partition key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    values: Vec<PartitionValue>,
}

impl PartitionKey {
    /// Create a partition key from key components.
    pub fn new(values: Vec<PartitionValue>) -> Self {
        Self { values }
    }

    /// Return the key components in partition-column order.
    pub fn values(&self) -> &[PartitionValue] {
        &self.values
    }

    fn from_batch_row(batch: &RecordBatch, key_columns: &[usize], row_idx: usize) -> Result<Self> {
        let mut values = Vec::with_capacity(key_columns.len());
        for &column_idx in key_columns {
            let column = batch
                .columns
                .get(column_idx)
                .ok_or_else(|| Error::InvalidParameter {
                    param: "key_columns".into(),
                    reason: format!("column index {column_idx} is out of bounds"),
                })?;
            let bitmap = batch
                .null_bitmaps
                .get(column_idx)
                .and_then(|bitmap| bitmap.as_ref());
            if let Some(bitmap) = bitmap {
                if !bitmap.get(row_idx) {
                    values.push(PartitionValue::Null);
                    continue;
                }
            }
            values.push(partition_value(column, row_idx)?);
        }
        Ok(Self::new(values))
    }
}

/// A contiguous chunk of rows belonging to one logical partition.
#[derive(Clone, Debug)]
pub struct PartitionChunk {
    /// Partition key for this chunk.
    pub key: PartitionKey,
    /// Shared record batch containing the rows.
    pub batch: Arc<RecordBatch>,
    /// Row range in `batch` belonging to this chunk.
    pub rows: Range<usize>,
    /// Zero-based ordinal of the logical partition in input order.
    pub partition_ordinal: usize,
    /// True when this chunk starts a logical partition.
    pub is_partition_start: bool,
    /// True when this chunk ends a logical partition.
    pub is_partition_end: bool,
}

impl PartitionChunk {
    /// Return the number of rows in this chunk.
    pub fn row_count(&self) -> usize {
        self.rows.end.saturating_sub(self.rows.start)
    }
}

#[derive(Clone)]
struct RawChunk {
    key: PartitionKey,
    batch: Arc<RecordBatch>,
    rows: Range<usize>,
}

/// Source of projected columnar batches for partition scanning.
pub trait BatchSource {
    /// Return the next record batch, or `None` when exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// In-memory batch source useful for callers that already own batches.
#[derive(Clone, Debug)]
pub struct VecBatchSource {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl VecBatchSource {
    /// Create a source from pre-built batches.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches: batches.into_iter(),
        }
    }
}

impl BatchSource for VecBatchSource {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

/// Row-group source backed by [`SegmentReaderV2`].
pub struct SegmentBatchSource<'a> {
    reader: &'a SegmentReaderV2,
    columns: Vec<usize>,
    row_group_idx: usize,
}

impl<'a> SegmentBatchSource<'a> {
    /// Create a row-group source that reads only `columns` from `reader`.
    pub fn new(reader: &'a SegmentReaderV2, columns: Vec<usize>) -> Self {
        Self {
            reader,
            columns,
            row_group_idx: 0,
        }
    }
}

impl BatchSource for SegmentBatchSource<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.row_group_idx >= self.reader.row_group_count() {
            return Ok(None);
        }
        let batch = self
            .reader
            .read_row_group_by_index(&self.columns, self.row_group_idx)?;
        self.row_group_idx += 1;
        Ok(Some(batch))
    }
}

/// Streaming scanner that splits sorted input batches into partition chunks.
pub struct PartitionScanner<S> {
    source: S,
    key_columns: Vec<usize>,
    current_batch: Option<Arc<RecordBatch>>,
    row_idx: usize,
    lookahead: Option<RawChunk>,
    previous_key: Option<PartitionKey>,
    completed_partition_keys: HashSet<PartitionKey>,
    active_partition_ordinal: Option<usize>,
    next_partition_ordinal: usize,
}

impl<S: BatchSource> PartitionScanner<S> {
    /// Create a scanner over batches that are already ordered by partition key.
    ///
    /// `key_columns` are indexes in each projected [`RecordBatch`]. An empty
    /// key list treats the full input stream as a single partition.
    pub fn new(source: S, key_columns: Vec<usize>) -> Result<Self> {
        Ok(Self {
            source,
            key_columns,
            current_batch: None,
            row_idx: 0,
            lookahead: None,
            previous_key: None,
            completed_partition_keys: HashSet::new(),
            active_partition_ordinal: None,
            next_partition_ordinal: 0,
        })
    }

    /// Return the next partition chunk with start/end boundary markers.
    pub fn next_chunk(&mut self) -> Result<Option<PartitionChunk>> {
        let current = match self.take_next_raw_chunk()? {
            Some(chunk) => chunk,
            None => return Ok(None),
        };
        let next = self.read_raw_chunk()?;

        let is_partition_start = self.previous_key.as_ref() != Some(&current.key);
        let is_partition_end = next.as_ref().is_none_or(|chunk| chunk.key != current.key);
        if is_partition_start && self.completed_partition_keys.contains(&current.key) {
            return Err(Error::InvalidFormat(
                "partition key reappeared after its partition ended; input must be sorted by partition key"
                    .into(),
            ));
        }
        let partition_ordinal = if is_partition_start {
            self.next_partition_ordinal
        } else {
            self.active_partition_ordinal
                .expect("continued partition has an active ordinal")
        };

        self.previous_key = Some(current.key.clone());
        if is_partition_end {
            // Policy A: the scanner is streaming and requires partition-key-sorted input.
            self.completed_partition_keys.insert(current.key.clone());
            self.next_partition_ordinal = partition_ordinal.saturating_add(1);
            self.active_partition_ordinal = None;
        } else {
            self.active_partition_ordinal = Some(partition_ordinal);
        }
        self.lookahead = next;

        Ok(Some(PartitionChunk {
            key: current.key,
            batch: current.batch,
            rows: current.rows,
            partition_ordinal,
            is_partition_start,
            is_partition_end,
        }))
    }

    /// Invoke `visit` once per logical partition.
    ///
    /// The partition view owns chunk descriptors. Row data remains shared with
    /// the underlying batches, so callers can evaluate window or rolling logic
    /// without copying whole partitions.
    pub fn for_each_partition<F>(&mut self, mut visit: F) -> Result<()>
    where
        F: FnMut(Partition<'_>) -> Result<()>,
    {
        let mut chunks = Vec::new();
        while let Some(chunk) = self.next_chunk()? {
            chunks.push(chunk);
            if chunks.last().is_some_and(|chunk| chunk.is_partition_end) {
                let key = chunks
                    .first()
                    .expect("partition has at least one chunk")
                    .key
                    .clone();
                let partition_ordinal = chunks
                    .first()
                    .expect("partition has at least one chunk")
                    .partition_ordinal;
                visit(Partition {
                    key,
                    partition_ordinal,
                    chunks: &chunks,
                })?;
                chunks.clear();
            }
        }
        Ok(())
    }

    fn take_next_raw_chunk(&mut self) -> Result<Option<RawChunk>> {
        if self.lookahead.is_some() {
            return Ok(self.lookahead.take());
        }
        self.read_raw_chunk()
    }

    fn read_raw_chunk(&mut self) -> Result<Option<RawChunk>> {
        loop {
            self.ensure_current_batch()?;
            let batch = match &self.current_batch {
                Some(batch) => Arc::clone(batch),
                None => return Ok(None),
            };
            let row_count = batch.num_rows();
            if self.row_idx >= row_count {
                self.current_batch = None;
                self.row_idx = 0;
                continue;
            }

            validate_batch(&batch, &self.key_columns)?;
            let start = self.row_idx;
            let key = PartitionKey::from_batch_row(&batch, &self.key_columns, start)?;
            self.row_idx += 1;
            while self.row_idx < row_count
                && PartitionKey::from_batch_row(&batch, &self.key_columns, self.row_idx)? == key
            {
                self.row_idx += 1;
            }
            return Ok(Some(RawChunk {
                key,
                batch,
                rows: start..self.row_idx,
            }));
        }
    }

    fn ensure_current_batch(&mut self) -> Result<()> {
        while self.current_batch.is_none() {
            let Some(batch) = self.source.next_batch()? else {
                return Ok(());
            };
            validate_batch(&batch, &self.key_columns)?;
            if batch.num_rows() == 0 {
                continue;
            }
            self.current_batch = Some(Arc::new(batch));
            self.row_idx = 0;
        }
        Ok(())
    }
}

/// Borrowed view of one logical partition.
pub struct Partition<'a> {
    key: PartitionKey,
    partition_ordinal: usize,
    chunks: &'a [PartitionChunk],
}

impl<'a> Partition<'a> {
    /// Return the partition key.
    pub fn key(&self) -> &PartitionKey {
        &self.key
    }

    /// Return the partition ordinal in input order.
    pub fn partition_ordinal(&self) -> usize {
        self.partition_ordinal
    }

    /// Return chunks belonging to the partition.
    pub fn chunks(&self) -> &'a [PartitionChunk] {
        self.chunks
    }
}

fn validate_batch(batch: &RecordBatch, key_columns: &[usize]) -> Result<()> {
    let row_count = batch.num_rows();
    for (column_idx, column) in batch.columns.iter().enumerate() {
        let len = column_len(column);
        if len != row_count {
            return Err(Error::InvalidFormat(format!(
                "record batch column {column_idx} has {len} rows, expected {row_count}"
            )));
        }
    }
    for &column_idx in key_columns {
        if column_idx >= batch.columns.len() {
            return Err(Error::InvalidParameter {
                param: "key_columns".into(),
                reason: format!("column index {column_idx} is out of bounds"),
            });
        }
    }
    Ok(())
}

fn partition_value(column: &Column, row_idx: usize) -> Result<PartitionValue> {
    match column {
        Column::Int64(values) => values
            .get(row_idx)
            .copied()
            .map(PartitionValue::Int64)
            .ok_or_else(row_out_of_bounds),
        Column::Float32(values) => values
            .get(row_idx)
            .map(|value| PartitionValue::Float32(value.to_bits()))
            .ok_or_else(row_out_of_bounds),
        Column::Float64(values) => values
            .get(row_idx)
            .map(|value| PartitionValue::Float64(value.to_bits()))
            .ok_or_else(row_out_of_bounds),
        Column::Bool(values) => values
            .get(row_idx)
            .copied()
            .map(PartitionValue::Bool)
            .ok_or_else(row_out_of_bounds),
        Column::Binary(values) => values
            .get(row_idx)
            .cloned()
            .map(PartitionValue::Binary)
            .ok_or_else(row_out_of_bounds),
        Column::Fixed { values, .. } => values
            .get(row_idx)
            .cloned()
            .map(PartitionValue::Fixed)
            .ok_or_else(row_out_of_bounds),
    }
}

fn row_out_of_bounds() -> Error {
    Error::InvalidFormat("row index out of bounds".into())
}

fn column_len(column: &Column) -> usize {
    match column {
        Column::Int64(values) => values.len(),
        Column::Float32(values) => values.len(),
        Column::Float64(values) => values.len(),
        Column::Bool(values) => values.len(),
        Column::Binary(values) => values.len(),
        Column::Fixed { values, .. } => values.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::encoding::{Column, LogicalType};
    use crate::columnar::segment_v2::{
        ColumnSchema, InMemorySegmentSource, RecordBatch, Schema, SegmentConfigV2, SegmentReaderV2,
        SegmentWriterV2,
    };

    fn schema() -> Schema {
        Schema {
            columns: vec![
                ColumnSchema {
                    name: "partition_key".into(),
                    logical_type: LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
                ColumnSchema {
                    name: "value".into(),
                    logical_type: LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
            ],
        }
    }

    fn batch(keys: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        RecordBatch::new(
            schema(),
            vec![Column::Int64(keys), Column::Int64(values)],
            vec![None, None],
        )
    }

    fn chunk_values(chunk: &PartitionChunk) -> Vec<i64> {
        let Column::Int64(values) = &chunk.batch.columns[1] else {
            panic!("expected int64 values");
        };
        values[chunk.rows.clone()].to_vec()
    }

    fn key_i64(chunk: &PartitionChunk) -> i64 {
        match &chunk.key.values()[0] {
            PartitionValue::Int64(value) => *value,
            other => panic!("expected int64 key, got {other:?}"),
        }
    }

    #[test]
    fn preserves_partition_boundaries_and_order_across_batches() {
        let source = VecBatchSource::new(vec![
            batch(vec![1, 1, 2], vec![10, 11, 20]),
            batch(vec![2, 3], vec![21, 30]),
        ]);
        let mut scan = PartitionScanner::new(source, vec![0]).unwrap();

        let mut chunks = Vec::new();
        while let Some(chunk) = scan.next_chunk().unwrap() {
            chunks.push(chunk);
        }

        assert_eq!(chunks.len(), 4);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| {
                    (
                        key_i64(chunk),
                        chunk.rows.clone(),
                        chunk.is_partition_start,
                        chunk.is_partition_end,
                        chunk.partition_ordinal,
                        chunk_values(chunk),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (1, 0..2, true, true, 0, vec![10, 11]),
                (2, 2..3, true, false, 1, vec![20]),
                (2, 0..1, false, true, 1, vec![21]),
                (3, 1..2, true, true, 2, vec![30]),
            ]
        );
    }

    #[test]
    fn skips_empty_batches_without_emitting_empty_chunks() {
        let source = VecBatchSource::new(vec![
            batch(Vec::new(), Vec::new()),
            batch(vec![7], vec![70]),
            batch(Vec::new(), Vec::new()),
        ]);
        let mut scan = PartitionScanner::new(source, vec![0]).unwrap();

        let first = scan.next_chunk().unwrap().expect("non-empty partition");
        assert_eq!(key_i64(&first), 7);
        assert_eq!(chunk_values(&first), vec![70]);
        assert!(first.is_partition_start);
        assert!(first.is_partition_end);
        assert!(scan.next_chunk().unwrap().is_none());

        let mut empty =
            PartitionScanner::new(VecBatchSource::new(vec![batch(vec![], vec![])]), vec![0])
                .unwrap();
        assert!(empty.next_chunk().unwrap().is_none());
    }

    #[test]
    fn streams_skewed_partition_sizes_without_losing_boundaries() {
        let mut batches = vec![batch(vec![1], vec![10])];
        for offset in (0..10_000).step_by(1024) {
            let len = (10_000 - offset).min(1024);
            batches.push(batch(
                vec![2; len],
                (offset..offset + len).map(|v| v as i64).collect(),
            ));
        }
        batches.push(batch(vec![3], vec![30]));

        let mut scan = PartitionScanner::new(VecBatchSource::new(batches), vec![0]).unwrap();
        let mut seen = Vec::new();
        while let Some(chunk) = scan.next_chunk().unwrap() {
            seen.push((
                key_i64(&chunk),
                chunk.row_count(),
                chunk.is_partition_start,
                chunk.is_partition_end,
                chunk.partition_ordinal,
            ));
        }

        assert_eq!(seen.first(), Some(&(1, 1, true, true, 0)));
        assert_eq!(seen.last(), Some(&(3, 1, true, true, 2)));
        let skewed_rows: usize = seen
            .iter()
            .filter(|(key, _, _, _, _)| *key == 2)
            .map(|(_, rows, _, _, _)| *rows)
            .sum();
        assert_eq!(skewed_rows, 10_000);
        assert_eq!(
            seen.iter()
                .filter(|(key, _, is_start, _, _)| *key == 2 && *is_start)
                .count(),
            1
        );
        assert_eq!(
            seen.iter()
                .filter(|(key, _, _, is_end, _)| *key == 2 && *is_end)
                .count(),
            1
        );
    }

    #[test]
    fn handles_thousands_of_partitions_in_input_order() {
        let partition_count = 5_000usize;
        let batches = (0..partition_count)
            .collect::<Vec<_>>()
            .chunks(257)
            .map(|chunk| {
                batch(
                    chunk.iter().map(|v| *v as i64).collect(),
                    chunk.iter().map(|v| (*v as i64) * 10).collect(),
                )
            })
            .collect::<Vec<_>>();

        let mut scan = PartitionScanner::new(VecBatchSource::new(batches), vec![0]).unwrap();
        let mut count = 0usize;
        while let Some(chunk) = scan.next_chunk().unwrap() {
            assert_eq!(key_i64(&chunk), count as i64);
            assert_eq!(chunk.partition_ordinal, count);
            assert!(chunk.is_partition_start);
            assert!(chunk.is_partition_end);
            count += 1;
        }

        assert_eq!(count, partition_count);
    }

    #[test]
    fn rejects_non_contiguous_partition_key_reappearance() {
        let source = VecBatchSource::new(vec![batch(vec![1, 1, 2, 1], vec![10, 11, 20, 12])]);
        let mut scan = PartitionScanner::new(source, vec![0]).unwrap();

        assert!(scan.next_chunk().unwrap().is_some());
        assert!(scan.next_chunk().unwrap().is_some());

        match scan.next_chunk().unwrap_err() {
            Error::InvalidFormat(message) => {
                assert!(message.contains("partition key"));
                assert!(message.contains("sorted"));
            }
            other => panic!("expected InvalidFormat, got {other:?}"),
        }
    }

    #[test]
    fn reads_partitions_from_segment_reader_row_groups() {
        let config = SegmentConfigV2 {
            row_group_size: 2,
            ..SegmentConfigV2::default()
        };
        let mut writer = SegmentWriterV2::new(config);
        writer
            .write_batch(batch(vec![1, 1, 2, 2, 3], vec![10, 11, 20, 21, 30]))
            .unwrap();
        let segment = writer.finish().unwrap();
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data))).unwrap();

        let source = SegmentBatchSource::new(&reader, vec![0, 1]);
        let mut scan = PartitionScanner::new(source, vec![0]).unwrap();
        let mut partitions = Vec::new();
        scan.for_each_partition(|partition| {
            let row_count: usize = partition
                .chunks()
                .iter()
                .map(PartitionChunk::row_count)
                .sum();
            partitions.push((
                partition.key().clone(),
                partition.partition_ordinal(),
                row_count,
            ));
            Ok(())
        })
        .unwrap();

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].0.values(), &[PartitionValue::Int64(1)]);
        assert_eq!(partitions[0].1, 0);
        assert_eq!(partitions[0].2, 2);
        assert_eq!(partitions[1].0.values(), &[PartitionValue::Int64(2)]);
        assert_eq!(partitions[1].1, 1);
        assert_eq!(partitions[1].2, 2);
        assert_eq!(partitions[2].0.values(), &[PartitionValue::Int64(3)]);
        assert_eq!(partitions[2].1, 2);
        assert_eq!(partitions[2].2, 1);
    }
}
