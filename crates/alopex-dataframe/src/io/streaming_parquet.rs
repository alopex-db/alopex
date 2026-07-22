//! Bounded Parquet source backed by one row-group reader at a time.
//!
//! Metadata is read once under a footer reservation. Each selected row group is then checked
//! against the remaining resource budget before a Parquet reader is built, and every decoded
//! Arrow batch receives its reservation before `next()` is called.

use std::collections::VecDeque;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::io::options::ParquetReadOptions;
use crate::physical::budget::{ResourceReservation, ResourceScope};
use crate::physical::{
    BatchOpenContext, BatchSource, BatchSourceFactory, PlanSubject, SourceLimit, StreamBatch,
};
use crate::{DataFrameError, Result};

/// Re-consumable bounded Parquet source factory.
#[derive(Debug, Clone)]
pub struct ParquetBatchSourceFactory {
    path: PathBuf,
    options: ParquetReadOptions,
}

impl ParquetBatchSourceFactory {
    /// Construct a factory for a Parquet path and read options.
    pub fn new(path: impl AsRef<Path>, options: ParquetReadOptions) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            options,
        }
    }

    /// Construct a factory from a physical Parquet scan.
    pub(crate) fn from_scan(
        path: PathBuf,
        predicate: Option<crate::Expr>,
        columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            path,
            options: ParquetReadOptions {
                columns,
                predicate,
                ..ParquetReadOptions::default()
            },
        }
    }
}

impl BatchSourceFactory for ParquetBatchSourceFactory {
    fn source_name(&self) -> &'static str {
        "parquet"
    }

    fn schema(&self) -> Result<SchemaRef> {
        Err(DataFrameError::streaming_unsupported(
            "parquet_scan",
            "schema_is_available_after_bounded_open",
        ))
    }

    fn source_limits(&self) -> Vec<SourceLimit> {
        vec![
            SourceLimit {
                source: PlanSubject::ParquetScan,
                code: "stable_row_group_order",
                description: "Parquet streaming preserves selected row-group and row order",
            },
            SourceLimit {
                source: PlanSubject::ParquetScan,
                code: "row_group_must_fit_resource_bound",
                description: "A selected row group whose declared upper bound exceeds the resource limit is rejected before page decode",
            },
        ]
    }

    fn open(&self, context: BatchOpenContext) -> Result<Box<dyn BatchSource>> {
        if self.options.predicate.is_some() {
            return Err(DataFrameError::streaming_unsupported(
                "parquet_scan",
                "predicate_batch_operator_not_installed",
            ));
        }

        // `try_new` parses footer metadata. Reserve the complete configured bound first, then
        // shrink to the metadata's actual retained size before any row-group reader is opened.
        let mut footer_reservation = context
            .budget
            .reserve(ResourceScope::Source, context.options.memory_limit_bytes)?;
        let file = File::open(&self.path)
            .map_err(|source| DataFrameError::io_with_path(source, &self.path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|source| DataFrameError::Parquet { source })?;
        let metadata = builder.metadata().clone();
        let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), Default::default())
            .map_err(|source| DataFrameError::Parquet { source })?;
        let full_schema = reader_metadata.schema().clone();
        let projection_indices = projection_indices(&full_schema, self.options.columns.as_deref())?;
        let output_schema = projected_schema(&full_schema, projection_indices.as_deref());
        drop(builder);

        let row_groups =
            selected_row_groups(&metadata, &self.options, projection_indices.as_deref())?;
        // The initial reservation covers opaque footer parsing and all source-open allocations.
        // Retain only their conservative footprint before decoded batches begin reserving bytes.
        footer_reservation.shrink_to(
            parquet_metadata_bytes(&metadata)
                .saturating_add(schema_reservation_bytes(&full_schema))
                .saturating_add(schema_reservation_bytes(&output_schema))
                .saturating_add(
                    u64::try_from(row_groups.len())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(32),
                )
                .saturating_add(
                    u64::try_from(projection_indices.as_ref().map_or(0, Vec::len))
                        .unwrap_or(u64::MAX)
                        .saturating_mul(8),
                ),
        );
        Ok(Box::new(ParquetBatchSource {
            context,
            path: self.path.clone(),
            reader_metadata,
            options: self.options.clone(),
            output_schema,
            projection_indices,
            row_groups,
            current: None,
            footer_reservation: Some(footer_reservation),
        }))
    }
}

struct ParquetBatchSource {
    context: BatchOpenContext,
    path: PathBuf,
    reader_metadata: ArrowReaderMetadata,
    options: ParquetReadOptions,
    output_schema: SchemaRef,
    projection_indices: Option<Vec<usize>>,
    row_groups: VecDeque<RowGroupPlan>,
    current: Option<CurrentRowGroup>,
    footer_reservation: Option<ResourceReservation>,
}

struct RowGroupPlan {
    index: usize,
    decode_upper_bound: u64,
}

struct CurrentRowGroup {
    reader: ParquetRecordBatchReader,
    decode_upper_bound: u64,
}

impl BatchSource for ParquetBatchSource {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<StreamBatch>> {
        loop {
            if self.current.is_none() {
                let Some(plan) = self.row_groups.pop_front() else {
                    return Ok(None);
                };
                self.current = Some(self.open_row_group(plan)?);
            }

            let current = self.current.as_mut().expect("current row group is set");
            let reservation = self
                .context
                .budget
                .reserve_batch(ResourceScope::Decode, current.decode_upper_bound)?;
            match current.reader.next() {
                Some(Ok(batch)) => return Ok(Some(StreamBatch::new(batch, reservation))),
                Some(Err(source)) => return Err(DataFrameError::Arrow { source }),
                None => {
                    drop(reservation);
                    self.current = None;
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.current = None;
        self.footer_reservation.take();
        Ok(())
    }
}

impl ParquetBatchSource {
    fn open_row_group(&self, plan: RowGroupPlan) -> Result<CurrentRowGroup> {
        let footer_bytes = self.context.budget.usage().reserved_bytes;
        let observed = footer_bytes.saturating_add(plan.decode_upper_bound);
        if observed > self.context.budget.memory_limit_bytes() {
            return Err(DataFrameError::resource_limit_exceeded(
                self.context.budget.memory_limit_bytes(),
                observed,
                self.context.budget.max_in_flight_batches(),
                self.context
                    .budget
                    .usage()
                    .reserved_batches
                    .saturating_add(1),
                ResourceScope::Decode,
            ));
        }

        let file = File::open(&self.path)
            .map_err(|source| DataFrameError::io_with_path(source, &self.path))?;
        let mut builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.reader_metadata.clone())
                .with_batch_size(
                    self.options
                        .batch_size
                        .min(self.context.options.batch_rows.get())
                        .max(1),
                )
                .with_row_groups(vec![plan.index]);
        if let Some(indices) = self.projection_indices.as_deref() {
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices.to_vec());
            builder = builder.with_projection(mask);
        }
        let reader = builder
            .build()
            .map_err(|source| DataFrameError::Parquet { source })?;
        Ok(CurrentRowGroup {
            reader,
            decode_upper_bound: plan.decode_upper_bound,
        })
    }
}

fn selected_row_groups(
    metadata: &ParquetMetaData,
    options: &ParquetReadOptions,
    projection: Option<&[usize]>,
) -> Result<VecDeque<RowGroupPlan>> {
    let requested: Vec<usize> = match options.row_groups.as_deref() {
        Some(indices) => indices.to_vec(),
        None => (0..metadata.num_row_groups()).collect(),
    };
    requested
        .into_iter()
        .map(|index| {
            let row_group = metadata.row_groups().get(index).ok_or_else(|| {
                DataFrameError::configuration(
                    "row_groups",
                    format!("row group index {index} is outside the file metadata"),
                )
            })?;
            let decode_upper_bound = selected_row_group_upper_bound(row_group, projection)?;
            Ok(RowGroupPlan {
                index,
                decode_upper_bound,
            })
        })
        .collect()
}

fn selected_row_group_upper_bound(
    row_group: &parquet::file::metadata::RowGroupMetaData,
    projection: Option<&[usize]>,
) -> Result<u64> {
    let selected = match projection {
        Some(indices) => indices
            .iter()
            .map(|index| {
                row_group.columns().get(*index).ok_or_else(|| {
                    DataFrameError::schema_mismatch(
                        "Parquet projection index exceeds row group schema",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?,
        None => row_group.columns().iter().collect(),
    };
    Ok(selected.into_iter().fold(1024_u64, |bound, column| {
        let compressed = u64::try_from(column.compressed_size()).unwrap_or(u64::MAX);
        let uncompressed = u64::try_from(column.uncompressed_size()).unwrap_or(u64::MAX);
        bound.saturating_add(compressed.max(uncompressed))
    }))
}

fn parquet_metadata_bytes(metadata: &ParquetMetaData) -> u64 {
    u64::try_from(metadata.memory_size()).unwrap_or(u64::MAX)
}

fn schema_reservation_bytes(schema: &SchemaRef) -> u64 {
    schema.fields().iter().fold(256_u64, |bytes, field| {
        bytes
            .saturating_add(u64::try_from(field.name().len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(field.data_type().to_string().len()).unwrap_or(u64::MAX))
            .saturating_add(256)
    })
}

fn projection_indices(
    schema: &SchemaRef,
    columns: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    let Some(columns) = columns else {
        return Ok(None);
    };
    columns
        .iter()
        .map(|name| {
            schema
                .fields()
                .iter()
                .position(|field| field.name() == name)
                .ok_or_else(|| DataFrameError::column_not_found(name.clone()))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    let Some(projection) = projection else {
        return schema.clone();
    };
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|index| schema.field(*index).clone())
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::ParquetBatchSourceFactory;
    use crate::io::{write_parquet, ParquetReadOptions};
    use crate::physical::budget::StreamOptions;
    use crate::physical::DataFrameStream;
    use crate::{DataFrame, DataFrameError};

    fn options(memory_limit_bytes: u64, batch_rows: usize) -> StreamOptions {
        StreamOptions::new(
            memory_limit_bytes,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(batch_rows).unwrap(),
        )
    }

    fn parquet_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])) as ArrayRef],
        )
        .unwrap();
        write_parquet(&path, &DataFrame::from_batches(vec![batch]).unwrap()).unwrap();
        (dir, path)
    }

    #[test]
    fn parquet_source_yields_incremental_stable_order_batches() {
        let (_dir, path) = parquet_fixture();
        let factory =
            ParquetBatchSourceFactory::new(&path, ParquetReadOptions::default().with_batch_size(2));
        let mut stream = DataFrameStream::from_factory(&factory, options(64 * 1024, 2)).unwrap();
        let first = stream.next_batch().unwrap().unwrap();
        let second = stream.next_batch().unwrap().unwrap();
        assert_eq!(first.height(), 2);
        assert_eq!(second.height(), 2);
        let first_values = first.column("a").unwrap().to_arrow();
        let second_values = second.column("a").unwrap().to_arrow();
        assert_eq!(
            first_values[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            second_values[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            4
        );
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn row_group_over_budget_fails_before_reader_build() {
        let (_dir, path) = parquet_fixture();
        let factory = ParquetBatchSourceFactory::new(&path, ParquetReadOptions::default());
        let mut stream = DataFrameStream::from_factory(&factory, options(1024, 1)).unwrap();
        assert!(matches!(
            stream.next_batch(),
            Err(DataFrameError::StreamFailed {
                code: "resource_limit_exceeded",
                ..
            })
        ));
    }

    #[test]
    fn malformed_footer_fails_during_bounded_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.parquet");
        std::fs::write(&path, b"not a parquet file").unwrap();
        let factory = ParquetBatchSourceFactory::new(&path, ParquetReadOptions::default());
        assert!(matches!(
            DataFrameStream::from_factory(&factory, options(64 * 1024, 1)),
            Err(DataFrameError::Parquet { .. })
        ));
    }
}
