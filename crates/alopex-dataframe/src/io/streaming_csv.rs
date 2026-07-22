//! Bounded, quote-aware CSV source for `DataFrameStream`.
//!
//! This reader deliberately does not call `read_csv_with_options`: it frames one complete CSV
//! record group, reserves conservative decode ownership, and only then invokes Arrow's CSV
//! decoder for that group.

use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_csv::reader::{Format, ReaderBuilder};
use regex::Regex;

use crate::io::options::CsvReadOptions;
use crate::physical::budget::{ResourceReservation, ResourceScope};
use crate::physical::{
    BatchOpenContext, BatchSource, BatchSourceFactory, SourceLimit, StreamBatch,
};
use crate::{DataFrameError, Result};

/// Re-consumable bounded CSV source factory.
#[derive(Debug, Clone)]
pub struct CsvBatchSourceFactory {
    path: PathBuf,
    options: CsvReadOptions,
}

impl CsvBatchSourceFactory {
    /// Construct a factory for a CSV path and read options.
    pub fn new(path: impl AsRef<Path>, options: CsvReadOptions) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            options,
        }
    }

    /// Construct a factory from a physical CSV scan. Predicate execution is rejected by the
    /// streaming compiler until its budgeted batch operator is installed.
    pub(crate) fn from_scan(
        path: PathBuf,
        predicate: Option<crate::Expr>,
        projection: Option<Vec<String>>,
    ) -> Self {
        let options = CsvReadOptions {
            predicate,
            projection,
            ..CsvReadOptions::default()
        };
        Self { path, options }
    }
}

impl BatchSourceFactory for CsvBatchSourceFactory {
    fn source_name(&self) -> &'static str {
        "csv"
    }

    fn schema(&self) -> Result<SchemaRef> {
        Err(DataFrameError::streaming_unsupported(
            "csv_scan",
            "schema_is_available_after_bounded_open",
        ))
    }

    fn source_limits(&self) -> Vec<SourceLimit> {
        vec![SourceLimit {
            source: crate::physical::PlanSubject::CsvScan,
            code: "stable_input_order",
            description: "CSV streaming preserves physical input record order",
        }]
    }

    fn open(&self, context: BatchOpenContext) -> Result<Box<dyn BatchSource>> {
        validate_options(&self.options)?;
        if self.options.predicate.is_some() {
            return Err(DataFrameError::streaming_unsupported(
                "csv_scan",
                "predicate_batch_operator_not_installed",
            ));
        }

        let block_bytes = bounded_block_bytes(context.options.memory_limit_bytes)?;
        let (full_schema, schema_reservation) =
            infer_schema_bounded(&self.path, &self.options, &context, block_bytes)?;
        let projection_indices =
            projection_indices(&full_schema, self.options.projection.as_deref())?;
        let output_schema = projected_schema(&full_schema, projection_indices.as_deref());

        let file = File::open(&self.path)
            .map_err(|source| DataFrameError::io_with_path(source, &self.path))?;
        let mut framer = BoundedCsvFramer::new(
            file,
            self.path.clone(),
            self.options.quote_char,
            block_bytes,
        );
        if self.options.has_header {
            framer.skip_record(&context)?;
        }

        Ok(Box::new(CsvBatchSource {
            context,
            framer,
            full_schema,
            output_schema,
            projection_indices,
            options: self.options.clone(),
            schema_reservation: Some(schema_reservation),
        }))
    }
}

struct CsvBatchSource {
    context: BatchOpenContext,
    framer: BoundedCsvFramer<File>,
    full_schema: SchemaRef,
    output_schema: SchemaRef,
    projection_indices: Option<Vec<usize>>,
    options: CsvReadOptions,
    // Holds the bounded schema-inference allocation for the source lifetime.
    schema_reservation: Option<ResourceReservation>,
}

impl BatchSource for CsvBatchSource {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<StreamBatch>> {
        let Some(frame) = self
            .framer
            .next_frame(self.context.options.batch_rows.get(), &self.context)?
        else {
            return Ok(None);
        };

        let decode_bytes = conservative_decode_upper_bound(
            frame.bytes.len(),
            frame.rows,
            self.full_schema.fields().len(),
        );
        let reservation = self
            .context
            .budget
            .reserve_batch(ResourceScope::Decode, decode_bytes)?;

        let batch = decode_frame(
            &frame.bytes,
            frame.rows,
            self.full_schema.clone(),
            &self.options,
        )?;
        let batch = project_batch(batch, self.projection_indices.as_deref())?;
        drop(frame);

        Ok(Some(StreamBatch::new(batch, reservation)))
    }

    fn close(&mut self) -> Result<()> {
        self.schema_reservation.take();
        Ok(())
    }
}

fn infer_schema_bounded(
    path: &Path,
    options: &CsvReadOptions,
    context: &BatchOpenContext,
    block_bytes: usize,
) -> Result<(SchemaRef, ResourceReservation)> {
    let file = File::open(path).map_err(|source| DataFrameError::io_with_path(source, path))?;
    let mut framer =
        BoundedCsvFramer::new(file, path.to_path_buf(), options.quote_char, block_bytes);
    let record_count = options
        .infer_schema_length
        .saturating_add(usize::from(options.has_header))
        .max(1);
    let Some(frame) = framer.next_frame(record_count, context)? else {
        return Err(DataFrameError::schema_mismatch("CSV input is empty"));
    };
    let schema_reservation = context.budget.reserve(
        ResourceScope::Source,
        schema_reservation_bytes(frame.bytes.len()),
    )?;
    let mut reader = BufReader::with_capacity(
        frame.bytes.len().max(1),
        Cursor::new(frame.bytes.as_slice()),
    );
    let (schema, _) = csv_format(options, options.has_header)?
        .infer_schema(&mut reader, Some(options.infer_schema_length))
        .map_err(|source| DataFrameError::Arrow { source })?;
    drop(frame);
    Ok((Arc::new(schema), schema_reservation))
}

fn decode_frame(
    bytes: &[u8],
    rows: usize,
    schema: SchemaRef,
    options: &CsvReadOptions,
) -> Result<RecordBatch> {
    let reader = ReaderBuilder::new(schema)
        .with_format(csv_format(options, false)?)
        .with_batch_size(rows.max(1))
        .build(Cursor::new(bytes))
        .map_err(|source| DataFrameError::Arrow { source })?;
    let mut batches = reader;
    let batch = batches
        .next()
        .transpose()
        .map_err(|source| DataFrameError::Arrow { source })?
        .ok_or_else(|| DataFrameError::schema_mismatch("CSV frame contained no records"))?;
    if batches.next().is_some() {
        return Err(DataFrameError::schema_mismatch(
            "CSV frame decoded to more than one bounded batch",
        ));
    }
    Ok(batch)
}

fn csv_format(options: &CsvReadOptions, has_header: bool) -> Result<Format> {
    let mut format = Format::default()
        .with_header(has_header)
        .with_delimiter(options.delimiter);
    if let Some(quote_char) = options.quote_char {
        format = format.with_quote(quote_char);
    }
    if !options.null_values.is_empty() {
        let pattern = options
            .null_values
            .iter()
            .map(|value| regex::escape(value))
            .collect::<Vec<_>>()
            .join("|");
        let regex = Regex::new(&format!("^(?:{pattern})$")).map_err(|error| {
            DataFrameError::configuration("null_values", format!("invalid regex: {error}"))
        })?;
        format = format.with_null_regex(regex);
    }
    Ok(format)
}

fn validate_options(options: &CsvReadOptions) -> Result<()> {
    if options.delimiter == b'\0' {
        return Err(DataFrameError::configuration(
            "delimiter",
            "delimiter must not be NUL (0x00)",
        ));
    }
    if options.quote_char == Some(b'\0') {
        return Err(DataFrameError::configuration(
            "quote_char",
            "quote_char must not be NUL (0x00)",
        ));
    }
    Ok(())
}

fn projection_indices(
    schema: &SchemaRef,
    projection: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    let Some(projection) = projection else {
        return Ok(None);
    };
    projection
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

fn project_batch(batch: RecordBatch, projection: Option<&[usize]>) -> Result<RecordBatch> {
    match projection {
        Some(indices) => batch.project(indices).map_err(|error| {
            DataFrameError::schema_mismatch(format!("failed to project CSV batch: {error}"))
        }),
        None => Ok(batch),
    }
}

fn bounded_block_bytes(memory_limit_bytes: u64) -> Result<usize> {
    let bytes = (memory_limit_bytes / 32).clamp(1, 64 * 1024);
    usize::try_from(bytes).map_err(|_| {
        DataFrameError::configuration("memory_limit_bytes", "does not fit this platform")
    })
}

fn conservative_decode_upper_bound(raw_bytes: usize, rows: usize, columns: usize) -> u64 {
    let raw = u64::try_from(raw_bytes).unwrap_or(u64::MAX);
    let cells = u64::try_from(rows)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(columns).unwrap_or(u64::MAX));
    raw.saturating_mul(16)
        .saturating_add(cells.saturating_mul(16))
        .saturating_add(1024)
}

fn schema_reservation_bytes(raw_bytes: usize) -> u64 {
    u64::try_from(raw_bytes)
        .unwrap_or(u64::MAX)
        .saturating_mul(2)
        .saturating_add(128)
}

struct FramedBytes {
    bytes: Vec<u8>,
    rows: usize,
    _reservation: ResourceReservation,
}

/// Fixed-capacity framing reader that understands escaped quotes and multiline quoted records.
struct BoundedCsvFramer<R> {
    reader: R,
    path: PathBuf,
    quote_char: Option<u8>,
    byte_cap: usize,
    eof: bool,
}

impl<R: Read> BoundedCsvFramer<R> {
    fn new(reader: R, path: PathBuf, quote_char: Option<u8>, byte_cap: usize) -> Self {
        Self {
            reader,
            path,
            quote_char,
            byte_cap,
            eof: false,
        }
    }

    fn next_frame(
        &mut self,
        max_rows: usize,
        context: &BatchOpenContext,
    ) -> Result<Option<FramedBytes>> {
        if self.eof {
            return Ok(None);
        }
        let reservation = context
            .budget
            .reserve(ResourceScope::Source, self.byte_cap as u64)?;
        let mut bytes = Vec::with_capacity(self.byte_cap);
        let mut tracker = QuoteTracker::new(self.quote_char);
        let mut rows: usize = 0;

        loop {
            let Some(byte) = self.read_byte()? else {
                self.eof = true;
                if bytes.is_empty() {
                    return Ok(None);
                }
                tracker.finish()?;
                if bytes.last() != Some(&b'\n') {
                    rows = rows.saturating_add(1);
                }
                break;
            };

            if bytes.len() == self.byte_cap {
                self.discard_record_tail(tracker, byte)?;
                return Err(self.record_limit_error(context));
            }
            bytes.push(byte);
            if tracker.observe(byte) {
                rows = rows.saturating_add(1);
                if rows == max_rows {
                    break;
                }
            }
        }

        Ok(Some(FramedBytes {
            bytes,
            rows,
            _reservation: reservation,
        }))
    }

    fn skip_record(&mut self, context: &BatchOpenContext) -> Result<()> {
        let mut tracker = QuoteTracker::new(self.quote_char);
        let mut bytes = 0usize;
        loop {
            let Some(byte) = self.read_byte()? else {
                self.eof = true;
                tracker.finish()?;
                return Ok(());
            };
            bytes = bytes.saturating_add(1);
            if bytes > self.byte_cap {
                self.discard_record_tail(tracker, byte)?;
                return Err(self.record_limit_error(context));
            }
            if tracker.observe(byte) {
                return Ok(());
            }
        }
    }

    fn read_byte(&mut self) -> Result<Option<u8>> {
        let mut byte = [0_u8; 1];
        match self.reader.read(&mut byte) {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(byte[0])),
            Err(source) => Err(DataFrameError::io_with_path(source, &self.path)),
        }
    }

    fn discard_record_tail(&mut self, mut tracker: QuoteTracker, first: u8) -> Result<()> {
        if tracker.observe(first) {
            return Ok(());
        }
        while let Some(byte) = self.read_byte()? {
            if tracker.observe(byte) {
                return Ok(());
            }
        }
        self.eof = true;
        tracker.finish()
    }

    fn record_limit_error(&self, context: &BatchOpenContext) -> DataFrameError {
        DataFrameError::resource_limit_exceeded(
            context.budget.memory_limit_bytes(),
            context
                .budget
                .usage()
                .reserved_bytes
                .saturating_add(self.byte_cap as u64)
                .saturating_add(1),
            context.budget.max_in_flight_batches(),
            context.budget.usage().reserved_batches,
            ResourceScope::Source,
        )
    }
}

#[derive(Clone, Copy)]
struct QuoteTracker {
    quote: Option<u8>,
    in_quotes: bool,
    quote_pending: bool,
}

impl QuoteTracker {
    fn new(quote: Option<u8>) -> Self {
        Self {
            quote,
            in_quotes: false,
            quote_pending: false,
        }
    }

    fn observe(&mut self, byte: u8) -> bool {
        let Some(quote) = self.quote else {
            return byte == b'\n';
        };

        if self.quote_pending {
            if byte == quote {
                self.quote_pending = false;
                return false;
            }
            self.quote_pending = false;
            self.in_quotes = false;
        }
        if self.in_quotes {
            if byte == quote {
                self.quote_pending = true;
            }
            return false;
        }
        if byte == quote {
            self.in_quotes = true;
            false
        } else {
            byte == b'\n'
        }
    }

    fn finish(&mut self) -> Result<()> {
        if self.quote_pending {
            self.quote_pending = false;
            self.in_quotes = false;
        }
        if self.in_quotes {
            return Err(DataFrameError::schema_mismatch(
                "CSV input ended inside a quoted record",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use arrow::array::Int64Array;

    use super::CsvBatchSourceFactory;
    use crate::io::CsvReadOptions;
    use crate::physical::budget::StreamOptions;
    use crate::physical::DataFrameStream;
    use crate::DataFrameError;

    fn options(memory_limit_bytes: u64, batch_rows: usize) -> StreamOptions {
        StreamOptions::new(
            memory_limit_bytes,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(batch_rows).unwrap(),
        )
    }

    #[test]
    fn quoted_newline_is_one_record_and_input_order_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quoted.csv");
        std::fs::write(&path, "a,b\n1,\"first\nline\"\n2,second\n").unwrap();

        let factory = CsvBatchSourceFactory::new(
            &path,
            CsvReadOptions::default().with_infer_schema_length(1),
        );
        let mut stream = DataFrameStream::from_factory(&factory, options(16 * 1024, 1)).unwrap();
        let first = stream.next_batch().unwrap().unwrap();
        let second = stream.next_batch().unwrap().unwrap();

        assert_eq!(first.height(), 1);
        assert_eq!(second.height(), 1);
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
                .value(0),
            2
        );
    }

    #[test]
    fn oversized_record_fails_without_accumulating_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.csv");
        std::fs::write(&path, format!("a\n{}\n", "x".repeat(256))).unwrap();

        let factory = CsvBatchSourceFactory::new(&path, CsvReadOptions::default());
        let err = match DataFrameStream::from_factory(&factory, options(512, 1)) {
            Ok(_) => panic!("oversized CSV record must not open a stream"),
            Err(error) => error,
        };
        assert!(matches!(err, DataFrameError::ResourceLimitExceeded { .. }));
    }

    #[test]
    fn schema_error_is_terminal_and_does_not_yield_another_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid.csv");
        std::fs::write(&path, "a,b\n1,2\n3,4,5\n").unwrap();

        let factory = CsvBatchSourceFactory::new(
            &path,
            CsvReadOptions::default().with_infer_schema_length(1),
        );
        let mut stream = DataFrameStream::from_factory(&factory, options(16 * 1024, 1)).unwrap();
        assert!(stream.next_batch().unwrap().is_some());
        assert!(matches!(
            stream.next_batch(),
            Err(DataFrameError::StreamFailed { .. })
        ));
        assert!(matches!(
            stream.next_batch(),
            Err(DataFrameError::StreamFailed { .. })
        ));
    }

    #[test]
    fn invalid_utf8_is_a_repeatable_decode_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid-utf8.csv");
        std::fs::write(&path, b"a\nok\n\xff\n").unwrap();

        let factory = CsvBatchSourceFactory::new(
            &path,
            CsvReadOptions::default().with_infer_schema_length(1),
        );
        let mut stream = DataFrameStream::from_factory(&factory, options(16 * 1024, 1)).unwrap();
        assert!(stream.next_batch().unwrap().is_some());
        assert!(matches!(
            stream.next_batch(),
            Err(DataFrameError::StreamFailed {
                code: "decode_error",
                ..
            })
        ));
        assert!(matches!(
            stream.next_batch(),
            Err(DataFrameError::StreamFailed {
                code: "decode_error",
                ..
            })
        ));
    }
}
