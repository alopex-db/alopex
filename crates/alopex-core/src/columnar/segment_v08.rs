//! Read-only, range-addressable V08 columnar segment access.
//!
//! V08 is deliberately a distinct layout from the legacy V2 single-blob
//! segment.  It stores a checksummed header, schema, directory, and each
//! compressed column chunk at independently addressable locations.  This
//! module only reads a provisioned V08 artifact; it does not create, migrate,
//! stage, or publish one.

use std::fmt;
use std::sync::Arc;

use bincode::Options;
use crc32fast::Hasher;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::columnar::encoding::Column;
use crate::columnar::encoding_v2::{create_decoder, Decoder, EncodingV2};
use crate::columnar::error::{ColumnarError, Result};
use crate::columnar::segment_v2::{RecordBatch, Schema};
use crate::storage::compression::{create_compressor, CompressionV2};
use crate::storage::format::bincode_config;

/// Magic marker for a range-addressable V08 segment artifact.
pub const STREAMING_SEGMENT_MAGIC_V08: [u8; 4] = *b"ALX8";
/// The V08 layout version accepted by this compatibility reader.
pub const STREAMING_SEGMENT_LAYOUT_VERSION_V08: u16 = 8;
/// Default maximum size for each V08 metadata object (header/schema/directory).
///
/// A V08 directory is an intentionally small preflight object.  Deployments
/// that provision a larger bounded directory may opt in explicitly through
/// [`ChunkedSegmentAccessV08::open_with_metadata_limit`].
pub const DEFAULT_V08_METADATA_LIMIT_BYTES: u64 = 8 * 1024 * 1024;

/// Identifies one provisioned V08 segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SegmentReferenceV08 {
    /// Owning table identifier.
    pub table_id: u32,
    /// Segment identifier within the table.
    pub segment_id: u64,
}

impl SegmentReferenceV08 {
    /// Creates a V08 segment reference.
    pub const fn new(table_id: u32, segment_id: u64) -> Self {
        Self {
            table_id,
            segment_id,
        }
    }
}

/// Header for a provisioned V08 streaming segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingSegmentHeaderV08 {
    /// [`STREAMING_SEGMENT_MAGIC_V08`].
    pub magic: [u8; 4],
    /// [`STREAMING_SEGMENT_LAYOUT_VERSION_V08`].
    pub format_version: u16,
    /// Total number of rows represented by the directory ranges.
    pub row_count: u64,
    /// Number of columns in the separately stored schema.
    pub column_count: u16,
    /// Number of row groups in the directory.
    pub row_group_count: u32,
    /// CRC32 of the serialized schema payload.
    pub schema_checksum: u32,
    /// CRC32 of the serialized directory payload.
    pub directory_checksum: u32,
}

/// Checksummed wire envelope for V08 metadata objects.
///
/// The checksum covers `value`'s canonical bincode payload, rather than the
/// envelope, so header fields can independently bind the schema and directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksummedMetadataV08<T> {
    /// Decoded metadata payload.
    pub value: T,
    /// CRC32 of the canonical serialized payload.
    pub checksum: u32,
}

/// Directory of V08 row groups in stable input order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingSegmentDirectoryV08 {
    /// Row groups, ordered by their contiguous source row range.
    pub row_groups: Vec<StreamingRowGroupV08>,
}

/// Validated metadata layout for one provisioned V08 streaming segment.
///
/// This type contains no chunk bytes.  It is safe to inspect during source
/// preflight and exposes the byte upper bounds a caller must reserve before it
/// asks [`ChunkedSegmentAccessV08`] to fetch a row group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamingSegmentLayoutV08 {
    /// Checksummed V08 header.
    pub header: StreamingSegmentHeaderV08,
    /// Provisioned schema.
    pub schema: Schema,
    /// Stable-order row-group directory.
    pub directory: StreamingSegmentDirectoryV08,
}

/// One V08 row group and its bounded decode metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingRowGroupV08 {
    /// Inclusive start row in the segment.
    pub row_start: u64,
    /// Number of rows in the group.
    pub row_count: u64,
    /// Sum of encoded chunk sizes in this row group.
    pub encoded_bytes: u64,
    /// Sum of decoded column payload sizes in this row group.
    pub decoded_bytes: u64,
    /// Conservative upper bound for the completed Arrow-compatible batch.
    pub arrow_allocation_upper_bound: u64,
    /// Exactly one chunk for every schema column.
    pub chunks: Vec<StreamingChunkMetaV08>,
}

/// Metadata for one independently addressable V08 column chunk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingChunkMetaV08 {
    /// Zero-based schema column index.
    pub column_index: u16,
    /// Encoding used for this chunk.
    pub encoding: EncodingV2,
    /// Compression used for this chunk.
    pub compression: CompressionV2,
    /// Number of bytes fetched from the chunk location.
    pub encoded_bytes: u64,
    /// Required decoded payload size before the column decoder runs.
    pub decoded_bytes: u64,
    /// CRC32 of the stored (compressed) chunk bytes.
    pub checksum: u32,
}

/// A verified V08 chunk.
///
/// The bytes remain owned by this value and are released when it is dropped or
/// consumed with [`Self::into_bytes`].  Callers can reserve the directory's
/// upper bound before creating it and therefore never need to retain a segment
/// blob or multiple unconsumed chunks.
#[derive(Debug)]
pub struct VerifiedChunkV08 {
    metadata: StreamingChunkMetaV08,
    bytes: Vec<u8>,
}

impl VerifiedChunkV08 {
    /// Returns the validated directory metadata for this chunk.
    pub fn metadata(&self) -> &StreamingChunkMetaV08 {
        &self.metadata
    }

    /// Transfers the verified stored bytes to the immediate decoder.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Read-only provider for independently addressable V08 segment objects.
///
/// Implementations must fetch only the requested header, schema, directory, or
/// one column chunk.  In particular, an implementation must not read a legacy
/// V2 blob and split it into artificial batches.
pub trait RangeAddressableSegmentProvider: Send + Sync {
    /// Reads the small V08 header object.
    fn read_header(&self, segment: SegmentReferenceV08) -> Result<Vec<u8>>;
    /// Reads the separately stored V08 schema object.
    fn read_schema(&self, segment: SegmentReferenceV08) -> Result<Vec<u8>>;
    /// Reads the separately stored V08 directory object.
    fn read_directory(&self, segment: SegmentReferenceV08) -> Result<Vec<u8>>;
    /// Reads one stored column chunk from one row group.
    fn read_chunk(
        &self,
        segment: SegmentReferenceV08,
        row_group_index: u32,
        column_index: u16,
    ) -> Result<Vec<u8>>;
}

/// Read-only cursor over a validated V08 segment directory.
pub struct ChunkedSegmentAccessV08 {
    provider: Arc<dyn RangeAddressableSegmentProvider>,
    segment: SegmentReferenceV08,
    layout: StreamingSegmentLayoutV08,
}

impl fmt::Debug for ChunkedSegmentAccessV08 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkedSegmentAccessV08")
            .field("segment", &self.segment)
            .field("schema", &self.layout.schema)
            .field("row_group_count", &self.layout.directory.row_groups.len())
            .finish_non_exhaustive()
    }
}

impl ChunkedSegmentAccessV08 {
    /// Opens and validates a V08 segment using the default metadata bound.
    pub fn open(
        provider: Arc<dyn RangeAddressableSegmentProvider>,
        segment: SegmentReferenceV08,
    ) -> Result<Self> {
        Self::open_with_metadata_limit(provider, segment, DEFAULT_V08_METADATA_LIMIT_BYTES)
    }

    /// Opens and validates a V08 segment under an explicit metadata bound.
    ///
    /// Header, schema, and directory validation happens before any chunk is
    /// fetched.  An invalid layout therefore cannot cause a later full-blob
    /// fallback or source read.
    pub fn open_with_metadata_limit(
        provider: Arc<dyn RangeAddressableSegmentProvider>,
        segment: SegmentReferenceV08,
        metadata_limit_bytes: u64,
    ) -> Result<Self> {
        if metadata_limit_bytes == 0 {
            return Err(invalid("V08 metadata limit must be greater than zero"));
        }

        let header_bytes = provider.read_header(segment)?;
        ensure_metadata_limit("header", &header_bytes, metadata_limit_bytes)?;
        let (header, _) = decode_metadata("header", &header_bytes)?;
        validate_header(&header)?;

        let schema_bytes = provider.read_schema(segment)?;
        ensure_metadata_limit("schema", &schema_bytes, metadata_limit_bytes)?;
        let (schema, schema_checksum) = decode_metadata("schema", &schema_bytes)?;
        if schema_checksum != header.schema_checksum {
            return Err(ColumnarError::ChecksumMismatch);
        }

        let directory_bytes = provider.read_directory(segment)?;
        ensure_metadata_limit("directory", &directory_bytes, metadata_limit_bytes)?;
        let (directory, directory_checksum) = decode_metadata("directory", &directory_bytes)?;
        if directory_checksum != header.directory_checksum {
            return Err(ColumnarError::ChecksumMismatch);
        }

        validate_directory(&header, &schema, &directory)?;

        Ok(Self {
            provider,
            segment,
            layout: StreamingSegmentLayoutV08 {
                header,
                schema,
                directory,
            },
        })
    }

    /// Returns the validated V08 layout without fetching a chunk.
    pub fn layout(&self) -> &StreamingSegmentLayoutV08 {
        &self.layout
    }

    /// Returns the provisioned schema without fetching a chunk.
    pub fn schema(&self) -> &Schema {
        &self.layout.schema
    }

    /// Returns the immutable V08 directory without fetching a chunk.
    pub fn directory(&self) -> &StreamingSegmentDirectoryV08 {
        &self.layout.directory
    }

    /// Returns the number of stable-order row groups.
    pub fn row_group_count(&self) -> usize {
        self.layout.directory.row_groups.len()
    }

    /// Conservative retained-memory estimate for the validated metadata layout.
    ///
    /// This excludes chunk bytes and is intended for a source-open reservation.
    /// It deliberately over-accounts `String`, `Vec`, and directory entry
    /// headers so a caller can shrink an initial opaque-parser reservation
    /// without releasing memory still owned by this cursor.
    pub fn metadata_footprint_upper_bound(&self) -> u64 {
        let schema_bytes = self
            .layout
            .schema
            .columns
            .iter()
            .fold(128_u64, |total, column| {
                total
                    .saturating_add(128)
                    .saturating_add(u64::try_from(column.name.len()).unwrap_or(u64::MAX))
            });
        let directory_bytes =
            self.layout
                .directory
                .row_groups
                .iter()
                .fold(128_u64, |total, row_group| {
                    total.saturating_add(160).saturating_add(
                        u64::try_from(row_group.chunks.len())
                            .unwrap_or(u64::MAX)
                            .saturating_mul(128),
                    )
                });
        256_u64
            .saturating_add(schema_bytes)
            .saturating_add(directory_bytes)
    }

    /// Returns metadata for one row group without fetching it.
    pub fn row_group(&self, row_group_index: usize) -> Result<&StreamingRowGroupV08> {
        self.layout
            .directory
            .row_groups
            .get(row_group_index)
            .ok_or_else(|| invalid("V08 row group index out of bounds"))
    }

    /// Reads exactly one chunk and verifies its declared length and checksum.
    pub fn read_verified_chunk(
        &self,
        row_group_index: usize,
        column_index: usize,
    ) -> Result<VerifiedChunkV08> {
        let row_group = self.row_group(row_group_index)?;
        let column_index = u16::try_from(column_index)
            .map_err(|_| invalid("V08 column index does not fit u16"))?;
        let metadata = row_group
            .chunks
            .iter()
            .find(|chunk| chunk.column_index == column_index)
            .cloned()
            .ok_or_else(|| invalid("V08 chunk is missing from directory"))?;
        let row_group_index = u32::try_from(row_group_index)
            .map_err(|_| invalid("V08 row group index does not fit u32"))?;

        let bytes = self
            .provider
            .read_chunk(self.segment, row_group_index, column_index)?;
        let actual_len =
            u64::try_from(bytes.len()).map_err(|_| invalid("V08 chunk length does not fit u64"))?;
        if actual_len != metadata.encoded_bytes {
            return Err(invalid("V08 chunk length differs from directory"));
        }
        if checksum(&bytes) != metadata.checksum {
            return Err(ColumnarError::ChecksumMismatch);
        }

        Ok(VerifiedChunkV08 { metadata, bytes })
    }

    /// Fetches and decodes one row group in the caller's declared column order.
    ///
    /// The caller is expected to reserve
    /// [`StreamingRowGroupV08::arrow_allocation_upper_bound`] before calling
    /// this method.  The implementation fetches and releases one verified
    /// stored chunk at a time; it never materializes the segment or all row
    /// groups.
    pub fn read_row_group(&self, row_group_index: usize, columns: &[usize]) -> Result<RecordBatch> {
        let row_group = self.row_group(row_group_index)?;
        if columns.is_empty() {
            return Err(invalid("V08 projection must contain at least one column"));
        }

        let mut seen = vec![false; self.layout.schema.column_count()];
        let mut output_columns = Vec::with_capacity(columns.len());
        let mut output_bitmaps = Vec::with_capacity(columns.len());
        let mut output_schema = Vec::with_capacity(columns.len());

        for &column_index in columns {
            let schema_column = self
                .layout
                .schema
                .columns
                .get(column_index)
                .ok_or_else(|| invalid("V08 projection column index out of bounds"))?;
            if std::mem::replace(
                seen.get_mut(column_index)
                    .ok_or_else(|| invalid("V08 projection column index out of bounds"))?,
                true,
            ) {
                return Err(invalid("V08 projection contains a duplicate column"));
            }

            let chunk = self.read_verified_chunk(row_group_index, column_index)?;
            let metadata = chunk.metadata().clone();
            let stored = chunk.into_bytes();
            let expected_decoded_bytes = usize::try_from(metadata.decoded_bytes)
                .map_err(|_| invalid("V08 decoded chunk length does not fit usize"))?;
            let decoded = match metadata.compression {
                CompressionV2::None => stored,
                compression => create_compressor(compression)
                    .map_err(|error| invalid(format!("V08 compression is unavailable: {error}")))?
                    .decompress(&stored, expected_decoded_bytes)
                    .map_err(|error| invalid(format!("V08 chunk decompression failed: {error}")))?,
            };
            if decoded.len() != expected_decoded_bytes {
                return Err(invalid("V08 decoded chunk length differs from directory"));
            }

            let decoder: Box<dyn Decoder> = create_decoder(metadata.encoding);
            let expected_rows = usize::try_from(row_group.row_count)
                .map_err(|_| invalid("V08 row count does not fit usize"))?;
            let (column, bitmap) =
                decoder.decode(&decoded, expected_rows, schema_column.logical_type)?;
            if column_len(&column) != expected_rows {
                return Err(invalid(
                    "V08 decoded column row count differs from directory",
                ));
            }

            output_schema.push(schema_column.clone());
            output_columns.push(column);
            output_bitmaps.push(bitmap);
        }

        Ok(RecordBatch::new(
            Schema {
                columns: output_schema,
            },
            output_columns,
            output_bitmaps,
        ))
    }
}

fn validate_header(header: &StreamingSegmentHeaderV08) -> Result<()> {
    if header.magic != STREAMING_SEGMENT_MAGIC_V08 {
        return Err(invalid("invalid V08 segment magic"));
    }
    if header.format_version != STREAMING_SEGMENT_LAYOUT_VERSION_V08 {
        return Err(ColumnarError::UnsupportedFormatVersion {
            found: header.format_version,
            expected: STREAMING_SEGMENT_LAYOUT_VERSION_V08,
        });
    }
    if header.column_count == 0 {
        return Err(invalid("V08 segment must declare at least one column"));
    }
    if header.row_count == 0 && header.row_group_count != 0 {
        return Err(invalid("empty V08 segment must not declare row groups"));
    }
    if header.row_count > 0 && header.row_group_count == 0 {
        return Err(invalid("non-empty V08 segment is missing row groups"));
    }
    Ok(())
}

fn validate_directory(
    header: &StreamingSegmentHeaderV08,
    schema: &Schema,
    directory: &StreamingSegmentDirectoryV08,
) -> Result<()> {
    if schema.column_count() != usize::from(header.column_count) {
        return Err(invalid("V08 schema column count differs from header"));
    }
    if directory.row_groups.len()
        != usize::try_from(header.row_group_count)
            .map_err(|_| invalid("V08 row group count does not fit usize"))?
    {
        return Err(invalid("V08 directory row group count differs from header"));
    }

    let mut expected_row_start = 0_u64;
    for row_group in &directory.row_groups {
        if row_group.row_count == 0 {
            return Err(invalid("V08 row group must not be empty"));
        }
        usize::try_from(row_group.row_count)
            .map_err(|_| invalid("V08 row group row count does not fit usize"))?;
        if row_group.row_start != expected_row_start {
            return Err(invalid("V08 row ranges must be contiguous and ordered"));
        }
        expected_row_start = expected_row_start
            .checked_add(row_group.row_count)
            .ok_or_else(|| invalid("V08 row range overflows u64"))?;

        let mut seen = vec![false; schema.column_count()];
        let mut encoded_bytes = 0_u64;
        let mut decoded_bytes = 0_u64;
        for chunk in &row_group.chunks {
            let column_index = usize::from(chunk.column_index);
            let slot = seen
                .get_mut(column_index)
                .ok_or_else(|| invalid("V08 chunk column index out of bounds"))?;
            if std::mem::replace(slot, true) {
                return Err(invalid("V08 row group contains duplicate column chunks"));
            }
            if chunk.encoded_bytes == 0 || chunk.decoded_bytes == 0 {
                return Err(invalid("V08 chunk size must be greater than zero"));
            }
            encoded_bytes = encoded_bytes
                .checked_add(chunk.encoded_bytes)
                .ok_or_else(|| invalid("V08 encoded size overflows u64"))?;
            decoded_bytes = decoded_bytes
                .checked_add(chunk.decoded_bytes)
                .ok_or_else(|| invalid("V08 decoded size overflows u64"))?;
        }
        if seen.iter().any(|present| !present) {
            return Err(invalid("V08 row group is missing a schema column chunk"));
        }
        if encoded_bytes != row_group.encoded_bytes || decoded_bytes != row_group.decoded_bytes {
            return Err(invalid("V08 row group sizes differ from chunk metadata"));
        }
        if row_group.arrow_allocation_upper_bound < row_group.decoded_bytes {
            return Err(invalid(
                "V08 Arrow allocation upper bound is smaller than decoded bytes",
            ));
        }
    }

    if expected_row_start != header.row_count {
        return Err(invalid(
            "V08 directory row ranges differ from header row count",
        ));
    }
    Ok(())
}

fn ensure_metadata_limit(label: &str, bytes: &[u8], limit: u64) -> Result<()> {
    if u64::try_from(bytes.len()).map_err(|_| invalid("metadata size does not fit u64"))? > limit {
        return Err(invalid(format!("V08 {label} exceeds metadata limit")));
    }
    Ok(())
}

fn decode_metadata<T>(label: &str, bytes: &[u8]) -> Result<(T, u32)>
where
    T: DeserializeOwned + Serialize,
{
    let envelope: ChecksummedMetadataV08<T> = bincode_config()
        .deserialize(bytes)
        .map_err(|error| invalid(format!("invalid V08 {label}: {error}")))?;
    let computed = metadata_checksum(&envelope.value)?;
    if computed != envelope.checksum {
        return Err(ColumnarError::ChecksumMismatch);
    }
    Ok((envelope.value, computed))
}

fn metadata_checksum<T: Serialize>(value: &T) -> Result<u32> {
    let bytes = bincode_config()
        .serialize(value)
        .map_err(|error| invalid(format!("cannot serialize V08 metadata: {error}")))?;
    Ok(checksum(&bytes))
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
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

fn invalid(message: impl Into<String>) -> ColumnarError {
    ColumnarError::InvalidFormat(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::columnar::encoding::LogicalType;
    use crate::columnar::encoding_v2::create_encoder;
    use crate::columnar::segment_v2::ColumnSchema;

    #[derive(Debug)]
    struct TestProvider {
        header: Vec<u8>,
        schema: Vec<u8>,
        directory: Vec<u8>,
        chunks: HashMap<(u32, u16), Vec<u8>>,
        chunk_reads: AtomicUsize,
    }

    impl RangeAddressableSegmentProvider for TestProvider {
        fn read_header(&self, _segment: SegmentReferenceV08) -> Result<Vec<u8>> {
            Ok(self.header.clone())
        }

        fn read_schema(&self, _segment: SegmentReferenceV08) -> Result<Vec<u8>> {
            Ok(self.schema.clone())
        }

        fn read_directory(&self, _segment: SegmentReferenceV08) -> Result<Vec<u8>> {
            Ok(self.directory.clone())
        }

        fn read_chunk(
            &self,
            _segment: SegmentReferenceV08,
            row_group_index: u32,
            column_index: u16,
        ) -> Result<Vec<u8>> {
            self.chunk_reads.fetch_add(1, Ordering::SeqCst);
            self.chunks
                .get(&(row_group_index, column_index))
                .cloned()
                .ok_or(ColumnarError::NotFound)
        }
    }

    fn envelope<T: Serialize + Clone>(value: T) -> Vec<u8> {
        let checksum = metadata_checksum(&value).unwrap();
        bincode_config()
            .serialize(&ChecksummedMetadataV08 { value, checksum })
            .unwrap()
    }

    fn encoded_column(values: Vec<i64>) -> Vec<u8> {
        create_encoder(EncodingV2::Plain)
            .encode(&Column::Int64(values), None)
            .unwrap()
    }

    fn fixture() -> Arc<TestProvider> {
        let schema = Schema {
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
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
        };
        let values = [vec![1, 2], vec![10, 20], vec![3], vec![30]];
        let encoded = values.map(encoded_column);
        let chunk = |column_index: u16, bytes: &Vec<u8>| StreamingChunkMetaV08 {
            column_index,
            encoding: EncodingV2::Plain,
            compression: CompressionV2::None,
            encoded_bytes: bytes.len() as u64,
            decoded_bytes: bytes.len() as u64,
            checksum: checksum(bytes),
        };
        let row_group = |row_start, row_count, first: usize| {
            let chunks = vec![chunk(0, &encoded[first]), chunk(1, &encoded[first + 1])];
            let encoded_bytes = chunks.iter().map(|entry| entry.encoded_bytes).sum();
            let decoded_bytes = chunks.iter().map(|entry| entry.decoded_bytes).sum();
            StreamingRowGroupV08 {
                row_start,
                row_count,
                encoded_bytes,
                decoded_bytes,
                arrow_allocation_upper_bound: decoded_bytes + 64,
                chunks,
            }
        };
        let directory = StreamingSegmentDirectoryV08 {
            row_groups: vec![row_group(0, 2, 0), row_group(2, 1, 2)],
        };
        let header = StreamingSegmentHeaderV08 {
            magic: STREAMING_SEGMENT_MAGIC_V08,
            format_version: STREAMING_SEGMENT_LAYOUT_VERSION_V08,
            row_count: 3,
            column_count: 2,
            row_group_count: 2,
            schema_checksum: metadata_checksum(&schema).unwrap(),
            directory_checksum: metadata_checksum(&directory).unwrap(),
        };
        Arc::new(TestProvider {
            header: envelope(header),
            schema: envelope(schema),
            directory: envelope(directory),
            chunks: HashMap::from([
                ((0, 0), encoded[0].clone()),
                ((0, 1), encoded[1].clone()),
                ((1, 0), encoded[2].clone()),
                ((1, 1), encoded[3].clone()),
            ]),
            chunk_reads: AtomicUsize::new(0),
        })
    }

    fn open(provider: Arc<TestProvider>) -> ChunkedSegmentAccessV08 {
        ChunkedSegmentAccessV08::open(provider, SegmentReferenceV08::new(7, 9)).unwrap()
    }

    #[test]
    fn reads_one_verified_chunk_without_reading_another() {
        let provider = fixture();
        let access = open(provider.clone());

        let chunk = access.read_verified_chunk(0, 1).unwrap();
        assert_eq!(chunk.metadata().column_index, 1);
        assert!(!chunk.into_bytes().is_empty());
        assert_eq!(provider.chunk_reads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn chunk_checksum_failure_is_reported_before_decode() {
        let mut provider = fixture();
        Arc::get_mut(&mut provider)
            .unwrap()
            .chunks
            .get_mut(&(0, 0))
            .unwrap()[0] ^= 0xFF;
        let access = open(provider);

        assert!(matches!(
            access.read_verified_chunk(0, 0),
            Err(ColumnarError::ChecksumMismatch)
        ));
    }

    #[test]
    fn invalid_row_range_fails_before_a_chunk_is_read() {
        let mut provider = fixture();
        let directory: ChecksummedMetadataV08<StreamingSegmentDirectoryV08> =
            bincode_config().deserialize(&provider.directory).unwrap();
        let mut directory = directory.value;
        directory.row_groups[1].row_start = 3;
        let inner = Arc::get_mut(&mut provider).unwrap();
        let header: ChecksummedMetadataV08<StreamingSegmentHeaderV08> =
            bincode_config().deserialize(&inner.header).unwrap();
        let mut header = header.value;
        header.directory_checksum = metadata_checksum(&directory).unwrap();
        inner.header = envelope(header);
        inner.directory = envelope(directory);

        assert!(matches!(
            ChunkedSegmentAccessV08::open(provider.clone(), SegmentReferenceV08::new(7, 9)),
            Err(ColumnarError::InvalidFormat(_))
        ));
        assert_eq!(provider.chunk_reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn directory_upper_bound_smaller_than_decode_is_rejected() {
        let mut provider = fixture();
        let directory: ChecksummedMetadataV08<StreamingSegmentDirectoryV08> =
            bincode_config().deserialize(&provider.directory).unwrap();
        let mut directory = directory.value;
        directory.row_groups[0].arrow_allocation_upper_bound = 0;
        let inner = Arc::get_mut(&mut provider).unwrap();
        let header: ChecksummedMetadataV08<StreamingSegmentHeaderV08> =
            bincode_config().deserialize(&inner.header).unwrap();
        let mut header = header.value;
        header.directory_checksum = metadata_checksum(&directory).unwrap();
        inner.header = envelope(header);
        inner.directory = envelope(directory);

        assert!(matches!(
            ChunkedSegmentAccessV08::open(provider, SegmentReferenceV08::new(7, 9)),
            Err(ColumnarError::InvalidFormat(_))
        ));
    }

    #[test]
    fn row_group_is_decoded_in_requested_column_order() {
        let provider = fixture();
        let access = open(provider);
        let batch = access.read_row_group(0, &[1, 0]).unwrap();

        assert_eq!(batch.schema.columns[0].name, "value");
        assert_eq!(batch.schema.columns[1].name, "id");
        assert_eq!(batch.num_rows(), 2);
        assert!(matches!(&batch.columns[0], Column::Int64(values) if values == &vec![10, 20]));
        assert!(matches!(&batch.columns[1], Column::Int64(values) if values == &vec![1, 2]));
    }
}
