use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor, Type};

use crate::executor::{ExecutorError, Result};
use crate::planner::types::ResolvedType;

use super::{CopyField, parquet_io_error};

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataContext {
    Unknown,
    AesGcmCtrV1,
    AesGcmV1,
    BsonType,
    ColumnChunk,
    ColumnCryptoMetaData,
    ColumnEncoding,
    ColumnHistogram,
    ColumnMetaData,
    ColumnOrder,
    DataPageHeader,
    DataPageHeaderV2,
    DateType,
    DecimalType,
    DictionaryPageHeader,
    EncryptionAlgorithm,
    EncryptionWithColumnKey,
    EncryptionWithFooterKey,
    EnumType,
    FileKeyValue,
    FileMetaData,
    Float16Type,
    IndexPageHeader,
    IntType,
    JsonType,
    KeyValue,
    ListType,
    LogicalType,
    MapType,
    MicroSeconds,
    MilliSeconds,
    NanoSeconds,
    NullType,
    PageEncodingStats,
    PageHeader,
    RetainedMetadata,
    RetainedStatistics,
    RowGroup,
    SchemaElement,
    SizeStatistics,
    SortingColumn,
    Statistics,
    StringType,
    TimeType,
    TimeUnit,
    TimestampType,
    TypeDefinedOrder,
    UuidType,
}

fn arc_bytes<T>() -> usize {
    std::mem::size_of::<(usize, usize, T)>()
}

fn validate_arrow_field(field: arrow_ipc::Field<'_>) -> Result<()> {
    use arrow_ipc::{TimeUnit, Type};
    let time_unit = |unit| {
        matches!(
            unit,
            TimeUnit::SECOND | TimeUnit::MILLISECOND | TimeUnit::MICROSECOND | TimeUnit::NANOSECOND
        )
    };
    let integer = |value: arrow_ipc::Int<'_>| matches!(value.bitWidth(), 8 | 16 | 32 | 64);
    if let Some(dictionary) = field.dictionary()
        && !dictionary.indexType().is_some_and(integer)
    {
        return Err(invalid_page());
    }
    let children = field.children().map_or(0, |children| children.len());
    // FlatBuffers validates structure; Arrow 53's owned converter assumes these semantics.
    let valid = match field.type_type() {
        Type::Null
        | Type::Bool
        | Type::Binary
        | Type::LargeBinary
        | Type::BinaryView
        | Type::Utf8
        | Type::LargeUtf8
        | Type::Utf8View
        | Type::Struct_ => true,
        Type::Int => field.type_as_int().is_some_and(integer),
        Type::FloatingPoint => field.type_as_floating_point().is_some_and(|value| {
            matches!(
                value.precision(),
                arrow_ipc::Precision::HALF
                    | arrow_ipc::Precision::SINGLE
                    | arrow_ipc::Precision::DOUBLE
            )
        }),
        Type::Date => field.type_as_date().is_some_and(|value| {
            matches!(
                value.unit(),
                arrow_ipc::DateUnit::DAY | arrow_ipc::DateUnit::MILLISECOND
            )
        }),
        Type::Time => field.type_as_time().is_some_and(|value| {
            matches!(
                (value.bitWidth(), value.unit()),
                (32, TimeUnit::SECOND | TimeUnit::MILLISECOND)
                    | (64, TimeUnit::MICROSECOND | TimeUnit::NANOSECOND)
            )
        }),
        Type::Timestamp => field
            .type_as_timestamp()
            .is_some_and(|value| time_unit(value.unit())),
        Type::Duration => field
            .type_as_duration()
            .is_some_and(|value| time_unit(value.unit())),
        Type::Interval => field.type_as_interval().is_some_and(|value| {
            matches!(
                value.unit(),
                arrow_ipc::IntervalUnit::YEAR_MONTH
                    | arrow_ipc::IntervalUnit::DAY_TIME
                    | arrow_ipc::IntervalUnit::MONTH_DAY_NANO
            )
        }),
        Type::FixedSizeBinary => field
            .type_as_fixed_size_binary()
            .is_some_and(|value| value.byteWidth() >= 0),
        Type::Decimal => field.type_as_decimal().is_some_and(|value| {
            matches!(value.bitWidth(), 128 | 256)
                && u8::try_from(value.precision()).is_ok()
                && i8::try_from(value.scale()).is_ok()
        }),
        Type::List | Type::LargeList => children == 1,
        Type::FixedSizeList => {
            children == 1
                && field
                    .type_as_fixed_size_list()
                    .is_some_and(|value| value.listSize() >= 0)
        }
        Type::Map => children == 1 && field.type_as_map().is_some(),
        Type::RunEndEncoded => children == 2,
        Type::Union => field.type_as_union().is_some_and(|value| {
            if !matches!(
                value.mode(),
                arrow_ipc::UnionMode::Dense | arrow_ipc::UnionMode::Sparse
            ) {
                return false;
            }
            if let Some(ids) = value.typeIds() {
                let mut seen = 0_u128;
                ids.len() == children
                    && ids.iter().all(|id| {
                        let Ok(id) = i8::try_from(id) else {
                            return false;
                        };
                        if id < 0 {
                            return false;
                        }
                        let bit = 1_u128 << id;
                        let unique = seen & bit == 0;
                        seen |= bit;
                        unique
                    })
            } else {
                children <= i8::MAX as usize
            }
        }),
        _ => false,
    };
    if !valid {
        return Err(invalid_page());
    }
    Ok(())
}

#[cold]
fn invalid_page() -> ExecutorError {
    ExecutorError::BulkLoad("invalid parquet page or chunk declaration".into())
}

pub(super) fn check_declared_bytes(bytes: u64, limit: u64) -> Result<()> {
    if bytes > limit {
        return Err(ExecutorError::ResourceExhausted {
            message: format!(
                "READ_PARQUET declares {bytes} bytes (limit {limit}); spilling this operator is not supported"
            ),
        });
    }
    Ok(())
}

pub(super) struct FooterReservation {
    pub(super) start: u64,
    pub(super) size: u64,
    pub(super) retained_metadata_bytes: u64,
    pub(super) has_arrow_schema: bool,
    pub(super) row_groups: Option<std::ops::Range<u64>>,
}

pub(super) fn check_footer(file: &mut File, limit: u64) -> Result<FooterReservation> {
    let length = file.metadata().map_err(parquet_io_error)?.len();
    let trailer_start = length.checked_sub(8).ok_or_else(invalid_page)?;
    file.seek(SeekFrom::Start(trailer_start))
        .map_err(parquet_io_error)?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer).map_err(parquet_io_error)?;
    if &trailer[4..] != b"PAR1" {
        return Err(invalid_page());
    }
    let size = u64::from(u32::from_le_bytes(trailer[..4].try_into().unwrap()));
    check_declared_bytes(size, limit)?;
    let start = trailer_start
        .checked_sub(size)
        .filter(|start| *start >= 4)
        .ok_or_else(invalid_page)?;
    file.seek(SeekFrom::Start(start))
        .map_err(parquet_io_error)?;
    let mut reservation = check_metadata(
        BufReader::with_capacity(size.min(4096) as usize, (&mut *file).take(size)),
        size,
        limit,
    )?;
    reservation.start = start;
    if let Some(groups) = &mut reservation.row_groups {
        groups.start += start;
        groups.end += start;
    }
    file.seek(SeekFrom::Start(0)).map_err(parquet_io_error)?;
    Ok(reservation)
}

pub(super) fn check_schema_projection(bytes: &[u8], limit: u64) -> Result<bool> {
    let size = bytes.len() as u64;
    check_declared_bytes(size, limit)?;
    let reservation = check_metadata(
        BufReader::with_capacity(0, std::io::Cursor::new(bytes).take(size)),
        size,
        limit,
    )?;
    Ok(reservation.has_arrow_schema)
}

fn check_metadata<R: Read>(
    input: BufReader<std::io::Take<R>>,
    size: u64,
    limit: u64,
) -> Result<FooterReservation> {
    let mut footer = CompactHeader {
        input,
        maximum: limit,
        decoded_bytes: size,
        retained_key_values: 0,
        retained_other_metadata: 0,
        schema_path: Vec::new(),
        schema_root_seen: false,
        schema_leaves: 0,
        has_arrow_schema: false,
        arrow_schema: None,
        file_key_values: 0,
    };
    let roots = arc_bytes::<parquet::file::metadata::ParquetMetaData>()
        + arc_bytes::<SchemaDescriptor>()
        + arc_bytes::<parquet::arrow::FieldLevels>()
        + 2 * arc_bytes::<arrow_schema::Schema>();
    footer.charge_metadata(roots as u64, MetadataContext::RetainedMetadata)?;
    let row_groups = footer.metadata_fields(0, MetadataContext::FileMetaData)?;
    if footer
        .schema_path
        .iter()
        .any(|(remaining, _)| *remaining != 0)
    {
        return Err(invalid_page());
    }
    for width in [
        std::mem::size_of::<Arc<ColumnDescriptor>>(),
        std::mem::size_of::<usize>(),
        std::mem::size_of::<CopyField>(),
        std::mem::size_of::<ResolvedType>(),
    ] {
        footer.reserve_converted_metadata(footer.schema_leaves, width)?;
    }
    if let Some(encoded) = footer.arrow_schema.take() {
        footer.reserve_arrow_schema(&encoded)?;
    }
    let has_arrow_schema = footer.has_arrow_schema;
    let retained_metadata_bytes = footer
        .retained_key_values
        .checked_mul(if has_arrow_schema { 2 } else { 1 })
        .and_then(|bytes| bytes.checked_add(footer.retained_other_metadata))
        .ok_or_else(invalid_page)?;
    check_declared_bytes(retained_metadata_bytes, limit)?;
    drop(footer);
    Ok(FooterReservation {
        start: 0,
        size,
        retained_metadata_bytes,
        has_arrow_schema,
        row_groups: row_groups.map(|(begin, end)| size - begin..size - end),
    })
}

pub(super) fn check_pages(
    file: &mut (impl Read + Seek),
    metadata: &parquet::file::metadata::ParquetMetaData,
    footer_start: u64,
    limit: u64,
    mut reserved_bytes: u64,
) -> Result<u64> {
    // Transient buffering uses only headroom not already reserved for retained data.
    let capacity = limit.saturating_sub(reserved_bytes).min(4096) as usize;
    let mut input = BufReader::with_capacity(capacity, file);
    let mut previous_end = None;
    for group in metadata.row_groups() {
        for column in group.columns() {
            let offset = column
                .dictionary_page_offset()
                .unwrap_or(column.data_page_offset());
            let start = u64::try_from(offset).map_err(|_| invalid_page())?;
            let size = u64::try_from(column.compressed_size()).map_err(|_| invalid_page())?;
            let end = start
                .checked_add(size)
                .filter(|end| *end <= footer_start)
                .ok_or_else(invalid_page)?;
            if start < 4 {
                return Err(invalid_page());
            }
            if previous_end != Some(start) {
                input
                    .seek(SeekFrom::Start(start))
                    .map_err(parquet_io_error)?;
            }
            let mut position = start;
            while position < end {
                let maximum = (end - position).min(limit);
                let mut header = CompactHeader {
                    input: BufReader::with_capacity(0, (&mut input).take(maximum)),
                    maximum,
                    decoded_bytes: 0,
                    retained_key_values: 0,
                    retained_other_metadata: 0,
                    schema_path: Vec::new(),
                    schema_root_seen: false,
                    schema_leaves: 0,
                    has_arrow_schema: false,
                    arrow_schema: None,
                    file_key_values: 0,
                };
                let (uncompressed, compressed, dictionary_values) = header.page()?;
                let consumed = maximum - header.remaining();
                drop(header);
                check_declared_bytes(uncompressed, limit)?;
                check_declared_bytes(compressed, limit)?;
                // Bound the header copy, decoded binary fields, and moved statistics owners.
                let header_storage = consumed
                    .checked_mul(2)
                    .and_then(|bytes| {
                        bytes.checked_add((2 * std::mem::size_of::<bytes::Bytes>()) as u64)
                    })
                    .and_then(|bytes| {
                        bytes.checked_add(std::mem::size_of::<parquet::format::PageHeader>() as u64)
                    })
                    .ok_or_else(invalid_page)?;
                reserved_bytes = reserved_bytes
                    .checked_add(header_storage)
                    .and_then(|bytes| bytes.checked_add(uncompressed))
                    .and_then(|bytes| bytes.checked_add(compressed))
                    .ok_or_else(invalid_page)?;
                if let Some(count) = dictionary_values {
                    use parquet::basic::Type;
                    use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
                    let width = match column.column_type() {
                        Type::BOOLEAN => std::mem::size_of::<bool>(),
                        Type::INT32 => std::mem::size_of::<i32>(),
                        Type::INT64 => std::mem::size_of::<i64>(),
                        Type::INT96 => std::mem::size_of::<Int96>(),
                        Type::FLOAT => std::mem::size_of::<f32>(),
                        Type::DOUBLE => std::mem::size_of::<f64>(),
                        Type::BYTE_ARRAY => std::mem::size_of::<ByteArray>(),
                        Type::FIXED_LEN_BYTE_ARRAY => std::mem::size_of::<FixedLenByteArray>(),
                    };
                    // DictDecoder reserves every declared entry before validating its payload.
                    let bytes = count.checked_mul(width as u64).ok_or_else(invalid_page)?;
                    check_declared_bytes(bytes, limit)?;
                    reserved_bytes = reserved_bytes.checked_add(bytes).ok_or_else(invalid_page)?;
                }
                // Reserve page storage together with retained rows before any page decoding.
                check_declared_bytes(reserved_bytes, limit)?;
                position = position
                    .checked_add(consumed)
                    .and_then(|p| p.checked_add(compressed))
                    .filter(|p| *p <= end)
                    .ok_or_else(invalid_page)?;
                input
                    .seek_relative(i64::try_from(compressed).map_err(|_| invalid_page())?)
                    .map_err(parquet_io_error)?;
            }
            previous_end = Some(end);
        }
    }
    Ok(reserved_bytes)
}

pub(super) fn read_page_header_bytes(
    file: &mut File,
    start: u64,
    end: u64,
    limit: u64,
) -> Result<Vec<u8>> {
    let maximum = end.checked_sub(start).ok_or_else(invalid_page)?.min(limit);
    file.seek(SeekFrom::Start(start))
        .map_err(parquet_io_error)?;
    let mut header = CompactHeader {
        input: BufReader::with_capacity(maximum.min(4096) as usize, (&mut *file).take(maximum)),
        maximum,
        decoded_bytes: 0,
        retained_key_values: 0,
        retained_other_metadata: 0,
        schema_path: Vec::new(),
        schema_root_seen: false,
        schema_leaves: 0,
        has_arrow_schema: false,
        arrow_schema: None,
        file_key_values: 0,
    };
    let (uncompressed, compressed, _) = header.page()?;
    check_declared_bytes(uncompressed, limit)?;
    check_declared_bytes(compressed, limit)?;
    let consumed = maximum - header.remaining();
    start
        .checked_add(consumed)
        .and_then(|p| p.checked_add(compressed))
        .filter(|p| *p <= end)
        .ok_or_else(invalid_page)?;
    drop(header);
    let mut bytes = vec![0; usize::try_from(consumed).map_err(|_| invalid_page())?];
    file.seek(SeekFrom::Start(start))
        .map_err(parquet_io_error)?;
    file.read_exact(&mut bytes).map_err(parquet_io_error)?;
    let mut copied = CompactHeader {
        input: BufReader::with_capacity(
            bytes.len().min(4096),
            std::io::Cursor::new(&bytes).take(consumed),
        ),
        maximum: consumed,
        decoded_bytes: 0,
        retained_key_values: 0,
        retained_other_metadata: 0,
        schema_path: Vec::new(),
        schema_root_seen: false,
        schema_leaves: 0,
        has_arrow_schema: false,
        arrow_schema: None,
        file_key_values: 0,
    };
    let (uncompressed, compressed, _) = copied.page()?;
    check_declared_bytes(uncompressed, limit)?;
    check_declared_bytes(compressed, limit)?;
    if copied.remaining() != 0 {
        return Err(invalid_page());
    }
    start
        .checked_add(consumed)
        .and_then(|p| p.checked_add(compressed))
        .filter(|p| *p <= end)
        .ok_or_else(invalid_page)?;
    drop(copied);
    let mut typed = CompactHeader {
        input: BufReader::with_capacity(
            bytes.len().min(4096),
            std::io::Cursor::new(&bytes).take(consumed),
        ),
        maximum: consumed,
        decoded_bytes: 0,
        retained_key_values: 0,
        retained_other_metadata: 0,
        schema_path: Vec::new(),
        schema_root_seen: false,
        schema_leaves: 0,
        has_arrow_schema: false,
        arrow_schema: None,
        file_key_values: 0,
    };
    let _ = typed.metadata_fields(0, MetadataContext::PageHeader)?;
    if typed.remaining() != 0 {
        return Err(invalid_page());
    }
    drop(typed);
    Ok(bytes)
}

// Skip compact Thrift values without allocating statistics or unknown-field payloads.
struct CompactHeader<R: Read> {
    input: BufReader<std::io::Take<R>>,
    maximum: u64,
    decoded_bytes: u64,
    retained_key_values: u64,
    retained_other_metadata: u64,
    schema_path: Vec<(u64, u64)>,
    schema_root_seen: bool,
    schema_leaves: u64,
    has_arrow_schema: bool,
    arrow_schema: Option<Vec<u8>>,
    file_key_values: u64,
}

impl<R: Read> CompactHeader<R> {
    fn charge_metadata(&mut self, bytes: u64, context: MetadataContext) -> Result<()> {
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(bytes)
            .ok_or_else(invalid_page)?;
        check_declared_bytes(self.decoded_bytes, self.maximum)?;
        if context == MetadataContext::FileKeyValue {
            self.retained_key_values = self
                .retained_key_values
                .checked_add(bytes)
                .ok_or_else(invalid_page)?;
        }
        if context == MetadataContext::RetainedMetadata {
            self.retained_other_metadata = self
                .retained_other_metadata
                .checked_add(bytes)
                .ok_or_else(invalid_page)?;
        }
        Ok(())
    }

    fn reserve_converted_metadata(&mut self, count: u64, width: usize) -> Result<()> {
        if count == 0 || width == 0 {
            return Ok(());
        }
        // Parquet 53 pushes into empty Vecs; account for their retained growth capacity.
        let initial = if width == 1 {
            8
        } else if width <= 1024 {
            4
        } else {
            1
        };
        let capacity = count
            .checked_next_power_of_two()
            .ok_or_else(invalid_page)?
            .max(initial);
        let bytes = capacity
            .checked_mul(width as u64)
            .ok_or_else(invalid_page)?;
        self.charge_metadata(bytes, MetadataContext::RetainedMetadata)?;
        if count > initial {
            // A relocating growth may keep the old converted backing alive temporarily.
            let previous = (capacity / 2)
                .checked_mul(width as u64)
                .ok_or_else(invalid_page)?;
            self.charge_metadata(previous, MetadataContext::Unknown)?;
        }
        Ok(())
    }

    fn metadata_binary_length(&mut self, context: MetadataContext) -> Result<u64> {
        let length = self.varint()?;
        check_declared_bytes(length, self.maximum)?;
        if length > self.remaining() {
            return Err(invalid_page());
        }
        let statistics = context == MetadataContext::RetainedStatistics;
        let context = if statistics {
            MetadataContext::RetainedMetadata
        } else {
            context
        };
        self.charge_metadata(length, context)?;
        if statistics {
            self.charge_metadata(std::mem::size_of::<bytes::Bytes>() as u64, context)?;
        }
        Ok(length)
    }

    fn metadata_binary(
        &mut self,
        context: MetadataContext,
        inspect_arrow_key: bool,
    ) -> Result<(u64, bool)> {
        let length = self.metadata_binary_length(context)?;
        let schema_key = parquet::arrow::ARROW_SCHEMA_META_KEY.as_bytes();
        let mut arrow_key = inspect_arrow_key && length == schema_key.len() as u64;
        if !arrow_key {
            self.skip_bytes(length)?;
            return Ok((length, false));
        }
        for index in 0..length {
            let byte = self.byte()?;
            if arrow_key && schema_key.get(index as usize) != Some(&byte) {
                arrow_key = false;
            }
        }
        Ok((length, arrow_key))
    }

    fn metadata_binary_copy(&mut self, context: MetadataContext) -> Result<Vec<u8>> {
        let length = self.metadata_binary_length(context)?;
        let length = usize::try_from(length).map_err(|_| invalid_page())?;
        let mut value = Vec::new();
        value
            .try_reserve_exact(length)
            .map_err(|_| invalid_page())?;
        value.resize(length, 0);
        self.input
            .read_exact(&mut value)
            .map_err(parquet_io_error)?;
        Ok(value)
    }

    fn reserve_arrow_map(&mut self, count: u64) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        // std HashMap uses four buckets for up to three entries, then 7/8 occupancy.
        let buckets = if count < 4 {
            4
        } else if count < 8 {
            8
        } else {
            count
                .checked_mul(8)
                .and_then(|v| v.checked_div(7))
                .and_then(u64::checked_next_power_of_two)
                .ok_or_else(invalid_page)?
        };
        let table = buckets
            .checked_mul((std::mem::size_of::<(String, String)>() + 1) as u64)
            .and_then(|v| v.checked_add(16))
            .ok_or_else(invalid_page)?;
        // Original IPC metadata and the converted reader field can coexist.
        self.charge_metadata(
            table.checked_mul(2).ok_or_else(invalid_page)?,
            MetadataContext::RetainedMetadata,
        )?;
        // Rehashing can retain the preceding smaller table until relocation finishes.
        self.charge_metadata(table, MetadataContext::Unknown)
    }

    fn reserve_arrow_key_values<'a>(
        &mut self,
        values: impl Iterator<Item = arrow_ipc::KeyValue<'a>>,
    ) -> Result<u64> {
        let mut count = 0_u64;
        for value in values {
            if let (Some(key), Some(value)) = (value.key(), value.value()) {
                count = count.checked_add(1).ok_or_else(invalid_page)?;
                let bytes = key
                    .len()
                    .checked_add(value.len())
                    .and_then(|v| v.checked_mul(2))
                    .ok_or_else(invalid_page)?;
                self.charge_metadata(bytes as u64, MetadataContext::RetainedMetadata)?;
            }
        }
        self.reserve_arrow_map(count)?;
        Ok(count)
    }

    fn reserve_arrow_fields<'a>(
        &mut self,
        fields: impl Iterator<Item = arrow_ipc::Field<'a>>,
        depth: usize,
    ) -> Result<()> {
        if depth > 32 {
            return Err(invalid_page());
        }
        let mut count = 0_u64;
        for field in fields {
            validate_arrow_field(field)?;
            count = count.checked_add(1).ok_or_else(invalid_page)?;
            let name = field.name().ok_or_else(invalid_page)?;
            let owners = (2 * arc_bytes::<arrow_schema::Field>())
                .checked_add(name.len().checked_mul(2).ok_or_else(invalid_page)?)
                .ok_or_else(invalid_page)?;
            // Visit each occurrence: IPC tables may be shared, owned Arrow Fields are not.
            self.charge_metadata(owners as u64, MetadataContext::RetainedMetadata)?;
            self.reserve_arrow_key_values(
                field.custom_metadata().into_iter().flat_map(|v| v.iter()),
            )?;
            if field.dictionary().is_some() {
                self.charge_metadata(
                    (6 * std::mem::size_of::<arrow_schema::DataType>()) as u64,
                    MetadataContext::RetainedMetadata,
                )?;
            }
            if let Some(timestamp) = field.type_as_timestamp()
                && let Some(zone) = timestamp.timezone()
            {
                let bytes = zone
                    .len()
                    .checked_add(2 * std::mem::size_of::<usize>())
                    .and_then(|v| v.checked_mul(2))
                    .ok_or_else(invalid_page)?;
                self.charge_metadata(bytes as u64, MetadataContext::RetainedMetadata)?;
            }
            if let Some(union) = field.type_as_union()
                && let Some(ids) = union.typeIds()
            {
                self.reserve_converted_metadata(ids.len() as u64, std::mem::size_of::<i8>())?;
            }
            if let Some(children) = field.children() {
                self.reserve_arrow_fields(children.iter(), depth + 1)?;
            }
        }
        // IPC first builds Vec<Field>; nested Fields additionally retain Arc-backed slots.
        self.reserve_converted_metadata(count, std::mem::size_of::<arrow_schema::Field>())?;
        for _ in 0..2 {
            self.reserve_converted_metadata(count, std::mem::size_of::<arrow_schema::FieldRef>())?;
            if count != 0 {
                self.charge_metadata(
                    (2 * std::mem::size_of::<usize>()) as u64,
                    MetadataContext::RetainedMetadata,
                )?;
            }
        }
        Ok(())
    }

    fn reserve_arrow_schema(&mut self, encoded: &[u8]) -> Result<()> {
        use base64::Engine;
        let capacity = base64::decoded_len_estimate(encoded.len());
        self.charge_metadata(capacity as u64, MetadataContext::Unknown)?;
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(capacity)
            .map_err(|_| invalid_page())?;
        decoded.resize(capacity, 0);
        let length = base64::engine::general_purpose::STANDARD
            .decode_slice(encoded, &mut decoded)
            .map_err(|_| invalid_page())?;
        decoded.truncate(length);
        // Match Parquet 53's continuation-marker and legacy raw-message boundary.
        let message = if decoded.len() > 8 && decoded[..4] == [255; 4] {
            &decoded[8..]
        } else {
            &decoded
        };
        let message = arrow_ipc::root_as_message(message).map_err(|_| invalid_page())?;
        let schema = message.header_as_schema().ok_or_else(invalid_page)?;
        let count = self.reserve_arrow_key_values(
            schema.custom_metadata().into_iter().flat_map(|v| v.iter()),
        )?;
        // File key-values are cloned into the map that receives IPC root metadata.
        self.reserve_arrow_map(
            self.file_key_values
                .checked_add(count)
                .ok_or_else(invalid_page)?,
        )?;
        let fields = schema.fields().ok_or_else(invalid_page)?;
        if schema.endianness() == arrow_ipc::Endianness::Big
            && fields
                .iter()
                .any(|field| field.type_type() == arrow_ipc::Type::Decimal)
        {
            return Err(invalid_page());
        }
        self.reserve_arrow_fields(fields.iter(), 0)
    }

    fn schema_name(
        &mut self,
        length: u64,
        children: Option<u64>,
        field_id: Option<i32>,
    ) -> Result<()> {
        let root = !self.schema_root_seen;
        self.charge_metadata(
            arc_bytes::<Type>() as u64,
            MetadataContext::RetainedMetadata,
        )?;
        if !root {
            self.charge_metadata(
                (2 * arc_bytes::<arrow_schema::Field>()) as u64,
                MetadataContext::RetainedMetadata,
            )?;
        }
        if !root && field_id.is_some() {
            // Parquet 53 owns an inferred map and a reader clone; std hashbrown uses
            // four buckets plus controls (at most 16 bytes per SIMD group).
            let table = 4 * std::mem::size_of::<(String, String)>() + 4 + 16;
            let strings = parquet::arrow::PARQUET_FIELD_ID_META_KEY.len() + "-2147483648".len();
            self.charge_metadata(
                (2 * (table + strings)) as u64,
                MetadataContext::RetainedMetadata,
            )?;
        }
        let (prefix, mut retained) = if root {
            self.schema_root_seen = true;
            if children.is_none() {
                return Err(invalid_page());
            }
            (0, length)
        } else {
            while self.schema_path.last().is_some_and(|(left, _)| *left == 0) {
                self.schema_path.pop();
            }
            let (left, prefix) = self.schema_path.last_mut().ok_or_else(invalid_page)?;
            *left -= 1;
            // Type, inferred Arrow field, rebuilt reader field, and SQL CopyField own names.
            (*prefix, length.checked_mul(4).ok_or_else(invalid_page)?)
        };
        let path = if self.schema_path.is_empty() {
            0 // SchemaDescriptor paths exclude the message name.
        } else {
            prefix.checked_add(length).ok_or_else(invalid_page)?
        };
        if let Some(children) = children.filter(|children| root || *children != 0) {
            self.reserve_converted_metadata(children, std::mem::size_of::<Arc<Type>>())?;
            // FieldLevels contains the private ParquetField inline and bounds its slot size.
            self.reserve_converted_metadata(
                children,
                std::mem::size_of::<parquet::arrow::FieldLevels>(),
            )?;
            for _ in 0..2 {
                self.reserve_converted_metadata(
                    children,
                    std::mem::size_of::<arrow_schema::FieldRef>(),
                )?;
                self.charge_metadata(
                    (2 * std::mem::size_of::<usize>()) as u64,
                    MetadataContext::RetainedMetadata,
                )?;
            }
            // The already charged SchemaElement slots also bound this smaller traversal stack.
            self.schema_path
                .try_reserve_exact(1)
                .map_err(|_| invalid_page())?;
            self.schema_path.push((children, path));
        } else {
            self.schema_leaves = self.schema_leaves.checked_add(1).ok_or_else(invalid_page)?;
            self.charge_metadata(
                arc_bytes::<ColumnDescriptor>() as u64,
                MetadataContext::RetainedMetadata,
            )?;
            self.reserve_converted_metadata(
                self.schema_path.len() as u64,
                std::mem::size_of::<String>(),
            )?;
            // Each primitive ColumnDescriptor owns the names of all its ancestors.
            retained = retained.checked_add(path).ok_or_else(invalid_page)?;
        }
        self.charge_metadata(retained, MetadataContext::RetainedMetadata)
    }

    fn remaining(&self) -> u64 {
        self.input.get_ref().limit() + self.input.buffer().len() as u64
    }

    #[inline]
    fn byte(&mut self) -> Result<u8> {
        if let Some(&byte) = self.input.buffer().first() {
            self.input.consume(1);
            return Ok(byte);
        }
        self.read_byte()
    }

    // Keep I/O and its error formatting out of buffered scalar reads.
    #[inline]
    fn read_byte(&mut self) -> Result<u8> {
        let mut byte = [0];
        self.input.read_exact(&mut byte).map_err(parquet_io_error)?;
        Ok(byte[0])
    }

    fn skip_bytes(&mut self, mut length: u64) -> Result<()> {
        // Reuse the existing buffer rather than decoding opaque payloads byte by byte.
        while length != 0 {
            let buffered = self.input.buffer().len() as u64;
            if buffered == 0 {
                self.byte()?;
                length -= 1;
            } else {
                let consumed = length.min(buffered);
                self.input.consume(consumed as usize);
                length -= consumed;
            }
        }
        Ok(())
    }

    #[inline]
    fn varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(invalid_page());
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(invalid_page())
    }

    fn integer(&mut self) -> Result<i64> {
        let value = self.varint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    #[inline]
    fn field_id(&mut self, field: u8, previous: i16) -> Result<i16> {
        let id = if field >> 4 == 0 {
            self.integer()?
        } else {
            i64::from(previous) + i64::from(field >> 4)
        };
        i16::try_from(id).map_err(|_| invalid_page())
    }

    fn metadata_fields(
        &mut self,
        depth: usize,
        context: MetadataContext,
    ) -> Result<Option<(u64, u64)>> {
        if depth > 32 {
            return Err(invalid_page());
        }
        let mut id = 0_i16;
        let mut schema_name = None;
        let mut schema_children = None;
        let mut schema_field_id = None;
        let mut arrow_key = false;
        let mut metadata_value = None;
        let mut row_groups = None;
        let mut row_groups_seen = false;
        loop {
            let field = self.byte()?;
            if field == 0 {
                if context == MetadataContext::SchemaElement {
                    self.schema_name(
                        schema_name.ok_or_else(invalid_page)?,
                        schema_children,
                        schema_field_id,
                    )?;
                }
                if context == MetadataContext::FileKeyValue && arrow_key && metadata_value.is_some()
                {
                    self.arrow_schema = metadata_value;
                    self.has_arrow_schema = true;
                }
                return Ok(row_groups);
            }
            id = self.field_id(field, id)?;
            let kind = field & 15;
            let known = metadata_field(context, id);
            let (expected, child, width, element) =
                known.unwrap_or((kind, MetadataContext::Unknown, 0, 0));
            if kind != expected && !(expected == 1 && kind == 2) {
                return Err(invalid_page());
            }
            let begin = self.remaining();
            // Compact field booleans carry their value in the field tag.
            if context == MetadataContext::SchemaElement && id == 4 {
                schema_name = Some(self.metadata_binary(MetadataContext::Unknown, false)?.0);
            } else if context == MetadataContext::SchemaElement && id == 5 {
                let children = self.integer()?;
                let children = i32::try_from(children).map_err(|_| invalid_page())?;
                schema_children = Some(u64::try_from(children).map_err(|_| invalid_page())?);
            } else if context == MetadataContext::SchemaElement && id == 9 {
                schema_field_id = Some(i32::try_from(self.integer()?).map_err(|_| invalid_page())?);
            } else if context == MetadataContext::FileKeyValue && id == 1 {
                arrow_key = self.metadata_binary(context, true)?.1;
            } else if context == MetadataContext::FileKeyValue && id == 2 {
                // Thrift permits the value before its key; retain only the last matching value.
                metadata_value = Some(self.metadata_binary_copy(context)?);
            } else if kind != 1 && kind != 2 {
                let child = if known.is_some() && context == MetadataContext::FileKeyValue {
                    context
                } else {
                    child
                };
                self.metadata_value(kind, depth, child, width, element)?;
            }
            if context == MetadataContext::FileMetaData && id == 4 {
                // Repeated fields retain the ordinary decoder's last-value semantics.
                row_groups = if row_groups_seen {
                    None
                } else {
                    Some((begin, self.remaining()))
                };
                row_groups_seen = true;
            }
        }
    }

    fn metadata_value(
        &mut self,
        kind: u8,
        depth: usize,
        context: MetadataContext,
        width: usize,
        element: u8,
    ) -> Result<()> {
        if depth > 32 {
            return Err(invalid_page());
        }
        match kind {
            1..=3 => {
                self.byte()?;
            }
            4..=6 => {
                self.varint()?;
            }
            7 => {
                self.skip_bytes(8)?;
            }
            8 => {
                self.metadata_binary(context, false)?;
            }
            9 | 10 => {
                let list = self.byte()?;
                let count = if list >> 4 == 15 {
                    self.varint()?
                } else {
                    u64::from(list >> 4)
                };
                i32::try_from(count).map_err(|_| invalid_page())?;
                let kind = list & 15;
                if element != 0 && kind != element && !(element == 1 && kind == 2) {
                    return Err(invalid_page());
                }
                let bytes = count.checked_mul(width as u64).ok_or_else(invalid_page)?;
                check_declared_bytes(bytes, self.maximum)?;
                if count > self.remaining() {
                    return Err(invalid_page());
                }
                self.charge_metadata(
                    bytes,
                    if matches!(
                        context,
                        MetadataContext::SortingColumn | MetadataContext::ColumnHistogram
                    ) {
                        MetadataContext::RetainedMetadata
                    } else {
                        context
                    },
                )?;
                let converted_width = match context {
                    MetadataContext::RowGroup => {
                        std::mem::size_of::<parquet::file::metadata::RowGroupMetaData>()
                    }
                    MetadataContext::ColumnChunk => {
                        std::mem::size_of::<parquet::file::metadata::ColumnChunkMetaData>()
                    }
                    MetadataContext::ColumnOrder => {
                        std::mem::size_of::<parquet::basic::ColumnOrder>()
                    }
                    MetadataContext::ColumnEncoding => {
                        std::mem::size_of::<parquet::basic::Encoding>()
                    }
                    MetadataContext::PageEncodingStats => {
                        std::mem::size_of::<parquet::file::page_encoding_stats::PageEncodingStats>()
                    }
                    _ => 0,
                };
                self.reserve_converted_metadata(count, converted_width)?;
                if context == MetadataContext::FileKeyValue {
                    self.arrow_schema = None;
                    self.has_arrow_schema = false;
                    self.file_key_values = count;
                }
                for _ in 0..count {
                    self.metadata_value(kind, depth + 1, context, 0, 0)?;
                }
            }
            11 => {
                let count = self.varint()?;
                i32::try_from(count).map_err(|_| invalid_page())?;
                if count > self.remaining() {
                    return Err(invalid_page());
                }
                if count != 0 {
                    let kinds = self.byte()?;
                    for _ in 0..count {
                        self.metadata_value(kinds >> 4, depth + 1, MetadataContext::Unknown, 0, 0)?;
                        self.metadata_value(kinds & 15, depth + 1, MetadataContext::Unknown, 0, 0)?;
                    }
                }
            }
            12 => {
                let _ = self.metadata_fields(depth + 1, context)?;
            }
            _ => return Err(invalid_page()),
        }
        Ok(())
    }

    fn fields(
        &mut self,
        depth: usize,
        page: bool,
        dictionary: bool,
    ) -> Result<(u64, u64, Option<u64>)> {
        if depth > 32 {
            return Err(invalid_page());
        }
        let mut id = 0_i16;
        let mut sizes = [None, None];
        let mut page_type = None;
        let mut dictionary_values = None;
        loop {
            let field = self.byte()?;
            if field == 0 {
                break;
            }
            id = self.field_id(field, id)?;
            if (page && (1..=3).contains(&id)) || (dictionary && id == 1) {
                if field & 15 != 5 {
                    return Err(invalid_page());
                }
                let value = self.integer()?;
                let value = i32::try_from(value)
                    .ok()
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or_else(invalid_page)?;
                let slot = if dictionary {
                    &mut dictionary_values
                } else if id == 1 {
                    &mut page_type
                } else {
                    &mut sizes[(id - 2) as usize]
                };
                if slot.replace(value).is_some() {
                    return Err(invalid_page());
                }
            } else if page && id == 7 {
                if field & 15 != 12 || dictionary_values.is_some() {
                    return Err(invalid_page());
                }
                dictionary_values = self.fields(depth + 1, false, true)?.2;
            } else if field & 15 != 1 && field & 15 != 2 {
                self.skip(field & 15, depth)?;
            }
        }
        if page {
            Ok((
                sizes[0].ok_or_else(invalid_page)?,
                sizes[1].ok_or_else(invalid_page)?,
                if page_type == Some(2) {
                    Some(dictionary_values.ok_or_else(invalid_page)?)
                } else {
                    None
                },
            ))
        } else {
            if dictionary && dictionary_values.is_none() {
                return Err(invalid_page());
            }
            Ok((0, 0, dictionary_values))
        }
    }

    fn skip(&mut self, kind: u8, depth: usize) -> Result<()> {
        if depth > 32 {
            return Err(invalid_page());
        }
        match kind {
            1..=3 => {
                self.byte()?;
            }
            4..=6 => {
                self.varint()?;
            }
            7 => {
                self.skip_bytes(8)?;
            }
            8 => {
                let length = self.varint()?;
                if length > self.remaining() {
                    return Err(invalid_page());
                }
                self.skip_bytes(length)?;
            }
            9 | 10 => {
                let list = self.byte()?;
                let length = if list >> 4 == 15 {
                    self.varint()?
                } else {
                    u64::from(list >> 4)
                };
                if length > self.maximum {
                    return Err(invalid_page());
                }
                for _ in 0..length {
                    self.skip(list & 15, depth + 1)?;
                }
            }
            11 => {
                let length = self.varint()?;
                if length > self.maximum {
                    return Err(invalid_page());
                }
                if length != 0 {
                    let types = self.byte()?;
                    for _ in 0..length {
                        self.skip(types >> 4, depth + 1)?;
                        self.skip(types & 15, depth + 1)?;
                    }
                }
            }
            12 => {
                self.fields(depth + 1, false, false)?;
            }
            _ => return Err(invalid_page()),
        }
        Ok(())
    }

    fn page(&mut self) -> Result<(u64, u64, Option<u64>)> {
        self.fields(0, true, false)
    }
}

// Field and slot types follow the pinned Parquet format decoder to keep both readers aligned.
#[inline]
fn metadata_field(context: MetadataContext, id: i16) -> Option<(u8, MetadataContext, usize, u8)> {
    Some(match (context, id) {
        (MetadataContext::PageHeader, 1..=4) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::PageHeader, 5) => (12, MetadataContext::DataPageHeader, 0, 0),
        (MetadataContext::PageHeader, 6) => (12, MetadataContext::IndexPageHeader, 0, 0),
        (MetadataContext::PageHeader, 7) => (12, MetadataContext::DictionaryPageHeader, 0, 0),
        (MetadataContext::PageHeader, 8) => (12, MetadataContext::DataPageHeaderV2, 0, 0),
        (MetadataContext::DataPageHeader, 1..=4) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::DataPageHeader, 5) => (12, MetadataContext::Statistics, 0, 0),
        (MetadataContext::DataPageHeaderV2, 1..=6) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::DataPageHeaderV2, 7) => (1, MetadataContext::Unknown, 0, 0),
        (MetadataContext::DataPageHeaderV2, 8) => (12, MetadataContext::Statistics, 0, 0),
        (MetadataContext::DictionaryPageHeader, 1..=2) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::DictionaryPageHeader, 3) => (1, MetadataContext::Unknown, 0, 0),
        (MetadataContext::AesGcmCtrV1, 1) | (MetadataContext::AesGcmCtrV1, 2) => {
            (8, MetadataContext::Unknown, 0, 0)
        }
        (MetadataContext::AesGcmCtrV1, 3) => (1, MetadataContext::Unknown, 0, 0),
        (MetadataContext::AesGcmV1, 1) | (MetadataContext::AesGcmV1, 2) => {
            (8, MetadataContext::Unknown, 0, 0)
        }
        (MetadataContext::AesGcmV1, 3) => (1, MetadataContext::Unknown, 0, 0),
        (MetadataContext::ColumnChunk, 1) => (8, MetadataContext::RetainedMetadata, 0, 0),
        (MetadataContext::ColumnChunk, 9) => (8, MetadataContext::Unknown, 0, 0),
        (MetadataContext::ColumnChunk, 2)
        | (MetadataContext::ColumnChunk, 4)
        | (MetadataContext::ColumnChunk, 6) => (6, MetadataContext::Unknown, 0, 0),
        (MetadataContext::ColumnChunk, 3) => (12, MetadataContext::ColumnMetaData, 0, 0),
        (MetadataContext::ColumnChunk, 5) | (MetadataContext::ColumnChunk, 7) => {
            (5, MetadataContext::Unknown, 0, 0)
        }
        (MetadataContext::ColumnChunk, 8) => (12, MetadataContext::ColumnCryptoMetaData, 0, 0),
        (MetadataContext::ColumnCryptoMetaData, 1) => {
            (12, MetadataContext::EncryptionWithFooterKey, 0, 0)
        }
        (MetadataContext::ColumnCryptoMetaData, 2) => {
            (12, MetadataContext::EncryptionWithColumnKey, 0, 0)
        }
        (MetadataContext::ColumnMetaData, 1)
        | (MetadataContext::ColumnMetaData, 4)
        | (MetadataContext::ColumnMetaData, 15) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::ColumnMetaData, 2) => (
            9,
            MetadataContext::ColumnEncoding,
            std::mem::size_of::<parquet::format::Encoding>(),
            5,
        ),
        (MetadataContext::ColumnMetaData, 3) => (
            9,
            MetadataContext::Unknown,
            std::mem::size_of::<String>(),
            8,
        ),
        (MetadataContext::ColumnMetaData, 5)
        | (MetadataContext::ColumnMetaData, 6)
        | (MetadataContext::ColumnMetaData, 7)
        | (MetadataContext::ColumnMetaData, 9)
        | (MetadataContext::ColumnMetaData, 10)
        | (MetadataContext::ColumnMetaData, 11)
        | (MetadataContext::ColumnMetaData, 14) => (6, MetadataContext::Unknown, 0, 0),
        (MetadataContext::ColumnMetaData, 8) => (
            9,
            MetadataContext::KeyValue,
            std::mem::size_of::<parquet::format::KeyValue>(),
            12,
        ),
        (MetadataContext::ColumnMetaData, 12) => (12, MetadataContext::RetainedStatistics, 0, 0),
        (MetadataContext::ColumnMetaData, 13) => (
            9,
            MetadataContext::PageEncodingStats,
            std::mem::size_of::<parquet::format::PageEncodingStats>(),
            12,
        ),
        (MetadataContext::ColumnMetaData, 16) => (12, MetadataContext::SizeStatistics, 0, 0),
        (MetadataContext::ColumnOrder, 1) => (12, MetadataContext::TypeDefinedOrder, 0, 0),
        (MetadataContext::DecimalType, 1) | (MetadataContext::DecimalType, 2) => {
            (5, MetadataContext::Unknown, 0, 0)
        }
        (MetadataContext::EncryptionAlgorithm, 1) => (12, MetadataContext::AesGcmV1, 0, 0),
        (MetadataContext::EncryptionAlgorithm, 2) => (12, MetadataContext::AesGcmCtrV1, 0, 0),
        (MetadataContext::EncryptionWithColumnKey, 1) => (
            9,
            MetadataContext::Unknown,
            std::mem::size_of::<String>(),
            8,
        ),
        (MetadataContext::EncryptionWithColumnKey, 2) => (8, MetadataContext::Unknown, 0, 0),
        (MetadataContext::FileMetaData, 1) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::FileMetaData, 2) => (
            9,
            MetadataContext::SchemaElement,
            std::mem::size_of::<parquet::format::SchemaElement>(),
            12,
        ),
        (MetadataContext::FileMetaData, 3) => (6, MetadataContext::Unknown, 0, 0),
        (MetadataContext::FileMetaData, 4) => (
            9,
            MetadataContext::RowGroup,
            std::mem::size_of::<parquet::format::RowGroup>(),
            12,
        ),
        (MetadataContext::FileMetaData, 5) => (
            9,
            MetadataContext::FileKeyValue,
            std::mem::size_of::<parquet::format::KeyValue>(),
            12,
        ),
        (MetadataContext::FileMetaData, 6) => (8, MetadataContext::RetainedMetadata, 0, 0),
        (MetadataContext::FileMetaData, 9) => (8, MetadataContext::Unknown, 0, 0),
        (MetadataContext::FileMetaData, 7) => (
            9,
            MetadataContext::ColumnOrder,
            std::mem::size_of::<parquet::format::ColumnOrder>(),
            12,
        ),
        (MetadataContext::FileMetaData, 8) => (12, MetadataContext::EncryptionAlgorithm, 0, 0),
        (MetadataContext::IntType, 1) => (3, MetadataContext::Unknown, 0, 0),
        (MetadataContext::IntType, 2) => (1, MetadataContext::Unknown, 0, 0),
        (MetadataContext::KeyValue, 1)
        | (MetadataContext::KeyValue, 2)
        | (MetadataContext::FileKeyValue, 1)
        | (MetadataContext::FileKeyValue, 2) => (8, MetadataContext::Unknown, 0, 0),
        (MetadataContext::LogicalType, 1) => (12, MetadataContext::StringType, 0, 0),
        (MetadataContext::LogicalType, 2) => (12, MetadataContext::MapType, 0, 0),
        (MetadataContext::LogicalType, 3) => (12, MetadataContext::ListType, 0, 0),
        (MetadataContext::LogicalType, 4) => (12, MetadataContext::EnumType, 0, 0),
        (MetadataContext::LogicalType, 5) => (12, MetadataContext::DecimalType, 0, 0),
        (MetadataContext::LogicalType, 6) => (12, MetadataContext::DateType, 0, 0),
        (MetadataContext::LogicalType, 7) => (12, MetadataContext::TimeType, 0, 0),
        (MetadataContext::LogicalType, 8) => (12, MetadataContext::TimestampType, 0, 0),
        (MetadataContext::LogicalType, 10) => (12, MetadataContext::IntType, 0, 0),
        (MetadataContext::LogicalType, 11) => (12, MetadataContext::NullType, 0, 0),
        (MetadataContext::LogicalType, 12) => (12, MetadataContext::JsonType, 0, 0),
        (MetadataContext::LogicalType, 13) => (12, MetadataContext::BsonType, 0, 0),
        (MetadataContext::LogicalType, 14) => (12, MetadataContext::UuidType, 0, 0),
        (MetadataContext::LogicalType, 15) => (12, MetadataContext::Float16Type, 0, 0),
        (MetadataContext::PageEncodingStats, 1)
        | (MetadataContext::PageEncodingStats, 2)
        | (MetadataContext::PageEncodingStats, 3) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::RowGroup, 1) => (
            9,
            MetadataContext::ColumnChunk,
            std::mem::size_of::<parquet::format::ColumnChunk>(),
            12,
        ),
        (MetadataContext::RowGroup, 2)
        | (MetadataContext::RowGroup, 3)
        | (MetadataContext::RowGroup, 5)
        | (MetadataContext::RowGroup, 6) => (6, MetadataContext::Unknown, 0, 0),
        (MetadataContext::RowGroup, 4) => (
            9,
            MetadataContext::SortingColumn,
            std::mem::size_of::<parquet::format::SortingColumn>(),
            12,
        ),
        (MetadataContext::RowGroup, 7) => (4, MetadataContext::Unknown, 0, 0),
        (MetadataContext::SchemaElement, 1)
        | (MetadataContext::SchemaElement, 2)
        | (MetadataContext::SchemaElement, 3)
        | (MetadataContext::SchemaElement, 5)
        | (MetadataContext::SchemaElement, 6)
        | (MetadataContext::SchemaElement, 7)
        | (MetadataContext::SchemaElement, 8)
        | (MetadataContext::SchemaElement, 9) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::SchemaElement, 4) => (8, MetadataContext::Unknown, 0, 0),
        (MetadataContext::SchemaElement, 10) => (12, MetadataContext::LogicalType, 0, 0),
        (MetadataContext::SizeStatistics, 1) => (6, MetadataContext::Unknown, 0, 0),
        (MetadataContext::SizeStatistics, 2) | (MetadataContext::SizeStatistics, 3) => (
            9,
            MetadataContext::ColumnHistogram,
            std::mem::size_of::<i64>(),
            6,
        ),
        (MetadataContext::SortingColumn, 1) => (5, MetadataContext::Unknown, 0, 0),
        (MetadataContext::SortingColumn, 2) | (MetadataContext::SortingColumn, 3) => {
            (1, MetadataContext::Unknown, 0, 0)
        }
        (MetadataContext::Statistics | MetadataContext::RetainedStatistics, 1 | 2 | 5 | 6) => (
            8,
            if context == MetadataContext::RetainedStatistics {
                MetadataContext::RetainedStatistics
            } else {
                MetadataContext::Unknown
            },
            0,
            0,
        ),
        (MetadataContext::Statistics | MetadataContext::RetainedStatistics, 3 | 4) => {
            (6, MetadataContext::Unknown, 0, 0)
        }
        (MetadataContext::Statistics | MetadataContext::RetainedStatistics, 7 | 8) => {
            (1, MetadataContext::Unknown, 0, 0)
        }
        (MetadataContext::TimeType, 1) => (1, MetadataContext::Unknown, 0, 0),
        (MetadataContext::TimeType, 2) => (12, MetadataContext::TimeUnit, 0, 0),
        (MetadataContext::TimeUnit, 1) => (12, MetadataContext::MilliSeconds, 0, 0),
        (MetadataContext::TimeUnit, 2) => (12, MetadataContext::MicroSeconds, 0, 0),
        (MetadataContext::TimeUnit, 3) => (12, MetadataContext::NanoSeconds, 0, 0),
        (MetadataContext::TimestampType, 1) => (1, MetadataContext::Unknown, 0, 0),
        (MetadataContext::TimestampType, 2) => (12, MetadataContext::TimeUnit, 0, 0),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatchReader;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::{Repetition, Type as PhysicalType};
    use parquet::data_type::Int64Type;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::writer::SerializedFileWriter;

    fn field_id_fixture(id: Option<i32>) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let field = Arc::new(
            Type::primitive_type_builder("value", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .with_id(id)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("fixture")
                .with_fields(vec![field])
                .build()
                .unwrap(),
        );
        let properties = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<Int64Type>()
            .write_batch(&[7], None, None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
        writer.close().unwrap();
        file
    }

    #[test]
    fn issue577_field_id_map_shared_budget() {
        let plain = field_id_fixture(None);
        let baseline = check_footer(&mut File::open(plain.path()).unwrap(), u64::MAX).unwrap();
        for id in [0, i32::MIN, i32::MAX] {
            let file = field_id_fixture(Some(id));
            let reservation =
                check_footer(&mut File::open(file.path()).unwrap(), u64::MAX).unwrap();
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap()).unwrap();
            let schema = builder.schema();
            let metadata = schema.fields()[0].metadata();
            assert_eq!(
                metadata[parquet::arrow::PARQUET_FIELD_ID_META_KEY],
                id.to_string()
            );
            let reader = super::super::pages::reader(
                File::open(file.path()).unwrap(),
                builder.metadata().clone(),
                schema,
                1024,
                u64::MAX,
                0,
            )
            .unwrap();
            let reader_schema = reader.schema();
            let reader_metadata = reader_schema.fields()[0].metadata();
            assert_eq!(reader_metadata, metadata);
            assert!(!std::ptr::eq(reader_metadata, metadata));
            let owned_lower_bound = [metadata, reader_metadata]
                .into_iter()
                .map(|map| {
                    map.capacity() * std::mem::size_of::<(String, String)>()
                        + map
                            .iter()
                            .map(|(key, value)| key.capacity() + value.capacity())
                            .sum::<usize>()
                })
                .sum::<usize>();
            let added = reservation.retained_metadata_bytes - baseline.retained_metadata_bytes;
            println!(
                "field_id={id} actual_map_lower_bound={owned_lower_bound} retained_reservation_delta={added}"
            );
            assert!(
                added >= owned_lower_bound as u64,
                "Field ID map storage must enter the shared reservation before Arrow conversion"
            );
        }
    }

    #[test]
    fn issue577_metadata_payload_consumption_preserves_bounds_and_reservation() {
        let header = |bytes: Vec<u8>, capacity, maximum| {
            let length = bytes.len() as u64;
            CompactHeader {
                input: BufReader::with_capacity(capacity, std::io::Cursor::new(bytes).take(length)),
                maximum,
                decoded_bytes: 0,
                retained_key_values: 0,
                retained_other_metadata: 0,
                schema_path: Vec::new(),
                schema_root_seen: false,
                schema_leaves: 0,
                has_arrow_schema: false,
                arrow_schema: None,
                file_key_values: 0,
            }
        };
        let length = 4097_u64;
        let reservation = length + std::mem::size_of::<bytes::Bytes>() as u64;
        let mut bytes = vec![0x81, 0x20];
        bytes.resize(2 + length as usize, b'x');
        bytes.push(0x2a);
        for capacity in [0, 1, 7, 4096] {
            for value in [0, 1, 127, 128, 16_384, u64::MAX] {
                let mut encoded = Vec::new();
                let mut remaining = value;
                loop {
                    let byte = (remaining & 127) as u8;
                    remaining >>= 7;
                    encoded.push(byte | if remaining == 0 { 0 } else { 128 });
                    if remaining == 0 {
                        break;
                    }
                }
                encoded.push(0x2a);
                let mut input = header(encoded, capacity, reservation);
                assert_eq!(input.varint().unwrap(), value);
                assert_eq!(input.remaining(), 1);
                assert_eq!(input.byte().unwrap(), 0x2a);
                assert_eq!(input.remaining(), 0);
                assert!(input.byte().is_err());
                assert_eq!(input.decoded_bytes, 0);
            }
            for mut encoded in [vec![0x80], vec![0x80; 10], vec![0xff; 9]] {
                if encoded[0] == 0xff {
                    encoded.push(2);
                }
                let mut input = header(encoded, capacity, reservation);
                assert!(input.varint().is_err());
            }
            let mut input = header(vec![0x80; 9], capacity, reservation);
            assert!(input.varint().is_err());

            let mut input = header(vec![0x2a, 0x7f], capacity, reservation);
            input.input.get_mut().set_limit(1);
            assert_eq!(input.byte().unwrap(), 0x2a);
            assert_eq!(input.remaining(), 0);
            assert!(input.byte().is_err());
            assert_eq!(input.input.get_ref().get_ref().position(), 1);

            let mut input = header(vec![0x2a], capacity, reservation);
            input.input.get_mut().set_limit(2);
            assert_eq!(input.byte().unwrap(), 0x2a);
            assert!(input.byte().is_err());

            let mut input = header(bytes.clone(), capacity, reservation);
            assert_eq!(
                input
                    .metadata_binary(MetadataContext::RetainedStatistics, false)
                    .unwrap(),
                (length, false)
            );
            assert_eq!(input.decoded_bytes, reservation);
            assert_eq!(input.retained_other_metadata, reservation);
            assert_eq!(input.remaining(), 1);
            assert_eq!(input.byte().unwrap(), 0x2a);
            assert_eq!(input.remaining(), 0);

            let mut input = header(bytes.clone(), capacity, reservation - 1);
            assert!(matches!(
                input.metadata_binary(MetadataContext::RetainedStatistics, false),
                Err(ExecutorError::ResourceExhausted { .. })
            ));
            assert_eq!(input.remaining(), length + 1);

            let mut input = header(bytes.clone(), capacity, reservation);
            input.skip(8, 0).unwrap();
            assert_eq!(input.decoded_bytes, 0);
            assert_eq!(input.remaining(), 1);
            assert_eq!(input.byte().unwrap(), 0x2a);

            // A file can become shorter after its declared length was checked.
            let mut input = header(bytes[..bytes.len() - 2].to_vec(), capacity, reservation);
            input.input.get_mut().set_limit(bytes.len() as u64);
            assert!(
                input
                    .metadata_binary(MetadataContext::RetainedStatistics, false)
                    .is_err()
            );

            let key = parquet::arrow::ARROW_SCHEMA_META_KEY.as_bytes();
            for matches in [true, false] {
                let mut bytes = vec![key.len() as u8];
                bytes.extend_from_slice(key);
                if !matches {
                    *bytes.last_mut().unwrap() = b'!';
                }
                bytes.push(0x2a);
                let mut input = header(bytes, capacity, reservation);
                assert_eq!(
                    input
                        .metadata_binary(MetadataContext::FileKeyValue, true)
                        .unwrap(),
                    (key.len() as u64, matches)
                );
                assert_eq!(input.retained_key_values, key.len() as u64);
                assert_eq!(input.byte().unwrap(), 0x2a);
            }

            for available in 0..8 {
                let mut input = header(vec![0; available], capacity, reservation);
                input.input.get_mut().set_limit(8);
                assert!(
                    input
                        .metadata_value(7, 0, MetadataContext::Unknown, 0, 0)
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn issue577_page_preflight_buffers_io_and_preserves_shared_budget() {
        struct CountedFile {
            file: File,
            reads: usize,
        }
        impl Read for CountedFile {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                self.reads += 1;
                self.file.read(bytes)
            }
        }
        impl Seek for CountedFile {
            fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
                self.file.seek(position)
            }
        }
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let fields = (0..4)
            .map(|column| {
                Arc::new(
                    Type::primitive_type_builder(&format!("c{column}"), PhysicalType::INT64)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let schema = Arc::new(
            Type::group_type_builder("fixture")
                .with_fields(fields)
                .build()
                .unwrap(),
        );
        let mut writer = SerializedFileWriter::new(
            File::create(file.path()).unwrap(),
            schema,
            Arc::new(WriterProperties::default()),
        )
        .unwrap();
        for value in 0..128 {
            let mut group = writer.next_row_group().unwrap();
            while let Some(mut column) = group.next_column().unwrap() {
                column
                    .typed::<Int64Type>()
                    .write_batch(&[value], None, None)
                    .unwrap();
                column.close().unwrap();
            }
            group.close().unwrap();
        }
        writer.close().unwrap();
        let footer = check_footer(&mut File::open(file.path()).unwrap(), u64::MAX).unwrap();
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap()).unwrap();
        let metadata = builder.metadata();
        let mut input = CountedFile {
            file: File::open(file.path()).unwrap(),
            reads: 0,
        };
        let reserved = check_pages(&mut input, metadata, footer.start, u64::MAX, 0).unwrap();
        // Contiguous tiny pages must share bounded reads, rather than one read per header.
        let maximum_reads = footer.start.div_ceil(4096) as usize + 2;
        println!(
            "page_preflight_reads={} maximum_reads={maximum_reads} reserved={reserved}",
            input.reads
        );
        assert!(
            input.reads <= maximum_reads,
            "page reads must scale with bytes, not page count"
        );
        assert_eq!(
            check_pages(&mut input, metadata, footer.start, reserved, 0).unwrap(),
            reserved
        );
        assert!(matches!(
            check_pages(&mut input, metadata, footer.start, reserved - 1, 0),
            Err(ExecutorError::ResourceExhausted { .. })
        ));
        let reversed = parquet::file::metadata::ParquetMetaData::new(
            metadata.file_metadata().clone(),
            metadata.row_groups().iter().rev().cloned().collect(),
        );
        assert_eq!(
            check_pages(&mut input, &reversed, footer.start, u64::MAX, 0).unwrap(),
            reserved
        );
    }
}
