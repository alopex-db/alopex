use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use crate::executor::{ExecutorError, Result};

use super::parquet_io_error;

fn invalid_page() -> ExecutorError {
    ExecutorError::BulkLoad("invalid parquet page or chunk declaration".into())
}

fn check_declared_bytes(bytes: u64, limit: u64) -> Result<()> {
    if bytes > limit {
        return Err(ExecutorError::ResourceExhausted {
            message: format!(
                "READ_PARQUET declares {bytes} bytes (limit {limit}); spilling this operator is not supported"
            ),
        });
    }
    Ok(())
}

pub(super) fn check_footer(file: &mut File, limit: u64) -> Result<u64> {
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
    let mut footer = CompactHeader {
        input: BufReader::with_capacity(size.min(4096) as usize, (&mut *file).take(size)),
        maximum: limit,
    };
    footer.metadata_fields(0, "FileMetaData")?;
    file.seek(SeekFrom::Start(0)).map_err(parquet_io_error)?;
    Ok(start)
}

pub(super) fn check_pages(
    file: &mut File,
    metadata: &parquet::file::metadata::ParquetMetaData,
    footer_start: u64,
    limit: u64,
    mut reserved_bytes: u64,
) -> Result<u64> {
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
            let mut position = start;
            while position < end {
                file.seek(SeekFrom::Start(position))
                    .map_err(parquet_io_error)?;
                let maximum = (end - position).min(limit);
                let mut header = CompactHeader {
                    input: BufReader::with_capacity(
                        maximum.min(4096) as usize,
                        (&mut *file).take(maximum),
                    ),
                    maximum,
                };
                let (uncompressed, compressed, dictionary_values) = header.page()?;
                let consumed = maximum - header.remaining();
                check_declared_bytes(uncompressed, limit)?;
                check_declared_bytes(compressed, limit)?;
                reserved_bytes = reserved_bytes
                    .checked_add(uncompressed)
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
            }
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
    };
    typed.metadata_fields(0, "PageHeader")?;
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
}

impl<R: Read> CompactHeader<R> {
    fn remaining(&self) -> u64 {
        self.input.get_ref().limit() + self.input.buffer().len() as u64
    }

    fn byte(&mut self) -> Result<u8> {
        let mut byte = [0];
        self.input.read_exact(&mut byte).map_err(parquet_io_error)?;
        Ok(byte[0])
    }

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

    fn field_id(&mut self, field: u8, previous: i16) -> Result<i16> {
        let id = if field >> 4 == 0 {
            self.integer()?
        } else {
            i64::from(previous) + i64::from(field >> 4)
        };
        i16::try_from(id).map_err(|_| invalid_page())
    }

    fn metadata_fields(&mut self, depth: usize, context: &str) -> Result<()> {
        if depth > 32 {
            return Err(invalid_page());
        }
        let mut id = 0_i16;
        loop {
            let field = self.byte()?;
            if field == 0 {
                return Ok(());
            }
            id = self.field_id(field, id)?;
            let kind = field & 15;
            let (expected, child, width, element) =
                metadata_field(context, id).unwrap_or((kind, "", 0, 0));
            if kind != expected && !(expected == 1 && kind == 2) {
                return Err(invalid_page());
            }
            // Compact field booleans carry their value in the field tag.
            if kind != 1 && kind != 2 {
                self.metadata_value(kind, depth, child, width, element)?;
            }
        }
    }

    fn metadata_value(
        &mut self,
        kind: u8,
        depth: usize,
        context: &str,
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
                for _ in 0..8 {
                    self.byte()?;
                }
            }
            8 => {
                let length = self.varint()?;
                check_declared_bytes(length, self.maximum)?;
                if length > self.remaining() {
                    return Err(invalid_page());
                }
                for _ in 0..length {
                    self.byte()?;
                }
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
                        self.metadata_value(kinds >> 4, depth + 1, "", 0, 0)?;
                        self.metadata_value(kinds & 15, depth + 1, "", 0, 0)?;
                    }
                }
            }
            12 => self.metadata_fields(depth + 1, context)?,
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
                for _ in 0..8 {
                    self.byte()?;
                }
            }
            8 => {
                let length = self.varint()?;
                if length > self.remaining() {
                    return Err(invalid_page());
                }
                for _ in 0..length {
                    self.byte()?;
                }
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
fn metadata_field(context: &str, id: i16) -> Option<(u8, &'static str, usize, u8)> {
    Some(match (context, id) {
        ("PageHeader", 1..=4) => (5, "", 0, 0),
        ("PageHeader", 5) => (12, "DataPageHeader", 0, 0),
        ("PageHeader", 6) => (12, "IndexPageHeader", 0, 0),
        ("PageHeader", 7) => (12, "DictionaryPageHeader", 0, 0),
        ("PageHeader", 8) => (12, "DataPageHeaderV2", 0, 0),
        ("DataPageHeader", 1..=4) => (5, "", 0, 0),
        ("DataPageHeader", 5) => (12, "Statistics", 0, 0),
        ("DataPageHeaderV2", 1..=6) => (5, "", 0, 0),
        ("DataPageHeaderV2", 7) => (1, "", 0, 0),
        ("DataPageHeaderV2", 8) => (12, "Statistics", 0, 0),
        ("DictionaryPageHeader", 1..=2) => (5, "", 0, 0),
        ("DictionaryPageHeader", 3) => (1, "", 0, 0),
        ("AesGcmCtrV1", 1) | ("AesGcmCtrV1", 2) => (8, "", 0, 0),
        ("AesGcmCtrV1", 3) => (1, "", 0, 0),
        ("AesGcmV1", 1) | ("AesGcmV1", 2) => (8, "", 0, 0),
        ("AesGcmV1", 3) => (1, "", 0, 0),
        ("ColumnChunk", 1) | ("ColumnChunk", 9) => (8, "", 0, 0),
        ("ColumnChunk", 2) | ("ColumnChunk", 4) | ("ColumnChunk", 6) => (6, "", 0, 0),
        ("ColumnChunk", 3) => (12, "ColumnMetaData", 0, 0),
        ("ColumnChunk", 5) | ("ColumnChunk", 7) => (5, "", 0, 0),
        ("ColumnChunk", 8) => (12, "ColumnCryptoMetaData", 0, 0),
        ("ColumnCryptoMetaData", 1) => (12, "EncryptionWithFooterKey", 0, 0),
        ("ColumnCryptoMetaData", 2) => (12, "EncryptionWithColumnKey", 0, 0),
        ("ColumnMetaData", 1) | ("ColumnMetaData", 4) | ("ColumnMetaData", 15) => (5, "", 0, 0),
        ("ColumnMetaData", 2) => (9, "", std::mem::size_of::<parquet::format::Encoding>(), 5),
        ("ColumnMetaData", 3) => (9, "", std::mem::size_of::<String>(), 8),
        ("ColumnMetaData", 5)
        | ("ColumnMetaData", 6)
        | ("ColumnMetaData", 7)
        | ("ColumnMetaData", 9)
        | ("ColumnMetaData", 10)
        | ("ColumnMetaData", 11)
        | ("ColumnMetaData", 14) => (6, "", 0, 0),
        ("ColumnMetaData", 8) => (
            9,
            "KeyValue",
            std::mem::size_of::<parquet::format::KeyValue>(),
            12,
        ),
        ("ColumnMetaData", 12) => (12, "Statistics", 0, 0),
        ("ColumnMetaData", 13) => (
            9,
            "PageEncodingStats",
            std::mem::size_of::<parquet::format::PageEncodingStats>(),
            12,
        ),
        ("ColumnMetaData", 16) => (12, "SizeStatistics", 0, 0),
        ("ColumnOrder", 1) => (12, "TypeDefinedOrder", 0, 0),
        ("DecimalType", 1) | ("DecimalType", 2) => (5, "", 0, 0),
        ("EncryptionAlgorithm", 1) => (12, "AesGcmV1", 0, 0),
        ("EncryptionAlgorithm", 2) => (12, "AesGcmCtrV1", 0, 0),
        ("EncryptionWithColumnKey", 1) => (9, "", std::mem::size_of::<String>(), 8),
        ("EncryptionWithColumnKey", 2) => (8, "", 0, 0),
        ("FileMetaData", 1) => (5, "", 0, 0),
        ("FileMetaData", 2) => (
            9,
            "SchemaElement",
            std::mem::size_of::<parquet::format::SchemaElement>(),
            12,
        ),
        ("FileMetaData", 3) => (6, "", 0, 0),
        ("FileMetaData", 4) => (
            9,
            "RowGroup",
            std::mem::size_of::<parquet::format::RowGroup>(),
            12,
        ),
        ("FileMetaData", 5) => (
            9,
            "KeyValue",
            std::mem::size_of::<parquet::format::KeyValue>(),
            12,
        ),
        ("FileMetaData", 6) | ("FileMetaData", 9) => (8, "", 0, 0),
        ("FileMetaData", 7) => (
            9,
            "ColumnOrder",
            std::mem::size_of::<parquet::format::ColumnOrder>(),
            12,
        ),
        ("FileMetaData", 8) => (12, "EncryptionAlgorithm", 0, 0),
        ("IntType", 1) => (3, "", 0, 0),
        ("IntType", 2) => (1, "", 0, 0),
        ("KeyValue", 1) | ("KeyValue", 2) => (8, "", 0, 0),
        ("LogicalType", 1) => (12, "StringType", 0, 0),
        ("LogicalType", 2) => (12, "MapType", 0, 0),
        ("LogicalType", 3) => (12, "ListType", 0, 0),
        ("LogicalType", 4) => (12, "EnumType", 0, 0),
        ("LogicalType", 5) => (12, "DecimalType", 0, 0),
        ("LogicalType", 6) => (12, "DateType", 0, 0),
        ("LogicalType", 7) => (12, "TimeType", 0, 0),
        ("LogicalType", 8) => (12, "TimestampType", 0, 0),
        ("LogicalType", 10) => (12, "IntType", 0, 0),
        ("LogicalType", 11) => (12, "NullType", 0, 0),
        ("LogicalType", 12) => (12, "JsonType", 0, 0),
        ("LogicalType", 13) => (12, "BsonType", 0, 0),
        ("LogicalType", 14) => (12, "UUIDType", 0, 0),
        ("LogicalType", 15) => (12, "Float16Type", 0, 0),
        ("PageEncodingStats", 1) | ("PageEncodingStats", 2) | ("PageEncodingStats", 3) => {
            (5, "", 0, 0)
        }
        ("RowGroup", 1) => (
            9,
            "ColumnChunk",
            std::mem::size_of::<parquet::format::ColumnChunk>(),
            12,
        ),
        ("RowGroup", 2) | ("RowGroup", 3) | ("RowGroup", 5) | ("RowGroup", 6) => (6, "", 0, 0),
        ("RowGroup", 4) => (
            9,
            "SortingColumn",
            std::mem::size_of::<parquet::format::SortingColumn>(),
            12,
        ),
        ("RowGroup", 7) => (4, "", 0, 0),
        ("SchemaElement", 1)
        | ("SchemaElement", 2)
        | ("SchemaElement", 3)
        | ("SchemaElement", 5)
        | ("SchemaElement", 6)
        | ("SchemaElement", 7)
        | ("SchemaElement", 8)
        | ("SchemaElement", 9) => (5, "", 0, 0),
        ("SchemaElement", 4) => (8, "", 0, 0),
        ("SchemaElement", 10) => (12, "LogicalType", 0, 0),
        ("SizeStatistics", 1) => (6, "", 0, 0),
        ("SizeStatistics", 2) | ("SizeStatistics", 3) => (9, "", std::mem::size_of::<i64>(), 6),
        ("SortingColumn", 1) => (5, "", 0, 0),
        ("SortingColumn", 2) | ("SortingColumn", 3) => (1, "", 0, 0),
        ("Statistics", 1) | ("Statistics", 2) | ("Statistics", 5) | ("Statistics", 6) => {
            (8, "", 0, 0)
        }
        ("Statistics", 3) | ("Statistics", 4) => (6, "", 0, 0),
        ("Statistics", 7) | ("Statistics", 8) => (1, "", 0, 0),
        ("TimeType", 1) => (1, "", 0, 0),
        ("TimeType", 2) => (12, "TimeUnit", 0, 0),
        ("TimeUnit", 1) => (12, "MilliSeconds", 0, 0),
        ("TimeUnit", 2) => (12, "MicroSeconds", 0, 0),
        ("TimeUnit", 3) => (12, "NanoSeconds", 0, 0),
        ("TimestampType", 1) => (1, "", 0, 0),
        ("TimestampType", 2) => (12, "TimeUnit", 0, 0),
        _ => return None,
    })
}
