#[cfg(test)]
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_schema::Schema;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroups};
use parquet::arrow::{ProjectionMask, parquet_to_arrow_field_levels};
use parquet::basic::Encoding;
use parquet::column::page::{Page, PageIterator, PageMetadata, PageReader};
use parquet::data_type::Int32Type;
use parquet::decoding::{Decoder, DeltaBitPackDecoder};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::serialized_reader::SerializedPageReader;

use super::input::InputFile;

mod bounded;

pub(super) fn reader_schema_hint(schema: Arc<Schema>) -> Arc<Schema> {
    let semantic =
        |key: &str| key == parquet::arrow::PARQUET_FIELD_ID_META_KEY || key.starts_with("ARROW:");
    if schema
        .fields()
        .iter()
        .all(|field| field.metadata().keys().all(|key| semantic(key)))
    {
        return schema;
    }
    // SQL exposes names/types/values; the decoder only needs semantic field metadata.
    let Schema { fields, metadata } = Arc::unwrap_or_clone(schema);
    let owned_fields = fields.to_vec();
    drop(fields);
    let fields = owned_fields
        .into_iter()
        .map(|field| {
            if field.metadata().keys().all(|key| semantic(key)) {
                return field;
            }
            let mut field = Arc::unwrap_or_clone(field);
            let metadata = field
                .metadata()
                .iter()
                .filter(|(key, _)| semantic(key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            field.set_metadata(metadata);
            Arc::new(field)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

pub(super) fn reader(
    file: impl Into<InputFile>,
    metadata: Arc<ParquetMetaData>,
    schema: &Schema,
    batch_size: usize,
    limit: u64,
    reserved_bytes: u64,
) -> Result<ParquetRecordBatchReader> {
    let declared_rows = usize::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| ParquetError::General("invalid parquet row count".into()))?;
    let num_rows = metadata
        .row_groups()
        .iter()
        .try_fold(0_usize, |sum, group| {
            let count = usize::try_from(group.num_rows())
                .map_err(|_| ParquetError::General("invalid parquet row group count".into()))?;
            sum.checked_add(count)
                .ok_or_else(|| ParquetError::General("parquet row count overflow".into()))
        })?;
    let levels = parquet_to_arrow_field_levels(
        metadata.file_metadata().schema_descr(),
        ProjectionMask::all(),
        Some(schema.fields()),
    )?;
    let groups = FileRowGroups {
        file: Arc::new(file.into()),
        metadata,
        num_rows,
        limit,
        reserved_bytes: Arc::new(AtomicU64::new(reserved_bytes)),
    };
    ParquetRecordBatchReader::try_new_with_row_groups(
        &levels,
        &groups,
        batch_size.min(declared_rows.max(num_rows)),
        None,
    )
}

struct FileRowGroups {
    file: Arc<InputFile>,
    metadata: Arc<ParquetMetaData>,
    num_rows: usize,
    limit: u64,
    reserved_bytes: Arc<AtomicU64>,
}

impl RowGroups for FileRowGroups {
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn column_chunks(&self, column: usize) -> Result<Box<dyn PageIterator>> {
        let count = self.metadata.file_metadata().schema_descr().num_columns();
        if column >= count {
            return Err(ParquetError::IndexOutOfBound(column, count));
        }
        Ok(Box::new(ColumnChunks {
            file: Arc::clone(&self.file),
            metadata: Arc::clone(&self.metadata),
            column,
            next_group: 0,
            limit: self.limit,
            reserved_bytes: Arc::clone(&self.reserved_bytes),
        }))
    }
}

struct ColumnChunks {
    file: Arc<InputFile>,
    metadata: Arc<ParquetMetaData>,
    column: usize,
    next_group: usize,
    limit: u64,
    reserved_bytes: Arc<AtomicU64>,
}

impl Iterator for ColumnChunks {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        let group_index = self.next_group;
        let group = self.metadata.row_groups().get(group_index)?;
        self.next_group += 1;
        Some((|| {
            let column = group
                .columns()
                .get(self.column)
                .ok_or_else(|| ParquetError::IndexOutOfBound(self.column, group.num_columns()))?;
            let locations = if let Some(index) = self.metadata.offset_index() {
                let columns = index
                    .get(group_index)
                    .ok_or_else(|| ParquetError::IndexOutOfBound(group_index, index.len()))?;
                if columns.is_empty() {
                    None
                } else {
                    Some(
                        columns
                            .get(self.column)
                            .ok_or_else(|| {
                                ParquetError::IndexOutOfBound(self.column, columns.len())
                            })?
                            .page_locations
                            .clone(),
                    )
                }
            } else {
                None
            };
            let inner: Box<dyn PageReader> = if matches!(
                column.compression(),
                parquet::basic::Compression::GZIP(_)
                    | parquet::basic::Compression::ZSTD(_)
                    | parquet::basic::Compression::BROTLI(_)
            ) {
                Box::new(bounded::BoundedPages::new(
                    Arc::clone(self.file.raw_file()),
                    column,
                    group.num_rows() as usize,
                    locations,
                    self.limit,
                    Arc::clone(&self.reserved_bytes),
                )?)
            } else {
                Box::new(SerializedPageReader::new(
                    Arc::clone(&self.file),
                    column,
                    group.num_rows() as usize,
                    locations,
                )?)
            };
            Ok(Box::new(ColumnPages {
                inner,
                max_rep_level: column.column_descr().max_rep_level(),
                max_def_level: column.column_descr().max_def_level(),
                limit: self.limit,
                reserved_bytes: Arc::clone(&self.reserved_bytes),
            }) as Box<dyn PageReader>)
        })())
    }
}

impl PageIterator for ColumnChunks {}

struct ColumnPages {
    inner: Box<dyn PageReader>,
    max_rep_level: i16,
    max_def_level: i16,
    limit: u64,
    reserved_bytes: Arc<AtomicU64>,
}

fn invalid_delta() -> ParquetError {
    ParquetError::General("invalid or over-budget parquet DELTA length declaration".into())
}

fn delta_varint(input: &mut &[u8]) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = input.split_first().ok_or_else(invalid_delta)?;
        *input = rest;
        if shift == 63 && byte > 1 {
            return Err(invalid_delta());
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid_delta())
}

fn delta_header(input: &mut &[u8], max_values: u64, limit: u64) -> Result<(u64, u64, u64)> {
    let block = delta_varint(input)?;
    let mini_blocks = delta_varint(input)?;
    let count = delta_varint(input)?;
    if block == 0
        || !block.is_multiple_of(128)
        || mini_blocks == 0
        || !block.is_multiple_of(mini_blocks)
        || !(block / mini_blocks).is_multiple_of(32)
        || count > max_values
        || count.checked_mul(4).is_none_or(|bytes| bytes > limit)
        || mini_blocks > limit
    {
        return Err(invalid_delta());
    }
    Ok((block, mini_blocks, count))
}

fn skip_delta_values(input: &mut &[u8], block: u64, mini_blocks: u64, count: u64) -> Result<()> {
    delta_varint(input)?;
    let per_mini = block / mini_blocks;
    let mini_blocks = usize::try_from(mini_blocks).map_err(|_| invalid_delta())?;
    let mut remaining = count.saturating_sub(1);
    while remaining != 0 {
        delta_varint(input)?;
        let data = *input;
        let widths = data.get(..mini_blocks).ok_or_else(invalid_delta)?;
        *input = &data[mini_blocks..];
        for &width in widths {
            if remaining == 0 {
                break;
            }
            if width > 32 {
                return Err(invalid_delta());
            }
            // parquet's suffix offset includes the full final mini block's padding.
            let size = per_mini
                .checked_mul(u64::from(width))
                .ok_or_else(invalid_delta)?
                / 8;
            let size = usize::try_from(size).map_err(|_| invalid_delta())?;
            *input = input.get(size..).ok_or_else(invalid_delta)?;
            remaining = remaining.saturating_sub(per_mini);
        }
    }
    Ok(())
}

impl ColumnPages {
    fn check_delta_expansion(
        &self,
        prefix: Bytes,
        suffix: Bytes,
        count: u64,
        payload_bytes: usize,
        levels: u32,
    ) -> Result<()> {
        let count = usize::try_from(count).map_err(|_| invalid_delta())?;
        let mut prefixes = DeltaBitPackDecoder::<Int32Type>::new();
        prefixes.set_data(prefix, count)?;
        let mut suffixes = DeltaBitPackDecoder::<Int32Type>::new();
        suffixes.set_data(suffix, count)?;
        let mut prefix_values = [0; 32];
        let mut suffix_values = [0; 32];
        let mut remaining = count;
        let mut previous = 0_u64;
        let mut expanded = 0_u64;
        let mut widest = 0_u64;
        let mut payload = 0_u64;
        while remaining != 0 {
            let count = remaining.min(prefix_values.len());
            if prefixes.get(&mut prefix_values[..count])? != count
                || suffixes.get(&mut suffix_values[..count])? != count
            {
                return Err(invalid_delta());
            }
            for (&prefix, &suffix) in prefix_values[..count].iter().zip(&suffix_values[..count]) {
                let prefix = u64::try_from(prefix).map_err(|_| invalid_delta())?;
                let suffix = u64::try_from(suffix).map_err(|_| invalid_delta())?;
                if prefix > previous {
                    return Err(invalid_delta());
                }
                previous = prefix.checked_add(suffix).ok_or_else(invalid_delta)?;
                expanded = expanded.checked_add(previous).ok_or_else(invalid_delta)?;
                widest = widest.max(previous);
                payload = payload.checked_add(suffix).ok_or_else(invalid_delta)?;
                if expanded > self.limit || payload > payload_bytes as u64 {
                    return Err(invalid_delta());
                }
            }
            remaining -= count;
        }
        let arrow_capacity = if expanded == 0 {
            0
        } else {
            expanded.checked_mul(2).ok_or_else(invalid_delta)?.max(8)
        };
        // Arrow growth can relocate its backing; SQL copies each decoded value.
        let payload_peak = arrow_capacity
            .checked_add(expanded.max(arrow_capacity / 2))
            .ok_or_else(invalid_delta)?;
        let previous_capacity = if widest == 0 {
            0
        } else {
            widest.checked_mul(2).ok_or_else(invalid_delta)?.max(8)
        };
        let length_vectors = (count as u64)
            .checked_mul((2 * std::mem::size_of::<i32>()) as u64)
            .ok_or_else(invalid_delta)?;
        // LargeBinary offsets cover both Arrow offset widths and nullable slots.
        let offsets = (u64::from(levels) + 1)
            .checked_mul(2)
            .and_then(|slots| slots.max(4).checked_mul(std::mem::size_of::<i64>() as u64))
            .ok_or_else(invalid_delta)?;
        let bitmap_capacity = if self.max_def_level == 0 || levels == 0 {
            0
        } else {
            u64::from(levels)
                .div_ceil(8)
                .checked_next_power_of_two()
                .ok_or_else(invalid_delta)?
                .max(64)
        };
        let level_width = (u64::from(self.max_def_level > 0) + u64::from(self.max_rep_level > 0))
            * std::mem::size_of::<i16>() as u64;
        let level_capacity = u64::from(levels).checked_mul(2).ok_or_else(invalid_delta)?;
        let level_buffers = level_capacity
            .checked_add(level_capacity / 2)
            .and_then(|slots| slots.checked_mul(level_width))
            .ok_or_else(invalid_delta)?;
        let capacity = payload_peak
            .checked_add(previous_capacity)
            .and_then(|bytes| bytes.checked_add(previous_capacity / 2))
            .and_then(|bytes| bytes.checked_add(length_vectors))
            .and_then(|bytes| bytes.checked_add(offsets))
            .and_then(|bytes| bytes.checked_add(bitmap_capacity))
            .and_then(|bytes| bytes.checked_add(bitmap_capacity / 2))
            .and_then(|bytes| bytes.checked_add(level_buffers))
            .ok_or_else(invalid_delta)?;
        #[allow(
            deprecated,
            reason = "Release builds use Rust 1.90; try_update requires 1.95"
        )]
        self.reserved_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let used = used.checked_add(capacity)?;
                (used <= self.limit).then_some(used)
            })
            .map_err(|_| invalid_delta())?;
        Ok(())
    }

    fn check_delta_lengths(&self, page: &Page) -> Result<()> {
        if !matches!(
            page.encoding(),
            Encoding::DELTA_LENGTH_BYTE_ARRAY | Encoding::DELTA_BYTE_ARRAY
        ) {
            return Ok(());
        }
        let mut input = page.buffer().as_ref();
        match page {
            Page::DataPage {
                num_values,
                rep_level_encoding,
                def_level_encoding,
                ..
            } => {
                for (level, encoding) in [
                    (self.max_rep_level, *rep_level_encoding),
                    (self.max_def_level, *def_level_encoding),
                ] {
                    if level == 0 {
                        continue;
                    }
                    let size = match encoding {
                        Encoding::RLE => {
                            let prefix = input.get(..4).ok_or_else(invalid_delta)?;
                            let size = u32::from_le_bytes(prefix.try_into().unwrap()) as usize;
                            input = &input[4..];
                            size
                        }
                        #[allow(deprecated)]
                        Encoding::BIT_PACKED => {
                            let width = 16 - level.leading_zeros();
                            (u64::from(*num_values) * u64::from(width)).div_ceil(8) as usize
                        }
                        _ => return Err(invalid_delta()),
                    };
                    input = input.get(size..).ok_or_else(invalid_delta)?;
                }
            }
            Page::DataPageV2 {
                rep_levels_byte_len,
                def_levels_byte_len,
                ..
            } => {
                let offset = (*rep_levels_byte_len as usize)
                    .checked_add(*def_levels_byte_len as usize)
                    .ok_or_else(invalid_delta)?;
                input = input.get(offset..).ok_or_else(invalid_delta)?;
            }
            Page::DictionaryPage { .. } => return Err(invalid_delta()),
        }
        let prefix_start = page.buffer().len() - input.len();
        // parquet 53.4.1 resizes its i32 vectors from the encoded counts.
        let (block, mini_blocks, count) =
            delta_header(&mut input, u64::from(page.num_values()), self.limit)?;
        if page.encoding() == Encoding::DELTA_BYTE_ARRAY {
            skip_delta_values(&mut input, block, mini_blocks, count)?;
            let suffix_start = page.buffer().len() - input.len();
            let (suffix_block, suffix_mini_blocks, suffix_count) =
                delta_header(&mut input, u64::from(page.num_values()), self.limit)?;
            if suffix_count != count {
                return Err(invalid_delta());
            }
            skip_delta_values(&mut input, suffix_block, suffix_mini_blocks, suffix_count)?;
            self.check_delta_expansion(
                page.buffer().slice(prefix_start..),
                page.buffer().slice(suffix_start..),
                count,
                input.len(),
                page.num_values(),
            )?;
        }
        Ok(())
    }
}

impl Iterator for ColumnPages {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for ColumnPages {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        let page = self.inner.get_next_page()?;
        if let Some(page) = &page {
            self.check_delta_lengths(page)?;
        }
        Ok(page)
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        self.inner.peek_next_page()
    }

    fn skip_next_page(&mut self) -> Result<()> {
        self.inner.skip_next_page()
    }

    fn at_record_boundary(&mut self) -> Result<bool> {
        self.inner.at_record_boundary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder};
    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::file::properties::WriterProperties;

    #[test]
    fn page_index_peek_skip_and_record_boundaries_preserve_rows() {
        for compression in [
            parquet::basic::Compression::GZIP(Default::default()),
            parquet::basic::Compression::ZSTD(Default::default()),
            parquet::basic::Compression::BROTLI(Default::default()),
        ] {
            assert_indexed_pages(compression);
        }
    }

    fn assert_indexed_pages(compression: parquet::basic::Compression) {
        let limit = if matches!(compression, parquet::basic::Compression::ZSTD(_)) {
            zstd::zstd_safe::DCtx::create().sizeof() as u64 + 65536
        } else {
            65536
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut lists = ListBuilder::new(Int32Builder::new());
        lists.values().append_value(1);
        lists.values().append_value(2);
        lists.append(true);
        lists.append(false);
        lists.append(true);
        lists.values().append_null();
        lists.values().append_value(4);
        lists.append(true);
        let lists = lists.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::LargeUtf8, true),
            Field::new("items", lists.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
                Arc::new(LargeStringArray::from(vec![
                    Some("ten"),
                    None,
                    Some("thirty"),
                    Some("forty"),
                ])),
                Arc::new(lists),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(2)
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(file.path()).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let input = File::open(file.path()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            input.try_clone().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let metadata = Arc::clone(builder.metadata());
        assert!(metadata.offset_index().is_some());
        let groups = FileRowGroups {
            file: Arc::new(InputFile::from(input.try_clone().unwrap())),
            metadata: Arc::clone(&metadata),
            num_rows: 4,
            limit,
            reserved_bytes: Arc::new(AtomicU64::new(0)),
        };
        let mut chunks = groups.column_chunks(0).unwrap();
        assert!(groups.column_chunks(3).is_err());
        let mut pages = chunks.next().unwrap().unwrap();
        assert!(pages.peek_next_page().unwrap().unwrap().is_dict);
        assert!(matches!(
            pages.get_next_page().unwrap(),
            Some(Page::DictionaryPage { .. })
        ));
        assert!(pages.at_record_boundary().unwrap());
        assert_eq!(pages.peek_next_page().unwrap().unwrap().num_rows, Some(1));
        pages.skip_next_page().unwrap();
        assert_eq!(pages.get_next_page().unwrap().unwrap().num_values(), 1);
        assert!(pages.peek_next_page().unwrap().is_none());
        assert!(chunks.next().unwrap().is_ok());
        assert!(chunks.next().is_none());

        let actual = reader(input, metadata, builder.schema(), 1024, limit, 0)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(actual, vec![batch]);
    }
}

#[cfg(test)]
mod schema_hint_tests {
    use super::reader_schema_hint;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn hint_preserves_semantics_without_copying_ordinary_field_metadata() {
        let metadata = HashMap::from([
            ("description".into(), "x".repeat(4096)),
            (PARQUET_FIELD_ID_META_KEY.into(), "7".into()),
            ("ARROW:extension:name".into(), "example".into()),
            ("ARROW:extension:metadata".into(), "{}".into()),
        ]);
        let field = Field::new_dict(
            "value",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            17,
            true,
        )
        .with_metadata(metadata);
        let root = HashMap::from([("root".into(), "retained".into())]);
        let source = Arc::new(Schema::new_with_metadata(vec![field], root));
        let hint = reader_schema_hint(Arc::clone(&source));
        let field = hint.field(0);
        assert_eq!(field.name(), source.field(0).name());
        assert_eq!(field.data_type(), source.field(0).data_type());
        assert_eq!(field.is_nullable(), source.field(0).is_nullable());
        assert_eq!(field.dict_id(), Some(17));
        assert_eq!(field.dict_is_ordered(), Some(true));
        assert_eq!(hint.metadata(), source.metadata());
        assert_eq!(
            field.metadata().get(PARQUET_FIELD_ID_META_KEY).unwrap(),
            "7"
        );
        assert_eq!(
            field.metadata().get("ARROW:extension:name").unwrap(),
            "example"
        );
        assert_eq!(
            field.metadata().get("ARROW:extension:metadata").unwrap(),
            "{}"
        );
        assert!(!field.metadata().contains_key("description"));
        assert_eq!(source.field(0).metadata()["description"].len(), 4096);

        let plain = Arc::new(Schema::new(vec![Field::new(
            "plain",
            DataType::Int64,
            false,
        )]));
        assert!(Arc::ptr_eq(&plain, &reader_schema_hint(Arc::clone(&plain))));
        let owned = reader_schema_hint(source);
        assert_eq!(owned, hint);
    }
}
