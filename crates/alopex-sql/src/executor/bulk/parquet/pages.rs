use std::fs::File;
use std::sync::Arc;

use arrow_schema::Schema;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroups};
use parquet::arrow::{ProjectionMask, parquet_to_arrow_field_levels};
use parquet::column::page::{Page, PageIterator, PageMetadata, PageReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::serialized_reader::SerializedPageReader;

mod bounded;

pub(super) fn reader(
    file: File,
    metadata: Arc<ParquetMetaData>,
    schema: &Schema,
    batch_size: usize,
    limit: u64,
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
        file: Arc::new(file),
        metadata,
        num_rows,
        limit,
    };
    ParquetRecordBatchReader::try_new_with_row_groups(
        &levels,
        &groups,
        batch_size.min(declared_rows),
        None,
    )
}

struct FileRowGroups {
    file: Arc<File>,
    metadata: Arc<ParquetMetaData>,
    num_rows: usize,
    limit: u64,
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
        }))
    }
}

struct ColumnChunks {
    file: Arc<File>,
    metadata: Arc<ParquetMetaData>,
    column: usize,
    next_group: usize,
    limit: u64,
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
            if matches!(
                column.compression(),
                parquet::basic::Compression::GZIP(_) | parquet::basic::Compression::ZSTD(_)
            ) {
                return Ok(Box::new(bounded::BoundedPages::new(
                    Arc::clone(&self.file),
                    column,
                    group.num_rows() as usize,
                    locations,
                    self.limit,
                )?) as Box<dyn PageReader>);
            }
            let inner = SerializedPageReader::new(
                Arc::clone(&self.file),
                column,
                group.num_rows() as usize,
                locations,
            )?;
            Ok(Box::new(ColumnPages { inner }) as Box<dyn PageReader>)
        })())
    }
}

impl PageIterator for ColumnChunks {}

struct ColumnPages {
    inner: SerializedPageReader<File>,
}

impl Iterator for ColumnPages {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for ColumnPages {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        // shortcut: Brotli and legacy LZ4 working allocations still need budget enforcement.
        self.inner.get_next_page()
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
        ] {
            assert_indexed_pages(compression);
        }
    }

    fn assert_indexed_pages(compression: parquet::basic::Compression) {
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
            file: Arc::new(input.try_clone().unwrap()),
            metadata: Arc::clone(&metadata),
            num_rows: 4,
            limit: 65536,
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

        let actual = reader(input, metadata, builder.schema(), 1024, 65536)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(actual, vec![batch]);
    }
}
