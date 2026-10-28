use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow_schema::Schema;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use super::parquet_io_error;
use super::preflight::{check_declared_bytes, check_schema_projection, schema_footer};
use crate::executor::{ExecutorError, Result};

/// Budget only metadata retained by schema planning before owned decoding.
/// The data reader retains responsibility for decoding and validating row groups.
pub(super) fn read(file: &mut (impl Read + Seek), limit: Option<u64>) -> Result<Arc<Schema>> {
    let footer = schema_footer(file, limit.unwrap_or(u64::MAX))?;
    let invalid = || ExecutorError::BulkLoad("invalid parquet schema projection".into());
    let groups = &footer.row_groups;
    let end = footer.start.checked_add(footer.size).ok_or_else(invalid)?;
    if groups.start < footer.start || groups.start >= groups.end || groups.end > end {
        return Err(invalid());
    }
    let prefix = usize::try_from(groups.start - footer.start).map_err(|_| invalid())?;
    let suffix = usize::try_from(end - groups.end).map_err(|_| invalid())?;
    let length = prefix
        .checked_add(1)
        .and_then(|length| length.checked_add(suffix))
        .ok_or_else(invalid)?;
    check_declared_bytes(length as u64, limit.unwrap_or(u64::MAX))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| invalid())?;
    bytes.resize(length, 0);
    file.seek(SeekFrom::Start(footer.start))
        .map_err(parquet_io_error)?;
    file.read_exact(&mut bytes[..prefix])
        .map_err(parquet_io_error)?;
    // Preserve all other metadata bytes; only the unused row-group list is projected out.
    bytes[prefix] = 0x0c; // Compact Thrift: empty list of structs.
    file.seek(SeekFrom::Start(groups.end))
        .map_err(parquet_io_error)?;
    file.read_exact(&mut bytes[prefix + 1..])
        .map_err(parquet_io_error)?;

    // Configured budgets must cover copied schema/KV bytes before owned decoding.
    let budgeted_arrow_schema = limit
        .map(|limit| check_schema_projection(&bytes, limit))
        .transpose()?;
    let metadata = ParquetMetaDataReader::decode_metadata(&bytes).map_err(|error| {
        ExecutorError::BulkLoad(format!("failed to read parquet metadata: {error}"))
    })?;
    let reader_metadata =
        reader_metadata(metadata, budgeted_arrow_schema == Some(false)).map_err(|error| {
            ExecutorError::BulkLoad(format!("failed to read parquet metadata: {error}"))
        })?;
    Ok(Arc::clone(reader_metadata.schema()))
}

/// SQL exposes positional types and values, not Arrow's application metadata.
/// Reuse Parquet's supplied-schema validation to avoid cloning those maps while
/// it constructs field levels. Incompatible hints use its ordinary conversion.
pub(super) fn reader_metadata(
    metadata: ParquetMetaData,
    skip_arrow_metadata: bool,
) -> parquet::errors::Result<ArrowReaderMetadata> {
    let metadata = Arc::new(metadata);
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(skip_arrow_metadata);
    if !skip_arrow_metadata {
        let encoded = metadata
            .file_metadata()
            .key_value_metadata()
            .into_iter()
            .flatten()
            .filter(|entry| entry.key == parquet::arrow::ARROW_SCHEMA_META_KEY)
            .filter_map(|entry| entry.value.as_deref())
            .next_back();
        if let Some(schema) = encoded.and_then(sql_arrow_hint)
            && let Ok(reader) = ArrowReaderMetadata::try_new(
                Arc::clone(&metadata),
                options.clone().with_schema(Arc::new(schema)),
            )
        {
            return Ok(reader);
        }
    }
    ArrowReaderMetadata::try_new(metadata, options)
}

fn sql_arrow_hint(encoded: &str) -> Option<Schema> {
    use base64::Engine;
    // Match Parquet 53's continuation-marker and legacy raw-message handling.
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let bytes = if decoded.len() > 8 && decoded[..4] == [255; 4] {
        &decoded[8..]
    } else {
        &decoded
    };
    let message = arrow_ipc::root_as_message(bytes).ok()?;
    let ipc_schema = message.header_as_schema()?;
    if ipc_schema.endianness() != arrow_ipc::Endianness::Little {
        // Keep the reference converter's endian handling, including Decimal.
        return Some(strip_arrow_application_metadata(
            arrow_ipc::convert::try_fb_to_schema(ipc_schema).ok()?,
        ));
    }
    let fields = ipc_schema
        .fields()?
        .iter()
        .map(sql_arrow_field)
        .collect::<Vec<_>>();
    let metadata = sql_arrow_metadata(
        ipc_schema
            .custom_metadata()
            .into_iter()
            .flat_map(|values| values.iter()),
    );
    Some(Schema::new_with_metadata(fields, metadata))
}

fn sql_arrow_field(field: arrow_ipc::Field<'_>) -> arrow_schema::Field {
    use arrow_schema::{DataType, Field};

    // These common SQL scalars need no recursive or dictionary conversion.
    // Keep the public reference converter for every other Arrow type.
    let scalar = if field.dictionary().is_none() {
        match field.type_type() {
            arrow_ipc::Type::Int => field
                .type_as_int()
                .filter(|integer| integer.is_signed() && integer.bitWidth() == 64)
                .map(|_| DataType::Int64),
            arrow_ipc::Type::LargeBinary => Some(DataType::LargeBinary),
            _ => None,
        }
    } else {
        None
    };
    let arrow_field = match scalar {
        Some(data_type) => Field::new(field.name().unwrap(), data_type, field.nullable()),
        None => Field::from(field),
    };
    arrow_field.with_metadata(sql_arrow_metadata(
        field
            .custom_metadata()
            .into_iter()
            .flat_map(|values| values.iter()),
    ))
}

fn sql_arrow_metadata<'a>(
    values: impl Iterator<Item = arrow_ipc::KeyValue<'a>>,
) -> std::collections::HashMap<String, String> {
    values
        .filter_map(|entry| {
            let (key, value) = (entry.key()?, entry.value()?);
            is_reserved_arrow_key(key).then(|| (key.to_owned(), value.to_owned()))
        })
        .collect()
}

fn is_reserved_arrow_key(key: &str) -> bool {
    key.starts_with("PARQUET:") || key.starts_with("ARROW:")
}

fn strip_arrow_application_metadata(schema: Schema) -> Schema {
    let Schema { fields, metadata } = schema;
    let metadata: arrow_schema::Metadata = metadata
        .into_iter()
        .filter(|(key, _)| is_reserved_arrow_key(key))
        .collect();
    // Drop the slice's references before consuming the fields, so stripping
    // metadata does not first clone the metadata maps we intend to discard.
    let owned = fields.iter().cloned().collect::<Vec<_>>();
    drop(fields);
    let fields = owned
        .into_iter()
        .map(|field| {
            let field = Arc::unwrap_or_clone(field);
            let metadata: arrow_schema::Metadata = field
                .metadata()
                .iter()
                .filter(|(key, _)| is_reserved_arrow_key(key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            field.with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, LargeBinaryArray, RecordBatch};
    use arrow_schema::{DataType, Field};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::FileMetaData;

    fn fixture(binary: bool, entries: usize, field_id: bool) -> Bytes {
        let mut metadata = (0..entries)
            .map(|i| (format!("application{i}"), String::new()))
            .collect::<std::collections::HashMap<_, _>>();
        if field_id {
            metadata.insert("PARQUET:field_id".into(), "7".into());
            metadata.insert("ARROW:extension:name".into(), "example.type".into());
        }
        let (data_type, array): (_, ArrayRef) = if binary {
            (
                DataType::LargeBinary,
                Arc::new(LargeBinaryArray::from(vec![Some(b"abc".as_slice()), None])),
            )
        } else {
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![Some(7), None])),
            )
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", data_type, true).with_metadata(metadata),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes.into()
    }

    #[test]
    fn sql_schema_discards_application_maps_without_changing_values() {
        for binary in [false, true] {
            let bytes = fixture(binary, 512, false);
            let metadata = ParquetMetaDataReader::new()
                .parse_and_finish(&bytes)
                .unwrap();
            let reference =
                ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), Default::default())
                    .unwrap();
            let projected = reader_metadata(metadata, false).unwrap();
            assert_eq!(reference.schema().field(0).metadata().len(), 512);
            assert!(projected.schema().field(0).metadata().is_empty());
            assert_eq!(
                reference.schema().field(0).data_type(),
                projected.schema().field(0).data_type()
            );
            let collect = |metadata| {
                super::super::ParquetRecordBatchReaderBuilder::new_with_metadata(
                    bytes.clone(),
                    metadata,
                )
                .build()
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
            };
            let expected = collect(reference);
            let actual = collect(projected);
            assert_eq!(expected.len(), actual.len());
            for (expected, actual) in expected.iter().zip(actual.iter()) {
                assert_eq!(expected.columns(), actual.columns());
            }
            println!(
                "SQL_ARROW_MAP binary={binary} reference_entries=512 projected_entries=0 values_equal=true"
            );
        }
    }

    #[test]
    fn sql_schema_mixed_fields_keep_order_nullability_and_dictionary_properties() {
        use arrow_array::{DictionaryArray, Int32Array, UInt64Array, types::Int8Type};
        use std::collections::HashMap;

        let dictionary = DictionaryArray::<Int8Type>::try_new(
            arrow_array::Int8Array::from(vec![Some(0), None]),
            Arc::new(Int64Array::from(vec![19])),
        )
        .unwrap();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"abc".as_slice()), None])),
            Arc::new(Int32Array::from(vec![-4, 5])),
            Arc::new(UInt64Array::from(vec![Some(17), None])),
            Arc::new(dictionary),
        ];
        let fields = arrays
            .iter()
            .enumerate()
            .map(|(index, array)| {
                Field::new(
                    format!("field{index}"),
                    array.data_type().clone(),
                    array.null_count() != 0,
                )
                .with_metadata(HashMap::from([
                    ("PARQUET:field_id".into(), (index + 1).to_string()),
                    ("ARROW:extension:name".into(), "example.type".into()),
                    ("application".into(), "discarded".into()),
                ]))
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([
                ("ARROW:example".into(), "root".into()),
                ("application".into(), "discarded".into()),
            ]),
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = Bytes::from(buffer);
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&bytes)
            .unwrap();
        let reference =
            ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), Default::default()).unwrap();
        let actual = reader_metadata(metadata, false).unwrap();
        assert_eq!(actual.schema().fields().len(), 5);
        for (index, (actual, expected)) in actual
            .schema()
            .fields()
            .iter()
            .zip(reference.schema().fields())
            .enumerate()
        {
            let expected = expected.as_ref().clone().with_metadata(HashMap::from([
                ("PARQUET:field_id".into(), (index + 1).to_string()),
                ("ARROW:extension:name".into(), "example.type".into()),
            ]));
            assert_eq!(actual.as_ref(), &expected);
        }
        assert_eq!(
            actual.schema().metadata(),
            &HashMap::from([("ARROW:example".into(), "root".into())])
        );
        let collect = |metadata| {
            super::super::ParquetRecordBatchReaderBuilder::new_with_metadata(
                bytes.clone(),
                metadata,
            )
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
        };
        let actual = collect(actual);
        let expected = collect(reference);
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            assert_eq!(actual.columns(), expected.columns());
        }
    }

    #[test]
    fn sql_schema_preserves_field_ids_and_incompatible_hint_fallback() {
        for incompatible_hint in [false, true] {
            let bytes = fixture(false, 2, !incompatible_hint);
            let mut metadata = ParquetMetaDataReader::new()
                .parse_and_finish(&bytes)
                .unwrap();
            if incompatible_hint {
                let hint_bytes = fixture(true, 2, false);
                let hint = ParquetMetaDataReader::new()
                    .parse_and_finish(&hint_bytes)
                    .unwrap();
                let physical = metadata.file_metadata();
                let file = FileMetaData::new(
                    physical.version(),
                    physical.num_rows(),
                    physical.created_by().map(str::to_owned),
                    hint.file_metadata().key_value_metadata().cloned(),
                    physical.schema_descr_ptr(),
                    physical.column_orders().cloned(),
                );
                metadata = ParquetMetaData::new(file, metadata.row_groups().to_vec());
            }
            let reference =
                ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), Default::default());
            let actual = reader_metadata(metadata, false);
            match (reference, actual) {
                (Ok(reference), Ok(actual)) => {
                    if incompatible_hint {
                        assert_eq!(reference.schema(), actual.schema());
                    } else {
                        let field = actual.schema().field(0);
                        assert_eq!(field.data_type(), reference.schema().field(0).data_type());
                        assert_eq!(
                            field.metadata().get("PARQUET:field_id").map(String::as_str),
                            Some("7")
                        );
                        assert_eq!(
                            field
                                .metadata()
                                .get("ARROW:extension:name")
                                .map(String::as_str),
                            Some("example.type")
                        );
                        assert_eq!(field.metadata().len(), 2);
                    }
                }
                (Err(_), Err(_)) => {}
                _ => panic!("SQL metadata projection changed physical-schema fallback"),
            }
        }
    }
}
