use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow_schema::Schema;
use parquet::file::metadata::ParquetMetaDataReader;

use super::parquet_io_error;
use super::preflight::{check_declared_bytes, check_schema_projection, schema_footer};
use crate::executor::{ExecutorError, Result};

/// Budget only metadata retained by schema planning before owned decoding.
/// The data reader retains responsibility for decoding and validating row groups.
pub(super) fn read(file: &mut File, limit: u64) -> Result<Arc<Schema>> {
    let footer = schema_footer(file, limit)?;
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
    check_declared_bytes(length as u64, limit)?;
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

    // Validate the copied bytes before owned decoding, including any changed schema/KV data.
    let has_arrow_schema = check_schema_projection(&bytes, limit)?;
    let metadata = ParquetMetaDataReader::decode_metadata(&bytes).map_err(|error| {
        ExecutorError::BulkLoad(format!("failed to read parquet metadata: {error}"))
    })?;
    let file_metadata = metadata.file_metadata();
    let key_values = if has_arrow_schema {
        file_metadata.key_value_metadata()
    } else {
        None
    };
    let schema = parquet::arrow::parquet_to_arrow_schema(file_metadata.schema_descr(), key_values)
        .map_err(|error| {
            ExecutorError::BulkLoad(format!("failed to read parquet metadata: {error}"))
        })?;
    Ok(Arc::new(schema))
}
