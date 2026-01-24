use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, UInt32Builder};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::{DataFrameError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RowKey(Vec<KeyValue>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyValue {
    Null { dtype: DataType },
    Boolean(bool),
    Signed(i128),
    Unsigned(u128),
    Float32(u32),
    Float64(u64),
    Utf8(String),
}

pub fn unique_batches(
    input: Vec<RecordBatch>,
    subset: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    let batch = concat_batches(&input)?;
    if batch.num_rows() == 0 {
        return Ok(vec![batch]);
    }

    let indices = resolve_subset(&batch, subset)?;

    let mut seen = HashSet::<RowKey>::new();
    let mut selected = Vec::new();
    for row in 0..batch.num_rows() {
        let key = build_key(&batch, &indices, row)?;
        if seen.insert(key) {
            selected.push(row);
        }
    }

    let index_array = build_indices(&selected)?;
    let mut arrays = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let array = arrow::compute::take(col.as_ref(), &index_array, None)
            .map_err(|source| DataFrameError::Arrow { source })?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(batch.schema(), arrays).map_err(|e| {
        DataFrameError::schema_mismatch(format!("failed to build RecordBatch: {e}"))
    })?;
    Ok(vec![batch])
}

fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    let schema = batches[0].schema();
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    arrow::compute::concat_batches(&schema, batches)
        .map_err(|source| DataFrameError::Arrow { source })
}

fn resolve_subset(batch: &RecordBatch, subset: Option<&[String]>) -> Result<Vec<usize>> {
    let schema = batch.schema();
    let indices = match subset {
        Some(cols) => {
            if cols.is_empty() {
                return Err(DataFrameError::invalid_operation(
                    "unique subset must be non-empty",
                ));
            }
            cols.iter()
                .map(|name| {
                    schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                        .ok_or_else(|| DataFrameError::column_not_found(name.clone()))
                })
                .collect::<Result<Vec<_>>>()?
        }
        None => (0..schema.fields().len()).collect(),
    };
    Ok(indices)
}

fn build_indices(indices: &[usize]) -> Result<arrow::array::UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(indices.len());
    for idx in indices {
        let value = u32::try_from(*idx)
            .map_err(|_| DataFrameError::invalid_operation("row index exceeds u32 range"))?;
        builder.append_value(value);
    }
    Ok(builder.finish())
}

fn build_key(batch: &RecordBatch, indices: &[usize], row: usize) -> Result<RowKey> {
    let mut values = Vec::with_capacity(indices.len());
    for idx in indices {
        let array = batch.column(*idx).as_ref();
        values.push(key_value_from_array(array, row)?);
    }
    Ok(RowKey(values))
}

fn key_value_from_array(array: &dyn Array, row: usize) -> Result<KeyValue> {
    if array.is_null(row) {
        return Ok(KeyValue::Null {
            dtype: array.data_type().clone(),
        });
    }

    use arrow::datatypes::DataType::*;
    match array.data_type() {
        Boolean => Ok(KeyValue::Boolean(
            array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad BooleanArray downcast"))?
                .value(row),
        )),
        Int8 => Ok(KeyValue::Signed(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int8Array downcast"))?
                .value(row) as i128,
        )),
        Int16 => Ok(KeyValue::Signed(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int16Array downcast"))?
                .value(row) as i128,
        )),
        Int32 => Ok(KeyValue::Signed(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int32Array downcast"))?
                .value(row) as i128,
        )),
        Int64 => Ok(KeyValue::Signed(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int64Array downcast"))?
                .value(row) as i128,
        )),
        UInt8 => Ok(KeyValue::Unsigned(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt8Array downcast"))?
                .value(row) as u128,
        )),
        UInt16 => Ok(KeyValue::Unsigned(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt16Array downcast"))?
                .value(row) as u128,
        )),
        UInt32 => Ok(KeyValue::Unsigned(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt32Array downcast"))?
                .value(row) as u128,
        )),
        UInt64 => Ok(KeyValue::Unsigned(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt64Array downcast"))?
                .value(row) as u128,
        )),
        Float32 => Ok(KeyValue::Float32(
            array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Float32Array downcast"))?
                .value(row)
                .to_bits(),
        )),
        Float64 => Ok(KeyValue::Float64(
            array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Float64Array downcast"))?
                .value(row)
                .to_bits(),
        )),
        Utf8 => Ok(KeyValue::Utf8(
            array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad StringArray downcast"))?
                .value(row)
                .to_string(),
        )),
        other => Err(DataFrameError::invalid_operation(format!(
            "unsupported unique key type {other:?}",
        ))),
    }
}
