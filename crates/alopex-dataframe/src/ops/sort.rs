use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::array::{Float32Array, Float64Array, UInt32Builder};
use arrow::array::{StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::ops::SortOptions;
use crate::{DataFrameError, Result};

#[derive(Clone)]
struct RowKey {
    index: usize,
    values: Vec<Option<SortValue>>,
}

#[derive(Clone, Debug, PartialEq)]
enum SortValue {
    Boolean(bool),
    Signed(i128),
    Unsigned(u128),
    Float64(f64),
    Utf8(String),
}

pub fn sort_batches(input: Vec<RecordBatch>, options: &SortOptions) -> Result<Vec<RecordBatch>> {
    let batch = concat_batches(&input)?;
    if batch.num_rows() == 0 {
        return Ok(vec![batch]);
    }
    if options.by.is_empty() {
        return Err(DataFrameError::invalid_operation("sort requires columns"));
    }
    if options.by.len() != options.descending.len() {
        return Err(DataFrameError::invalid_operation(
            "descending length must match sort columns",
        ));
    }

    let columns = build_sort_columns(&batch, &options.by)?;

    let mut keys = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(columns.len());
        for col in &columns {
            values.push(col.value(row)?);
        }
        keys.push(RowKey { index: row, values });
    }

    keys.sort_by(|a, b| compare_keys(a, b, &options.descending));

    let index_array = build_indices(keys.iter().map(|k| k.index))?;
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

pub fn slice_batches(
    input: Vec<RecordBatch>,
    offset: usize,
    len: usize,
    from_end: bool,
) -> Result<Vec<RecordBatch>> {
    let batch = concat_batches(&input)?;
    let total = batch.num_rows();
    if total == 0 || len == 0 {
        return Ok(vec![batch.slice(0, 0)]);
    }

    let start = if from_end {
        total.saturating_sub(offset + len)
    } else {
        offset
    };
    if start >= total {
        return Ok(vec![batch.slice(0, 0)]);
    }
    let end = std::cmp::min(start + len, total);
    Ok(vec![batch.slice(start, end - start)])
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

struct SortColumn {
    name: String,
    data: SortColumnData,
}

enum SortColumnData {
    Boolean(Arc<BooleanArray>),
    Int8(Arc<Int8Array>),
    Int16(Arc<Int16Array>),
    Int32(Arc<Int32Array>),
    Int64(Arc<Int64Array>),
    UInt8(Arc<UInt8Array>),
    UInt16(Arc<UInt16Array>),
    UInt32(Arc<UInt32Array>),
    UInt64(Arc<UInt64Array>),
    Float32(Arc<Float32Array>),
    Float64(Arc<Float64Array>),
    Utf8(Arc<StringArray>),
}

impl SortColumn {
    fn value(&self, row: usize) -> Result<Option<SortValue>> {
        match &self.data {
            SortColumnData::Boolean(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Boolean(array.value(row))))
                }
            }
            SortColumnData::Int8(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Signed(array.value(row) as i128)))
                }
            }
            SortColumnData::Int16(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Signed(array.value(row) as i128)))
                }
            }
            SortColumnData::Int32(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Signed(array.value(row) as i128)))
                }
            }
            SortColumnData::Int64(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Signed(array.value(row) as i128)))
                }
            }
            SortColumnData::UInt8(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Unsigned(array.value(row) as u128)))
                }
            }
            SortColumnData::UInt16(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Unsigned(array.value(row) as u128)))
                }
            }
            SortColumnData::UInt32(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Unsigned(array.value(row) as u128)))
                }
            }
            SortColumnData::UInt64(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Unsigned(array.value(row) as u128)))
                }
            }
            SortColumnData::Float32(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Float64(array.value(row) as f64)))
                }
            }
            SortColumnData::Float64(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Float64(array.value(row))))
                }
            }
            SortColumnData::Utf8(array) => {
                if array.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(SortValue::Utf8(array.value(row).to_string())))
                }
            }
        }
    }
}

fn build_sort_columns(batch: &RecordBatch, by: &[String]) -> Result<Vec<SortColumn>> {
    let mut columns = Vec::with_capacity(by.len());

    for name in by {
        let idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
        let array = batch.column(idx);
        let data = match array.data_type() {
            DataType::Boolean => SortColumnData::Boolean(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad BooleanArray"))?
                    .clone(),
            )),
            DataType::Int8 => SortColumnData::Int8(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad Int8Array"))?
                    .clone(),
            )),
            DataType::Int16 => SortColumnData::Int16(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad Int16Array"))?
                    .clone(),
            )),
            DataType::Int32 => SortColumnData::Int32(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad Int32Array"))?
                    .clone(),
            )),
            DataType::Int64 => SortColumnData::Int64(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad Int64Array"))?
                    .clone(),
            )),
            DataType::UInt8 => SortColumnData::UInt8(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad UInt8Array"))?
                    .clone(),
            )),
            DataType::UInt16 => SortColumnData::UInt16(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad UInt16Array"))?
                    .clone(),
            )),
            DataType::UInt32 => SortColumnData::UInt32(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad UInt32Array"))?
                    .clone(),
            )),
            DataType::UInt64 => SortColumnData::UInt64(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad UInt64Array"))?
                    .clone(),
            )),
            DataType::Float32 => SortColumnData::Float32(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad Float32Array"))?
                    .clone(),
            )),
            DataType::Float64 => SortColumnData::Float64(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad Float64Array"))?
                    .clone(),
            )),
            DataType::Utf8 => SortColumnData::Utf8(Arc::new(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFrameError::invalid_operation("bad StringArray"))?
                    .clone(),
            )),
            other => {
                return Err(DataFrameError::invalid_operation(format!(
                    "unsupported sort type {other:?}",
                )))
            }
        };

        columns.push(SortColumn {
            name: name.clone(),
            data,
        });
    }

    let mut seen = HashSet::new();
    for col in &columns {
        if !seen.insert(col.name.clone()) {
            return Err(DataFrameError::invalid_operation("duplicate sort column"));
        }
    }

    Ok(columns)
}

fn compare_keys(a: &RowKey, b: &RowKey, descending: &[bool]) -> Ordering {
    for (idx, (av, bv)) in a.values.iter().zip(b.values.iter()).enumerate() {
        match (av, bv) {
            (None, None) => continue,
            (None, Some(_)) => return Ordering::Greater,
            (Some(_), None) => return Ordering::Less,
            (Some(av), Some(bv)) => {
                let mut ord = compare_value(av, bv);
                if descending[idx] {
                    ord = ord.reverse();
                }
                if ord != Ordering::Equal {
                    return ord;
                }
            }
        }
    }
    Ordering::Equal
}

fn compare_value(a: &SortValue, b: &SortValue) -> Ordering {
    match (a, b) {
        (SortValue::Boolean(a), SortValue::Boolean(b)) => a.cmp(b),
        (SortValue::Signed(a), SortValue::Signed(b)) => a.cmp(b),
        (SortValue::Unsigned(a), SortValue::Unsigned(b)) => a.cmp(b),
        (SortValue::Float64(a), SortValue::Float64(b)) => a.total_cmp(b),
        (SortValue::Utf8(a), SortValue::Utf8(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn build_indices<I>(indices: I) -> Result<arrow::array::UInt32Array>
where
    I: IntoIterator<Item = usize>,
{
    let iter = indices.into_iter();
    let (lower, _) = iter.size_hint();
    let mut builder = UInt32Builder::with_capacity(lower);
    for idx in iter {
        let value = u32::try_from(idx)
            .map_err(|_| DataFrameError::invalid_operation("row index exceeds u32 range"))?;
        builder.append_value(value);
    }
    Ok(builder.finish())
}
