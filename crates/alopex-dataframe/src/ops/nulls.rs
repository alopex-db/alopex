use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    Int8Array, Int8Builder, StringArray, StringBuilder, UInt16Array, UInt16Builder, UInt32Array,
    UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::expr::Scalar;
use crate::ops::{FillNull, FillNullStrategy};
use crate::{DataFrameError, Result};

pub fn fill_null_batches(input: Vec<RecordBatch>, fill: &FillNull) -> Result<Vec<RecordBatch>> {
    let batch = concat_batches(&input)?;
    if batch.num_rows() == 0 {
        return Ok(vec![batch]);
    }

    let arrays = match fill {
        FillNull::Value(value) => fill_with_scalar(&batch, value)?,
        FillNull::Strategy(strategy) => fill_with_strategy(&batch, *strategy)?,
    };

    let batch = RecordBatch::try_new(batch.schema(), arrays).map_err(|e| {
        DataFrameError::schema_mismatch(format!("failed to build RecordBatch: {e}"))
    })?;
    Ok(vec![batch])
}

pub fn drop_nulls_batches(
    input: Vec<RecordBatch>,
    subset: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    let batch = concat_batches(&input)?;
    if batch.num_rows() == 0 {
        return Ok(vec![batch]);
    }
    let indices = resolve_subset(&batch, subset)?;

    let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut keep = true;
        for idx in &indices {
            if batch.column(*idx).is_null(row) {
                keep = false;
                break;
            }
        }
        mask_builder.append_value(keep);
    }
    let mask = mask_builder.finish();
    let batch = arrow::compute::filter_record_batch(&batch, &mask)
        .map_err(|source| DataFrameError::Arrow { source })?;
    Ok(vec![batch])
}

pub fn null_count_batches(input: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    if input.is_empty() {
        return Ok(vec![RecordBatch::new_empty(Arc::new(Schema::empty()))]);
    }

    let schema = input[0].schema();
    let mut counts = vec![0_u64; schema.fields().len()];
    for batch in &input {
        for (idx, col) in batch.columns().iter().enumerate() {
            counts[idx] += col.null_count() as u64;
        }
    }

    let mut fields = Vec::with_capacity(counts.len());
    let mut arrays = Vec::with_capacity(counts.len());
    for (idx, field) in schema.fields().iter().enumerate() {
        fields.push(Field::new(field.name(), DataType::UInt64, false));
        let array = UInt64Array::from(vec![counts[idx]]);
        arrays.push(Arc::new(array) as _);
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| {
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
                    "drop_nulls subset must be non-empty",
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

fn fill_with_scalar(batch: &RecordBatch, value: &Scalar) -> Result<Vec<Arc<dyn Array>>> {
    let mut arrays = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let filled: Arc<dyn Array> = match col.data_type() {
            DataType::Boolean => Arc::new(fill_boolean(col, value)?),
            DataType::Int8 => Arc::new(fill_int8(col, value)?),
            DataType::Int16 => Arc::new(fill_int16(col, value)?),
            DataType::Int32 => Arc::new(fill_int32(col, value)?),
            DataType::Int64 => Arc::new(fill_int64(col, value)?),
            DataType::UInt8 => Arc::new(fill_uint8(col, value)?),
            DataType::UInt16 => Arc::new(fill_uint16(col, value)?),
            DataType::UInt32 => Arc::new(fill_uint32(col, value)?),
            DataType::UInt64 => Arc::new(fill_uint64(col, value)?),
            DataType::Float32 => Arc::new(fill_float32(col, value)?),
            DataType::Float64 => Arc::new(fill_float64(col, value)?),
            DataType::Utf8 => Arc::new(fill_utf8(col, value)?),
            other => {
                return Err(DataFrameError::type_mismatch(
                    None::<String>,
                    other.to_string(),
                    format!("{value:?}"),
                ))
            }
        };
        arrays.push(filled);
    }
    Ok(arrays)
}

fn fill_with_strategy(
    batch: &RecordBatch,
    strategy: FillNullStrategy,
) -> Result<Vec<Arc<dyn Array>>> {
    let mut arrays = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let filled: Arc<dyn Array> = match strategy {
            FillNullStrategy::Forward => Arc::new(fill_forward(col)?),
            FillNullStrategy::Backward => Arc::new(fill_backward(col)?),
            FillNullStrategy::Min => Arc::new(fill_min(col)?),
            FillNullStrategy::Max => Arc::new(fill_max(col)?),
            FillNullStrategy::Mean => Arc::new(fill_mean(col)?),
            FillNullStrategy::Zero => Arc::new(fill_numeric_constant(col, 0)?),
            FillNullStrategy::One => Arc::new(fill_numeric_constant(col, 1)?),
        };
        arrays.push(filled);
    }
    Ok(arrays)
}

fn fill_boolean(col: &Arc<dyn Array>, value: &Scalar) -> Result<BooleanArray> {
    let array = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad BooleanArray downcast"))?;
    let fill = match value {
        Scalar::Boolean(v) => *v,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Boolean".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = BooleanBuilder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_int8(col: &Arc<dyn Array>, value: &Scalar) -> Result<Int8Array> {
    let array = col
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int8Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => i8::try_from(*v)
            .map_err(|_| DataFrameError::type_mismatch(None::<String>, "Int8", "out of range"))?,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Int8".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = Int8Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_int16(col: &Arc<dyn Array>, value: &Scalar) -> Result<Int16Array> {
    let array = col
        .as_any()
        .downcast_ref::<Int16Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int16Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => i16::try_from(*v)
            .map_err(|_| DataFrameError::type_mismatch(None::<String>, "Int16", "out of range"))?,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Int16".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = Int16Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_int32(col: &Arc<dyn Array>, value: &Scalar) -> Result<Int32Array> {
    let array = col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int32Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => i32::try_from(*v)
            .map_err(|_| DataFrameError::type_mismatch(None::<String>, "Int32", "out of range"))?,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Int32".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = Int32Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_int64(col: &Arc<dyn Array>, value: &Scalar) -> Result<Int64Array> {
    let array = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int64Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => *v,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Int64".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = Int64Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_uint8(col: &Arc<dyn Array>, value: &Scalar) -> Result<UInt8Array> {
    let array = col
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt8Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => u8::try_from(*v)
            .map_err(|_| DataFrameError::type_mismatch(None::<String>, "UInt8", "out of range"))?,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "UInt8".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = UInt8Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_uint16(col: &Arc<dyn Array>, value: &Scalar) -> Result<UInt16Array> {
    let array = col
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt16Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => u16::try_from(*v)
            .map_err(|_| DataFrameError::type_mismatch(None::<String>, "UInt16", "out of range"))?,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "UInt16".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = UInt16Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_uint32(col: &Arc<dyn Array>, value: &Scalar) -> Result<UInt32Array> {
    let array = col
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt32Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => u32::try_from(*v)
            .map_err(|_| DataFrameError::type_mismatch(None::<String>, "UInt32", "out of range"))?,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "UInt32".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = UInt32Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_uint64(col: &Arc<dyn Array>, value: &Scalar) -> Result<UInt64Array> {
    let array = col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt64Array downcast"))?;
    let fill = match value {
        Scalar::Int64(v) => u64::try_from(*v)
            .map_err(|_| DataFrameError::type_mismatch(None::<String>, "UInt64", "out of range"))?,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "UInt64".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = UInt64Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_float32(col: &Arc<dyn Array>, value: &Scalar) -> Result<Float32Array> {
    let array = col
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Float32Array downcast"))?;
    let fill = match value {
        Scalar::Float64(v) => *v as f32,
        Scalar::Int64(v) => *v as f32,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Float32".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = Float32Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_float64(col: &Arc<dyn Array>, value: &Scalar) -> Result<Float64Array> {
    let array = col
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Float64Array downcast"))?;
    let fill = match value {
        Scalar::Float64(v) => *v,
        Scalar::Int64(v) => *v as f64,
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Float64".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = Float64Builder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_utf8(col: &Arc<dyn Array>, value: &Scalar) -> Result<StringArray> {
    let array = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad StringArray downcast"))?;
    let fill = match value {
        Scalar::Utf8(v) => v.as_str(),
        Scalar::Null => return Ok(array.clone()),
        other => {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                "Utf8".to_string(),
                format!("{other:?}"),
            ))
        }
    };

    let mut builder = StringBuilder::with_capacity(array.len(), array.value_data().len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_value(fill);
        } else {
            builder.append_value(array.value(i));
        }
    }
    Ok(builder.finish())
}

fn fill_forward(col: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match col.data_type() {
        DataType::Boolean => Ok(Arc::new(fill_forward_bool(col)?)),
        DataType::Int8 => Ok(Arc::new(fill_forward_i8(col)?)),
        DataType::Int16 => Ok(Arc::new(fill_forward_i16(col)?)),
        DataType::Int32 => Ok(Arc::new(fill_forward_i32(col)?)),
        DataType::Int64 => Ok(Arc::new(fill_forward_i64(col)?)),
        DataType::UInt8 => Ok(Arc::new(fill_forward_u8(col)?)),
        DataType::UInt16 => Ok(Arc::new(fill_forward_u16(col)?)),
        DataType::UInt32 => Ok(Arc::new(fill_forward_u32(col)?)),
        DataType::UInt64 => Ok(Arc::new(fill_forward_u64(col)?)),
        DataType::Float32 => Ok(Arc::new(fill_forward_f32(col)?)),
        DataType::Float64 => Ok(Arc::new(fill_forward_f64(col)?)),
        DataType::Utf8 => Ok(Arc::new(fill_forward_utf8(col)?)),
        other => Err(DataFrameError::invalid_operation(format!(
            "unsupported fill_null forward type {other:?}",
        ))),
    }
}

fn fill_backward(col: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match col.data_type() {
        DataType::Boolean => Ok(Arc::new(fill_backward_bool(col)?)),
        DataType::Int8 => Ok(Arc::new(fill_backward_i8(col)?)),
        DataType::Int16 => Ok(Arc::new(fill_backward_i16(col)?)),
        DataType::Int32 => Ok(Arc::new(fill_backward_i32(col)?)),
        DataType::Int64 => Ok(Arc::new(fill_backward_i64(col)?)),
        DataType::UInt8 => Ok(Arc::new(fill_backward_u8(col)?)),
        DataType::UInt16 => Ok(Arc::new(fill_backward_u16(col)?)),
        DataType::UInt32 => Ok(Arc::new(fill_backward_u32(col)?)),
        DataType::UInt64 => Ok(Arc::new(fill_backward_u64(col)?)),
        DataType::Float32 => Ok(Arc::new(fill_backward_f32(col)?)),
        DataType::Float64 => Ok(Arc::new(fill_backward_f64(col)?)),
        DataType::Utf8 => Ok(Arc::new(fill_backward_utf8(col)?)),
        other => Err(DataFrameError::invalid_operation(format!(
            "unsupported fill_null backward type {other:?}",
        ))),
    }
}

fn fill_min(col: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    fill_numeric_stat(col, Stat::Min)
}

fn fill_max(col: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    fill_numeric_stat(col, Stat::Max)
}

fn fill_mean(col: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    fill_numeric_stat(col, Stat::Mean)
}

fn fill_numeric_constant(col: &Arc<dyn Array>, value: i64) -> Result<Arc<dyn Array>> {
    match col.data_type() {
        DataType::Int8 => Ok(Arc::new(fill_int8(col, &Scalar::Int64(value))?)),
        DataType::Int16 => Ok(Arc::new(fill_int16(col, &Scalar::Int64(value))?)),
        DataType::Int32 => Ok(Arc::new(fill_int32(col, &Scalar::Int64(value))?)),
        DataType::Int64 => Ok(Arc::new(fill_int64(col, &Scalar::Int64(value))?)),
        DataType::UInt8 => Ok(Arc::new(fill_uint8(col, &Scalar::Int64(value))?)),
        DataType::UInt16 => Ok(Arc::new(fill_uint16(col, &Scalar::Int64(value))?)),
        DataType::UInt32 => Ok(Arc::new(fill_uint32(col, &Scalar::Int64(value))?)),
        DataType::UInt64 => Ok(Arc::new(fill_uint64(col, &Scalar::Int64(value))?)),
        DataType::Float32 => Ok(Arc::new(fill_float32(col, &Scalar::Int64(value))?)),
        DataType::Float64 => Ok(Arc::new(fill_float64(col, &Scalar::Int64(value))?)),
        other => Err(DataFrameError::type_mismatch(
            None::<String>,
            "numeric".to_string(),
            other.to_string(),
        )),
    }
}

enum Stat {
    Min,
    Max,
    Mean,
}

fn fill_numeric_stat(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    match col.data_type() {
        DataType::Int8 => fill_stat_i8(col, stat),
        DataType::Int16 => fill_stat_i16(col, stat),
        DataType::Int32 => fill_stat_i32(col, stat),
        DataType::Int64 => fill_stat_i64(col, stat),
        DataType::UInt8 => fill_stat_u8(col, stat),
        DataType::UInt16 => fill_stat_u16(col, stat),
        DataType::UInt32 => fill_stat_u32(col, stat),
        DataType::UInt64 => fill_stat_u64(col, stat),
        DataType::Float32 => fill_stat_f32(col, stat),
        DataType::Float64 => fill_stat_f64(col, stat),
        other => Err(DataFrameError::type_mismatch(
            None::<String>,
            "numeric".to_string(),
            other.to_string(),
        )),
    }
}

fn fill_forward_bool(col: &Arc<dyn Array>) -> Result<BooleanArray> {
    let array = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad BooleanArray downcast"))?;
    let mut builder = BooleanBuilder::with_capacity(array.len());
    let mut last: Option<bool> = None;
    for i in 0..array.len() {
        if array.is_null(i) {
            match last {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        } else {
            let v = array.value(i);
            last = Some(v);
            builder.append_value(v);
        }
    }
    Ok(builder.finish())
}

fn fill_backward_bool(col: &Arc<dyn Array>) -> Result<BooleanArray> {
    let array = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad BooleanArray downcast"))?;
    let mut tmp: Vec<Option<bool>> = Vec::with_capacity(array.len());
    let mut next: Option<bool> = None;
    for i in (0..array.len()).rev() {
        if array.is_null(i) {
            tmp.push(next);
        } else {
            let v = array.value(i);
            next = Some(v);
            tmp.push(Some(v));
        }
    }
    tmp.reverse();
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for v in tmp {
        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

macro_rules! fill_forward_numeric {
    ($name:ident, $array_ty:ty, $builder_ty:ty, $cast:expr) => {
        fn $name(col: &Arc<dyn Array>) -> Result<$array_ty> {
            let array = col
                .as_any()
                .downcast_ref::<$array_ty>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad array downcast"))?;
            let mut builder = <$builder_ty>::with_capacity(array.len());
            let mut last = None;
            for i in 0..array.len() {
                if array.is_null(i) {
                    match last {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                } else {
                    let v = array.value(i);
                    let v = $cast(v);
                    last = Some(v);
                    builder.append_value(v);
                }
            }
            Ok(builder.finish())
        }
    };
}

macro_rules! fill_backward_numeric {
    ($name:ident, $array_ty:ty, $builder_ty:ty, $cast:expr) => {
        fn $name(col: &Arc<dyn Array>) -> Result<$array_ty> {
            let array = col
                .as_any()
                .downcast_ref::<$array_ty>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad array downcast"))?;
            let mut tmp = Vec::with_capacity(array.len());
            let mut next = None;
            for i in (0..array.len()).rev() {
                if array.is_null(i) {
                    tmp.push(next);
                } else {
                    let v = $cast(array.value(i));
                    next = Some(v);
                    tmp.push(Some(v));
                }
            }
            tmp.reverse();
            let mut builder = <$builder_ty>::with_capacity(array.len());
            for v in tmp {
                match v {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(builder.finish())
        }
    };
}

fill_forward_numeric!(fill_forward_i8, Int8Array, Int8Builder, |v: i8| v);
fill_forward_numeric!(fill_forward_i16, Int16Array, Int16Builder, |v: i16| v);
fill_forward_numeric!(fill_forward_i32, Int32Array, Int32Builder, |v: i32| v);
fill_forward_numeric!(fill_forward_i64, Int64Array, Int64Builder, |v: i64| v);
fill_forward_numeric!(fill_forward_u8, UInt8Array, UInt8Builder, |v: u8| v);
fill_forward_numeric!(fill_forward_u16, UInt16Array, UInt16Builder, |v: u16| v);
fill_forward_numeric!(fill_forward_u32, UInt32Array, UInt32Builder, |v: u32| v);
fill_forward_numeric!(fill_forward_u64, UInt64Array, UInt64Builder, |v: u64| v);
fill_forward_numeric!(fill_forward_f32, Float32Array, Float32Builder, |v: f32| v);
fill_forward_numeric!(fill_forward_f64, Float64Array, Float64Builder, |v: f64| v);

fill_backward_numeric!(fill_backward_i8, Int8Array, Int8Builder, |v: i8| v);
fill_backward_numeric!(fill_backward_i16, Int16Array, Int16Builder, |v: i16| v);
fill_backward_numeric!(fill_backward_i32, Int32Array, Int32Builder, |v: i32| v);
fill_backward_numeric!(fill_backward_i64, Int64Array, Int64Builder, |v: i64| v);
fill_backward_numeric!(fill_backward_u8, UInt8Array, UInt8Builder, |v: u8| v);
fill_backward_numeric!(fill_backward_u16, UInt16Array, UInt16Builder, |v: u16| v);
fill_backward_numeric!(fill_backward_u32, UInt32Array, UInt32Builder, |v: u32| v);
fill_backward_numeric!(fill_backward_u64, UInt64Array, UInt64Builder, |v: u64| v);
fill_backward_numeric!(fill_backward_f32, Float32Array, Float32Builder, |v: f32| v);
fill_backward_numeric!(fill_backward_f64, Float64Array, Float64Builder, |v: f64| v);

fn fill_forward_utf8(col: &Arc<dyn Array>) -> Result<StringArray> {
    let array = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad StringArray downcast"))?;
    let mut builder = StringBuilder::with_capacity(array.len(), array.value_data().len());
    let mut last: Option<String> = None;
    for i in 0..array.len() {
        if array.is_null(i) {
            match last.as_deref() {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        } else {
            let v = array.value(i).to_string();
            last = Some(v.clone());
            builder.append_value(v);
        }
    }
    Ok(builder.finish())
}

fn fill_backward_utf8(col: &Arc<dyn Array>) -> Result<StringArray> {
    let array = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad StringArray downcast"))?;
    let mut tmp = Vec::with_capacity(array.len());
    let mut next: Option<String> = None;
    for i in (0..array.len()).rev() {
        if array.is_null(i) {
            tmp.push(next.clone());
        } else {
            let v = array.value(i).to_string();
            next = Some(v.clone());
            tmp.push(Some(v));
        }
    }
    tmp.reverse();
    let mut builder = StringBuilder::with_capacity(array.len(), array.value_data().len());
    for v in tmp {
        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

fn fill_stat_i8(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int8Array downcast"))?;
    let (min, max, mean) = stats_signed(array.iter().flatten().map(|v| v as i128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_int8(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_i16(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<Int16Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int16Array downcast"))?;
    let (min, max, mean) = stats_signed(array.iter().flatten().map(|v| v as i128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_int16(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_i32(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int32Array downcast"))?;
    let (min, max, mean) = stats_signed(array.iter().flatten().map(|v| v as i128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_int32(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_i64(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Int64Array downcast"))?;
    let (min, max, mean) = stats_signed(array.iter().flatten().map(|v| v as i128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_int64(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_u8(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt8Array downcast"))?;
    let (min, max, mean) = stats_unsigned(array.iter().flatten().map(|v| v as u128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_uint8(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_u16(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt16Array downcast"))?;
    let (min, max, mean) = stats_unsigned(array.iter().flatten().map(|v| v as u128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_uint16(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_u32(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt32Array downcast"))?;
    let (min, max, mean) = stats_unsigned(array.iter().flatten().map(|v| v as u128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_uint32(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_u64(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad UInt64Array downcast"))?;
    let (min, max, mean) = stats_unsigned(array.iter().flatten().map(|v| v as u128));
    let value = match stat {
        Stat::Min => min.map(|v| v as i64),
        Stat::Max => max.map(|v| v as i64),
        Stat::Mean => mean.map(|v| v as i64),
    };
    match value {
        Some(v) => Ok(Arc::new(fill_uint64(col, &Scalar::Int64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_f32(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Float32Array downcast"))?;
    let (min, max, mean) = stats_float(array.iter().flatten().map(|v| v as f64));
    let value = match stat {
        Stat::Min => min,
        Stat::Max => max,
        Stat::Mean => mean,
    };
    match value {
        Some(v) => Ok(Arc::new(fill_float32(col, &Scalar::Float64(v))?)),
        None => Ok(col.clone()),
    }
}

fn fill_stat_f64(col: &Arc<dyn Array>, stat: Stat) -> Result<Arc<dyn Array>> {
    let array = col
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFrameError::invalid_operation("bad Float64Array downcast"))?;
    let (min, max, mean) = stats_float(array.iter().flatten());
    let value = match stat {
        Stat::Min => min,
        Stat::Max => max,
        Stat::Mean => mean,
    };
    match value {
        Some(v) => Ok(Arc::new(fill_float64(col, &Scalar::Float64(v))?)),
        None => Ok(col.clone()),
    }
}

fn stats_signed<I>(values: I) -> (Option<i128>, Option<i128>, Option<i128>)
where
    I: Iterator<Item = i128>,
{
    let mut min: Option<i128> = None;
    let mut max: Option<i128> = None;
    let mut sum: i128 = 0;
    let mut count = 0;
    for v in values {
        min = Some(min.map_or(v, |m| m.min(v)));
        max = Some(max.map_or(v, |m| m.max(v)));
        sum += v;
        count += 1;
    }
    let mean = sum.checked_div(count);
    (min, max, mean)
}

fn stats_unsigned<I>(values: I) -> (Option<u128>, Option<u128>, Option<u128>)
where
    I: Iterator<Item = u128>,
{
    let mut min: Option<u128> = None;
    let mut max: Option<u128> = None;
    let mut sum: u128 = 0;
    let mut count = 0;
    for v in values {
        min = Some(min.map_or(v, |m| m.min(v)));
        max = Some(max.map_or(v, |m| m.max(v)));
        sum += v;
        count += 1;
    }
    let mean = sum.checked_div(count);
    (min, max, mean)
}

fn stats_float<I>(values: I) -> (Option<f64>, Option<f64>, Option<f64>)
where
    I: Iterator<Item = f64>,
{
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    let mut sum = 0.0_f64;
    let mut count = 0;
    for v in values {
        min = Some(min.map_or(v, |m| m.min(v)));
        max = Some(max.map_or(v, |m| m.max(v)));
        sum += v;
        count += 1;
    }
    let mean = if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    };
    (min, max, mean)
}
