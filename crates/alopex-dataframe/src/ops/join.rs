use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::ops::{JoinKeys, JoinType};
use crate::{DataFrameError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JoinKey(Vec<KeyValue>);

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

pub fn join_batches(
    left_batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
    keys: &JoinKeys,
    how: &JoinType,
) -> Result<Vec<RecordBatch>> {
    let left_batch = concat_batches(&left_batches)?;
    let right_batch = concat_batches(&right_batches)?;
    let left_schema = left_batch.schema();
    let right_schema = right_batch.schema();

    let resolved = resolve_join_keys(left_schema.as_ref(), right_schema.as_ref(), keys)?;
    let output = build_output_spec(left_schema.as_ref(), right_schema.as_ref(), &resolved, how)?;

    let left_rows = left_batch.num_rows();
    let right_rows = right_batch.num_rows();

    let mut right_map = HashMap::<JoinKey, Vec<usize>>::new();
    for row in 0..right_rows {
        let key = build_join_key(&right_batch, &resolved.right_indices, row)?;
        right_map.entry(key).or_default().push(row);
    }

    let mut left_indices: Vec<Option<usize>> = Vec::new();
    let mut right_indices: Vec<Option<usize>> = Vec::new();
    let mut matched_right = vec![false; right_rows];

    for row in 0..left_rows {
        let key = build_join_key(&left_batch, &resolved.left_indices, row)?;
        match right_map.get(&key) {
            Some(matches) => match how {
                JoinType::Semi => {
                    left_indices.push(Some(row));
                }
                JoinType::Anti => {}
                _ => {
                    for &r in matches {
                        left_indices.push(Some(row));
                        right_indices.push(Some(r));
                        matched_right[r] = true;
                    }
                }
            },
            None => match how {
                JoinType::Left | JoinType::Full => {
                    left_indices.push(Some(row));
                    right_indices.push(None);
                }
                JoinType::Anti => {
                    left_indices.push(Some(row));
                }
                _ => {}
            },
        }
    }

    if matches!(how, JoinType::Right | JoinType::Full) {
        for r in 0..right_rows {
            if !matched_right[r] {
                left_indices.push(None);
                right_indices.push(Some(r));
            }
        }
    }

    let left_index_array = build_indices(&left_indices)?;
    let right_index_array = build_indices(&right_indices)?;

    let mut arrays = Vec::with_capacity(output.columns.len());
    for col in &output.columns {
        match col {
            OutputColumn::Left(idx) => {
                let array = arrow::compute::take(left_batch.column(*idx), &left_index_array, None)
                    .map_err(|source| DataFrameError::Arrow { source })?;
                arrays.push(array);
            }
            OutputColumn::Right(idx) => {
                let array =
                    arrow::compute::take(right_batch.column(*idx), &right_index_array, None)
                        .map_err(|source| DataFrameError::Arrow { source })?;
                arrays.push(array);
            }
        }
    }

    let schema = Arc::new(Schema::new(output.fields));
    let batch = RecordBatch::try_new(schema, arrays).map_err(|e| {
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

struct ResolvedJoinKeys {
    left_indices: Vec<usize>,
    right_indices: Vec<usize>,
    right_key_indices: HashSet<usize>,
    on_same_names: bool,
}

fn resolve_join_keys(left: &Schema, right: &Schema, keys: &JoinKeys) -> Result<ResolvedJoinKeys> {
    let (left_names, right_names, on_same_names) = match keys {
        JoinKeys::On(cols) => (cols.clone(), cols.clone(), true),
        JoinKeys::LeftRight { left_on, right_on } => (left_on.clone(), right_on.clone(), false),
    };

    if left_names.is_empty() {
        return Err(DataFrameError::invalid_operation(
            "join keys must be non-empty",
        ));
    }

    if left_names.len() != right_names.len() {
        return Err(DataFrameError::invalid_operation(
            "join key lengths do not match",
        ));
    }

    let mut left_indices = Vec::with_capacity(left_names.len());
    let mut right_indices = Vec::with_capacity(right_names.len());
    let mut right_key_indices = HashSet::with_capacity(right_names.len());

    for (l_name, r_name) in left_names.iter().zip(right_names.iter()) {
        let l_idx = left
            .fields()
            .iter()
            .position(|f| f.name() == l_name)
            .ok_or_else(|| DataFrameError::column_not_found(l_name.clone()))?;
        let r_idx = right
            .fields()
            .iter()
            .position(|f| f.name() == r_name)
            .ok_or_else(|| DataFrameError::column_not_found(r_name.clone()))?;

        let l_type = left.fields()[l_idx].data_type();
        let r_type = right.fields()[r_idx].data_type();
        if l_type != r_type {
            return Err(DataFrameError::type_mismatch(
                Some(l_name.clone()),
                l_type.to_string(),
                r_type.to_string(),
            ));
        }

        left_indices.push(l_idx);
        right_indices.push(r_idx);
        right_key_indices.insert(r_idx);
    }

    Ok(ResolvedJoinKeys {
        left_indices,
        right_indices,
        right_key_indices,
        on_same_names,
    })
}

struct OutputSpec {
    fields: Vec<Field>,
    columns: Vec<OutputColumn>,
}

enum OutputColumn {
    Left(usize),
    Right(usize),
}

fn build_output_spec(
    left: &Schema,
    right: &Schema,
    keys: &ResolvedJoinKeys,
    how: &JoinType,
) -> Result<OutputSpec> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    let mut seen = HashSet::<String>::new();

    let left_nullable = matches!(how, JoinType::Right | JoinType::Full);
    for (idx, f) in left.fields().iter().enumerate() {
        let field = Field::new(
            f.name(),
            f.data_type().clone(),
            f.is_nullable() || left_nullable,
        );
        seen.insert(field.name().to_string());
        fields.push(field);
        columns.push(OutputColumn::Left(idx));
    }

    if matches!(how, JoinType::Semi | JoinType::Anti) {
        return Ok(OutputSpec { fields, columns });
    }

    let right_nullable = matches!(how, JoinType::Left | JoinType::Full);
    for (idx, f) in right.fields().iter().enumerate() {
        if keys.on_same_names && keys.right_key_indices.contains(&idx) {
            continue;
        }

        let mut name = f.name().to_string();
        if seen.contains(&name) {
            if keys.right_key_indices.contains(&idx) {
                return Err(DataFrameError::schema_mismatch(format!(
                    "duplicate column name '{name}'",
                )));
            }
            let suffixed = format!("{name}_right");
            if seen.contains(&suffixed) {
                return Err(DataFrameError::schema_mismatch(format!(
                    "duplicate column name '{suffixed}'",
                )));
            }
            name = suffixed;
        }

        seen.insert(name.clone());
        fields.push(Field::new(
            &name,
            f.data_type().clone(),
            f.is_nullable() || right_nullable,
        ));
        columns.push(OutputColumn::Right(idx));
    }

    Ok(OutputSpec { fields, columns })
}

fn build_indices(indices: &[Option<usize>]) -> Result<arrow::array::UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(indices.len());
    for idx in indices {
        match idx {
            Some(value) => {
                let value = u32::try_from(*value).map_err(|_| {
                    DataFrameError::invalid_operation("row index exceeds u32 range")
                })?;
                builder.append_value(value);
            }
            None => {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
}

fn build_join_key(batch: &RecordBatch, indices: &[usize], row: usize) -> Result<JoinKey> {
    let mut values = Vec::with_capacity(indices.len());
    for idx in indices {
        let array = batch.column(*idx).as_ref();
        values.push(key_value_from_array(array, row)?);
    }
    Ok(JoinKey(values))
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
            "unsupported join key type {other:?}",
        ))),
    }
}
