use std::fs::File;

use arrow_array::types::IntervalMonthDayNanoType;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, IntervalMonthDayNanoArray, LargeBinaryArray, LargeListArray, ListArray,
    MapArray, StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDataType, IntervalUnit, TimeUnit};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::catalog::TableMetadata;
use crate::executor::{ExecutorError, Result};
use crate::planner::types::ResolvedType;
use crate::storage::SqlValue;

use super::{BulkReader, CopyField, CopySchema};

/// Parquet リーダー（Arrow 経由でスキーマ抽出とデータ読み込み）。
pub struct ParquetReader {
    schema: CopySchema,
    target_types: Vec<ResolvedType>,
    reader: ParquetRecordBatchReader,
    buffer: Option<Vec<Vec<SqlValue>>>,
}

impl ParquetReader {
    pub fn open(path: &str, table_meta: &TableMetadata, _header: bool) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| ExecutorError::BulkLoad(format!("failed to open parquet: {e}")))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            ExecutorError::BulkLoad(format!("failed to read parquet metadata: {e}"))
        })?;

        let arrow_schema = builder.schema();
        let mut fields = Vec::with_capacity(arrow_schema.fields().len());
        for f in arrow_schema.fields() {
            let ty = map_arrow_type(f.data_type())?;
            fields.push(CopyField {
                name: Some(f.name().clone()),
                data_type: Some(ty),
            });
        }

        let reader = builder
            .with_batch_size(1024)
            .build()
            .map_err(|e| ExecutorError::BulkLoad(format!("failed to build parquet reader: {e}")))?;
        // TODO: バッチサイズを open 引数で受け取れるようにし、呼び出し側で柔軟に制御できるようにする。

        let target_types: Vec<ResolvedType> = table_meta
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        Ok(Self {
            schema: CopySchema { fields },
            target_types,
            reader,
            buffer: None,
        })
    }
}

impl BulkReader for ParquetReader {
    fn schema(&self) -> &CopySchema {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<SqlValue>>>> {
        let max_rows = max_rows.max(1);

        if let Some(mut buffered) = self.buffer.take() {
            if buffered.len() > max_rows {
                let rest = buffered.split_off(max_rows);
                self.buffer = Some(rest);
            }
            return Ok(Some(buffered));
        }

        let maybe_batch = self.reader.next();
        let batch = match maybe_batch {
            Some(b) => b.map_err(|e| {
                ExecutorError::BulkLoad(format!("failed to read parquet batch: {e}"))
            })?,
            None => return Ok(None),
        };

        let mut rows: Vec<Vec<SqlValue>> = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(self.schema.fields.len());
            for col_idx in 0..self.schema.fields.len() {
                let value = arrow_value_to_sql(
                    batch.column(col_idx).as_ref(),
                    batch.schema().field(col_idx).data_type(),
                    self.target_types
                        .get(col_idx)
                        .ok_or_else(|| ExecutorError::BulkLoad("missing target type".into()))?,
                    row_idx,
                )?;
                row.push(value);
            }
            rows.push(row);
        }

        if rows.len() > max_rows {
            let rest = rows.split_off(max_rows);
            self.buffer = Some(rest);
        }

        Ok(Some(rows))
    }
}

fn map_arrow_type(dt: &ArrowDataType) -> Result<ResolvedType> {
    match dt {
        ArrowDataType::Int32 => Ok(ResolvedType::Integer),
        ArrowDataType::Int64 => Ok(ResolvedType::BigInt),
        ArrowDataType::Float32 => Ok(ResolvedType::Float),
        ArrowDataType::Float64 => Ok(ResolvedType::Double),
        ArrowDataType::Boolean => Ok(ResolvedType::Boolean),
        ArrowDataType::Utf8 => Ok(ResolvedType::Text),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(ResolvedType::Blob),
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            Ok(ResolvedType::Timestamp)
        }
        ArrowDataType::Date32 => Ok(ResolvedType::Date),
        ArrowDataType::Time64(TimeUnit::Microsecond) => Ok(ResolvedType::Time),
        ArrowDataType::Interval(IntervalUnit::MonthDayNano) => Ok(ResolvedType::Interval),
        ArrowDataType::Decimal128(precision, scale) if *scale >= 0 => Ok(ResolvedType::Decimal {
            precision: *precision,
            scale: *scale as u8,
        }),
        other => Err(ExecutorError::BulkLoad(format!(
            "unsupported parquet/arrow type: {other:?}"
        ))),
    }
}

fn arrow_value_to_sql(
    array: &dyn Array,
    dt: &ArrowDataType,
    expected: &ResolvedType,
    row_idx: usize,
) -> Result<SqlValue> {
    if array.is_null(row_idx) {
        return Ok(SqlValue::Null);
    }

    match (dt, expected) {
        (ArrowDataType::Int32, ResolvedType::Integer) => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(SqlValue::Integer(arr.value(row_idx)))
        }
        (ArrowDataType::Int32, ResolvedType::BigInt) => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(SqlValue::BigInt(arr.value(row_idx) as i64))
        }
        (ArrowDataType::Int32, ResolvedType::Float) => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(SqlValue::Float(arr.value(row_idx) as f32))
        }
        (ArrowDataType::Int32, ResolvedType::Double) => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(SqlValue::Double(arr.value(row_idx) as f64))
        }
        (ArrowDataType::Int64, ResolvedType::BigInt) => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(SqlValue::BigInt(arr.value(row_idx)))
        }
        (ArrowDataType::Int64, ResolvedType::Double) => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(SqlValue::Double(arr.value(row_idx) as f64))
        }
        (ArrowDataType::Float32, ResolvedType::Float) => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(SqlValue::Float(arr.value(row_idx)))
        }
        (ArrowDataType::Float32, ResolvedType::Double) => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(SqlValue::Double(arr.value(row_idx) as f64))
        }
        (ArrowDataType::Float64, ResolvedType::Double) => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(SqlValue::Double(arr.value(row_idx)))
        }
        (ArrowDataType::Boolean, ResolvedType::Boolean) => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(SqlValue::Boolean(arr.value(row_idx)))
        }
        (ArrowDataType::Utf8, ResolvedType::Text) => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(SqlValue::Text(arr.value(row_idx).to_string()))
        }
        (ArrowDataType::Utf8, ResolvedType::Json) => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            crate::storage::JsonValue::parse(arr.value(row_idx))
                .map(SqlValue::Json)
                .map_err(|error| ExecutorError::BulkLoad(format!("invalid JSON: {error}")))
        }
        (
            ArrowDataType::Utf8,
            expected
            @ (ResolvedType::Array(_) | ResolvedType::Map { .. } | ResolvedType::Struct(_)),
        ) => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            crate::executor::evaluator::nested::parse_typed_json(arr.value(row_idx), expected)
        }
        (ArrowDataType::List(_), ResolvedType::Array(element)) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = arr.value(row_idx);
            (0..values.len())
                .map(|index| {
                    arrow_value_to_sql(values.as_ref(), values.data_type(), element, index)
                })
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Array)
        }
        (ArrowDataType::LargeList(_), ResolvedType::Array(element)) => {
            let arr = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = arr.value(row_idx);
            (0..values.len())
                .map(|index| {
                    arrow_value_to_sql(values.as_ref(), values.data_type(), element, index)
                })
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Array)
        }
        (ArrowDataType::Map(_, _), ResolvedType::Map { key, value }) => {
            let arr = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = arr.value(row_idx);
            let keys = entries.column(0);
            let values = entries.column(1);
            (0..entries.len())
                .map(|index| {
                    Ok((
                        arrow_value_to_sql(keys.as_ref(), keys.data_type(), key, index)?,
                        arrow_value_to_sql(values.as_ref(), values.data_type(), value, index)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Map)
        }
        (ArrowDataType::Struct(arrow_fields), ResolvedType::Struct(expected_fields))
            if arrow_fields.len() == expected_fields.len() =>
        {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            expected_fields
                .iter()
                .enumerate()
                .map(|(index, (name, data_type))| {
                    if arrow_fields[index].name() != name {
                        return Err(ExecutorError::BulkLoad(format!(
                            "struct field mismatch: expected {name}, found {}",
                            arrow_fields[index].name()
                        )));
                    }
                    Ok((
                        name.clone(),
                        arrow_value_to_sql(
                            arr.column(index).as_ref(),
                            arrow_fields[index].data_type(),
                            data_type,
                            row_idx,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Struct)
        }
        (ArrowDataType::Binary, ResolvedType::Blob) => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(SqlValue::Blob(arr.value(row_idx).to_vec()))
        }
        (ArrowDataType::LargeBinary, ResolvedType::Blob) => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(SqlValue::Blob(arr.value(row_idx).to_vec()))
        }
        (
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _),
            ResolvedType::Timestamp,
        ) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(SqlValue::Timestamp(arr.value(row_idx)))
        }
        (ArrowDataType::Date32, ResolvedType::Date) => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(SqlValue::Date(arr.value(row_idx)))
        }
        (ArrowDataType::Time64(TimeUnit::Microsecond), ResolvedType::Time) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            Ok(SqlValue::Time(arr.value(row_idx)))
        }
        (ArrowDataType::Interval(IntervalUnit::MonthDayNano), ResolvedType::Interval) => {
            let arr = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap();
            let value = arr.value(row_idx);
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(value);
            if nanos % 1_000 != 0 {
                return Err(ExecutorError::BulkLoad(
                    "interval nanoseconds cannot be represented as microseconds".into(),
                ));
            }
            Ok(SqlValue::Interval {
                months,
                days,
                micros: nanos / 1_000,
            })
        }
        (ArrowDataType::Decimal128(_, arrow_scale), ResolvedType::Decimal { precision, scale })
            if *arrow_scale >= 0 =>
        {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let value = crate::storage::DecimalValue::new(arr.value(row_idx), *arrow_scale as u8)
                .rescale(*scale)
                .ok_or_else(|| ExecutorError::BulkLoad("decimal rescale overflow".into()))?;
            if !value.fits_precision(*precision) {
                return Err(ExecutorError::BulkLoad("decimal precision overflow".into()));
            }
            Ok(SqlValue::Decimal(value))
        }
        _ => Err(ExecutorError::BulkLoad(format!(
            "parquet field type {:?} does not match expected {:?}",
            dt, expected
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DecimalValue;

    #[test]
    fn arrow_list_maps_to_native_array_with_null_elements() {
        let mut builder =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_null();
        builder.values().append_value(2);
        builder.append(true);
        let array = builder.finish();

        assert_eq!(
            arrow_value_to_sql(
                &array,
                array.data_type(),
                &ResolvedType::Array(Box::new(ResolvedType::Integer)),
                0,
            )
            .unwrap(),
            SqlValue::Array(vec![
                SqlValue::Integer(1),
                SqlValue::Null,
                SqlValue::Integer(2),
            ])
        );
    }

    #[test]
    fn decimal128_maps_to_exact_sql_decimal() {
        let array = Decimal128Array::from(vec![Some(12345)])
            .with_precision_and_scale(10, 3)
            .unwrap();
        let ty = ArrowDataType::Decimal128(10, 3);
        assert_eq!(
            map_arrow_type(&ty).unwrap(),
            ResolvedType::Decimal {
                precision: 10,
                scale: 3,
            }
        );
        assert_eq!(
            arrow_value_to_sql(
                &array,
                &ty,
                &ResolvedType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                0,
            )
            .unwrap(),
            SqlValue::Decimal(DecimalValue::new(1235, 2))
        );
    }
}
