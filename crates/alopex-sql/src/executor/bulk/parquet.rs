use std::fs::File;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::DataType as ArrowDataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::catalog::TableMetadata;
use crate::executor::{ExecutorError, Result};
use crate::planner::types::ResolvedType;
use crate::storage::SqlValue;

use super::{BulkReader, CopyField, CopySchema};

/// Parquet リーダー（Arrow 経由でスキーマ抽出とデータ読み込み）。
pub struct ParquetReader {
    schema: CopySchema,
    target_types: Vec<ResolvedType>,
    rows: Vec<Vec<SqlValue>>,
    position: usize,
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

        let mut reader = builder
            .with_batch_size(1024)
            .build()
            .map_err(|e| ExecutorError::BulkLoad(format!("failed to build parquet reader: {e}")))?;

        let mut rows: Vec<Vec<SqlValue>> = Vec::new();
        let target_types: Vec<ResolvedType> = table_meta
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();
        while let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| {
                ExecutorError::BulkLoad(format!("failed to read parquet batch: {e}"))
            })?;
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(fields.len());
                for (col_idx, field) in fields.iter().enumerate() {
                    let value = arrow_value_to_sql(
                        batch.column(col_idx).as_ref(),
                        batch.schema().field(col_idx).data_type(),
                        target_types
                            .get(col_idx)
                            .ok_or_else(|| ExecutorError::BulkLoad("missing target type".into()))?,
                        row_idx,
                    )?;
                    row.push(value);
                }
                rows.push(row);
            }
        }

        Ok(Self {
            schema: CopySchema { fields },
            target_types,
            rows,
            position: 0,
        })
    }
}

impl BulkReader for ParquetReader {
    fn schema(&self) -> &CopySchema {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<SqlValue>>>> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }
        let end = (self.position + max_rows).min(self.rows.len());
        let batch = self.rows[self.position..end].to_vec();
        self.position = end;
        Ok(Some(batch))
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
        _ => Err(ExecutorError::BulkLoad(format!(
            "parquet field type {:?} does not match expected {:?}",
            dt, expected
        ))),
    }
}
