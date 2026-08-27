use std::sync::Arc;

use arrow::array::types::IntervalMonthDayNanoType;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, IntervalMonthDayNanoArray, NullArray, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use alopex_dataframe::{DataFrame, DataFrameError};
use alopex_sql::{ColumnInfo, ExecutionResult, QueryResult, ResolvedType, SqlValue};

use crate::{Database, Result, SqlResult, Transaction};

type DfResult<T> = std::result::Result<T, DataFrameError>;

impl Database {
    /// Execute SQL and return a DataFrame for query results.
    pub fn query_df(&self, sql: &str) -> Result<DataFrame> {
        let result = self.execute_sql(sql)?;
        let df = sql_result_to_dataframe(result)?;
        Ok(df)
    }
}

impl<'a> Transaction<'a> {
    /// Execute SQL within the transaction and return a DataFrame for query results.
    pub fn query_df(&mut self, sql: &str) -> Result<DataFrame> {
        let result = self.execute_sql(sql)?;
        let df = sql_result_to_dataframe(result)?;
        Ok(df)
    }
}

fn sql_result_to_dataframe(result: SqlResult) -> DfResult<DataFrame> {
    match result {
        ExecutionResult::Query(query) => query_result_to_dataframe(query),
        ExecutionResult::Success | ExecutionResult::RowsAffected(_) => Err(
            DataFrameError::invalid_operation("query_df requires a SELECT query that returns rows"),
        ),
    }
}

fn query_result_to_dataframe(query: QueryResult) -> DfResult<DataFrame> {
    let row_count = query.rows.len();
    let mut fields = Vec::with_capacity(query.columns.len());
    let mut builders = Vec::with_capacity(query.columns.len());

    for ColumnInfo { name, data_type } in query.columns {
        let arrow_type = arrow_type_for(&data_type)?;
        fields.push(Field::new(&name, arrow_type, true));
        builders.push(ColumnBuilder::new(name, data_type, row_count)?);
    }

    for row in query.rows {
        if row.len() != builders.len() {
            return Err(DataFrameError::schema_mismatch(format!(
                "row has {} columns, expected {}",
                row.len(),
                builders.len()
            )));
        }

        for (value, builder) in row.into_iter().zip(builders.iter_mut()) {
            builder.push(value)?;
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let arrays = builders
        .into_iter()
        .map(ColumnBuilder::finish)
        .collect::<DfResult<Vec<_>>>()?;
    let batch = RecordBatch::try_new(schema, arrays).map_err(|e| {
        DataFrameError::schema_mismatch(format!("failed to build RecordBatch: {e}"))
    })?;

    DataFrame::from_batches(vec![batch])
}

fn arrow_type_for(ty: &ResolvedType) -> DfResult<DataType> {
    match ty {
        ResolvedType::Integer => Ok(DataType::Int32),
        ResolvedType::BigInt => Ok(DataType::Int64),
        ResolvedType::Float => Ok(DataType::Float32),
        ResolvedType::Double => Ok(DataType::Float64),
        ResolvedType::Text => Ok(DataType::Utf8),
        ResolvedType::Json => Ok(DataType::Utf8),
        ResolvedType::Blob => Ok(DataType::Binary),
        ResolvedType::Boolean => Ok(DataType::Boolean),
        ResolvedType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        ResolvedType::Date => Ok(DataType::Date32),
        ResolvedType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        ResolvedType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        ResolvedType::Decimal { precision, scale } => {
            Ok(DataType::Decimal128(*precision, *scale as i8))
        }
        ResolvedType::Null => Ok(DataType::Null),
        ResolvedType::Vector { .. } => Err(DataFrameError::invalid_operation(
            "vector columns are not supported for DataFrame conversion",
        )),
    }
}

struct ColumnBuilder {
    name: String,
    expected: ResolvedType,
    kind: ColumnBuilderKind,
}

enum ColumnBuilderKind {
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
    Binary(Vec<Option<Vec<u8>>>),
    Boolean(Vec<Option<bool>>),
    Timestamp(Vec<Option<i64>>),
    Date(Vec<Option<i32>>),
    Time(Vec<Option<i64>>),
    Interval(Vec<Option<<IntervalMonthDayNanoType as arrow::array::ArrowPrimitiveType>::Native>>),
    Decimal(Vec<Option<i128>>, u8, i8),
    Null(usize),
}

impl ColumnBuilder {
    fn new(name: String, expected: ResolvedType, row_count: usize) -> DfResult<Self> {
        let kind = match expected {
            ResolvedType::Integer => ColumnBuilderKind::Int32(Vec::with_capacity(row_count)),
            ResolvedType::BigInt => ColumnBuilderKind::Int64(Vec::with_capacity(row_count)),
            ResolvedType::Float => ColumnBuilderKind::Float32(Vec::with_capacity(row_count)),
            ResolvedType::Double => ColumnBuilderKind::Float64(Vec::with_capacity(row_count)),
            ResolvedType::Text => ColumnBuilderKind::Utf8(Vec::with_capacity(row_count)),
            ResolvedType::Json => ColumnBuilderKind::Utf8(Vec::with_capacity(row_count)),
            ResolvedType::Blob => ColumnBuilderKind::Binary(Vec::with_capacity(row_count)),
            ResolvedType::Boolean => ColumnBuilderKind::Boolean(Vec::with_capacity(row_count)),
            ResolvedType::Timestamp => ColumnBuilderKind::Timestamp(Vec::with_capacity(row_count)),
            ResolvedType::Date => ColumnBuilderKind::Date(Vec::with_capacity(row_count)),
            ResolvedType::Time => ColumnBuilderKind::Time(Vec::with_capacity(row_count)),
            ResolvedType::Interval => ColumnBuilderKind::Interval(Vec::with_capacity(row_count)),
            ResolvedType::Decimal { precision, scale } => {
                ColumnBuilderKind::Decimal(Vec::with_capacity(row_count), precision, scale as i8)
            }
            ResolvedType::Null => ColumnBuilderKind::Null(0),
            ResolvedType::Vector { .. } => {
                return Err(DataFrameError::invalid_operation(
                    "vector columns are not supported for DataFrame conversion",
                ))
            }
        };

        Ok(Self {
            name,
            expected,
            kind,
        })
    }

    fn push(&mut self, value: SqlValue) -> DfResult<()> {
        match (&mut self.kind, value) {
            (ColumnBuilderKind::Int32(values), SqlValue::Integer(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Int64(values), SqlValue::BigInt(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Float32(values), SqlValue::Float(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Float64(values), SqlValue::Double(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Utf8(values), SqlValue::Text(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Utf8(values), SqlValue::Json(v)) => {
                values.push(Some(v.to_string()));
                Ok(())
            }
            (ColumnBuilderKind::Binary(values), SqlValue::Blob(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Boolean(values), SqlValue::Boolean(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Timestamp(values), SqlValue::Timestamp(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Date(values), SqlValue::Date(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (ColumnBuilderKind::Time(values), SqlValue::Time(v)) => {
                values.push(Some(v));
                Ok(())
            }
            (
                ColumnBuilderKind::Interval(values),
                SqlValue::Interval {
                    months,
                    days,
                    micros,
                },
            ) => {
                let nanos = micros.checked_mul(1_000).ok_or_else(|| {
                    DataFrameError::invalid_operation("interval nanoseconds overflow Arrow i64")
                })?;
                values.push(Some(IntervalMonthDayNanoType::make_value(
                    months, days, nanos,
                )));
                Ok(())
            }
            (ColumnBuilderKind::Decimal(values, _, _), SqlValue::Decimal(v)) => {
                values.push(Some(v.coefficient));
                Ok(())
            }
            (ColumnBuilderKind::Int32(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Int64(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Float32(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Float64(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Utf8(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Binary(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Boolean(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Timestamp(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Date(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Time(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Interval(values), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Decimal(values, _, _), SqlValue::Null) => {
                values.push(None);
                Ok(())
            }
            (ColumnBuilderKind::Null(count), SqlValue::Null) => {
                *count += 1;
                Ok(())
            }
            (_, other) => Err(DataFrameError::type_mismatch(
                Some(self.name.clone()),
                self.expected.to_string(),
                other.type_name().to_string(),
            )),
        }
    }

    fn finish(self) -> DfResult<ArrayRef> {
        let array: ArrayRef = match self.kind {
            ColumnBuilderKind::Int32(values) => Arc::new(Int32Array::from(values)),
            ColumnBuilderKind::Int64(values) => Arc::new(Int64Array::from(values)),
            ColumnBuilderKind::Float32(values) => Arc::new(Float32Array::from(values)),
            ColumnBuilderKind::Float64(values) => Arc::new(Float64Array::from(values)),
            ColumnBuilderKind::Utf8(values) => Arc::new(StringArray::from(values)),
            ColumnBuilderKind::Binary(values) => {
                let slices: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
                Arc::new(BinaryArray::from(slices))
            }
            ColumnBuilderKind::Boolean(values) => Arc::new(BooleanArray::from(values)),
            ColumnBuilderKind::Timestamp(values) => {
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            ColumnBuilderKind::Date(values) => Arc::new(Date32Array::from(values)),
            ColumnBuilderKind::Time(values) => Arc::new(Time64MicrosecondArray::from(values)),
            ColumnBuilderKind::Interval(values) => {
                Arc::new(IntervalMonthDayNanoArray::from(values))
            }
            ColumnBuilderKind::Decimal(values, precision, scale) => Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|error| {
                        DataFrameError::schema_mismatch(format!(
                            "invalid DECIMAL({precision},{scale}) Arrow type: {error}"
                        ))
                    })?,
            ),
            ColumnBuilderKind::Null(len) => Arc::new(NullArray::new(len)),
        };

        Ok(array)
    }
}
