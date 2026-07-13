use std::sync::Arc;

use alopex_core::dataframe as core_df;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, NullArray,
    StringArray, StringBuilder, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::expr::{
    DatetimeFunction, Expr as E, ExprFunction, ListFunction, Scalar, StringFunction,
};
use crate::{DataFrameError, Expr, Result};

/// Evaluates `Expr` values over Arrow `RecordBatch` inputs.
pub struct ExprEval;

impl ExprEval {
    /// Evaluate `expr` for every row in `batch` and return the resulting Arrow array.
    pub fn evaluate(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
        eval_expr(expr, batch)
    }
}

fn eval_expr(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expr {
        E::Column(name) => {
            let idx = batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
            Ok(batch.column(idx).clone())
        }
        E::Literal(s) => scalar_to_array(s, batch.num_rows()),
        E::Alias { expr, .. } => eval_expr(expr, batch),
        E::Wildcard => Err(DataFrameError::invalid_operation(
            "wildcard cannot be evaluated as a standalone expression",
        )),
        E::UnaryOp { op, expr } => {
            let v = eval_expr(expr, batch)?;
            match op {
                crate::expr::UnaryOperator::Not => {
                    if v.data_type() != &DataType::Boolean {
                        return Err(DataFrameError::type_mismatch(
                            None::<String>,
                            DataType::Boolean.to_string(),
                            v.data_type().to_string(),
                        ));
                    }
                    let b = v.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                        DataFrameError::type_mismatch(
                            None::<String>,
                            "BooleanArray".to_string(),
                            format!("{:?}", v.data_type()),
                        )
                    })?;
                    Ok(Arc::new(
                        arrow::compute::not(b)
                            .map_err(|source| DataFrameError::Arrow { source })?,
                    ))
                }
            }
        }
        E::BinaryOp { left, op, right } => {
            let l = eval_expr(left, batch)?;
            let r = eval_expr(right, batch)?;
            eval_binary(op, &l, &r)
        }
        E::Function { input, function } => {
            let input = eval_expr(input, batch)?;
            eval_function(&input, function)
        }
        E::Agg { .. } => Err(DataFrameError::invalid_operation(
            "aggregation expressions must be evaluated by aggregate operator",
        )),
    }
}

fn scalar_to_array(s: &Scalar, len: usize) -> Result<ArrayRef> {
    match s {
        Scalar::Null => Ok(Arc::new(NullArray::new(len))),
        Scalar::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![Some(*v); len]))),
        Scalar::Int64(v) => Ok(Arc::new(Int64Array::from(vec![Some(*v); len]))),
        Scalar::Float64(v) => Ok(Arc::new(Float64Array::from(vec![Some(*v); len]))),
        Scalar::Utf8(v) => Ok(Arc::new(StringArray::from(vec![Some(v.as_str()); len]))),
    }
}

fn eval_binary(op: &crate::expr::Operator, lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    use crate::expr::Operator;

    let l = lhs.as_ref();
    let r = rhs.as_ref();

    match op {
        Operator::Add => arrow::compute::kernels::numeric::add(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Sub => arrow::compute::kernels::numeric::sub(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Mul => arrow::compute::kernels::numeric::mul(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Div => arrow::compute::kernels::numeric::div(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Eq => Ok(Arc::new(
            arrow::compute::kernels::cmp::eq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Neq => Ok(Arc::new(
            arrow::compute::kernels::cmp::neq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Gt => Ok(Arc::new(
            arrow::compute::kernels::cmp::gt(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Lt => Ok(Arc::new(
            arrow::compute::kernels::cmp::lt(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Ge => Ok(Arc::new(
            arrow::compute::kernels::cmp::gt_eq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Le => Ok(Arc::new(
            arrow::compute::kernels::cmp::lt_eq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::And => {
            let l = lhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    lhs.data_type().to_string(),
                )
            })?;
            let r = rhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    rhs.data_type().to_string(),
                )
            })?;
            Ok(Arc::new(
                arrow::compute::kernels::boolean::and_kleene(l, r)
                    .map_err(|source| DataFrameError::Arrow { source })?,
            ))
        }
        Operator::Or => {
            let l = lhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    lhs.data_type().to_string(),
                )
            })?;
            let r = rhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    rhs.data_type().to_string(),
                )
            })?;
            Ok(Arc::new(
                arrow::compute::kernels::boolean::or_kleene(l, r)
                    .map_err(|source| DataFrameError::Arrow { source })?,
            ))
        }
    }
}

fn eval_function(input: &ArrayRef, function: &ExprFunction) -> Result<ArrayRef> {
    match function {
        ExprFunction::String(function) => eval_string_function(input, function),
        ExprFunction::Datetime(function) => eval_datetime_function(input, function),
        ExprFunction::List(function) => eval_list_function(input, function),
    }
}

fn eval_string_function(input: &ArrayRef, function: &StringFunction) -> Result<ArrayRef> {
    let values = utf8_to_core(input)?;
    match function {
        StringFunction::ToLowercase => Ok(utf8_from_core(core_df::str_to_lowercase(&values))),
        StringFunction::ToUppercase => Ok(utf8_from_core(core_df::str_to_uppercase(&values))),
        StringFunction::Contains { pattern } => Ok(bool_from_core(core_to_df_result(
            core_df::str_contains(&values, pattern),
        )?)),
        StringFunction::Replace {
            pattern,
            replacement,
        } => Ok(utf8_from_core(core_to_df_result(core_df::str_replace(
            &values,
            pattern,
            replacement,
        ))?)),
        StringFunction::StripChars { chars } => Ok(utf8_from_core(core_df::str_strip_chars(
            &values,
            chars.as_deref(),
        ))),
        StringFunction::Split { separator } => {
            Ok(list_utf8_from_core(core_df::str_split(&values, separator)))
        }
        StringFunction::LenChars => Ok(uint_from_core(core_df::str_len_chars(&values))),
        StringFunction::Extract {
            pattern,
            capture_group,
        } => Ok(utf8_from_core(core_to_df_result(core_df::str_extract(
            &values,
            pattern,
            *capture_group,
        ))?)),
    }
}

fn eval_datetime_function(input: &ArrayRef, function: &DatetimeFunction) -> Result<ArrayRef> {
    let values = timestamp_micros_to_core(input)?;
    match function {
        DatetimeFunction::Year => Ok(int32_from_core(core_df::dt_year(&values))),
        DatetimeFunction::Month => Ok(uint_from_core(
            core_df::dt_month(&values)
                .into_iter()
                .map(|v| v.map(|v| v as usize))
                .collect(),
        )),
        DatetimeFunction::Day => Ok(uint_from_core(
            core_df::dt_day(&values)
                .into_iter()
                .map(|v| v.map(|v| v as usize))
                .collect(),
        )),
        DatetimeFunction::Weekday => Ok(uint_from_core(
            core_df::dt_weekday(&values)
                .into_iter()
                .map(|v| v.map(|v| v as usize))
                .collect(),
        )),
        DatetimeFunction::ToString => Ok(utf8_from_core(core_df::dt_to_string(&values))),
        DatetimeFunction::ConvertTimeZone {
            from_offset,
            to_offset,
        } => Ok(timestamp_micros_from_core(core_to_df_result(
            core_df::dt_convert_time_zone(&values, from_offset, to_offset),
        )?)),
    }
}

fn eval_list_function(input: &ArrayRef, function: &ListFunction) -> Result<ArrayRef> {
    let values = list_utf8_to_core(input)?;
    match function {
        ListFunction::Join {
            separator,
            null_value,
        } => Ok(utf8_from_core(core_df::list_join(
            &values,
            separator,
            null_value.as_deref(),
        ))),
        ListFunction::Len => Ok(uint_from_core(core_df::list_len(&values))),
        ListFunction::Contains { value } => {
            Ok(bool_from_core(core_df::list_contains(&values, value)))
        }
    }
}

fn utf8_to_core(input: &ArrayRef) -> Result<Vec<Option<String>>> {
    let array = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFrameError::type_mismatch(
                None::<String>,
                DataType::Utf8.to_string(),
                input.data_type().to_string(),
            )
        })?;
    Ok((0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx).to_string())
            }
        })
        .collect())
}

fn timestamp_micros_to_core(input: &ArrayRef) -> Result<Vec<Option<i64>>> {
    if !matches!(
        input.data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, _)
    ) {
        return Err(DataFrameError::type_mismatch(
            None::<String>,
            "Timestamp(Microsecond, _)".to_string(),
            input.data_type().to_string(),
        ));
    }
    let array = input
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            DataFrameError::type_mismatch(
                None::<String>,
                "TimestampMicrosecondArray".to_string(),
                input.data_type().to_string(),
            )
        })?;
    Ok((0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect())
}

fn list_utf8_to_core(input: &ArrayRef) -> Result<Vec<Option<Vec<Option<String>>>>> {
    let array = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFrameError::type_mismatch(
            None::<String>,
            "List<Utf8>".to_string(),
            input.data_type().to_string(),
        )
    })?;
    let DataType::List(field) = input.data_type() else {
        return Err(DataFrameError::type_mismatch(
            None::<String>,
            "List<Utf8>".to_string(),
            input.data_type().to_string(),
        ));
    };
    if field.data_type() != &DataType::Utf8 {
        return Err(DataFrameError::type_mismatch(
            None::<String>,
            "List<Utf8>".to_string(),
            input.data_type().to_string(),
        ));
    }

    let mut out = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            out.push(None);
            continue;
        }
        let values = array.value(row);
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    "StringArray".to_string(),
                    values.data_type().to_string(),
                )
            })?;
        out.push(Some(
            (0..values.len())
                .map(|idx| {
                    if values.is_null(idx) {
                        None
                    } else {
                        Some(values.value(idx).to_string())
                    }
                })
                .collect(),
        ));
    }
    Ok(out)
}

fn utf8_from_core(values: Vec<Option<String>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

fn bool_from_core(values: Vec<Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from(values))
}

fn int32_from_core(values: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from(values))
}

fn uint_from_core(values: Vec<Option<usize>>) -> ArrayRef {
    Arc::new(UInt64Array::from(
        values
            .into_iter()
            .map(|value| value.map(|value| value as u64))
            .collect::<Vec<_>>(),
    ))
}

fn timestamp_micros_from_core(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(values))
}

fn list_utf8_from_core(values: Vec<Option<Vec<Option<String>>>>) -> ArrayRef {
    let mut builder = arrow::array::ListBuilder::new(StringBuilder::new());
    for list in values {
        match list {
            Some(items) => {
                for item in items {
                    match item {
                        Some(value) => builder.values().append_value(value),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

fn core_to_df_result<T>(result: alopex_core::Result<T>) -> Result<T> {
    result.map_err(|err| DataFrameError::invalid_operation(err.to_string()))
}
