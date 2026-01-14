use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::expr::{Expr as E, Scalar};
use crate::lazy::ProjectionKind;
use crate::physical::expr_eval::ExprEval;
use crate::physical::plan::ScanSource;
use crate::{DataFrame, DataFrameError, Expr, Result};

/// Scan operator that reads from an in-memory DataFrame, CSV, or Parquet source.
pub fn scan_source(source: &ScanSource) -> Result<Vec<RecordBatch>> {
    match source {
        ScanSource::DataFrame(df) => Ok(scan_dataframe(df)),
        ScanSource::Csv {
            path,
            predicate,
            projection,
        } => {
            let mut opts = crate::io::CsvReadOptions::default();
            if let Some(cols) = projection {
                opts = opts.with_projection(cols.clone());
            }
            if let Some(pred) = predicate {
                opts = opts.with_predicate(pred.clone());
            }
            let df = crate::io::read_csv_with_options(path, &opts)?;
            Ok(df.to_arrow())
        }
        ScanSource::Parquet {
            path,
            predicate,
            projection,
        } => {
            let mut opts = crate::io::ParquetReadOptions::default();
            if let Some(cols) = projection {
                opts = opts.with_columns(cols.clone());
            }
            if let Some(pred) = predicate {
                opts = opts.with_predicate(pred.clone());
            }
            let df = crate::io::read_parquet_with_options(path, &opts)?;
            Ok(df.to_arrow())
        }
    }
}

/// Scan an in-memory `DataFrame` into Arrow record batches.
pub fn scan_dataframe(df: &DataFrame) -> Vec<RecordBatch> {
    df.to_arrow()
}

/// Filter record batches using the provided boolean predicate expression.
pub fn filter_batches(batches: Vec<RecordBatch>, predicate: &Expr) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let pred = ExprEval::evaluate(predicate, &batch)?;
        if pred.data_type() != &DataType::Boolean {
            return Err(DataFrameError::type_mismatch(
                None::<String>,
                DataType::Boolean.to_string(),
                pred.data_type().to_string(),
            ));
        }
        let pred = pred
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    "BooleanArray".to_string(),
                    format!("{:?}", pred.data_type()),
                )
            })?;
        out.push(
            arrow::compute::filter_record_batch(&batch, pred)
                .map_err(|source| DataFrameError::Arrow { source })?,
        );
    }
    Ok(out)
}

/// Apply a projection (`select` or `with_columns`) to record batches.
pub fn project_batches(
    batches: Vec<RecordBatch>,
    exprs: &[Expr],
    kind: ProjectionKind,
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        match kind {
            ProjectionKind::Select => out.push(project_one_batch(&batch, exprs)?),
            ProjectionKind::WithColumns => out.push(with_columns_one_batch(&batch, exprs)?),
        }
    }
    Ok(out)
}

fn project_one_batch(batch: &RecordBatch, exprs: &[Expr]) -> Result<RecordBatch> {
    let expanded = expand_projection_exprs(exprs, batch.schema().as_ref())?;
    let mut arrays = Vec::with_capacity(expanded.len());
    let mut fields = Vec::with_capacity(expanded.len());

    for (name, expr) in expanded {
        let a = ExprEval::evaluate(&expr, batch)?;
        fields.push(Field::new(name, a.data_type().clone(), true));
        arrays.push(a);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| DataFrameError::schema_mismatch(format!("failed to build RecordBatch: {e}")))
}

fn with_columns_one_batch(batch: &RecordBatch, exprs: &[Expr]) -> Result<RecordBatch> {
    let mut arrays = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();

    for expr in exprs {
        let (name, inner) = match expr {
            E::Alias { expr, name } => (name.clone(), expr.as_ref().clone()),
            E::Column(name) => (name.clone(), expr.clone()),
            _ => {
                return Err(DataFrameError::invalid_operation(
                    "with_columns requires alias for non-column expressions",
                ))
            }
        };

        let a = ExprEval::evaluate(&inner, batch)?;
        if let Some(idx) = fields.iter().position(|f| f.name() == &name) {
            fields[idx] = Arc::new(Field::new(&name, a.data_type().clone(), true));
            arrays[idx] = a;
        } else {
            fields.push(Arc::new(Field::new(&name, a.data_type().clone(), true)));
            arrays.push(a);
        }
    }

    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| DataFrameError::schema_mismatch(format!("failed to build RecordBatch: {e}")))
}

fn expand_projection_exprs(exprs: &[Expr], schema: &Schema) -> Result<Vec<(String, Expr)>> {
    let mut out = Vec::new();
    for expr in exprs {
        match expr {
            E::Wildcard => {
                for f in schema.fields() {
                    out.push((f.name().to_string(), E::Column(f.name().to_string())));
                }
            }
            E::Alias { name, .. } => out.push((name.clone(), expr.clone())),
            E::Column(name) => out.push((name.clone(), expr.clone())),
            other => out.push((format!("{other:?}"), other.clone())),
        }
    }
    Ok(out)
}

/// Aggregate record batches using group keys and aggregation expressions.
pub fn aggregate_batches(
    input: Vec<RecordBatch>,
    group_by: &[Expr],
    aggs: &[Expr],
) -> Result<Vec<RecordBatch>> {
    if group_by.is_empty() {
        return Err(DataFrameError::invalid_operation(
            "group_by expressions must be non-empty",
        ));
    }
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let schema = input[0].schema();
    let group_specs = group_by
        .iter()
        .map(|e| resolve_column_expr(e, schema.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let agg_specs = aggs
        .iter()
        .map(|e| resolve_agg_expr(e, schema.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    let mut index = HashMap::<GroupKey, usize>::new();
    let mut groups: Vec<GroupAccum> = Vec::new();

    for batch in input {
        for row in 0..batch.num_rows() {
            let key_items = group_specs
                .iter()
                .map(|s| key_value_from_array(batch.column(s.index).as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let key = GroupKey(key_items);

            let entry = index.get(&key).copied();
            let group_idx = match entry {
                Some(i) => i,
                None => {
                    let i = groups.len();
                    groups.push(GroupAccum::new(&key, &agg_specs)?);
                    index.insert(key, i);
                    i
                }
            };

            groups[group_idx].update(&batch, row, &agg_specs)?;
        }
    }

    let batch = build_grouped_batch(groups, &group_specs, &agg_specs)?;
    Ok(vec![batch])
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey(Vec<KeyValue>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyValue {
    Null { dtype: DataType },
    Boolean(bool),
    Signed(i128),
    Unsigned(u128),
    Float64(u64),
    Utf8(String),
}

fn key_value_from_array(array: &dyn arrow::array::Array, row: usize) -> Result<KeyValue> {
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
                .downcast_ref::<BooleanArray>()
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
        Float32 => Ok(KeyValue::Float64(
            (array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Float32Array downcast"))?
                .value(row) as f64)
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
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad StringArray downcast"))?
                .value(row)
                .to_string(),
        )),
        other => Err(DataFrameError::type_mismatch(
            None::<String>,
            "group-by key type supported".to_string(),
            other.to_string(),
        )),
    }
}

#[derive(Debug, Clone)]
struct ColumnSpec {
    name: String,
    index: usize,
    dtype: DataType,
}

fn resolve_column_expr(expr: &Expr, schema: &Schema) -> Result<ColumnSpec> {
    match expr {
        E::Alias { expr, name } => {
            let mut s = resolve_column_expr(expr, schema)?;
            s.name = name.clone();
            Ok(s)
        }
        E::Column(name) => {
            let idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
            Ok(ColumnSpec {
                name: name.clone(),
                index: idx,
                dtype: schema.fields()[idx].data_type().clone(),
            })
        }
        _ => Err(DataFrameError::invalid_operation(
            "group_by only supports column expressions (with optional alias)",
        )),
    }
}

#[derive(Debug, Clone)]
struct AggSpec {
    name: String,
    func: crate::expr::AggFunc,
    input: Expr,
    dtype: DataType,
}

fn resolve_agg_expr(expr: &Expr, schema: &Schema) -> Result<AggSpec> {
    match expr {
        E::Alias { expr, name } => {
            let mut s = resolve_agg_expr(expr, schema)?;
            s.name = name.clone();
            Ok(s)
        }
        E::Agg { func, expr } => {
            let (input, dtype) = match expr.as_ref() {
                E::Column(name) => {
                    let idx = schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                        .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
                    (
                        E::Column(name.clone()),
                        schema.fields()[idx].data_type().clone(),
                    )
                }
                E::Literal(s) => (E::Literal(s.clone()), scalar_dtype(s)),
                _ => {
                    return Err(DataFrameError::invalid_operation(
                        "aggregation input must be a column or literal (optionally aliased)",
                    ))
                }
            };

            Ok(AggSpec {
                name: format!("{:?}({:?})", func, input),
                func: *func,
                input,
                dtype,
            })
        }
        _ => Err(DataFrameError::invalid_operation(
            "aggregation expression must be Agg or Alias(Agg)",
        )),
    }
}

fn scalar_dtype(s: &Scalar) -> DataType {
    match s {
        Scalar::Null => DataType::Null,
        Scalar::Boolean(_) => DataType::Boolean,
        Scalar::Int64(_) => DataType::Int64,
        Scalar::Float64(_) => DataType::Float64,
        Scalar::Utf8(_) => DataType::Utf8,
    }
}

#[derive(Debug, Clone)]
struct GroupAccum {
    keys: Vec<KeyValue>,
    aggs: Vec<AggState>,
}

impl GroupAccum {
    fn new(key: &GroupKey, aggs: &[AggSpec]) -> Result<Self> {
        Ok(Self {
            keys: key.0.clone(),
            aggs: aggs.iter().map(AggState::new).collect::<Result<_>>()?,
        })
    }

    fn update(&mut self, batch: &RecordBatch, row: usize, aggs: &[AggSpec]) -> Result<()> {
        for (i, spec) in aggs.iter().enumerate() {
            let val = match &spec.input {
                E::Column(name) => {
                    let idx = batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                        .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
                    array_scalar_at(batch.column(idx).as_ref(), row)?
                }
                E::Literal(s) => s.clone(),
                _ => unreachable!("validated in resolve_agg_expr"),
            };
            self.aggs[i].update(&spec.func, &val, &spec.dtype)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum AggState {
    Sum { sum: f64, count: i64 },
    Min { v: f64, seen: bool },
    Max { v: f64, seen: bool },
    Count { count: i64 },
    Mean { sum: f64, count: i64 },
}

impl AggState {
    fn new(spec: &AggSpec) -> Result<Self> {
        use crate::expr::AggFunc;
        match spec.func {
            AggFunc::Count => Ok(AggState::Count { count: 0 }),
            AggFunc::Mean => Ok(AggState::Mean { sum: 0.0, count: 0 }),
            AggFunc::Sum => {
                numeric_kind(&spec.dtype)?;
                Ok(AggState::Sum { sum: 0.0, count: 0 })
            }
            AggFunc::Min => {
                numeric_kind(&spec.dtype)?;
                Ok(AggState::Min {
                    v: 0.0,
                    seen: false,
                })
            }
            AggFunc::Max => {
                numeric_kind(&spec.dtype)?;
                Ok(AggState::Max {
                    v: 0.0,
                    seen: false,
                })
            }
        }
    }

    fn update(&mut self, func: &crate::expr::AggFunc, v: &Scalar, dtype: &DataType) -> Result<()> {
        use crate::expr::AggFunc;
        match func {
            AggFunc::Count => {
                if !matches!(v, Scalar::Null) {
                    if let AggState::Count { count } = self {
                        *count += 1;
                    }
                }
                Ok(())
            }
            AggFunc::Mean => {
                if matches!(v, Scalar::Null) {
                    return Ok(());
                }
                let fv = scalar_as_f64(v, dtype)?;
                if let AggState::Mean { sum, count } = self {
                    *sum += fv;
                    *count += 1;
                    Ok(())
                } else {
                    Err(DataFrameError::invalid_operation("invalid mean state"))
                }
            }
            AggFunc::Sum => {
                if matches!(v, Scalar::Null) {
                    return Ok(());
                }
                let fv = scalar_as_f64(v, dtype)?;
                if let AggState::Sum { sum, count } = self {
                    *sum += fv;
                    *count += 1;
                    Ok(())
                } else {
                    Err(DataFrameError::invalid_operation("invalid sum state"))
                }
            }
            AggFunc::Min => {
                if matches!(v, Scalar::Null) {
                    return Ok(());
                }
                let fv = scalar_as_f64(v, dtype)?;
                if let AggState::Min { v, seen } = self {
                    if !*seen || fv < *v {
                        *v = fv;
                        *seen = true;
                    }
                    Ok(())
                } else {
                    Err(DataFrameError::invalid_operation("invalid min state"))
                }
            }
            AggFunc::Max => {
                if matches!(v, Scalar::Null) {
                    return Ok(());
                }
                let fv = scalar_as_f64(v, dtype)?;
                if let AggState::Max { v, seen } = self {
                    if !*seen || fv > *v {
                        *v = fv;
                        *seen = true;
                    }
                    Ok(())
                } else {
                    Err(DataFrameError::invalid_operation("invalid max state"))
                }
            }
        }
    }
}

fn numeric_kind(dtype: &DataType) -> Result<()> {
    use arrow::datatypes::DataType::*;
    match dtype {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
            Ok(())
        }
        other => Err(DataFrameError::type_mismatch(
            None::<String>,
            "numeric type".to_string(),
            other.to_string(),
        )),
    }
}

fn array_scalar_at(array: &dyn arrow::array::Array, row: usize) -> Result<Scalar> {
    if array.is_null(row) {
        return Ok(Scalar::Null);
    }
    match array.data_type() {
        DataType::Int8 => Ok(Scalar::Int64(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int8Array downcast"))?
                .value(row) as i64,
        )),
        DataType::Int16 => Ok(Scalar::Int64(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int16Array downcast"))?
                .value(row) as i64,
        )),
        DataType::Int32 => Ok(Scalar::Int64(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int32Array downcast"))?
                .value(row) as i64,
        )),
        DataType::Int64 => Ok(Scalar::Int64(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Int64Array downcast"))?
                .value(row),
        )),
        DataType::UInt8 => Ok(Scalar::Int64(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt8Array downcast"))?
                .value(row) as i64,
        )),
        DataType::UInt16 => Ok(Scalar::Int64(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt16Array downcast"))?
                .value(row) as i64,
        )),
        DataType::UInt32 => Ok(Scalar::Int64(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt32Array downcast"))?
                .value(row) as i64,
        )),
        DataType::UInt64 => {
            let v = array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad UInt64Array downcast"))?
                .value(row);
            let v = i64::try_from(v).map_err(|_| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    "UInt64 within i64 range".to_string(),
                    v.to_string(),
                )
            })?;
            Ok(Scalar::Int64(v))
        }
        DataType::Float32 => Ok(Scalar::Float64(
            array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Float32Array downcast"))?
                .value(row) as f64,
        )),
        DataType::Float64 => Ok(Scalar::Float64(
            array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad Float64Array downcast"))?
                .value(row),
        )),
        DataType::Utf8 => Ok(Scalar::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad StringArray downcast"))?
                .value(row)
                .to_string(),
        )),
        DataType::Boolean => Ok(Scalar::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| DataFrameError::invalid_operation("bad BooleanArray downcast"))?
                .value(row),
        )),
        other => Err(DataFrameError::type_mismatch(
            None::<String>,
            "scalar extraction supported type".to_string(),
            other.to_string(),
        )),
    }
}

fn scalar_as_f64(v: &Scalar, dtype: &DataType) -> Result<f64> {
    match (v, dtype) {
        (Scalar::Int64(i), _) => Ok(*i as f64),
        (Scalar::Float64(f), _) => Ok(*f),
        _ => Err(DataFrameError::type_mismatch(
            None::<String>,
            "numeric".to_string(),
            dtype.to_string(),
        )),
    }
}

fn build_grouped_batch(
    groups: Vec<GroupAccum>,
    keys: &[ColumnSpec],
    aggs: &[AggSpec],
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(keys.len() + aggs.len());
    let mut arrays = Vec::with_capacity(keys.len() + aggs.len());

    for (key_idx, k) in keys.iter().enumerate() {
        fields.push(Field::new(&k.name, k.dtype.clone(), true));
        arrays.push(build_key_array(&k.dtype, &groups, key_idx)?);
    }

    for (agg_idx, a) in aggs.iter().enumerate() {
        let out_dtype = match a.func {
            crate::expr::AggFunc::Count => DataType::Int64,
            crate::expr::AggFunc::Mean => DataType::Float64,
            crate::expr::AggFunc::Sum | crate::expr::AggFunc::Min | crate::expr::AggFunc::Max => {
                a.dtype.clone()
            }
        };
        fields.push(Field::new(&a.name, out_dtype.clone(), true));
        arrays.push(build_agg_array(&out_dtype, &groups, agg_idx, &a.dtype)?);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| DataFrameError::schema_mismatch(format!("failed to build RecordBatch: {e}")))
}

fn build_key_array(dtype: &DataType, groups: &[GroupAccum], key_idx: usize) -> Result<ArrayRef> {
    match dtype {
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Utf8(v) => Some(v.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Boolean(v) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int8 => Ok(Arc::new(arrow::array::Int8Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Signed(v) => Some(*v as i8),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int16 => Ok(Arc::new(arrow::array::Int16Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Signed(v) => Some(*v as i16),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int32 => Ok(Arc::new(arrow::array::Int32Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Signed(v) => Some(*v as i32),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int64 => Ok(Arc::new(arrow::array::Int64Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Signed(v) => Some(*v as i64),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt8 => Ok(Arc::new(arrow::array::UInt8Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Unsigned(v) => Some(*v as u8),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt16 => Ok(Arc::new(arrow::array::UInt16Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Unsigned(v) => Some(*v as u16),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt32 => Ok(Arc::new(arrow::array::UInt32Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Unsigned(v) => Some(*v as u32),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt64 => Ok(Arc::new(arrow::array::UInt64Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Unsigned(v) => Some(*v as u64),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Float32 => Ok(Arc::new(arrow::array::Float32Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Float64(bits) => Some(f64::from_bits(*bits) as f32),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Float64 => Ok(Arc::new(arrow::array::Float64Array::from(
            groups
                .iter()
                .map(|g| match &g.keys[key_idx] {
                    KeyValue::Null { .. } => None,
                    KeyValue::Float64(bits) => Some(f64::from_bits(*bits)),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        other => Err(DataFrameError::type_mismatch(
            None::<String>,
            "group-by key output supported type".to_string(),
            other.to_string(),
        )),
    }
}

fn build_agg_array(
    dtype: &DataType,
    groups: &[GroupAccum],
    agg_idx: usize,
    input_dtype: &DataType,
) -> Result<ArrayRef> {
    match dtype {
        DataType::Int64 => Ok(Arc::new(arrow::array::Int64Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Count { count } => Some(*count),
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as i64),
                    AggState::Min { v, seen } => (*seen).then_some(*v as i64),
                    AggState::Max { v, seen } => (*seen).then_some(*v as i64),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int8 => Ok(Arc::new(arrow::array::Int8Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as i8),
                    AggState::Min { v, seen } => (*seen).then_some(*v as i8),
                    AggState::Max { v, seen } => (*seen).then_some(*v as i8),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int16 => Ok(Arc::new(arrow::array::Int16Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as i16),
                    AggState::Min { v, seen } => (*seen).then_some(*v as i16),
                    AggState::Max { v, seen } => (*seen).then_some(*v as i16),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int32 => Ok(Arc::new(arrow::array::Int32Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as i32),
                    AggState::Min { v, seen } => (*seen).then_some(*v as i32),
                    AggState::Max { v, seen } => (*seen).then_some(*v as i32),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt8 => Ok(Arc::new(arrow::array::UInt8Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as u8),
                    AggState::Min { v, seen } => (*seen).then_some(*v as u8),
                    AggState::Max { v, seen } => (*seen).then_some(*v as u8),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt16 => Ok(Arc::new(arrow::array::UInt16Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as u16),
                    AggState::Min { v, seen } => (*seen).then_some(*v as u16),
                    AggState::Max { v, seen } => (*seen).then_some(*v as u16),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt32 => Ok(Arc::new(arrow::array::UInt32Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as u32),
                    AggState::Min { v, seen } => (*seen).then_some(*v as u32),
                    AggState::Max { v, seen } => (*seen).then_some(*v as u32),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::UInt64 => Ok(Arc::new(arrow::array::UInt64Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as u64),
                    AggState::Min { v, seen } => (*seen).then_some(*v as u64),
                    AggState::Max { v, seen } => (*seen).then_some(*v as u64),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Float64 => Ok(Arc::new(arrow::array::Float64Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Mean { sum, count } => {
                        if *count == 0 {
                            None
                        } else {
                            Some(*sum / (*count as f64))
                        }
                    }
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum),
                    AggState::Min { v, seen } => (*seen).then_some(*v),
                    AggState::Max { v, seen } => (*seen).then_some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Float32 => Ok(Arc::new(arrow::array::Float32Array::from(
            groups
                .iter()
                .map(|g| match &g.aggs[agg_idx] {
                    AggState::Mean { sum, count } => {
                        if *count == 0 {
                            None
                        } else {
                            Some((*sum / (*count as f64)) as f32)
                        }
                    }
                    AggState::Sum { sum, count } => (*count > 0).then_some(*sum as f32),
                    AggState::Min { v, seen } => (*seen).then_some(*v as f32),
                    AggState::Max { v, seen } => (*seen).then_some(*v as f32),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        other => Err(DataFrameError::type_mismatch(
            None::<String>,
            format!("aggregation output supported type (input dtype {input_dtype:?})"),
            other.to_string(),
        )),
    }
}
