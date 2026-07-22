use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alopex_core::dataframe as core_df;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, NullArray,
    StringArray, StringBuilder, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::expr::{
    ConcatStrNullBehavior, DatetimeFunction, Expr as E, ExprFunction, ListFunction, Scalar,
    StringFunction,
};
use crate::{DataFrameError, Expr, Result};

/// Evaluates `Expr` values over Arrow `RecordBatch` inputs.
pub struct ExprEval;

impl ExprEval {
    /// Evaluate `expr` for every row in `batch` and return the resulting Arrow array.
    pub fn evaluate(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
        let mut evaluator = BatchExprEvaluator::for_expressions(batch, &[expr]);
        evaluator.evaluate(expr)
    }

    /// Construct a batch-scoped evaluator shared by one projection or operator invocation.
    ///
    /// Its cache owns only `ArrayRef`s derived from this batch and is dropped with the evaluator.
    /// Expression fingerprints remain internal implementation details.
    pub(crate) fn for_expressions<'a>(
        batch: &'a RecordBatch,
        expressions: &[&Expr],
    ) -> BatchExprEvaluator<'a> {
        BatchExprEvaluator::for_expressions(batch, expressions)
    }
}

/// Reuse plan for deterministic calculations within exactly one input batch.
///
/// Only expressions that occur more than once are retained. Aliases are intentionally omitted
/// from the fingerprint so two aliases of the same calculation share the computation. There are
/// no UDF or volatile expression variants in the v0.8 AST; unclassifiable forms (`Agg` and
/// `Wildcard`) are excluded rather than guessed.
#[derive(Debug, Clone)]
pub(crate) struct ExpressionReusePlan {
    reusable: HashSet<ExprFingerprint>,
}

impl ExpressionReusePlan {
    fn for_expressions(batch: &RecordBatch, expressions: &[&Expr]) -> Self {
        let mut counts = HashMap::new();
        for expression in expressions {
            collect_fingerprints(expression, batch.schema().as_ref(), &mut counts);
        }
        let mut reusable = HashSet::new();
        for expression in expressions {
            collect_maximal_reusable_fingerprints(
                expression,
                batch.schema().as_ref(),
                &counts,
                &mut reusable,
            );
        }
        Self { reusable }
    }

    fn fingerprint(&self, expression: &Expr, schema: &Schema) -> Option<ExprFingerprint> {
        expression_fingerprint(expression, schema)
    }

    fn should_reuse(&self, fingerprint: &ExprFingerprint) -> bool {
        self.reusable.contains(fingerprint)
    }
}

/// Evaluator and temporary cache for one `RecordBatch` only.
pub(crate) struct BatchExprEvaluator<'a> {
    batch: &'a RecordBatch,
    reuse_plan: ExpressionReusePlan,
    cache: HashMap<ExprFingerprint, ArrayRef>,
}

impl<'a> BatchExprEvaluator<'a> {
    fn for_expressions(batch: &'a RecordBatch, expressions: &[&Expr]) -> Self {
        Self {
            batch,
            reuse_plan: ExpressionReusePlan::for_expressions(batch, expressions),
            cache: HashMap::new(),
        }
    }

    /// Evaluate one expression, reusing only deterministic values retained for this batch.
    pub(crate) fn evaluate(&mut self, expression: &Expr) -> Result<ArrayRef> {
        let fingerprint = self
            .reuse_plan
            .fingerprint(expression, self.batch.schema().as_ref());
        if let Some(fingerprint) = &fingerprint {
            if self.reuse_plan.should_reuse(fingerprint) {
                if let Some(cached) = self.cache.get(fingerprint) {
                    return Ok(cached.clone());
                }
                let output = self.evaluate_uncached(expression)?;
                self.cache.insert(fingerprint.clone(), output.clone());
                return Ok(output);
            }
        }
        self.evaluate_uncached(expression)
    }

    #[cfg(test)]
    fn cache_len(&self) -> usize {
        self.cache.len()
    }

    fn evaluate_uncached(&mut self, expr: &Expr) -> Result<ArrayRef> {
        match expr {
            E::Column(name) => {
                let idx = self
                    .batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
                Ok(self.batch.column(idx).clone())
            }
            E::Literal(s) => scalar_to_array(s, self.batch.num_rows()),
            E::Alias { expr, .. } => self.evaluate(expr),
            E::Wildcard => Err(DataFrameError::invalid_operation(
                "wildcard cannot be evaluated as a standalone expression",
            )),
            E::UnaryOp { op, expr } => {
                let v = self.evaluate(expr)?;
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
                let l = self.evaluate(left)?;
                let r = self.evaluate(right)?;
                eval_binary(op, &l, &r)
            }
            E::Function { input, function } => {
                let input = self.evaluate(input)?;
                eval_function(&input, function)
            }
            E::ConcatStr {
                inputs,
                separator,
                null_behavior,
            } => self.eval_concat_str(inputs, separator, null_behavior),
            E::Agg { .. } => Err(DataFrameError::invalid_operation(
                "aggregation expressions must be evaluated by aggregate operator",
            )),
        }
    }

    fn eval_concat_str(
        &mut self,
        inputs: &[Expr],
        separator: &str,
        null_behavior: &ConcatStrNullBehavior,
    ) -> Result<ArrayRef> {
        if inputs.len() < 2 {
            return Err(DataFrameError::invalid_operation(
                "concat_str requires at least two input expressions",
            ));
        }

        let arrays = inputs
            .iter()
            .map(|input| self.evaluate(input))
            .collect::<Result<Vec<_>>>()?;
        let strings = arrays
            .iter()
            .map(|array| {
                array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFrameError::type_mismatch(
                        None::<String>,
                        DataType::Utf8.to_string(),
                        array.data_type().to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut builder = StringBuilder::new();
        for row in 0..self.batch.num_rows() {
            let mut output = String::new();
            let mut included = 0usize;
            let mut propagate_null = false;

            for input in &strings {
                let value = if input.is_null(row) {
                    match null_behavior {
                        ConcatStrNullBehavior::Propagate => {
                            propagate_null = true;
                            break;
                        }
                        ConcatStrNullBehavior::Ignore => continue,
                        ConcatStrNullBehavior::Replace(value) => value.as_str(),
                    }
                } else {
                    input.value(row)
                };
                if included > 0 {
                    output.push_str(separator);
                }
                output.push_str(value);
                included += 1;
            }

            if propagate_null || included == 0 {
                builder.append_null();
            } else {
                builder.append_value(output);
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExprFingerprint(String);

fn collect_fingerprints(
    expression: &Expr,
    schema: &Schema,
    counts: &mut HashMap<ExprFingerprint, usize>,
) {
    if let E::Alias { expr, .. } = expression {
        collect_fingerprints(expr, schema, counts);
        return;
    }

    if let Some(fingerprint) = expression_fingerprint(expression, schema) {
        *counts.entry(fingerprint).or_default() += 1;
    }
    match expression {
        E::UnaryOp { expr, .. } | E::Function { input: expr, .. } => {
            collect_fingerprints(expr, schema, counts)
        }
        E::BinaryOp { left, right, .. } => {
            collect_fingerprints(left, schema, counts);
            collect_fingerprints(right, schema, counts);
        }
        E::ConcatStr { inputs, .. } => {
            for input in inputs {
                collect_fingerprints(input, schema, counts);
            }
        }
        E::Column(_) | E::Literal(_) | E::Wildcard | E::Agg { .. } | E::Alias { .. } => {}
    }
}

/// Retain only maximal repeated calculations. Once a repeated parent is cached, evaluating its
/// second occurrence cannot reach its children, so caching those descendants would retain extra
/// Arrow arrays without saving work. This gives the same retained intermediate as explicitly
/// naming the repeated parent.
fn collect_maximal_reusable_fingerprints(
    expression: &Expr,
    schema: &Schema,
    counts: &HashMap<ExprFingerprint, usize>,
    reusable: &mut HashSet<ExprFingerprint>,
) {
    if let E::Alias { expr, .. } = expression {
        collect_maximal_reusable_fingerprints(expr, schema, counts, reusable);
        return;
    }

    if let Some(fingerprint) = expression_fingerprint(expression, schema) {
        if counts.get(&fingerprint).copied().unwrap_or_default() > 1 {
            reusable.insert(fingerprint);
            return;
        }
    }

    match expression {
        E::UnaryOp { expr, .. } | E::Function { input: expr, .. } => {
            collect_maximal_reusable_fingerprints(expr, schema, counts, reusable)
        }
        E::BinaryOp { left, right, .. } => {
            collect_maximal_reusable_fingerprints(left, schema, counts, reusable);
            collect_maximal_reusable_fingerprints(right, schema, counts, reusable);
        }
        E::ConcatStr { inputs, .. } => {
            for input in inputs {
                collect_maximal_reusable_fingerprints(input, schema, counts, reusable);
            }
        }
        E::Column(_) | E::Literal(_) | E::Wildcard | E::Agg { .. } | E::Alias { .. } => {}
    }
}

fn expression_fingerprint(expression: &Expr, schema: &Schema) -> Option<ExprFingerprint> {
    let fingerprint = match expression {
        E::Column(name) => {
            let field = schema.fields().iter().find(|field| field.name() == name)?;
            format!(
                "column(name={name:?},type={:?},nullable={})",
                field.data_type(),
                field.is_nullable()
            )
        }
        E::Literal(Scalar::Null) => "literal(null)".to_string(),
        E::Literal(Scalar::Boolean(value)) => format!("literal(bool={value})"),
        E::Literal(Scalar::Int64(value)) => format!("literal(i64={value})"),
        E::Literal(Scalar::Float64(value)) => format!("literal(f64={:x})", value.to_bits()),
        E::Literal(Scalar::Utf8(value)) => format!("literal(utf8={value:?})"),
        E::UnaryOp { op, expr } => {
            format!("unary({op:?},{})", expression_fingerprint(expr, schema)?.0)
        }
        E::BinaryOp { left, op, right } => format!(
            "binary({op:?},{},{})",
            expression_fingerprint(left, schema)?.0,
            expression_fingerprint(right, schema)?.0
        ),
        E::Function { input, function } => format!(
            "function({function:?},{})",
            expression_fingerprint(input, schema)?.0
        ),
        E::ConcatStr {
            inputs,
            separator,
            null_behavior,
        } => {
            let inputs = inputs
                .iter()
                .map(|input| expression_fingerprint(input, schema).map(|fingerprint| fingerprint.0))
                .collect::<Option<Vec<_>>>()?;
            format!("concat_str({separator:?},{null_behavior:?},{inputs:?})")
        }
        E::Alias { expr, .. } => return expression_fingerprint(expr, schema),
        E::Wildcard | E::Agg { .. } => return None,
    };
    Some(ExprFingerprint(fingerprint))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{ExprEval, ExpressionReusePlan};
    use crate::expr::{col, concat_str, lit, ConcatStrNullBehavior, Expr};
    use crate::DataFrameError;

    fn strings_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("first", DataType::Utf8, true),
            Field::new("second", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("b"), Some("c"), None])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn concat_values(null_behavior: ConcatStrNullBehavior) -> Vec<Option<String>> {
        let batch = strings_batch();
        let expression = concat_str(vec![col("first"), col("second")], "-", null_behavior).unwrap();
        let output = ExprEval::evaluate(&expression, &batch).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        (0..output.len())
            .map(|index| (!output.is_null(index)).then(|| output.value(index).to_string()))
            .collect()
    }

    #[test]
    fn concat_str_applies_all_declared_null_policies() {
        assert_eq!(
            concat_values(ConcatStrNullBehavior::Propagate),
            vec![Some("a-b".into()), None, None]
        );
        assert_eq!(
            concat_values(ConcatStrNullBehavior::Ignore),
            vec![Some("a-b".into()), Some("c".into()), None]
        );
        assert_eq!(
            concat_values(ConcatStrNullBehavior::Replace("?".into())),
            vec![Some("a-b".into()), Some("?-c".into()), Some("?-?".into()),]
        );
    }

    #[test]
    fn concat_str_rejects_non_utf8_input_before_projecting_output() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("number", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
            ],
        )
        .unwrap();
        let expression = concat_str(
            vec![col("text"), col("number")],
            "-",
            ConcatStrNullBehavior::Propagate,
        )
        .unwrap();

        assert!(matches!(
            ExprEval::evaluate(&expression, &batch),
            Err(DataFrameError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn deterministic_duplicate_subexpressions_share_only_the_current_batch_cache() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
        )
        .unwrap();
        let calculation = col("value").add(lit(1_i64));
        let first = calculation.clone().alias("first");
        let second = calculation.alias("second");
        let expressions = [&first, &second];

        let mut evaluator = ExprEval::for_expressions(&batch, &expressions);
        let first_output = evaluator.evaluate(&first).unwrap();
        let second_output = evaluator.evaluate(&second).unwrap();
        assert!(Arc::ptr_eq(&first_output, &second_output));
        assert_eq!(evaluator.cache_len(), 1);

        let fresh = ExprEval::for_expressions(&batch, &expressions);
        assert_eq!(fresh.cache_len(), 0);
    }

    #[test]
    fn unclassifiable_expressions_are_not_added_to_a_reuse_plan() {
        let batch = strings_batch();
        let plan = ExpressionReusePlan::for_expressions(&batch, &[&Expr::Wildcard]);
        assert!(plan.reusable.is_empty());
    }
}
