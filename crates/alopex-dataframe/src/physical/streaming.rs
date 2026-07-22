//! Incremental stream ownership contracts.
//!
//! Source implementations live in `io` and are added independently. This module owns no eager
//! reader fallback: a factory either opens an owned batch source or the compiler reports a
//! structured preflight outcome before the source is opened.

use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::expr::{Expr as E, Operator, Scalar};
use crate::lazy::ProjectionKind;
use crate::physical::budget::{
    ResourceBudget, ResourceReservation, ResourceScope, StreamFailure, StreamFailureClass,
    StreamOptions, StreamTerminal, StreamTerminalState,
};
use crate::physical::plan::{
    ScanSource, SourceLimit, StreamingOperator, StreamingPhysicalPlan, StreamingSource,
};
use crate::physical::{operators, record_batch_memory_size};
use crate::{DataFrame, DataFrameError, Expr, Result};

/// Context supplied to a newly opened source. It shares one budget with the stream.
#[derive(Debug, Clone)]
pub struct BatchOpenContext {
    /// The validated resource options selected by the caller.
    pub options: StreamOptions,
    /// Shared resource budget for source/decode/operator/output reservations.
    pub budget: ResourceBudget,
}

impl BatchOpenContext {
    /// Construct source-open context and its shared budget.
    pub fn new(options: StreamOptions) -> Self {
        Self {
            options,
            budget: ResourceBudget::from_options(options),
        }
    }
}

/// A source-owned batch whose reservation remains held until consumer ownership transfer.
#[derive(Debug)]
pub struct StreamBatch {
    batch: RecordBatch,
    reservation: ResourceReservation,
}

impl StreamBatch {
    /// Attach the reservation acquired before the source published this batch.
    pub fn new(batch: RecordBatch, reservation: ResourceReservation) -> Self {
        Self { batch, reservation }
    }

    fn into_parts(self) -> (RecordBatch, ResourceReservation) {
        (self.batch, self.reservation)
    }
}

/// Owned incremental reader for one logical source execution.
pub trait BatchSource: Send {
    /// Return the source schema known at open time.
    fn schema(&self) -> SchemaRef;

    /// Produce at most one reserved batch. `None` denotes normal end-of-stream.
    fn next_batch(&mut self) -> Result<Option<StreamBatch>>;

    /// Release source handles and source-owned reservations. It is called at most once.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Re-consumable factory for a source; a factory must not perform source I/O until `open`.
pub trait BatchSourceFactory {
    /// Stable source identifier for diagnostics.
    fn source_name(&self) -> &'static str;

    /// Return the source schema known before the source produces batches.
    fn schema(&self) -> Result<SchemaRef>;

    /// Return static capability limitations visible before opening the source.
    fn source_limits(&self) -> Vec<SourceLimit> {
        Vec::new()
    }

    /// Open an independently owned source cursor.
    fn open(&self, context: BatchOpenContext) -> Result<Box<dyn BatchSource>>;
}

/// Pre-decode ownership estimate for the next V08 columnar row group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnarSegmentBatchPlan {
    /// Bytes retained by the core column decoder while the Arrow batch is built.
    pub decoded_bytes: u64,
    /// Declared upper bound for the completed Arrow-compatible batch.
    pub arrow_allocation_upper_bound: u64,
}

impl ColumnarSegmentBatchPlan {
    /// Total peak ownership that must be reserved before the row group is fetched.
    pub const fn reservation_upper_bound(self) -> u64 {
        self.decoded_bytes
            .saturating_add(self.arrow_allocation_upper_bound)
    }
}

/// One owned V08 columnar segment cursor supplied by an adapter crate.
///
/// The cursor exposes its next row-group bound before the DataFrame source
/// invokes `next_batch`.  This preserves the allocation-before-reservation
/// contract without making `alopex-dataframe` depend on `alopex-embedded`.
pub trait ColumnarSegmentCursor: Send {
    /// Schema fixed during bounded source open.
    fn schema(&self) -> SchemaRef;

    /// Bytes still retained by source metadata after the cursor has opened.
    fn metadata_footprint_upper_bound(&self) -> u64;

    /// Return the next row-group bound without fetching its chunks.
    fn next_batch_plan(&self) -> Result<Option<ColumnarSegmentBatchPlan>>;

    /// Fetch and decode exactly the row group described by `next_batch_plan`.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Release adapter-owned source handles. It is called at most once.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Re-consumable V08 columnar source provider supplied by an adapter crate.
pub trait ColumnarSegmentProvider: Send + Sync {
    /// Stable source identifier for diagnostics.
    fn source_name(&self) -> &'static str {
        "columnar_segment"
    }

    /// Static limitations visible before a V08 cursor is opened.
    fn source_limits(&self) -> Vec<SourceLimit>;

    /// Open a V08 cursor, bounded by the caller's metadata allocation limit.
    fn open(&self, metadata_limit_bytes: u64) -> Result<Box<dyn ColumnarSegmentCursor>>;
}

/// Open a compiler-produced source tree without ever entering the eager executor or an eager
/// reader. This is crate-visible because `LazyFrame` owns physical compilation while this module
/// owns the source and reservation lifecycle.
pub(crate) fn open_streaming_plan(
    plan: StreamingPhysicalPlan,
    context: BatchOpenContext,
) -> Result<Box<dyn BatchSource>> {
    Ok(Box::new(StreamingPlanBatchSource::open(plan, context)?))
}

/// A streaming physical plan with one currently-open source and a per-batch operator pipeline.
struct StreamingPlanBatchSource {
    runner: StreamingSourceRunner,
    operators: Vec<RuntimeStreamingOperator>,
    schema: SchemaRef,
    budget: ResourceBudget,
    closed: bool,
}

impl StreamingPlanBatchSource {
    fn open(plan: StreamingPhysicalPlan, context: BatchOpenContext) -> Result<Self> {
        let schema = preflight_streaming_schema(&plan, &context)?;
        let runner = StreamingSourceRunner::open(plan.source, context.clone(), schema.clone())?;
        Ok(Self {
            runner,
            operators: plan
                .operators
                .into_iter()
                .map(RuntimeStreamingOperator::from)
                .collect(),
            schema,
            budget: context.budget,
            closed: false,
        })
    }

    fn next_transformed_batch(&mut self) -> Result<Option<StreamBatch>> {
        loop {
            let Some(batch) = self.runner.next_batch()? else {
                return Ok(None);
            };
            let mut batch = Some(batch);
            for operator in &mut self.operators {
                let Some(input) = batch.take() else {
                    break;
                };
                batch = operator.apply(input, &self.budget)?;
            }
            if let Some(batch) = batch {
                return Ok(Some(batch));
            }
        }
    }

    fn close_once(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.runner.close()
    }
}

impl BatchSource for StreamingPlanBatchSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<StreamBatch>> {
        self.next_transformed_batch()
    }

    fn close(&mut self) -> Result<()> {
        self.close_once()
    }
}

/// Only one child source of a concat is open at a time. Child schemas are preflighted before the
/// first result, while handles are opened/released in declared input order during consumption.
enum StreamingSourceRunner {
    Leaf(Box<dyn BatchSource>),
    Concat {
        inputs: Vec<StreamingPhysicalPlan>,
        schema: SchemaRef,
        context: BatchOpenContext,
        next_input: usize,
        active: Option<Box<StreamingPlanBatchSource>>,
    },
}

impl StreamingSourceRunner {
    fn open(
        source: StreamingSource,
        context: BatchOpenContext,
        expected_schema: SchemaRef,
    ) -> Result<Self> {
        match source {
            StreamingSource::Scan(source) => Ok(Self::Leaf(open_scan_source(source, context)?)),
            StreamingSource::Concat { inputs, schema } => Ok(Self::Concat {
                inputs,
                schema: match schema {
                    Some(schema) if schema.as_ref() != expected_schema.as_ref() => {
                        return Err(DataFrameError::schema_mismatch(
                            "concat_schema_mismatch: declared schema differs from bounded preflight schema",
                        ));
                    }
                    Some(schema) => schema,
                    None => expected_schema,
                },
                context,
                next_input: 0,
                active: None,
            }),
        }
    }

    fn next_batch(&mut self) -> Result<Option<StreamBatch>> {
        match self {
            Self::Leaf(source) => source.next_batch(),
            Self::Concat {
                inputs,
                schema,
                context,
                next_input,
                active,
            } => loop {
                if let Some(child) = active.as_mut() {
                    match child.next_batch()? {
                        Some(batch) => {
                            if batch.batch.schema().as_ref() != schema.as_ref() {
                                return Err(DataFrameError::schema_mismatch(
                                    "concat_schema_mismatch: child emitted a schema different from the validated concat schema",
                                ));
                            }
                            return Ok(Some(batch));
                        }
                        None => {
                            child.close()?;
                            active.take();
                            *next_input += 1;
                        }
                    }
                    continue;
                }
                let Some(plan) = inputs.get(*next_input).cloned() else {
                    return Ok(None);
                };
                *active = Some(Box::new(StreamingPlanBatchSource::open(
                    plan,
                    context.clone(),
                )?));
            },
        }
    }

    fn close(&mut self) -> Result<()> {
        match self {
            Self::Leaf(source) => source.close(),
            Self::Concat { active, .. } => {
                if let Some(mut child) = active.take() {
                    child.close()?;
                }
                Ok(())
            }
        }
    }
}

fn open_scan_source(source: ScanSource, context: BatchOpenContext) -> Result<Box<dyn BatchSource>> {
    match source {
        ScanSource::Csv {
            path,
            predicate,
            projection,
        } => {
            if predicate.is_some() {
                return Err(DataFrameError::streaming_unsupported(
                    "csv_scan",
                    "predicate_was_not_lowered_to_batch_operator",
                ));
            }
            let factory = crate::io::CsvBatchSourceFactory::from_scan(path, None, projection);
            factory.open(context)
        }
        ScanSource::Parquet {
            path,
            predicate,
            projection,
        } => {
            if predicate.is_some() {
                return Err(DataFrameError::streaming_unsupported(
                    "parquet_scan",
                    "predicate_was_not_lowered_to_batch_operator",
                ));
            }
            let factory = crate::io::ParquetBatchSourceFactory::from_scan(path, None, projection);
            factory.open(context)
        }
        ScanSource::DataFrame(_) => Err(DataFrameError::streaming_unsupported(
            "dataframe_scan",
            "legacy_materialized_source",
        )),
    }
}

fn preflight_streaming_schema(
    plan: &StreamingPhysicalPlan,
    context: &BatchOpenContext,
) -> Result<SchemaRef> {
    let source_schema = match &plan.source {
        StreamingSource::Scan(source) => {
            let mut source = open_scan_source(source.clone(), context.clone())?;
            let schema = source.schema();
            source.close()?;
            schema
        }
        StreamingSource::Concat { inputs, schema } => {
            let mut expected = schema.clone();
            for (index, input) in inputs.iter().enumerate() {
                let input_schema = preflight_streaming_schema(input, context)?;
                if let Some(expected_schema) = &expected {
                    if input_schema.as_ref() != expected_schema.as_ref() {
                        return Err(DataFrameError::schema_mismatch(format!(
                            "concat_schema_mismatch: input {index} differs from the validated concat schema"
                        )));
                    }
                } else {
                    expected = Some(input_schema);
                }
            }
            expected.ok_or_else(|| {
                DataFrameError::invalid_operation("concat requires at least two inputs")
            })?
        }
    };
    infer_streaming_output_schema(&source_schema, &plan.operators)
}

fn infer_streaming_output_schema(
    source_schema: &SchemaRef,
    operators: &[StreamingOperator],
) -> Result<SchemaRef> {
    let mut schema = source_schema.clone();
    for operator in operators {
        schema = match operator {
            StreamingOperator::Projection { exprs, kind } => {
                infer_projection_schema(&schema, exprs, kind)?
            }
            StreamingOperator::Filter { predicate } => {
                let dtype = infer_expression_dtype(predicate, &schema)?;
                if dtype != DataType::Boolean {
                    return Err(DataFrameError::type_mismatch(
                        None::<String>,
                        DataType::Boolean.to_string(),
                        dtype.to_string(),
                    ));
                }
                schema
            }
            StreamingOperator::ForwardSlice { .. } => schema,
        };
    }
    Ok(schema)
}

fn infer_projection_schema(
    schema: &SchemaRef,
    expressions: &[Expr],
    kind: &ProjectionKind,
) -> Result<SchemaRef> {
    match kind {
        ProjectionKind::Select => {
            let mut fields = Vec::new();
            for expression in expressions {
                match expression {
                    E::Wildcard => fields.extend(schema.fields().iter().cloned()),
                    E::Column(name) => fields.push(schema_field(schema, name)?),
                    E::Alias { expr, name } => fields.push(Arc::new(Field::new(
                        name,
                        infer_expression_dtype(expr, schema)?,
                        true,
                    ))),
                    expression => fields.push(Arc::new(Field::new(
                        format!("{expression:?}"),
                        infer_expression_dtype(expression, schema)?,
                        true,
                    ))),
                }
            }
            Ok(Arc::new(Schema::new(fields)))
        }
        ProjectionKind::WithColumns => {
            let mut fields = schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            for expression in expressions {
                let (name, expression) = match expression {
                    E::Alias { expr, name } => (name, expr.as_ref()),
                    E::Column(name) => (name, expression),
                    _ => {
                        return Err(DataFrameError::invalid_operation(
                            "with_columns requires alias for non-column expressions",
                        ))
                    }
                };
                let field = Field::new(name, infer_expression_dtype(expression, schema)?, true);
                if let Some(index) = fields.iter().position(|existing| existing.name() == name) {
                    fields[index] = field;
                } else {
                    fields.push(field);
                }
            }
            Ok(Arc::new(Schema::new(fields)))
        }
    }
}

fn infer_expression_dtype(expression: &Expr, schema: &SchemaRef) -> Result<DataType> {
    match expression {
        E::Column(name) => Ok(schema_field(schema, name)?.data_type().clone()),
        E::Literal(Scalar::Null) => Ok(DataType::Null),
        E::Literal(Scalar::Boolean(_)) => Ok(DataType::Boolean),
        E::Literal(Scalar::Int64(_)) => Ok(DataType::Int64),
        E::Literal(Scalar::Float64(_)) => Ok(DataType::Float64),
        E::Literal(Scalar::Utf8(_)) => Ok(DataType::Utf8),
        E::Alias { expr, .. } => infer_expression_dtype(expr, schema),
        E::UnaryOp { expr, .. } => {
            let dtype = infer_expression_dtype(expr, schema)?;
            if dtype != DataType::Boolean {
                return Err(DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    dtype.to_string(),
                ));
            }
            Ok(DataType::Boolean)
        }
        E::BinaryOp { left, op, right } => {
            let left = infer_expression_dtype(left, schema)?;
            let right = infer_expression_dtype(right, schema)?;
            match op {
                Operator::And | Operator::Or => {
                    if left != DataType::Boolean || right != DataType::Boolean {
                        return Err(DataFrameError::type_mismatch(
                            None::<String>,
                            DataType::Boolean.to_string(),
                            format!("{left} and {right}"),
                        ));
                    }
                    Ok(DataType::Boolean)
                }
                Operator::Eq
                | Operator::Neq
                | Operator::Gt
                | Operator::Lt
                | Operator::Ge
                | Operator::Le => {
                    if left != right {
                        return Err(DataFrameError::type_mismatch(
                            None::<String>,
                            left.to_string(),
                            right.to_string(),
                        ));
                    }
                    Ok(DataType::Boolean)
                }
                Operator::Add | Operator::Sub | Operator::Mul | Operator::Div => {
                    if left != right || !is_numeric_dtype(&left) {
                        return Err(DataFrameError::type_mismatch(
                            None::<String>,
                            "matching numeric operands".to_string(),
                            format!("{left} and {right}"),
                        ));
                    }
                    Ok(left)
                }
            }
        }
        E::ConcatStr { inputs, .. } => {
            if inputs.len() < 2 {
                return Err(DataFrameError::invalid_operation(
                    "concat_str requires at least two input expressions",
                ));
            }
            for input in inputs {
                let dtype = infer_expression_dtype(input, schema)?;
                if dtype != DataType::Utf8 {
                    return Err(DataFrameError::type_mismatch(
                        None::<String>,
                        DataType::Utf8.to_string(),
                        dtype.to_string(),
                    ));
                }
            }
            Ok(DataType::Utf8)
        }
        E::Wildcard => Err(DataFrameError::invalid_operation(
            "wildcard cannot be evaluated as a standalone expression",
        )),
        E::Agg { .. } | E::Function { .. } => Err(DataFrameError::streaming_unsupported(
            "expression",
            "expression_allocation_bound_not_installed",
        )),
    }
}

fn schema_field(schema: &SchemaRef, name: &str) -> Result<Arc<Field>> {
    schema
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .cloned()
        .ok_or_else(|| DataFrameError::column_not_found(name.to_string()))
}

fn is_numeric_dtype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Stateful, batch-at-a-time transforms selected by the streaming compiler.
enum RuntimeStreamingOperator {
    Projection {
        exprs: Vec<Expr>,
        kind: ProjectionKind,
    },
    Filter {
        predicate: Expr,
    },
    ForwardSlice {
        remaining_offset: usize,
        remaining_len: usize,
    },
}

impl From<StreamingOperator> for RuntimeStreamingOperator {
    fn from(operator: StreamingOperator) -> Self {
        match operator {
            StreamingOperator::Projection { exprs, kind } => Self::Projection { exprs, kind },
            StreamingOperator::Filter { predicate } => Self::Filter { predicate },
            StreamingOperator::ForwardSlice { offset, len } => Self::ForwardSlice {
                remaining_offset: offset,
                remaining_len: len,
            },
        }
    }
}

impl RuntimeStreamingOperator {
    fn apply(
        &mut self,
        input: StreamBatch,
        budget: &ResourceBudget,
    ) -> Result<Option<StreamBatch>> {
        match self {
            Self::Projection { exprs, kind } => {
                let bound = projection_upper_bound(&input.batch, exprs)?;
                let output = replace_with_operator_output(input, budget, bound, |batch| {
                    let mut output = operators::project_batches(vec![batch], exprs, kind.clone())?;
                    output.pop().ok_or_else(|| {
                        DataFrameError::invalid_operation("projection produced no batch")
                    })
                })?;
                Ok((output.batch.num_rows() > 0).then_some(output))
            }
            Self::Filter { predicate } => {
                let bound = filter_upper_bound(&input.batch, predicate)?;
                let output = replace_with_operator_output(input, budget, bound, |batch| {
                    let mut output = operators::filter_batches(vec![batch], predicate)?;
                    output.pop().ok_or_else(|| {
                        DataFrameError::invalid_operation("filter produced no batch")
                    })
                })?;
                Ok((output.batch.num_rows() > 0).then_some(output))
            }
            Self::ForwardSlice {
                remaining_offset,
                remaining_len,
            } => {
                if *remaining_len == 0 {
                    return Ok(None);
                }
                let (batch, mut reservation) = input.into_parts();
                let rows = batch.num_rows();
                let skip = (*remaining_offset).min(rows);
                *remaining_offset -= skip;
                let available = rows.saturating_sub(skip);
                let take = (*remaining_len).min(available);
                *remaining_len -= take;
                if take == 0 {
                    reservation.release();
                    return Ok(None);
                }
                let output = batch.slice(skip, take);
                reservation.shrink_to(record_batch_memory_size(&output));
                Ok(Some(StreamBatch::new(output, reservation)))
            }
        }
    }
}

/// Reserve the complete transform/output upper bound before creating an Arrow output batch.
/// The source reservation stays live while its input is read. Once no input ownership remains,
/// its batch slot is released and the result reservation becomes the sole published batch slot.
fn replace_with_operator_output<F>(
    input: StreamBatch,
    budget: &ResourceBudget,
    upper_bound: u64,
    transform: F,
) -> Result<StreamBatch>
where
    F: FnOnce(RecordBatch) -> Result<RecordBatch>,
{
    let (batch, mut source_reservation) = input.into_parts();
    let mut output_reservation = budget.reserve(ResourceScope::Operator, upper_bound)?;
    let output = transform(batch)?;

    // `transform` consumed the input `RecordBatch`; only the result remains live now.
    source_reservation.release();
    let actual = record_batch_memory_size(&output);
    if actual > output_reservation.bytes() {
        return Err(DataFrameError::resource_limit_exceeded(
            budget.memory_limit_bytes(),
            budget
                .usage()
                .reserved_bytes
                .saturating_add(actual.saturating_sub(output_reservation.bytes())),
            budget.max_in_flight_batches(),
            budget.usage().reserved_batches,
            ResourceScope::Operator,
        ));
    }
    output_reservation.shrink_to(actual);
    output_reservation.promote_to_batch(ResourceScope::Output)?;
    Ok(StreamBatch::new(output, output_reservation))
}

fn projection_upper_bound(batch: &RecordBatch, expressions: &[Expr]) -> Result<u64> {
    expressions
        .iter()
        .try_fold(record_batch_memory_size(batch), |bound, expression| {
            Ok(bound.saturating_add(expression_upper_bound(expression, batch)?))
        })
}

fn filter_upper_bound(batch: &RecordBatch, predicate: &Expr) -> Result<u64> {
    // Arrow filtering can retain an input-sized temporary while it builds the
    // selected output. Reserve both the output-sized allocation and that
    // temporary in addition to predicate evaluation before invoking the
    // kernel; otherwise a valid filter can exceed its reservation after the
    // allocation has already happened.
    let batch_bytes = record_batch_memory_size(batch);
    Ok(batch_bytes
        .saturating_mul(2)
        .saturating_add(expression_upper_bound(predicate, batch)?))
}

/// Conservative peak allocation upper bound for a supported expression tree. Source columns are
/// borrowed from the input batch and therefore add no new allocation; every constructed array and
/// temporary child value is included.
fn expression_upper_bound(expression: &Expr, batch: &RecordBatch) -> Result<u64> {
    let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    match expression {
        E::Column(_) | E::Wildcard => Ok(0),
        E::Alias { expr, .. } => expression_upper_bound(expr, batch),
        E::Literal(scalar) => Ok(literal_upper_bound(scalar, rows)),
        E::UnaryOp { expr, .. } => Ok(expression_upper_bound(expr, batch)?
            .saturating_add(fixed_width_upper_bound(&DataType::Boolean, rows))),
        E::BinaryOp { left, op, right } => {
            let dtype = infer_expression_dtype(expression, &batch.schema())?;
            let output = match op {
                Operator::And
                | Operator::Or
                | Operator::Eq
                | Operator::Neq
                | Operator::Gt
                | Operator::Lt
                | Operator::Ge
                | Operator::Le => fixed_width_upper_bound(&DataType::Boolean, rows),
                Operator::Add | Operator::Sub | Operator::Mul | Operator::Div => {
                    fixed_width_upper_bound(&dtype, rows)
                }
            };
            Ok(expression_upper_bound(left, batch)?
                .saturating_add(expression_upper_bound(right, batch)?)
                .saturating_add(output))
        }
        E::ConcatStr {
            inputs,
            separator,
            null_behavior,
        } => {
            let children = inputs.iter().try_fold(0_u64, |bound, input| {
                Ok(bound.saturating_add(expression_upper_bound(input, batch)?))
            })?;
            let values = concat_string_value_upper_bound(inputs, separator, null_behavior, batch)?;
            Ok(children.saturating_add(utf8_array_upper_bound(values, rows)))
        }
        E::Agg { .. } | E::Function { .. } => Err(DataFrameError::streaming_unsupported(
            "expression",
            "expression_allocation_bound_not_installed",
        )),
    }
}

fn literal_upper_bound(value: &Scalar, rows: u64) -> u64 {
    match value {
        Scalar::Null => 128,
        Scalar::Boolean(_) => fixed_width_upper_bound(&DataType::Boolean, rows),
        Scalar::Int64(_) => fixed_width_upper_bound(&DataType::Int64, rows),
        Scalar::Float64(_) => fixed_width_upper_bound(&DataType::Float64, rows),
        Scalar::Utf8(value) => utf8_array_upper_bound(
            rows.saturating_mul(u64::try_from(value.len()).unwrap_or(u64::MAX)),
            rows,
        ),
    }
}

/// Conservative Arrow buffer baseline for one constructed output array.
const ARROW_ARRAY_ALLOCATION_OVERHEAD: u64 = 2 * 1024;

fn fixed_width_upper_bound(dtype: &DataType, rows: u64) -> u64 {
    let value_bytes = match dtype {
        DataType::Boolean => rows.saturating_add(7) / 8,
        DataType::Int8 | DataType::UInt8 => rows,
        DataType::Int16 | DataType::UInt16 => rows.saturating_mul(2),
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => rows.saturating_mul(4),
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => rows.saturating_mul(8),
        DataType::Null => 0,
        _ => rows.saturating_mul(16),
    };
    value_bytes
        .saturating_add(rows.saturating_add(7) / 8)
        // Arrow's small-array buffers retain an allocator-sized baseline even
        // when their logical value count is tiny. Include that ownership in
        // the pre-allocation bound rather than discovering it afterward.
        .saturating_add(ARROW_ARRAY_ALLOCATION_OVERHEAD)
}

fn utf8_array_upper_bound(value_bytes: u64, rows: u64) -> u64 {
    value_bytes
        .saturating_add(rows.saturating_add(1).saturating_mul(4))
        .saturating_add(rows.saturating_add(7) / 8)
        .saturating_add(ARROW_ARRAY_ALLOCATION_OVERHEAD)
}

fn concat_string_value_upper_bound(
    inputs: &[Expr],
    separator: &str,
    null_behavior: &crate::expr::ConcatStrNullBehavior,
    batch: &RecordBatch,
) -> Result<u64> {
    let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    let input_values = inputs.iter().try_fold(0_u64, |bound, input| {
        Ok(bound.saturating_add(string_value_upper_bound(input, batch)?))
    })?;
    let separators = rows
        .saturating_mul(u64::try_from(inputs.len().saturating_sub(1)).unwrap_or(u64::MAX))
        .saturating_mul(u64::try_from(separator.len()).unwrap_or(u64::MAX));
    let replacements = match null_behavior {
        crate::expr::ConcatStrNullBehavior::Replace(value) => rows
            .saturating_mul(u64::try_from(inputs.len()).unwrap_or(u64::MAX))
            .saturating_mul(u64::try_from(value.len()).unwrap_or(u64::MAX)),
        crate::expr::ConcatStrNullBehavior::Propagate
        | crate::expr::ConcatStrNullBehavior::Ignore => 0,
    };
    Ok(input_values
        .saturating_add(separators)
        .saturating_add(replacements))
}

fn string_value_upper_bound(expression: &Expr, batch: &RecordBatch) -> Result<u64> {
    match expression {
        E::Column(name) => {
            let field = schema_field(&batch.schema(), name)?;
            if field.data_type() != &DataType::Utf8 {
                return Err(DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Utf8.to_string(),
                    field.data_type().to_string(),
                ));
            }
            let index = batch
                .schema()
                .fields()
                .iter()
                .position(|field| field.name() == name)
                .expect("schema_field found the column");
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFrameError::type_mismatch(
                        None::<String>,
                        DataType::Utf8.to_string(),
                        batch.column(index).data_type().to_string(),
                    )
                })?;
            Ok((0..array.len()).fold(0_u64, |bytes, index| {
                if array.is_null(index) {
                    bytes
                } else {
                    bytes
                        .saturating_add(u64::try_from(array.value(index).len()).unwrap_or(u64::MAX))
                }
            }))
        }
        E::Literal(Scalar::Utf8(value)) => Ok(u64::try_from(batch.num_rows())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(value.len()).unwrap_or(u64::MAX))),
        E::Literal(Scalar::Null) => Ok(0),
        E::Alias { expr, .. } => string_value_upper_bound(expr, batch),
        E::ConcatStr {
            inputs,
            separator,
            null_behavior,
        } => concat_string_value_upper_bound(inputs, separator, null_behavior, batch),
        _ => {
            let dtype = infer_expression_dtype(expression, &batch.schema())?;
            Err(DataFrameError::type_mismatch(
                None::<String>,
                DataType::Utf8.to_string(),
                dtype.to_string(),
            ))
        }
    }
}

/// Incremental `DataFrame` result stream with one-way, repeatable terminal outcomes.
pub struct DataFrameStream {
    source: Option<Box<dyn BatchSource>>,
    schema: SchemaRef,
    terminal: StreamTerminalState,
    budget: ResourceBudget,
}

impl DataFrameStream {
    /// Open a source factory after a compiler has already reported `Supported` eligibility.
    pub fn from_factory(factory: &dyn BatchSourceFactory, options: StreamOptions) -> Result<Self> {
        let context = BatchOpenContext::new(options);
        let source = factory.open(context.clone())?;
        Ok(Self::from_opened_source(source, context.budget))
    }

    /// Construct a stream from a source tree that has already completed bounded preflight.
    pub(crate) fn from_opened_source(source: Box<dyn BatchSource>, budget: ResourceBudget) -> Self {
        let schema = source.schema();
        Self {
            source: Some(source),
            schema,
            terminal: StreamTerminalState::new(),
            budget,
        }
    }

    /// Return the source schema while the stream remains open or terminal.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Return the current lifecycle state.
    pub fn status(&self) -> StreamTerminal {
        self.terminal.status()
    }

    /// Return the shared budget used by this stream.
    pub fn budget(&self) -> &ResourceBudget {
        &self.budget
    }

    /// Yield the next finite `DataFrame` batch, or `None` after normal exhaustion.
    pub fn next_batch(&mut self) -> Result<Option<DataFrame>> {
        match self.terminal.status() {
            StreamTerminal::Open => {}
            StreamTerminal::Exhausted => return Ok(None),
            terminal => return Err(terminal.as_error().expect("terminal has stable error")),
        }

        let next = self
            .source
            .as_mut()
            .expect("open stream always owns a source")
            .next_batch();

        match next {
            Ok(Some(batch)) => match self.transfer_to_consumer(batch) {
                Ok(batch) => Ok(Some(batch)),
                Err(error) => Err(self.fail(error)),
            },
            Ok(None) => {
                if let Err(error) = self.close_source() {
                    return Err(self.fail(error));
                }
                self.terminal.finish(StreamTerminal::Exhausted);
                Ok(None)
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    /// Close an open stream. Repeated close calls are successful and never reopen a source.
    pub fn close(&mut self) -> Result<()> {
        match self.terminal.status() {
            StreamTerminal::Open => {
                if let Err(error) = self.close_source() {
                    return Err(self.fail(error));
                }
                self.terminal.finish(StreamTerminal::Closed);
                Ok(())
            }
            StreamTerminal::Exhausted | StreamTerminal::Closed => Ok(()),
            terminal => Err(terminal.as_error().expect("terminal has stable error")),
        }
    }

    /// Cancel an open stream. Repeated cancellation is successful and never reopens a source.
    pub fn cancel(&mut self) -> Result<()> {
        match self.terminal.status() {
            StreamTerminal::Open => {
                if let Err(error) = self.close_source() {
                    return Err(self.fail(error));
                }
                self.terminal.finish(StreamTerminal::Cancelled);
                Ok(())
            }
            StreamTerminal::Cancelled => Ok(()),
            StreamTerminal::Exhausted => Ok(()),
            terminal => Err(terminal.as_error().expect("terminal has stable error")),
        }
    }

    fn transfer_to_consumer(&self, batch: StreamBatch) -> Result<DataFrame> {
        let (batch, reservation) = batch.into_parts();
        // This one-batch Vec is the public DataFrame wrapper, never a preloaded source/result.
        let dataframe = DataFrame::from_batches(vec![batch]);
        drop(reservation);
        dataframe
    }

    fn close_source(&mut self) -> Result<()> {
        if let Some(mut source) = self.source.take() {
            source.close()?;
        }
        Ok(())
    }

    fn fail(&mut self, error: DataFrameError) -> DataFrameError {
        let failure = failure_for(&error);
        let _ = self.close_source();
        let terminal = self.terminal.finish(StreamTerminal::Failed(failure));
        terminal
            .as_error()
            .expect("failed terminal has a stable error")
    }
}

impl Drop for DataFrameStream {
    fn drop(&mut self) {
        if matches!(self.terminal.status(), StreamTerminal::Open) {
            let _ = self.close_source();
            self.terminal.finish(StreamTerminal::Closed);
        }
    }
}

fn failure_for(error: &DataFrameError) -> StreamFailure {
    match error {
        DataFrameError::ResourceLimitExceeded { .. } => {
            StreamFailure::new("resource_limit_exceeded", StreamFailureClass::ResourceLimit)
        }
        DataFrameError::Io { .. } => StreamFailure::new("source_error", StreamFailureClass::Source),
        DataFrameError::Arrow { .. } | DataFrameError::Parquet { .. } => {
            StreamFailure::new("decode_error", StreamFailureClass::Decode)
        }
        DataFrameError::SchemaMismatch { .. } => {
            StreamFailure::new("schema_error", StreamFailureClass::Schema)
        }
        DataFrameError::StreamingUnsupported { .. }
        | DataFrameError::StreamingRequiresMaterialization { .. } => {
            StreamFailure::new("streaming_unsupported", StreamFailureClass::Unsupported)
        }
        DataFrameError::StreamClosed => {
            StreamFailure::new("stream_closed", StreamFailureClass::Closed)
        }
        DataFrameError::StreamCancelled => {
            StreamFailure::new("stream_cancelled", StreamFailureClass::Cancelled)
        }
        DataFrameError::StreamFailed {
            code,
            classification,
        } => StreamFailure::new(code, *classification),
        _ => StreamFailure::new("stream_internal", StreamFailureClass::Internal),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use super::{BatchOpenContext, BatchSource, BatchSourceFactory, DataFrameStream, StreamBatch};
    use crate::physical::budget::{ResourceScope, StreamOptions, StreamTerminal};
    use crate::{DataFrameError, Result};

    struct TestFactory {
        opened: Cell<usize>,
        closed: Arc<AtomicUsize>,
        batches: usize,
    }

    impl BatchSourceFactory for TestFactory {
        fn source_name(&self) -> &'static str {
            "test"
        }

        fn schema(&self) -> Result<SchemaRef> {
            Ok(schema())
        }

        fn open(&self, context: BatchOpenContext) -> Result<Box<dyn BatchSource>> {
            self.opened.set(self.opened.get() + 1);
            Ok(Box::new(TestSource {
                context,
                closed: self.closed.clone(),
                remaining: self.batches,
            }))
        }
    }

    struct TestSource {
        context: BatchOpenContext,
        closed: Arc<AtomicUsize>,
        remaining: usize,
    }

    impl BatchSource for TestSource {
        fn schema(&self) -> SchemaRef {
            schema()
        }

        fn next_batch(&mut self) -> Result<Option<StreamBatch>> {
            if self.remaining == 0 {
                return Ok(None);
            }
            self.remaining -= 1;
            let batch = batch();
            let reservation = self.context.budget.reserve_batch(
                ResourceScope::Decode,
                super::super::executor::record_batch_memory_size(&batch),
            )?;
            Ok(Some(StreamBatch::new(batch, reservation)))
        }

        fn close(&mut self) -> Result<()> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .unwrap()
    }

    fn options() -> StreamOptions {
        StreamOptions::new(
            1024,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
    }

    #[test]
    fn stream_exhaustion_is_repeatable_and_releases_source() {
        let closed = Arc::new(AtomicUsize::new(0));
        let factory = TestFactory {
            opened: Cell::new(0),
            closed: closed.clone(),
            batches: 1,
        };
        let mut stream = DataFrameStream::from_factory(&factory, options()).unwrap();

        assert_eq!(factory.opened.get(), 1);
        assert_eq!(stream.next_batch().unwrap().unwrap().height(), 1);
        assert!(stream.next_batch().unwrap().is_none());
        assert!(stream.next_batch().unwrap().is_none());
        assert_eq!(stream.status(), StreamTerminal::Exhausted);
        assert_eq!(stream.schema().fields()[0].name(), "value");
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        assert_eq!(stream.budget().usage().reserved_bytes, 0);
    }

    #[test]
    fn cancel_is_repeatable_and_prevents_extra_batches() {
        let closed = Arc::new(AtomicUsize::new(0));
        let factory = TestFactory {
            opened: Cell::new(0),
            closed: closed.clone(),
            batches: 2,
        };
        let mut stream = DataFrameStream::from_factory(&factory, options()).unwrap();

        stream.cancel().unwrap();
        stream.cancel().unwrap();
        assert!(matches!(
            stream.next_batch(),
            Err(DataFrameError::StreamCancelled)
        ));
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }
}
