//! Atomic coordinator-side assembly for distributed SQL reads.
//!
//! Range workers never expose a client-visible stream directly. This module
//! accepts only already fenced range payloads, verifies that every planned
//! range has acknowledged cleanup, and then produces an immutable result.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use alopex_core::sql::stream::ByteSized;

use crate::catalog::ColumnMetadata;
use crate::executor::ColumnInfo;
use crate::executor::query::{
    aggregate::{create_accumulator, encode_group_key, merge_exact_aggregate_states},
    projected_columns,
};
use crate::planner::aggregate_expr::AggregateExpr;
use crate::planner::typed_expr::Projection;
use crate::storage::SqlValue;

const ASSEMBLER_ENTRY_OVERHEAD_BYTES: u64 = 32;

/// Bounds coordinator-owned materialization. Spill is intentionally not
/// implemented yet; a non-zero spill budget remains a declared upper bound,
/// never permission for an unbounded temporary file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistributedReadBudget {
    pub max_assembler_bytes: u64,
    pub max_spill_bytes: u64,
}

impl Default for DistributedReadBudget {
    fn default() -> Self {
        Self {
            max_assembler_bytes: 64 * 1024 * 1024,
            max_spill_bytes: 0,
        }
    }
}

/// One normalized global ORDER BY key. Values are produced by a worker's
/// closed descriptor; this type deliberately carries no executable SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalOrder {
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Final shaping shared by row and aggregate assembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultPresentation {
    pub columns: Vec<ColumnInfo>,
    pub distinct: bool,
    pub order: Vec<GlobalOrder>,
    /// Final-row column positions used by aggregate modes. Row mode instead
    /// receives precomputed keys so ORDER BY expressions need not be decoded.
    pub final_order_key_indexes: Vec<usize>,
    pub offset: u64,
    pub limit: Option<u64>,
}

impl ResultPresentation {
    /// Construct output metadata with the existing local projection kernel.
    pub fn from_projection(
        projection: &Projection,
        input_schema: &[ColumnMetadata],
        distinct: bool,
        order: Vec<GlobalOrder>,
        final_order_key_indexes: Vec<usize>,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Self, DistributedReadAssemblyError> {
        Ok(Self {
            columns: projected_columns(projection, input_schema)
                .map_err(|error| DistributedReadAssemblyError::Projection(error.to_string()))?,
            distinct,
            order,
            final_order_key_indexes,
            offset,
            limit,
        })
    }
}

/// Row-mode assembly for already projected range rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowMergePlan {
    pub presentation: ResultPresentation,
}

/// Aggregate assembly whose worker states can be merged exactly.
#[derive(Debug, Clone)]
pub struct ExactAggregatePlan {
    pub presentation: ResultPresentation,
    pub group_column_count: usize,
    pub aggregates: Vec<AggregateExpr>,
}

/// Aggregate assembly that must replay globally ordered inputs to preserve
/// local floating-point, DISTINCT, and string-aggregate semantics.
#[derive(Debug, Clone)]
pub struct OrderedAggregatePlan {
    pub presentation: ResultPresentation,
    pub group_column_count: usize,
    pub aggregates: Vec<AggregateExpr>,
    pub logical_input_order: Vec<GlobalOrder>,
}

/// Closed assembly mode selected by the SQL catalog/coordinator.
#[derive(Debug, Clone)]
pub enum AssemblyPlan {
    Rows(RowMergePlan),
    ExactAggregates(ExactAggregatePlan),
    OrderedAggregates(OrderedAggregatePlan),
}

/// One final-projection row emitted by a range worker during preparation.
#[derive(Debug, Clone, PartialEq)]
pub struct AssemblerRow {
    pub values: Vec<SqlValue>,
    pub order_keys: Vec<SqlValue>,
    /// Stable physical row key used only to break otherwise equal global keys.
    pub row_key: u64,
}

/// One group-local vector of exact accumulator states.
#[derive(Debug, Clone, PartialEq)]
pub struct ExactAggregatePartial {
    pub group_key: Vec<SqlValue>,
    /// One serializable state vector per aggregate expression.
    pub states: Vec<Vec<SqlValue>>,
}

/// One raw aggregate input. The worker has already evaluated the closed
/// descriptor's aggregate arguments; the coordinator only replays values.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderedAggregateInput {
    pub group_key: Vec<SqlValue>,
    pub aggregate_arguments: Vec<Option<SqlValue>>,
    pub logical_order_keys: Vec<SqlValue>,
    pub row_key: u64,
}

/// Typed payloads accepted from a completed range session.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeAssemblerPayload {
    Rows(Vec<AssemblerRow>),
    ExactAggregatePartial(ExactAggregatePartial),
    OrderedAggregateInput(OrderedAggregateInput),
}

/// Worker terminal state supplied by the transport coordinator. A completed
/// range is usable only when it explicitly acknowledges cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeTerminal {
    Pending,
    Completed { cleanup_acknowledged: bool },
    Failed { reason: String },
}

/// All preparation payloads and terminal evidence for one planned range.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeAssemblerInput {
    pub range_id: String,
    /// Output schema for row payloads. Aggregate payload schemas are defined
    /// by the assembly plan because partial states are not public rows.
    pub columns: Vec<ColumnInfo>,
    pub payloads: Vec<RangeAssemblerPayload>,
    pub terminal: RangeTerminal,
}

/// Observable lifecycle state. A failed preparation never retains a prepared
/// result and therefore cannot be opened as a row stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssemblerTerminalStatus {
    Pending,
    Prepared,
    Failed,
    Closed,
}

/// Classified preparation failure. No variant carries partial SQL rows.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DistributedReadAssemblyError {
    #[error("distributed assembler budget must be non-zero")]
    ZeroAssemblerBudget,
    #[error("distributed assembler requires at least one planned range")]
    EmptyRangeSet,
    #[error("range '{range_id}' is outside the immutable read fence")]
    UnexpectedRange { range_id: String },
    #[error("range '{range_id}' was supplied more than once")]
    DuplicateRange { range_id: String },
    #[error("range '{range_id}' did not reach a terminal End")]
    MissingRangeEnd { range_id: String },
    #[error("range '{range_id}' reached End without cleanup acknowledgement")]
    CleanupNotAcknowledged { range_id: String },
    #[error("range '{range_id}' failed during preparation: {reason}")]
    RangeFailed { range_id: String, reason: String },
    #[error("range '{range_id}' output schema differs from the planned result schema")]
    SchemaMismatch { range_id: String },
    #[error("range '{range_id}' supplied an incompatible {payload} payload")]
    PayloadMismatch {
        range_id: String,
        payload: &'static str,
    },
    #[error("range '{range_id}' supplied {actual} values where {expected} were required")]
    ValueArity {
        range_id: String,
        expected: usize,
        actual: usize,
    },
    #[error("range '{range_id}' supplied {actual} order keys where {expected} were required")]
    OrderKeyArity {
        range_id: String,
        expected: usize,
        actual: usize,
    },
    #[error(
        "distributed assembler requires {required_bytes} bytes, exceeding the {max_assembler_bytes}-byte memory budget (spill limit {max_spill_bytes} bytes)"
    )]
    ResourceLimitExceeded {
        required_bytes: u64,
        max_assembler_bytes: u64,
        max_spill_bytes: u64,
    },
    #[error("distributed projection metadata could not be built: {0}")]
    Projection(String),
    #[error("distributed aggregate assembly failed: {0}")]
    Aggregate(String),
    #[error("the assembler is not prepared")]
    NotPrepared,
    #[error("the assembler has already been closed")]
    Closed,
    #[error("the assembler has already terminated with a failure")]
    Failed,
}

/// Immutable result made visible only after every range has cleaned up.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedResult {
    columns: Arc<[ColumnInfo]>,
    rows: Arc<[Vec<SqlValue>]>,
}

impl PreparedResult {
    fn new(columns: Vec<ColumnInfo>, rows: Vec<Vec<SqlValue>>) -> Self {
        Self {
            columns: columns.into(),
            rows: rows.into(),
        }
    }

    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Open an independent read-only stream over the immutable result.
    pub fn open_prepared_stream(&self) -> PreparedResultStream {
        PreparedResultStream {
            result: self.clone(),
            index: 0,
        }
    }

    pub fn query_result(&self) -> crate::executor::QueryResult {
        crate::executor::QueryResult::new(self.columns.to_vec(), self.rows.to_vec())
    }
}

/// Reader for a [`PreparedResult`]. It has no transport/session dependency.
pub struct PreparedResultStream {
    result: PreparedResult,
    index: usize,
}

impl PreparedResultStream {
    pub fn next_row(&mut self) -> Option<Vec<SqlValue>> {
        let row = self.result.rows.get(self.index)?.clone();
        self.index += 1;
        Some(row)
    }
}

/// Coordinator-owned atomic result assembler.
pub struct GlobalResultAssembler {
    plan: AssemblyPlan,
    budget: DistributedReadBudget,
    expected_ranges: BTreeSet<String>,
    ranges: BTreeMap<String, RangeAssemblerInput>,
    prepared: Option<PreparedResult>,
    status: AssemblerTerminalStatus,
}

impl GlobalResultAssembler {
    pub fn new(
        expected_ranges: impl IntoIterator<Item = String>,
        plan: AssemblyPlan,
        budget: DistributedReadBudget,
    ) -> Result<Self, DistributedReadAssemblyError> {
        if budget.max_assembler_bytes == 0 {
            return Err(DistributedReadAssemblyError::ZeroAssemblerBudget);
        }
        let expected_ranges = expected_ranges.into_iter().collect::<BTreeSet<_>>();
        if expected_ranges.is_empty() {
            return Err(DistributedReadAssemblyError::EmptyRangeSet);
        }
        Ok(Self {
            plan,
            budget,
            expected_ranges,
            ranges: BTreeMap::new(),
            prepared: None,
            status: AssemblerTerminalStatus::Pending,
        })
    }

    /// Register all buffered payloads for one range. Callers must not open a
    /// client stream before [`Self::prepare`] returns successfully.
    pub fn push_range(
        &mut self,
        input: RangeAssemblerInput,
    ) -> Result<(), DistributedReadAssemblyError> {
        self.ensure_pending()?;
        if !self.expected_ranges.contains(&input.range_id) {
            return Err(DistributedReadAssemblyError::UnexpectedRange {
                range_id: input.range_id,
            });
        }
        if self.ranges.contains_key(&input.range_id) {
            return Err(DistributedReadAssemblyError::DuplicateRange {
                range_id: input.range_id,
            });
        }
        self.ranges.insert(input.range_id.clone(), input);
        Ok(())
    }

    /// Validate all terminal acknowledgements and atomically materialize the
    /// final result. On every error, coordinator-owned payloads are discarded.
    pub fn prepare(&mut self) -> Result<PreparedResult, DistributedReadAssemblyError> {
        self.ensure_pending()?;
        let ranges = std::mem::take(&mut self.ranges);
        let outcome = self.prepare_from_ranges(ranges);
        match outcome {
            Ok(result) => {
                self.status = AssemblerTerminalStatus::Prepared;
                self.prepared = Some(result.clone());
                Ok(result)
            }
            Err(error) => {
                self.status = AssemblerTerminalStatus::Failed;
                self.prepared = None;
                Err(error)
            }
        }
    }

    pub fn open_prepared_stream(
        &self,
    ) -> Result<PreparedResultStream, DistributedReadAssemblyError> {
        self.prepared
            .as_ref()
            .map(PreparedResult::open_prepared_stream)
            .ok_or(DistributedReadAssemblyError::NotPrepared)
    }

    pub fn terminal_status(&self) -> AssemblerTerminalStatus {
        self.status
    }

    /// Idempotently discard preparation and prepared-result state.
    pub fn close(&mut self) {
        self.ranges.clear();
        self.prepared = None;
        self.status = AssemblerTerminalStatus::Closed;
    }

    fn ensure_pending(&self) -> Result<(), DistributedReadAssemblyError> {
        match self.status {
            AssemblerTerminalStatus::Pending => Ok(()),
            AssemblerTerminalStatus::Closed => Err(DistributedReadAssemblyError::Closed),
            AssemblerTerminalStatus::Failed => Err(DistributedReadAssemblyError::Failed),
            AssemblerTerminalStatus::Prepared => Err(DistributedReadAssemblyError::NotPrepared),
        }
    }

    fn prepare_from_ranges(
        &self,
        ranges: BTreeMap<String, RangeAssemblerInput>,
    ) -> Result<PreparedResult, DistributedReadAssemblyError> {
        self.validate_range_terminals(&ranges)?;
        let mut budget = BudgetAccount::new(self.budget);
        match &self.plan {
            AssemblyPlan::Rows(plan) => prepare_rows(ranges, plan, &mut budget),
            AssemblyPlan::ExactAggregates(plan) => {
                prepare_exact_aggregates(ranges, plan, &mut budget)
            }
            AssemblyPlan::OrderedAggregates(plan) => {
                prepare_ordered_aggregates(ranges, plan, &mut budget)
            }
        }
    }

    fn validate_range_terminals(
        &self,
        ranges: &BTreeMap<String, RangeAssemblerInput>,
    ) -> Result<(), DistributedReadAssemblyError> {
        for range_id in &self.expected_ranges {
            let input = ranges.get(range_id).ok_or_else(|| {
                DistributedReadAssemblyError::MissingRangeEnd {
                    range_id: range_id.clone(),
                }
            })?;
            match &input.terminal {
                RangeTerminal::Pending => {
                    return Err(DistributedReadAssemblyError::MissingRangeEnd {
                        range_id: range_id.clone(),
                    });
                }
                RangeTerminal::Completed {
                    cleanup_acknowledged: false,
                } => {
                    return Err(DistributedReadAssemblyError::CleanupNotAcknowledged {
                        range_id: range_id.clone(),
                    });
                }
                RangeTerminal::Completed {
                    cleanup_acknowledged: true,
                } => {}
                RangeTerminal::Failed { reason } => {
                    return Err(DistributedReadAssemblyError::RangeFailed {
                        range_id: range_id.clone(),
                        reason: reason.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

struct BudgetAccount {
    budget: DistributedReadBudget,
    used_bytes: u64,
}

impl BudgetAccount {
    fn new(budget: DistributedReadBudget) -> Self {
        Self {
            budget,
            used_bytes: 0,
        }
    }

    fn reserve_values(
        &mut self,
        values: impl IntoIterator<Item = SqlValue>,
    ) -> Result<(), DistributedReadAssemblyError> {
        let bytes = values
            .into_iter()
            .map(|value| value.estimated_bytes())
            .sum::<u64>();
        self.reserve(bytes)
    }

    fn reserve(&mut self, bytes: u64) -> Result<(), DistributedReadAssemblyError> {
        let required_bytes = self.used_bytes.saturating_add(bytes);
        if required_bytes > self.budget.max_assembler_bytes {
            return Err(DistributedReadAssemblyError::ResourceLimitExceeded {
                required_bytes,
                max_assembler_bytes: self.budget.max_assembler_bytes,
                max_spill_bytes: self.budget.max_spill_bytes,
            });
        }
        self.used_bytes = required_bytes;
        Ok(())
    }
}

#[derive(Debug)]
struct RankedRow {
    values: Vec<SqlValue>,
    order_keys: Vec<SqlValue>,
    range_id: String,
    row_key: u64,
}

fn prepare_rows(
    ranges: BTreeMap<String, RangeAssemblerInput>,
    plan: &RowMergePlan,
    budget: &mut BudgetAccount,
) -> Result<PreparedResult, DistributedReadAssemblyError> {
    let mut rows = Vec::new();
    for (range_id, input) in ranges {
        if input.columns != plan.presentation.columns {
            return Err(DistributedReadAssemblyError::SchemaMismatch { range_id });
        }
        for payload in input.payloads {
            let RangeAssemblerPayload::Rows(payload_rows) = payload else {
                return Err(DistributedReadAssemblyError::PayloadMismatch {
                    range_id,
                    payload: "non-row",
                });
            };
            for row in payload_rows {
                validate_row(&range_id, &row, &plan.presentation)?;
                budget.reserve_values(row.values.iter().chain(row.order_keys.iter()).cloned())?;
                budget.reserve(ASSEMBLER_ENTRY_OVERHEAD_BYTES)?;
                rows.push(RankedRow {
                    values: row.values,
                    order_keys: row.order_keys,
                    range_id: range_id.clone(),
                    row_key: row.row_key,
                });
            }
        }
    }
    materialize_presentation(plan.presentation.clone(), rows, budget)
}

fn prepare_exact_aggregates(
    ranges: BTreeMap<String, RangeAssemblerInput>,
    plan: &ExactAggregatePlan,
    budget: &mut BudgetAccount,
) -> Result<PreparedResult, DistributedReadAssemblyError> {
    let mut groups = BTreeMap::<Vec<u8>, ExactGroup>::new();
    for (range_id, input) in ranges {
        for payload in input.payloads {
            let RangeAssemblerPayload::ExactAggregatePartial(partial) = payload else {
                return Err(DistributedReadAssemblyError::PayloadMismatch {
                    range_id,
                    payload: "non-exact-aggregate",
                });
            };
            validate_exact_partial(&range_id, &partial, plan)?;
            budget.reserve_values(
                partial
                    .group_key
                    .iter()
                    .chain(partial.states.iter().flatten())
                    .cloned(),
            )?;
            budget.reserve(ASSEMBLER_ENTRY_OVERHEAD_BYTES)?;
            let encoded_group = encode_group_key(&partial.group_key)
                .map_err(|error| DistributedReadAssemblyError::Aggregate(error.to_string()))?;
            let group = groups.entry(encoded_group).or_insert_with(|| ExactGroup {
                values: partial.group_key.clone(),
                state_rows: Vec::new(),
            });
            group.state_rows.push(partial.states);
        }
    }
    if groups.is_empty() && plan.group_column_count == 0 {
        groups.insert(
            Vec::new(),
            ExactGroup {
                values: Vec::new(),
                state_rows: Vec::new(),
            },
        );
    }

    let mut rows = Vec::with_capacity(groups.len());
    for (_, group) in groups {
        let aggregate_values = merge_exact_aggregate_states(&plan.aggregates, group.state_rows)
            .map_err(|error| DistributedReadAssemblyError::Aggregate(error.to_string()))?;
        let mut values = group.values;
        values.extend(aggregate_values);
        validate_final_width(&values, &plan.presentation)?;
        let order_keys = final_order_keys(&values, &plan.presentation)?;
        budget.reserve_values(values.iter().chain(order_keys.iter()).cloned())?;
        budget.reserve(ASSEMBLER_ENTRY_OVERHEAD_BYTES)?;
        rows.push(RankedRow {
            values,
            order_keys,
            range_id: String::new(),
            row_key: 0,
        });
    }
    materialize_presentation(plan.presentation.clone(), rows, budget)
}

fn prepare_ordered_aggregates(
    ranges: BTreeMap<String, RangeAssemblerInput>,
    plan: &OrderedAggregatePlan,
    budget: &mut BudgetAccount,
) -> Result<PreparedResult, DistributedReadAssemblyError> {
    let mut inputs = Vec::new();
    for (range_id, input) in ranges {
        for payload in input.payloads {
            let RangeAssemblerPayload::OrderedAggregateInput(payload) = payload else {
                return Err(DistributedReadAssemblyError::PayloadMismatch {
                    range_id,
                    payload: "non-ordered-aggregate",
                });
            };
            validate_ordered_input(&range_id, &payload, plan)?;
            budget.reserve_values(
                payload
                    .group_key
                    .iter()
                    .chain(payload.aggregate_arguments.iter().flatten())
                    .chain(payload.logical_order_keys.iter())
                    .cloned(),
            )?;
            budget.reserve(ASSEMBLER_ENTRY_OVERHEAD_BYTES)?;
            let encoded_group = encode_group_key(&payload.group_key)
                .map_err(|error| DistributedReadAssemblyError::Aggregate(error.to_string()))?;
            inputs.push(RankedOrderedInput {
                input: payload,
                range_id: range_id.clone(),
                encoded_group,
            });
        }
    }
    inputs.sort_by(|left, right| compare_ordered_input(left, right, plan));

    let mut groups = BTreeMap::<Vec<u8>, OrderedGroup>::new();
    for ranked in inputs {
        let group = groups
            .entry(ranked.encoded_group)
            .or_insert_with(|| OrderedGroup {
                values: ranked.input.group_key.clone(),
                accumulators: plan
                    .aggregates
                    .iter()
                    .map(|aggregate| create_accumulator(&aggregate.function, aggregate.distinct))
                    .collect(),
            });
        for (accumulator, value) in group
            .accumulators
            .iter_mut()
            .zip(ranked.input.aggregate_arguments)
        {
            accumulator
                .update(value)
                .map_err(|error| DistributedReadAssemblyError::Aggregate(error.to_string()))?;
        }
    }
    if groups.is_empty() && plan.group_column_count == 0 {
        groups.insert(
            Vec::new(),
            OrderedGroup {
                values: Vec::new(),
                accumulators: plan
                    .aggregates
                    .iter()
                    .map(|aggregate| create_accumulator(&aggregate.function, aggregate.distinct))
                    .collect(),
            },
        );
    }

    let mut rows = Vec::with_capacity(groups.len());
    for (_, group) in groups {
        let mut values = group.values;
        let aggregate_values = group
            .accumulators
            .iter()
            .map(|accumulator| accumulator.finalize())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| DistributedReadAssemblyError::Aggregate(error.to_string()))?;
        values.extend(aggregate_values);
        validate_final_width(&values, &plan.presentation)?;
        let order_keys = final_order_keys(&values, &plan.presentation)?;
        budget.reserve_values(values.iter().chain(order_keys.iter()).cloned())?;
        budget.reserve(ASSEMBLER_ENTRY_OVERHEAD_BYTES)?;
        rows.push(RankedRow {
            values,
            order_keys,
            range_id: String::new(),
            row_key: 0,
        });
    }
    materialize_presentation(plan.presentation.clone(), rows, budget)
}

fn validate_row(
    range_id: &str,
    row: &AssemblerRow,
    presentation: &ResultPresentation,
) -> Result<(), DistributedReadAssemblyError> {
    validate_final_width(&row.values, presentation).map_err(|_| {
        DistributedReadAssemblyError::ValueArity {
            range_id: range_id.to_string(),
            expected: presentation.columns.len(),
            actual: row.values.len(),
        }
    })?;
    if row.order_keys.len() != presentation.order.len() {
        return Err(DistributedReadAssemblyError::OrderKeyArity {
            range_id: range_id.to_string(),
            expected: presentation.order.len(),
            actual: row.order_keys.len(),
        });
    }
    Ok(())
}

fn validate_exact_partial(
    range_id: &str,
    partial: &ExactAggregatePartial,
    plan: &ExactAggregatePlan,
) -> Result<(), DistributedReadAssemblyError> {
    if partial.group_key.len() != plan.group_column_count {
        return Err(DistributedReadAssemblyError::ValueArity {
            range_id: range_id.to_string(),
            expected: plan.group_column_count,
            actual: partial.group_key.len(),
        });
    }
    if partial.states.len() != plan.aggregates.len() {
        return Err(DistributedReadAssemblyError::ValueArity {
            range_id: range_id.to_string(),
            expected: plan.aggregates.len(),
            actual: partial.states.len(),
        });
    }
    Ok(())
}

fn validate_ordered_input(
    range_id: &str,
    input: &OrderedAggregateInput,
    plan: &OrderedAggregatePlan,
) -> Result<(), DistributedReadAssemblyError> {
    if input.group_key.len() != plan.group_column_count {
        return Err(DistributedReadAssemblyError::ValueArity {
            range_id: range_id.to_string(),
            expected: plan.group_column_count,
            actual: input.group_key.len(),
        });
    }
    if input.aggregate_arguments.len() != plan.aggregates.len() {
        return Err(DistributedReadAssemblyError::ValueArity {
            range_id: range_id.to_string(),
            expected: plan.aggregates.len(),
            actual: input.aggregate_arguments.len(),
        });
    }
    if input.logical_order_keys.len() != plan.logical_input_order.len() {
        return Err(DistributedReadAssemblyError::OrderKeyArity {
            range_id: range_id.to_string(),
            expected: plan.logical_input_order.len(),
            actual: input.logical_order_keys.len(),
        });
    }
    Ok(())
}

fn validate_final_width(
    values: &[SqlValue],
    presentation: &ResultPresentation,
) -> Result<(), DistributedReadAssemblyError> {
    if values.len() == presentation.columns.len() {
        Ok(())
    } else {
        Err(DistributedReadAssemblyError::Aggregate(format!(
            "final row has {} value(s), expected {}",
            values.len(),
            presentation.columns.len()
        )))
    }
}

fn materialize_presentation(
    presentation: ResultPresentation,
    mut rows: Vec<RankedRow>,
    budget: &mut BudgetAccount,
) -> Result<PreparedResult, DistributedReadAssemblyError> {
    if presentation.distinct {
        let mut seen = HashSet::new();
        let mut distinct_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let key = encode_group_key(&row.values)
                .map_err(|error| DistributedReadAssemblyError::Aggregate(error.to_string()))?;
            budget.reserve(key.len() as u64 + ASSEMBLER_ENTRY_OVERHEAD_BYTES)?;
            if seen.insert(key) {
                distinct_rows.push(row);
            }
        }
        rows = distinct_rows;
    }
    rows.sort_by(|left, right| compare_ranked_rows(left, right, &presentation.order));
    let start = usize::try_from(presentation.offset).unwrap_or(usize::MAX);
    let limit = presentation
        .limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
    let rows = rows
        .into_iter()
        .skip(start)
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| row.values)
        .collect();
    Ok(PreparedResult::new(presentation.columns, rows))
}

fn final_order_keys(
    values: &[SqlValue],
    presentation: &ResultPresentation,
) -> Result<Vec<SqlValue>, DistributedReadAssemblyError> {
    if presentation.order.is_empty() {
        return Ok(Vec::new());
    }
    if presentation.final_order_key_indexes.len() != presentation.order.len() {
        return Err(DistributedReadAssemblyError::Aggregate(
            "aggregate final ordering requires one output-column index per ORDER BY key".into(),
        ));
    }
    presentation
        .final_order_key_indexes
        .iter()
        .map(|index| {
            values.get(*index).cloned().ok_or_else(|| {
                DistributedReadAssemblyError::Aggregate(format!(
                    "aggregate final ORDER BY index {index} is outside the result row"
                ))
            })
        })
        .collect()
}

fn compare_ranked_rows(left: &RankedRow, right: &RankedRow, order: &[GlobalOrder]) -> Ordering {
    compare_order_keys(&left.order_keys, &right.order_keys, order)
        .then_with(|| left.range_id.cmp(&right.range_id))
        .then_with(|| left.row_key.cmp(&right.row_key))
}

struct ExactGroup {
    values: Vec<SqlValue>,
    state_rows: Vec<Vec<Vec<SqlValue>>>,
}

struct RankedOrderedInput {
    input: OrderedAggregateInput,
    range_id: String,
    encoded_group: Vec<u8>,
}

struct OrderedGroup {
    values: Vec<SqlValue>,
    accumulators: Vec<Box<dyn crate::executor::query::aggregate::Accumulator>>,
}

fn compare_ordered_input(
    left: &RankedOrderedInput,
    right: &RankedOrderedInput,
    plan: &OrderedAggregatePlan,
) -> Ordering {
    left.encoded_group
        .cmp(&right.encoded_group)
        .then_with(|| {
            compare_order_keys(
                &left.input.logical_order_keys,
                &right.input.logical_order_keys,
                &plan.logical_input_order,
            )
        })
        .then_with(|| left.range_id.cmp(&right.range_id))
        .then_with(|| left.input.row_key.cmp(&right.input.row_key))
}

fn compare_order_keys(left: &[SqlValue], right: &[SqlValue], order: &[GlobalOrder]) -> Ordering {
    for ((left, right), order) in left.iter().zip(right).zip(order) {
        let comparison = compare_sql_value(left, right, *order);
        if comparison != Ordering::Equal {
            return comparison;
        }
    }
    Ordering::Equal
}

fn compare_sql_value(left: &SqlValue, right: &SqlValue, order: GlobalOrder) -> Ordering {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => {
            if order.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValue::Null) => {
            if order.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => match left.partial_cmp(right).unwrap_or(Ordering::Equal) {
            Ordering::Equal => Ordering::Equal,
            comparison if order.ascending => comparison,
            comparison => comparison.reverse(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::query::aggregate::create_accumulator;
    use crate::planner::ResolvedType;
    use crate::planner::aggregate_expr::AggregateFunction;

    fn columns(names: &[(&str, ResolvedType)]) -> Vec<ColumnInfo> {
        names
            .iter()
            .map(|(name, data_type)| ColumnInfo::new(*name, data_type.clone()))
            .collect()
    }

    fn presentation(
        columns: Vec<ColumnInfo>,
        distinct: bool,
        order: Vec<GlobalOrder>,
        final_order_key_indexes: Vec<usize>,
        offset: u64,
        limit: Option<u64>,
    ) -> ResultPresentation {
        ResultPresentation {
            columns,
            distinct,
            order,
            final_order_key_indexes,
            offset,
            limit,
        }
    }

    fn completed_range(
        range_id: &str,
        columns: Vec<ColumnInfo>,
        payloads: Vec<RangeAssemblerPayload>,
    ) -> RangeAssemblerInput {
        RangeAssemblerInput {
            range_id: range_id.into(),
            columns,
            payloads,
            terminal: RangeTerminal::Completed {
                cleanup_acknowledged: true,
            },
        }
    }

    fn row(value: &str, key: i32, row_key: u64) -> AssemblerRow {
        AssemblerRow {
            values: vec![SqlValue::Text(value.into())],
            order_keys: vec![SqlValue::Integer(key)],
            row_key,
        }
    }

    #[test]
    fn rows_remain_private_until_every_range_cleanup_acknowledges() {
        let output_columns = columns(&[("name", ResolvedType::Text)]);
        let plan = AssemblyPlan::Rows(RowMergePlan {
            presentation: presentation(output_columns.clone(), false, vec![], vec![], 0, None),
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into(), "range-b".into()],
            plan,
            DistributedReadBudget::default(),
        )
        .unwrap();
        assembler
            .push_range(completed_range(
                "range-a",
                output_columns.clone(),
                vec![RangeAssemblerPayload::Rows(vec![AssemblerRow {
                    values: vec![SqlValue::Text("private".into())],
                    order_keys: vec![],
                    row_key: 1,
                }])],
            ))
            .unwrap();
        assembler
            .push_range(RangeAssemblerInput {
                range_id: "range-b".into(),
                columns: output_columns,
                payloads: vec![],
                terminal: RangeTerminal::Completed {
                    cleanup_acknowledged: false,
                },
            })
            .unwrap();

        assert!(matches!(
            assembler.prepare(),
            Err(DistributedReadAssemblyError::CleanupNotAcknowledged { range_id }) if range_id == "range-b"
        ));
        assert_eq!(assembler.terminal_status(), AssemblerTerminalStatus::Failed);
        assert!(matches!(
            assembler.open_prepared_stream(),
            Err(DistributedReadAssemblyError::NotPrepared)
        ));
    }

    #[test]
    fn row_merge_applies_global_distinct_order_offset_and_limit_before_opening_stream() {
        let output_columns = columns(&[("name", ResolvedType::Text)]);
        let plan = AssemblyPlan::Rows(RowMergePlan {
            presentation: presentation(
                output_columns.clone(),
                true,
                vec![GlobalOrder {
                    ascending: true,
                    nulls_first: false,
                }],
                vec![],
                1,
                Some(1),
            ),
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into(), "range-b".into()],
            plan,
            DistributedReadBudget::default(),
        )
        .unwrap();
        assembler
            .push_range(completed_range(
                "range-a",
                output_columns.clone(),
                vec![RangeAssemblerPayload::Rows(vec![
                    row("charlie", 3, 1),
                    row("alpha", 1, 2),
                ])],
            ))
            .unwrap();
        assembler
            .push_range(completed_range(
                "range-b",
                output_columns,
                vec![RangeAssemblerPayload::Rows(vec![
                    row("alpha", 1, 3),
                    row("bravo", 2, 4),
                ])],
            ))
            .unwrap();

        let prepared = assembler.prepare().unwrap();
        assert_eq!(prepared.row_count(), 1);
        let mut stream = prepared.open_prepared_stream();
        assert_eq!(
            stream.next_row(),
            Some(vec![SqlValue::Text("bravo".into())])
        );
        assert_eq!(stream.next_row(), None);
    }

    #[test]
    fn exact_partial_merge_is_limited_to_proven_accumulators() {
        let aggregate = AggregateExpr::count_star();
        let output_columns = columns(&[("count", ResolvedType::BigInt)]);
        let plan = AssemblyPlan::ExactAggregates(ExactAggregatePlan {
            presentation: presentation(output_columns.clone(), false, vec![], vec![], 0, None),
            group_column_count: 0,
            aggregates: vec![aggregate],
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into(), "range-b".into()],
            plan,
            DistributedReadBudget::default(),
        )
        .unwrap();
        for (range_id, count) in [("range-a", 2), ("range-b", 3)] {
            let mut accumulator = create_accumulator(&AggregateFunction::Count, false);
            for _ in 0..count {
                accumulator.update(None).unwrap();
            }
            assembler
                .push_range(completed_range(
                    range_id,
                    vec![],
                    vec![RangeAssemblerPayload::ExactAggregatePartial(
                        ExactAggregatePartial {
                            group_key: vec![],
                            states: vec![accumulator.state().unwrap()],
                        },
                    )],
                ))
                .unwrap();
        }

        assert_eq!(
            assembler.prepare().unwrap().query_result().rows,
            vec![vec![SqlValue::BigInt(5)]]
        );
    }

    #[test]
    fn aggregate_results_apply_global_order_and_limit_after_all_range_merges() {
        let aggregate = AggregateExpr::count_star();
        let output_columns = columns(&[
            ("team", ResolvedType::Text),
            ("count", ResolvedType::BigInt),
        ]);
        let plan = AssemblyPlan::ExactAggregates(ExactAggregatePlan {
            presentation: presentation(
                output_columns.clone(),
                false,
                vec![GlobalOrder {
                    ascending: false,
                    nulls_first: false,
                }],
                vec![1],
                0,
                Some(1),
            ),
            group_column_count: 1,
            aggregates: vec![aggregate],
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into(), "range-b".into()],
            plan,
            DistributedReadBudget::default(),
        )
        .unwrap();
        for (range_id, team, count) in [("range-a", "alpha", 1), ("range-b", "beta", 3)] {
            let mut accumulator = create_accumulator(&AggregateFunction::Count, false);
            for _ in 0..count {
                accumulator.update(None).unwrap();
            }
            assembler
                .push_range(completed_range(
                    range_id,
                    output_columns.clone(),
                    vec![RangeAssemblerPayload::ExactAggregatePartial(
                        ExactAggregatePartial {
                            group_key: vec![SqlValue::Text(team.into())],
                            states: vec![accumulator.state().unwrap()],
                        },
                    )],
                ))
                .unwrap();
        }

        assert_eq!(
            assembler.prepare().unwrap().query_result().rows,
            vec![vec![SqlValue::Text("beta".into()), SqlValue::BigInt(3)]]
        );
    }

    #[test]
    fn ordered_inputs_replay_distinct_string_aggregates_in_global_order() {
        let aggregate = AggregateExpr {
            function: AggregateFunction::GroupConcat {
                separator: Some("|".into()),
            },
            arg: None,
            distinct: true,
            result_type: ResolvedType::Text,
        };
        let output_columns = columns(&[("names", ResolvedType::Text)]);
        let plan = AssemblyPlan::OrderedAggregates(OrderedAggregatePlan {
            presentation: presentation(output_columns.clone(), false, vec![], vec![], 0, None),
            group_column_count: 0,
            aggregates: vec![aggregate],
            logical_input_order: vec![GlobalOrder {
                ascending: true,
                nulls_first: false,
            }],
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into(), "range-b".into()],
            plan,
            DistributedReadBudget::default(),
        )
        .unwrap();
        assembler
            .push_range(completed_range(
                "range-a",
                output_columns.clone(),
                vec![
                    RangeAssemblerPayload::OrderedAggregateInput(OrderedAggregateInput {
                        group_key: vec![],
                        aggregate_arguments: vec![Some(SqlValue::Text("b".into()))],
                        logical_order_keys: vec![SqlValue::Integer(2)],
                        row_key: 1,
                    }),
                    RangeAssemblerPayload::OrderedAggregateInput(OrderedAggregateInput {
                        group_key: vec![],
                        aggregate_arguments: vec![Some(SqlValue::Text("a".into()))],
                        logical_order_keys: vec![SqlValue::Integer(1)],
                        row_key: 2,
                    }),
                ],
            ))
            .unwrap();
        assembler
            .push_range(completed_range(
                "range-b",
                output_columns,
                vec![RangeAssemblerPayload::OrderedAggregateInput(
                    OrderedAggregateInput {
                        group_key: vec![],
                        aggregate_arguments: vec![Some(SqlValue::Text("b".into()))],
                        logical_order_keys: vec![SqlValue::Integer(3)],
                        row_key: 3,
                    },
                )],
            ))
            .unwrap();

        assert_eq!(
            assembler.prepare().unwrap().query_result().rows,
            vec![vec![SqlValue::Text("a|b".into())]]
        );
    }

    #[test]
    fn unproven_exact_partial_form_is_rejected_without_a_prepared_result() {
        let aggregate = AggregateExpr {
            function: AggregateFunction::Sum,
            arg: None,
            distinct: false,
            result_type: ResolvedType::Double,
        };
        let output_columns = columns(&[("sum", ResolvedType::Double)]);
        let plan = AssemblyPlan::ExactAggregates(ExactAggregatePlan {
            presentation: presentation(output_columns, false, vec![], vec![], 0, None),
            group_column_count: 0,
            aggregates: vec![aggregate],
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into()],
            plan,
            DistributedReadBudget::default(),
        )
        .unwrap();
        assembler
            .push_range(completed_range("range-a", vec![], vec![]))
            .unwrap();

        assert!(matches!(
            assembler.prepare(),
            Err(DistributedReadAssemblyError::Aggregate(reason)) if reason.contains("requires ordered input replay")
        ));
        assert!(matches!(
            assembler.open_prepared_stream(),
            Err(DistributedReadAssemblyError::NotPrepared)
        ));
    }

    #[test]
    fn budget_exhaustion_exposes_no_prepared_rows() {
        let output_columns = columns(&[("name", ResolvedType::Text)]);
        let plan = AssemblyPlan::Rows(RowMergePlan {
            presentation: presentation(output_columns.clone(), false, vec![], vec![], 0, None),
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into()],
            plan,
            DistributedReadBudget {
                max_assembler_bytes: 3,
                max_spill_bytes: 0,
            },
        )
        .unwrap();
        assembler
            .push_range(completed_range(
                "range-a",
                output_columns,
                vec![RangeAssemblerPayload::Rows(vec![AssemblerRow {
                    values: vec![SqlValue::Text("four".into())],
                    order_keys: vec![],
                    row_key: 1,
                }])],
            ))
            .unwrap();

        assert!(matches!(
            assembler.prepare(),
            Err(DistributedReadAssemblyError::ResourceLimitExceeded { .. })
        ));
        assert!(matches!(
            assembler.open_prepared_stream(),
            Err(DistributedReadAssemblyError::NotPrepared)
        ));
    }
}
