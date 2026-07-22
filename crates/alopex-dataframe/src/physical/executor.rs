use arrow::record_batch::RecordBatch;

use crate::physical::budget::{ResourceBudget, ResourceReservation, ResourceScope};
use crate::physical::operators;
use crate::physical::plan::{PhysicalPlan, ScanSource};
use crate::physical::streaming::DataFrameStream;
use crate::{DataFrame, Result};

/// Executes `PhysicalPlan` trees and returns the resulting `RecordBatch` output.
pub struct Executor;

impl Executor {
    /// Execute a physical plan and return the resulting record batches.
    pub fn execute(plan: PhysicalPlan) -> Result<Vec<RecordBatch>> {
        execute_plan(&plan)
    }
}

/// Bounded materializing collector for callers that explicitly opt into a resource limit.
///
/// This collector only takes ownership of a batch after its retained output reservation has
/// succeeded. Source readers and operators must reserve their own allocations before creating
/// a batch; the streaming executor introduced later supplies that contract at those boundaries.
#[derive(Debug, Clone)]
pub struct BudgetedMaterializedExecutor {
    budget: ResourceBudget,
}

impl BudgetedMaterializedExecutor {
    /// Construct a collector using the supplied shared resource budget.
    pub fn new(budget: ResourceBudget) -> Self {
        Self { budget }
    }

    /// Materialize already-produced batches without exceeding the retained-output reservation.
    ///
    /// The returned `DataFrame` transfers batch ownership to the caller, so this collector's
    /// reservations are released before returning. An error drops all retained batches and their
    /// reservations.
    pub fn collect_batches<I>(&self, batches: I) -> Result<DataFrame>
    where
        I: IntoIterator<Item = Result<RecordBatch>>,
    {
        let mut retained = Vec::new();
        let mut reservations: Vec<ResourceReservation> = Vec::new();

        for batch in batches {
            let batch = batch?;
            let bytes = record_batch_memory_size(&batch);
            let reservation = self
                .budget
                .reserve_batch(ResourceScope::MaterializedOutput, bytes)?;

            // `reserve_batch` precedes the only retained-output ownership transition.
            retained.push(batch);
            reservations.push(reservation);
        }

        let dataframe = DataFrame::from_batches(retained)?;
        drop(reservations);
        Ok(dataframe)
    }

    /// Drain an already-bounded stream while reserving each result batch before retaining it.
    ///
    /// Source and operator allocations are owned by `DataFrameStream`; this collector adds only
    /// the materialized-result ownership needed for an explicit bounded collect. A reservation
    /// failure closes the stream so the source handle and all transient reservations are released.
    pub fn collect_stream(&self, stream: &mut DataFrameStream) -> Result<DataFrame> {
        let mut retained = Vec::new();
        let mut reservations: Vec<ResourceReservation> = Vec::new();

        loop {
            let next = stream.next_batch()?;
            let Some(dataframe) = next else {
                break;
            };
            for batch in dataframe.to_arrow() {
                let bytes = record_batch_memory_size(&batch);
                let reservation = match self
                    .budget
                    .reserve_batch(ResourceScope::MaterializedOutput, bytes)
                {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        let _ = stream.close();
                        return Err(error);
                    }
                };
                retained.push(batch);
                reservations.push(reservation);
            }
        }

        let dataframe = match DataFrame::from_batches(retained) {
            Ok(dataframe) => dataframe,
            Err(error) => {
                let _ = stream.close();
                return Err(error);
            }
        };
        drop(reservations);
        Ok(dataframe)
    }

    /// Return the shared budget used by this collector.
    pub fn budget(&self) -> &ResourceBudget {
        &self.budget
    }
}

/// Estimate Arrow-owned bytes for an already decoded batch.
///
/// Streaming sources use conservative pre-decode bounds. The materializing collector uses the
/// Arrow-reported size because its responsibility starts when it retains a completed batch.
pub fn record_batch_memory_size(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size() as u64)
        .sum()
}

fn execute_plan(plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
    match plan {
        PhysicalPlan::ScanExec { source } => execute_scan(source),
        PhysicalPlan::ConcatExec { inputs, schema } => {
            let mut output = Vec::new();
            let mut expected_schema = schema.clone();
            for (input_index, input) in inputs.iter().enumerate() {
                let batches = execute_plan(input)?;
                for (batch_index, batch) in batches.into_iter().enumerate() {
                    if let Some(expected_schema) = &expected_schema {
                        if batch.schema().as_ref() == expected_schema.as_ref() {
                            output.push(batch);
                            continue;
                        }
                        return Err(crate::DataFrameError::schema_mismatch(format!(
                            "concat_schema_mismatch: input {input_index} batch {batch_index} differs from validated schema"
                        )));
                    }
                    expected_schema = Some(batch.schema());
                    output.push(batch);
                }
            }
            Ok(output)
        }
        PhysicalPlan::ProjectionExec { input, exprs, kind } => {
            let batches = execute_plan(input)?;
            operators::project_batches(batches, exprs, kind.clone())
        }
        PhysicalPlan::FilterExec { input, predicate } => {
            let batches = execute_plan(input)?;
            operators::filter_batches(batches, predicate)
        }
        PhysicalPlan::AggregateExec {
            input,
            group_by,
            aggs,
        } => {
            let batches = execute_plan(input)?;
            operators::aggregate_batches(batches, group_by, aggs)
        }
        PhysicalPlan::JoinExec {
            left,
            right,
            keys,
            how,
        } => {
            let left_batches = execute_plan(left)?;
            let right_batches = execute_plan(right)?;
            operators::join_batches(left_batches, right_batches, keys, how)
        }
        PhysicalPlan::SortExec { input, options } => {
            let batches = execute_plan(input)?;
            operators::sort_batches(batches, options)
        }
        PhysicalPlan::SliceExec {
            input,
            offset,
            len,
            from_end,
        } => {
            let batches = execute_plan(input)?;
            operators::slice_batches(batches, *offset, *len, *from_end)
        }
        PhysicalPlan::UniqueExec { input, subset } => {
            let batches = execute_plan(input)?;
            operators::unique_batches(batches, subset.as_deref())
        }
        PhysicalPlan::FillNullExec { input, fill } => {
            let batches = execute_plan(input)?;
            operators::fill_null_batches(batches, fill)
        }
        PhysicalPlan::DropNullsExec { input, subset } => {
            let batches = execute_plan(input)?;
            operators::drop_nulls_batches(batches, subset.as_deref())
        }
        PhysicalPlan::NullCountExec { input } => {
            let batches = execute_plan(input)?;
            operators::null_count_batches(batches)
        }
        PhysicalPlan::ExplodeExec { input, column } => {
            let batches = execute_plan(input)?;
            operators::explode_batches(batches, column)
        }
        PhysicalPlan::ImplodeExec { input } => {
            let batches = execute_plan(input)?;
            operators::implode_batches(batches)
        }
    }
}

fn execute_scan(source: &ScanSource) -> Result<Vec<RecordBatch>> {
    operators::scan_source(source)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{BudgetedMaterializedExecutor, Executor};
    use crate::physical::budget::ResourceBudget;
    use crate::physical::{PhysicalPlan, ScanSource};
    use crate::{DataFrame, DataFrameError};

    fn batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef]).unwrap()
    }

    #[test]
    fn bounded_materialization_reserves_before_retaining_and_releases_on_error() {
        let one = batch(vec![1, 2]);
        let bytes = super::record_batch_memory_size(&one);
        let budget = ResourceBudget::new(bytes, NonZeroUsize::new(2).unwrap());
        let collector = BudgetedMaterializedExecutor::new(budget.clone());

        let err = collector.collect_batches(vec![Ok(one), Ok(batch(vec![3, 4]))]);
        assert!(matches!(
            err,
            Err(DataFrameError::ResourceLimitExceeded { .. })
        ));
        assert_eq!(budget.usage().reserved_bytes, 0);
        assert_eq!(budget.usage().reserved_batches, 0);
    }

    #[test]
    fn bounded_materialization_transfers_output_and_releases_budget() {
        let batch = batch(vec![1, 2]);
        let bytes = super::record_batch_memory_size(&batch);
        let budget = ResourceBudget::new(bytes, NonZeroUsize::new(1).unwrap());
        let output = BudgetedMaterializedExecutor::new(budget.clone())
            .collect_batches(vec![Ok(batch)])
            .unwrap();

        assert_eq!(output.height(), 2);
        assert_eq!(budget.usage().reserved_bytes, 0);
        assert_eq!(budget.usage().reserved_batches, 0);
    }

    #[test]
    fn concat_executor_emits_all_batches_of_each_input_in_declared_order() {
        let first = DataFrame::from_batches(vec![batch(vec![1, 2])]).unwrap();
        let second = DataFrame::from_batches(vec![batch(vec![3, 4])]).unwrap();
        let plan = PhysicalPlan::ConcatExec {
            inputs: vec![
                PhysicalPlan::ScanExec {
                    source: ScanSource::DataFrame(first.clone()),
                },
                PhysicalPlan::ScanExec {
                    source: ScanSource::DataFrame(second),
                },
            ],
            schema: Some(first.schema()),
        };

        let batches = Executor::execute(plan).unwrap();
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
            })
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn concat_executor_rejects_a_runtime_schema_contract_violation_before_output() {
        let first = DataFrame::from_batches(vec![batch(vec![1])]).unwrap();
        let other_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            true,
        )]));
        let second = DataFrame::from_batches(vec![RecordBatch::try_new(
            other_schema,
            vec![Arc::new(Int64Array::from(vec![2])) as ArrayRef],
        )
        .unwrap()])
        .unwrap();
        let plan = PhysicalPlan::ConcatExec {
            inputs: vec![
                PhysicalPlan::ScanExec {
                    source: ScanSource::DataFrame(first.clone()),
                },
                PhysicalPlan::ScanExec {
                    source: ScanSource::DataFrame(second),
                },
            ],
            schema: Some(first.schema()),
        };

        let err = Executor::execute(plan).unwrap_err();
        assert!(matches!(err, DataFrameError::SchemaMismatch { message }
            if message.contains("concat_schema_mismatch")));
    }
}
