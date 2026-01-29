use arrow::record_batch::RecordBatch;

use crate::physical::operators;
use crate::physical::plan::{PhysicalPlan, ScanSource};
use crate::Result;

/// Executes `PhysicalPlan` trees and returns the resulting `RecordBatch` output.
pub struct Executor;

impl Executor {
    /// Execute a physical plan and return the resulting record batches.
    pub fn execute(plan: PhysicalPlan) -> Result<Vec<RecordBatch>> {
        execute_plan(&plan)
    }
}

fn execute_plan(plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
    match plan {
        PhysicalPlan::ScanExec { source } => execute_scan(source),
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
    }
}

fn execute_scan(source: &ScanSource) -> Result<Vec<RecordBatch>> {
    operators::scan_source(source)
}
