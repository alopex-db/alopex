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
    }
}

fn execute_scan(source: &ScanSource) -> Result<Vec<RecordBatch>> {
    operators::scan_source(source)
}
