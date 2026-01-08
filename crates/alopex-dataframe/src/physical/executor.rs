use arrow::record_batch::RecordBatch;

use crate::io::{CsvReadOptions, ParquetReadOptions};
use crate::physical::operators;
use crate::physical::plan::{PhysicalPlan, ScanSource};
use crate::Result;

pub struct Executor;

impl Executor {
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
    match source {
        ScanSource::DataFrame(df) => Ok(operators::scan_dataframe(df)),
        ScanSource::Csv {
            path,
            predicate,
            projection,
        } => {
            let mut opts = CsvReadOptions::default();
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
            let mut opts = ParquetReadOptions::default();
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
