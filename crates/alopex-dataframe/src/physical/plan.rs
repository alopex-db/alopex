use std::path::PathBuf;

use crate::lazy::{LogicalPlan, ProjectionKind};
use crate::{DataFrame, Expr, Result};

/// Source for a physical scan operator.
#[derive(Debug, Clone)]
pub enum ScanSource {
    /// In-memory scan of a `DataFrame`.
    DataFrame(DataFrame),
    /// CSV file scan with optional predicate/projection pushdown.
    Csv {
        path: PathBuf,
        predicate: Option<Expr>,
        projection: Option<Vec<String>>,
    },
    /// Parquet file scan with optional predicate/projection pushdown.
    Parquet {
        path: PathBuf,
        predicate: Option<Expr>,
        projection: Option<Vec<String>>,
    },
}

/// Physical execution plan produced from a `LogicalPlan`.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Scan operator.
    ScanExec { source: ScanSource },
    /// Projection operator.
    ProjectionExec {
        input: Box<PhysicalPlan>,
        exprs: Vec<Expr>,
        kind: ProjectionKind,
    },
    /// Filter operator.
    FilterExec {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    /// Aggregate operator.
    AggregateExec {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggs: Vec<Expr>,
    },
}

/// Compile a `LogicalPlan` into a `PhysicalPlan`.
pub fn compile(logical: &LogicalPlan) -> Result<PhysicalPlan> {
    let plan = match logical {
        LogicalPlan::DataFrameScan { df } => PhysicalPlan::ScanExec {
            source: ScanSource::DataFrame(df.clone()),
        },
        LogicalPlan::CsvScan {
            path,
            predicate,
            projection,
        } => PhysicalPlan::ScanExec {
            source: ScanSource::Csv {
                path: path.clone(),
                predicate: predicate.clone(),
                projection: projection.clone(),
            },
        },
        LogicalPlan::ParquetScan {
            path,
            predicate,
            projection,
        } => PhysicalPlan::ScanExec {
            source: ScanSource::Parquet {
                path: path.clone(),
                predicate: predicate.clone(),
                projection: projection.clone(),
            },
        },
        LogicalPlan::Projection { input, exprs, kind } => PhysicalPlan::ProjectionExec {
            input: Box::new(compile(input)?),
            exprs: exprs.clone(),
            kind: kind.clone(),
        },
        LogicalPlan::Filter { input, predicate } => PhysicalPlan::FilterExec {
            input: Box::new(compile(input)?),
            predicate: predicate.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => PhysicalPlan::AggregateExec {
            input: Box::new(compile(input)?),
            group_by: group_by.clone(),
            aggs: aggs.clone(),
        },
    };

    Ok(plan)
}
