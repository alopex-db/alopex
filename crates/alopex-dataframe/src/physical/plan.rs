use std::path::PathBuf;

use crate::lazy::{LogicalPlan, ProjectionKind};
use crate::{DataFrame, Expr, Result};

#[derive(Debug, Clone)]
pub enum ScanSource {
    DataFrame(DataFrame),
    Csv {
        path: PathBuf,
        predicate: Option<Expr>,
        projection: Option<Vec<String>>,
    },
    Parquet {
        path: PathBuf,
        predicate: Option<Expr>,
        projection: Option<Vec<String>>,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    ScanExec {
        source: ScanSource,
    },
    ProjectionExec {
        input: Box<PhysicalPlan>,
        exprs: Vec<Expr>,
        kind: ProjectionKind,
    },
    FilterExec {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    AggregateExec {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggs: Vec<Expr>,
    },
}

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
