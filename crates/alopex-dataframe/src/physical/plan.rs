use std::path::PathBuf;

use crate::lazy::{LogicalPlan, ProjectionKind};
use crate::ops::{FillNull, JoinKeys, JoinType, SortOptions};
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
    /// Join operator.
    JoinExec {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        keys: JoinKeys,
        how: JoinType,
    },
    /// Sort operator.
    SortExec {
        input: Box<PhysicalPlan>,
        options: SortOptions,
    },
    /// Slice operator (head/tail).
    SliceExec {
        input: Box<PhysicalPlan>,
        offset: usize,
        len: usize,
        from_end: bool,
    },
    /// Unique operator.
    UniqueExec {
        input: Box<PhysicalPlan>,
        subset: Option<Vec<String>>,
    },
    /// Fill-null operator.
    FillNullExec {
        input: Box<PhysicalPlan>,
        fill: FillNull,
    },
    /// Drop-nulls operator.
    DropNullsExec {
        input: Box<PhysicalPlan>,
        subset: Option<Vec<String>>,
    },
    /// Null-count operator.
    NullCountExec { input: Box<PhysicalPlan> },
    /// Explode one list column.
    ExplodeExec {
        input: Box<PhysicalPlan>,
        column: String,
    },
    /// Implode columns into one row of list columns.
    ImplodeExec { input: Box<PhysicalPlan> },
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
        LogicalPlan::Join {
            left,
            right,
            keys,
            how,
        } => PhysicalPlan::JoinExec {
            left: Box::new(compile(left)?),
            right: Box::new(compile(right)?),
            keys: keys.clone(),
            how: *how,
        },
        LogicalPlan::Sort { input, options } => PhysicalPlan::SortExec {
            input: Box::new(compile(input)?),
            options: options.clone(),
        },
        LogicalPlan::Slice {
            input,
            offset,
            len,
            from_end,
        } => PhysicalPlan::SliceExec {
            input: Box::new(compile(input)?),
            offset: *offset,
            len: *len,
            from_end: *from_end,
        },
        LogicalPlan::Unique { input, subset } => PhysicalPlan::UniqueExec {
            input: Box::new(compile(input)?),
            subset: subset.clone(),
        },
        LogicalPlan::FillNull { input, fill } => PhysicalPlan::FillNullExec {
            input: Box::new(compile(input)?),
            fill: fill.clone(),
        },
        LogicalPlan::DropNulls { input, subset } => PhysicalPlan::DropNullsExec {
            input: Box::new(compile(input)?),
            subset: subset.clone(),
        },
        LogicalPlan::NullCount { input } => PhysicalPlan::NullCountExec {
            input: Box::new(compile(input)?),
        },
        LogicalPlan::Explode { input, column } => PhysicalPlan::ExplodeExec {
            input: Box::new(compile(input)?),
            column: column.clone(),
        },
        LogicalPlan::Implode { input } => PhysicalPlan::ImplodeExec {
            input: Box::new(compile(input)?),
        },
    };

    Ok(plan)
}
