use std::path::PathBuf;

use crate::lazy::{LogicalPlan, ProjectionKind};
use crate::ops::{FillNull, JoinKeys, JoinType, SortOptions};
use crate::physical::budget::StreamOptions;
use crate::{DataFrame, Expr, Result};
use arrow::datatypes::SchemaRef;

/// Plan/source subject named by a streaming eligibility result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanSubject {
    /// In-memory materialized DataFrame source.
    DataFrameScan,
    /// CSV source.
    CsvScan,
    /// Parquet source.
    ParquetScan,
    /// Provisioned range-addressable Alopex V08 columnar segment source.
    ColumnarSegmentScan,
    /// Strict vertical concatenation.
    Concat,
    /// Projection or with-columns operator.
    Projection,
    /// Filter operator.
    Filter,
    /// Aggregate operator.
    Aggregate,
    /// Join operator.
    Join,
    /// Sort operator.
    Sort,
    /// Slice operator.
    Slice,
    /// Unique operator.
    Unique,
    /// Fill-null operator.
    FillNull,
    /// Drop-nulls operator.
    DropNulls,
    /// Null-count operator.
    NullCount,
    /// Explode operator.
    Explode,
    /// Implode operator.
    Implode,
}

impl PlanSubject {
    /// Stable diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DataFrameScan => "dataframe_scan",
            Self::CsvScan => "csv_scan",
            Self::ParquetScan => "parquet_scan",
            Self::ColumnarSegmentScan => "columnar_segment_scan",
            Self::Concat => "concat",
            Self::Projection => "projection",
            Self::Filter => "filter",
            Self::Aggregate => "aggregate",
            Self::Join => "join",
            Self::Sort => "sort",
            Self::Slice => "slice",
            Self::Unique => "unique",
            Self::FillNull => "fill_null",
            Self::DropNulls => "drop_nulls",
            Self::NullCount => "null_count",
            Self::Explode => "explode",
            Self::Implode => "implode",
        }
    }
}

/// Stable reason for a streaming eligibility outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingReason {
    /// The v0.8 bounded source reader is not registered yet.
    SourceReaderUnavailable,
    /// The legacy source is already materialized and cannot be used as a streaming source.
    LegacyMaterializedSource,
    /// The operator needs global state and has no bounded streaming algorithm.
    GlobalState,
    /// Tail requires knowledge of the complete input.
    ReverseSlice,
    /// The requested bound cannot support an approved algorithm.
    InsufficientResourceBound,
    /// The source is available but its required batch operator is not installed yet.
    OperatorNotInstalled,
}

impl StreamingReason {
    /// Stable diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceReaderUnavailable => "source_reader_unavailable",
            Self::LegacyMaterializedSource => "legacy_materialized_source",
            Self::GlobalState => "global_state",
            Self::ReverseSlice => "reverse_slice_requires_materialization",
            Self::InsufficientResourceBound => "insufficient_resource_bound",
            Self::OperatorNotInstalled => "operator_not_installed",
        }
    }
}

/// Source limitation visible before source I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLimit {
    /// Source that owns the limitation.
    pub source: PlanSubject,
    /// Stable limitation code.
    pub code: &'static str,
    /// Short public explanation.
    pub description: &'static str,
}

/// Eligibility outcome returned before opening a source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamingEligibility {
    /// The compiler can construct a bounded source and per-batch pipeline.
    Supported { source_limits: Vec<SourceLimit> },
    /// No streaming implementation exists for this source or operator.
    Unsupported {
        /// The rejected source or operator.
        subject: PlanSubject,
        /// Stable reason code.
        reason: StreamingReason,
    },
    /// The operation is known but needs an explicit bounded materialization algorithm.
    RequiresMaterialization {
        /// The rejected operator.
        subject: PlanSubject,
        /// Whether a bounded materialization implementation is available.
        bounded_possible: bool,
    },
}

impl StreamingEligibility {
    /// Convert a non-supported result to its public preflight error.
    pub fn into_result(self) -> Result<Vec<SourceLimit>> {
        match self {
            Self::Supported { source_limits } => Ok(source_limits),
            Self::Unsupported { subject, reason } => Err(
                crate::DataFrameError::streaming_unsupported(subject.as_str(), reason.as_str()),
            ),
            Self::RequiresMaterialization {
                subject,
                bounded_possible,
            } => Err(crate::DataFrameError::streaming_requires_materialization(
                subject.as_str(),
                if bounded_possible {
                    "bounded_algorithm_required"
                } else {
                    "no_bounded_algorithm"
                },
            )),
        }
    }
}

/// A physical source and only the operators approved for batch-at-a-time execution.
#[derive(Debug, Clone)]
pub struct StreamingPhysicalPlan {
    /// Source tree to open through bounded source factories.
    pub source: StreamingSource,
    /// Operators in source-to-consumer order.
    pub operators: Vec<StreamingOperator>,
    /// Source limitations determined before opening the source.
    pub source_limits: Vec<SourceLimit>,
}

/// Streaming source tree preserving concat input order and child operators.
#[derive(Debug, Clone)]
pub enum StreamingSource {
    /// A single scan source with no nested stream boundary.
    Scan(ScanSource),
    /// Strict vertical concat of independently compiled child streams.
    Concat {
        /// Children in declared output order.
        inputs: Vec<StreamingPhysicalPlan>,
        /// Validated common schema, or deferred bounded source preflight.
        schema: Option<SchemaRef>,
    },
}

/// Per-batch operator selected by the streaming compiler.
#[derive(Debug, Clone)]
pub enum StreamingOperator {
    /// Projection/select or with-columns applied to one batch.
    Projection {
        /// Expressions in declaration order.
        exprs: Vec<Expr>,
        /// Projection behavior.
        kind: ProjectionKind,
    },
    /// Predicate applied to one batch.
    Filter {
        /// Predicate expression.
        predicate: Expr,
    },
    /// Forward-only head/slice operation.
    ForwardSlice {
        /// Rows to skip from the stream start.
        offset: usize,
        /// Maximum rows to publish.
        len: usize,
    },
}

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
    /// Strict vertical concat execution preserving input order.
    ConcatExec {
        /// Inputs in declared output order.
        inputs: Vec<PhysicalPlan>,
        /// Validated common schema, or deferred bounded source preflight.
        schema: Option<SchemaRef>,
    },
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
        LogicalPlan::Concat { inputs, schema } => PhysicalPlan::ConcatExec {
            inputs: inputs.iter().map(compile).collect::<Result<Vec<_>>>()?,
            schema: schema.clone(),
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

/// Analyze a plan before source I/O. Global nodes win over leaf-source results so callers never
/// open a source merely to discover that a global operator cannot stream.
pub fn analyze_streaming(plan: &PhysicalPlan, options: StreamOptions) -> StreamingEligibility {
    if options.memory_limit_bytes == 0 {
        return StreamingEligibility::Unsupported {
            subject: PlanSubject::DataFrameScan,
            reason: StreamingReason::InsufficientResourceBound,
        };
    }

    match plan {
        PhysicalPlan::ScanExec { source } => match source {
            ScanSource::DataFrame(_) => StreamingEligibility::Unsupported {
                subject: PlanSubject::DataFrameScan,
                reason: StreamingReason::LegacyMaterializedSource,
            },
            ScanSource::Csv { predicate, .. } => {
                if predicate
                    .as_ref()
                    .is_some_and(|predicate| !expression_is_streamable(predicate, false))
                {
                    StreamingEligibility::Unsupported {
                        subject: PlanSubject::Filter,
                        reason: StreamingReason::OperatorNotInstalled,
                    }
                } else {
                    StreamingEligibility::Supported {
                        source_limits: vec![SourceLimit {
                            source: PlanSubject::CsvScan,
                            code: "stable_input_order",
                            description: "CSV streaming preserves physical input record order",
                        }],
                    }
                }
            }
            ScanSource::Parquet { predicate, .. } => {
                if predicate
                    .as_ref()
                    .is_some_and(|predicate| !expression_is_streamable(predicate, false))
                {
                    StreamingEligibility::Unsupported {
                        subject: PlanSubject::Filter,
                        reason: StreamingReason::OperatorNotInstalled,
                    }
                } else {
                    StreamingEligibility::Supported {
                        source_limits: vec![
                            SourceLimit {
                                source: PlanSubject::ParquetScan,
                                code: "stable_row_group_order",
                                description: "Parquet streaming preserves selected row-group and row order",
                            },
                            SourceLimit {
                                source: PlanSubject::ParquetScan,
                                code: "row_group_must_fit_resource_bound",
                                description: "A selected row group whose declared upper bound exceeds the resource limit is rejected before page decode",
                            },
                        ],
                    }
                }
            }
        },
        PhysicalPlan::ConcatExec { inputs, .. } => {
            let mut source_limits = Vec::new();
            for input in inputs {
                match analyze_streaming(input, options) {
                    StreamingEligibility::Supported {
                        source_limits: input_limits,
                    } => source_limits.extend(input_limits),
                    outcome => return outcome,
                }
            }
            StreamingEligibility::Supported { source_limits }
        }
        PhysicalPlan::ProjectionExec { input, exprs, .. } => {
            if !exprs
                .iter()
                .all(|expression| expression_is_streamable(expression, true))
            {
                return StreamingEligibility::Unsupported {
                    subject: PlanSubject::Projection,
                    reason: StreamingReason::OperatorNotInstalled,
                };
            }
            analyze_streaming(input, options)
        }
        PhysicalPlan::FilterExec { input, predicate } => {
            if !expression_is_streamable(predicate, false) {
                return StreamingEligibility::Unsupported {
                    subject: PlanSubject::Filter,
                    reason: StreamingReason::OperatorNotInstalled,
                };
            }
            analyze_streaming(input, options)
        }
        PhysicalPlan::SliceExec {
            input,
            from_end: false,
            ..
        } => analyze_streaming(input, options),
        PhysicalPlan::SliceExec { from_end: true, .. } => {
            StreamingEligibility::RequiresMaterialization {
                subject: PlanSubject::Slice,
                bounded_possible: false,
            }
        }
        PhysicalPlan::AggregateExec { .. } => StreamingEligibility::RequiresMaterialization {
            subject: PlanSubject::Aggregate,
            bounded_possible: false,
        },
        PhysicalPlan::JoinExec { .. } => StreamingEligibility::RequiresMaterialization {
            subject: PlanSubject::Join,
            bounded_possible: false,
        },
        PhysicalPlan::SortExec { .. } => StreamingEligibility::RequiresMaterialization {
            subject: PlanSubject::Sort,
            bounded_possible: false,
        },
        PhysicalPlan::UniqueExec { .. } => StreamingEligibility::RequiresMaterialization {
            subject: PlanSubject::Unique,
            bounded_possible: false,
        },
        PhysicalPlan::FillNullExec { .. } => StreamingEligibility::Unsupported {
            subject: PlanSubject::FillNull,
            reason: StreamingReason::GlobalState,
        },
        PhysicalPlan::DropNullsExec { .. } => StreamingEligibility::Unsupported {
            subject: PlanSubject::DropNulls,
            reason: StreamingReason::GlobalState,
        },
        PhysicalPlan::NullCountExec { .. } => StreamingEligibility::RequiresMaterialization {
            subject: PlanSubject::NullCount,
            bounded_possible: false,
        },
        PhysicalPlan::ExplodeExec { .. } => StreamingEligibility::Unsupported {
            subject: PlanSubject::Explode,
            reason: StreamingReason::GlobalState,
        },
        PhysicalPlan::ImplodeExec { .. } => StreamingEligibility::RequiresMaterialization {
            subject: PlanSubject::Implode,
            bounded_possible: false,
        },
    }
}

/// Return whether a v0.8 bounded evaluator has an allocation upper bound for this expression.
/// Namespace functions remain eager-only until each one has an independently verified bound.
fn expression_is_streamable(expression: &Expr, allow_wildcard: bool) -> bool {
    match expression {
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::Alias { expr, .. } | Expr::UnaryOp { expr, .. } => {
            expression_is_streamable(expr, allow_wildcard)
        }
        Expr::BinaryOp { left, right, .. } => {
            expression_is_streamable(left, allow_wildcard)
                && expression_is_streamable(right, allow_wildcard)
        }
        Expr::ConcatStr { inputs, .. } => {
            inputs.len() >= 2
                && inputs
                    .iter()
                    .all(|input| expression_is_streamable(input, allow_wildcard))
        }
        Expr::Wildcard => allow_wildcard,
        Expr::Agg { .. } | Expr::Function { .. } => false,
    }
}

/// Compile only a preflight-supported plan into a source plus batch-at-a-time operators.
pub fn compile_streaming(
    plan: &PhysicalPlan,
    options: StreamOptions,
) -> Result<StreamingPhysicalPlan> {
    let source_limits = analyze_streaming(plan, options).into_result()?;
    let mut operators = Vec::new();
    let source = compile_streaming_inner(plan, &mut operators, options)?;
    Ok(StreamingPhysicalPlan {
        source,
        operators,
        source_limits,
    })
}

fn compile_streaming_inner(
    plan: &PhysicalPlan,
    operators: &mut Vec<StreamingOperator>,
    options: StreamOptions,
) -> Result<StreamingSource> {
    match plan {
        PhysicalPlan::ScanExec { source } => {
            let mut source = source.clone();
            let predicate = match &mut source {
                ScanSource::Csv { predicate, .. } | ScanSource::Parquet { predicate, .. } => {
                    predicate.take()
                }
                ScanSource::DataFrame(_) => None,
            };
            if let Some(predicate) = predicate {
                operators.push(StreamingOperator::Filter { predicate });
            }
            Ok(StreamingSource::Scan(source))
        }
        PhysicalPlan::ConcatExec { inputs, schema } => Ok(StreamingSource::Concat {
            inputs: inputs
                .iter()
                .map(|input| compile_streaming(input, options))
                .collect::<Result<Vec<_>>>()?,
            schema: schema.clone(),
        }),
        PhysicalPlan::ProjectionExec { input, exprs, kind } => {
            let source = compile_streaming_inner(input, operators, options)?;
            operators.push(StreamingOperator::Projection {
                exprs: exprs.clone(),
                kind: kind.clone(),
            });
            Ok(source)
        }
        PhysicalPlan::FilterExec { input, predicate } => {
            let source = compile_streaming_inner(input, operators, options)?;
            operators.push(StreamingOperator::Filter {
                predicate: predicate.clone(),
            });
            Ok(source)
        }
        PhysicalPlan::SliceExec {
            input,
            offset,
            len,
            from_end: false,
        } => {
            let source = compile_streaming_inner(input, operators, options)?;
            operators.push(StreamingOperator::ForwardSlice {
                offset: *offset,
                len: *len,
            });
            Ok(source)
        }
        _ => Err(crate::DataFrameError::streaming_requires_materialization(
            "physical_plan",
            "no_batch_at_a_time_compiler",
        )),
    }
}

#[cfg(test)]
mod streaming_tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{
        analyze_streaming, compile_streaming, PhysicalPlan, ScanSource, StreamingEligibility,
        StreamingSource,
    };
    use crate::physical::budget::StreamOptions;

    fn options() -> StreamOptions {
        StreamOptions::new(
            1024,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
    }

    #[test]
    fn global_operator_is_rejected_before_csv_source_opening() {
        let plan = PhysicalPlan::AggregateExec {
            input: Box::new(PhysicalPlan::ScanExec {
                source: ScanSource::Csv {
                    path: "must-not-open.csv".into(),
                    predicate: None,
                    projection: None,
                },
            }),
            group_by: Vec::new(),
            aggs: Vec::new(),
        };

        assert!(matches!(
            analyze_streaming(&plan, options()),
            StreamingEligibility::RequiresMaterialization { .. }
        ));
        assert!(compile_streaming(&plan, options()).is_err());
    }

    #[test]
    fn csv_source_is_supported_after_its_bounded_factory_is_registered() {
        let plan = PhysicalPlan::ScanExec {
            source: ScanSource::Csv {
                path: "does-not-exist.csv".into(),
                predicate: None,
                projection: None,
            },
        };

        assert!(matches!(
            analyze_streaming(&plan, options()),
            StreamingEligibility::Supported { .. }
        ));
        assert!(compile_streaming(&plan, options()).is_ok());
    }

    #[test]
    fn concat_streaming_compiler_keeps_child_sources_in_declared_order() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let scan = |path: &str| PhysicalPlan::ScanExec {
            source: ScanSource::Csv {
                path: path.into(),
                predicate: None,
                projection: None,
            },
        };
        let plan = PhysicalPlan::ConcatExec {
            inputs: vec![scan("first.csv"), scan("second.csv")],
            schema: Some(schema),
        };

        let compiled = compile_streaming(&plan, options()).unwrap();
        let StreamingSource::Concat { inputs, .. } = compiled.source else {
            panic!("expected concat streaming source");
        };
        assert_eq!(inputs.len(), 2);
        for (input, expected_path) in inputs.iter().zip(["first.csv", "second.csv"]) {
            match &input.source {
                StreamingSource::Scan(ScanSource::Csv { path, .. }) => {
                    assert_eq!(path, std::path::Path::new(expected_path));
                }
                other => panic!("expected CSV child source, got {other:?}"),
            }
        }
    }
}
