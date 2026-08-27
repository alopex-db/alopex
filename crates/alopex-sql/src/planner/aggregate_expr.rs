use crate::planner::typed_expr::{SortExpr, TypedExpr};
use crate::planner::types::ResolvedType;

/// Supported aggregate function types.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Total,
    Avg,
    Min,
    Max,
    GroupConcat {
        separator: Option<String>,
    },
    StringAgg {
        separator: Option<String>,
    },
    JsonGroupArray,
    JsonGroupObject,
    /// Ordered-set aggregate `PERCENTILE_DISC(fraction) WITHIN GROUP
    /// (ORDER BY ...)` (issue #148). `fraction` is validated by the planner to
    /// be a literal in `[0, 1]`. Issue #154 (PERCENTILE_CONT / MODE) extends
    /// this enum with sibling variants reusing the same ordered-input path.
    PercentileDisc {
        fraction: f64,
    },
    PercentileCont {
        fraction: f64,
    },
    QuantileCont {
        fraction: f64,
    },
    Variance {
        sample: bool,
    },
    Stddev {
        sample: bool,
    },
    Covariance {
        sample: bool,
    },
    Corr,
    Median,
    Mode,
    RegrCount,
    RegrAvgX,
    RegrAvgY,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    RegrSlope,
    RegrIntercept,
    RegrR2,
    AnyValue,
    First,
    Last,
    ArgMin,
    ArgMax,
    BitAnd,
    BitOr,
    BitXor,
    BoolAnd,
    BoolOr,
}

/// Aggregate expression definition.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub arg: Option<TypedExpr>,
    /// Additional inputs for two-argument aggregates. Existing accumulators
    /// remain single-input through `arg`; only covariance, regression, and
    /// arg-min/max consume this vector.
    pub extra_args: Vec<TypedExpr>,
    pub distinct: bool,
    pub result_type: ResolvedType,
    /// `FILTER (WHERE predicate)`: rows where the predicate is not TRUE are
    /// skipped before the accumulator (and any DISTINCT set) sees them.
    pub filter: Option<TypedExpr>,
    /// Aggregate-local ordering. Non-empty only for order-sensitive
    /// aggregates (GROUP_CONCAT / STRING_AGG / ordered-set aggregates); the
    /// planner discards validated ORDER BY on order-insensitive aggregates
    /// (D3 in docs/sql-aggregate-filter-within-group.md).
    pub order_by: Vec<SortExpr>,
}

impl AggregateExpr {
    pub fn count_star() -> Self {
        Self {
            function: AggregateFunction::Count,
            arg: None,
            extra_args: Vec::new(),
            distinct: false,
            result_type: ResolvedType::BigInt,
            filter: None,
            order_by: Vec::new(),
        }
    }

    pub fn count(arg: TypedExpr, distinct: bool) -> Self {
        Self {
            function: AggregateFunction::Count,
            arg: Some(arg),
            extra_args: Vec::new(),
            distinct,
            result_type: ResolvedType::BigInt,
            filter: None,
            order_by: Vec::new(),
        }
    }

    pub fn sum(arg: TypedExpr) -> Self {
        let result_type = sum_result_type(&arg.resolved_type);
        Self {
            function: AggregateFunction::Sum,
            arg: Some(arg),
            extra_args: Vec::new(),
            distinct: false,
            result_type,
            filter: None,
            order_by: Vec::new(),
        }
    }

    pub fn total(arg: TypedExpr) -> Self {
        Self {
            function: AggregateFunction::Total,
            arg: Some(arg),
            extra_args: Vec::new(),
            distinct: false,
            result_type: ResolvedType::Double,
            filter: None,
            order_by: Vec::new(),
        }
    }

    pub fn avg(arg: TypedExpr) -> Self {
        Self {
            function: AggregateFunction::Avg,
            arg: Some(arg),
            extra_args: Vec::new(),
            distinct: false,
            result_type: ResolvedType::Double,
            filter: None,
            order_by: Vec::new(),
        }
    }

    pub fn min(arg: TypedExpr) -> Self {
        let result_type = arg.resolved_type.clone();
        Self {
            function: AggregateFunction::Min,
            arg: Some(arg),
            extra_args: Vec::new(),
            distinct: false,
            result_type,
            filter: None,
            order_by: Vec::new(),
        }
    }

    pub fn max(arg: TypedExpr) -> Self {
        let result_type = arg.resolved_type.clone();
        Self {
            function: AggregateFunction::Max,
            arg: Some(arg),
            extra_args: Vec::new(),
            distinct: false,
            result_type,
            filter: None,
            order_by: Vec::new(),
        }
    }
}

/// Return the SQL result type for `SUM` over a value of `input_type`.
///
/// Fixed-width integral inputs retain their integral type. All other numeric
/// inputs accumulate and return DOUBLE, matching the historical floating-point
/// behaviour and keeping `TOTAL`/`AVG` semantics distinct.
/// `SUM` keeps integer inputs exact, but accumulates them in a wider type: a
/// 32-bit accumulator overflows on ordinary data, so summing INTEGER yields
/// BIGINT. PostgreSQL sums int4 into int8 for the same reason, and DuckDB
/// widens further to hugeint.
pub fn sum_result_type(input_type: &ResolvedType) -> ResolvedType {
    match input_type {
        ResolvedType::Integer | ResolvedType::BigInt => ResolvedType::BigInt,
        _ => ResolvedType::Double,
    }
}
