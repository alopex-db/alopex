use crate::planner::typed_expr::TypedExpr;
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
    GroupConcat { separator: Option<String> },
    StringAgg { separator: Option<String> },
}

/// Aggregate expression definition.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub arg: Option<TypedExpr>,
    pub distinct: bool,
    pub result_type: ResolvedType,
}

impl AggregateExpr {
    pub fn count_star() -> Self {
        Self {
            function: AggregateFunction::Count,
            arg: None,
            distinct: false,
            result_type: ResolvedType::BigInt,
        }
    }

    pub fn count(arg: TypedExpr, distinct: bool) -> Self {
        Self {
            function: AggregateFunction::Count,
            arg: Some(arg),
            distinct,
            result_type: ResolvedType::BigInt,
        }
    }

    pub fn sum(arg: TypedExpr) -> Self {
        let result_type = sum_result_type(&arg.resolved_type);
        Self {
            function: AggregateFunction::Sum,
            arg: Some(arg),
            distinct: false,
            result_type,
        }
    }

    pub fn total(arg: TypedExpr) -> Self {
        Self {
            function: AggregateFunction::Total,
            arg: Some(arg),
            distinct: false,
            result_type: ResolvedType::Double,
        }
    }

    pub fn avg(arg: TypedExpr) -> Self {
        Self {
            function: AggregateFunction::Avg,
            arg: Some(arg),
            distinct: false,
            result_type: ResolvedType::Double,
        }
    }

    pub fn min(arg: TypedExpr) -> Self {
        let result_type = arg.resolved_type.clone();
        Self {
            function: AggregateFunction::Min,
            arg: Some(arg),
            distinct: false,
            result_type,
        }
    }

    pub fn max(arg: TypedExpr) -> Self {
        let result_type = arg.resolved_type.clone();
        Self {
            function: AggregateFunction::Max,
            arg: Some(arg),
            distinct: false,
            result_type,
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
