use crate::planner::typed_expr::TypedExpr;
use crate::planner::types::ResolvedType;

/// Supported aggregate function types.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
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
        let result_type = arg.resolved_type.clone();
        Self {
            function: AggregateFunction::Sum,
            arg: Some(arg),
            distinct: false,
            result_type,
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
