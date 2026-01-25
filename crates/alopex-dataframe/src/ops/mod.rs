//! Operation-specific types and helpers for P1 DataFrame features.

pub mod join;
pub mod nulls;
pub mod sort;
pub mod unique;

/// Supported join types for `DataFrame::join`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// Join key specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKeys {
    /// Same column names on both sides.
    On(Vec<String>),
    /// Different column names for each side.
    LeftRight {
        left_on: Vec<String>,
        right_on: Vec<String>,
    },
}

impl From<Vec<String>> for JoinKeys {
    fn from(cols: Vec<String>) -> Self {
        JoinKeys::On(cols)
    }
}

impl From<(Vec<String>, Vec<String>)> for JoinKeys {
    fn from((left_on, right_on): (Vec<String>, Vec<String>)) -> Self {
        JoinKeys::LeftRight { left_on, right_on }
    }
}

/// Sort configuration for `DataFrame::sort`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortOptions {
    pub by: Vec<String>,
    pub descending: Vec<bool>,
    pub nulls_last: bool,
    pub stable: bool,
}

/// Strategies for `fill_null` operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillNullStrategy {
    Forward,
    Backward,
    Min,
    Max,
    Mean,
    Zero,
    One,
}

/// Fill-null specification (scalar value or strategy).
#[derive(Debug, Clone, PartialEq)]
pub enum FillNull {
    Value(crate::expr::Scalar),
    Strategy(FillNullStrategy),
}

impl From<crate::expr::Scalar> for FillNull {
    fn from(value: crate::expr::Scalar) -> Self {
        FillNull::Value(value)
    }
}

impl From<FillNullStrategy> for FillNull {
    fn from(strategy: FillNullStrategy) -> Self {
        FillNull::Strategy(strategy)
    }
}

impl From<i64> for FillNull {
    fn from(value: i64) -> Self {
        FillNull::Value(value.into())
    }
}

impl From<f64> for FillNull {
    fn from(value: f64) -> Self {
        FillNull::Value(value.into())
    }
}

impl From<bool> for FillNull {
    fn from(value: bool) -> Self {
        FillNull::Value(value.into())
    }
}

impl From<String> for FillNull {
    fn from(value: String) -> Self {
        FillNull::Value(value.into())
    }
}

impl From<&str> for FillNull {
    fn from(value: &str) -> Self {
        FillNull::Value(value.into())
    }
}
