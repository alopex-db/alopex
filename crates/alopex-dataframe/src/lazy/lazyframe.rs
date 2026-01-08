use std::path::{Path, PathBuf};

use crate::{DataFrame, DataFrameError, Expr, Result};

#[derive(Debug, Clone)]
pub struct LazyFrame {
    _private: (),
}

impl LazyFrame {
    pub fn from_dataframe(_df: DataFrame) -> Self {
        Self { _private: () }
    }

    pub fn scan_csv(path: impl AsRef<Path>) -> Result<Self> {
        let _path: PathBuf = path.as_ref().to_path_buf();
        Err(DataFrameError::invalid_operation(
            "LazyFrame::scan_csv is not implemented yet",
        ))
    }

    pub fn scan_parquet(path: impl AsRef<Path>) -> Result<Self> {
        let _path: PathBuf = path.as_ref().to_path_buf();
        Err(DataFrameError::invalid_operation(
            "LazyFrame::scan_parquet is not implemented yet",
        ))
    }

    pub fn select(self, _exprs: Vec<Expr>) -> Self {
        self
    }

    pub fn filter(self, _predicate: Expr) -> Self {
        self
    }

    pub fn with_columns(self, _exprs: Vec<Expr>) -> Self {
        self
    }

    pub fn group_by(self, _by: Vec<Expr>) -> LazyGroupBy {
        LazyGroupBy { lf: self, by: _by }
    }

    pub fn collect(self) -> Result<DataFrame> {
        Err(DataFrameError::invalid_operation(
            "LazyFrame::collect is not implemented yet",
        ))
    }

    pub fn explain(self, _optimized: bool) -> String {
        "LogicalPlan(<unimplemented>)".to_string()
    }
}

#[derive(Debug, Clone)]
pub struct LazyGroupBy {
    lf: LazyFrame,
    by: Vec<Expr>,
}

impl LazyGroupBy {
    pub fn agg(self, _aggs: Vec<Expr>) -> LazyFrame {
        let _ = self.by;
        self.lf
    }
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    df: DataFrame,
    by: Vec<Expr>,
}

impl GroupBy {
    pub(crate) fn new(df: DataFrame, by: Vec<Expr>) -> Self {
        Self { df, by }
    }

    pub fn agg(self, _aggs: Vec<Expr>) -> Result<DataFrame> {
        let _ = self.by;
        Err(DataFrameError::invalid_operation(
            "GroupBy::agg is not implemented yet",
        ))
    }

    pub fn into_df(self) -> DataFrame {
        self.df
    }
}
