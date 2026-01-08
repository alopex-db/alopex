use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{DataFrameError, Expr, Result, Series};

#[derive(Debug, Clone)]
pub struct DataFrame {
    _private: (),
}

impl DataFrame {
    pub fn new(_columns: Vec<Series>) -> Result<Self> {
        Err(DataFrameError::invalid_operation(
            "DataFrame::new is not implemented yet",
        ))
    }

    pub fn from_batches(_batches: Vec<arrow::record_batch::RecordBatch>) -> Result<Self> {
        Err(DataFrameError::invalid_operation(
            "DataFrame::from_batches is not implemented yet",
        ))
    }

    pub fn from_series(_series: Vec<Series>) -> Result<Self> {
        Err(DataFrameError::invalid_operation(
            "DataFrame::from_series is not implemented yet",
        ))
    }

    pub fn empty() -> Self {
        Self { _private: () }
    }

    pub fn height(&self) -> usize {
        0
    }

    pub fn width(&self) -> usize {
        0
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::empty())
    }

    pub fn column(&self, _name: &str) -> Result<Series> {
        Err(DataFrameError::invalid_operation(
            "DataFrame::column is not implemented yet",
        ))
    }

    pub fn columns(&self) -> Vec<Series> {
        Vec::new()
    }

    pub fn to_arrow(&self) -> Vec<arrow::record_batch::RecordBatch> {
        Vec::new()
    }

    pub fn lazy(&self) -> crate::LazyFrame {
        crate::LazyFrame::from_dataframe(self.clone())
    }

    pub fn select(&self, _exprs: Vec<Expr>) -> Result<Self> {
        Err(DataFrameError::invalid_operation(
            "DataFrame::select is not implemented yet",
        ))
    }

    pub fn filter(&self, _predicate: Expr) -> Result<Self> {
        Err(DataFrameError::invalid_operation(
            "DataFrame::filter is not implemented yet",
        ))
    }

    pub fn with_columns(&self, _exprs: Vec<Expr>) -> Result<Self> {
        Err(DataFrameError::invalid_operation(
            "DataFrame::with_columns is not implemented yet",
        ))
    }

    pub fn group_by(&self, _by: Vec<Expr>) -> crate::lazy::GroupBy {
        crate::lazy::GroupBy::new(self.clone(), _by)
    }
}
