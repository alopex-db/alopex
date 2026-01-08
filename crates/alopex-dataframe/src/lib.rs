mod error;

pub mod dataframe;
pub mod expr;
pub mod io;
pub mod lazy;
pub mod physical;

pub use crate::dataframe::{DataFrame, Series};
pub use crate::error::{DataFrameError, Result};
pub use crate::expr::{all, col, lit, Expr};
pub use crate::io::{read_csv, read_parquet, write_csv, write_parquet};
pub use crate::lazy::LazyFrame;

pub fn scan_csv(path: impl AsRef<std::path::Path>) -> Result<LazyFrame> {
    LazyFrame::scan_csv(path)
}

pub fn scan_parquet(path: impl AsRef<std::path::Path>) -> Result<LazyFrame> {
    LazyFrame::scan_parquet(path)
}
