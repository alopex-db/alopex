//! `alopex-dataframe` is a small, Polars-inspired DataFrame API built on Arrow.
//!
//! The v0.1 scope focuses on a minimal eager `DataFrame` and a lazy query pipeline
//! (`LazyFrame`) that compiles logical plans into physical plans and executes them.
//! CSV / Parquet I/O is provided via `arrow-csv` and `parquet`.

mod error;

/// Eager DataFrame and Series types.
pub mod dataframe;
/// Expression DSL used by both eager and lazy APIs.
pub mod expr;
/// CSV / Parquet I/O and option types.
pub mod io;
/// Lazy query planning and optimization.
pub mod lazy;
/// Physical plan compilation and execution.
pub mod physical;

/// Re-export of the primary eager types.
pub use crate::dataframe::{DataFrame, GroupBy, Series};
/// Re-export of the crate error type and result alias.
pub use crate::error::{DataFrameError, Result};
/// Re-export of the expression DSL entrypoints.
pub use crate::expr::{all, col, lit, Expr};
/// Re-export of eager CSV / Parquet I/O helpers.
pub use crate::io::{read_csv, read_parquet, write_csv, write_parquet};
/// Re-export of the primary lazy type.
pub use crate::lazy::LazyFrame;

/// Create a `LazyFrame` that scans a CSV file without performing I/O eagerly.
pub fn scan_csv(path: impl AsRef<std::path::Path>) -> Result<LazyFrame> {
    LazyFrame::scan_csv(path)
}

/// Create a `LazyFrame` that scans a Parquet file without performing I/O eagerly.
pub fn scan_parquet(path: impl AsRef<std::path::Path>) -> Result<LazyFrame> {
    LazyFrame::scan_parquet(path)
}
