use std::path::Path;

use crate::{DataFrame, DataFrameError, Result};

pub fn read_parquet(_path: impl AsRef<Path>) -> Result<DataFrame> {
    Err(DataFrameError::invalid_operation(
        "read_parquet is not implemented yet",
    ))
}

pub fn write_parquet(_path: impl AsRef<Path>, _df: &DataFrame) -> Result<()> {
    Err(DataFrameError::invalid_operation(
        "write_parquet is not implemented yet",
    ))
}
