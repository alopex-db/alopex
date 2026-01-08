use std::path::Path;

use crate::{DataFrame, DataFrameError, Result};

pub fn read_csv(_path: impl AsRef<Path>) -> Result<DataFrame> {
    Err(DataFrameError::invalid_operation(
        "read_csv is not implemented yet",
    ))
}

pub fn write_csv(_path: impl AsRef<Path>, _df: &DataFrame) -> Result<()> {
    Err(DataFrameError::invalid_operation(
        "write_csv is not implemented yet",
    ))
}
