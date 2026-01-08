use std::path::Path;

use crate::io::options::ParquetReadOptions;
use crate::{DataFrame, DataFrameError, Result};

pub fn read_parquet(_path: impl AsRef<Path>) -> Result<DataFrame> {
    read_parquet_with_options(_path, &ParquetReadOptions::default())
}

pub fn write_parquet(_path: impl AsRef<Path>, _df: &DataFrame) -> Result<()> {
    Err(DataFrameError::invalid_operation(
        "write_parquet is not implemented yet",
    ))
}

pub(crate) fn read_parquet_with_options(
    _path: impl AsRef<Path>,
    _options: &ParquetReadOptions,
) -> Result<DataFrame> {
    Err(DataFrameError::invalid_operation(
        "read_parquet_with_options is not implemented yet",
    ))
}
