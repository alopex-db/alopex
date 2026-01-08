use std::path::Path;

use crate::io::options::CsvReadOptions;
use crate::{DataFrame, DataFrameError, Result};

pub fn read_csv(_path: impl AsRef<Path>) -> Result<DataFrame> {
    read_csv_with_options(_path, &CsvReadOptions::default())
}

pub fn write_csv(_path: impl AsRef<Path>, _df: &DataFrame) -> Result<()> {
    Err(DataFrameError::invalid_operation(
        "write_csv is not implemented yet",
    ))
}

pub(crate) fn read_csv_with_options(
    _path: impl AsRef<Path>,
    _options: &CsvReadOptions,
) -> Result<DataFrame> {
    Err(DataFrameError::invalid_operation(
        "read_csv_with_options is not implemented yet",
    ))
}
