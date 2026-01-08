use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::{DataFrameError, Result};

#[derive(Debug, Clone)]
pub struct Series {
    _private: (),
}

impl Series {
    pub fn from_arrow(_name: &str, _chunks: Vec<ArrayRef>) -> Result<Self> {
        Err(DataFrameError::invalid_operation(
            "Series::from_arrow is not implemented yet",
        ))
    }

    pub fn to_arrow(&self) -> Vec<ArrayRef> {
        Vec::new()
    }

    pub fn name(&self) -> &str {
        ""
    }

    pub fn len(&self) -> usize {
        0
    }

    pub fn dtype(&self) -> DataType {
        DataType::Null
    }

    pub fn is_empty(&self) -> bool {
        true
    }
}
