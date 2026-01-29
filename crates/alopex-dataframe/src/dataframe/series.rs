use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::{DataFrameError, Result};

/// A named column represented as one or more Arrow `ArrayRef` chunks.
#[derive(Debug, Clone)]
pub struct Series {
    name: String,
    chunks: Vec<ArrayRef>,
}

impl Series {
    /// Construct a `Series` from Arrow chunks, validating that all chunks share the same dtype.
    pub fn from_arrow(name: &str, chunks: Vec<ArrayRef>) -> Result<Self> {
        if chunks.is_empty() {
            return Ok(Self {
                name: name.to_string(),
                chunks,
            });
        }

        let expected = chunks[0].data_type().clone();
        for chunk in &chunks[1..] {
            let actual = chunk.data_type();
            if actual != &expected {
                return Err(DataFrameError::type_mismatch(
                    Some(name.to_string()),
                    expected.to_string(),
                    actual.to_string(),
                ));
            }
        }

        Ok(Self {
            name: name.to_string(),
            chunks,
        })
    }

    /// Convert this series into Arrow chunks.
    pub fn to_arrow(&self) -> Vec<ArrayRef> {
        self.chunks.clone()
    }

    /// Return the series name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the logical length of the series.
    pub fn len(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    /// Return the Arrow dtype of the series.
    pub fn dtype(&self) -> DataType {
        self.chunks
            .first()
            .map(|c| c.data_type().clone())
            .unwrap_or(DataType::Null)
    }

    /// Returns `true` if this series is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn chunks(&self) -> &[ArrayRef] {
        &self.chunks
    }

    pub(crate) fn from_arrow_unchecked(name: &str, chunks: Vec<ArrayRef>) -> Self {
        Self {
            name: name.to_string(),
            chunks,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};

    use super::Series;
    use crate::DataFrameError;

    #[test]
    fn from_arrow_accepts_empty_chunks() {
        let s = Series::from_arrow("a", vec![]).unwrap();
        assert_eq!(s.name(), "a");
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn from_arrow_rejects_mixed_dtypes() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));

        let err = Series::from_arrow("col", vec![a, b]).unwrap_err();
        match err {
            DataFrameError::TypeMismatch { column, .. } => {
                assert_eq!(column.as_deref(), Some("col"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
