use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::{DataFrameError, Expr, Result, Series};

/// An eager table backed by one or more Arrow `RecordBatch` values.
#[derive(Debug, Clone)]
pub struct DataFrame {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl DataFrame {
    /// Construct a `DataFrame` from a list of `Series`.
    ///
    /// Chunk boundaries do not need to align across series as long as total lengths match.
    pub fn new(columns: Vec<Series>) -> Result<Self> {
        if columns.is_empty() {
            return Ok(Self::empty());
        }

        let mut seen_names = HashSet::with_capacity(columns.len());
        for c in &columns {
            if !seen_names.insert(c.name().to_string()) {
                return Err(DataFrameError::schema_mismatch(format!(
                    "duplicate column name '{}'",
                    c.name()
                )));
            }
        }

        let expected_len = columns[0].len();
        for c in &columns[1..] {
            if c.len() != expected_len {
                return Err(DataFrameError::schema_mismatch(format!(
                    "column length mismatch: '{}' has length {}, expected {}",
                    c.name(),
                    c.len(),
                    expected_len
                )));
            }
        }

        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(c.name(), c.dtype(), true))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        let arrays = columns
            .iter()
            .map(|c| {
                if c.chunks().is_empty() {
                    Ok(arrow::array::new_empty_array(&c.dtype()))
                } else if c.chunks().len() == 1 {
                    Ok(c.chunks()[0].clone())
                } else {
                    let arrays = c
                        .chunks()
                        .iter()
                        .map(|a| a.as_ref() as &dyn arrow::array::Array)
                        .collect::<Vec<_>>();
                    arrow::compute::concat(&arrays)
                        .map_err(|source| DataFrameError::Arrow { source })
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
            DataFrameError::schema_mismatch(format!("failed to build RecordBatch: {e}"))
        })?;

        Ok(Self {
            schema,
            batches: vec![batch],
        })
    }

    /// Construct a `DataFrame` from Arrow record batches (all batches must share the same schema).
    pub fn from_batches(batches: Vec<RecordBatch>) -> Result<Self> {
        if batches.is_empty() {
            return Ok(Self::empty());
        }

        let schema = batches[0].schema();
        for (i, b) in batches.iter().enumerate().skip(1) {
            if b.schema().as_ref() != schema.as_ref() {
                return Err(DataFrameError::schema_mismatch(format!(
                    "schema mismatch between batches: batch 0 != batch {i}"
                )));
            }
        }

        Ok(Self { schema, batches })
    }

    /// Alias for `DataFrame::new`.
    pub fn from_series(series: Vec<Series>) -> Result<Self> {
        Self::new(series)
    }

    /// Return an empty `DataFrame` (no columns, no rows).
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            batches: Vec::new(),
        }
    }

    /// Return the number of rows.
    pub fn height(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Return the number of columns.
    pub fn width(&self) -> usize {
        self.schema.fields().len()
    }

    /// Return the Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get a column by name (case-sensitive).
    pub fn column(&self, name: &str) -> Result<Series> {
        let idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| DataFrameError::column_not_found(name.to_string()))?;

        let chunks = self
            .batches
            .iter()
            .map(|b| b.column(idx).clone())
            .collect::<Vec<_>>();
        Ok(Series::from_arrow_unchecked(name, chunks))
    }

    /// Return all columns in construction order.
    pub fn columns(&self) -> Vec<Series> {
        self.schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let chunks = self
                    .batches
                    .iter()
                    .map(|b| b.column(idx).clone())
                    .collect::<Vec<_>>();
                Series::from_arrow_unchecked(f.name(), chunks)
            })
            .collect()
    }

    /// Return the underlying Arrow batches.
    pub fn to_arrow(&self) -> Vec<RecordBatch> {
        self.batches.clone()
    }

    /// Convert this eager `DataFrame` to a `LazyFrame` for query planning/execution.
    pub fn lazy(&self) -> crate::LazyFrame {
        crate::LazyFrame::from_dataframe(self.clone())
    }

    /// Eager `select`, implemented by delegating to `LazyFrame`.
    pub fn select(&self, exprs: Vec<Expr>) -> Result<Self> {
        self.clone().lazy().select(exprs).collect()
    }

    /// Eager `filter`, implemented by delegating to `LazyFrame`.
    pub fn filter(&self, predicate: Expr) -> Result<Self> {
        self.clone().lazy().filter(predicate).collect()
    }

    /// Eager `with_columns`, implemented by delegating to `LazyFrame`.
    pub fn with_columns(&self, exprs: Vec<Expr>) -> Result<Self> {
        self.clone().lazy().with_columns(exprs).collect()
    }

    /// Start a group-by aggregation (eager API).
    pub fn group_by(&self, by: Vec<Expr>) -> GroupBy {
        GroupBy {
            df: self.clone(),
            by,
        }
    }
}

/// Eager group-by handle that delegates execution to `LazyFrame`.
#[derive(Debug, Clone)]
pub struct GroupBy {
    df: DataFrame,
    by: Vec<Expr>,
}

impl GroupBy {
    /// Perform aggregations for this group-by.
    pub fn agg(self, aggs: Vec<Expr>) -> Result<DataFrame> {
        self.df.lazy().group_by(self.by).agg(aggs).collect()
    }

    /// Return the underlying `DataFrame`.
    pub fn into_df(self) -> DataFrame {
        self.df
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::DataFrame;
    use crate::{DataFrameError, Series};

    fn s_i32(name: &str, chunks: Vec<Vec<i32>>) -> Series {
        let arrays: Vec<ArrayRef> = chunks
            .into_iter()
            .map(|v| Arc::new(Int32Array::from(v)) as ArrayRef)
            .collect();
        Series::from_arrow(name, arrays).unwrap()
    }

    #[test]
    fn dataframe_new_accepts_misaligned_chunks_by_normalizing() {
        let a = s_i32("a", vec![vec![1, 2], vec![3]]);
        let b = s_i32("b", vec![vec![10], vec![20, 30]]);

        let df = DataFrame::new(vec![a, b]).unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
        assert_eq!(df.schema().fields()[0].name(), "a");
        assert_eq!(df.schema().fields()[1].name(), "b");

        let batches = df.to_arrow();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn dataframe_new_rejects_duplicate_column_names() {
        let a1 = s_i32("a", vec![vec![1]]);
        let a2 = s_i32("a", vec![vec![2]]);
        let err = DataFrame::new(vec![a1, a2]).unwrap_err();
        assert!(matches!(err, DataFrameError::SchemaMismatch { .. }));
    }

    #[test]
    fn dataframe_new_rejects_length_mismatch() {
        let a = s_i32("a", vec![vec![1, 2]]);
        let b = s_i32("b", vec![vec![10]]);
        let err = DataFrame::new(vec![a, b]).unwrap_err();
        assert!(matches!(err, DataFrameError::SchemaMismatch { .. }));
    }

    #[test]
    fn dataframe_new_accepts_different_chunk_counts() {
        let a = s_i32("a", vec![vec![1], vec![2], vec![3]]);
        let b = s_i32("b", vec![vec![10, 20, 30]]);
        let df = DataFrame::new(vec![a, b]).unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.to_arrow().len(), 1);
    }

    #[test]
    fn dataframe_column_is_case_sensitive() {
        let a = s_i32("a", vec![vec![1]]);
        let df = DataFrame::new(vec![a]).unwrap();
        assert!(matches!(
            df.column("A").unwrap_err(),
            DataFrameError::ColumnNotFound { .. }
        ));
    }

    #[test]
    fn dataframe_from_batches_rejects_schema_mismatch() {
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let a2: ArrayRef = Arc::new(StringArray::from(vec!["x"]));

        let s1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        let b1 = RecordBatch::try_new(s1, vec![a1]).unwrap();
        let b2 = RecordBatch::try_new(s2, vec![a2]).unwrap();

        let err = DataFrame::from_batches(vec![b1, b2]).unwrap_err();
        assert!(matches!(err, DataFrameError::SchemaMismatch { .. }));
    }

    #[test]
    fn dataframe_columns_preserves_schema_order() {
        let a = s_i32("a", vec![vec![1], vec![2]]);
        let b = s_i32("b", vec![vec![10], vec![20]]);
        let df = DataFrame::new(vec![b.clone(), a.clone()]).unwrap();

        let cols = df.columns();
        assert_eq!(cols[0].name(), "b");
        assert_eq!(cols[1].name(), "a");
        assert_eq!(cols[0].len(), 2);
        assert_eq!(cols[1].len(), 2);
    }
}
