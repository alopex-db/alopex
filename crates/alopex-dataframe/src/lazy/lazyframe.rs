use std::path::Path;

use crate::lazy::{LogicalPlan, Optimizer, ProjectionKind};
use crate::ops::{FillNull, JoinKeys, JoinType, SortOptions};
use crate::physical::budget::StreamOptions;
use crate::physical::{BatchOpenContext, BudgetedMaterializedExecutor, DataFrameStream};
use crate::{DataFrame, DataFrameError, Expr, Result};

/// A lazily-evaluated query backed by a `LogicalPlan`.
#[derive(Debug, Clone)]
pub struct LazyFrame {
    plan: LogicalPlan,
}

impl LazyFrame {
    /// Create a `LazyFrame` that scans an in-memory `DataFrame`.
    pub fn from_dataframe(df: DataFrame) -> Self {
        Self {
            plan: LogicalPlan::DataFrameScan { df },
        }
    }

    /// Build a CSV scan plan (no file I/O is performed until `collect()`).
    pub fn scan_csv(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::CsvScan {
                path: path.as_ref().to_path_buf(),
                predicate: None,
                projection: None,
            },
        })
    }

    /// Build a Parquet scan plan (no file I/O is performed until `collect()`).
    pub fn scan_parquet(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::ParquetScan {
                path: path.as_ref().to_path_buf(),
                predicate: None,
                projection: None,
            },
        })
    }

    /// Construct strict vertical concatenation from two or more lazy inputs.
    ///
    /// When every schema is already known, mismatch is reported at plan construction. For lazy
    /// file scans whose schema is intentionally unavailable until bounded open, the streaming
    /// executor preflights every child before its first output and rejects any mismatch then.
    pub fn concat(inputs: Vec<LazyFrame>) -> Result<Self> {
        let mut plans = Vec::with_capacity(inputs.len());
        let mut known_schemas = Vec::with_capacity(inputs.len());
        for input in inputs {
            known_schemas.push(known_plan_schema(&input.plan));
            plans.push(input.plan);
        }
        if known_schemas.iter().all(Option::is_some) {
            let plans = plans
                .into_iter()
                .zip(known_schemas)
                .map(|(plan, schema)| (plan, schema.expect("all schemas were checked")))
                .collect();
            return Ok(Self {
                plan: LogicalPlan::concat(plans)?,
            });
        }

        let mut first_known: Option<arrow::datatypes::SchemaRef> = None;
        for (index, schema) in known_schemas.iter().enumerate() {
            let Some(schema) = schema else {
                continue;
            };
            if let Some(first) = &first_known {
                if schema.as_ref() != first.as_ref() {
                    return Err(DataFrameError::schema_mismatch(format!(
                        "concat_schema_mismatch: known input {index} differs from another known input"
                    )));
                }
            } else {
                first_known = Some(schema.clone());
            }
        }
        Ok(Self {
            plan: LogicalPlan::concat_deferred(plans)?,
        })
    }

    /// Add a projection (`select`) node to the logical plan.
    pub fn select(self, exprs: Vec<Expr>) -> Self {
        Self {
            plan: LogicalPlan::Projection {
                input: Box::new(self.plan),
                exprs,
                kind: ProjectionKind::Select,
            },
        }
    }

    /// Add a filter node to the logical plan.
    pub fn filter(self, predicate: Expr) -> Self {
        Self {
            plan: LogicalPlan::Filter {
                input: Box::new(self.plan),
                predicate,
            },
        }
    }

    /// Add a projection (`with_columns`) node to the logical plan.
    pub fn with_columns(self, exprs: Vec<Expr>) -> Self {
        Self {
            plan: LogicalPlan::Projection {
                input: Box::new(self.plan),
                exprs,
                kind: ProjectionKind::WithColumns,
            },
        }
    }

    /// Start a group-by on this `LazyFrame`.
    pub fn group_by(self, by: Vec<Expr>) -> LazyGroupBy {
        LazyGroupBy {
            plan: self.plan,
            by,
        }
    }

    /// Join with another `LazyFrame` using provided join keys.
    pub fn join<K: Into<JoinKeys>>(self, other: LazyFrame, keys: K, how: JoinType) -> Self {
        let keys = keys.into();
        Self {
            plan: LogicalPlan::Join {
                left: Box::new(self.plan),
                right: Box::new(other.plan),
                keys,
                how,
            },
        }
    }

    /// Sort by one or more columns.
    pub fn sort(self, mut options: SortOptions) -> Self {
        options.nulls_last = true;
        options.stable = true;
        Self {
            plan: LogicalPlan::Sort {
                input: Box::new(self.plan),
                options,
            },
        }
    }

    /// Return the first `n` rows.
    pub fn head(self, n: usize) -> Self {
        Self {
            plan: LogicalPlan::Slice {
                input: Box::new(self.plan),
                offset: 0,
                len: n,
                from_end: false,
            },
        }
    }

    /// Return the last `n` rows.
    pub fn tail(self, n: usize) -> Self {
        Self {
            plan: LogicalPlan::Slice {
                input: Box::new(self.plan),
                offset: 0,
                len: n,
                from_end: true,
            },
        }
    }

    /// Remove duplicate rows.
    pub fn unique(self, subset: Option<Vec<String>>) -> Self {
        Self {
            plan: LogicalPlan::Unique {
                input: Box::new(self.plan),
                subset,
            },
        }
    }

    /// Fill null values using a scalar or strategy.
    pub fn fill_null<T: Into<FillNull>>(self, fill: T) -> Self {
        Self {
            plan: LogicalPlan::FillNull {
                input: Box::new(self.plan),
                fill: fill.into(),
            },
        }
    }

    /// Drop rows containing null values.
    pub fn drop_nulls(self, subset: Option<Vec<String>>) -> Self {
        Self {
            plan: LogicalPlan::DropNulls {
                input: Box::new(self.plan),
                subset,
            },
        }
    }

    /// Count null values per column.
    pub fn null_count(self) -> Self {
        Self {
            plan: LogicalPlan::NullCount {
                input: Box::new(self.plan),
            },
        }
    }

    /// Explode one `List<Utf8>` column into multiple rows.
    pub fn explode(self, column: impl Into<String>) -> Self {
        Self {
            plan: LogicalPlan::Explode {
                input: Box::new(self.plan),
                column: column.into(),
            },
        }
    }

    /// Implode UTF-8 columns into one row of `List<Utf8>` columns.
    pub fn implode(self) -> Self {
        Self {
            plan: LogicalPlan::Implode {
                input: Box::new(self.plan),
            },
        }
    }

    /// Optimize, compile, and execute this `LazyFrame` into an eager `DataFrame`.
    pub fn collect(self) -> Result<DataFrame> {
        let optimized = Optimizer::optimize(&self.plan);
        let physical = crate::physical::compile(&optimized)?;
        let batches = crate::physical::Executor::execute(physical)?;
        DataFrame::from_batches(batches)
    }

    /// Materialize a supported bounded stream while reserving every retained output batch.
    ///
    /// This deliberately uses the streaming source/operator path and never wraps the eager
    /// executor with accounting after allocation.
    pub fn collect_with_options(self, options: StreamOptions) -> Result<DataFrame> {
        let mut stream = self.collect_streaming(options)?;
        BudgetedMaterializedExecutor::new(stream.budget().clone()).collect_stream(&mut stream)
    }

    /// Compile this plan for incremental bounded execution.
    ///
    /// The compiler performs eligibility analysis before any source is opened. Until a source's
    /// v0.8 `BatchSourceFactory` is installed, it returns `StreamingUnsupported` rather than
    /// calling the legacy eager executor.
    pub fn collect_streaming(self, options: StreamOptions) -> Result<DataFrameStream> {
        let optimized = Optimizer::optimize(&self.plan);
        let physical = crate::physical::compile(&optimized)?;
        let compiled = crate::physical::compile_streaming(&physical, options)?;
        let context = BatchOpenContext::new(options);
        let source = crate::physical::open_streaming_plan(compiled, context.clone())?;
        Ok(DataFrameStream::from_opened_source(source, context.budget))
    }

    /// Render the logical plan as a human-readable string.
    ///
    /// If `optimized` is `true`, includes optimizer rewrites such as pushdowns.
    pub fn explain(self, optimized: bool) -> String {
        if optimized {
            Optimizer::optimize(&self.plan).display()
        } else {
            self.plan.display()
        }
    }
}

fn known_plan_schema(plan: &LogicalPlan) -> Option<arrow::datatypes::SchemaRef> {
    match plan {
        LogicalPlan::DataFrameScan { df } => Some(df.schema()),
        LogicalPlan::Concat { schema, .. } => schema.clone(),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Slice { input, .. }
        | LogicalPlan::Unique { input, .. }
        | LogicalPlan::FillNull { input, .. }
        | LogicalPlan::DropNulls { input, .. } => known_plan_schema(input),
        LogicalPlan::CsvScan { .. }
        | LogicalPlan::ParquetScan { .. }
        | LogicalPlan::Projection { .. }
        | LogicalPlan::Aggregate { .. }
        | LogicalPlan::Join { .. }
        | LogicalPlan::NullCount { .. }
        | LogicalPlan::Explode { .. }
        | LogicalPlan::Implode { .. } => None,
    }
}

/// Group-by builder for `LazyFrame`.
#[derive(Debug, Clone)]
pub struct LazyGroupBy {
    by: Vec<Expr>,
    plan: LogicalPlan,
}

impl LazyGroupBy {
    /// Add an aggregate node to the logical plan.
    pub fn agg(self, aggs: Vec<Expr>) -> LazyFrame {
        LazyFrame {
            plan: LogicalPlan::Aggregate {
                input: Box::new(self.plan),
                group_by: self.by,
                aggs,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};

    use super::LazyFrame;
    use crate::expr::{col, concat_str, lit, ConcatStrNullBehavior};
    use crate::physical::budget::StreamOptions;
    use crate::{DataFrame, Series};

    fn df() -> DataFrame {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        DataFrame::new(vec![
            Series::from_arrow("a", vec![a]).unwrap(),
            Series::from_arrow("b", vec![b]).unwrap(),
        ])
        .unwrap()
    }

    #[test]
    fn explain_builds_plan_without_io() {
        let lf = LazyFrame::scan_csv("test.csv").unwrap();
        let s = lf.explain(false);
        assert!(s.contains("scan[csv"));
    }

    #[test]
    fn collect_executes_filter_and_select_on_dataframe_scan() {
        let lf = LazyFrame::from_dataframe(df())
            .filter(col("a").gt(lit(1_i64)))
            .select(vec![col("b").alias("bb")]);
        let out = lf.collect().unwrap();
        assert_eq!(out.height(), 2);
        let bb = out.column("bb").unwrap();
        assert_eq!(bb.len(), 2);
    }

    #[test]
    fn group_by_agg_executes_sum_and_count() {
        let lf = LazyFrame::from_dataframe(df())
            .group_by(vec![col("a")])
            .agg(vec![
                col("b").sum().alias("sum_b"),
                col("b").count().alias("cnt_b"),
            ]);
        let out = lf.collect().unwrap();
        assert_eq!(out.width(), 3);
        assert_eq!(out.column("a").unwrap().len(), 3);
        assert_eq!(out.column("sum_b").unwrap().len(), 3);
        assert_eq!(out.column("cnt_b").unwrap().len(), 3);
    }

    #[test]
    fn collect_streaming_opens_the_bounded_csv_source_without_eager_collect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream.csv");
        std::fs::write(&path, "a\n1\n2\n").unwrap();
        let options = StreamOptions::new(
            16 * 1024,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );

        let mut stream = LazyFrame::scan_csv(&path)
            .unwrap()
            .collect_streaming(options)
            .unwrap();
        assert_eq!(stream.next_batch().unwrap().unwrap().height(), 1);
        assert_eq!(stream.next_batch().unwrap().unwrap().height(), 1);
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn collect_streaming_opens_the_bounded_parquet_source_without_eager_collect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream.parquet");
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
        )
        .unwrap();
        crate::io::write_parquet(&path, &DataFrame::from_batches(vec![batch]).unwrap()).unwrap();
        let options = StreamOptions::new(
            64 * 1024,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );

        let mut stream = LazyFrame::scan_parquet(&path)
            .unwrap()
            .collect_streaming(options)
            .unwrap();
        assert_eq!(stream.next_batch().unwrap().unwrap().height(), 1);
        assert_eq!(stream.next_batch().unwrap().unwrap().height(), 1);
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn lazy_concat_preflights_deferred_csv_schemas_then_preserves_input_order() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.csv");
        let second = dir.path().join("second.csv");
        std::fs::write(&first, "a\n1\n2\n").unwrap();
        std::fs::write(&second, "a\n3\n").unwrap();
        let options = StreamOptions::new(
            16 * 1024,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );

        let mut stream = LazyFrame::concat(vec![
            LazyFrame::scan_csv(&first).unwrap(),
            LazyFrame::scan_csv(&second).unwrap(),
        ])
        .unwrap()
        .collect_streaming(options)
        .unwrap();
        let mut values = Vec::new();
        while let Some(batch) = stream.next_batch().unwrap() {
            let batch = batch.to_arrow().pop().unwrap();
            let values_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.extend((0..values_array.len()).map(|index| values_array.value(index)));
        }
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn deferred_concat_schema_mismatch_fails_before_a_stream_is_returned() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.csv");
        let second = dir.path().join("second.csv");
        std::fs::write(&first, "a\n1\n").unwrap();
        std::fs::write(&second, "a\nnot-a-number\n").unwrap();
        let options = StreamOptions::new(
            16 * 1024,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );

        let result = LazyFrame::concat(vec![
            LazyFrame::scan_csv(&first).unwrap(),
            LazyFrame::scan_csv(&second).unwrap(),
        ])
        .unwrap()
        .collect_streaming(options);
        let err = match result {
            Ok(_) => panic!("mismatched deferred concat must fail before returning a stream"),
            Err(error) => error,
        };
        assert!(err.to_string().contains("concat_schema_mismatch"));
    }

    #[test]
    fn streaming_select_filter_concat_str_and_bounded_collect_share_batch_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operators.csv");
        std::fs::write(&path, "a,left,right\n1,x,y\n2,m,n\n3,p,q\n").unwrap();
        // This test asserts streaming and bounded collection semantics rather
        // than a particular reservation threshold. Dedicated budget tests
        // exercise rejection at constrained limits.
        let options = StreamOptions::default();
        let repeated = col("a").add(lit(1_i64));
        let label = concat_str(
            vec![col("left"), col("right")],
            "-",
            ConcatStrNullBehavior::Propagate,
        )
        .unwrap()
        .alias("label");
        let plan = LazyFrame::scan_csv(&path)
            .unwrap()
            .filter(col("a").gt(lit(1_i64)))
            .select(vec![
                repeated.clone().alias("next"),
                repeated.alias("next_again"),
                label,
            ]);

        let mut stream = plan.clone().collect_streaming(options).unwrap();
        let mut rows = Vec::new();
        while let Some(dataframe) = stream.next_batch().unwrap() {
            let batch = dataframe.to_arrow().pop().unwrap();
            let next = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let repeated = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let label = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for index in 0..batch.num_rows() {
                rows.push((
                    next.value(index),
                    repeated.value(index),
                    label.value(index).to_string(),
                ));
            }
        }
        assert_eq!(rows, vec![(3, 3, "m-n".into()), (4, 4, "p-q".into())]);

        let materialized = plan.collect_with_options(options).unwrap();
        assert_eq!(materialized.height(), 2);
        assert_eq!(materialized.schema().fields()[2].name(), "label");
    }

    #[test]
    fn streaming_with_columns_and_forward_head_preserve_schema_and_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("with-columns.csv");
        std::fs::write(&path, "a\n1\n2\n3\n").unwrap();
        let options = StreamOptions::new(
            16 * 1024,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );

        let mut stream = LazyFrame::scan_csv(&path)
            .unwrap()
            .with_columns(vec![col("a").add(lit(10_i64)).alias("next")])
            .head(2)
            .collect_streaming(options)
            .unwrap();
        let mut values = Vec::new();
        while let Some(dataframe) = stream.next_batch().unwrap() {
            let batch = dataframe.to_arrow().pop().unwrap();
            assert_eq!(batch.schema().fields()[1].name(), "next");
            let next = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.extend((0..next.len()).map(|index| next.value(index)));
        }
        assert_eq!(values, vec![11, 12]);
    }

    #[test]
    fn known_lazy_concat_schema_mismatch_fails_during_plan_construction() {
        let first = df().lazy();
        let incompatible = DataFrame::new(vec![Series::from_arrow(
            "other",
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap()])
        .unwrap()
        .lazy();

        let err = LazyFrame::concat(vec![first, incompatible]).unwrap_err();
        assert!(err.to_string().contains("concat_schema_mismatch"));
    }
}
