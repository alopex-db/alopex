use std::path::Path;

use crate::lazy::{LogicalPlan, Optimizer, ProjectionKind};
use crate::{DataFrame, Expr, Result};

#[derive(Debug, Clone)]
pub struct LazyFrame {
    plan: LogicalPlan,
}

impl LazyFrame {
    pub fn from_dataframe(df: DataFrame) -> Self {
        Self {
            plan: LogicalPlan::DataFrameScan { df },
        }
    }

    pub fn scan_csv(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::CsvScan {
                path: path.as_ref().to_path_buf(),
                predicate: None,
                projection: None,
            },
        })
    }

    pub fn scan_parquet(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::ParquetScan {
                path: path.as_ref().to_path_buf(),
                predicate: None,
                projection: None,
            },
        })
    }

    pub fn select(self, exprs: Vec<Expr>) -> Self {
        Self {
            plan: LogicalPlan::Projection {
                input: Box::new(self.plan),
                exprs,
                kind: ProjectionKind::Select,
            },
        }
    }

    pub fn filter(self, predicate: Expr) -> Self {
        Self {
            plan: LogicalPlan::Filter {
                input: Box::new(self.plan),
                predicate,
            },
        }
    }

    pub fn with_columns(self, exprs: Vec<Expr>) -> Self {
        Self {
            plan: LogicalPlan::Projection {
                input: Box::new(self.plan),
                exprs,
                kind: ProjectionKind::WithColumns,
            },
        }
    }

    pub fn group_by(self, by: Vec<Expr>) -> LazyGroupBy {
        LazyGroupBy {
            plan: self.plan,
            by,
        }
    }

    pub fn collect(self) -> Result<DataFrame> {
        let optimized = Optimizer::optimize(&self.plan);
        let physical = crate::physical::compile(&optimized)?;
        let batches = crate::physical::Executor::execute(physical)?;
        DataFrame::from_batches(batches)
    }

    pub fn explain(self, optimized: bool) -> String {
        if optimized {
            Optimizer::optimize(&self.plan).display()
        } else {
            self.plan.display()
        }
    }
}

#[derive(Debug, Clone)]
pub struct LazyGroupBy {
    by: Vec<Expr>,
    plan: LogicalPlan,
}

impl LazyGroupBy {
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
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};

    use super::LazyFrame;
    use crate::expr::{col, lit};
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
}
