use std::num::NonZeroUsize;
use std::sync::Arc;

use alopex_dataframe::expr::{col, lit};
use alopex_dataframe::physical::budget::ResourceBudget;
use alopex_dataframe::physical::BudgetedMaterializedExecutor;
use alopex_dataframe::{DataFrame, DataFrameError, Series};
use arrow::array::{ArrayRef, Int64Array};

fn df() -> DataFrame {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
    DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
    ])
    .unwrap()
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn column_resolution_is_case_sensitive() {
    let df = df();
    let err = df.column("A").unwrap_err();
    assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn select_preserves_input_expression_order() {
    let df = df();
    let out = df.select(vec![col("b"), col("a")]).unwrap();
    let names: Vec<_> = out
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["b", "a"]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn filter_has_expected_semantics() {
    let df = df();
    let out = df.filter(col("a").gt(lit(1_i64))).unwrap();
    assert_eq!(out.height(), 2);
    assert_eq!(out.schema().fields()[0].name(), "a");
    assert_eq!(out.schema().fields()[1].name(), "b");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lazy_collect_and_collect_batches_materialize_polars_compatible_results() {
    let lazy = df().lazy().filter(col("a").gt(lit(1_i64)));
    let collected = lazy.clone().collect().unwrap();
    assert_eq!(collected.height(), 2);

    let budget = ResourceBudget::new(1024 * 1024, NonZeroUsize::new(2).unwrap());
    let materialized = BudgetedMaterializedExecutor::new(budget)
        .collect_batches(collected.to_arrow().into_iter().map(Ok))
        .unwrap();
    assert_eq!(materialized.height(), 2);
}
