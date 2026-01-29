use std::sync::Arc;

use alopex_dataframe::expr::{col, lit};
use alopex_dataframe::lazy::{LogicalPlan, Optimizer, ProjectionKind};
use alopex_dataframe::physical::{compile, Executor};
use alopex_dataframe::{write_parquet, DataFrame, LazyFrame, Series};
use arrow::array::{ArrayRef, Int64Array, StringArray};

fn execute(plan: &LogicalPlan) -> Vec<arrow::record_batch::RecordBatch> {
    let physical = compile(plan).unwrap();
    Executor::execute(physical).unwrap()
}

#[test]
fn pushdown_equivalence_csv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.csv");

    std::fs::write(&path, "a,b,c\n1,x,10\n2,y,20\n3,z,30\n").unwrap();

    let lf = LazyFrame::scan_csv(&path)
        .unwrap()
        .filter(col("a").gt(lit(1_i64)))
        .select(vec![col("b")]);

    let unoptimized_explain = lf.clone().explain(false);
    assert!(unoptimized_explain.contains("scan[csv"));
    assert!(!unoptimized_explain.contains("projection="));
    assert!(!unoptimized_explain.contains("filters=["));

    let optimized_explain = lf.clone().explain(true);
    assert!(optimized_explain.contains("scan[csv"));
    assert!(optimized_explain.contains("projection="));
    assert!(optimized_explain.contains("filters=["));

    let plan = LogicalPlan::Projection {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(LogicalPlan::CsvScan {
                path: path.clone(),
                predicate: None,
                projection: None,
            }),
            predicate: col("a").gt(lit(1_i64)),
        }),
        exprs: vec![col("b")],
        kind: ProjectionKind::Select,
    };

    let unoptimized = execute(&plan);
    let optimized_plan = Optimizer::optimize(&plan);
    let optimized = execute(&optimized_plan);

    assert_eq!(unoptimized, optimized);
}

#[test]
fn pushdown_equivalence_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.parquet");

    let a: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let c: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "z"]));

    let df = DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
        Series::from_arrow("c", vec![c]).unwrap(),
    ])
    .unwrap();

    write_parquet(&path, &df).unwrap();

    let lf = LazyFrame::scan_parquet(&path)
        .unwrap()
        .filter(col("c").gt(lit(10_i64)))
        .select(vec![col("a"), col("b")]);

    let unoptimized_explain = lf.clone().explain(false);
    assert!(unoptimized_explain.contains("scan[parquet"));
    assert!(!unoptimized_explain.contains("projection="));
    assert!(!unoptimized_explain.contains("filters=["));

    let optimized_explain = lf.clone().explain(true);
    assert!(optimized_explain.contains("scan[parquet"));
    assert!(optimized_explain.contains("projection="));
    assert!(optimized_explain.contains("filters=["));

    let plan = LogicalPlan::Projection {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(LogicalPlan::ParquetScan {
                path: path.clone(),
                predicate: None,
                projection: None,
            }),
            predicate: col("c").gt(lit(10_i64)),
        }),
        exprs: vec![col("a"), col("b")],
        kind: ProjectionKind::Select,
    };

    let unoptimized = execute(&plan);
    let optimized_plan = Optimizer::optimize(&plan);
    let optimized = execute(&optimized_plan);

    assert_eq!(unoptimized, optimized);
}
