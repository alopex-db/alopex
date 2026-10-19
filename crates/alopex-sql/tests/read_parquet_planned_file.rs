use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

fn write_value(path: &Path, value: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn planned_parquet_does_not_follow_same_schema_file_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("input.parquet");
    let replacement = directory.path().join("replacement.parquet");
    write_value(&input, 11);
    write_value(&replacement, 99);

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let statement = Parser::parse_sql(
        &AlopexDialect,
        &format!("SELECT * FROM read_parquet('{}')", input.display()),
    )
    .unwrap()
    .remove(0);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let ExecutionResult::Query(original) = executor.execute(plan.clone()).unwrap() else {
        panic!("expected the planned file to produce rows");
    };
    assert_eq!(original.rows, vec![vec![SqlValue::BigInt(11)]]);

    std::fs::rename(&input, directory.path().join("original.parquet")).unwrap();
    std::fs::rename(&replacement, &input).unwrap();

    // A new plan proves the replacement is valid and has the same SQL schema.
    let fresh_plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let ExecutionResult::Query(fresh) = executor.execute(fresh_plan).unwrap() else {
        panic!("expected the newly planned replacement to produce rows");
    };
    assert_eq!(fresh.rows, vec![vec![SqlValue::BigInt(99)]]);

    let error = executor
        .execute(plan)
        .expect_err("a planned input must reject a path replacement even without a runtime policy");
    assert!(matches!(error, ExecutorError::PathValidationFailed { .. }));
}

#[test]
fn cloned_parquet_plans_keep_independent_read_positions() {
    use alopex_sql::executor::query::execute_query_with_policy;
    use alopex_sql::executor::{MemoryPolicy, SpillPolicy};
    use alopex_sql::storage::TxnBridge;
    use std::sync::Barrier;

    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("shared.parquet");
    write_value(&input, 37);
    let catalog = MemoryCatalog::new();
    let statement = Parser::parse_sql(
        &AlopexDialect,
        &format!("SELECT * FROM read_parquet('{}')", input.display()),
    )
    .unwrap()
    .remove(0);
    let plan = Planner::new(&catalog).plan(&statement).unwrap();
    let barrier = Barrier::new(4);

    // Clones retain the same open file. Finite reads exercise metadata/page
    // preflight seeks while unbounded reads exercise the ordinary Arrow reader.
    std::thread::scope(|scope| {
        let workers = (0..4)
            .map(|worker| {
                let plan = plan.clone();
                let barrier = &barrier;
                scope.spawn(move || {
                    let catalog = MemoryCatalog::new();
                    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
                    let mut txn = bridge.begin_write().unwrap();
                    let policy = MemoryPolicy::new(Some(1024 * 1024), SpillPolicy::FailFast);
                    let mut failures = Vec::new();
                    for iteration in 0..8 {
                        barrier.wait();
                        let memory = ((worker + iteration) % 2 == 0).then_some(&policy);
                        let result =
                            execute_query_with_policy(&mut txn, &catalog, plan.clone(), memory);
                        // Do not panic before every worker leaves the barrier loop.
                        let rows = result.map(|result| match result {
                            ExecutionResult::Query(query) => query.rows,
                            _ => Vec::new(),
                        });
                        if rows.as_ref().ok() != Some(&vec![vec![SqlValue::BigInt(37)]]) {
                            failures
                                .push(format!("worker {worker}, iteration {iteration}: {rows:?}"));
                        }
                    }
                    failures
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert!(worker.join().unwrap().is_empty());
        }
    });
}
