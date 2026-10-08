//! Prepared SQL execution only; standalone manifests are produced by the runner.
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn sql(case: &str) -> Result<&'static str> {
    Ok(match case {
        "ordinary_update" => "UPDATE hw SET v=v+1",
        "scalar_update" => "UPDATE hw SET v=(SELECT MAX(v) FROM source)+1",
        "membership_delete" => "DELETE FROM hw WHERE id IN (SELECT id FROM source)",
        "correlated_update" => "UPDATE hw SET v=(SELECT MAX(s.v) FROM source s WHERE s.id<=hw.id)",
        "two_membership_delete" => {
            "DELETE FROM hw WHERE id IN (SELECT id FROM source) AND id IN (SELECT id+0 FROM source)"
        }
        _ => return Err(format!("unknown case {case}").into()),
    })
}

fn fixture(n: i32) -> Vec<String> {
    let mut statements = vec![
        "CREATE TABLE hw(id INTEGER PRIMARY KEY,v INTEGER)".into(),
        "CREATE TABLE source(id INTEGER PRIMARY KEY,v INTEGER)".into(),
    ];
    for table in ["hw", "source"] {
        for start in (1..=n).step_by(128) {
            let values = (start..=(start + 127).min(n))
                .map(|id| format!("({id},{})", id + if table == "source" { n } else { 0 }))
                .collect::<Vec<_>>()
                .join(",");
            statements.push(format!("INSERT INTO {table} VALUES {values}"));
        }
    }
    statements
}

fn expected(case: &str, n: i32) -> Vec<(i64, i64)> {
    if case.ends_with("delete") {
        return Vec::new();
    }
    (1..=n)
        .map(|id| {
            (
                i64::from(id),
                i64::from(match case {
                    "ordinary_update" => id + 1,
                    "scalar_update" => 2 * n + 1,
                    "correlated_update" => n + id,
                    _ => unreachable!(),
                }),
            )
        })
        .collect()
}

fn record(
    file: &mut std::fs::File,
    engine: &str,
    case: &str,
    n: i32,
    iteration: usize,
    elapsed_ns: u64,
    affected: u64,
    valid: bool,
) -> Result<()> {
    serde_json::to_writer(
        &mut *file,
        &serde_json::json!({
            "kind":"sample", "engine":engine, "case":case, "rows":n,
            "phase":"prepared_execute", "iteration":iteration,
            "warmup":iteration == 0, "elapsed_ns":elapsed_ns,
            "affected_rows":affected, "result_check":valid,
            "status":if valid { "pass" } else { "fail" }
        }),
    )?;
    writeln!(file)?;
    file.flush()?;
    if !valid {
        return Err("result mismatch".into());
    }
    Ok(())
}

#[cfg(feature = "engine-alopex")]
fn run(engine: &str, case: &str, n: i32, file: &mut std::fs::File) -> Result<()> {
    use alopex_core::kv::{KVStore, KVTransaction, memory::MemoryKV};
    use alopex_core::types::TxnMode;
    use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog};
    use alopex_sql::{
        AlopexDialect, ExecutionResult, Executor, Parser, Planner, SqlValue, TxnBridge,
    };
    use std::sync::{Arc, RwLock};
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    let plan = |text: &str| -> Result<alopex_sql::LogicalPlan> {
        let statements = Parser::parse_sql(&AlopexDialect, text)?;
        Ok(Planner::new(&*catalog.read().unwrap()).plan(&statements[0])?)
    };
    for statement in fixture(n) {
        executor.execute(plan(&statement)?)?;
    }
    let prepared = plan(sql(case)?)?;
    let check = plan("SELECT id,v FROM hw ORDER BY id")?;
    let expected = expected(case, n);
    for iteration in 0..8 {
        let execution = prepared.clone(); // Ownership transfer is outside measurement.
        let mut transaction = store.begin(TxnMode::ReadWrite)?;
        let mut overlay = CatalogOverlay::new();
        let mut borrowed = TxnBridge::<MemoryKV>::wrap_external(
            &mut transaction,
            TxnMode::ReadWrite,
            &mut overlay,
        );
        let start = Instant::now();
        let result = executor.execute_in_txn(execution, &mut borrowed)?;
        let affected = match result {
            ExecutionResult::RowsAffected(count) => count,
            _ => return Err("DML returned unexpected result".into()),
        };
        std::hint::black_box(affected);
        let elapsed = start.elapsed().as_nanos() as u64;
        let ExecutionResult::Query(query) =
            executor.execute_in_txn(check.clone(), &mut borrowed)?
        else {
            return Err("verification did not return rows".into());
        };
        let actual = query
            .rows
            .iter()
            .map(|row| match row.as_slice() {
                [SqlValue::Integer(id), SqlValue::Integer(value)] => {
                    Ok((i64::from(*id), i64::from(*value)))
                }
                _ => Err("unexpected verification value"),
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let valid = affected == n as u64 && actual == expected;
        drop(borrowed);
        transaction.rollback_self()?;
        record(file, engine, case, n, iteration, elapsed, affected, valid)?;
    }
    Ok(())
}

#[cfg(feature = "engine-sqlite")]
fn run(engine: &str, case: &str, n: i32, file: &mut std::fs::File) -> Result<()> {
    let connection = rusqlite::Connection::open_in_memory()?;
    connection
        .execute_batch("PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY; PRAGMA threads=1;")?;
    for statement in fixture(n) {
        connection.execute_batch(&statement)?;
    }
    let options = connection
        .prepare("PRAGMA compile_options")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    serde_json::to_writer(
        &mut *file,
        &serde_json::json!({"kind":"reference", "version":rusqlite::version(), "compile_options":options}),
    )?;
    writeln!(file)?;
    file.flush()?;
    let mut prepared = connection.prepare(sql(case)?)?;
    let mut check = connection.prepare("SELECT id,v FROM hw ORDER BY id")?;
    let expected = expected(case, n);
    for iteration in 0..8 {
        connection.execute_batch("BEGIN")?;
        let start = Instant::now();
        let affected = prepared.execute([])? as u64;
        std::hint::black_box(affected);
        let elapsed = start.elapsed().as_nanos() as u64;
        let actual = check
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let valid = affected == n as u64 && actual == expected;
        connection.execute_batch("ROLLBACK")?;
        record(file, engine, case, n, iteration, elapsed, affected, valid)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: dml_statement_perf ENGINE CASE ROWS RAW_JSONL".into());
    }
    let n: i32 = args[2].parse()?;
    if ![600, 1025].contains(&n) {
        return Err("rows must be 600 or 1025".into());
    }
    sql(&args[1])?;
    #[cfg(feature = "engine-alopex")]
    if !["baseline", "fixed"].contains(&args[0].as_str()) {
        return Err("engine feature mismatch".into());
    }
    #[cfg(feature = "engine-sqlite")]
    if args[0] != "sqlite" {
        return Err("engine feature mismatch".into());
    }
    // The runner owns create_new and writes immutable provenance before launch.
    let mut file = OpenOptions::new().append(true).open(&args[3])?;
    run(&args[0], &args[1], n, &mut file)
}
