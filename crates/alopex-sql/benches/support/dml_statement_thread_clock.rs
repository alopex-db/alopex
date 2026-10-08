//! Diagnostic-only clocks; copied public-API fixture from dml_statement_perf.rs.
use std::fs::OpenOptions;
use std::io::Write;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// Diagnostic clocks bracket only the same public SQL execution as the acceptance harness.
#[derive(Clone, Copy)]
struct Clocks {
    wall_ns: u64,
    thread_cpu_ns: u64,
}

fn clock_ns(clock: libc::clockid_t, name: &str) -> Result<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable, and callers supply a clock_gettime clock ID.
    if unsafe { libc::clock_gettime(clock, &mut value) } != 0 {
        return Err(format!("{name} clock failed: {}", std::io::Error::last_os_error()).into());
    }
    let seconds = u64::try_from(value.tv_sec).map_err(|_| format!("negative {name} clock"))?;
    let nanos = u64::try_from(value.tv_nsec).map_err(|_| format!("negative {name} nanoseconds"))?;
    if nanos >= 1_000_000_000 {
        return Err(format!("invalid {name} timespec").into());
    }
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nanos))
        .ok_or_else(|| format!("{name} clock overflow").into())
}

fn measured<T>(operation: impl FnOnce() -> Result<T>) -> Result<(T, Clocks)> {
    let cpu_start = clock_ns(libc::CLOCK_THREAD_CPUTIME_ID, "thread CPU")?;
    let wall_start = clock_ns(libc::CLOCK_MONOTONIC, "monotonic wall")?;
    let result = operation();
    let wall_end = clock_ns(libc::CLOCK_MONOTONIC, "monotonic wall")?;
    let cpu_end = clock_ns(libc::CLOCK_THREAD_CPUTIME_ID, "thread CPU")?;
    let clocks = Clocks {
        wall_ns: wall_end
            .checked_sub(wall_start)
            .ok_or("non-monotonic wall clock")?,
        thread_cpu_ns: cpu_end
            .checked_sub(cpu_start)
            .ok_or("non-monotonic thread CPU clock")?,
    };
    Ok((result?, clocks))
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

fn expected(n: i32) -> Vec<(i64, i64)> {
    (1..=n)
        .map(|id| (i64::from(id), i64::from(id + 1)))
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
    clocks: Clocks,
    empty: Clocks,
) -> Result<()> {
    serde_json::to_writer(
        &mut *file,
        &serde_json::json!({
            "kind":"sample", "engine":engine, "case":case, "rows":n,
            "phase":"prepared_execute", "iteration":iteration,
            "warmup":iteration == 0, "elapsed_ns":elapsed_ns,
            "purpose":"diagnostic_thread_cpu", "thread_cpu_ns":clocks.thread_cpu_ns,
            "empty_thread_cpu_ns":empty.thread_cpu_ns, "empty_wall_ns":empty.wall_ns,
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
    let prepared = plan("UPDATE hw SET v=v+1")?;
    let check = plan("SELECT id,v FROM hw ORDER BY id")?;
    let expected = expected(n);
    for iteration in 0..8 {
        let execution = prepared.clone(); // Ownership transfer is outside measurement.
        let mut transaction = store.begin(TxnMode::ReadWrite)?;
        let mut overlay = CatalogOverlay::new();
        let mut borrowed = TxnBridge::<MemoryKV>::wrap_external(
            &mut transaction,
            TxnMode::ReadWrite,
            &mut overlay,
        );
        let (_, empty) = measured(|| Ok(std::hint::black_box(())))?;
        let (affected, clocks) = measured(|| {
            let result = executor.execute_in_txn(execution, &mut borrowed)?;
            let affected = match result {
                ExecutionResult::RowsAffected(count) => count,
                _ => return Err("DML returned unexpected result".into()),
            };
            Ok(std::hint::black_box(affected))
        })?;
        if clocks.wall_ns == 0 || clocks.thread_cpu_ns == 0 {
            return Err("non-positive diagnostic duration".into());
        }
        let elapsed = clocks.wall_ns;
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
        record(
            file, engine, case, n, iteration, elapsed, affected, valid, clocks, empty,
        )?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: dml_statement_thread_clock ENGINE CASE ROWS RAW_JSONL".into());
    }
    let n: i32 = args[2].parse()?;
    if n != 600 || args[1] != "ordinary_update" {
        return Err("diagnostic accepts ordinary_update / 600 only".into());
    }
    if !["baseline", "fixed"].contains(&args[0].as_str()) {
        return Err("diagnostic accepts baseline or fixed only".into());
    }
    // The runner owns create_new and writes immutable provenance before launch.
    let mut file = OpenOptions::new().append(true).open(&args[3])?;
    serde_json::to_writer(
        &mut file,
        &serde_json::json!({"kind":"diagnostic", "purpose":"diagnostic_thread_cpu", "acceptance_replacement":false}),
    )?;
    writeln!(file)?;
    file.flush()?;
    run(&args[0], &args[1], n, &mut file)
}
