use std::error::Error;
use std::fs;
use std::path::Path;
#[cfg(any(feature = "alopex", feature = "reference"))]
use std::time::Instant;

use serde::Deserialize;
#[cfg(any(feature = "alopex", feature = "reference"))]
use sha2::{Digest, Sha256};

#[cfg(all(feature = "alopex", feature = "reference"))]
compile_error!("Build one engine adapter per executable");

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Deserialize)]
struct Profile {
    #[cfg(any(feature = "alopex", feature = "reference"))]
    warmup_runs: usize,
    #[cfg(any(feature = "alopex", feature = "reference"))]
    measurement_runs: usize,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    rows: usize,
    columns: usize,
    #[cfg(feature = "fixtures")]
    row_group_rows: usize,
    #[cfg(feature = "fixtures")]
    field_metadata_entries: usize,
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 4 {
        return Err("usage: binary generate PROFILE DIRECTORY | ENGINE PROFILE CASE DIRECTORY POLICY OUTPUT".into());
    }
    let profile: Profile = serde_json::from_slice(&fs::read(&args[2])?)?;
    #[cfg(feature = "fixtures")]
    if args[1] == "generate" {
        return generate(&profile, Path::new(&args[3]));
    }
    #[cfg(any(feature = "alopex", feature = "reference"))]
    {
        run(&args, &profile)
    }
    #[cfg(not(any(feature = "alopex", feature = "reference")))]
    Err("enable an engine adapter".into())
}

#[cfg(any(feature = "alopex", feature = "reference"))]
fn run(args: &[String], profile: &Profile) -> Result<()> {
    if cfg!(debug_assertions) || profile.warmup_runs != 3 || profile.measurement_runs != 21 {
        return Err("the frozen release profile requires 3 warmups and 21 measurements".into());
    }
    let status = fs::read_to_string("/proc/self/status")?;
    let affinity = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .ok_or("CPU affinity unavailable")?
        .trim();
    if affinity != "0" {
        return Err("the frozen profile requires CPU affinity 0".into());
    }
    if args.len() != 7 {
        return Err("one case, policy and output are required".into());
    }
    let case = profile
        .cases
        .iter()
        .find(|case| case.id == args[3])
        .ok_or("unknown case")?;
    let path = Path::new(&args[4])
        .join(format!("{}.parquet", case.id))
        .canonicalize()?;
    let fixture_sha256 = format!("{:x}", Sha256::digest(fs::read(&path)?));
    let limit = if args[5] == "none" {
        None
    } else {
        Some(args[5].parse::<u64>()?)
    };
    if ![None, Some(268_435_456)].contains(&limit) {
        return Err("policy is outside the frozen matrix".into());
    }
    if path.to_string_lossy().contains('\'') {
        return Err("fixture path cannot contain SQL quotes".into());
    }
    let sql = format!("SELECT * FROM read_parquet('{}')", path.display());

    #[cfg(feature = "alopex")]
    let durations = run_alopex(&args[1], profile, case, &sql, limit)?;
    #[cfg(feature = "reference")]
    let durations = run_reference(&args[1], profile, case, &sql, limit)?;

    let mut sorted = durations.clone();
    sorted.sort_by(f64::total_cmp);
    let p50 = sorted[sorted.len() / 2];
    let p95 = sorted[(sorted.len() * 95).div_ceil(100) - 1];
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let rss = u64::try_from(unsafe { usage.assume_init() }.ru_maxrss)?;
    if rss == 0 {
        return Err("RSS observation unavailable".into());
    }
    let rss = if cfg!(target_os = "macos") {
        rss
    } else {
        rss * 1024
    };
    let result = serde_json::json!({
        "schema": "alopex.parquet-comparison-result/v1",
        "engine": args[1], "case": case.id, "memory_limit_bytes": limit,
        "fixture_sha256": fixture_sha256, "rows": case.rows, "columns": case.columns,
        "profile_sha256": format!("{:x}", Sha256::digest(fs::read(&args[2])?)),
        "cpu_affinity": [0], "thread_count": 1,
        "phase": "public_sql_owned_rows", "build_profile": "release",
        "warmup_runs": profile.warmup_runs, "measurement_runs": profile.measurement_runs,
        "cache_state": "warm", "latency_samples_ms": durations,
        "latency_p50_ms": p50, "latency_p95_ms": p95,
        "rows_per_second": case.rows as f64 * 1000.0 / p50,
        "peak_rss_bytes": rss, "values_verified": true
    });
    // Persist each real case before another process can start.
    let output = Path::new(&args[6]);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    serde_json::to_writer_pretty(&mut file, &result)?;
    file.sync_all()?;
    println!("{}", result);
    Ok(())
}

#[cfg(any(feature = "alopex", feature = "reference"))]
fn measure<T>(
    profile: &Profile,
    mut query: impl FnMut() -> Result<T>,
    check: impl Fn(&T) -> Result<()>,
) -> Result<Vec<f64>> {
    let mut durations = Vec::with_capacity(profile.measurement_runs);
    for index in 0..profile.warmup_runs + profile.measurement_runs {
        let start = Instant::now();
        let rows = query()?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        check(&rows)?;
        if index >= profile.warmup_runs {
            durations.push(elapsed);
        }
    }
    Ok(durations)
}

#[cfg(feature = "alopex")]
fn run_alopex(
    engine: &str,
    profile: &Profile,
    case: &Case,
    sql: &str,
    limit: Option<u64>,
) -> Result<Vec<f64>> {
    use alopex_core::kv::{AsyncKVStore, async_adapter::AsyncKVStoreAdapter, memory::MemoryKV};
    use alopex_core::types::TxnMode;
    use alopex_sql::catalog::{Catalog, MemoryCatalog};
    use alopex_sql::executor::{ExecutionResult, MemoryPolicy, SpillPolicy};
    use alopex_sql::storage::{
        SqlValue,
        async_storage::{AsyncSqlTransaction, AsyncTxnBridge},
    };
    use std::sync::{Arc, RwLock};
    if !["before", "after"].contains(&engine) {
        return Err("wrong engine adapter".into());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(2)
        .build()?;
    let store = AsyncKVStoreAdapter::from_arc(Arc::new(MemoryKV::new()), TxnMode::ReadWrite);
    let transaction = runtime.block_on(store.begin_async())?;
    let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> =
        Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut bridge = AsyncTxnBridge::with_catalog(transaction, TxnMode::ReadWrite, catalog);
    bridge.set_memory_policy(
        limit.map(|value| MemoryPolicy::new(Some(value), SpillPolicy::FailFast)),
    );
    measure(
        profile,
        || {
            let ExecutionResult::Query(result) = runtime.block_on(bridge.async_execute(sql))?
            else {
                return Err("expected rows".into());
            };
            Ok(result)
        },
        |result| {
            if result.rows.len() != case.rows || result.columns.len() != case.columns {
                return Err("result shape mismatch".into());
            }
            if result
                .columns
                .iter()
                .enumerate()
                .any(|(index, column)| column.name != format!("c{index}"))
                || result.rows.iter().any(|row| row.len() != case.columns)
            {
                return Err("column metadata mismatch".into());
            }
            check_values(case, |row, column| match result.rows[row].get(column) {
                Some(SqlValue::BigInt(value)) => Some(*value),
                _ => None,
            })
        },
    )
}

#[cfg(feature = "reference")]
fn run_reference(
    engine: &str,
    profile: &Profile,
    case: &Case,
    sql: &str,
    limit: Option<u64>,
) -> Result<Vec<f64>> {
    if engine != "duckdb" {
        return Err("wrong engine adapter".into());
    }
    let connection = duckdb::Connection::open_in_memory()?;
    let version: String = connection.query_row("SELECT version()", [], |row| row.get(0))?;
    if version != "v1.4.0" {
        return Err(format!("reference version mismatch: {version}").into());
    }
    connection.execute_batch("SET threads = 1")?;
    let memory = limit.map_or_else(|| "-1".to_owned(), |value| format!("{value}B"));
    connection.execute_batch(&format!("SET memory_limit = '{memory}'"))?;
    measure(
        profile,
        || {
            let mut statement = connection.prepare(sql)?;
            let rows = statement
                .query_map([], |row| {
                    (0..case.columns)
                        .map(|column| row.get::<_, i64>(column))
                        .collect::<duckdb::Result<Vec<_>>>()
                })?
                .collect::<duckdb::Result<Vec<_>>>()?;
            let names = statement.column_names();
            Ok((names, rows))
        },
        |(names, rows)| {
            if names.len() != case.columns || rows.len() != case.rows {
                return Err("reference shape mismatch".into());
            }
            if names
                .iter()
                .enumerate()
                .any(|(index, name)| *name != format!("c{index}"))
                || rows.iter().any(|row| row.len() != case.columns)
            {
                return Err("reference columns mismatch".into());
            }
            check_values(case, |row, column| rows[row].get(column).copied())
        },
    )
}

#[cfg(any(feature = "alopex", feature = "reference"))]
fn check_values(case: &Case, value: impl Fn(usize, usize) -> Option<i64>) -> Result<()> {
    let mut seen = vec![false; case.rows];
    for row in 0..case.rows {
        let key = value(row, 0).ok_or("missing row key")?;
        if key < 0 || key % 1000 != 0 {
            return Err("invalid row key".into());
        }
        let logical = usize::try_from(key / 1000)?;
        if logical >= case.rows || std::mem::replace(&mut seen[logical], true) {
            return Err("missing or duplicated row".into());
        }
        for column in 0..case.columns {
            if value(row, column) != Some((logical * 1000 + column) as i64) {
                return Err("value mismatch".into());
            }
        }
    }
    Ok(())
}

#[cfg(feature = "fixtures")]
fn generate(profile: &Profile, directory: &Path) -> Result<()> {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use std::sync::Arc;
    fs::create_dir_all(directory)?;
    for case in &profile.cases {
        let fields = (0..case.columns)
            .map(|column| {
                let metadata = (0..case.field_metadata_entries)
                    .map(|index| (format!("k{index:04}"), String::new()))
                    .collect();
                Field::new(format!("c{column}"), DataType::Int64, false).with_metadata(metadata)
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let arrays = (0..case.columns)
            .map(|column| {
                Arc::new(Int64Array::from(
                    (0..case.rows)
                        .map(|row| (row * 1000 + column) as i64)
                        .collect::<Vec<_>>(),
                )) as _
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{}.parquet", case.id)))?;
        let properties = WriterProperties::builder()
            .set_max_row_group_size(case.row_group_rows)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(())
}
