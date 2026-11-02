//! Manual #684 native index/resource diagnostic; never execute from ordinary CI.
//! The runner prepares each 1,000-row fixture in a separate process.
#[cfg(target_os = "macos")]
mod diagnostic {
    use alopex_embedded::{Database, DatabaseOptions, TxnMode};
    use alopex_sql::{executor::ExecutionResult, SqlValue};
    use serde::Serialize;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::{
        error::Error, fs, os::unix::fs::MetadataExt, path::Path, process::Command, sync::Arc,
        time::Instant,
    };

    type Result<T> = std::result::Result<T, Box<dyn Error>>;
    const ROWS: i32 = 1000;

    #[derive(Serialize)]
    struct Snapshot {
        rss_bytes: u64,
        allocated_bytes: u64,
        logical_bytes: u64,
    }

    fn snapshot(path: &Path) -> Result<Snapshot> {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()?;
        if !output.status.success() {
            return Err("RSS measurement failed".into());
        }
        let rss_bytes = std::str::from_utf8(&output.stdout)?.trim().parse::<u64>()? * 1024;
        if rss_bytes == 0 || rss_bytes >= 2 * 1024_u64.pow(3) {
            return Err("RSS unavailable or reached 2 GiB stop".into());
        }
        let mut pending = vec![path.parent().ok_or("missing fixture parent")?.to_path_buf()];
        let (mut allocated_bytes, mut logical_bytes) = (0, 0);
        while let Some(path) = pending.pop() {
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.is_dir() {
                for entry in fs::read_dir(path)? {
                    pending.push(entry?.path());
                }
            } else if metadata.is_file() {
                allocated_bytes += metadata.blocks() * 512;
                logical_bytes += metadata.len();
            }
        }
        Ok(Snapshot {
            rss_bytes,
            allocated_bytes,
            logical_bytes,
        })
    }

    fn check_query(result: ExecutionResult) -> Result<()> {
        let ExecutionResult::Query(result) = result else {
            return Err("expected query".into());
        };
        if result.rows != vec![vec![SqlValue::Integer(0)]] {
            return Err("nearest row differs".into());
        }
        Ok(())
    }

    fn validate_rows(db: &Database) -> Result<()> {
        let ExecutionResult::Query(result) =
            db.execute_sql("SELECT id, value, e, f FROM items ORDER BY id")?
        else {
            return Err("expected complete rows".into());
        };
        let expected: Vec<_> = (0..ROWS)
            .map(|i| {
                vec![
                    SqlValue::Integer(i),
                    SqlValue::Integer(i * 3),
                    SqlValue::Vector(vec![i as f32, 0.0]),
                    SqlValue::Vector(vec![0.0, i as f32]),
                ]
            })
            .collect();
        if result.rows != expected {
            return Err("complete rows/vectors differ".into());
        }
        Ok(())
    }

    pub fn run() -> Result<()> {
        let args: Vec<String> = std::env::args().collect();
        if args.len() != 8 || args[1] != "--manual" {
            return Err("usage: begin_resources --manual seed|probe PATH INDEXES borrowed|owned|auto noop|query SAMPLES".into());
        }
        let action = args[2].as_str();
        let path = Path::new(&args[3]);
        let indexes: usize = args[4].parse()?;
        let api = args[5].as_str();
        let operation = args[6].as_str();
        let samples: usize = args[7].parse()?;
        if !["seed", "probe"].contains(&action)
            || indexes > 2
            || !["borrowed", "owned", "auto"].contains(&api)
            || !["noop", "query"].contains(&operation)
            || ![1, 6].contains(&samples)
            || (api == "auto" && operation != "query")
        {
            return Err("unsupported bounded diagnostic case".into());
        }
        if (action == "seed" && path.exists()) || (action == "probe" && !path.is_file()) {
            return Err("fixture must be seeded exactly once in a separate process".into());
        }
        let binary_sha256 = format!("{:x}", Sha256::digest(fs::read(std::env::current_exe()?)?));
        let db = Database::open_with_options(
            path,
            DatabaseOptions::new().with_hnsw_cache_limits(8 * 1024 * 1024, Some(2)),
        )?;
        if action == "seed" {
            db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER, e VECTOR(2,L2), f VECTOR(2,L2))")?;
            {
                let mut tx = db.begin(TxnMode::ReadWrite)?;
                for i in 0..ROWS {
                    tx.execute_sql(&format!(
                        "INSERT INTO items VALUES ({i}, {}, [{i},0], [0,{i}])",
                        i * 3
                    ))?;
                }
                tx.commit()?;
            }
            for column in ["e", "f"].iter().take(indexes) {
                db.execute_sql(&format!(
                    "CREATE INDEX i_{column} ON items ({column}) USING HNSW"
                ))?;
            }
            validate_rows(&db)?;
            println!(
                "{}",
                json!({"event":"seeded", "binary_sha256":binary_sha256, "all_rows_unchanged":true})
            );
            return Ok(());
        }
        let db = Arc::new(db);
        if db.hnsw_cache_stats().entries != 0 {
            return Err("fresh reader cache must be empty".into());
        }
        let query = if indexes == 0 {
            "SELECT id FROM items ORDER BY vector_distance(e, [0,0], 'l2') LIMIT 1"
        } else {
            "SELECT id FROM items ORDER BY vector_distance(e, [0,0], 'l2') LIMIT 1 WITH (enable_hnsw = true)"
        };
        println!(
            "{}",
            json!({"event":"ready", "binary_sha256":binary_sha256, "rows":ROWS, "indexes":indexes,
            "api":api, "operation":operation, "query":query, "debug_assertions":cfg!(debug_assertions),
            "cache_note":"fresh reader, not OS cold; warm all indexes before sample1"})
        );
        for iteration in 0..samples {
            if iteration == 1 {
                for column in ["e", "f"].iter().take(indexes) {
                    db.warm_hnsw_index(&format!("i_{column}"))?;
                }
                if db.hnsw_cache_stats().entries != indexes {
                    return Err("indexes must fit shared cache".into());
                }
            }
            let before = snapshot(path)?;
            let cache_before = db.hnsw_cache_stats();
            let started = Instant::now();
            let mut event = if api == "auto" {
                let result = db.execute_sql(query)?;
                let auto_total_ns = started.elapsed().as_nanos();
                let after_query = snapshot(path)?;
                check_query(result)?;
                json!({"begin_ns":null,"query_ns":null,"rollback_ns":null,"auto_total_ns":auto_total_ns,
                    "after_begin":null,"after_query":after_query})
            } else {
                // Both public ownership routes use the identical observation boundaries.
                macro_rules! measure {
                    ($begin:expr) => {{
                        let mut tx = $begin?;
                        let begin_ns = started.elapsed().as_nanos();
                        let after_begin = snapshot(path)?;
                        let (query_ns, after_query) = if operation == "query" {
                            let query_started = Instant::now();
                            let result = tx.execute_sql(query)?;
                            let query_ns = query_started.elapsed().as_nanos();
                            let after_query = snapshot(path)?;
                            check_query(result)?;
                            (Some(query_ns), Some(after_query))
                        } else { (None, None) };
                        let rollback_started = Instant::now();
                        tx.rollback()?;
                        let rollback_ns = rollback_started.elapsed().as_nanos();
                        json!({"begin_ns":begin_ns,"query_ns":query_ns,"rollback_ns":rollback_ns,
                            "auto_total_ns":null,"after_begin":after_begin,"after_query":after_query})
                    }};
                }
                if api == "owned" {
                    measure!(db
                        .clone()
                        .begin_owned_embedded_transaction(TxnMode::ReadWrite))
                } else {
                    measure!(db.begin(TxnMode::ReadWrite))
                }
            };
            let cache_after = db.hnsw_cache_stats();
            if iteration > 0
                && indexes > 0
                && operation == "query"
                && cache_after.hits <= cache_before.hits
            {
                return Err("warm query must hit shared HNSW cache".into());
            }
            event["event"] = json!("sample");
            event["iteration"] = json!(iteration);
            event["first_after_open"] = json!(iteration == 0);
            event["before"] = json!(before);
            event["after_finish"] = json!(snapshot(path)?);
            event["cache_before"] = json!(cache_before);
            event["cache_after"] = json!(cache_after);
            println!("{event}");
        }
        validate_rows(&db)?;
        println!("{}", json!({"event":"complete", "all_rows_unchanged":true}));
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    diagnostic::run()
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("this manual resource diagnostic requires native macOS");
    std::process::exit(2);
}
