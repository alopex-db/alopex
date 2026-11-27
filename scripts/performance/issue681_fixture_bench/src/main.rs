//! Precondition-vs-SUT separated #681 HNSW measurement binary.
//!
//! - `prepare <db_dir> <rows> <dimensions>`:
//!   Builds the immutable on-disk database fixture ONCE per scale (`seed` +
//!   `CREATE INDEX ... USING HNSW` + `flush` + `close`).
//! - `measure <trial_db_dir> <rows> <dimensions> <commit|rollback>`:
//!   Opens a fresh copy of the prebuilt fixture with an empty 256 MiB HNSW
//!   cache and measures ONLY the 7 #681 SUT phases (`cold_transaction_1`,
//!   `cold_transaction_2`, `warm`, `writer_begin`, `first_insert`,
//!   `second_insert`, `<commit|rollback>`) using phase handshakes.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use alopex_core::TxnMode;
use alopex_embedded::wal_overflow::{
    LsmOverflowConfig, OperationLimits, SpoolLimits, SyncMode, WalConfig,
};
use alopex_embedded::{Database, DatabaseOptions};
use alopex_sql::{ExecutionResult, SqlValue};
use serde_json::json;

const SEED_BATCH_ROWS: usize = 5_000;
const HNSW_CACHE_LIMIT_BYTES: u64 = 256 * 1024 * 1024;

fn bench_options() -> Result<DatabaseOptions, Box<dyn std::error::Error>> {
    const FRAGMENT_BYTES: u32 = 1024 * 1024;
    const PAYLOAD_BYTES: u64 = 1024 * 1024 * 1024;
    const FILE_BYTES: u64 = 1536 * 1024 * 1024;
    const DIRECTORY_BYTES: u64 = 2 * 1024 * 1024 * 1024;
    const REPLAY_BYTES: usize = 1024 * 1024 * 1024;
    let spool = SpoolLimits::new(PAYLOAD_BYTES, FILE_BYTES, FRAGMENT_BYTES)?;
    let operations = OperationLimits::new(PAYLOAD_BYTES, 1024 * 1024, 16 * 1024 * 1024)?;
    let overflow = LsmOverflowConfig::new(
        spool,
        operations,
        DIRECTORY_BYTES,
        FRAGMENT_BYTES,
        REPLAY_BYTES,
    )
    .with_inline_bytes(16 * 1024 * 1024)
    .with_activation_bytes(1024 * 1024 * 1024);
    let wal = WalConfig {
        segment_size: 64 * 1024 * 1024,
        max_segments: 8,
        sync_mode: SyncMode::EveryWrite,
    };
    Ok(DatabaseOptions::new().with_wal_overflow(overflow, wal))
}

fn event(phase: &str, value: serde_json::Value) {
    println!("{}", json!({"phase": phase, "data": value}));
    std::io::stdout().flush().unwrap();
    let mut acknowledgement = String::new();
    std::io::stdin().read_line(&mut acknowledgement).unwrap();
    assert_eq!(
        acknowledgement.trim(),
        "continue",
        "manual monitor disconnected"
    );
}

fn vector(row: usize, dimensions: usize) -> Vec<f32> {
    let mut state = (row as u32).wrapping_add(0x6810_0817);
    (0..dimensions)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            (state >> 24) as f32
        })
        .collect()
}

fn run_prepare(
    db_dir: &Path,
    rows: usize,
    dimensions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::open_with_options(db_dir, bench_options()?)?);
    let seed_started = Instant::now();
    db.execute_sql(&format!(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, e VECTOR({dimensions},L2))"
    ))?;
    let mut start = 0usize;
    while start < rows {
        let end = (start + SEED_BATCH_ROWS).min(rows);
        let mut insert = db.prepare("INSERT INTO items VALUES (?,?)")?;
        insert.execute_many((start..end).map(|row| {
            vec![
                SqlValue::Integer(row as i32),
                SqlValue::Vector(vector(row, dimensions)),
            ]
        }))?;
        insert.finalize()?;
        start = end;
    }
    let seed_elapsed_ns = seed_started.elapsed().as_nanos();

    let index_started = Instant::now();
    db.execute_sql("CREATE INDEX ih ON items(e) USING HNSW")?;
    let index_build_elapsed_ns = index_started.elapsed().as_nanos();

    db.flush()?;
    db.close()?;

    println!(
        "{}",
        json!({
            "event": "prepared",
            "rows": rows,
            "dimensions": dimensions,
            "seed_elapsed_ns": seed_elapsed_ns,
            "index_build_elapsed_ns": index_build_elapsed_ns,
        })
    );
    std::io::stdout().flush()?;
    Ok(())
}

fn run_measure(
    db_dir: &Path,
    rows: usize,
    dimensions: usize,
    finish: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::open_with_options(db_dir, bench_options()?)?);
    let query = format!(
        "SELECT id FROM items ORDER BY vector_distance(e,[{}],'l2') LIMIT 1 WITH (enable_hnsw=true,ef_search=16)",
        vec!["0"; dimensions].join(",")
    );
    // Start each SUT trial with an empty shared HNSW cache at the declared 256 MiB budget.
    db.set_hnsw_cache_limit(0);
    db.set_hnsw_cache_limit(HNSW_CACHE_LIMIT_BYTES);

    for phase in ["cold_transaction_1", "cold_transaction_2"] {
        event(
            phase,
            json!({"event": "start", "stats": db.hnsw_cache_stats()}),
        );
        let started = Instant::now();
        let mut transaction = db
            .clone()
            .begin_owned_embedded_transaction(TxnMode::ReadOnly)?;
        transaction.execute_sql(&query)?;
        transaction.rollback()?;
        event(
            phase,
            json!({
                "event": "end",
                "elapsed_ns": started.elapsed().as_nanos(),
                "stats": db.hnsw_cache_stats(),
            }),
        );
    }

    event("warm", json!({"event": "start"}));
    let bytes = db.warm_hnsw_index("ih")?;
    assert!(bytes > 0, "graph must fit the declared 256 MiB cache");
    event(
        "warm",
        json!({"event": "end", "graph_bytes": bytes, "stats": db.hnsw_cache_stats()}),
    );

    event("writer_begin", json!({"event": "start"}));
    let started = Instant::now();
    let mut transaction = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)?;
    event(
        "writer_begin",
        json!({"event": "end", "elapsed_ns": started.elapsed().as_nanos()}),
    );

    for (offset, phase) in ["first_insert", "second_insert"].into_iter().enumerate() {
        let values = vector(rows + offset, dimensions)
            .iter()
            .map(f32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("INSERT INTO items VALUES ({},[{values}])", rows + offset);
        event(phase, json!({"event": "start"}));
        let started = Instant::now();
        transaction.execute_sql(&sql)?;
        event(
            phase,
            json!({
                "event": "end",
                "elapsed_ns": started.elapsed().as_nanos(),
                "stats": db.hnsw_cache_stats(),
            }),
        );
    }

    event(finish, json!({"event": "start"}));
    let started = Instant::now();
    if finish == "commit" {
        transaction.commit()?;
    } else {
        transaction.rollback()?;
    }
    let elapsed_ns = started.elapsed().as_nanos();
    event(finish, json!({"event": "end", "elapsed_ns": elapsed_ns}));

    // Validate through a new reader outside the finish timer and RSS window.
    let result = db.execute_sql(&format!(
        "SELECT id,e FROM items WHERE id >= {rows} ORDER BY id"
    ))?;
    let ExecutionResult::Query(result) = result else {
        panic!("completion verification must return rows");
    };
    let expected = if finish == "commit" {
        (rows..rows + 2)
            .map(|row| {
                vec![
                    SqlValue::Integer(row as i32),
                    SqlValue::Vector(vector(row, dimensions)),
                ]
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    assert_eq!(result.rows, expected, "transaction completion differs");
    event(
        "complete",
        json!({
            "stats": db.hnsw_cache_stats(),
            "finish": finish,
            "result_validated": true,
        }),
    );
    db.close()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        std::env::var("ALOPEX_MANUAL_681").as_deref(),
        Ok("1"),
        "set ALOPEX_MANUAL_681=1 only under the bounded manual runner"
    );
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("");
    match mode {
        "prepare" => {
            assert_eq!(args.len(), 4, "usage: prepare <db_dir> <rows> <dimensions>");
            let db_dir = Path::new(&args[1]);
            let rows: usize = args[2].parse()?;
            let dimensions: usize = args[3].parse()?;
            assert!((1..=100_000).contains(&rows) && (1..=384).contains(&dimensions));
            run_prepare(db_dir, rows, dimensions)
        }
        "measure" => {
            assert_eq!(
                args.len(),
                5,
                "usage: measure <db_dir> <rows> <dimensions> <commit|rollback>"
            );
            let db_dir = Path::new(&args[1]);
            let rows: usize = args[2].parse()?;
            let dimensions: usize = args[3].parse()?;
            let finish = args[4].as_str();
            assert!((1..=100_000).contains(&rows) && (1..=384).contains(&dimensions));
            assert!(matches!(finish, "commit" | "rollback"));
            run_measure(db_dir, rows, dimensions, finish)
        }
        _ => panic!("expected subcommand 'prepare' or 'measure'"),
    }
}
