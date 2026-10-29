//! Manual #681 measurement. Never run this workload as a normal CI test.
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use alopex_core::TxnMode;
use alopex_embedded::Database;
use alopex_sql::SqlValue;
use serde_json::json;

fn event(phase: &str, value: serde_json::Value) {
    println!("{}", json!({"phase":phase,"data":value}));
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        std::env::var("ALOPEX_MANUAL_681").as_deref(),
        Ok("1"),
        "set ALOPEX_MANUAL_681=1 only under the bounded manual runner"
    );
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let rows: usize = arguments.first().map_or(Ok(100_000), |s| s.parse())?;
    let dimensions: usize = arguments.get(1).map_or(Ok(384), |s| s.parse())?;
    assert!((1..=100_000).contains(&rows) && (1..=384).contains(&dimensions));
    event(
        "seed",
        json!({"event":"start","rows":rows,"dimensions":dimensions}),
    );
    let started = Instant::now();
    let db = Arc::new(Database::open_in_memory()?);
    db.execute_sql(&format!(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, e VECTOR({dimensions},L2))"
    ))?;
    let mut insert = db.prepare("INSERT INTO items VALUES (?,?)")?;
    // The iterator keeps one vector live; avoid one enormous SQL string or
    // retaining a second 100k-vector input alongside the database.
    insert.execute_many((0..rows).map(|row| {
        vec![
            SqlValue::Integer(row as i32),
            SqlValue::Vector(vector(row, dimensions)),
        ]
    }))?;
    insert.finalize()?;
    event(
        "seed",
        json!({"event":"end","elapsed_ns":started.elapsed().as_nanos()}),
    );
    event("index_build", json!({"event":"start"}));
    let started = Instant::now();
    db.execute_sql("CREATE INDEX ih ON items(e) USING HNSW")?;
    event(
        "index_build",
        json!({"event":"end","elapsed_ns":started.elapsed().as_nanos()}),
    );
    let query = format!("SELECT id FROM items ORDER BY vector_distance(e,[{}],'l2') LIMIT 1 WITH (enable_hnsw=true,ef_search=16)",vec!["0";dimensions].join(","));
    // Force an empty shared cache while retaining the normal cache limit.
    db.set_hnsw_cache_limit(0);
    db.set_hnsw_cache_limit(256 * 1024 * 1024);
    for phase in ["cold_transaction_1", "cold_transaction_2"] {
        event(
            phase,
            json!({"event":"start","stats":db.hnsw_cache_stats()}),
        );
        let started = Instant::now();
        let mut transaction = db
            .clone()
            .begin_owned_embedded_transaction(TxnMode::ReadOnly)?;
        transaction.execute_sql(&query)?;
        transaction.rollback()?;
        event(
            phase,
            json!({"event":"end","elapsed_ns":started.elapsed().as_nanos(),"stats":db.hnsw_cache_stats()}),
        );
    }
    event("warm", json!({"event":"start"}));
    let bytes = db.warm_hnsw_index("ih")?;
    assert!(bytes > 0, "graph must fit the declared 256 MiB cache");
    event(
        "warm",
        json!({"event":"end","graph_bytes":bytes,"stats":db.hnsw_cache_stats()}),
    );
    event("writer_begin", json!({"event":"start"}));
    let started = Instant::now();
    let mut transaction = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)?;
    event(
        "writer_begin",
        json!({"event":"end","elapsed_ns":started.elapsed().as_nanos()}),
    );
    for (offset, phase) in ["first_insert", "second_insert"].into_iter().enumerate() {
        let values = vector(rows + offset, dimensions)
            .iter()
            .map(f32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("INSERT INTO items VALUES ({},[{values}])", rows + offset);
        event(phase, json!({"event":"start"}));
        let started = Instant::now();
        transaction.execute_sql(&sql)?;
        event(
            phase,
            json!({"event":"end","elapsed_ns":started.elapsed().as_nanos(),"stats":db.hnsw_cache_stats()}),
        );
    }
    event("rollback", json!({"event":"start"}));
    transaction.rollback()?;
    event("rollback", json!({"event":"end"}));
    event("complete", json!({"stats":db.hnsw_cache_stats()}));
    Ok(())
}
