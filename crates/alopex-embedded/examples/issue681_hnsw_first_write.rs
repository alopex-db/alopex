//! Manual #681 measurement. Never run this workload as a normal CI test.
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use alopex_core::TxnMode;
use alopex_embedded::Database;
use alopex_sql::storage::bridge::manual_hnsw_writer_profile as writer_profile;
use alopex_sql::{ExecutionResult, SqlValue};
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
    let measured_finish = arguments.get(2).map(String::as_str);
    assert!(arguments.len() <= 3);
    assert!(matches!(
        measured_finish,
        None | Some("commit" | "rollback")
    ));
    let finish = measured_finish.unwrap_or("rollback");
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
        writer_profile::begin();
        let started = Instant::now();
        transaction.execute_sql(&sql)?;
        let elapsed_ns = started.elapsed().as_nanos();
        let internal = writer_profile::finish();
        assert_eq!(
            internal.stage_calls, 1,
            "one HNSW flush per INSERT required"
        );
        assert_eq!(
            internal.fork_calls,
            u64::from(offset == 0),
            "first INSERT forks; second reuses owned graph"
        );
        assert!(u128::from(internal.fork_ns) + u128::from(internal.stage_ns) <= elapsed_ns);
        event(
            phase,
            json!({"event":"end","elapsed_ns":elapsed_ns,"stats":db.hnsw_cache_stats(),
                   "writer_internal":{"schema":"alopex.hnsw-writer-diagnostic/v1",
                       "fork_calls":internal.fork_calls,"fork_ns":internal.fork_ns,
                       "stage_calls":internal.stage_calls,"stage_ns":internal.stage_ns,
                       "scope":"synchronous INSERT on the calling thread",
                       "performance_qualification":false}}),
        );
    }
    event(finish, json!({"event":"start"}));
    let started = Instant::now();
    if finish == "commit" {
        transaction.commit()?;
    } else {
        transaction.rollback()?;
    }
    let elapsed_ns = started.elapsed().as_nanos();
    if measured_finish.is_some() {
        event(finish, json!({"event":"end","elapsed_ns":elapsed_ns}));
        // Validate through a new reader after the finish timer and RSS window.
        // No verification query or input construction is charged to completion.
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
            json!({"stats":db.hnsw_cache_stats(),"finish":finish,"result_validated":true}),
        );
    } else {
        event(finish, json!({"event":"end"}));
        event("complete", json!({"stats":db.hnsw_cache_stats()}));
    }
    Ok(())
}
