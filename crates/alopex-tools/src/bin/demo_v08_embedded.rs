//! Complete v0.8.x embedded-local public-surface walkthrough.
//!
//! Every scenario prints a stable ID and checks its observable result. A failed
//! assertion or API error makes the release verification step fail.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema};
use alopex_core::kv::{OwnedReadOptions, ReadAtCapability, ReadAtPoint};
use alopex_core::txn::OwnedLeaseOutcome;
use alopex_embedded::{
    ColumnDefinition, ColumnarIndexType, CreateCatalogRequest, CreateNamespaceRequest,
    CreateTableRequest, Database, DatabaseOptions, EmbeddedConfig, HnswConfig, Metric,
    OwnedSqlRowOutcome, OwnedSqlStreamPlan, SqlStreamingResult, StreamingQueryResult,
    TransactionManager, TxnMode,
};
use alopex_sql::ast::ddl::DataType;
use alopex_sql::{ExecutionResult, SqlValue};

type DemoResult<T = ()> = Result<T, Box<dyn Error>>;

struct DemoTemp {
    root: PathBuf,
}

impl DemoTemp {
    fn new() -> DemoResult<Self> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!(
            "alopex-v085-embedded-demo-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir(&root)?;
        Ok(Self { root })
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for DemoTemp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn require(condition: bool, message: &str) -> DemoResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn query_rows(result: ExecutionResult) -> DemoResult<Vec<Vec<SqlValue>>> {
    match result {
        ExecutionResult::Query(query) => Ok(query.rows),
        other => Err(std::io::Error::other(format!("query result expected, got {other:?}")).into()),
    }
}

fn check_rows(db: &Database, sql: &str, expected: &[Vec<SqlValue>]) -> DemoResult {
    let actual = query_rows(db.execute_sql(sql)?)?;
    if actual == expected {
        println!("SQL rows={} :: {sql}", actual.len());
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "SQL row mismatch for `{sql}`: expected={expected:?}; actual={actual:?}"
        ))
        .into())
    }
}

fn expect_sql_error(db: &Database, sql: &str, expected: &str) -> DemoResult {
    match db.execute_sql(sql) {
        Err(error)
            if error
                .to_string()
                .to_lowercase()
                .contains(&expected.to_lowercase()) =>
        {
            println!("SQL rejected as expected ({expected}) :: {sql}");
            Ok(())
        }
        Err(error) => Err(std::io::Error::other(format!(
            "SQL error for `{sql}` did not contain `{expected}`: {error}"
        ))
        .into()),
        Ok(result) => Err(std::io::Error::other(format!(
            "SQL expected to fail was accepted: `{sql}` returned {result:?}"
        ))
        .into()),
    }
}

fn run_scenario(id: &str, title: &str, scenario: fn() -> DemoResult) -> DemoResult {
    println!("\n=== {id}: {title} ===");
    scenario()?;
    println!("PASS {id}");
    Ok(())
}

fn scenario_storage_durability() -> DemoResult {
    let tmp = DemoTemp::new()?;
    let options = DatabaseOptions::in_memory().with_memory_limit(8 * 1024 * 1024);
    require(options.memory_mode(), "in-memory option was not enabled")?;
    require(
        options.memory_limit() == Some(8 * 1024 * 1024),
        "memory limit was not retained",
    )?;
    let db = Database::open_in_memory_with_options(options)?;
    {
        let mut txn = db.begin(TxnMode::ReadWrite)?;
        txn.put(b"durable:one", b"1")?;
        txn.put(b"durable:two", b"2")?;
        txn.commit()?;
    }
    require(
        db.snapshot().len() >= 2,
        "snapshot omitted committed KV rows",
    )?;
    require(
        db.memory_usage().is_some(),
        "memory statistics are unavailable",
    )?;
    db.set_memory_limit(Some(16 * 1024 * 1024));

    let clone = db.clone_to_memory()?;
    {
        let mut read = clone.begin(TxnMode::ReadOnly)?;
        require(
            read.get(b"durable:one")? == Some(b"1".to_vec()),
            "in-memory clone changed data",
        )?;
    }
    clone.clear()?;
    require(clone.snapshot().is_empty(), "clear left data behind")?;

    let path = tmp.path("durable.alopex");
    db.persist_to_disk(&path)?;
    let disk = Database::open(&path)?;
    {
        let mut read = disk.begin(TxnMode::ReadOnly)?;
        require(
            read.get(b"durable:two")? == Some(b"2".to_vec()),
            "persist/reopen changed data",
        )?;
    }
    disk.flush()?;
    println!("file format: {:?}", disk.file_format_version());
    drop(disk);

    let uri = format!("file://{}", path.display());
    let reopened = Database::open_with_uri(&uri)?;
    require(reopened.snapshot().len() >= 2, "file URI reopen lost data")?;
    Ok(())
}

fn scenario_kv_transactions() -> DemoResult {
    let db = Database::new();
    {
        let mut txn = db.begin(TxnMode::ReadWrite)?;
        txn.put(b"user:1", b"alice")?;
        txn.put(b"user:2", b"bob")?;
        txn.commit()?;
    }
    {
        let mut txn = db.begin(TxnMode::ReadWrite)?;
        txn.put(b"user:3", b"discarded")?;
        txn.delete(b"user:1")?;
        txn.rollback()?;
    }
    {
        let mut read = db.begin(TxnMode::ReadOnly)?;
        let rows = read.scan_prefix(b"user:")?.collect::<Vec<_>>();
        require(rows.len() == 2, "rollback or prefix scan contract failed")?;
        require(
            read.get(b"user:1")? == Some(b"alice".to_vec()),
            "rollback did not restore deleted value",
        )?;
        require(
            read.get(b"user:3")?.is_none(),
            "rollback committed a staged put",
        )?;
        read.commit()?;
    }
    Ok(())
}

fn scenario_persisted_transaction_manager() -> DemoResult {
    let db = Database::new();
    let committed = TransactionManager::begin_with_timeout(&db, Duration::from_secs(30))?;
    let info = TransactionManager::get_info(&db, &committed)?;
    require(info.txn_id == committed, "transaction identity changed")?;
    require(info.timeout_secs == 30, "transaction timeout changed")?;
    TransactionManager::put(&db, &committed, b"managed:key", b"committed")?;
    require(
        TransactionManager::get(&db, &committed, b"managed:key")? == Some(b"committed".to_vec()),
        "staged transaction read is not read-your-writes",
    )?;
    TransactionManager::commit(&db, &committed)?;

    let rolled_back = TransactionManager::begin_with_timeout(&db, Duration::from_secs(30))?;
    TransactionManager::delete(&db, &rolled_back, b"managed:key")?;
    TransactionManager::put(&db, &rolled_back, b"managed:temp", b"discarded")?;
    TransactionManager::rollback(&db, &rolled_back)?;
    let mut read = db.begin(TxnMode::ReadOnly)?;
    require(
        read.get(b"managed:key")? == Some(b"committed".to_vec()),
        "managed rollback deleted committed data",
    )?;
    require(
        read.get(b"managed:temp")?.is_none(),
        "managed rollback retained staged data",
    )?;
    Ok(())
}

fn seed_sql_database() -> DemoResult<Database> {
    let db = Database::new();
    if db.get_catalog("default").is_err() {
        db.create_catalog(CreateCatalogRequest::new("default"))?;
    }
    if db.get_namespace("default", "default").is_err() {
        db.create_namespace(CreateNamespaceRequest::new("default", "default"))?;
    }
    let results = db.execute_sql_multi(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT);\
         CREATE TABLE metrics (id INTEGER PRIMARY KEY, dept_id INTEGER, n INTEGER, f FLOAT, ts TIMESTAMP, embedding VECTOR(3, L2));\
         CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL, qty INTEGER, bonus REAL);\
         INSERT INTO departments VALUES (1, 'search'), (2, 'storage');\
         INSERT INTO metrics VALUES\
           (1, 1, 2, 1.5, '2025-01-15 10:30:00', [1.0, 0.0, 0.0]),\
           (2, 1, 3, 2.5, '2025-01-15 10:30:01.25', [0.0, 0.0, 0.0]),\
           (3, 2, 4, 3.5, '2025-01-15 10:30:02', [-1.0, 0.0, 0.0]);\
         INSERT INTO sales VALUES\
           (1, 'east', 100.0, 3, 10.0),\
           (2, 'east', 200.0, 1, NULL),\
           (3, 'west', 150.0, 5, 20.0),\
           (4, 'west', 150.0, 2, NULL),\
           (5, 'north', 50.0, 0, 5.0)",
    )?;
    require(results.len() == 6, "multi-statement result count changed")?;
    db.execute_sql("CREATE INDEX idx_metrics_dept ON metrics(dept_id)")?;
    Ok(db)
}

fn scenario_local_sql_matrix() -> DemoResult {
    let db = seed_sql_database()?;
    let inherited_matrix = vec![
        (
            "SELECT DISTINCT dept_id FROM metrics WHERE n IN (2, 3, 4) AND n BETWEEN 2 AND 4 ORDER BY dept_id LIMIT 2 OFFSET 0",
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        ),
        (
            "SELECT d.name, SUM(m.n) AS total, AVG(m.n) AS mean FROM departments AS d JOIN metrics AS m ON d.id = m.dept_id GROUP BY d.name HAVING SUM(m.n) >= 4 ORDER BY d.name",
            vec![
                vec![
                    SqlValue::Text("search".into()),
                    SqlValue::BigInt(5),
                    SqlValue::Double(2.5),
                ],
                vec![
                    SqlValue::Text("storage".into()),
                    SqlValue::BigInt(4),
                    SqlValue::Double(4.0),
                ],
            ],
        ),
        (
            "SELECT id FROM metrics WHERE n > (SELECT AVG(n) FROM metrics) ORDER BY id",
            vec![vec![SqlValue::Integer(3)]],
        ),
        (
            "SELECT CAST(n AS DOUBLE) AS converted, n * 2.0 AS doubled FROM metrics WHERE id = 1",
            vec![vec![SqlValue::Double(2.0), SqlValue::Double(4.0)]],
        ),
        (
            "SELECT md5('abc') AS digest, hex(unhex('A1B2')) AS roundtrip, upper('alopex') AS name",
            vec![vec![
                SqlValue::Text("900150983cd24fb0d6963f7d28e17f72".into()),
                SqlValue::Text("A1B2".into()),
                SqlValue::Text("ALOPEX".into()),
            ]],
        ),
        (
            "SELECT SUM(n) AS integer_sum, COUNT(DISTINCT dept_id) AS departments FROM metrics",
            vec![vec![SqlValue::BigInt(9), SqlValue::BigInt(2)]],
        ),
        (
            "SELECT id, vector_distance(embedding, [1.0, 0.0, 0.0], 'l2') AS distance FROM metrics ORDER BY vector_distance(embedding, [1.0, 0.0, 0.0], 'l2') LIMIT 2",
            vec![
                vec![SqlValue::Integer(1), SqlValue::Double(0.0)],
                vec![SqlValue::Integer(2), SqlValue::Double(1.0)],
            ],
        ),
        (
            "SELECT ts FROM metrics WHERE id = 1",
            vec![vec![SqlValue::Timestamp(1_736_937_000_000_000)]],
        ),
    ];
    require(
        inherited_matrix.len() == 8,
        "inherited SQL exact-check count changed",
    )?;
    for (sql, expected) in &inherited_matrix {
        check_rows(&db, sql, expected)?;
    }

    let v08_matrix = vec![
        (
            "SELECT TRUE IS TRUE AS truth_value, NULL IS DISTINCT FROM 1 AS distinct_null, (1, 2) < (1, 3) AS row_less, (1, NULL) = (1, NULL) AS row_unknown",
            vec![vec![
                SqlValue::Boolean(true),
                SqlValue::Boolean(true),
                SqlValue::Boolean(true),
                SqlValue::Null,
            ]],
        ),
        (
            "SELECT amount AS id FROM sales ORDER BY id",
            vec![
                vec![SqlValue::Float(50.0)],
                vec![SqlValue::Float(100.0)],
                vec![SqlValue::Float(150.0)],
                vec![SqlValue::Float(150.0)],
                vec![SqlValue::Float(200.0)],
            ],
        ),
        (
            "SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING total >= 300 ORDER BY region",
            vec![
                vec![SqlValue::Text("east".into()), SqlValue::Double(300.0)],
                vec![SqlValue::Text("west".into()), SqlValue::Double(300.0)],
            ],
        ),
        (
            "SELECT amount, pg_typeof(amount) AS kind FROM sales WHERE id = 1",
            vec![vec![SqlValue::Float(100.0), SqlValue::Text("real".into())]],
        ),
        (
            "SELECT id FROM sales WHERE amount >= 150 UNION SELECT id FROM sales WHERE qty <= 2 ORDER BY id",
            vec![
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(4)],
                vec![SqlValue::Integer(5)],
            ],
        ),
        (
            "SELECT id FROM sales WHERE amount >= 150 UNION ALL SELECT id FROM sales WHERE qty <= 2 ORDER BY id",
            vec![
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(4)],
                vec![SqlValue::Integer(4)],
                vec![SqlValue::Integer(5)],
            ],
        ),
        (
            "SELECT id FROM sales WHERE amount >= 150 INTERSECT SELECT id FROM sales WHERE qty <= 2 ORDER BY id",
            vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(4)]],
        ),
        (
            "SELECT id FROM sales WHERE qty <= 2 EXCEPT SELECT id FROM sales WHERE amount >= 150 ORDER BY id",
            vec![vec![SqlValue::Integer(5)]],
        ),
        (
            "SELECT id, CASE WHEN qty > 2 THEN 'bulk' ELSE 'small' END AS band, CASE WHEN bonus > 10 THEN 'large' END AS bonus_band, CASE WHEN qty = 0 THEN 1 ELSE 2.5 END AS numeric_case, CASE WHEN qty = 0 THEN TRUE ELSE FALSE END AS is_zero FROM sales ORDER BY id",
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Text("bulk".into()),
                    SqlValue::Null,
                    SqlValue::Double(2.5),
                    SqlValue::Boolean(false),
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Text("small".into()),
                    SqlValue::Null,
                    SqlValue::Double(2.5),
                    SqlValue::Boolean(false),
                ],
                vec![
                    SqlValue::Integer(3),
                    SqlValue::Text("bulk".into()),
                    SqlValue::Text("large".into()),
                    SqlValue::Double(2.5),
                    SqlValue::Boolean(false),
                ],
                vec![
                    SqlValue::Integer(4),
                    SqlValue::Text("small".into()),
                    SqlValue::Null,
                    SqlValue::Double(2.5),
                    SqlValue::Boolean(false),
                ],
                vec![
                    SqlValue::Integer(5),
                    SqlValue::Text("small".into()),
                    SqlValue::Null,
                    SqlValue::Double(1.0),
                    SqlValue::Boolean(true),
                ],
            ],
        ),
        (
            "WITH chosen AS (SELECT id, region FROM sales WHERE amount >= 150) SELECT sales.id AS sales_id, chosen.id AS chosen_id FROM sales JOIN chosen ON sales.region = chosen.region ORDER BY sales_id, chosen_id",
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(2)],
                vec![SqlValue::Integer(2), SqlValue::Integer(2)],
                vec![SqlValue::Integer(3), SqlValue::Integer(3)],
                vec![SqlValue::Integer(3), SqlValue::Integer(4)],
                vec![SqlValue::Integer(4), SqlValue::Integer(3)],
                vec![SqlValue::Integer(4), SqlValue::Integer(4)],
            ],
        ),
        (
            "WITH renamed(identifier, territory) AS (SELECT id, region FROM sales WHERE id = 1) SELECT territory, identifier FROM renamed",
            vec![vec![SqlValue::Text("east".into()), SqlValue::Integer(1)]],
        ),
        (
            "SELECT id FROM sales WHERE amount >= 150 EXCEPT SELECT id FROM sales WHERE qty <= 2 ORDER BY id",
            vec![vec![SqlValue::Integer(3)]],
        ),
        (
            "WITH sales AS (SELECT id + 100 AS id FROM sales WHERE id = 1) SELECT id FROM sales",
            vec![vec![SqlValue::Integer(101)]],
        ),
        (
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, RANK() OVER (ORDER BY amount) AS rank_value, DENSE_RANK() OVER (ORDER BY amount) AS dense_value, SUM(amount) OVER (ORDER BY id) AS running FROM sales ORDER BY id",
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::BigInt(1),
                    SqlValue::BigInt(2),
                    SqlValue::BigInt(2),
                    SqlValue::Double(100.0),
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::BigInt(2),
                    SqlValue::BigInt(5),
                    SqlValue::BigInt(4),
                    SqlValue::Double(300.0),
                ],
                vec![
                    SqlValue::Integer(3),
                    SqlValue::BigInt(3),
                    SqlValue::BigInt(3),
                    SqlValue::BigInt(3),
                    SqlValue::Double(450.0),
                ],
                vec![
                    SqlValue::Integer(4),
                    SqlValue::BigInt(4),
                    SqlValue::BigInt(3),
                    SqlValue::BigInt(3),
                    SqlValue::Double(600.0),
                ],
                vec![
                    SqlValue::Integer(5),
                    SqlValue::BigInt(5),
                    SqlValue::BigInt(1),
                    SqlValue::BigInt(1),
                    SqlValue::Double(650.0),
                ],
            ],
        ),
        (
            "SELECT DISTINCT SUM(amount) AS total, DENSE_RANK() OVER (ORDER BY SUM(amount)) AS sales_rank, SUM(SUM(amount)) OVER () AS grand FROM sales GROUP BY region HAVING SUM(amount) >= 50 ORDER BY total",
            vec![
                vec![
                    SqlValue::Double(50.0),
                    SqlValue::BigInt(1),
                    SqlValue::Double(650.0),
                ],
                vec![
                    SqlValue::Double(300.0),
                    SqlValue::BigInt(2),
                    SqlValue::Double(650.0),
                ],
            ],
        ),
        (
            "SELECT id, LAG(amount) OVER (ORDER BY id) AS previous, LEAD(amount, 2, -1) OVER (ORDER BY id) AS two_ahead, LAG(amount, 0) OVER (ORDER BY id) AS current_value, amount - LAG(amount, 1, amount) OVER (ORDER BY id) AS delta FROM sales ORDER BY id",
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Null,
                    SqlValue::Double(150.0),
                    SqlValue::Float(100.0),
                    SqlValue::Float(0.0),
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Float(100.0),
                    SqlValue::Double(150.0),
                    SqlValue::Float(200.0),
                    SqlValue::Float(100.0),
                ],
                vec![
                    SqlValue::Integer(3),
                    SqlValue::Float(200.0),
                    SqlValue::Double(50.0),
                    SqlValue::Float(150.0),
                    SqlValue::Float(-50.0),
                ],
                vec![
                    SqlValue::Integer(4),
                    SqlValue::Float(150.0),
                    SqlValue::Double(-1.0),
                    SqlValue::Float(150.0),
                    SqlValue::Float(0.0),
                ],
                vec![
                    SqlValue::Integer(5),
                    SqlValue::Float(150.0),
                    SqlValue::Double(-1.0),
                    SqlValue::Float(50.0),
                    SqlValue::Float(-100.0),
                ],
            ],
        ),
        (
            "SELECT id, LAG(amount, 1, -1) OVER (PARTITION BY region ORDER BY id) AS previous, LEAD(bonus, 1, -1) OVER (PARTITION BY region ORDER BY id) AS following_bonus FROM sales ORDER BY id",
            vec![
                vec![SqlValue::Integer(1), SqlValue::Double(-1.0), SqlValue::Null],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Double(100.0),
                    SqlValue::Double(-1.0),
                ],
                vec![SqlValue::Integer(3), SqlValue::Double(-1.0), SqlValue::Null],
                vec![
                    SqlValue::Integer(4),
                    SqlValue::Double(150.0),
                    SqlValue::Double(-1.0),
                ],
                vec![
                    SqlValue::Integer(5),
                    SqlValue::Double(-1.0),
                    SqlValue::Double(-1.0),
                ],
            ],
        ),
        (
            "SELECT id, SUM(qty) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS physical FROM sales ORDER BY id",
            vec![
                vec![SqlValue::Integer(1), SqlValue::BigInt(4)],
                vec![SqlValue::Integer(2), SqlValue::BigInt(9)],
                vec![SqlValue::Integer(3), SqlValue::BigInt(8)],
                vec![SqlValue::Integer(4), SqlValue::BigInt(7)],
                vec![SqlValue::Integer(5), SqlValue::BigInt(2)],
            ],
        ),
        (
            "SELECT id, SUM(qty) OVER (ORDER BY amount RANGE BETWEEN 50 PRECEDING AND CURRENT ROW) AS value_frame FROM sales ORDER BY id",
            vec![
                vec![SqlValue::Integer(1), SqlValue::BigInt(3)],
                vec![SqlValue::Integer(2), SqlValue::BigInt(8)],
                vec![SqlValue::Integer(3), SqlValue::BigInt(10)],
                vec![SqlValue::Integer(4), SqlValue::BigInt(10)],
                vec![SqlValue::Integer(5), SqlValue::BigInt(0)],
            ],
        ),
        (
            "SELECT id, FIRST_VALUE(amount) OVER (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS first_amount, LAST_VALUE(amount) OVER (ORDER BY amount ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS last_amount, NTH_VALUE(amount, 2) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS second_amount, NTILE(3) OVER (ORDER BY amount) AS bucket, PERCENT_RANK() OVER (ORDER BY amount) AS percent_rank, CUME_DIST() OVER (ORDER BY amount) AS cumulative_distribution FROM sales ORDER BY id",
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Float(50.0),
                    SqlValue::Float(150.0),
                    SqlValue::Float(100.0),
                    SqlValue::BigInt(1),
                    SqlValue::Double(0.25),
                    SqlValue::Double(0.4),
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Float(150.0),
                    SqlValue::Float(200.0),
                    SqlValue::Float(100.0),
                    SqlValue::BigInt(3),
                    SqlValue::Double(1.0),
                    SqlValue::Double(1.0),
                ],
                vec![
                    SqlValue::Integer(3),
                    SqlValue::Float(100.0),
                    SqlValue::Float(150.0),
                    SqlValue::Float(100.0),
                    SqlValue::BigInt(2),
                    SqlValue::Double(0.5),
                    SqlValue::Double(0.8),
                ],
                vec![
                    SqlValue::Integer(4),
                    SqlValue::Float(150.0),
                    SqlValue::Float(200.0),
                    SqlValue::Float(100.0),
                    SqlValue::BigInt(2),
                    SqlValue::Double(0.5),
                    SqlValue::Double(0.8),
                ],
                vec![
                    SqlValue::Integer(5),
                    SqlValue::Float(50.0),
                    SqlValue::Float(100.0),
                    SqlValue::Null,
                    SqlValue::BigInt(1),
                    SqlValue::Double(0.0),
                    SqlValue::Double(0.2),
                ],
            ],
        ),
        (
            "SELECT id, ROW_NUMBER() OVER ranked AS row_number FROM sales WINDOW ranked AS (base ORDER BY amount DESC, id), base AS (PARTITION BY region) QUALIFY row_number = 1 ORDER BY id",
            vec![
                vec![SqlValue::Integer(2), SqlValue::BigInt(1)],
                vec![SqlValue::Integer(3), SqlValue::BigInt(1)],
                vec![SqlValue::Integer(5), SqlValue::BigInt(1)],
            ],
        ),
        (
            "SELECT id, label FROM (VALUES (2, 'b'), (1, 'a')) AS v(id, label) ORDER BY id",
            vec![
                vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
                vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
            ],
        ),
        (
            "SELECT TRY_CAST('42' AS INTEGER), TRY_CAST('bad' AS INTEGER), TRY_CAST([1.0, 2.0] AS VECTOR(3))",
            vec![vec![SqlValue::Integer(42), SqlValue::Null, SqlValue::Null]],
        ),
        (
            "VALUES (2), (2), (1) ORDER BY column1 DESC FETCH FIRST 1 ROW WITH TIES",
            vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(2)]],
        ),
        (
            "SELECT id, label FROM (VALUES (2, 'b'), (1, 'a'), (3, 'c')) AS v(id, label) \
             ORDER BY id OFFSET 1 ROW FETCH NEXT 1 + 0 ROWS ONLY",
            vec![vec![SqlValue::Integer(2), SqlValue::Text("b".into())]],
        ),
        (
            // The two west rows tie on amount; the deterministic D4
            // tie-breaker (schema-order columns) elects id 3.
            "SELECT DISTINCT ON (region) region, id FROM sales ORDER BY region, amount",
            vec![
                vec![SqlValue::Text("east".into()), SqlValue::Integer(1)],
                vec![SqlValue::Text("north".into()), SqlValue::Integer(5)],
                vec![SqlValue::Text("west".into()), SqlValue::Integer(3)],
            ],
        ),
        (
            // Aggregate FILTER + ordered GROUP_CONCAT + ordered-set aggregate
            // (issue #148): the filter excludes qty <= 1 before counting, the
            // concat orders by amount DESC with id ASC tie-break, and
            // PERCENTILE_DISC picks the discrete median of qty.
            "SELECT COUNT(*) FILTER (WHERE qty > 1), \
             GROUP_CONCAT(region ORDER BY amount DESC, id ASC), \
             PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY qty) FROM sales",
            vec![vec![
                SqlValue::BigInt(3),
                SqlValue::Text("east,west,west,east,north".into()),
                SqlValue::Integer(2),
            ]],
        ),
        (
            // GROUPING SETS / ROLLUP (issue #149): the grand-total row prints
            // region as NULL but GROUPING(region) = 1 distinguishes it from
            // any real NULL group (D7).
            "SELECT region, SUM(qty), GROUPING(region) FROM sales \
             GROUP BY ROLLUP(region) ORDER BY GROUPING(region), region",
            vec![
                vec![
                    SqlValue::Text("east".into()),
                    SqlValue::BigInt(4),
                    SqlValue::BigInt(0),
                ],
                vec![
                    SqlValue::Text("north".into()),
                    SqlValue::BigInt(0),
                    SqlValue::BigInt(0),
                ],
                vec![
                    SqlValue::Text("west".into()),
                    SqlValue::BigInt(7),
                    SqlValue::BigInt(0),
                ],
                vec![SqlValue::Null, SqlValue::BigInt(11), SqlValue::BigInt(1)],
            ],
        ),
        (
            // LATERAL (issue #151): the correlated subquery is evaluated once
            // per department row.
            "SELECT d.name, m.total FROM departments AS d CROSS JOIN LATERAL \
             (SELECT SUM(x.n) AS total FROM metrics AS x WHERE x.dept_id = d.id) AS m \
             ORDER BY d.id",
            vec![
                vec![SqlValue::Text("search".into()), SqlValue::BigInt(5)],
                vec![SqlValue::Text("storage".into()), SqlValue::BigInt(4)],
            ],
        ),
        (
            // A table function reads the preceding FROM item without LATERAL
            // being written (D2), and an alias column list renames its output
            // (D8).
            "SELECT m.id, e.component FROM metrics AS m, UNNEST(m.embedding) AS e(component) \
             WHERE m.id = 1 ORDER BY e.component",
            vec![
                vec![SqlValue::Integer(1), SqlValue::Float(0.0)],
                vec![SqlValue::Integer(1), SqlValue::Float(0.0)],
                vec![SqlValue::Integer(1), SqlValue::Float(1.0)],
            ],
        ),
    ];
    require(
        v08_matrix.len() == 30,
        "v0.8.x SQL success-check count changed",
    )?;
    for (sql, expected) in &v08_matrix {
        check_rows(&db, sql, expected)?;
    }

    let rejected_v08 = [
        (
            "SELECT amount AS ident FROM sales WHERE ident > 100",
            "ALOPEX-C003",
        ),
        (
            "SELECT region AS area, COUNT(*) FROM sales GROUP BY area",
            "ALOPEX-C003",
        ),
        ("SELECT CAST('bad' AS INTEGER)", "ALOPEX-E004"),
        (
            "SELECT 1 FETCH FIRST 1 ROW WITH TIES",
            "FETCH ... WITH TIES requires ORDER BY",
        ),
        ("SELECT 1 LIMIT -1", "LIMIT must not be negative"),
        (
            "SELECT DISTINCT ON (region) region FROM sales ORDER BY amount",
            "ALOPEX-T014",
        ),
        (
            "SELECT SUM(amount) WITHIN GROUP (ORDER BY amount) FROM sales",
            "WITHIN GROUP is only valid for ordered-set aggregate functions",
        ),
    ];
    for (sql, expected) in rejected_v08 {
        expect_sql_error(&db, sql, expected)?;
    }
    require(
        v08_matrix.len() + rejected_v08.len() == 37,
        "v0.8.x SQL check count changed",
    )?;
    require(
        matches!(
            db.execute_sql("PRAGMA memory_limit = '64MiB'")?,
            ExecutionResult::Success
        ),
        "PRAGMA memory_limit did not succeed",
    )?;
    let _ = db.execute_sql("SELECT memory_stats() AS memory_stats, io_stats() AS io_stats")?;

    let failed = db.execute_sql_multi(
        "INSERT INTO metrics VALUES (9, 1, 9, 9.0, '2025-01-15 10:30:09', [0.0, 0.0, 1.0]);\
         INSERT INTO metrics VALUES (1, 1, 1, 1.0, '2025-01-15 10:30:10', [0.0, 0.0, 1.0])",
    );
    require(failed.is_err(), "a failing SQL batch was accepted")?;
    let rows = query_rows(db.execute_sql("SELECT id FROM metrics WHERE id = 9")?)?;
    require(rows.is_empty(), "failed SQL batch was partially committed")?;
    Ok(())
}

fn scenario_catalog_cluster_diagnostics() -> DemoResult {
    let db = seed_sql_database()?;
    let epoch_before = db.table_info_cache_epoch();
    let catalog = db.create_catalog(CreateCatalogRequest::new("demo"))?;
    require(
        catalog.name == "demo",
        "catalog creation returned wrong identity",
    )?;
    db.create_namespace(CreateNamespaceRequest::new("demo", "analytics"))?;
    let table = db.create_table(
        CreateTableRequest::new("events")
            .with_catalog_name("demo")
            .with_namespace_name("analytics")
            .with_schema(vec![
                ColumnDefinition::new("id", DataType::Integer).with_nullable(false),
                ColumnDefinition::new("payload", DataType::Text),
            ])
            .with_primary_key(vec!["id".to_string()]),
    )?;
    require(table.name == "events", "catalog table creation failed")?;
    require(
        db.list_tables("demo", "analytics")?.len() == 1,
        "catalog table observation failed",
    )?;
    db.create_table_simple(
        "catalog_indexed",
        vec![ColumnDefinition::new("id", DataType::Integer).with_nullable(false)],
    )?;
    db.execute_sql("CREATE INDEX idx_catalog_id ON catalog_indexed(id)")?;
    require(
        db.list_indexes_simple("catalog_indexed")?
            .iter()
            .any(|index| index.name == "idx_catalog_id"),
        "SQL-created index is absent from catalog API",
    )?;
    let _ = db.get_table_info_cached("default", "default", "metrics")?;
    require(
        db.get_cached_table_info("default", "default", "metrics")
            .is_some(),
        "table cache did not retain metadata",
    )?;
    require(
        db.table_info_cache_epoch() > epoch_before,
        "catalog mutation did not invalidate cache epoch",
    )?;

    let _ = db.execute_sql("SELECT id FROM metrics WHERE id = 1")?;
    let status = db.cluster_status_snapshot()?;
    let routing = db.routing_diagnostics()?;
    println!("cluster status: {status:?}");
    println!("routing diagnostics: {routing:?}");
    require(
        format!("{status:?}").contains("SingleNode"),
        "default embedded status is not single-node",
    )?;
    require(
        format!("{routing:?}").contains("Local"),
        "embedded SQL was not diagnosed as local routing",
    )?;
    Ok(())
}

fn scenario_owned_and_sql_streams() -> DemoResult {
    let db = seed_sql_database()?;
    let callback = db.execute_sql_with_rows(
        "SELECT id, n FROM metrics WHERE n >= 2 ORDER BY id",
        |mut rows| {
            let mut count = 0;
            while rows.next_row()?.is_some() {
                count += 1;
            }
            Ok(count)
        },
    )?;
    require(
        matches!(callback, StreamingQueryResult::QueryProcessed(3)),
        "callback SQL stream lost rows",
    )?;

    let mut iterator = match db.execute_sql_streaming("SELECT id FROM metrics ORDER BY id")? {
        SqlStreamingResult::Query(rows) => rows,
        _ => return Err(std::io::Error::other("iterator SQL stream was not returned").into()),
    };
    let mut count = 0;
    while iterator.next_row()?.is_some() {
        count += 1;
    }
    require(count == 3, "iterator SQL stream lost rows")?;

    let mut plan = OwnedSqlStreamPlan::preflight(
        &db,
        "SELECT id + 10 AS next_id FROM metrics WHERE n >= 3 LIMIT 2",
    )?;
    let session = db.begin_owned_read(OwnedReadOptions::default())?;
    let lease = session.acquire_lease()?;
    let mut cursor = lease.with_transaction(|transaction| plan.open_cursor(transaction))?;
    let mut owned_rows = Vec::new();
    while let Some((key, value)) = cursor.next_entry()? {
        if let OwnedSqlRowOutcome::Row(row) = plan.process_entry(key, value)? {
            owned_rows.push(row);
        }
    }
    require(
        plan.is_exhausted(),
        "owned SQL plan did not reach exhaustion",
    )?;
    cursor.close()?;
    lease.finish(OwnedLeaseOutcome::Exhausted)?;
    require(owned_rows.len() == 2, "owned SQL stream lost rows")?;

    let shared = Arc::new(db);
    let mut owned = Arc::clone(&shared).begin_owned_embedded_transaction(TxnMode::ReadWrite)?;
    owned.put(b"owned:rollback", b"no")?;
    owned.execute_sql(
        "INSERT INTO metrics VALUES (8, 2, 8, 8.0, '2025-01-15 10:30:08', [0.0, 0.0, 1.0])",
    )?;
    owned.rollback()?;
    let mut read = shared.begin(TxnMode::ReadOnly)?;
    require(
        read.get(b"owned:rollback")?.is_none(),
        "owned rollback committed KV",
    )?;
    drop(read);
    require(
        query_rows(shared.execute_sql("SELECT id FROM metrics WHERE id = 8")?)?.is_empty(),
        "owned rollback committed SQL",
    )?;

    let mut committed = Arc::clone(&shared).begin_owned_embedded_transaction(TxnMode::ReadWrite)?;
    committed.put(b"owned:commit", b"yes")?;
    committed.commit()?;
    let mut read = shared.begin(TxnMode::ReadOnly)?;
    require(
        read.get(b"owned:commit")? == Some(b"yes".to_vec()),
        "owned commit lost KV data",
    )?;
    Ok(())
}

fn columnar_batch() -> RecordBatch {
    let schema = Schema {
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
            ColumnSchema {
                name: "value".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
        ],
    };
    RecordBatch::new(
        schema,
        vec![
            Column::Int64(vec![1, 2, 3]),
            Column::Int64(vec![10, 20, 30]),
        ],
        vec![None, None],
    )
}

fn scenario_dataframe_columnar() -> DemoResult {
    let tmp = DemoTemp::new()?;
    let sql_db = seed_sql_database()?;
    let frame = sql_db.query_df("SELECT id, n, f, ts FROM metrics ORDER BY id")?;
    require(frame.height() == 3, "query_df row count changed")?;
    require(frame.width() == 4, "query_df schema width changed")?;
    let float_type = format!(
        "{:?}",
        frame
            .schema()
            .field_with_name("f")
            .map_err(|error| std::io::Error::other(error.to_string()))?
            .data_type()
    );
    require(
        float_type == "Float32",
        "query_df did not preserve FLOAT as Arrow Float32",
    )?;
    println!("DataFrame schema: {:?}", frame.schema());

    let db = Database::open_with_config(EmbeddedConfig::in_memory_with_limit(4 * 1024 * 1024))?;
    let segment = db.write_columnar_segment("events", columnar_batch())?;
    let projected = db.read_columnar_segment("events", segment, Some(&["value"]))?;
    require(
        projected.len() == 1,
        "columnar projection returned wrong batch count",
    )?;
    let table_id = db.resolve_table_id("events")?;
    let handle = format!("{table_id}:{segment}");
    require(
        db.scan_columnar_segment(&handle)?.len() == 3,
        "columnar row scan changed result size",
    )?;
    let stats = db.get_columnar_segment_stats(&handle)?;
    require(
        stats.row_count == 3 && stats.column_count == 2,
        "columnar stats changed",
    )?;
    require(
        db.list_columnar_segments()?.contains(&handle),
        "columnar segment listing omitted the segment",
    )?;
    db.create_columnar_index(&handle, "value", ColumnarIndexType::Minmax)?;
    require(
        db.list_columnar_indexes(&handle)?.len() == 1,
        "columnar index listing failed",
    )?;
    db.drop_columnar_index(&handle, "value")?;
    let segment_file = tmp.path("events.segment");
    db.flush_in_memory_segment_to_file("events", segment, &segment_file)?;
    require(
        segment_file.is_file(),
        "columnar flush did not create an artifact",
    )?;

    let rejection = db.columnar_segment_streaming_factory_v08("events", segment);
    require(
        rejection
            .err()
            .is_some_and(|error| error.to_string().contains("requires_v08_chunked_layout")),
        "legacy V2 was silently exposed as bounded V08 streaming",
    )?;
    Ok(())
}

fn scenario_vector_hnsw() -> DemoResult {
    for metric in [Metric::Cosine, Metric::L2, Metric::InnerProduct] {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadWrite)?;
        txn.upsert_vector(b"a", b"first", &[1.0, 0.0], metric)?;
        txn.upsert_vector(b"b", b"second", &[0.0, 1.0], metric)?;
        require(
            txn.get_vector(b"a", metric)? == Some(vec![1.0, 0.0]),
            "vector point read changed",
        )?;
        let batch = txn.get_vectors(&[b"a".to_vec(), b"missing".to_vec()], metric)?;
        require(
            batch[0].is_some() && batch[1].is_none(),
            "vector batch read changed",
        )?;
        let filtered = txn.search_similar(&[1.0, 0.0], metric, 2, Some(&[b"a".to_vec()]))?;
        require(
            filtered.len() == 1 && filtered[0].key == b"a",
            "vector filter failed",
        )?;
        txn.commit()?;
    }

    let db = Database::new();
    db.create_hnsw_index(
        "docs",
        HnswConfig {
            dimension: 2,
            metric: Metric::L2,
            m: 8,
            ef_construction: 32,
        },
    )?;
    let mut txn = db.begin(TxnMode::ReadWrite)?;
    txn.upsert_to_hnsw("docs", b"origin", &[0.0, 0.0], b"zero")?;
    txn.upsert_to_hnsw("docs", b"near", &[0.0, 1.0], b"one")?;
    txn.upsert_to_hnsw("docs", b"far", &[3.0, 4.0], b"five")?;
    txn.commit()?;
    let (results, search_stats) = db.search_hnsw("docs", &[0.0, 0.0], 3, Some(16))?;
    require(results.len() == 3, "HNSW search lost results")?;
    require(
        results[0].key == b"origin",
        "HNSW ordering is not lower-is-closer",
    )?;
    require(
        results
            .windows(2)
            .all(|pair| pair[0].distance <= pair[1].distance),
        "HNSW public distances are not ascending",
    )?;
    require(
        results.iter().all(|result| result.distance >= 0.0),
        "HNSW L2 public distance is negative",
    )?;
    println!("HNSW results: {results:?}; search stats: {search_stats:?}");
    println!("HNSW index stats: {:?}", db.get_hnsw_stats("docs")?);
    let mut txn = db.begin(TxnMode::ReadWrite)?;
    require(
        txn.delete_from_hnsw("docs", b"far")?,
        "HNSW delete missed key",
    )?;
    txn.commit()?;
    let _ = db.compact_hnsw_index("docs")?;
    db.drop_hnsw_index("docs")?;
    Ok(())
}

fn read_large_value(db: &Database, path: &Path) -> DemoResult<Vec<u8>> {
    let mut reader = db.open_large_value(path)?;
    let mut bytes = Vec::new();
    while let Some((_info, chunk)) = reader.next_chunk()? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn scenario_large_values() -> DemoResult {
    let tmp = DemoTemp::new()?;
    let db = Database::new();
    let payload = b"chunked-large-value";
    let blob_path = tmp.path("payload.blob");
    let mut blob = db.create_blob_writer(&blob_path, payload.len() as u64, Some(5))?;
    for chunk in payload.chunks(5) {
        blob.write_chunk(chunk)?;
    }
    blob.finish()?;
    require(
        read_large_value(&db, &blob_path)? == payload,
        "blob roundtrip changed bytes",
    )?;

    let typed_path = tmp.path("payload.typed");
    let mut typed = db.create_typed_writer(&typed_path, 42, payload.len() as u64, Some(7))?;
    for chunk in payload.chunks(7) {
        typed.write_chunk(chunk)?;
    }
    typed.finish()?;
    require(
        read_large_value(&db, &typed_path)? == payload,
        "typed large-value roundtrip changed bytes",
    )?;
    Ok(())
}

fn scenario_fail_closed_boundaries() -> DemoResult {
    let db = seed_sql_database()?;
    let mut read_only = db.begin(TxnMode::ReadOnly)?;
    require(
        read_only.put(b"forbidden", b"write").is_err(),
        "read-only transaction accepted a write",
    )?;
    read_only.rollback()?;

    let vectors = Database::new();
    let mut txn = vectors.begin(TxnMode::ReadWrite)?;
    txn.upsert_vector(b"v", b"", &[1.0, 0.0], Metric::L2)?;
    txn.commit()?;
    let mut read = vectors.begin(TxnMode::ReadOnly)?;
    require(
        read.search_similar(&[1.0, 0.0, 0.0], Metric::L2, 1, None)
            .is_err(),
        "dimension mismatch was accepted",
    )?;

    let capability = TransactionManager::read_at_capability(&db);
    require(
        matches!(capability, ReadAtCapability::Unavailable { .. }),
        "ordinary embedded backend advertised fenced reads",
    )?;
    let point = ReadAtPoint::new(1, 2, 3, 4);
    require(
        TransactionManager::validate_read_at(&db, &point).is_err(),
        "unavailable fenced read passed validation",
    )?;
    require(
        db.begin_read_at_sql(point).is_err(),
        "fenced SQL silently fell back to a local transaction",
    )?;
    require(
        OwnedSqlStreamPlan::preflight(&db, "SELECT id FROM metrics ORDER BY id")
            .err()
            .is_some_and(|error| error.sql_error_code() == Some("unsupported_streaming_sql")),
        "unsupported owned SQL was not rejected during preflight",
    )?;
    require(
        Database::open_with_uri("s3://demo/not-opened")
            .err()
            .is_some_and(|error| {
                error
                    .to_string()
                    .contains("s3".to_ascii_uppercase().as_str())
                    || error.to_string().contains("S3")
            }),
        "default package unexpectedly opened an S3 backend",
    )?;
    Ok(())
}

fn run() -> DemoResult {
    println!("AlopexDB v0.8.x complete embedded-local API demo");
    run_scenario(
        "EMB-01-storage-durability",
        "storage modes and durability",
        scenario_storage_durability,
    )?;
    run_scenario(
        "EMB-02-kv-transactions",
        "borrowed KV transactions",
        scenario_kv_transactions,
    )?;
    run_scenario(
        "EMB-03-persisted-transaction-manager",
        "persisted transaction manager",
        scenario_persisted_transaction_manager,
    )?;
    run_scenario(
        "EMB-04-local-sql-matrix",
        "complete inherited local SQL matrix",
        scenario_local_sql_matrix,
    )?;
    run_scenario(
        "EMB-05-catalog-cluster-diagnostics",
        "catalog and single-node cluster diagnostics",
        scenario_catalog_cluster_diagnostics,
    )?;
    run_scenario(
        "EMB-06-owned-and-sql-streams",
        "owned sessions and SQL streams",
        scenario_owned_and_sql_streams,
    )?;
    run_scenario(
        "EMB-07-dataframe-columnar",
        "DataFrame and columnar APIs",
        scenario_dataframe_columnar,
    )?;
    run_scenario(
        "EMB-08-vector-hnsw",
        "Vector and HNSW APIs",
        scenario_vector_hnsw,
    )?;
    run_scenario(
        "EMB-09-large-values",
        "chunked large values",
        scenario_large_values,
    )?;
    run_scenario(
        "EMB-10-fail-closed-boundaries",
        "unsupported and external-prerequisite boundaries",
        scenario_fail_closed_boundaries,
    )?;
    println!("\nAll 10 embedded-local scenarios passed.");
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("embedded demo failed: {error}");
        std::process::exit(1);
    }
}
