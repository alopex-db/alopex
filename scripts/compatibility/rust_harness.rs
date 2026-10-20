#[cfg(feature = "legacy-config-literal")]
include!("legacy_config_literal.rs");
mod rust_api;
mod rust_columnar;
use alopex_embedded::{Database, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};
use chrono::{Duration, NaiveDate};
use serde_json::{json, Value};
use std::{env, fs, path::Path};

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn rows(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    match db.execute_sql(sql).unwrap() {
        ExecutionResult::Query(q) => q.rows,
        other => panic!("expected query, got {other:?}"),
    }
}
fn norm(v: &SqlValue) -> Value {
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(x) => json!(x),
        SqlValue::BigInt(x) | SqlValue::Time(x) | SqlValue::Timestamp(x) => json!(x),
        SqlValue::Date(x) => json!(x),
        SqlValue::Float(x) => json!(x),
        SqlValue::Double(x) => json!(x),
        SqlValue::Text(x) => json!(x),
        SqlValue::Boolean(x) => json!(x),
        SqlValue::Vector(x) => json!(x),
        SqlValue::Decimal(x) => json!(x.to_string()),
        other => panic!("uncovered fixture type {other:?}"),
    }
}
fn literal(v: &SqlValue) -> String {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    match v {
        SqlValue::Date(x) => format!(
            "DATE {}",
            quote(&(epoch + Duration::days(i64::from(*x))).to_string())
        ),
        SqlValue::Time(x) => format!(
            "TIME '{:02}:{:02}:{:02}.{:06}'",
            x / 3_600_000_000,
            x / 60_000_000 % 60,
            x / 1_000_000 % 60,
            x % 1_000_000
        ),
        SqlValue::Timestamp(x) => format!(
            "TIMESTAMP {}",
            quote(
                &(epoch.and_hms_opt(0, 0, 0).unwrap() + Duration::microseconds(*x))
                    .format("%Y-%m-%d %H:%M:%S%.6f")
                    .to_string()
            )
        ),
        SqlValue::Null => "NULL".into(),
        SqlValue::Boolean(x) => if *x { "TRUE" } else { "FALSE" }.into(),
        SqlValue::Integer(x) => x.to_string(),
        SqlValue::BigInt(x) => x.to_string(),
        SqlValue::Float(x) => {
            assert!(x.is_finite());
            format!("{x:?}")
        }
        SqlValue::Double(x) => {
            assert!(x.is_finite());
            format!("{x:?}")
        }
        SqlValue::Text(x) => quote(x),
        SqlValue::Decimal(x) => format!("CAST({} AS DECIMAL(18,6))", quote(&x.to_string())),
        SqlValue::Vector(x) => serde_json::to_string(x).unwrap(),
        other => panic!("uncovered fixture type {other:?}"),
    }
}
fn snapshot(db: &Database, keys: &[Vec<u8>], knn: &str) -> Value {
    let mut out = serde_json::Map::new();
    for table in ["scalars", "typed", "vectors", "col"] {
        out.insert(
            table.into(),
            json!(rows(db, &format!("SELECT * FROM {table} ORDER BY id"))
                .iter()
                .map(|r| r.iter().map(norm).collect::<Vec<_>>())
                .collect::<Vec<_>>()),
        );
    }
    let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
    let kv: Vec<_> = keys
        .iter()
        .map(|k| json!([hex(k), hex(&tx.get(k).unwrap().unwrap())]))
        .collect();
    drop(tx);
    out.insert("kv".into(), json!(kv));
    out.insert("knn".into(), norm(&rows(db, knn)[0][0]));
    Value::Object(out)
}
fn read(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}
fn write(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}
fn main() {
    #[cfg(feature = "legacy-config-literal")]
    let _ = legacy_config_literal();
    let args: Vec<_> = env::args().collect();
    let root = Path::new(&args[1]);
    let mode = &args[2];
    let label = &args[3];
    let baseline = env!("CARGO_PKG_NAME").ends_with("baseline");
    if mode == "columnar" {
        rust_columnar::run(root, label);
    } else if mode == "api" {
        rust_api::run(root, label);
    } else if mode == "export" {
        assert!(baseline);
        let fixture = read(&root.join("fixture.json"));
        let path = root.join(format!("{label}-source-db"));
        let db = Database::open(&path).unwrap();
        for sql in fixture["ddl"]
            .as_array()
            .unwrap()
            .iter()
            .chain(fixture["seed"].as_array().unwrap())
        {
            db.execute_sql(sql.as_str().unwrap()).unwrap();
        }
        db.copy_from_csv_reader(
            "col",
            &b"id,note,score\n1,columnar,1.5\n2,second,-2.0\n"[..],
            true,
        )
        .unwrap();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        for (k, v) in [
            (b"migration:\0key".as_slice(), b"\0\xffvalue".as_slice()),
            (b"migration:empty".as_slice(), b"".as_slice()),
        ] {
            tx.put(k, v).unwrap();
        }
        tx.commit().unwrap();
        db.execute_sql(fixture["index"].as_str().unwrap()).unwrap();
        db.flush().unwrap();
        drop(db);
        let db = Database::open(&path).unwrap();
        let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
        let pairs: Vec<_> = tx
            .scan_prefix(b"migration:")
            .unwrap()
            .map(|(k, v)| json!([hex(&k), hex(&v)]))
            .collect();
        drop(tx);
        let keys: Vec<_> = pairs
            .iter()
            .map(|p| unhex(p[0].as_str().unwrap()))
            .collect();
        let expected = snapshot(&db, &keys, fixture["knn_query"].as_str().unwrap());
        let mut statements = Vec::new();
        for table in ["scalars", "typed", "vectors"] {
            for row in rows(&db, &format!("SELECT * FROM {table} ORDER BY id")) {
                statements.push(format!(
                    "INSERT INTO {table} VALUES ({})",
                    row.iter().map(literal).collect::<Vec<_>>().join(",")
                ));
            }
        }
        let columnar_csv = root.join(format!("{label}-col-logical.csv"));
        let mut writer = csv::Writer::from_path(&columnar_csv).unwrap();
        writer.write_record(["id", "note", "score"]).unwrap();
        for row in rows(&db, "SELECT * FROM col ORDER BY id") {
            writer
                .write_record(row.iter().map(|v| match norm(v) {
                    Value::String(s) => s,
                    x => x.to_string(),
                }))
                .unwrap();
        }
        writer.flush().unwrap();
        let parquet = root.join(format!("{label}-col.parquet"));
        let parquet_result = db
            .execute_sql(&format!(
                "COPY col TO {} WITH (FORMAT PARQUET)",
                quote(parquet.to_str().unwrap())
            ))
            .unwrap();
        let scalar_csv = root.join(format!("{label}-scalars.csv"));
        db.execute_sql(&format!(
            "COPY scalars TO {} WITH (FORMAT CSV, HEADER TRUE)",
            quote(scalar_csv.to_str().unwrap())
        ))
        .unwrap();
        write(
            &root.join(format!("{label}-bundle.json")),
            &json!({"source":"Rust 0.8.15", "ddl":fixture["ddl"], "statements":statements,"index":fixture["index"],"knn_query":fixture["knn_query"],"columnar_csv":columnar_csv,"columnar_parquet":parquet,"scalar_csv":scalar_csv,"kv":pairs,"expected":expected,"limitations":{"schema":"explicit supplied DDL/index recreation","kv":"Rust raw namespace enumeration","columnar_parquet_export_result":format!("{parquet_result:?}")}}),
        );
        println!("Rust 0.8.15 persisted/reopened export: {expected}");
    } else if mode == "direct" {
        assert!(!baseline);
        let source = &args[4];
        let bundle = read(&root.join(format!("{source}-bundle.json")));
        let path = root.join(format!("{source}-rust-direct-db"));
        let db = Database::open(&path).unwrap();
        let keys: Vec<_> = bundle["kv"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| unhex(p[0].as_str().unwrap()))
            .collect();
        let expected = &bundle["expected"];
        assert_eq!(
            snapshot(&db, &keys, bundle["knn_query"].as_str().unwrap()),
            *expected
        );
        let query = format!(
            "{} WITH (enable_hnsw = true)",
            bundle["knn_query"].as_str().unwrap()
        );
        assert_eq!(norm(&rows(&db, &query)[0][0]), expected["knn"]);
        let plan = format!("{:?}", db.execute_sql(&format!("EXPLAIN {query}")).unwrap());
        assert!(plan.contains("HnswSearch"), "{plan}");
        db.flush().unwrap();
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(
            snapshot(&db, &keys, bundle["knn_query"].as_str().unwrap()),
            *expected
        );
        write(
            &root.join(format!("{label}-result.json")),
            &json!({
            "direct_open_pass":true,"index_recreated":false,"second_reopen_pass":true,
            "source":bundle["source"],"destination":"Rust candidate","hnsw_plan":plan}),
        );
    } else {
        assert!(!baseline);
        let bundle = read(&root.join(format!("{}-bundle.json", args[4])));
        let path = root.join(format!("{label}-destination-db"));
        let db = Database::open(&path).unwrap();
        for sql in bundle["ddl"]
            .as_array()
            .unwrap()
            .iter()
            .chain(bundle["statements"].as_array().unwrap())
        {
            db.execute_sql(sql.as_str().unwrap()).unwrap();
        }
        db.copy_from_csv_reader(
            "col",
            fs::File::open(bundle["columnar_csv"].as_str().unwrap()).unwrap(),
            true,
        )
        .unwrap();
        let pairs = bundle["kv"].as_array().unwrap();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        for pair in pairs {
            tx.put(
                &unhex(pair[0].as_str().unwrap()),
                &unhex(pair[1].as_str().unwrap()),
            )
            .unwrap();
        }
        tx.commit().unwrap();
        db.execute_sql(bundle["index"].as_str().unwrap()).unwrap();
        db.flush().unwrap();
        drop(db);
        let db = Database::open(&path).unwrap();
        let keys: Vec<_> = pairs
            .iter()
            .map(|p| unhex(p[0].as_str().unwrap()))
            .collect();
        let actual = snapshot(&db, &keys, bundle["knn_query"].as_str().unwrap());
        assert_eq!(actual, bundle["expected"]);
        db.execute_sql("INSERT INTO scalars VALUES (4,'new write',42,2.5,TRUE)")
            .unwrap();
        db.execute_sql("UPDATE scalars SET note='updated' WHERE id=4")
            .unwrap();
        assert_eq!(
            norm(&rows(&db, "SELECT note FROM scalars WHERE id=4")[0][0]),
            json!("updated")
        );
        db.flush().unwrap();
        write(
            &root.join(format!("{label}-result.json")),
            &json!({"source":bundle["source"],"destination":"Rust candidate","logical_roundtrip_pass":true,"new_write_update_pass":true,"post_reopen_snapshot":actual,"limitations":bundle["limitations"]}),
        );
        println!("Rust 0.8.16 import/reopen/query/new-write PASS");
    }
}
