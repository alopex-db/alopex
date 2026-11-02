//! Three-row VECTOR Parquet acceptance through persistent public Database APIs.
use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};

fn rows(db: &Database) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = db
        .execute_sql("SELECT id,e FROM vectors ORDER BY id")
        .unwrap()
    else {
        panic!("query expected")
    };
    result.rows
}

fn roundtrip(storage: &str, dimension: usize) {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.db");
    let destination = dir.path().join("destination.db");
    let file = dir.path().join("vectors.parquet");
    let ddl = format!("CREATE TABLE vectors(id INTEGER PRIMARY KEY,e VECTOR({dimension},L2)) WITH (storage='{storage}')");
    let vector = |offset: f32| {
        (0..dimension)
            .map(|i| i as f32 / 4.0 + offset)
            .collect::<Vec<_>>()
    };
    let expected = vec![
        vec![SqlValue::Integer(1), SqlValue::Vector(vector(1.0))],
        vec![SqlValue::Integer(2), SqlValue::Vector(vector(10.0))],
        vec![SqlValue::Integer(3), SqlValue::Null],
    ];
    {
        let db = Database::open(&source).unwrap();
        db.execute_sql(&ddl).unwrap();
        for (id, offset) in [(1, 1.0), (2, 10.0)] {
            let values = vector(offset)
                .iter()
                .map(f32::to_string)
                .collect::<Vec<_>>()
                .join(",");
            db.copy_from_csv_reader(
                "vectors",
                std::io::Cursor::new(format!("{id},\"[{values}]\"\n")),
                false,
            )
            .unwrap();
        }
        db.copy_from_csv_reader("vectors", std::io::Cursor::new("3,\\N\n"), false)
            .unwrap();
    }
    {
        let db = Database::open(&source).unwrap();
        assert_eq!(rows(&db), expected);
        assert_eq!(
            db.execute_sql(&format!(
                "COPY vectors TO '{}' WITH (FORMAT PARQUET)",
                file.display()
            ))
            .unwrap(),
            ExecutionResult::RowsAffected(3)
        );
    }
    {
        let db = Database::open(&destination).unwrap();
        db.execute_sql(&ddl).unwrap();
        assert_eq!(
            db.execute_sql(&format!(
                "COPY vectors FROM '{}' WITH (FORMAT PARQUET)",
                file.display()
            ))
            .unwrap(),
            ExecutionResult::RowsAffected(3)
        );
    }
    let db = Database::open(&destination).unwrap();
    assert_eq!(rows(&db), expected);
    // SQL HNSW search requires row storage; retain the columnar roundtrip
    // assertion, then recreate a row table explicitly for index/search.
    let search_table = if storage == "columnar" {
        db.execute_sql(&format!(
            "CREATE TABLE searchable(id INTEGER PRIMARY KEY,e VECTOR({dimension},L2))"
        ))
        .unwrap();
        db.execute_sql(&format!(
            "COPY searchable FROM '{}' WITH (FORMAT PARQUET)",
            file.display()
        ))
        .unwrap();
        "searchable"
    } else {
        "vectors"
    };
    db.execute_sql(&format!("CREATE INDEX ih ON {search_table}(e) USING HNSW"))
        .unwrap();
    let query = vector(1.0)
        .iter()
        .map(f32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let sql=format!("SELECT id FROM {search_table} ORDER BY vector_distance(e,[{query}],'l2') LIMIT 1 WITH (enable_hnsw=true)");
    let ExecutionResult::Query(result) = db.execute_sql(&sql).unwrap() else {
        panic!("query expected")
    };
    assert_eq!(result.rows, vec![vec![SqlValue::Integer(1)]]);
    assert_eq!(db.hnsw_cache_stats().entries, 1);
}

#[test]
fn row_vector3_roundtrip() {
    roundtrip("row", 3);
}
#[test]
fn row_vector384_roundtrip() {
    roundtrip("row", 384);
}
#[test]
fn columnar_vector3_roundtrip() {
    roundtrip("columnar", 3);
}
#[test]
fn columnar_vector384_roundtrip() {
    roundtrip("columnar", 384);
}
