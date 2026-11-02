//! Small behavioral checks for bound query options; no timing or recall benchmark.
use std::sync::Arc;

use alopex_embedded::{bind_sql_parameters, Database};
use alopex_sql::{AlopexDialect, ExecutionResult, Parser, SqlValue};

const QUERY: &str = "SELECT id FROM items ORDER BY vector_distance(e, ?, 'l2') LIMIT 1 WITH (ef_search = ?, enable_hnsw = ?)";

fn database(rows: usize) -> Arc<Database> {
    let db = Arc::new(Database::open_in_memory().unwrap());
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, e VECTOR(2,L2))")
        .unwrap();
    let values = (0..rows)
        .map(|i| format!("({i},[{i},0])"))
        .collect::<Vec<_>>()
        .join(",");
    db.execute_sql(&format!("INSERT INTO items VALUES {values}"))
        .unwrap();
    db.execute_sql("CREATE INDEX items_h ON items(e) USING HNSW WITH (m=2, ef_construction=16)")
        .unwrap();
    db
}

fn bindings(ef: i32, enabled: bool) -> Vec<SqlValue> {
    vec![
        SqlValue::Vector(vec![0.0, 0.0]),
        SqlValue::Integer(ef),
        SqlValue::Boolean(enabled),
    ]
}

fn text(result: ExecutionResult) -> String {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query")
    };
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            SqlValue::Text(text) => text.as_str(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn metric(text: &str, key: &str) -> usize {
    text.split_whitespace()
        .find_map(|part| part.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("missing {key}: {text}"))
        .parse()
        .unwrap()
}

#[test]
fn bound_ef_search_changes_executed_graph_search() {
    // 32 two-dimensional vectors suffice to distinguish ef=16 from the
    // row-count-capped ef=256. No large recall fixture is needed.
    let db = database(32);
    let mut statement = db.prepare(&format!("EXPLAIN ANALYZE {QUERY}")).unwrap();
    let results = statement
        .execute_many([bindings(16, true), bindings(256, true)])
        .unwrap();
    let low = text(results[0].clone());
    let high = text(results[1].clone());
    assert!(low.contains("HnswSearch") && high.contains("HnswSearch"));
    assert_eq!(metric(&low, "ef_search"), 16, "{low}");
    assert_eq!(metric(&high, "ef_search"), 32, "{high}");
    println!(
        "bound ef 16/256: nodes_visited={}/{}",
        metric(&low, "nodes_visited"),
        metric(&high, "nodes_visited")
    );
    assert!(
        metric(&low, "nodes_visited") < metric(&high, "nodes_visited"),
        "low: {low}\nhigh: {high}"
    );
}

#[test]
fn bound_false_selects_exact_path() {
    let db = database(1);
    let mut statement = db.prepare(&format!("EXPLAIN ANALYZE {QUERY}")).unwrap();
    let mut results = statement
        .execute_many([bindings(16, true), bindings(16, false)])
        .unwrap()
        .into_iter();
    let enabled = text(results.next().unwrap());
    assert!(enabled.contains("HnswSearch"), "{enabled}");
    let output = text(results.next().unwrap());
    assert!(output.contains("ExactKnnScan"), "{output}");
    assert!(!output.contains("HnswSearch"), "{output}");
    assert_eq!(metric(&output, "nodes_visited"), 0, "{output}");
}

#[test]
fn execute_many_uses_each_bound_ef_search() {
    // Exactly 256 rows are needed to observe all requested effective values
    // without the live-row cap. This is a single 3-statement behavior check.
    let db = database(256);
    let mut statement = db.prepare(&format!("EXPLAIN ANALYZE {QUERY}")).unwrap();
    let results = statement
        .execute_many([16, 64, 256].map(|ef| bindings(ef, true)))
        .unwrap();
    assert_eq!(results.len(), 3);
    for (result, expected) in results.into_iter().zip([16, 64, 256]) {
        let output = text(result);
        assert_eq!(metric(&output, "ef_search"), expected, "{output}");
    }
}

#[test]
fn rendered_bound_options_reparse_and_match_typed_execution() {
    let db = database(1);
    for enabled in [false, true] {
        let values = bindings(16, enabled);
        let rendered = bind_sql_parameters(QUERY, &values).unwrap();
        Parser::parse_sql(&AlopexDialect, &rendered).unwrap();
        assert!(rendered.contains("ef_search = 16"), "{rendered}");
        let mut statement = db.prepare(QUERY).unwrap();
        let typed = statement.execute_many([values]).unwrap().remove(0);
        let ExecutionResult::Query(typed) = typed else {
            panic!("query expected")
        };
        let ExecutionResult::Query(rendered) = db.execute_sql(&rendered).unwrap() else {
            panic!("query expected")
        };
        assert_eq!(typed.rows, rendered.rows);
        assert_eq!(typed.rows, vec![vec![SqlValue::Integer(0)]]);
    }
}

fn assert_bad(option: &str, values: &[SqlValue]) {
    let db = database(1);
    let sql = format!(
        "SELECT id FROM items ORDER BY vector_distance(e,[0,0],'l2') LIMIT 1 WITH ({option} = ?)"
    );
    let mut statement = db.prepare(&sql).unwrap();
    for value in values {
        statement.bind(1, value.clone()).unwrap();
        let error = statement.execute().unwrap_err().to_string();
        assert!(error.contains(option) && error.contains("?1"), "{error}");
        let expected = match value {
            SqlValue::BigInt(value) => value.to_string(),
            _ => format!("got {value:?}"),
        };
        assert!(error.contains(&expected), "{error}");
    }
}

#[test]
fn invalid_bound_ef_reports_received_value() {
    assert_bad(
        "ef_search",
        &[
            SqlValue::Integer(0),
            SqlValue::Integer(-1),
            SqlValue::BigInt(1_000_001),
            SqlValue::Text("16".into()),
            SqlValue::Double(16.0),
            SqlValue::Null,
        ],
    );
}

#[test]
fn invalid_bound_hnsw_flag_reports_received_value() {
    assert_bad(
        "enable_hnsw",
        &[
            SqlValue::Integer(1),
            SqlValue::Text("true".into()),
            SqlValue::Null,
        ],
    );
}
