use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn query(sql: &str) -> QueryResult {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let statement = Parser::parse_sql(&AlopexDialect, sql)
        .expect("parse SQL")
        .remove(0);
    let plan = Planner::new(&*catalog.read().expect("catalog read"))
        .plan(&statement)
        .expect("plan SQL");
    match executor.execute(plan).expect("execute SQL") {
        ExecutionResult::Query(result) => result,
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn portable_scalar_functions_are_wired_through_sql() {
    let result = query(
        "SELECT CBRT(-8), COT(0.7853981633974483), LOG2(8), ACOSH(1), \
                ASINH(0), ATANH(0), COSH(0), SINH(0), TANH(0), \
                ISNAN(POWER(-1, 0.5)), ASCII('A'), CHR(65), \
                BIT_LENGTH('あ'), STARTS_WITH('alopex', 'alo'), \
                ENDS_WITH('alopex', 'pex'), TRANSLATE('abc', 'ac', 'XY'), \
                LEVENSHTEIN('kitten', 'sitting'), \
                REGEXP_LIKE('Alopex', '^alo', 'i')",
    );

    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert!(matches!(row[0], SqlValue::Double(value) if (value + 2.0).abs() < 1e-12));
    assert!(matches!(row[1], SqlValue::Double(value) if (value - 1.0).abs() < 1e-12));
    assert_eq!(row[2], SqlValue::Double(3.0));
    assert_eq!(
        &row[3..9],
        vec![
            SqlValue::Double(0.0),
            SqlValue::Double(0.0),
            SqlValue::Double(0.0),
            SqlValue::Double(1.0),
            SqlValue::Double(0.0),
            SqlValue::Double(0.0)
        ]
    );
    assert_eq!(row[9], SqlValue::Boolean(true));
    assert_eq!(row[10], SqlValue::Integer(65));
    assert_eq!(row[11], SqlValue::Text("A".into()));
    assert_eq!(row[12], SqlValue::Integer(24));
    assert_eq!(row[13], SqlValue::Boolean(true));
    assert_eq!(row[14], SqlValue::Boolean(true));
    assert_eq!(row[15], SqlValue::Text("XbY".into()));
    assert_eq!(row[16], SqlValue::Integer(3));
    assert_eq!(row[17], SqlValue::Boolean(true));
}

#[test]
fn portable_scalar_functions_propagate_null() {
    let result = query(
        "SELECT CBRT(NULL), ISNAN(NULL), ASCII(NULL), CHR(NULL), \
                BIT_LENGTH(NULL), STARTS_WITH(NULL, 'x'), \
                TRANSLATE(NULL, 'x', 'y'), LEVENSHTEIN(NULL, 'x'), \
                REGEXP_LIKE(NULL, 'x')",
    );
    assert!(result.rows[0].iter().all(SqlValue::is_null));
}
