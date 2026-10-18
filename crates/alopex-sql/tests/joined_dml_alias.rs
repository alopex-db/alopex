//! Public SQL source-alias contract; no hand-built plans or statement-snapshot assumptions.
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::planner::{LogicalPlan, PlannerError};
use alopex_sql::{AlopexDialect, ExecutionResult, Executor, Parser, Planner, SqlValue};

struct Fixture {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl Fixture {
    fn new() -> Self {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut fixture = Self {
            executor: Executor::new(Arc::new(MemoryKV::new()), catalog.clone()),
            catalog,
        };
        for sql in [
            "CREATE TABLE hw(id INTEGER PRIMARY KEY,v INTEGER)",
            "CREATE TABLE source(id INTEGER PRIMARY KEY,v INTEGER)",
            "INSERT INTO hw VALUES (1,10),(2,20),(3,30)",
            "INSERT INTO source VALUES (2,200),(4,400)",
        ] {
            fixture.execute(sql);
        }
        fixture
    }

    fn plan(&self, sql: &str) -> Result<LogicalPlan, PlannerError> {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse real SQL");
        assert_eq!(statements.len(), 1);
        Planner::new(&*self.catalog.read().unwrap()).plan(&statements[0])
    }

    fn execute(&mut self, sql: &str) -> ExecutionResult {
        let plan = self
            .plan(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        self.executor
            .execute(plan)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
    }

    fn assert_rows(&mut self, expected: &[(i32, i32)]) {
        let ExecutionResult::Query(query) = self.execute("SELECT id,v FROM hw ORDER BY id") else {
            panic!("expected query rows");
        };
        assert_eq!(query.rows, values(expected));
    }
}

fn values(rows: &[(i32, i32)]) -> Vec<Vec<SqlValue>> {
    rows.iter()
        .map(|&(id, value)| vec![SqlValue::Integer(id), SqlValue::Integer(value)])
        .collect()
}

fn assert_action(sql: &str, source: &str, returned: &[(i32, i32)], remaining: &[(i32, i32)]) {
    // Separate fresh fixtures prove affected counts and target-only RETURNING.
    for returning in [false, true] {
        let mut fixture = Fixture::new();
        let sql = if returning {
            format!("{sql} RETURNING id,v")
        } else {
            sql.to_owned()
        };
        let plan = fixture
            .plan(&sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        match &plan {
            LogicalPlan::Update {
                table, join_source, ..
            }
            | LogicalPlan::Delete {
                table, join_source, ..
            } => {
                assert_eq!(table, "hw");
                assert_eq!(join_source.as_ref().expect("joined DML").table, source);
            }
            _ => panic!("expected joined DML plan"),
        }
        let result = fixture.executor.execute(plan).expect("execute planned DML");
        if returning {
            let ExecutionResult::Query(query) = result else {
                panic!("expected RETURNING rows");
            };
            assert_eq!(query.rows, values(returned));
        } else {
            assert!(
                matches!(result, ExecutionResult::RowsAffected(n) if n == returned.len() as u64)
            );
        }
        fixture.assert_rows(remaining);
    }
}

#[test]
fn update_source_alias_resolves_with_and_without_as() {
    for source in ["source AS s", "source s"] {
        assert_action(
            &format!("UPDATE hw SET v=s.v+1 FROM {source} WHERE hw.id=s.id"),
            "source",
            &[(2, 201)],
            &[(1, 10), (2, 201), (3, 30)],
        );
    }
}

#[test]
fn delete_source_alias_resolves_with_and_without_as() {
    for source in ["source AS s", "source s"] {
        assert_action(
            &format!("DELETE FROM hw USING {source} WHERE hw.id=s.id"),
            "source",
            &[(2, 20)],
            &[(1, 10), (3, 30)],
        );
    }
}

#[test]
fn update_self_source_alias_distinguishes_column_positions() {
    assert_action(
        "UPDATE hw SET v=s.v+1 FROM hw AS s WHERE hw.id=s.id+1",
        "hw",
        &[(2, 11), (3, 21)],
        &[(1, 10), (2, 11), (3, 21)],
    );
}

#[test]
fn delete_self_source_alias_distinguishes_column_positions() {
    assert_action(
        "DELETE FROM hw USING hw AS s WHERE hw.id=s.id+1",
        "hw",
        &[(2, 20), (3, 30)],
        &[(1, 10)],
    );
}

#[test]
fn update_without_source_alias_preserves_existing_behavior() {
    assert_action(
        "UPDATE hw SET v=source.v+1 FROM source WHERE hw.id=source.id",
        "source",
        &[(2, 201)],
        &[(1, 10), (2, 201), (3, 30)],
    );
}

#[test]
fn delete_without_source_alias_preserves_existing_behavior() {
    assert_action(
        "DELETE FROM hw USING source WHERE hw.id=source.id",
        "source",
        &[(2, 20)],
        &[(1, 10), (3, 30)],
    );
}

fn assert_hidden_name(sql: &str) {
    let mut fixture = Fixture::new();
    let error = fixture
        .plan(sql)
        .expect_err("alias hides physical source qualifier");
    assert!(
        matches!(error, PlannerError::TableNotFound { ref name, .. } if name == "source"),
        "{error}"
    );
    fixture.assert_rows(&[(1, 10), (2, 20), (3, 30)]);
}

#[test]
fn update_alias_hides_physical_source_qualifier() {
    assert_hidden_name("UPDATE hw SET v=1 FROM source s WHERE source.id=hw.id");
}

#[test]
fn delete_alias_hides_physical_source_qualifier() {
    assert_hidden_name("DELETE FROM hw USING source s WHERE source.id=hw.id");
}

fn assert_ambiguous(sql: &str, duplicate: bool) {
    let mut fixture = Fixture::new();
    let error = fixture
        .plan(sql)
        .expect_err("ambiguous reference must not bind to first table");
    let PlannerError::AmbiguousColumn { column, tables, .. } = error else {
        panic!("expected existing ALOPEX-C004 AmbiguousColumn, got {error}");
    };
    assert_eq!(column, "id");
    if duplicate {
        assert_eq!(tables, vec!["hw", "hw"]);
    }
    fixture.assert_rows(&[(1, 10), (2, 20), (3, 30)]);
}

#[test]
fn update_unqualified_shared_column_remains_ambiguous() {
    assert_ambiguous("UPDATE hw SET v=1 FROM source s WHERE id=s.id", false);
}

#[test]
fn delete_unqualified_shared_column_remains_ambiguous() {
    assert_ambiguous("DELETE FROM hw USING source s WHERE id=s.id", false);
}

// These are preservation checks for the TypeChecker's existing C004 contract,
// not a request to introduce a different duplicate-table error variant.
#[test]
fn update_duplicate_qualifier_uses_existing_ambiguity_error() {
    assert_ambiguous("UPDATE hw SET v=1 FROM source hw WHERE hw.id=hw.id", true);
}

#[test]
fn delete_duplicate_qualifier_uses_existing_ambiguity_error() {
    assert_ambiguous("DELETE FROM hw USING source hw WHERE hw.id=hw.id", true);
}
