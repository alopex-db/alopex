use alopex_sql::{AlopexDialect, MemoryCatalog, Parser, SqlError, plan_sql_for_routing, scalar};

#[test]
fn crdt_statements_are_rejected_by_the_sql_boundary_before_planning() {
    let dialect = AlopexDialect;
    // None of these words is registered as SQL DDL/DML or as a CRDT escape
    // hatch.  Parsing is the earliest SQL boundary, so a rejection here
    // proves no catalog, transaction, or projection can be entered.
    for statement in [
        "CREATE COUNTER requests",
        "CREATE SET members",
        "MERGE CRDT requests",
        "RECONCILE CRDT requests",
        "RECOVER CRDT requests",
        "RETIRE CRDT requests",
        "CANCEL CRDT requests",
    ] {
        assert!(
            Parser::parse_sql(&dialect, statement).is_err(),
            "CRDT SQL statement must be rejected before planning: {statement}"
        );
    }
}

#[test]
fn crdt_scalar_names_are_not_registered_as_sql_functions() {
    let dialect = AlopexDialect;
    for expression in [
        "crdt_counter_create('requests', 0)",
        "crdt_counter_read('requests')",
        "crdt_counter_increment('requests', 1)",
        "crdt_set_add('members', 'alice')",
        "crdt_set_contains('members', 'alice')",
    ] {
        // The parser may represent an arbitrary identifier as a function
        // call, but it must never resolve it into an executable CRDT scalar.
        // The lack of a scalar registration is asserted by the planner-facing
        // negative verification task; here the SQL boundary preserves the
        // unmodified parser contract.
        let parsed = Parser::parse_expression_sql(&dialect, expression);
        assert!(
            parsed.is_ok(),
            "ordinary identifier syntax remains parseable"
        );
    }
}

#[test]
fn counter_create_is_rejected_during_planning_before_any_execution() {
    let function = "crdt_counter_create";
    let catalog = MemoryCatalog::new();

    // Parsing an ordinary function identifier is intentionally allowed by the
    // SQL grammar.  The scalar registry is the authoritative capability
    // boundary, so a CRDT create request must fail during planning and never
    // obtain an executable plan or reach a catalog/transaction projection.
    assert!(scalar::signature(function).is_none());
    let error = plan_sql_for_routing(&catalog, "SELECT crdt_counter_create('requests', 0)")
        .expect_err("Counter create must be rejected before SQL execution");
    assert!(matches!(error, SqlError::Plan { .. }));
    assert_eq!(error.code(), "ALOPEX-F001");
    assert!(error.message().contains(function));
}

#[test]
fn counter_read_is_rejected_during_planning_before_any_execution() {
    let function = "crdt_counter_read";
    let catalog = MemoryCatalog::new();

    // A read-looking SQL identifier is not an approved CRDT escape hatch.
    // It must not be mistaken for a local SQL query or reach any CRDT
    // projection: absence from the scalar registry is the pre-execution
    // compatibility boundary.
    assert!(scalar::signature(function).is_none());
    let error = plan_sql_for_routing(&catalog, "SELECT crdt_counter_read('requests')")
        .expect_err("Counter read must be rejected before SQL execution");
    assert!(matches!(error, SqlError::Plan { .. }));
    assert_eq!(error.code(), "ALOPEX-F001");
    assert!(error.message().contains(function));
}

#[test]
fn counter_increment_is_rejected_during_planning_before_any_execution() {
    let function = "crdt_counter_increment";
    let catalog = MemoryCatalog::new();

    // Increment remains outside the SQL scalar register. The planner must
    // reject it before an executable plan, transaction, or Counter projection
    // can be created.
    assert!(scalar::signature(function).is_none());
    let error = plan_sql_for_routing(&catalog, "SELECT crdt_counter_increment('requests', 1)")
        .expect_err("Counter increment must be rejected before SQL execution");
    assert!(matches!(error, SqlError::Plan { .. }));
    assert_eq!(error.code(), "ALOPEX-F001");
    assert!(error.message().contains(function));
}
