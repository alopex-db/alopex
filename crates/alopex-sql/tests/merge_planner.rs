use alopex_sql::catalog::{Catalog, ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::planner::{LogicalPlan, MergeActionPlan, Planner, PlannerError, TypedExprKind};
use alopex_sql::{AlopexDialect, Parser, ResolvedType};

fn plan(sql: &str) -> Result<LogicalPlan, PlannerError> {
    let mut catalog = MemoryCatalog::new();
    for name in ["target", "source"] {
        catalog
            .create_table(TableMetadata::new(
                name,
                vec![
                    ColumnMetadata::new("id", ResolvedType::Integer),
                    ColumnMetadata::new("value", ResolvedType::Integer),
                    ColumnMetadata::new("enabled", ResolvedType::Boolean),
                ],
            ))
            .unwrap();
    }
    let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
    Planner::new(&catalog).plan(&statement)
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn merge_infers_scoped_conditions_across_multiple_clauses() {
    let LogicalPlan::Merge { on, clauses, .. } = plan(
        "MERGE INTO target USING source ON target.id = source.id
         WHEN MATCHED AND source.enabled THEN UPDATE SET value = source.value
         WHEN MATCHED AND target.enabled THEN DO NOTHING
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)",
    )
    .unwrap() else {
        panic!("expected MERGE plan");
    };
    assert_eq!(on.resolved_type, ResolvedType::Boolean);
    assert_eq!(clauses.len(), 3);
    for (clause, relation, index) in [(&clauses[0], "source", 5), (&clauses[1], "target", 2)] {
        assert!(clause.matched);
        let condition = clause.condition.as_ref().expect("conditional WHEN");
        assert_eq!(condition.resolved_type, ResolvedType::Boolean);
        assert!(matches!(
            &condition.kind,
            TypedExprKind::ColumnRef { table, column, column_index }
                if table == relation && column == "enabled" && *column_index == index
        ));
    }
    assert!(!clauses[2].matched);
    assert!(clauses[2].condition.is_none());
    let MergeActionPlan::Update { assignments } = &clauses[0].action else {
        panic!("expected UPDATE");
    };
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].value.resolved_type, ResolvedType::Integer);
    assert!(matches!(
        &assignments[0].value.kind,
        TypedExprKind::ColumnRef { table, column_index: 4, .. } if table == "source"
    ));
    assert!(matches!(clauses[1].action, MergeActionPlan::DoNothing));
    let MergeActionPlan::Insert { values, .. } = &clauses[2].action else {
        panic!("expected INSERT");
    };
    assert_eq!(values.len(), 2);
    assert!(
        values
            .iter()
            .all(|value| value.resolved_type == ResolvedType::Integer)
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn merge_rejects_non_boolean_when_conditions() {
    for clause in ["MATCHED", "NOT MATCHED"] {
        let sql = format!(
            "MERGE INTO target USING source ON target.id = source.id
             WHEN {clause} AND source.value THEN DO NOTHING"
        );
        let error = plan(&sql).unwrap_err();
        assert!(
            matches!(
                error,
                PlannerError::TypeMismatch { expected, found, .. }
                    if expected == "Boolean" && found == ResolvedType::Integer.to_string()
            ),
            "{sql}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn merge_propagates_when_condition_resolution_errors() {
    for clause in ["MATCHED", "NOT MATCHED"] {
        let sql = format!(
            "MERGE INTO target USING source ON target.id = source.id
             WHEN {clause} AND source.missing = 1 THEN DO NOTHING"
        );
        assert!(
            matches!(
                plan(&sql).unwrap_err(),
                PlannerError::ColumnNotFound { column, .. } if column == "missing"
            ),
            "{sql}"
        );
    }
}
