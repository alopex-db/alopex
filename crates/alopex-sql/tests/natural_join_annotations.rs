use alopex_sql::ast::dml::{InsertSource, OnConflictAction, QueryBody, SelectItem};
use alopex_sql::{AlopexDialect, ExprKind, FromItem, Parser, StatementKind};

#[test]
fn natural_wire_flags_follow_relations_across_expression_reordering() {
    for sql in [
        "SELECT POSITION((SELECT CAST(COUNT(*) AS TEXT) FROM a NATURAL JOIN b) IN (SELECT CAST(COUNT(*) AS TEXT) FROM c JOIN d ON c.k = d.k))",
        "SELECT 1 OFFSET (SELECT COUNT(*) FROM a NATURAL JOIN b) LIMIT (SELECT COUNT(*) FROM c JOIN d ON c.k = d.k)",
        "SELECT 1 LIMIT (SELECT COUNT(*) FROM c JOIN d ON c.k = d.k) OFFSET (SELECT COUNT(*) FROM a NATURAL JOIN b)",
    ] {
        let parsed = Parser::parse_sql(&AlopexDialect, sql).unwrap();
        let ast = serde_json::to_value(parsed).unwrap();
        fn check(value: &serde_json::Value, seen: &mut usize) {
            match value {
                serde_json::Value::Object(fields) => {
                    if fields.get("variant").and_then(|v| v.as_str()) == Some("Join") {
                        let name = fields["left"]["name"].as_str().unwrap();
                        assert_eq!(fields["natural"].as_bool(), Some(name == "a"), "{name}");
                        *seen += 1;
                    }
                    for value in fields.values() {
                        check(value, seen);
                    }
                }
                serde_json::Value::Array(values) => {
                    for value in values {
                        check(value, seen);
                    }
                }
                _ => {}
            }
        }
        let mut seen = 0;
        check(&ast, &mut seen);
        assert_eq!(seen, 2, "{sql}");
    }
}

#[test]
fn natural_wire_comma_has_lower_precedence_than_explicit_join() {
    let parsed = Parser::parse_sql(&AlopexDialect, "SELECT * FROM a, b NATURAL JOIN c").unwrap();
    let StatementKind::Select(select) = &parsed[0].kind else {
        panic!("SELECT");
    };
    let FromItem::Join {
        left,
        right,
        join_type,
        natural,
        ..
    } = &select.from[0]
    else {
        panic!("comma");
    };
    assert_eq!(*join_type, alopex_sql::JoinType::Cross);
    assert!(!natural);
    assert!(matches!(left.as_ref(), FromItem::Table { name, .. } if name == "a"));
    let FromItem::Join {
        left,
        right,
        natural,
        ..
    } = right.as_ref()
    else {
        panic!("explicit JOIN");
    };
    assert!(*natural);
    assert!(matches!(left.as_ref(), FromItem::Table { name, .. } if name == "b"));
    assert!(matches!(right.as_ref(), FromItem::Table { name, .. } if name == "c"));
}

#[test]
fn natural_wire_invalid_combinations_are_rejected() {
    for sql in [
        "SELECT * FROM a NATURAL CROSS JOIN b",
        "SELECT * FROM a NATURAL JOIN b ON a.k = b.k",
        "SELECT * FROM a NATURAL JOIN b USING (k)",
    ] {
        assert!(Parser::parse_sql(&AlopexDialect, sql).is_err(), "{sql}");
    }
}

fn assert_join_natural(from: &FromItem, expected: bool) {
    let FromItem::Join { natural, .. } = from else {
        panic!("expected JOIN");
    };
    assert_eq!(*natural, expected);
}

#[test]
fn comma_and_cross_table_function_joins_do_not_consume_nested_natural_markers() {
    let parsed = Parser::parse_sql(
        &AlopexDialect,
        "SELECT p.id, u.unnest FROM lat_parent AS p, UNNEST(p.emb) AS u ORDER BY p.id, u.unnest",
    )
    .unwrap();
    let StatementKind::Select(select) = &parsed[0].kind else {
        panic!("expected SELECT")
    };
    assert_join_natural(&select.from[0], false);
    for separator in [",", "CROSS JOIN"] {
        let parsed = Parser::parse_sql(
            &AlopexDialect,
            &format!(
                "SELECT * FROM a {separator} (SELECT * FROM b NATURAL JOIN c) q;
             SELECT * FROM d NATURAL JOIN e"
            ),
        )
        .unwrap();
        let StatementKind::Select(select) = &parsed[0].kind else {
            panic!("expected SELECT")
        };
        assert_join_natural(&select.from[0], false);
        let FromItem::Join { right, .. } = &select.from[0] else {
            panic!("expected JOIN")
        };
        let FromItem::Derived { subquery, .. } = right.as_ref() else {
            panic!("expected derived")
        };
        let QueryBody::Select(inner) = subquery.as_ref() else {
            panic!("expected inner SELECT")
        };
        assert_join_natural(&inner.from[0], true);
        let StatementKind::Select(next) = &parsed[1].kind else {
            panic!("expected next SELECT")
        };
        assert_join_natural(&next.from[0], true);
    }
}

#[test]
fn cross_join_markers_preserve_both_children_and_ignore_quoted_keywords() {
    for separator in [",", "CROSS /* JOIN NATURAL */ JOIN"] {
        let parsed = Parser::parse_sql(
            &AlopexDialect,
            &format!(
                "SELECT 'CROSS JOIN NATURAL' FROM (SELECT * FROM a NATURAL JOIN b) l
             {separator} (SELECT * FROM c JOIN d USING (id)) r"
            ),
        )
        .unwrap();
        let StatementKind::Select(select) = &parsed[0].kind else {
            panic!("expected SELECT")
        };
        assert_join_natural(&select.from[0], false);
        let FromItem::Join { left, right, .. } = &select.from[0] else {
            panic!("expected JOIN")
        };
        for (child, natural) in [(left, true), (right, false)] {
            let FromItem::Derived { subquery, .. } = child.as_ref() else {
                panic!("expected derived")
            };
            let QueryBody::Select(inner) = subquery.as_ref() else {
                panic!("expected SELECT")
            };
            assert_join_natural(&inner.from[0], natural);
        }
    }
    let parsed = Parser::parse_sql(
        &AlopexDialect,
        "SELECT * FROM a AS \"cross\" JOIN b USING (id)",
    )
    .unwrap();
    let StatementKind::Select(select) = &parsed[0].kind else {
        panic!("expected SELECT")
    };
    assert_join_natural(&select.from[0], false);
}

#[test]
fn natural_cross_join_is_rejected_instead_of_losing_its_annotation() {
    let error =
        Parser::parse_sql(&AlopexDialect, "SELECT * FROM a NATURAL CROSS JOIN b").unwrap_err();
    assert!(error.to_string().contains("NATURAL CROSS JOIN"), "{error}");
}

#[test]
fn ctas_query_preserves_nested_join_markers_across_explain_and_statements() {
    for prefix in ["EXPLAIN", "EXPLAIN ANALYZE"] {
        let parsed = Parser::parse_sql(
            &AlopexDialect,
            &format!(
                "{prefix} CREATE TABLE copied AS
             SELECT * FROM (SELECT * FROM a NATURAL JOIN b) q JOIN d USING (id);
             SELECT * FROM e NATURAL JOIN f"
            ),
        )
        .unwrap();
        let StatementKind::Explain { statement, .. } = &parsed[0].kind else {
            panic!("expected EXPLAIN");
        };
        let StatementKind::CreateTable(table) = &statement.kind else {
            panic!("expected CTAS");
        };
        let query = table.query.as_ref().expect("CTAS SELECT");
        assert_join_natural(&query.from[0], false);
        let FromItem::Join { left, .. } = &query.from[0] else {
            panic!("expected outer USING join");
        };
        let FromItem::Derived { subquery, .. } = left.as_ref() else {
            panic!("expected derived source");
        };
        let QueryBody::Select(inner) = subquery.as_ref() else {
            panic!("expected inner SELECT");
        };
        assert_join_natural(&inner.from[0], true);
        let StatementKind::Select(next) = &parsed[1].kind else {
            panic!("expected next SELECT");
        };
        assert_join_natural(&next.from[0], true);
    }
}

#[test]
fn cte_and_main_query_keep_distinct_join_markers() {
    let parsed = Parser::parse_sql(
        &AlopexDialect,
        "EXPLAIN WITH c AS (SELECT * FROM a NATURAL JOIN b)
         SELECT * FROM c JOIN d USING (id)",
    )
    .unwrap();
    let StatementKind::Explain { statement, .. } = &parsed[0].kind else {
        panic!("expected EXPLAIN");
    };
    let StatementKind::Select(select) = &statement.kind else {
        panic!("expected SELECT");
    };
    let QueryBody::Select(cte) = select.with.as_ref().unwrap().ctes[0].query.as_ref() else {
        panic!("expected SELECT CTE");
    };
    assert_join_natural(&cte.from[0], true);
    assert_join_natural(&select.from[0], false);
}

#[test]
fn derived_and_outer_query_keep_distinct_join_markers() {
    let parsed = Parser::parse_sql(
        &AlopexDialect,
        "EXPLAIN SELECT * FROM (SELECT * FROM a NATURAL JOIN b) q
         JOIN c USING (id)",
    )
    .unwrap();
    let StatementKind::Explain { statement, .. } = &parsed[0].kind else {
        panic!("expected EXPLAIN");
    };
    let StatementKind::Select(select) = &statement.kind else {
        panic!("expected SELECT");
    };
    assert_join_natural(&select.from[0], false);
    let FromItem::Join { left, .. } = &select.from[0] else {
        panic!("expected outer JOIN");
    };
    let FromItem::Derived { subquery, .. } = left.as_ref() else {
        panic!("expected derived table");
    };
    let QueryBody::Select(derived) = subquery.as_ref() else {
        panic!("expected derived SELECT");
    };
    assert_join_natural(&derived.from[0], true);
}

#[test]
fn insert_source_conflict_and_returning_keep_distinct_join_markers() {
    let parsed = Parser::parse_sql(
        &AlopexDialect,
        "INSERT INTO x SELECT a.id FROM a JOIN b USING (id)
         ON CONFLICT (id) DO UPDATE SET id = (SELECT c.id FROM c NATURAL JOIN d)
         RETURNING (SELECT e.id FROM e JOIN f USING (id))",
    )
    .unwrap();
    let StatementKind::Insert(insert) = &parsed[0].kind else {
        panic!("expected INSERT");
    };
    let InsertSource::Select { select } = &insert.source else {
        panic!("expected INSERT SELECT");
    };
    assert_join_natural(&select.from[0], false);
    let OnConflictAction::DoUpdate { assignments, .. } =
        &insert.on_conflict.as_ref().unwrap().action
    else {
        panic!("expected conflict UPDATE");
    };
    let ExprKind::ScalarSubquery { subquery } = &assignments[0].value.kind else {
        panic!("expected assignment subquery");
    };
    let StatementKind::Select(conflict) = &subquery.kind else {
        panic!("expected conflict SELECT");
    };
    assert_join_natural(&conflict.from[0], true);
    let SelectItem::Expr { expr, .. } = &insert.returning[0] else {
        panic!("expected RETURNING expression");
    };
    let ExprKind::ScalarSubquery { subquery } = &expr.kind else {
        panic!("expected RETURNING subquery");
    };
    let StatementKind::Select(returning) = &subquery.kind else {
        panic!("expected RETURNING SELECT");
    };
    assert_join_natural(&returning.from[0], false);
}

#[test]
fn explain_join_forms_and_nested_queries_parse() {
    let queries = [
        "SELECT * FROM a INNER JOIN b ON a.id = b.id",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.id",
        "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id",
        "SELECT * FROM a JOIN b USING (id)",
        "SELECT * FROM a NATURAL JOIN b",
        "SELECT * FROM a NATURAL JOIN b JOIN c ON b.id = c.id",
        "WITH c AS (SELECT * FROM a NATURAL JOIN b) SELECT * FROM c JOIN d USING (id)",
        "SELECT * FROM (SELECT * FROM a NATURAL JOIN b) q JOIN c USING (id)",
        "SELECT * FROM a JOIN b ON a.id IN (SELECT c.id FROM c NATURAL JOIN d)",
        "VALUES ((SELECT a.id FROM a NATURAL JOIN b))",
    ];
    for prefix in ["EXPLAIN", "EXPLAIN ANALYZE"] {
        for query in queries {
            let sql = format!("{prefix} {query}");
            let statements = Parser::parse_sql(&AlopexDialect, &sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert!(matches!(
                &statements[0].kind,
                StatementKind::Explain { analyze, .. } if *analyze == (prefix == "EXPLAIN ANALYZE")
            ));
        }
    }
}

#[test]
fn statement_expression_positions_consume_join_markers() {
    // These are parser contracts. Execution support for a subquery in a given
    // position remains the planner's responsibility.
    let statements = [
        "INSERT INTO x SELECT a.id FROM a NATURAL JOIN b",
        "INSERT INTO x VALUES ((SELECT a.id FROM a NATURAL JOIN b))",
        "CREATE VIEW v AS SELECT * FROM a NATURAL JOIN b",
        "UPDATE x SET id = 1 WHERE id IN (SELECT a.id FROM a NATURAL JOIN b)",
        "UPDATE x SET id = (SELECT a.id FROM a NATURAL JOIN b)",
        "UPDATE x SET id = a.id FROM a NATURAL JOIN b WHERE x.id = a.id",
        "DELETE FROM x WHERE id IN (SELECT a.id FROM a NATURAL JOIN b)",
        "DELETE FROM x USING a NATURAL JOIN b WHERE x.id = a.id",
        "EXPLAIN INSERT INTO x SELECT a.id FROM a NATURAL JOIN b",
        "EXPLAIN ANALYZE INSERT INTO x SELECT a.id FROM a NATURAL JOIN b",
        "INSERT INTO x VALUES (1) ON CONFLICT (id) DO UPDATE SET id = (SELECT a.id FROM a NATURAL JOIN b)",
        "INSERT INTO x VALUES (1) ON CONFLICT (id) DO UPDATE SET id = 2 WHERE EXISTS (SELECT * FROM a NATURAL JOIN b)",
        "INSERT INTO x VALUES (1) RETURNING (SELECT a.id FROM a NATURAL JOIN b)",
        "UPDATE x SET id = 1 RETURNING (SELECT a.id FROM a NATURAL JOIN b)",
        "DELETE FROM x RETURNING (SELECT a.id FROM a NATURAL JOIN b)",
        "COPY (SELECT * FROM a NATURAL JOIN b) TO 'out.csv'",
        "MERGE INTO x USING (SELECT * FROM a NATURAL JOIN b) s ON x.id = s.id WHEN MATCHED THEN DELETE",
        "SELECT DISTINCT ON ((SELECT a.id FROM a NATURAL JOIN b)) id FROM x",
        "SELECT row_number() OVER (PARTITION BY (SELECT a.id FROM a NATURAL JOIN b)) FROM x",
        "SELECT row_number() OVER w FROM x WINDOW w AS (ORDER BY (SELECT a.id FROM a NATURAL JOIN b))",
        "SELECT id FROM x QUALIFY id IN (SELECT a.id FROM a NATURAL JOIN b)",
        "SELECT (VALUES ((SELECT a.id FROM a NATURAL JOIN b)))",
        "CREATE TABLE x (id INT DEFAULT (SELECT a.id FROM a NATURAL JOIN b))",
        "CREATE TABLE x (id INT CHECK (id IN (SELECT a.id FROM a NATURAL JOIN b)))",
        "CREATE TABLE x (id INT, CHECK (id IN (SELECT a.id FROM a NATURAL JOIN b)))",
        "ALTER TABLE x ADD COLUMN id INT DEFAULT (SELECT a.id FROM a NATURAL JOIN b)",
        "ALTER TABLE x ALTER COLUMN id SET DEFAULT (SELECT a.id FROM a NATURAL JOIN b)",
    ];
    for sql in statements {
        let parsed =
            Parser::parse_sql(&AlopexDialect, sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        // Inspect the parsed AST, not source wording: the one NATURAL join
        // must remain annotated even inside statement-specific containers.
        let ast = serde_json::to_value(&parsed).unwrap();
        assert_eq!(join_flags(&ast), vec![true], "{sql}");
    }
}

fn join_flags(value: &serde_json::Value) -> Vec<bool> {
    let mut flags = Vec::new();
    match value {
        serde_json::Value::Object(fields) => {
            if fields.get("variant").and_then(|value| value.as_str()) == Some("Join") {
                flags.push(fields["natural"].as_bool().expect("join natural flag"));
            }
            for value in fields.values() {
                flags.extend(join_flags(value));
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                flags.extend(join_flags(value));
            }
        }
        _ => {}
    }
    flags
}

#[test]
fn markers_stay_with_their_join_across_on_subqueries_and_statements() {
    let sql = "EXPLAIN SELECT * FROM a JOIN b
        ON a.id IN (SELECT c.id FROM c NATURAL JOIN d)
        NATURAL JOIN e;
        CREATE VIEW v AS SELECT * FROM f JOIN g USING (id)";
    let parsed = Parser::parse_sql(&AlopexDialect, sql).unwrap();
    let StatementKind::Explain { statement, .. } = &parsed[0].kind else {
        panic!("expected EXPLAIN");
    };
    let StatementKind::Select(select) = &statement.kind else {
        panic!("expected SELECT");
    };
    let FromItem::Join { left, natural, .. } = &select.from[0] else {
        panic!("expected outer JOIN");
    };
    assert!(*natural);
    let FromItem::Join {
        natural,
        condition: Some(condition),
        ..
    } = left.as_ref()
    else {
        panic!("expected inner JOIN with ON");
    };
    assert!(!*natural);
    let ExprKind::InSubquery { subquery, .. } = &condition.kind else {
        panic!("expected ON subquery");
    };
    assert_eq!(
        join_flags(&serde_json::to_value(subquery).unwrap()),
        vec![true]
    );
    assert_eq!(
        join_flags(&serde_json::to_value(&parsed[1]).unwrap()),
        vec![false]
    );
}
