use super::*;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn run(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> ExecutionResult {
    let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    executor.execute(plan).unwrap()
}

fn exact_integer_comparison_matrix(keyed: bool) {
    // SQLite INTEGER64 independently checks this same SQL/value corpus. No
    // floating-point value participates in the expected integer ordering.
    let values: [i64; 15] = [
        i64::MIN,
        i64::MIN + 1,
        -9007199254740993,
        -9007199254740992,
        -9007199254740991,
        i32::MIN as i64,
        -1,
        0,
        1,
        i32::MAX as i64,
        9007199254740991,
        9007199254740992,
        9007199254740993,
        i64::MAX - 1,
        i64::MAX,
    ];
    let (mut executor, catalog) = create_executor();
    let table = if keyed { "keyed" } else { "plain" };
    let key = if keyed { "PRIMARY KEY" } else { "" };
    run(
        &mut executor,
        &catalog,
        &format!("CREATE TABLE {table} (id BIGINT {key})"),
    );
    let inserted = values
        .iter()
        .map(|value| format!("(CAST('{value}' AS BIGINT))"))
        .collect::<Vec<_>>()
        .join(",");
    run(
        &mut executor,
        &catalog,
        &format!("INSERT INTO {table} VALUES {inserted}"),
    );
    for op in ["=", "!=", "<", "<=", ">", ">="] {
        for bound in values {
            let expected = values
                .iter()
                .filter(|&&value| match op {
                    "=" => value == bound,
                    "!=" => value != bound,
                    "<" => value < bound,
                    "<=" => value <= bound,
                    ">" => value > bound,
                    ">=" => value >= bound,
                    _ => unreachable!(),
                })
                .map(|&value| vec![SqlValue::BigInt(value)])
                .collect::<Vec<_>>();
            let sql = format!(
                "SELECT id FROM {table} WHERE id {op} CAST('{bound}' AS BIGINT) ORDER BY id"
            );
            assert_eq!(rows(run(&mut executor, &catalog, &sql)), expected, "{sql}");
        }
    }
    // Mixed INTEGER/BIGINT still compares exactly in both operand orders.
    for (left, right) in [
        ("INTEGER", "BIGINT"),
        ("BIGINT", "INTEGER"),
        ("INTEGER", "INTEGER"),
    ] {
        for (op, expected) in [
            ("=", false),
            ("!=", true),
            ("<", true),
            ("<=", true),
            (">", false),
            (">=", false),
        ] {
            let sql = format!(
                "SELECT CAST('-2147483648' AS {left}) {op} CAST('2147483647' AS {right}) AS compared"
            );
            assert_eq!(
                rows(run(&mut executor, &catalog, &sql)),
                vec![vec![SqlValue::Boolean(expected)]],
                "{sql}"
            );
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn integer_comparisons_match_exact_reference_without_index() {
    exact_integer_comparison_matrix(false);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn integer_comparisons_match_exact_reference_with_primary_key() {
    exact_integer_comparison_matrix(true);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn mixed_float_comparisons_keep_existing_promotion() {
    let (mut executor, catalog) = create_executor();
    // This is the existing Alopex promotion contract, not SQLite REAL parity.
    for (integer_type, integer, floating) in [
        ("INTEGER", "16777217", "16777216"),
        ("BIGINT", "9007199254740993", "9007199254740992"),
    ] {
        for floating_type in ["FLOAT", "DOUBLE"] {
            for reversed in [false, true] {
                let lhs = format!("CAST('{integer}' AS {integer_type})");
                let rhs = format!("CAST('{floating}' AS {floating_type})");
                let (lhs, rhs) = if reversed { (rhs, lhs) } else { (lhs, rhs) };
                let equal = integer_type == "BIGINT";
                for (op, expected) in [
                    ("=", equal),
                    ("!=", !equal),
                    ("<", !equal && reversed),
                    ("<=", equal || reversed),
                    (">", !equal && !reversed),
                    (">=", equal || !reversed),
                ] {
                    let sql = format!("SELECT {lhs} {op} {rhs} AS compared");
                    assert_eq!(
                        rows(run(&mut executor, &catalog, &sql)),
                        vec![vec![SqlValue::Boolean(expected)]],
                        "{sql}"
                    );
                }
            }
        }
    }
}

fn rows(result: ExecutionResult) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(query) = result else {
        panic!("expected query rows, got {result:?}");
    };
    query.rows
}

fn fractional_lower_bound(boundary: &str, first: i32) {
    let (mut executor, catalog) = create_executor();
    for (table, key) in [("plain", ""), ("keyed", "PRIMARY KEY")] {
        run(
            &mut executor,
            &catalog,
            &format!("CREATE TABLE {table} (id INTEGER {key})"),
        );
        let values = (-5..=10)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(",");
        run(
            &mut executor,
            &catalog,
            &format!("INSERT INTO {table} VALUES {values}"),
        );
    }
    for predicate in [
        format!("id >= {boundary}"),
        format!("id > {boundary}"),
        format!("{boundary} <= id"),
        format!("{boundary} < id"),
    ] {
        for (limit, offset) in [(2, 0), (5, 0), (2, 1), (0, 0), (30, 0)] {
            let expected = (first..=10)
                .skip(offset)
                .take(limit)
                .map(|id| vec![SqlValue::Integer(id)])
                .collect::<Vec<_>>();
            for table in ["plain", "keyed"] {
                let sql = format!(
                    "SELECT id FROM {table} WHERE {predicate} ORDER BY id LIMIT {limit} OFFSET {offset}"
                );
                assert_eq!(rows(run(&mut executor, &catalog, &sql)), expected, "{sql}");
            }
        }
    }
    // Exact INTEGER bounds remain valid index shortcuts, including strictness.
    for (predicate, expected) in [("id >= 2", vec![2, 3]), ("id > 2", vec![3, 4])] {
        let sql = format!("SELECT id FROM keyed WHERE {predicate} ORDER BY id LIMIT 2");
        assert_eq!(
            rows(run(&mut executor, &catalog, &sql)),
            expected
                .into_iter()
                .map(|id| vec![SqlValue::Integer(id)])
                .collect::<Vec<_>>()
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn positive_fractional_ordered_limit_preserves_scan_rows() {
    fractional_lower_bound("2.5", 3);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn negative_fractional_ordered_limit_preserves_scan_rows() {
    fractional_lower_bound("CAST('-2.5' AS DOUBLE)", -2);
}

fn bigint_fixture() -> (
    Executor<MemoryKV, MemoryCatalog>,
    Arc<RwLock<MemoryCatalog>>,
) {
    let (mut executor, catalog) = create_executor();
    for (table, key) in [("plain", ""), ("keyed", "PRIMARY KEY")] {
        run(
            &mut executor,
            &catalog,
            &format!("CREATE TABLE {table} (id BIGINT {key}, touched INTEGER)"),
        );
        run(
            &mut executor,
            &catalog,
            &format!(
                "INSERT INTO {table} VALUES (9007199254740992, 0), (9007199254740993, 0), (9007199254740994, 0)"
            ),
        );
    }
    (executor, catalog)
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bigint_double_equality_preserves_scan_rows() {
    let (mut executor, catalog) = bigint_fixture();
    for predicate in [
        "id = CAST('9007199254740993' AS DOUBLE)",
        "CAST('9007199254740993' AS DOUBLE) = id",
    ] {
        for table in ["plain", "keyed"] {
            let sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id");
            assert_eq!(
                rows(run(&mut executor, &catalog, &sql)),
                vec![
                    vec![SqlValue::BigInt(9007199254740992)],
                    vec![SqlValue::BigInt(9007199254740993)]
                ],
                "{sql}"
            );
        }
    }
    for table in ["plain", "keyed"] {
        assert_eq!(
            rows(run(
                &mut executor,
                &catalog,
                &format!("SELECT id FROM {table} WHERE id = 9007199254740993 ORDER BY id")
            )),
            vec![vec![SqlValue::BigInt(9007199254740993)]]
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bigint_same_type_ordered_limit_preserves_scan_rows() {
    let (mut executor, catalog) = bigint_fixture();
    for (predicate, expected) in [
        (
            "id >= 9007199254740993",
            vec![9007199254740993, 9007199254740994],
        ),
        (
            "id > 9007199254740992",
            vec![9007199254740993, 9007199254740994],
        ),
        (
            "9007199254740993 <= id",
            vec![9007199254740993, 9007199254740994],
        ),
    ] {
        for table in ["plain", "keyed"] {
            let sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id LIMIT 2");
            assert_eq!(
                rows(run(&mut executor, &catalog, &sql)),
                expected
                    .iter()
                    .map(|id| vec![SqlValue::BigInt(*id)])
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bigint_negative_same_type_boundaries_preserve_scan_rows() {
    let (mut executor, catalog) = create_executor();
    for (table, key) in [("plain", ""), ("keyed", "PRIMARY KEY")] {
        run(
            &mut executor,
            &catalog,
            &format!("CREATE TABLE {table} (id BIGINT {key})"),
        );
        run(
            &mut executor,
            &catalog,
            &format!(
                "INSERT INTO {table} VALUES (-9007199254740994), (-9007199254740993), (-9007199254740992)"
            ),
        );
    }
    for (predicate, expected) in [
        (
            "id = CAST('-9007199254740993' AS BIGINT)",
            vec![-9007199254740993],
        ),
        (
            "id >= CAST('-9007199254740992' AS BIGINT)",
            vec![-9007199254740992],
        ),
        (
            "id > CAST('-9007199254740993' AS BIGINT)",
            vec![-9007199254740992],
        ),
    ] {
        for table in ["plain", "keyed"] {
            let sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id LIMIT 2");
            assert_eq!(
                rows(run(&mut executor, &catalog, &sql)),
                expected
                    .iter()
                    .map(|id| vec![SqlValue::BigInt(*id)])
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bigint_same_type_update_preserves_scan_targets() {
    let (mut executor, catalog) = bigint_fixture();
    for table in ["plain", "keyed"] {
        let sql = format!("UPDATE {table} SET touched = 1 WHERE id = 9007199254740993");
        assert_eq!(
            run(&mut executor, &catalog, &sql),
            ExecutionResult::RowsAffected(1),
            "{sql}"
        );
        assert_eq!(
            rows(run(
                &mut executor,
                &catalog,
                &format!("SELECT id FROM {table} WHERE touched = 1 ORDER BY id")
            )),
            vec![vec![SqlValue::BigInt(9007199254740993)]]
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bigint_double_update_preserves_scan_targets() {
    let (mut executor, catalog) = bigint_fixture();
    for table in ["plain", "keyed"] {
        let sql =
            format!("UPDATE {table} SET touched = 1 WHERE id = CAST('9007199254740993' AS DOUBLE)");
        assert_eq!(
            run(&mut executor, &catalog, &sql),
            ExecutionResult::RowsAffected(2),
            "{sql}"
        );
        assert_eq!(
            rows(run(
                &mut executor,
                &catalog,
                &format!("SELECT id, touched FROM {table} ORDER BY id")
            )),
            vec![
                vec![SqlValue::BigInt(9007199254740992), SqlValue::Integer(1)],
                vec![SqlValue::BigInt(9007199254740993), SqlValue::Integer(1)],
                vec![SqlValue::BigInt(9007199254740994), SqlValue::Integer(0)]
            ]
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bigint_double_delete_preserves_scan_targets() {
    let (mut executor, catalog) = bigint_fixture();
    for table in ["plain", "keyed"] {
        let sql = format!("DELETE FROM {table} WHERE id = CAST('9007199254740993' AS DOUBLE)");
        assert_eq!(
            run(&mut executor, &catalog, &sql),
            ExecutionResult::RowsAffected(2),
            "{sql}"
        );
        assert_eq!(
            rows(run(
                &mut executor,
                &catalog,
                &format!("SELECT id FROM {table} ORDER BY id")
            )),
            vec![vec![SqlValue::BigInt(9007199254740994)]]
        );
    }
}
