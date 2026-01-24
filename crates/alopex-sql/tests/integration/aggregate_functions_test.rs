use alopex_sql::storage::SqlValue;

use super::TestHarness;

fn seed_products(harness: &mut TestHarness) {
    harness.execute_sql(
        r#"
        CREATE TABLE products (
            category TEXT,
            price DOUBLE,
            label TEXT
        );
        INSERT INTO products (category, price, label) VALUES
            ('book', 10.0, 'a'),
            ('book', 15.0, 'b'),
            ('book', NULL, 'a'),
            ('game', 20.0, 'c'),
            ('game', NULL, NULL),
            ('toy', 3.0, 'c');
        "#,
    );
}

#[test]
fn aggregates_without_group_by() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness.query_sql("SELECT COUNT(*), AVG(price), MAX(price) FROM products");
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    match &row[0] {
        SqlValue::BigInt(value) => assert_eq!(*value, 6),
        other => panic!("unexpected count {other:?}"),
    }
    match &row[1] {
        SqlValue::Double(value) => assert!((*value - 12.0).abs() < 1e-6),
        other => panic!("unexpected avg {other:?}"),
    }
    match &row[2] {
        SqlValue::Double(value) => assert!((*value - 20.0).abs() < 1e-6),
        other => panic!("unexpected max {other:?}"),
    }
}

#[test]
fn aggregates_with_group_by() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness.query_sql(
        "SELECT category, COUNT(*), COUNT(price), COUNT(DISTINCT label), SUM(price), AVG(price), MIN(price), MAX(price) FROM products GROUP BY category",
    );
    for row in result.rows {
        let SqlValue::Text(category) = &row[0] else {
            panic!("expected text category");
        };
        match category.as_str() {
            "book" => {
                assert_eq!(row[1], SqlValue::BigInt(3));
                assert_eq!(row[2], SqlValue::BigInt(2));
                assert_eq!(row[3], SqlValue::BigInt(2));
                assert_eq!(row[4], SqlValue::Double(25.0));
                match &row[5] {
                    SqlValue::Double(value) => assert!((*value - 12.5).abs() < 1e-6),
                    other => panic!("unexpected avg {other:?}"),
                }
                assert_eq!(row[6], SqlValue::Double(10.0));
                assert_eq!(row[7], SqlValue::Double(15.0));
            }
            "game" => {
                assert_eq!(row[1], SqlValue::BigInt(2));
                assert_eq!(row[2], SqlValue::BigInt(1));
                assert_eq!(row[3], SqlValue::BigInt(1));
                assert_eq!(row[4], SqlValue::Double(20.0));
                assert_eq!(row[6], SqlValue::Double(20.0));
                assert_eq!(row[7], SqlValue::Double(20.0));
            }
            "toy" => {
                assert_eq!(row[1], SqlValue::BigInt(1));
                assert_eq!(row[2], SqlValue::BigInt(1));
                assert_eq!(row[3], SqlValue::BigInt(1));
                assert_eq!(row[4], SqlValue::Double(3.0));
                assert_eq!(row[6], SqlValue::Double(3.0));
                assert_eq!(row[7], SqlValue::Double(3.0));
            }
            other => panic!("unexpected category {other}"),
        }
    }
}

#[test]
fn group_concat_with_separator() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness
        .query_sql("SELECT category, GROUP_CONCAT(label, '|') FROM products GROUP BY category");
    for row in result.rows {
        let SqlValue::Text(category) = &row[0] else {
            panic!("expected text category");
        };
        match category.as_str() {
            "book" => {
                assert_eq!(row[1], SqlValue::Text("a|b|a".into()));
            }
            "game" => {
                assert_eq!(row[1], SqlValue::Text("c".into()));
            }
            "toy" => {
                assert_eq!(row[1], SqlValue::Text("c".into()));
            }
            other => panic!("unexpected category {other}"),
        }
    }
}
