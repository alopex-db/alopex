use std::collections::HashMap;

use alopex_sql::storage::SqlValue;

use super::TestHarness;

fn seed_products(harness: &mut TestHarness) {
    harness.execute_sql(
        r#"
        CREATE TABLE products (
            category TEXT,
            region TEXT,
            sales INT
        );
        INSERT INTO products (category, region, sales) VALUES
            ('book', 'us', 10),
            ('book', 'us', 15),
            ('book', 'eu', 5),
            ('game', 'us', 20),
            ('game', 'eu', 7),
            ('toy', 'jp', 3);
        "#,
    );
}

#[test]
fn group_by_single_column_count() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness.query_sql("SELECT category, COUNT(*) FROM products GROUP BY category");
    let mut counts = HashMap::new();
    for row in result.rows {
        let SqlValue::Text(category) = &row[0] else {
            panic!("expected text category");
        };
        let SqlValue::BigInt(count) = row[1] else {
            panic!("expected bigint count");
        };
        counts.insert(category.clone(), count);
    }

    assert_eq!(counts.get("book"), Some(&3));
    assert_eq!(counts.get("game"), Some(&2));
    assert_eq!(counts.get("toy"), Some(&1));
}

#[test]
fn group_by_without_aggregates() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result =
        harness.query_sql("SELECT category FROM products GROUP BY category ORDER BY category ASC");
    let categories: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Text(value) => value.clone(),
            other => panic!("unexpected category {other:?}"),
        })
        .collect();
    assert_eq!(
        categories,
        vec!["book".to_string(), "game".to_string(), "toy".to_string()]
    );
}

#[test]
fn group_by_multi_column_sum() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness
        .query_sql("SELECT category, region, SUM(sales) FROM products GROUP BY category, region");
    let mut sums = HashMap::new();
    for row in result.rows {
        let SqlValue::Text(category) = &row[0] else {
            panic!("expected text category");
        };
        let SqlValue::Text(region) = &row[1] else {
            panic!("expected text region");
        };
        let SqlValue::Double(sum) = row[2] else {
            panic!("expected double sum");
        };
        sums.insert((category.clone(), region.clone()), sum);
    }

    assert_eq!(sums.get(&("book".into(), "us".into())), Some(&25.0));
    assert_eq!(sums.get(&("book".into(), "eu".into())), Some(&5.0));
    assert_eq!(sums.get(&("game".into(), "us".into())), Some(&20.0));
    assert_eq!(sums.get(&("game".into(), "eu".into())), Some(&7.0));
    assert_eq!(sums.get(&("toy".into(), "jp".into())), Some(&3.0));
}

#[test]
fn group_by_having_filters_groups() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness
        .query_sql("SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 1");
    let categories: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Text(value) => value.clone(),
            other => panic!("unexpected category {other:?}"),
        })
        .collect();
    assert_eq!(categories.len(), 2);
    assert!(categories.contains(&"book".to_string()));
    assert!(categories.contains(&"game".to_string()));
}

#[test]
fn group_by_having_complex_predicates_and_null() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        r#"
        CREATE TABLE orders (category TEXT, sales INT);
        INSERT INTO orders (category, sales) VALUES
            ('book', 10),
            ('book', 20),
            ('game', NULL),
            ('game', NULL),
            ('toy', 3);
        "#,
    );

    let result = harness.query_sql(
        "SELECT category FROM orders GROUP BY category HAVING COUNT(*) > 1 AND SUM(sales) > 15 ORDER BY category",
    );
    let categories: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Text(value) => value.clone(),
            other => panic!("unexpected category {other:?}"),
        })
        .collect();
    assert_eq!(categories, vec!["book".to_string()]);

    let result = harness.query_sql(
        "SELECT category FROM orders GROUP BY category HAVING COUNT(*) > 2 OR SUM(sales) > 15 ORDER BY category",
    );
    let categories: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Text(value) => value.clone(),
            other => panic!("unexpected category {other:?}"),
        })
        .collect();
    assert_eq!(categories, vec!["book".to_string()]);
}

#[test]
fn group_by_order_by_aggregate() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness.query_sql(
        "SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY COUNT(*) DESC",
    );
    let categories: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Text(value) => value.clone(),
            other => panic!("unexpected category {other:?}"),
        })
        .collect();
    assert_eq!(
        categories,
        vec!["book".to_string(), "game".to_string(), "toy".to_string()]
    );
}

#[test]
fn group_by_order_by_group_key() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness.query_sql(
        "SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY category ASC",
    );
    let categories: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Text(value) => value.clone(),
            other => panic!("unexpected category {other:?}"),
        })
        .collect();
    assert_eq!(
        categories,
        vec!["book".to_string(), "game".to_string(), "toy".to_string()]
    );
}

#[test]
fn select_distinct_removes_duplicates() {
    let mut harness = TestHarness::new();
    seed_products(&mut harness);

    let result = harness.query_sql("SELECT DISTINCT category FROM products ORDER BY category ASC");
    let categories: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Text(value) => value.clone(),
            other => panic!("unexpected category {other:?}"),
        })
        .collect();
    assert_eq!(
        categories,
        vec!["book".to_string(), "game".to_string(), "toy".to_string()]
    );
}
