use alopex_dataframe::DataFrameError;
use alopex_embedded::{Database, Error, JoinType};
use arrow::array::{
    Array, Date32Array, Decimal128Array, Float32Array, Int32Array, IntervalMonthDayNanoArray,
    StringArray, Time64MicrosecondArray,
};

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_preserves_float_column_as_float32() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE metrics (id INTEGER PRIMARY KEY, value FLOAT); \
         INSERT INTO metrics VALUES (1, 1.5), (2, 2.5);",
    )
    .unwrap();

    let frame = db
        .query_df("SELECT id, value FROM metrics ORDER BY id")
        .unwrap();
    let values = frame.column("value").unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(values.values(), &[1.5, 2.5]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_preserves_native_temporal_types() {
    let db = Database::new();
    let frame = db
        .query_df(
            "SELECT DATE '2024-02-29' AS d, TIME '23:59:59.123456' AS t, \
                    INTERVAL '1 month -2 days 3 microseconds' AS i",
        )
        .unwrap();

    let dates = frame.column("d").unwrap().to_arrow();
    let dates = dates[0].as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(dates.value(0), 19_782);

    let times = frame.column("t").unwrap().to_arrow();
    let times = times[0]
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    assert_eq!(times.value(0), 86_399_123_456);

    let intervals = frame.column("i").unwrap().to_arrow();
    let intervals = intervals[0]
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .unwrap();
    let interval = intervals.value(0);
    assert_eq!(
        (interval.months, interval.days, interval.nanoseconds),
        (1, -2, 3_000)
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_preserves_decimal128() {
    let db = Database::new();
    let frame = db
        .query_df("SELECT CAST('12.34' AS DECIMAL(10,2)) AS amount")
        .unwrap();
    let amounts = frame.column("amount").unwrap().to_arrow();
    let amounts = amounts[0]
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.value(0), 1234);
    assert_eq!((amounts.precision(), amounts.scale()), (10, 2));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_maps_native_json_to_canonical_utf8() {
    let db = Database::new();
    let frame = db
        .query_df(r#"SELECT JSONB '{"b":1,"a":2}' AS document"#)
        .unwrap();
    let documents = frame.column("document").unwrap().to_arrow();
    let documents = documents[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(documents.value(0), r#"{"a":2,"b":1}"#);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_maps_nested_values_to_canonical_utf8() {
    let db = Database::new();
    let frame = db
        .query_df("SELECT ARRAY[1, NULL] AS items, MAP(ARRAY['a'], ARRAY[1]) AS attrs")
        .unwrap();

    let items = frame.column("items").unwrap().to_arrow();
    let items = items[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(items.value(0), "[1,null]");

    let attrs = frame.column("attrs").unwrap().to_arrow();
    let attrs = attrs[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(attrs.value(0), r#"{"a":1}"#);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_join_sort_supported_types() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE left_t (id INTEGER, name TEXT, score DOUBLE);
        CREATE TABLE right_t (id INTEGER, value INTEGER);
        INSERT INTO left_t (id, name, score) VALUES
            (1, 'alpha', 10.0),
            (2, 'beta', 20.0),
            (3, 'gamma', 30.0);
        INSERT INTO right_t (id, value) VALUES
            (1, 100),
            (3, 300);
        "#,
    )
    .unwrap();

    let left = db
        .query_df("SELECT id, name, score FROM left_t ORDER BY id;")
        .unwrap();
    let right = db
        .query_df("SELECT id, value FROM right_t ORDER BY id;")
        .unwrap();

    let joined = left
        .join(&right, vec!["id".to_string()], JoinType::Left)
        .unwrap();
    let sorted = joined.sort(vec!["id".to_string()], vec![true]).unwrap();

    assert_eq!(sorted.height(), 3);

    let id = sorted.column("id").unwrap().to_arrow();
    let id = id[0].as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(id.value(0), 3);
    assert_eq!(id.value(1), 2);
    assert_eq!(id.value(2), 1);

    let name = sorted.column("name").unwrap().to_arrow();
    let name = name[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(0), "gamma");
    assert_eq!(name.value(1), "beta");
    assert_eq!(name.value(2), "alpha");

    let value = sorted.column("value").unwrap().to_arrow();
    let value = value[0].as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(value.value(0), 300);
    assert!(value.is_null(1));
    assert_eq!(value.value(2), 100);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_join_variants() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE left_t (id INTEGER, value INTEGER);
        CREATE TABLE right_t (id INTEGER, value INTEGER);
        INSERT INTO left_t (id, value) VALUES (1, 10), (2, 20), (3, 30);
        INSERT INTO right_t (id, value) VALUES (2, 200), (3, 300), (4, 400);
        "#,
    )
    .unwrap();

    let left = db
        .query_df("SELECT id, value FROM left_t ORDER BY id;")
        .unwrap();
    let right = db
        .query_df("SELECT id, value FROM right_t ORDER BY id;")
        .unwrap();

    let inner = left
        .join(&right, vec!["id".to_string()], JoinType::Inner)
        .unwrap();
    let inner_names: Vec<String> = inner
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(
        inner_names,
        vec![
            "id".to_string(),
            "value".to_string(),
            "value_right".to_string()
        ]
    );
    assert_eq!(inner.height(), 2);

    let left_join = left
        .join(&right, vec!["id".to_string()], JoinType::Left)
        .unwrap();
    assert_eq!(left_join.height(), 3);

    let right_join = left
        .join(&right, vec!["id".to_string()], JoinType::Right)
        .unwrap();
    assert_eq!(right_join.height(), 3);

    let full = left
        .join(&right, vec!["id".to_string()], JoinType::Full)
        .unwrap();
    assert_eq!(full.height(), 4);

    let semi = left
        .join(&right, vec!["id".to_string()], JoinType::Semi)
        .unwrap();
    let semi_names: Vec<String> = semi
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(semi_names, vec!["id".to_string(), "value".to_string()]);
    assert_eq!(semi.height(), 2);

    let anti = left
        .join(&right, vec!["id".to_string()], JoinType::Anti)
        .unwrap();
    let anti_names: Vec<String> = anti
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(anti_names, vec!["id".to_string(), "value".to_string()]);
    assert_eq!(anti.height(), 1);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_query_rejects_vector_columns() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2)); \
         INSERT INTO items (id, embedding) VALUES (1, [0.0, 1.0]);",
    )
    .unwrap();

    let err = db
        .query_df("SELECT id, embedding FROM items ORDER BY id;")
        .unwrap_err();
    assert!(matches!(err, Error::DataFrame(_)));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_sort_rejects_blob_columns() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE blobs (id INTEGER, data BLOB); \
         INSERT INTO blobs (id, data) VALUES (1, NULL), (2, NULL);",
    )
    .unwrap();

    let df = db
        .query_df("SELECT id, data FROM blobs ORDER BY id;")
        .unwrap();
    let err = df.sort(vec!["data".to_string()], vec![false]).unwrap_err();
    assert!(matches!(err, DataFrameError::InvalidOperation { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_sort_multi_column_stable_nulls_last() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE sort_t (id INTEGER, category TEXT, score INTEGER, tag TEXT);
        INSERT INTO sort_t (id, category, score, tag) VALUES
            (1, 'a', 2, 'x'),
            (2, 'a', 2, 'y'),
            (3, 'a', NULL, 'z'),
            (4, 'b', 1, 'b1'),
            (5, 'b', 3, 'b3');
        "#,
    )
    .unwrap();

    let df = db
        .query_df("SELECT id, category, score, tag FROM sort_t ORDER BY id;")
        .unwrap();
    let sorted = df
        .sort(
            vec!["category".to_string(), "score".to_string()],
            vec![false, true],
        )
        .unwrap();

    let ids = sorted.column("id").unwrap().to_arrow();
    let ids = ids[0].as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
    assert_eq!(ids.value(3), 5);
    assert_eq!(ids.value(4), 4);

    let tags = sorted.column("tag").unwrap().to_arrow();
    let tags = tags[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(tags.value(0), "x");
    assert_eq!(tags.value(1), "y");

    let scores = sorted.column("score").unwrap().to_arrow();
    let scores = scores[0].as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(scores.is_null(2));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn dataframe_join_validation_errors() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE left_t (id INTEGER, value INTEGER);
        CREATE TABLE right_t (id INTEGER, value INTEGER);
        INSERT INTO left_t (id, value) VALUES (1, 10), (2, 20);
        INSERT INTO right_t (id, value) VALUES (2, 200), (3, 300);
        "#,
    )
    .unwrap();

    let left = db
        .query_df("SELECT id, value FROM left_t ORDER BY id;")
        .unwrap();
    let right = db
        .query_df("SELECT id, value FROM right_t ORDER BY id;")
        .unwrap();

    let err = left
        .join(
            &right,
            (
                vec!["id".to_string()],
                vec!["id".to_string(), "value".to_string()],
            ),
            JoinType::Inner,
        )
        .unwrap_err();
    assert!(matches!(err, DataFrameError::InvalidOperation { .. }));

    let err = left
        .join(&right, vec!["missing".to_string()], JoinType::Inner)
        .unwrap_err();
    assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
}
