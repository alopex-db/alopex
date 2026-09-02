use alopex_embedded::Database;
use alopex_sql::ExecutionResult;

#[test]
fn sequence_ddl_is_transactional_and_persistent() {
    let db = Database::new();
    assert_eq!(
        db.execute_sql("CREATE SEQUENCE ids START WITH 10 INCREMENT BY 2")
            .unwrap(),
        ExecutionResult::Success
    );
    assert!(db.execute_sql("CREATE SEQUENCE ids").is_err());
    let ExecutionResult::Query(first) = db.execute_sql("SELECT nextval('ids')").unwrap() else {
        panic!("expected nextval query result")
    };
    assert_eq!(first.rows[0][0], alopex_sql::SqlValue::BigInt(10));
    assert_eq!(
        db.execute_sql("ALTER SEQUENCE ids RESTART WITH 20")
            .unwrap(),
        ExecutionResult::Success
    );
    let ExecutionResult::Query(restarted) = db.execute_sql("SELECT nextval('ids')").unwrap() else {
        panic!("expected restarted nextval query result")
    };
    assert_eq!(restarted.rows[0][0], alopex_sql::SqlValue::BigInt(20));
    assert_eq!(
        db.execute_sql("DROP SEQUENCE ids").unwrap(),
        ExecutionResult::Success
    );
    assert_eq!(
        db.execute_sql("DROP SEQUENCE IF EXISTS ids").unwrap(),
        ExecutionResult::Success
    );
}

#[test]
fn serial_and_identity_columns_generate_values_when_omitted() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE serials (id SERIAL PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO serials (label) VALUES ('a'), ('b')")
        .unwrap();
    let ExecutionResult::Query(serials) = db
        .execute_sql("SELECT id, label FROM serials ORDER BY id")
        .unwrap()
    else {
        panic!("expected serial query result")
    };
    assert_eq!(serials.rows[0][0], alopex_sql::SqlValue::Integer(1));
    assert_eq!(serials.rows[1][0], alopex_sql::SqlValue::Integer(2));

    db.execute_sql(
        "CREATE TABLE identities (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label TEXT)",
    )
    .unwrap();
    db.execute_sql("INSERT INTO identities (label) VALUES ('x'), ('y')")
        .unwrap();
    let ExecutionResult::Query(identities) = db
        .execute_sql("SELECT id, label FROM identities ORDER BY id")
        .unwrap()
    else {
        panic!("expected identity query result")
    };
    assert_eq!(identities.rows[0][0], alopex_sql::SqlValue::BigInt(1));
    assert_eq!(identities.rows[1][0], alopex_sql::SqlValue::BigInt(2));
}
