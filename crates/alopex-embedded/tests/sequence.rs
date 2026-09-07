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

#[test]
fn currval_bounds_cycle_and_partial_alter_are_transactional() {
    let db = Database::new();
    db.execute_sql("CREATE SEQUENCE bounded START WITH 2 MINVALUE 2 MAXVALUE 3 CYCLE")
        .unwrap();
    let metadata = db.list_sequences().unwrap();
    assert_eq!(metadata[0].name, "bounded");
    assert_eq!(metadata[0].start_value, 2);
    assert!(metadata[0].cycle);
    assert!(db.execute_sql("SELECT currval('bounded')").is_err());

    let ExecutionResult::Query(first) = db.execute_sql("SELECT nextval('bounded')").unwrap() else {
        panic!("expected nextval query result")
    };
    assert_eq!(first.rows[0][0], alopex_sql::SqlValue::BigInt(2));
    let ExecutionResult::Query(current) = db.execute_sql("SELECT currval('bounded')").unwrap()
    else {
        panic!("expected currval query result")
    };
    assert_eq!(current.rows[0][0], alopex_sql::SqlValue::BigInt(2));

    db.execute_sql("ALTER SEQUENCE bounded INCREMENT BY 1")
        .unwrap();
    let ExecutionResult::Query(second) = db.execute_sql("SELECT nextval('bounded')").unwrap()
    else {
        panic!("expected nextval query result")
    };
    assert_eq!(second.rows[0][0], alopex_sql::SqlValue::BigInt(3));
    let ExecutionResult::Query(wrapped) = db.execute_sql("SELECT nextval('bounded')").unwrap()
    else {
        panic!("expected wrapped nextval query result")
    };
    assert_eq!(wrapped.rows[0][0], alopex_sql::SqlValue::BigInt(2));
}

#[test]
fn generated_sequence_is_dropped_with_table_and_composes_with_returning_and_conflict() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE generated (id SERIAL PRIMARY KEY, label TEXT)")
        .unwrap();
    let ExecutionResult::Query(inserted) = db
        .execute_sql("INSERT INTO generated (label) VALUES ('first') RETURNING id")
        .unwrap()
    else {
        panic!("expected RETURNING rows")
    };
    assert_eq!(inserted.rows[0][0], alopex_sql::SqlValue::Integer(1));
    let ExecutionResult::Query(skipped) = db
        .execute_sql(
            "INSERT INTO generated (id, label) VALUES (1, 'duplicate') \
             ON CONFLICT DO NOTHING RETURNING id",
        )
        .unwrap()
    else {
        panic!("expected RETURNING rows")
    };
    assert!(skipped.rows.is_empty());

    db.execute_sql("DROP TABLE generated").unwrap();
    assert!(db.list_sequences().unwrap().is_empty());
    db.execute_sql("CREATE TABLE generated (id SERIAL PRIMARY KEY, label TEXT)")
        .unwrap();
    let ExecutionResult::Query(recreated) = db
        .execute_sql("INSERT INTO generated (label) VALUES ('second') RETURNING id")
        .unwrap()
    else {
        panic!("expected RETURNING rows")
    };
    assert_eq!(recreated.rows[0][0], alopex_sql::SqlValue::Integer(1));
}

#[test]
fn sequence_state_survives_reopen_and_rollback_does_not_consume_value() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sequence.alopex");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql("CREATE SEQUENCE durable START WITH 7 CACHE 4")
            .unwrap();
        let mut transaction = db.begin(alopex_embedded::TxnMode::ReadWrite).unwrap();
        transaction
            .execute_sql("SELECT nextval('durable')")
            .unwrap();
        transaction.rollback().unwrap();
        db.close().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        let ExecutionResult::Query(result) = db.execute_sql("SELECT nextval('durable')").unwrap()
        else {
            panic!("expected nextval rows")
        };
        assert_eq!(result.rows[0][0], alopex_sql::SqlValue::BigInt(7));
        db.close().unwrap();
    }
}

#[test]
fn sequence_state_and_ownership_survive_single_file_backup_restore() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("sequence-source.alopex");
    let restored = directory.path().join("sequence-restored.alopex");
    {
        let db = Database::open(&source).unwrap();
        db.execute_sql("CREATE TABLE owned (id SERIAL PRIMARY KEY, label TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO owned (label) VALUES ('first')")
            .unwrap();
        db.close().unwrap();
    }

    std::fs::copy(&source, &restored).unwrap();
    let db = Database::open(&restored).unwrap();
    let sequence = db.list_sequences().unwrap().remove(0);
    assert_eq!(sequence.name, "__alopex_auto__owned__id");
    assert_eq!(sequence.owned_by.as_deref(), Some("owned.id"));
    let ExecutionResult::Query(current) = db
        .execute_sql("SELECT currval('__alopex_auto__owned__id')")
        .unwrap()
    else {
        panic!("expected restored currval rows")
    };
    assert_eq!(current.rows[0][0], alopex_sql::SqlValue::BigInt(1));
    db.execute_sql("INSERT INTO owned (label) VALUES ('second')")
        .unwrap();
    let ExecutionResult::Query(rows) = db.execute_sql("SELECT id FROM owned ORDER BY id").unwrap()
    else {
        panic!("expected restored rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![alopex_sql::SqlValue::Integer(1)],
            vec![alopex_sql::SqlValue::Integer(2)],
        ]
    );
}

#[test]
fn concurrent_sequence_allocations_conflict_instead_of_committing_duplicates() {
    let db = Database::new();
    db.execute_sql("CREATE SEQUENCE ids").unwrap();
    let mut first = db.begin(alopex_embedded::TxnMode::ReadWrite).unwrap();
    let mut second = db.begin(alopex_embedded::TxnMode::ReadWrite).unwrap();
    first.execute_sql("SELECT nextval('ids')").unwrap();
    second.execute_sql("SELECT nextval('ids')").unwrap();
    first.commit().unwrap();
    assert!(second.commit().is_err());

    let ExecutionResult::Query(next) = db.execute_sql("SELECT nextval('ids')").unwrap() else {
        panic!("expected nextval rows")
    };
    assert_eq!(next.rows[0][0], alopex_sql::SqlValue::BigInt(2));
}
