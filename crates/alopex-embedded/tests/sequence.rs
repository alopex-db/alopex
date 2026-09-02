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
    assert_eq!(
        db.execute_sql("ALTER SEQUENCE ids RESTART WITH 20")
            .unwrap(),
        ExecutionResult::Success
    );
    assert_eq!(
        db.execute_sql("DROP SEQUENCE ids").unwrap(),
        ExecutionResult::Success
    );
    assert_eq!(
        db.execute_sql("DROP SEQUENCE IF EXISTS ids").unwrap(),
        ExecutionResult::Success
    );
}
