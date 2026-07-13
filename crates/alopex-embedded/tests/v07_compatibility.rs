use alopex_embedded::{Database, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};
use tempfile::tempdir;

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn opens_pre_cluster_disk_database_and_preserves_kv_and_sql_defaults() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("v06-default.db");

    {
        let db = Database::open(&path).expect("create v0.6-style db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        txn.put(b"compat-key", b"compat-value").expect("put kv");
        txn.commit().expect("commit kv");
        db.flush().expect("flush kv-only database");
    }

    {
        let db = Database::open(&path).expect("reopen kv-only db");
        let mut txn = db.begin(TxnMode::ReadOnly).expect("begin ro");
        assert_eq!(
            txn.get(b"compat-key").expect("get kv"),
            Some(b"compat-value".to_vec())
        );

        db.execute_sql(
            r#"
            CREATE TABLE compat_users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO compat_users (id, name) VALUES (1, 'alice');
            "#,
        )
        .expect("direct embedded SQL remains available");
        db.flush().expect("flush sql catalog and rows");
    }

    let db = Database::open(&path).expect("reopen upgraded db");
    let mut txn = db.begin(TxnMode::ReadOnly).expect("begin final ro");
    assert_eq!(
        txn.get(b"compat-key").expect("get final kv"),
        Some(b"compat-value".to_vec())
    );

    let result = db
        .execute_sql("SELECT id, name FROM compat_users ORDER BY id;")
        .expect("select direct embedded SQL");
    match result {
        ExecutionResult::Query(query) => {
            assert_eq!(query.rows.len(), 1);
            assert_eq!(query.rows[0][0], SqlValue::Integer(1));
            assert_eq!(query.rows[0][1], SqlValue::Text("alice".into()));
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn direct_embedded_sql_is_local_and_transactional_without_query_router() {
    let db = Database::new();

    {
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        txn.put(b"local-kv", b"v1").expect("put kv");
        txn.execute_sql("CREATE TABLE local_items (id INTEGER PRIMARY KEY, label TEXT);")
            .expect("create table");
        txn.execute_sql("INSERT INTO local_items (id, label) VALUES (7, 'local');")
            .expect("insert row");
        txn.commit().expect("commit mixed local txn");
    }

    let mut ro = db.begin(TxnMode::ReadOnly).expect("begin ro");
    assert_eq!(ro.get(b"local-kv").expect("get kv"), Some(b"v1".to_vec()));

    let result = ro
        .execute_sql("SELECT label FROM local_items WHERE id = 7;")
        .expect("read local SQL inside embedded transaction");
    match result {
        ExecutionResult::Query(query) => {
            assert_eq!(query.rows.len(), 1);
            assert_eq!(query.rows[0][0], SqlValue::Text("local".into()));
        }
        other => panic!("expected query result, got {other:?}"),
    }
}
