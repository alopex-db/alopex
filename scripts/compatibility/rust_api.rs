//! Same old-client code compiled against exact 0.8.15 and candidate dependencies.
//! Local regressions derived from acceptance_embedded/catalog_integration and
//! prepared_statement tests, pinned by api-cases.json/provenance.json.
use alopex_embedded::{
    ColumnDefinition, CreateCatalogRequest, CreateNamespaceRequest, CreateTableRequest, Database,
    EmbeddedConfig, SqlSessionState, SqlStreamingResult, TxnMode,
};
use alopex_sql::{ast::ddl::DataType, ExecutionResult, SqlValue};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    match db.execute_sql(sql).unwrap() {
        ExecutionResult::Query(result) => result.rows,
        other => panic!("expected rows: {other:?}"),
    }
}

fn record(
    root: &Path,
    label: &str,
    results: &mut BTreeMap<String, Value>,
    name: &str,
    value: Value,
) {
    results.insert(name.into(), value);
    fs::write(
        root.join(format!("{label}-result.json")),
        serde_json::to_vec_pretty(&json!({"status":"running","cases":results})).unwrap(),
    )
    .unwrap();
}

pub fn run(root: &Path, label: &str) {
    let mut results = BTreeMap::new();
    {
        let db = Database::open_in_memory().unwrap();
        assert!(db.memory_usage().is_some());
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        assert_eq!(tx.get(b"missing").unwrap(), None);
        tx.put(b"kv:\0", b"\0\xff").unwrap();
        tx.put(b"kv:empty", b"").unwrap();
        tx.commit().unwrap();
        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(b"kv:\0").unwrap(), Some(b"\0\xff".to_vec()));
        assert!(ro.put(b"blocked", b"v").is_err());
        drop(ro);
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.delete(b"kv:\0").unwrap();
        tx.put(b"rollback", b"temporary").unwrap();
        tx.rollback().unwrap();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        assert_eq!(tx.get(b"rollback").unwrap(), None);
        tx.delete(b"kv:\0").unwrap();
        tx.commit().unwrap();
        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(b"kv:\0").unwrap(), None);
        assert_eq!(ro.get(b"kv:empty").unwrap(), Some(vec![]));
        record(
            root,
            label,
            &mut results,
            "kv_crud_transaction_modes",
            json!({"pass":true}),
        );
    }
    {
        let db = Database::new();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        for key in [b"scan:b".as_slice(), b"scan:a", b"other"] {
            tx.put(key, key).unwrap();
        }
        let entries: Vec<_> = tx.scan_prefix(b"scan:").unwrap().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, b"scan:a");
        assert_eq!(entries[1].0, b"scan:b");
        tx.rollback().unwrap();
        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.scan_prefix(b"scan:").unwrap().count(), 0);
        record(
            root,
            label,
            &mut results,
            "kv_scan_pending_and_rollback",
            json!({"pass":true}),
        );
    }
    {
        let db = Database::new();
        // Match the historical catalog_integration fixture's explicit hierarchy.
        db.create_catalog(CreateCatalogRequest::new("default"))
            .unwrap();
        db.create_namespace(CreateNamespaceRequest::new("default", "default"))
            .unwrap();
        assert!(matches!(
            db.execute_sql("CREATE TABLE api_rows (id INTEGER PRIMARY KEY, label TEXT)")
                .unwrap(),
            ExecutionResult::Success
        ));
        db.execute_sql("INSERT INTO api_rows VALUES (1,'first'),(2,NULL)")
            .unwrap();
        db.execute_sql("UPDATE api_rows SET label='updated' WHERE id=1")
            .unwrap();
        assert_eq!(
            query(&db, "SELECT id,label FROM api_rows ORDER BY id"),
            vec![
                vec![SqlValue::Integer(1), SqlValue::Text("updated".into())],
                vec![SqlValue::Integer(2), SqlValue::Null]
            ]
        );
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.execute_sql("DELETE FROM api_rows WHERE id=2").unwrap();
        tx.rollback().unwrap();
        assert_eq!(query(&db, "SELECT id FROM api_rows ORDER BY id").len(), 2);
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.execute_sql("DELETE FROM api_rows WHERE id=2").unwrap();
        tx.commit().unwrap();
        assert_eq!(
            query(&db, "SELECT id FROM api_rows"),
            vec![vec![SqlValue::Integer(1)]]
        );
        assert!(db
            .execute_sql("INSERT INTO api_rows VALUES (1,'duplicate')")
            .is_err());
        assert!(db.execute_sql("SELECT * FROM absent_table").is_err());
        assert_eq!(
            db.get_table_info_simple("api_rows").unwrap().columns.len(),
            2
        );
        record(
            root,
            label,
            &mut results,
            "sql_crud_constraints_metadata",
            json!({"pass":true}),
        );
    }
    {
        let db = Arc::new(Database::new());
        db.execute_sql("CREATE TABLE bound_rows (id INTEGER PRIMARY KEY, label TEXT)")
            .unwrap();
        let mut statement = db.prepare("INSERT INTO bound_rows VALUES (?,?)").unwrap();
        assert_eq!(statement.parameter_count(), 2);
        statement.bind(1, SqlValue::Integer(1)).unwrap();
        statement
            .bind(2, SqlValue::Text("日本語,'quote'\nnext".into()))
            .unwrap();
        statement.execute().unwrap();
        statement.reset().unwrap();
        statement.bind(1, SqlValue::Integer(2)).unwrap();
        statement.bind(2, SqlValue::Null).unwrap();
        statement.execute().unwrap();
        statement.finalize().unwrap();
        assert!(statement.execute().is_err());
        assert_eq!(
            query(&db, "SELECT label FROM bound_rows ORDER BY id"),
            vec![
                vec![SqlValue::Text("日本語,'quote'\nnext".into())],
                vec![SqlValue::Null]
            ]
        );
        record(
            root,
            label,
            &mut results,
            "prepared_bind_reset_finalize",
            json!({"pass":true}),
        );
    }
    {
        let db = Arc::new(Database::new());
        db.execute_sql("CREATE TABLE session_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        let mut session = db.sql_session();
        assert_eq!(session.state(), SqlSessionState::Idle);
        session.execute_sql("BEGIN").unwrap();
        assert_eq!(session.state(), SqlSessionState::Active);
        session
            .execute_sql("INSERT INTO session_rows VALUES (1)")
            .unwrap();
        session.execute_sql("ROLLBACK").unwrap();
        assert!(query(&db, "SELECT id FROM session_rows").is_empty());
        session.execute_sql("BEGIN").unwrap();
        session
            .execute_sql("INSERT INTO session_rows VALUES (2)")
            .unwrap();
        session.execute_sql("COMMIT").unwrap();
        assert_eq!(session.state(), SqlSessionState::Idle);
        assert_eq!(
            query(&db, "SELECT id FROM session_rows"),
            vec![vec![SqlValue::Integer(2)]]
        );
        record(
            root,
            label,
            &mut results,
            "sql_session_control",
            json!({"pass":true}),
        );
    }
    {
        let db = Database::new();
        db.execute_sql("CREATE TABLE csv_rows (id INTEGER, label TEXT)")
            .unwrap();
        assert!(matches!(
            db.copy_from_csv_reader("csv_rows", &b"id,label\n1,alpha\n2,beta\n"[..], true)
                .unwrap(),
            ExecutionResult::RowsAffected(2)
        ));
        let mut output = Vec::new();
        assert!(matches!(
            db.copy_to_csv_writer("csv_rows", &mut output, true)
                .unwrap(),
            ExecutionResult::RowsAffected(2)
        ));
        assert_eq!(output, b"id,label\n1,alpha\n2,beta\n");
        db.execute_sql("CREATE TABLE csv_again (id INTEGER, label TEXT)")
            .unwrap();
        db.copy_from_csv_reader("csv_again", std::io::Cursor::new(output.clone()), true)
            .unwrap();
        assert_eq!(
            query(&db, "SELECT * FROM csv_rows ORDER BY id"),
            query(&db, "SELECT * FROM csv_again ORDER BY id")
        );
        record(
            root,
            label,
            &mut results,
            "csv_reader_writer",
            json!({"pass":true,"csv_utf8":String::from_utf8(output).unwrap()}),
        );
    }
    {
        let db = Database::new();
        db.create_catalog(CreateCatalogRequest::new("compat_catalog"))
            .unwrap();
        db.create_namespace(CreateNamespaceRequest::new("compat_catalog", "ns"))
            .unwrap();
        db.create_table(
            CreateTableRequest::new("events")
                .with_catalog_name("compat_catalog")
                .with_namespace_name("ns")
                .with_schema(vec![
                    ColumnDefinition::new("id", DataType::Integer).with_nullable(false),
                    ColumnDefinition::new("note", DataType::Text),
                ])
                .with_primary_key(vec!["id".to_string()]),
        )
        .unwrap();
        assert_eq!(
            db.get_catalog("compat_catalog").unwrap().name,
            "compat_catalog"
        );
        assert_eq!(db.get_namespace("compat_catalog", "ns").unwrap().name, "ns");
        assert!(db
            .list_catalogs()
            .unwrap()
            .iter()
            .any(|c| c.name == "compat_catalog"));
        assert_eq!(db.list_namespaces("compat_catalog").unwrap().len(), 1);
        assert_eq!(db.list_tables("compat_catalog", "ns").unwrap().len(), 1);
        let table = db.get_table_info("compat_catalog", "ns", "events").unwrap();
        assert_eq!(table.columns.len(), 2);
        assert!(db
            .get_table_info("compat_catalog", "ns", "missing")
            .is_err());
        db.delete_table("compat_catalog", "ns", "events").unwrap();
        assert!(db.list_tables("compat_catalog", "ns").unwrap().is_empty());
        db.delete_namespace("compat_catalog", "ns", false).unwrap();
        db.delete_catalog("compat_catalog", false).unwrap();
        record(
            root,
            label,
            &mut results,
            "catalog_hierarchy_and_errors",
            json!({"pass":true}),
        );
    }
    {
        let db = Database::new();
        db.execute_sql("CREATE TABLE frame_rows (id INTEGER, note TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO frame_rows VALUES (1,'one'),(2,'two')")
            .unwrap();
        let frame = db.query_df("SELECT * FROM frame_rows ORDER BY id").unwrap();
        assert_eq!(frame.height(), 2);
        assert_eq!(frame.width(), 2);
        assert!(db
            .query_df("CREATE TABLE not_a_query (id INTEGER)")
            .is_err());
        match db
            .execute_sql_streaming("SELECT * FROM frame_rows ORDER BY id")
            .unwrap()
        {
            SqlStreamingResult::Query(mut stream) => {
                let first = stream.next_row().unwrap().unwrap();
                assert_eq!(first[0], SqlValue::Integer(1));
                assert_eq!(stream.next_row().unwrap().unwrap()[0], SqlValue::Integer(2));
                assert!(stream.next_row().unwrap().is_none());
            }
            _ => panic!("expected streaming query"),
        }
        record(
            root,
            label,
            &mut results,
            "dataframe_and_sql_streaming",
            json!({"pass":true}),
        );
    }
    {
        let db = Database::open_with_config(EmbeddedConfig::in_memory()).unwrap();
        db.set_memory_limit(Some(16 * 1024 * 1024));
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"clone:key", b"clone:value").unwrap();
        tx.commit().unwrap();
        let copy = db.clone_to_memory().unwrap();
        let mut ro = copy.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(b"clone:key").unwrap(), Some(b"clone:value".to_vec()));
        drop(ro);
        assert!(db
            .snapshot()
            .iter()
            .any(|(k, v)| k == b"clone:key" && v == b"clone:value"));
        copy.clear().unwrap();
        assert!(copy.snapshot().is_empty());
        record(
            root,
            label,
            &mut results,
            "memory_config_clone_snapshot_clear",
            json!({"pass":true}),
        );
    }
    {
        let db = Database::new();
        let blob = root.join(format!("{label}.blob"));
        let typed = root.join(format!("{label}.typed"));
        let cancel = root.join(format!("{label}.cancel"));
        let mut writer = db.create_blob_writer(&blob, 6, Some(3)).unwrap();
        writer.write_chunk(b"abc").unwrap();
        writer.write_chunk(b"def").unwrap();
        writer.finish().unwrap();
        let mut writer = db.create_typed_writer(&typed, 42, 5, Some(4)).unwrap();
        writer.write_chunk(b"hel").unwrap();
        writer.write_chunk(b"lo").unwrap();
        writer.finish().unwrap();
        let mut writer = db.create_blob_writer(&cancel, 4, Some(4)).unwrap();
        writer.write_chunk(b"dead").unwrap();
        writer.cancel().unwrap();
        assert!(!cancel.exists());
        let mut payloads = Vec::new();
        for path in [&blob, &typed] {
            let mut reader = db.open_large_value(path).unwrap();
            let mut bytes = Vec::new();
            while let Some((_info, chunk)) = reader.next_chunk().unwrap() {
                bytes.extend_from_slice(&chunk);
            }
            payloads.push(bytes);
        }
        assert_eq!(payloads, vec![b"abcdef".to_vec(), b"hello".to_vec()]);
        fs::remove_file(blob).unwrap();
        fs::remove_file(typed).unwrap();
        record(
            root,
            label,
            &mut results,
            "large_value_blob_typed_cancel",
            json!({"pass":true}),
        );
    }
    fs::write(
        root.join(format!("{label}-result.json")),
        serde_json::to_vec_pretty(&json!({"status":"passed","cases":results})).unwrap(),
    )
    .unwrap();
}
