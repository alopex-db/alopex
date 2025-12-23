use alopex_embedded::{
    ColumnDefinition, CreateCatalogRequest, CreateNamespaceRequest, CreateTableRequest, Database,
    Error, TableType, TxnMode,
};
use alopex_sql::ast::ddl::DataType;
use alopex_sql::ExecutionResult;

fn basic_schema() -> Vec<ColumnDefinition> {
    vec![
        ColumnDefinition::new("id", DataType::Integer).with_nullable(false),
        ColumnDefinition::new("name", DataType::Text),
    ]
}

fn ensure_default_catalog(db: &Database) {
    if db.get_catalog("default").is_err() {
        db.create_catalog(CreateCatalogRequest::new("default"))
            .unwrap();
    }
    if db.get_namespace("default", "default").is_err() {
        db.create_namespace(CreateNamespaceRequest::new("default", "default"))
            .unwrap();
    }
}

#[test]
fn catalog_integration_hierarchy_create_and_list() {
    let db = Database::new();
    ensure_default_catalog(&db);

    db.create_catalog(CreateCatalogRequest::new("main"))
        .unwrap();
    db.create_namespace(CreateNamespaceRequest::new("main", "analytics"))
        .unwrap();
    db.create_table(
        CreateTableRequest::new("events")
            .with_catalog_name("main")
            .with_namespace_name("analytics")
            .with_schema(basic_schema())
            .with_primary_key(vec!["id".to_string()]),
    )
    .unwrap();

    let catalogs = db.list_catalogs().unwrap();
    assert!(catalogs.iter().any(|catalog| catalog.name == "main"));

    let namespaces = db.list_namespaces("main").unwrap();
    assert!(namespaces
        .iter()
        .any(|namespace| namespace.name == "analytics"));

    let tables = db.list_tables("main", "analytics").unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "events");

    db.create_table(
        CreateTableRequest::new("users")
            .with_schema(basic_schema())
            .with_primary_key(vec!["id".to_string()]),
    )
    .unwrap();
    db.execute_sql("CREATE INDEX idx_users_id ON users (id);")
        .unwrap();
    let indexes = db.list_indexes_simple("users").unwrap();
    assert!(indexes.iter().any(|index| index.name == "idx_users_id"));
}

#[test]
fn catalog_integration_force_cascade_delete() {
    let db = Database::new();
    db.create_catalog(CreateCatalogRequest::new("main"))
        .unwrap();
    db.create_namespace(CreateNamespaceRequest::new("main", "analytics"))
        .unwrap();
    db.create_table(
        CreateTableRequest::new("events")
            .with_catalog_name("main")
            .with_namespace_name("analytics")
            .with_schema(basic_schema())
            .with_primary_key(vec!["id".to_string()]),
    )
    .unwrap();

    db.delete_catalog("main", true).unwrap();
    let err = db.get_catalog("main").unwrap_err();
    assert!(matches!(err, Error::CatalogNotFound(ref name) if name == "main"));
    assert!(err.to_string().contains("main"));
}

#[test]
fn catalog_integration_delete_catalog_with_default_namespace_only() {
    let db = Database::new();
    db.create_catalog(CreateCatalogRequest::new("temp"))
        .unwrap();
    db.create_namespace(CreateNamespaceRequest::new("temp", "default"))
        .unwrap();

    db.delete_catalog("temp", false).unwrap();
    let err = db.get_catalog("temp").unwrap_err();
    assert!(matches!(err, Error::CatalogNotFound(name) if name == "temp"));
}

#[test]
fn catalog_integration_default_object_protection() {
    let db = Database::new();
    let err = db.delete_catalog("default", false).unwrap_err();
    assert!(matches!(err, Error::CannotDeleteDefault(_)));
    let err = db
        .delete_namespace("default", "default", false)
        .unwrap_err();
    assert!(matches!(err, Error::CannotDeleteDefault(_)));
}

#[test]
fn catalog_integration_txn_overlay_and_commit() {
    let db = Database::new();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();

    txn.create_catalog(CreateCatalogRequest::new("main"))
        .unwrap();
    txn.create_namespace(CreateNamespaceRequest::new("main", "analytics"))
        .unwrap();
    txn.create_table(
        CreateTableRequest::new("events")
            .with_catalog_name("main")
            .with_namespace_name("analytics")
            .with_schema(basic_schema())
            .with_primary_key(vec!["id".to_string()]),
    )
    .unwrap();

    let tables = txn.list_tables("main", "analytics").unwrap();
    assert_eq!(tables.len(), 1);
    assert!(matches!(
        db.get_catalog("main"),
        Err(Error::CatalogNotFound(_))
    ));

    txn.commit().unwrap();

    let tables = db.list_tables("main", "analytics").unwrap();
    assert_eq!(tables.len(), 1);
}

#[test]
fn catalog_integration_txn_rollback_discards_changes() {
    let db = Database::new();
    {
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.create_catalog(CreateCatalogRequest::new("main"))
            .unwrap();
        txn.create_namespace(CreateNamespaceRequest::new("main", "analytics"))
            .unwrap();
        txn.create_table(
            CreateTableRequest::new("events")
                .with_catalog_name("main")
                .with_namespace_name("analytics")
                .with_schema(basic_schema())
                .with_primary_key(vec!["id".to_string()]),
        )
        .unwrap();
        txn.rollback().unwrap();
    }

    assert!(matches!(
        db.get_catalog("main"),
        Err(Error::CatalogNotFound(_))
    ));
}

#[test]
fn catalog_integration_sql_create_table_defaults_to_default_namespace() {
    let db = Database::new();
    ensure_default_catalog(&db);
    db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY);")
        .unwrap();

    let info = db.get_table_info_simple("users").unwrap();
    assert_eq!(info.catalog_name, "default");
    assert_eq!(info.namespace_name, "default");
}

#[test]
fn catalog_integration_api_and_sql_share_catalog() {
    let db = Database::new();
    ensure_default_catalog(&db);
    db.create_table(
        CreateTableRequest::new("api_table")
            .with_schema(basic_schema())
            .with_primary_key(vec!["id".to_string()]),
    )
    .unwrap();

    let result = db.execute_sql("SELECT id FROM api_table;").unwrap();
    match result {
        ExecutionResult::Query(result) => assert!(result.rows.is_empty()),
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn catalog_integration_sql_table_visible_via_api() {
    let db = Database::new();
    ensure_default_catalog(&db);
    db.execute_sql("CREATE TABLE sql_table (id INTEGER PRIMARY KEY);")
        .unwrap();

    let info = db.get_table_info_simple("sql_table").unwrap();
    assert_eq!(info.name, "sql_table");
    assert_eq!(info.catalog_name, "default");
    assert_eq!(info.namespace_name, "default");
}

#[test]
fn catalog_integration_error_scenarios() {
    let db = Database::new();

    let err = db.get_catalog("missing").unwrap_err();
    assert!(matches!(err, Error::CatalogNotFound(ref name) if name == "missing"));
    assert!(err.to_string().contains("missing"));

    db.create_catalog(CreateCatalogRequest::new("main"))
        .unwrap();
    let err = db
        .create_catalog(CreateCatalogRequest::new("main"))
        .unwrap_err();
    assert!(matches!(
        err,
        Error::CatalogAlreadyExists(ref name) if name == "main"
    ));
    assert!(err.to_string().contains("main"));

    let err = db.get_namespace("main", "missing").unwrap_err();
    assert!(matches!(
        err,
        Error::NamespaceNotFound(ref catalog, ref namespace)
            if catalog == "main" && namespace == "missing"
    ));
    assert!(err.to_string().contains("main"));
    assert!(err.to_string().contains("missing"));

    db.create_namespace(CreateNamespaceRequest::new("main", "analytics"))
        .unwrap();
    let err = db
        .create_namespace(CreateNamespaceRequest::new("main", "analytics"))
        .unwrap_err();
    assert!(matches!(
        err,
        Error::NamespaceAlreadyExists(catalog, namespace)
            if catalog == "main" && namespace == "analytics"
    ));

    let err = db
        .get_table_info("main", "analytics", "missing")
        .unwrap_err();
    assert!(matches!(err, Error::TableNotFound(name) if name.contains("missing")));

    db.create_table(
        CreateTableRequest::new("events")
            .with_catalog_name("main")
            .with_namespace_name("analytics")
            .with_schema(basic_schema())
            .with_primary_key(vec!["id".to_string()]),
    )
    .unwrap();
    let err = db
        .create_table(
            CreateTableRequest::new("events")
                .with_catalog_name("main")
                .with_namespace_name("analytics")
                .with_schema(basic_schema())
                .with_primary_key(vec!["id".to_string()]),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        Error::TableAlreadyExists(name) if name == "main.analytics.events"
    ));

    let err = db
        .get_index_info("main", "analytics", "events", "missing_idx")
        .unwrap_err();
    assert!(matches!(err, Error::IndexNotFound(name) if name.contains("missing_idx")));

    let err = db.delete_namespace("main", "analytics", false).unwrap_err();
    assert!(matches!(
        err,
        Error::NamespaceNotEmpty(catalog, namespace)
            if catalog == "main" && namespace == "analytics"
    ));

    let err = db.delete_catalog("main", false).unwrap_err();
    assert!(matches!(err, Error::CatalogNotEmpty(name) if name == "main"));

    ensure_default_catalog(&db);
    let err = db
        .create_table(CreateTableRequest::new("no_schema"))
        .unwrap_err();
    assert!(matches!(err, Error::SchemaRequired));

    let err = db
        .create_table(
            CreateTableRequest::new("external")
                .with_table_type(TableType::External)
                .with_schema(basic_schema()),
        )
        .unwrap_err();
    assert!(matches!(err, Error::StorageRootRequired));

    let err = db
        .create_table(
            CreateTableRequest::new("bad_pk")
                .with_catalog_name("default")
                .with_namespace_name("default")
                .with_schema(basic_schema())
                .with_primary_key(vec!["missing".to_string()]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::Core(_)));
    assert!(err.to_string().contains("missing"));

    let err = db.delete_catalog("default", false).unwrap_err();
    assert!(matches!(err, Error::CannotDeleteDefault(_)));
    let err = db
        .delete_namespace("default", "default", false)
        .unwrap_err();
    assert!(matches!(err, Error::CannotDeleteDefault(_)));

    let err = db
        .execute_sql("INSERT INTO missing_table (id) VALUES (1);")
        .unwrap_err();
    assert!(matches!(err, Error::Sql(_)));
    assert_eq!(err.sql_error_code(), Some("ALOPEX-C001"));

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let err = ro
        .create_catalog(CreateCatalogRequest::new("readonly"))
        .unwrap_err();
    assert!(matches!(err, Error::TxnReadOnly));
}
