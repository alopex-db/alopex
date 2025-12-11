use alopex_core::{KVStore, KVTransaction, MemoryKV, TxnMode};
use alopex_sql::catalog::{Catalog, ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{AlopexDialect, Parser, Planner, PlannerError, SqlValue, StorageError, TableStorage};

fn build_catalog_with_table() -> MemoryCatalog {
    let mut catalog = MemoryCatalog::new();
    let table = TableMetadata::new(
        "t",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
        ],
    )
    .with_table_id(1);
    catalog.create_table(table).unwrap();
    catalog
}

#[test]
fn insert_missing_table_returns_planner_error() {
    let dialect = AlopexDialect::default();
    let stmts = Parser::parse_sql(&dialect, "INSERT INTO missing(id) VALUES (1)");
    let ast = stmts.expect("parse failed");

    let catalog = MemoryCatalog::new(); // no tables
    let planner = Planner::new(&catalog);
    let res = planner.plan(&ast[0]);
    assert!(matches!(res, Err(PlannerError::TableNotFound { .. })));
}

#[test]
fn type_mismatch_insert_detected_by_planner() {
    let dialect = AlopexDialect::default();
    let stmts =
        Parser::parse_sql(&dialect, "INSERT INTO t(id, name) VALUES ('oops', 'alice')").unwrap();
    let catalog = build_catalog_with_table();
    let planner = Planner::new(&catalog);
    let res = planner.plan(&stmts[0]);
    assert!(matches!(res, Err(PlannerError::TypeMismatch { .. })));
}

#[test]
fn null_constraint_violation_detected_by_planner() {
    let dialect = AlopexDialect::default();
    let stmts =
        Parser::parse_sql(&dialect, "INSERT INTO t(id, name) VALUES (1, NULL)").unwrap();
    let catalog = build_catalog_with_table();
    let planner = Planner::new(&catalog);
    let res = planner.plan(&stmts[0]);
    assert!(matches!(res, Err(PlannerError::NullConstraintViolation { .. })));
}

#[test]
fn parser_reports_invalid_sql_bulk() {
    let dialect = AlopexDialect::default();
    for _ in 0..100 {
        let res = Parser::parse_sql(&dialect, "INSRT bad syntax");
        assert!(res.is_err());
    }
}

#[test]
fn storage_detects_primary_key_duplicate() {
    let catalog = build_catalog_with_table();
    let table_meta = catalog.get_table("t").unwrap().clone();
    let store = MemoryKV::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    {
        let mut storage = TableStorage::new(&mut txn, &table_meta);
        storage
            .insert(
                1,
                &[
                    SqlValue::Integer(1),
                    SqlValue::Text("alice".to_string()),
                ],
            )
            .unwrap();
        let err = storage.insert(
            1,
            &[
                SqlValue::Integer(1),
                SqlValue::Text("dup".to_string()),
            ],
        );
        assert!(matches!(err, Err(StorageError::PrimaryKeyViolation { .. })));
    }
    txn.commit_self().unwrap();
}

#[test]
fn fk_constraint_not_supported_reports_error() {
    let dialect = AlopexDialect::default();
    let res = Parser::parse_sql(
        &dialect,
        "ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(id)",
    );
    // Parser does not support ALTER ... ADD FOREIGN KEY; treat as unsupported SQL surface.
    assert!(res.is_err());
}
