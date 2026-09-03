use alopex_core::{KVStore, KVTransaction, MemoryKV, TxnMode};
use alopex_sql::catalog::{Catalog, ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{
    AlopexDialect, AlterTableAction, Parser, Planner, PlannerError, SqlValue, StatementKind,
    StorageError, TableStorage,
};

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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn insert_missing_table_returns_planner_error() {
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, "INSERT INTO missing(id) VALUES (1)");
    let ast = stmts.expect("parse failed");

    let catalog = MemoryCatalog::new(); // no tables
    let planner = Planner::new(&catalog);
    let res = planner.plan(&ast[0]);
    assert!(matches!(res, Err(PlannerError::TableNotFound { .. })));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn type_mismatch_insert_detected_by_planner() {
    let dialect = AlopexDialect;
    let stmts =
        Parser::parse_sql(&dialect, "INSERT INTO t(id, name) VALUES ('oops', 'alice')").unwrap();
    let catalog = build_catalog_with_table();
    let planner = Planner::new(&catalog);
    let res = planner.plan(&stmts[0]);
    assert!(matches!(res, Err(PlannerError::TypeMismatch { .. })));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn null_constraint_violation_detected_by_planner() {
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, "INSERT INTO t(id, name) VALUES (1, NULL)").unwrap();
    let catalog = build_catalog_with_table();
    let planner = Planner::new(&catalog);
    let res = planner.plan(&stmts[0]);
    assert!(matches!(
        res,
        Err(PlannerError::NullConstraintViolation { .. })
    ));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn parser_reports_invalid_sql_bulk() {
    let dialect = AlopexDialect;
    for _ in 0..100 {
        let res = Parser::parse_sql(&dialect, "INSRT bad syntax");
        assert!(res.is_err());
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
                &[SqlValue::Integer(1), SqlValue::Text("alice".to_string())],
            )
            .unwrap();
        let err = storage.insert(
            1,
            &[SqlValue::Integer(1), SqlValue::Text("dup".to_string())],
        );
        assert!(matches!(err, Err(StorageError::PrimaryKeyViolation { .. })));
    }
    txn.commit_self().unwrap();
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn fk_constraint_not_supported_reports_error() {
    let dialect = AlopexDialect;
    let res = Parser::parse_sql(
        &dialect,
        "ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(id)",
    );
    // Parser does not support ALTER ... ADD FOREIGN KEY; treat as unsupported SQL surface.
    assert!(res.is_err());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn truncate_and_alter_schema_statements_are_parsed() {
    let dialect = AlopexDialect;
    let truncate = Parser::parse_sql(&dialect, "TRUNCATE TABLE t").expect("TRUNCATE should parse");
    assert!(matches!(truncate[0].kind, StatementKind::TruncateTable(_)));

    let alter = Parser::parse_sql(&dialect, "ALTER TABLE t RENAME COLUMN name TO display_name")
        .expect("ALTER TABLE should parse");
    let StatementKind::AlterTable(alter_stmt) = &alter[0].kind else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(
        alter_stmt.action,
        AlterTableAction::RenameColumn { .. }
    ));
}
