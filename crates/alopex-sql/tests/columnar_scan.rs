use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::Executor;
use alopex_sql::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
use alopex_sql::executor::query::columnar_scan::{
    build_columnar_scan_for_filter, execute_columnar_scan,
};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::typed_expr::{ProjectedColumn, Projection, TypedExpr};
use alopex_sql::storage::TxnBridge;
use alopex_sql::{Catalog, Span};

fn create_table(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
) {
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH (storage='columnar', row_group_size=1000);",
    )
    .unwrap()
    .pop()
    .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    executor.execute(plan).unwrap();
}

fn write_csv(path: &Path) {
    let mut f = File::create(path).unwrap();
    writeln!(f, "id,name").unwrap();
    writeln!(f, "1,alpha").unwrap();
    writeln!(f, "2,beta").unwrap();
    writeln!(f, "3,gamma").unwrap();
    writeln!(f, "4,delta").unwrap();
}

#[test]
fn columnar_scan_applies_pushdown_and_projection() {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    create_table(&mut executor, &catalog);

    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    let stored = catalog.read().unwrap().get_table("users").unwrap().clone();
    let projection = Projection::Columns(vec![ProjectedColumn {
        expr: TypedExpr::column_ref(
            stored.name.clone(),
            "name".into(),
            1,
            alopex_sql::planner::types::ResolvedType::Text,
            Span::default(),
        ),
        alias: None,
    }]);
    let predicate = TypedExpr::new(
        alopex_sql::planner::typed_expr::TypedExprKind::BinaryOp {
            left: Box::new(TypedExpr::column_ref(
                stored.name.clone(),
                "id".into(),
                0,
                alopex_sql::planner::types::ResolvedType::Integer,
                Span::default(),
            )),
            op: alopex_sql::ast::expr::BinaryOp::GtEq,
            right: Box::new(TypedExpr::literal(
                alopex_sql::ast::expr::Literal::Number("3".into()),
                alopex_sql::planner::types::ResolvedType::Integer,
                Span::default(),
            )),
        },
        alopex_sql::planner::types::ResolvedType::Boolean,
        Span::default(),
    );

    let scan = build_columnar_scan_for_filter(&stored, projection, &predicate);
    let mut txn = bridge.begin_read().unwrap();
    let rows = execute_columnar_scan(&mut txn, &stored, &scan).unwrap();
    txn.commit().unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| r.values[1].clone())
            .collect::<Vec<_>>(),
        vec![
            alopex_sql::storage::SqlValue::Text("gamma".into()),
            alopex_sql::storage::SqlValue::Text("delta".into())
        ]
    );
}
