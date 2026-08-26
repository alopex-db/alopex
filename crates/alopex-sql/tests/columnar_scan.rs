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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

/// A correlated predicate over columnar storage must not be fused into the
/// columnar scan (issue #151, D15).
///
/// Filter fusion resolves every column index against the scanned table, while a
/// correlated predicate also carries outer-row indexes past that width. Running
/// a LATERAL join whose right side filters a columnar table is what exercises
/// the boundary end to end.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn a_lateral_correlated_predicate_is_not_fused_into_a_columnar_scan() {
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

    let statement = Parser::parse_sql(
        &AlopexDialect,
        "SELECT v.id, l.name FROM (VALUES (3), (4)) AS v(id) CROSS JOIN LATERAL \
         (SELECT users.name FROM users WHERE users.id = v.id) AS l ORDER BY v.id",
    )
    .unwrap()
    .pop()
    .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&statement).unwrap()
    };
    let result = match executor.execute(plan).unwrap() {
        alopex_sql::executor::ExecutionResult::Query(result) => result,
        other => panic!("expected a query result, got {other:?}"),
    };

    assert_eq!(
        result.rows,
        vec![
            vec![
                alopex_sql::storage::SqlValue::Integer(3),
                alopex_sql::storage::SqlValue::Text("gamma".into()),
            ],
            vec![
                alopex_sql::storage::SqlValue::Integer(4),
                alopex_sql::storage::SqlValue::Text("delta".into()),
            ],
        ]
    );
}

fn create_scores_table(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
) {
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "CREATE TABLE cc (id INT PRIMARY KEY, v INT, flag INT, nm TEXT) \
         WITH (storage='columnar', row_group_size=1000);",
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

fn write_scores_csv(path: &Path) {
    let mut f = File::create(path).unwrap();
    writeln!(f, "id,v,flag,nm").unwrap();
    writeln!(f, "1,10,1,alpha").unwrap();
    writeln!(f, "2,20,0,beta").unwrap();
    writeln!(f, "3,30,1,gamma").unwrap();
}

/// A columnar scan materialises exactly the columns the pushed projection
/// names and fills every other column with NULL, so a column that appears only
/// inside an aggregate `FILTER` predicate or an aggregate-local `ORDER BY` must
/// be collected too. Before the fix `flag` was NULL in every row, the filter
/// accepted nothing, and `SUM(v) FILTER (WHERE flag > 0)` returned NULL
/// instead of 40.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn columnar_projection_pushdown_covers_aggregate_filter_and_order_by() {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    create_scores_table(&mut executor, &catalog);

    let file = tempfile::NamedTempFile::new().unwrap();
    write_scores_csv(file.path());
    {
        let guard = catalog.read().unwrap();
        let mut txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut txn,
            &*guard,
            "cc",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    let mut run = |sql: &str| {
        let statement = Parser::parse_sql(&AlopexDialect, sql)
            .unwrap()
            .pop()
            .unwrap();
        let plan = {
            let guard = catalog.read().unwrap();
            Planner::new(&*guard).plan(&statement).unwrap()
        };
        match executor.execute(plan).unwrap() {
            alopex_sql::executor::ExecutionResult::Query(result) => result.rows,
            other => panic!("expected a query result, got {other:?}"),
        }
    };

    assert_eq!(
        run("SELECT SUM(v) FILTER (WHERE flag > 0) FROM cc"),
        vec![vec![alopex_sql::storage::SqlValue::BigInt(40)]]
    );
    assert_eq!(
        run("SELECT GROUP_CONCAT(nm ORDER BY v DESC) FROM cc"),
        vec![vec![alopex_sql::storage::SqlValue::Text(
            "gamma,beta,alpha".into()
        )]]
    );
}
