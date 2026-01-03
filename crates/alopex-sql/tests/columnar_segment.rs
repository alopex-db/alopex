use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use alopex_core::columnar::kvs_bridge::key_layout;
use alopex_core::columnar::segment_v2::decode_row_id;
use alopex_core::kv::memory::MemoryKV;
use alopex_sql::Catalog;
use alopex_sql::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
use alopex_sql::executor::{ExecutionConfig, ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::typed_expr::{Projection, TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{SqlValue, TxnBridge};
use alopex_sql::{RowIdMode, Span};

type ExecutorContext = (
    Arc<MemoryKV>,
    TxnBridge<MemoryKV>,
    Arc<RwLock<MemoryCatalog>>,
    Executor<MemoryKV, MemoryCatalog>,
);

fn create_executor() -> ExecutorContext {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let executor = Executor::new(store.clone(), catalog.clone());
    (store, bridge, catalog, executor)
}

fn create_table(executor: &mut Executor<MemoryKV, MemoryCatalog>) {
    let table = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
            ColumnMetadata::new("name", ResolvedType::Text),
        ],
    );
    let config = ExecutionConfig::default();
    executor
        .execute(
            LogicalPlan::CreateTable {
                table,
                if_not_exists: false,
                with_options: vec![
                    ("storage".into(), "columnar".into()),
                    ("row_group_size".into(), "1000".into()),
                ],
            },
            &config,
        )
        .unwrap();
}

fn create_table_with_row_id_mode(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    name: &str,
    mode: RowIdMode,
) {
    let table = TableMetadata::new(
        name,
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
            ColumnMetadata::new("name", ResolvedType::Text),
        ],
    );
    let config = ExecutionConfig::default();
    let mode_str = match mode {
        RowIdMode::None => "none",
        RowIdMode::Direct => "direct",
    };
    executor
        .execute(
            LogicalPlan::CreateTable {
                table,
                if_not_exists: false,
                with_options: vec![
                    ("storage".into(), "columnar".into()),
                    ("row_group_size".into(), "1000".into()),
                    ("rowid_mode".into(), mode_str.into()),
                ],
            },
            &config,
        )
        .unwrap();
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
fn copy_and_select_columnar_with_pruning_and_projection() {
    let (_store, bridge, catalog, mut executor) = create_executor();
    create_table(&mut executor);

    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut copy_txn = bridge.begin_write().unwrap();
        let result = execute_copy(
            &mut copy_txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
        assert_eq!(result, ExecutionResult::RowsAffected(4));
    }

    let stored = catalog.read().unwrap().get_table("users").unwrap().clone();

    // Row storage should remain untouched for columnar tables.
    let mut verify_txn = bridge.begin_read().unwrap();
    let row_count = verify_txn.table_storage(&stored).scan().unwrap().count();
    verify_txn.commit().unwrap();
    assert_eq!(row_count, 0);

    // Query with filter should return rows from pruned row groups.
    let sql = "SELECT name FROM users WHERE id >= 3";
    let stmt = Parser::parse_sql(&AlopexDialect, sql)
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    let config = ExecutionConfig::default();
    let query_result = executor.execute(plan, &config).unwrap();

    match query_result {
        ExecutionResult::Query(q) => {
            assert_eq!(
                q.rows,
                vec![
                    vec![SqlValue::Text("gamma".into())],
                    vec![SqlValue::Text("delta".into())],
                ]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn columnar_default_row_id_mode_is_direct() {
    let (_store, bridge, catalog, mut executor) = create_executor();
    create_table(&mut executor); // storage=columnar, row_id_mode not specified -> default Direct

    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut copy_txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut copy_txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
    }

    let stored = catalog.read().unwrap().get_table("users").unwrap().clone();
    assert_eq!(stored.storage_options.row_id_mode, RowIdMode::Direct);

    let projection = Projection::All(
        stored
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
    );
    let scan =
        alopex_sql::executor::query::columnar_scan::build_columnar_scan(&stored, &projection);
    let mut txn = bridge.begin_read().unwrap();
    let rows =
        alopex_sql::executor::query::columnar_scan::execute_columnar_scan(&mut txn, &stored, &scan)
            .unwrap();
    txn.commit().unwrap();

    assert_eq!(rows.len(), 4);
    for (idx, row) in rows.iter().enumerate() {
        let (segment_id, offset) = decode_row_id(row.row_id);
        assert_eq!(segment_id, 0);
        assert_eq!(offset as usize, idx);
    }
}

#[test]
fn columnar_row_id_direct_is_encoded_with_segment_and_offset() {
    let (_store, bridge, catalog, mut executor) = create_executor();
    create_table_with_row_id_mode(&mut executor, "users_direct", RowIdMode::Direct);
    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut copy_txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut copy_txn,
            &*guard,
            "users_direct",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
    }

    let stored = catalog
        .read()
        .unwrap()
        .get_table("users_direct")
        .unwrap()
        .clone();

    let projection = Projection::All(
        stored
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
    );
    let scan =
        alopex_sql::executor::query::columnar_scan::build_columnar_scan(&stored, &projection);

    let mut txn = bridge.begin_read().unwrap();
    let rows =
        alopex_sql::executor::query::columnar_scan::execute_columnar_scan(&mut txn, &stored, &scan)
            .unwrap();
    txn.commit().unwrap();

    assert_eq!(rows.len(), 4);
    for (idx, row) in rows.iter().enumerate() {
        let (segment_id, offset) = decode_row_id(row.row_id);
        assert_eq!(segment_id, 0);
        assert_eq!(offset as usize, idx);
    }
}

#[test]
fn execute_columnar_row_ids_filters_and_returns_encoded_row_ids() {
    let (_store, bridge, catalog, mut executor) = create_executor();
    create_table_with_row_id_mode(&mut executor, "users_direct2", RowIdMode::Direct);
    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut copy_txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut copy_txn,
            &*guard,
            "users_direct2",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
    }

    let stored = catalog
        .read()
        .unwrap()
        .get_table("users_direct2")
        .unwrap()
        .clone();

    let projection = Projection::All(
        stored
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
    );
    let predicate = TypedExpr {
        kind: TypedExprKind::BinaryOp {
            left: Box::new(TypedExpr::column_ref(
                stored.name.clone(),
                "id".into(),
                0,
                ResolvedType::Integer,
                Span::default(),
            )),
            op: alopex_sql::ast::expr::BinaryOp::GtEq,
            right: Box::new(TypedExpr::literal(
                alopex_sql::ast::expr::Literal::Number("3".into()),
                ResolvedType::Integer,
                Span::default(),
            )),
        },
        resolved_type: ResolvedType::Boolean,
        span: Span::default(),
    };
    let scan = alopex_sql::executor::query::columnar_scan::build_columnar_scan_for_filter(
        &stored, projection, &predicate,
    );

    let mut txn = bridge.begin_read().unwrap();
    let row_ids = alopex_sql::executor::query::columnar_scan::execute_columnar_row_ids(
        &mut txn, &stored, &scan,
    )
    .unwrap();
    txn.commit().unwrap();

    assert_eq!(row_ids.len(), 2);
    let decoded: Vec<(u64, u64)> = row_ids.iter().map(|r| decode_row_id(*r)).collect();
    assert_eq!(decoded[0], (0, 2));
    assert_eq!(decoded[1], (0, 3));
}

#[test]
fn columnar_scan_falls_back_when_statistics_missing() {
    let (_store, bridge, catalog, mut executor) = create_executor();
    create_table(&mut executor);
    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut copy_txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut copy_txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
    }

    let stored = catalog.read().unwrap().get_table("users").unwrap().clone();

    // Remove RowGroup statistics to force fallback.
    let mut prefix = vec![key_layout::PREFIX_ROW_GROUP];
    prefix.extend_from_slice(&stored.table_id.to_le_bytes());
    bridge
        .with_write_txn(|txn| {
            txn.delete_prefix(&prefix)?;
            Ok(())
        })
        .unwrap();

    let sql = "SELECT name FROM users WHERE id = 2";
    let stmt = Parser::parse_sql(&AlopexDialect, sql)
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    let config = ExecutionConfig::default();
    let result = executor.execute(plan, &config).unwrap();

    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows, vec![vec![SqlValue::Text("beta".into())]]);
        }
        other => panic!("unexpected result {other:?}"),
    }
}
