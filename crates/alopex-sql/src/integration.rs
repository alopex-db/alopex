//! SQL API の Disk モード統合テスト（最小）。

use std::sync::{Arc, RwLock};

use alopex_core::lsm::LsmKVConfig;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::{StorageFactory, StorageMode};

use crate::ast::expr::Literal;
use crate::catalog::{Catalog, ColumnMetadata, MemoryCatalog, TableMetadata};
use crate::executor::{ExecutionConfig, ExecutionResult, Executor};
use crate::planner::typed_expr::{Projection, TypedAssignment, TypedExpr};
use crate::planner::types::ResolvedType;
use crate::{LogicalPlan, Span};

pub mod disk {
    use super::*;

    #[test]
    fn ddl_and_dml_work_on_disk_mode_store() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };
        let path = dir.path().to_path_buf();

        // DDL: CREATE TABLE
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer)
                    .with_primary_key(true)
                    .with_not_null(true),
                ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        // 書く → drop → reopen → 読める（プロセス再起動相当）
        // 1) CREATE TABLE + INSERT
        let table_id = {
            let store = Arc::new(
                StorageFactory::create(StorageMode::Disk {
                    path: path.clone(),
                    config: Some(cfg.clone()),
                })
                .unwrap(),
            );
            let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
            let mut executor = Executor::new(store, catalog.clone());
            let config = ExecutionConfig::default();

            let res = executor
                .execute(
                    LogicalPlan::CreateTable {
                        table: table.clone(),
                        if_not_exists: false,
                        with_options: vec![],
                    },
                    &config,
                )
                .unwrap();
            assert!(matches!(res, ExecutionResult::Success));

            let values = vec![
                vec![
                    TypedExpr::literal(
                        Literal::Number("1".into()),
                        ResolvedType::Integer,
                        Span::default(),
                    ),
                    TypedExpr::literal(
                        Literal::String("a".into()),
                        ResolvedType::Text,
                        Span::default(),
                    ),
                ],
                vec![
                    TypedExpr::literal(
                        Literal::Number("2".into()),
                        ResolvedType::Integer,
                        Span::default(),
                    ),
                    TypedExpr::literal(
                        Literal::String("b".into()),
                        ResolvedType::Text,
                        Span::default(),
                    ),
                ],
            ];
            let res = executor
                .execute(
                    LogicalPlan::Insert {
                        table: "users".into(),
                        columns: vec!["id".into(), "name".into()],
                        values,
                    },
                    &config,
                )
                .unwrap();
            assert!(matches!(res, ExecutionResult::RowsAffected(2)));

            let stored = catalog.read().unwrap().get_table("users").cloned().unwrap();
            stored.table_id
        };

        // 2) reopen: SELECT/UPDATE/DELETE/DROP
        let store = Arc::new(
            StorageFactory::create(StorageMode::Disk {
                path,
                config: Some(cfg),
            })
            .unwrap(),
        );
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        {
            let mut c = catalog.write().unwrap();
            c.create_table(table.with_table_id(table_id)).unwrap();
        }
        let mut executor = Executor::new(store, catalog.clone());
        let config = ExecutionConfig::default();

        // QUERY: SELECT（Scan のみ）
        let res = executor
            .execute(
                LogicalPlan::Scan {
                    table: "users".into(),
                    projection: Projection::All(vec!["id".into(), "name".into()]),
                },
                &config,
            )
            .unwrap();
        let ExecutionResult::Query(q) = res else {
            panic!("expected query result");
        };
        assert_eq!(q.rows.len(), 2);

        // DML: UPDATE（全行を更新）
        let assign = TypedAssignment {
            column: "name".into(),
            column_index: 1,
            value: TypedExpr::literal(
                Literal::String("z".into()),
                ResolvedType::Text,
                Span::default(),
            ),
        };
        let res = executor
            .execute(
                LogicalPlan::Update {
                    table: "users".into(),
                    assignments: vec![assign],
                    filter: None,
                },
                &config,
            )
            .unwrap();
        assert!(matches!(res, ExecutionResult::RowsAffected(2)));

        // DML: DELETE（全行を削除）
        let res = executor
            .execute(
                LogicalPlan::Delete {
                    table: "users".into(),
                    filter: None,
                },
                &config,
            )
            .unwrap();
        assert!(matches!(res, ExecutionResult::RowsAffected(2)));

        let res = executor
            .execute(
                LogicalPlan::Scan {
                    table: "users".into(),
                    projection: Projection::All(vec!["id".into(), "name".into()]),
                },
                &config,
            )
            .unwrap();
        let ExecutionResult::Query(q) = res else {
            panic!("expected query result");
        };
        assert_eq!(q.rows.len(), 0);

        // DDL: DROP TABLE
        let res = executor
            .execute(
                LogicalPlan::DropTable {
                    name: "users".into(),
                    if_exists: false,
                },
                &config,
            )
            .unwrap();
        assert!(matches!(res, ExecutionResult::Success));
    }
}
