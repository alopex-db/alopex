#![cfg(not(target_arch = "wasm32"))]

use std::sync::{Arc, RwLock};

use alopex_core::kv::AnyKV;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::{CheckpointConfig, ConvergePolicy, LsmKVConfig};
use alopex_core::{StorageFactory, StorageMode};
use alopex_sql::catalog::PersistentCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;

fn execute(
    executor: &mut Executor<AnyKV, PersistentCatalog<AnyKV>>,
    catalog: &Arc<RwLock<PersistentCatalog<AnyKV>>>,
    sql: &str,
) -> Vec<ExecutionResult> {
    Parser::parse_sql(&AlopexDialect, sql)
        .unwrap()
        .into_iter()
        .map(|statement| {
            let plan = Planner::new(&*catalog.read().unwrap())
                .plan(&statement)
                .unwrap();
            executor.execute(plan).unwrap()
        })
        .collect()
}

#[test]
fn alopex_container_reopens_rows_and_index_after_capacity_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let container = dir.path().join("tiny.alopex");
    let data_dir = dir.path().join("tiny.alopex.d");
    // Only three rows. The ring is small enough to require reclamation but
    // accommodates each catalog/index batch independently.
    let cfg = LsmKVConfig {
        wal: WalConfig {
            segment_size: 2048,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: true,
            wal_size_threshold: 1,
            min_interval_ms: 0,
        },
        converge: ConvergePolicy::Always {
            container: container.clone(),
        },
        ..Default::default()
    };
    {
        let store = Arc::new(
            StorageFactory::create(StorageMode::Disk {
                path: data_dir.clone(),
                config: Some(cfg.clone()),
            })
            .unwrap(),
        );
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        let mut executor = Executor::new(store.clone(), catalog.clone());
        execute(
            &mut executor,
            &catalog,
            "CREATE TABLE items (id INT PRIMARY KEY, value INT)",
        );
        for id in 1..=3 {
            execute(
                &mut executor,
                &catalog,
                &format!("INSERT INTO items VALUES ({id}, {})", id * 10),
            );
        }
        execute(
            &mut executor,
            &catalog,
            "CREATE INDEX by_value ON items (value)",
        );
        let AnyKV::Lsm(lsm) = store.as_ref() else {
            panic!("expected LSM")
        };
        assert!(lsm.wal_checkpoint_stats().checkpoint_count > 0);
    }
    assert!(container.is_file());
    // Drop prunes the sidecar, so this exercises the public container restore path.
    assert!(!data_dir.exists());
    let store = Arc::new(
        StorageFactory::create(StorageMode::Disk {
            path: data_dir,
            config: Some(cfg),
        })
        .unwrap(),
    );
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    let mut executor = Executor::new(store, catalog.clone());
    let all = execute(
        &mut executor,
        &catalog,
        "SELECT id, value FROM items ORDER BY id",
    );
    let ExecutionResult::Query(query) = &all[0] else {
        panic!("expected query")
    };
    use alopex_sql::storage::SqlValue;
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
            vec![SqlValue::Integer(3), SqlValue::Integer(30)],
        ]
    );
    let results = execute(
        &mut executor,
        &catalog,
        "SELECT id FROM items WHERE value = 20",
    );
    let ExecutionResult::Query(query) = &results[0] else {
        panic!("expected query")
    };
    assert_eq!(query.rows.len(), 1);
    // The index definition survives together with its rows.
    use alopex_sql::catalog::Catalog;
    assert!(catalog.read().unwrap().get_index("by_value").is_some());
}
