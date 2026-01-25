use std::sync::{Arc, RwLock};

use alopex_core::lsm::LsmKVConfig;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::{StorageFactory, StorageMode};
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use tempfile::TempDir;

mod aggregate_functions_test;
mod group_by_edge_cases_test;
mod group_by_test;

pub struct TestHarness {
    executor: Executor<alopex_core::kv::AnyKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
    _temp_dir: TempDir,
}

impl TestHarness {
    pub fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };
        let store = Arc::new(
            StorageFactory::create(StorageMode::Disk {
                path: temp_dir.path().to_path_buf(),
                config: Some(cfg),
            })
            .expect("disk store"),
        );
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let executor = Executor::new(store, Arc::clone(&catalog));
        Self {
            executor,
            catalog,
            _temp_dir: temp_dir,
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> Vec<ExecutionResult> {
        let dialect = AlopexDialect;
        let statements = Parser::parse_sql(&dialect, sql).expect("parse sql");
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            let plan = {
                let guard = self.catalog.read().expect("catalog read");
                let planner = Planner::new(&*guard);
                planner.plan(&stmt).expect("plan")
            };
            let result = self.executor.execute(plan).expect("execute");
            results.push(result);
        }
        results
    }

    pub fn query_sql(&mut self, sql: &str) -> QueryResult {
        let results = self.execute_sql(sql);
        results
            .into_iter()
            .rev()
            .find_map(|result| match result {
                ExecutionResult::Query(query) => Some(query),
                _ => None,
            })
            .expect("query result")
    }

    pub fn catalog(&self) -> &Arc<RwLock<MemoryCatalog>> {
        &self.catalog
    }
}
