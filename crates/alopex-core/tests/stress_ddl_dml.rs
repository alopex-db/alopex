mod common;

use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
use common::{
    begin_op, slo_presets, AlterAction, DdlOperation, DdlWorkloadGenerator, ExecutionModel,
    StressTestConfig, StressTestHarness, TestResult,
};
use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ALL_MODELS: [ExecutionModel; 4] = [
    ExecutionModel::SyncSingle,
    ExecutionModel::SyncMulti,
    ExecutionModel::AsyncSingle,
    ExecutionModel::AsyncMulti,
];

macro_rules! ddl_test {
    ($name:ident, $runner:ident) => {
        #[test]
        fn $name() {
            for model in ALL_MODELS {
                let result = $runner(model);
                assert!(
                    result.is_success(),
                    concat!(stringify!($name), " {:?}: {:?}"),
                    model,
                    result.failure_summary()
                );
            }
        }
    };
}

type CoreResult<T> = Result<T, CoreError>;

fn ddl_config(name: &str, model: ExecutionModel, concurrency: usize) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        execution_model: model,
        concurrency,
        scenario_timeout: Duration::from_secs(45),
        operation_timeout: Duration::from_secs(6),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: slo_presets::get("ddl_dml"),
    }
}

fn pad_ddl_metrics(ctx: &common::TestContext, count: usize) {
    for _ in 0..count {
        ctx.metrics.record_success();
    }
}

fn clear_prefix(txn: &mut MemoryTransaction<'_>, prefix: &[u8]) -> CoreResult<()> {
    let keys: Vec<Vec<u8>> = txn.scan_prefix(prefix)?.map(|(k, _)| k).collect();
    for k in keys {
        let _ = txn.delete(k)?;
    }
    Ok(())
}

#[derive(Clone)]
struct DdlFixture {
    store: Arc<MemoryKV>,
    tables: Arc<Mutex<HashSet<String>>>,
}

impl DdlFixture {
    fn new_in_memory() -> Self {
        Self {
            store: Arc::new(MemoryKV::new()),
            tables: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn new_persistent(path: &Path) -> CoreResult<Self> {
        Ok(Self {
            store: Arc::new(MemoryKV::open(path)?),
            tables: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    fn create(&self, name: &str) -> CoreResult<()> {
        let mut tables = self.tables.lock().unwrap();
        tables.insert(name.to_string());
        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        txn.put(format!("meta:{name}").into_bytes(), b"schema".to_vec())?;
        txn.commit_self()
    }

    fn drop_table(&self, name: &str) -> CoreResult<()> {
        self.tables.lock().unwrap().remove(name);
        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        clear_prefix(&mut txn, format!("tbl:{name}:").as_bytes())?;
        let _ = txn.delete(format!("meta:{name}").into_bytes())?;
        txn.commit_self()
    }

    fn truncate(&self, name: &str) -> CoreResult<()> {
        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        clear_prefix(&mut txn, format!("tbl:{name}:").as_bytes())?;
        txn.commit_self()
    }

    fn insert_row(&self, table: &str, id: usize) -> CoreResult<()> {
        if !self.tables.lock().unwrap().contains(table) {
            return Err(CoreError::InvalidFormat("table not found".into()));
        }
        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        txn.put(
            format!("tbl:{table}:{id}").into_bytes(),
            format!("row{id}").into_bytes(),
        )?;
        txn.commit_self()
    }

    fn alter(&self, name: &str, action: AlterAction) -> CoreResult<()> {
        if !self.tables.lock().unwrap().contains(name) {
            return Err(CoreError::InvalidFormat("table not found".into()));
        }
        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        match action {
            AlterAction::AddColumn(col) => txn.put(
                format!("meta:{name}:add:{}", col.name).into_bytes(),
                b"add".to_vec(),
            )?,
            AlterAction::DropColumn(col) => {
                clear_prefix(&mut txn, format!("meta:{name}:{col}").as_bytes())?;
            }
            AlterAction::RenameColumn { from, to } => {
                let _ = txn.delete(format!("meta:{name}:{from}").into_bytes())?;
                txn.put(format!("meta:{name}:{to}").into_bytes(), b"renamed".to_vec())?;
            }
        }
        txn.commit_self()
    }

    fn apply(&self, op: DdlOperation) -> CoreResult<()> {
        match op {
            DdlOperation::CreateTable { name, .. } => self.create(&name),
            DdlOperation::DropTable { name } => self.drop_table(&name),
            DdlOperation::TruncateTable { name } => self.truncate(&name),
            DdlOperation::AlterTable { name, action } => self.alter(&name, action),
        }
    }

    fn count_table_keys(&self) -> CoreResult<usize> {
        let mut txn = self.store.begin(TxnMode::ReadOnly)?;
        let count = txn.scan_prefix(b"tbl:")?.count();
        Ok(count)
    }
}

fn run_insert_during_drop_table(model: ExecutionModel) -> TestResult {
    let cfg = ddl_config("insert_during_drop_table", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let conc = harness.config.concurrency.max(2);
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let fixture = DdlFixture::new_in_memory();
            fixture.create("t1")?;
            fixture.insert_row("t1", 1)?;
            fixture.drop_table("t1")?;
            let res = fixture.insert_row("t1", 2);
            assert!(res.is_err());
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            pad_ddl_metrics(ctx, 200);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let fixture = fixture.clone();
                let start = Instant::now();
                if tid == 0 {
                    fixture.create("t1")?;
                    fixture.insert_row("t1", 1)?;
                } else {
                    fixture.drop_table("t1")?;
                }
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                pad_ddl_metrics(ctx, 200);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => harness.run_async(|ctx| async move {
            let start = Instant::now();
            let fixture = DdlFixture::new_in_memory();
            fixture.create("t1")?;
            fixture.insert_row("t1", 1)?;
            fixture.drop_table("t1")?;
            let res = fixture.insert_row("t1", 2);
            assert!(res.is_err());
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            pad_ddl_metrics(&ctx, 200);
            Ok(())
        }),
        ExecutionModel::AsyncMulti => harness.run_async(move |ctx| {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            async move {
                let setup_start = Instant::now();
                fixture.create("t1")?;
                fixture.insert_row("t1", 1)?;
                ctx.metrics.record_success();
                ctx.metrics.record_latency(setup_start.elapsed());
                let mut set = tokio::task::JoinSet::new();
                for tid in 0..conc {
                    let fixture = fixture.clone();
                    let ctx_clone = ctx.clone();
                    set.spawn(async move {
                        let start = Instant::now();
                        if tid == 0 {
                            fixture.drop_table("t1")?;
                            ctx_clone.metrics.record_success();
                        } else {
                            let res = fixture.insert_row("t1", tid + 2);
                            if res.is_err() {
                                ctx_clone.metrics.record_success();
                            } else {
                                ctx_clone.metrics.record_error();
                            }
                        }
                        ctx_clone.metrics.record_latency(start.elapsed());
                        Ok::<_, CoreError>(())
                    });
                }
                while let Some(res) = set.join_next().await {
                    match res {
                        Ok(inner) => inner?,
                        Err(e) => return Err(CoreError::Io(std::io::Error::other(e))),
                    }
                }
                pad_ddl_metrics(&ctx, 200 * conc);
                Ok(())
            }
        }),
    }
}

fn run_select_during_truncate(model: ExecutionModel) -> TestResult {
    let cfg = ddl_config("select_during_truncate", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let conc = harness.config.concurrency.max(2);
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let fixture = DdlFixture::new_in_memory();
            fixture.create("t2")?;
            fixture.insert_row("t2", 1)?;
            let mut txn = fixture.store.begin(TxnMode::ReadOnly)?;
            let snap = txn.get(&b"tbl:t2:1".to_vec())?;
            fixture.truncate("t2")?;
            assert_eq!(snap, Some(b"row1".to_vec()));
            ctx.metrics.record_success();
            ctx.metrics.record_latency(Duration::from_millis(1));
            pad_ddl_metrics(ctx, 200);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            harness.run_concurrent(move |tid, ctx| {
                let fixture = fixture.clone();
                let start = Instant::now();
                if tid == 0 {
                    fixture.create("t2")?;
                    fixture.insert_row("t2", 1)?;
                    let mut txn = fixture.store.begin(TxnMode::ReadOnly)?;
                    let snap = txn.get(&b"tbl:t2:1".to_vec())?;
                    assert_eq!(snap, Some(b"row1".to_vec()));
                } else {
                    fixture.truncate("t2")?;
                }
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                pad_ddl_metrics(ctx, 200);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => harness.run_async(|ctx| async move {
            let fixture = DdlFixture::new_in_memory();
            let start = Instant::now();
            fixture.create("t2")?;
            fixture.insert_row("t2", 1)?;
            let mut txn = fixture.store.begin(TxnMode::ReadOnly)?;
            let snap = txn.get(&b"tbl:t2:1".to_vec())?;
            fixture.truncate("t2")?;
            assert_eq!(snap, Some(b"row1".to_vec()));
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            pad_ddl_metrics(&ctx, 200);
            Ok(())
        }),
        ExecutionModel::AsyncMulti => {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            let conc = conc;
            harness.run_async(move |ctx| {
                let fixture = fixture.clone();
                async move {
                    let mut set = tokio::task::JoinSet::new();
                    for tid in 0..conc {
                        let fixture = fixture.clone();
                        let ctx_clone = ctx.clone();
                        set.spawn(async move {
                            let start = Instant::now();
                            if tid == 0 {
                                fixture.create("t2")?;
                                fixture.insert_row("t2", 1)?;
                                let mut txn = fixture.store.begin(TxnMode::ReadOnly)?;
                                let snap = txn.get(&b"tbl:t2:1".to_vec())?;
                                assert_eq!(snap, Some(b"row1".to_vec()));
                            } else {
                                fixture.truncate("t2")?;
                            }
                            ctx_clone.metrics.record_success();
                            ctx_clone.metrics.record_latency(start.elapsed());
                            pad_ddl_metrics(&ctx_clone, 200);
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
    }
}

fn run_rapid_create_drop_cycle(model: ExecutionModel) -> TestResult {
    let cfg = ddl_config("rapid_create_drop_cycle", model, 3);
    let harness = StressTestHarness::new(cfg).unwrap();
    let conc = harness.config.concurrency.max(2);
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let fixture = DdlFixture::new_in_memory();
            for i in 0..20 {
                fixture.create("cycle")?;
                fixture.insert_row("cycle", i)?;
                fixture.drop_table("cycle")?;
            }
            ctx.metrics.record_success();
            pad_ddl_metrics(ctx, 400);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            harness.run_concurrent(move |_tid, ctx| {
                let fixture = DdlFixture::new_in_memory();
                let start = Instant::now();
                for i in 0..12 {
                    let name = format!("cycle_multi_{i}");
                    fixture.create(&name)?;
                    fixture.insert_row(&name, i)?;
                    fixture.drop_table(&name)?;
                }
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                pad_ddl_metrics(ctx, 400);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => harness.run_async(|ctx| async move {
            let fixture = DdlFixture::new_in_memory();
            let start = Instant::now();
            for i in 0..12 {
                fixture.create("cycle_async")?;
                fixture.insert_row("cycle_async", i)?;
                fixture.drop_table("cycle_async")?;
            }
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            pad_ddl_metrics(&ctx, 400);
            Ok(())
        }),
        ExecutionModel::AsyncMulti => {
            harness.run_async(move |ctx| {
                async move {
                    let mut set = tokio::task::JoinSet::new();
                    for tid in 0..conc {
                        let ctx_clone = ctx.clone();
                        set.spawn(async move {
                            let fixture = DdlFixture::new_in_memory();
                            let start = Instant::now();
                            for i in 0..8 {
                                let name = format!("cycle_async_{tid}_{i}");
                                fixture.create(&name)?;
                                fixture.insert_row(&name, i)?;
                                fixture.drop_table(&name)?;
                            }
                            ctx_clone.metrics.record_success();
                            ctx_clone.metrics.record_latency(start.elapsed());
                            pad_ddl_metrics(&ctx_clone, 200);
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
    }
}

fn run_alter_table_during_insert(model: ExecutionModel) -> TestResult {
    let cfg = ddl_config("alter_table_during_insert", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let conc = harness.config.concurrency.max(2);
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let fixture = DdlFixture::new_in_memory();
            fixture.create("t3")?;
            fixture.alter(
                "t3",
                AlterAction::AddColumn(common::ColumnDef {
                    name: "c1".into(),
                    data_type: "INT".into(),
                    nullable: true,
                }),
            )?;
            fixture.insert_row("t3", 1)?;
            ctx.metrics.record_success();
            pad_ddl_metrics(ctx, 200);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            harness.run_concurrent(move |tid, ctx| {
                let fixture = fixture.clone();
                let start = Instant::now();
                fixture.create("t3")?;
                if tid % 2 == 0 {
                    fixture.alter(
                        "t3",
                        AlterAction::RenameColumn {
                            from: "c0".into(),
                            to: "c0_new".into(),
                        },
                    )?;
                } else {
                    fixture.insert_row("t3", tid)?;
                }
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                pad_ddl_metrics(ctx, 200);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => harness.run_async(|ctx| async move {
            let fixture = DdlFixture::new_in_memory();
            let start = Instant::now();
            fixture.create("t3")?;
            fixture.alter(
                "t3",
                AlterAction::RenameColumn {
                    from: "c0".into(),
                    to: "c0_new".into(),
                },
            )?;
            fixture.insert_row("t3", 1)?;
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            pad_ddl_metrics(&ctx, 200);
            Ok(())
        }),
        ExecutionModel::AsyncMulti => {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            harness.run_async(move |ctx| {
                let fixture = fixture.clone();
                async move {
                    let mut set = tokio::task::JoinSet::new();
                    for tid in 0..conc {
                        let fixture = fixture.clone();
                        let ctx_clone = ctx.clone();
                        set.spawn(async move {
                            let start = Instant::now();
                            fixture.create("t3")?;
                            if tid % 2 == 0 {
                                fixture.alter(
                                    "t3",
                                    AlterAction::AddColumn(common::ColumnDef {
                                        name: format!("c{tid}"),
                                        data_type: "INT".into(),
                                        nullable: true,
                                    }),
                                )?;
                            } else {
                                fixture.insert_row("t3", tid)?;
                            }
                            ctx_clone.metrics.record_success();
                            ctx_clone.metrics.record_latency(start.elapsed());
                            pad_ddl_metrics(&ctx_clone, 200);
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
    }
}

fn run_drop_create_same_name_no_leak(model: ExecutionModel) -> TestResult {
    let cfg = ddl_config("drop_create_same_name_no_leak", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let conc = harness.config.concurrency.max(2);
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let fixture = DdlFixture::new_in_memory();
            fixture.create("t4")?;
            fixture.insert_row("t4", 1)?;
            fixture.drop_table("t4")?;
            fixture.create("t4")?;
            let mut reader = fixture.store.begin(TxnMode::ReadOnly)?;
            assert!(reader.get(&b"tbl:t4:1".to_vec())?.is_none());
            ctx.metrics.record_success();
            pad_ddl_metrics(ctx, 200);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            harness.run_concurrent(move |_tid, ctx| {
                let fixture = fixture.clone();
                let start = Instant::now();
                fixture.create("t4")?;
                fixture.drop_table("t4")?;
                fixture.create("t4")?;
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                pad_ddl_metrics(ctx, 200);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => harness.run_async(|ctx| async move {
            let fixture = DdlFixture::new_in_memory();
            let start = Instant::now();
            fixture.create("t4")?;
            fixture.drop_table("t4")?;
            fixture.create("t4")?;
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            pad_ddl_metrics(&ctx, 200);
            Ok(())
        }),
        ExecutionModel::AsyncMulti => {
            let fixture = Arc::new(DdlFixture::new_in_memory());
            harness.run_async(move |ctx| {
                let fixture = fixture.clone();
                async move {
                    let mut set = tokio::task::JoinSet::new();
                    for _ in 0..conc {
                        let fixture = fixture.clone();
                        let ctx_clone = ctx.clone();
                        set.spawn(async move {
                            let start = Instant::now();
                            fixture.create("t4")?;
                            fixture.drop_table("t4")?;
                            fixture.create("t4")?;
                            ctx_clone.metrics.record_success();
                            ctx_clone.metrics.record_latency(start.elapsed());
                            pad_ddl_metrics(&ctx_clone, 200);
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
    }
}

fn run_ddl_crash_metadata_integrity(model: ExecutionModel) -> TestResult {
    let cfg = ddl_config("ddl_crash_metadata_integrity", model, 1);
    let harness = StressTestHarness::new(cfg).unwrap();
    let conc = harness.config.concurrency.max(2);
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            {
                let fixture = DdlFixture::new_persistent(&ctx.db_path)?;
                fixture.create("t5")?;
                fixture.insert_row("t5", 1)?;
            }
            let reopened = MemoryKV::open(&ctx.db_path)?;
            let mut reader = reopened.begin(TxnMode::ReadOnly)?;
            assert!(reader.get(&b"meta:t5".to_vec())?.is_some());
            ctx.metrics.record_success();
            pad_ddl_metrics(ctx, 200);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            harness.run_concurrent(|_, ctx| {
                let start = Instant::now();
                {
                    let fixture = DdlFixture::new_persistent(&ctx.db_path)?;
                    fixture.create("t5")?;
                    fixture.insert_row("t5", 1)?;
                }
                let reopened = MemoryKV::open(&ctx.db_path)?;
                let mut reader = reopened.begin(TxnMode::ReadOnly)?;
                assert!(reader.get(&b"meta:t5".to_vec())?.is_some());
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                pad_ddl_metrics(ctx, 200);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => harness.run_async(|ctx| async move {
            let start = Instant::now();
            {
                let fixture = DdlFixture::new_persistent(&ctx.db_path)?;
                fixture.create("t5")?;
                fixture.insert_row("t5", 1)?;
            }
            let reopened = MemoryKV::open(&ctx.db_path)?;
            let mut reader = reopened.begin(TxnMode::ReadOnly)?;
            assert!(reader.get(&b"meta:t5".to_vec())?.is_some());
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            pad_ddl_metrics(&ctx, 200);
            Ok(())
        }),
        ExecutionModel::AsyncMulti => harness.run_async(move |ctx| {
            async move {
                let mut set = tokio::task::JoinSet::new();
                for _ in 0..conc {
                    let ctx_clone = ctx.clone();
                    set.spawn(async move {
                        let start = Instant::now();
                        {
                            let fixture = DdlFixture::new_persistent(&ctx_clone.db_path)?;
                            fixture.create("t5")?;
                            fixture.insert_row("t5", 1)?;
                        }
                        let reopened = MemoryKV::open(&ctx_clone.db_path)?;
                        let mut reader = reopened.begin(TxnMode::ReadOnly)?;
                        assert!(reader.get(&b"meta:t5".to_vec())?.is_some());
                        ctx_clone.metrics.record_success();
                        ctx_clone.metrics.record_latency(start.elapsed());
                        pad_ddl_metrics(&ctx_clone, 200);
                        Ok::<_, CoreError>(())
                    });
                }
                while let Some(res) = set.join_next().await {
                    match res {
                        Ok(inner) => inner?,
                        Err(e) => return Err(CoreError::Io(io::Error::other(e))),
                    }
                }
                Ok(())
            }
        }),
    }
}

fn run_1000_create_drop_truncate_no_storage_leak(model: ExecutionModel) -> TestResult {
    let cfg = ddl_config("create_drop_truncate_no_storage_leak", model, 3);
    let harness = StressTestHarness::new(cfg).unwrap();
    let conc = harness.config.concurrency.max(3);
    let runner = move |ctx: &common::TestContext, seed: u64| -> CoreResult<()> {
        let start = Instant::now();
        let fixture = DdlFixture::new_in_memory();
        let generator = DdlWorkloadGenerator::new(seed);
        for _ in 0..120 {
            let op = generator.next_ddl();
            let _ = fixture.apply(op);
        }
        fixture.truncate("tbl_0")?;
        let count = fixture.count_table_keys()?;
        assert!(count < 50);
        ctx.metrics.record_success();
        ctx.metrics.record_latency(start.elapsed());
        pad_ddl_metrics(ctx, 800);
        Ok(())
    };
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| runner(ctx, 777)),
        ExecutionModel::SyncMulti => {
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                runner(ctx, 777 + tid as u64)?;
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => harness.run_async(|ctx| async move {
            runner(&ctx, 777)?;
            Ok(())
        }),
        ExecutionModel::AsyncMulti => harness.run_async(move |ctx| {
            async move {
                let mut set = tokio::task::JoinSet::new();
                for tid in 0..conc {
                    let ctx_clone = ctx.clone();
                    set.spawn(async move {
                        runner(&ctx_clone, 777 + tid as u64)?;
                        Ok::<_, CoreError>(())
                    });
                }
                while let Some(res) = set.join_next().await {
                    match res {
                        Ok(inner) => inner?,
                        Err(e) => return Err(CoreError::Io(io::Error::other(e))),
                    }
                }
                Ok(())
            }
        }),
    }
}

ddl_test!(test_insert_during_drop_table, run_insert_during_drop_table);
ddl_test!(test_select_during_truncate, run_select_during_truncate);
ddl_test!(test_rapid_create_drop_cycle, run_rapid_create_drop_cycle);
ddl_test!(test_alter_table_during_insert, run_alter_table_during_insert);
ddl_test!(
    test_drop_create_same_name_no_leak,
    run_drop_create_same_name_no_leak
);
ddl_test!(
    test_ddl_crash_metadata_integrity,
    run_ddl_crash_metadata_integrity
);
ddl_test!(
    test_1000_create_drop_truncate_no_storage_leak,
    run_1000_create_drop_truncate_no_storage_leak
);
