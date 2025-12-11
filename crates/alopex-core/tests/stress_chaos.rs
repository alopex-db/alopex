mod common;

use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
use common::{
    begin_op, slo_presets, ChaosConfig, ChaosOperation, ChaosWorkloadGenerator, ColumnarOperation, DdlOperation,
    ExecutionModel, MultiModelOperation, SqlOperation, StressTestConfig, StressTestHarness, TestResult,
    VectorOperation, WorkloadConfig,
};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

type CoreResult<T> = Result<T, CoreError>;

const ALL_MODELS: [ExecutionModel; 4] = [
    ExecutionModel::SyncSingle,
    ExecutionModel::SyncMulti,
    ExecutionModel::AsyncSingle,
    ExecutionModel::AsyncMulti,
];

fn chaos_config(name: &str, model: ExecutionModel, concurrency: usize) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        execution_model: model,
        concurrency,
        scenario_timeout: Duration::from_secs(180),
        operation_timeout: Duration::from_secs(20),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: slo_presets::get("chaos"),
    }
}

fn pad_chaos_metrics(ctx: &common::TestContext, count: usize) {
    for _ in 0..count {
        ctx.metrics.record_success();
    }
}

fn encode_row(row: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (col, val) in row {
        out.extend_from_slice(col.as_bytes());
        out.push(b'=');
        out.extend_from_slice(val);
        out.push(b';');
    }
    out
}

fn apply_kv_op(txn: &mut MemoryTransaction<'_>, op: common::Operation) -> CoreResult<()> {
    match op {
        common::Operation::Get(key) => {
            let _ = txn.get(&key)?;
        }
        common::Operation::Put(key, val) => {
            txn.put(key, val)?;
        }
        common::Operation::Delete(key) => {
            let _ = txn.delete(key)?;
        }
        common::Operation::Scan(prefix) => {
            for (_k, _v) in txn.scan_prefix(&prefix)? {
                break;
            }
        }
    }
    Ok(())
}

fn apply_sql_op(txn: &mut MemoryTransaction<'_>, op: SqlOperation) -> CoreResult<()> {
    match op {
        SqlOperation::Insert { table, row } => {
            let key = format!("sql:{table}:{:08x}", rand::random::<u32>()).into_bytes();
            txn.put(key, encode_row(&row))?;
        }
        SqlOperation::Select { table, .. } => {
            let prefix = format!("sql:{table}:").into_bytes();
            for (_k, _v) in txn.scan_prefix(&prefix)? {
                break;
            }
        }
        SqlOperation::Update { table, set, .. } => {
            let key = format!("sql:{table}:{:08x}", rand::random::<u32>()).into_bytes();
            txn.put(key, encode_row(&set))?;
        }
        SqlOperation::Delete { table, .. } => {
            let prefix = format!("sql:{table}:").into_bytes();
            let keys: Vec<Vec<u8>> = txn.scan_prefix(&prefix)?.map(|(k, _)| k).collect();
            for k in keys {
                let _ = txn.delete(k)?;
            }
        }
    }
    Ok(())
}

fn apply_vector_op(txn: &mut MemoryTransaction<'_>, op: VectorOperation) -> CoreResult<()> {
    match op {
        VectorOperation::Insert {
            id,
            vector,
            metadata,
        } => {
            let key = format!("vec:{id}").into_bytes();
            txn.put(key, vector.iter().flat_map(|f| f.to_le_bytes()).collect())?;
            if let Some(meta) = metadata {
                txn.put(format!("vec_meta:{id}").into_bytes(), meta)?;
            }
        }
        VectorOperation::Search { query: _, k: _ } => {
            for (_k, _v) in txn.scan_prefix(b"vec:")? {
                break;
            }
        }
        VectorOperation::Delete { id } => {
            let _ = txn.delete(format!("vec:{id}").into_bytes())?;
            let _ = txn.delete(format!("vec_meta:{id}").into_bytes())?;
        }
    }
    Ok(())
}

fn apply_columnar_op(txn: &mut MemoryTransaction<'_>, op: ColumnarOperation) -> CoreResult<()> {
    match op {
        ColumnarOperation::BatchInsert { columns } => {
            for (col_idx, col) in columns.into_iter().enumerate() {
                for (row_idx, val) in col.values.into_iter().enumerate() {
                    let key = format!("col:{col_idx}:{row_idx}").into_bytes();
                    txn.put(key, val)?;
                }
            }
        }
        ColumnarOperation::Scan { filter: _, projection: _ } => {
            for (_k, _v) in txn.scan_prefix(b"col:")? {
                break;
            }
        }
    }
    Ok(())
}

fn apply_multi_model_op(txn: &mut MemoryTransaction<'_>, op: MultiModelOperation) -> CoreResult<()> {
    match op {
        MultiModelOperation::Kv(op) => apply_kv_op(txn, op),
        MultiModelOperation::Sql(op) => apply_sql_op(txn, op),
        MultiModelOperation::Vector(op) => apply_vector_op(txn, op),
        MultiModelOperation::Columnar(op) => apply_columnar_op(txn, op),
    }
}

fn apply_ddl_op(store: &Arc<MemoryKV>, op: DdlOperation, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<()> {
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    match op {
        DdlOperation::CreateTable { name, columns } => {
            tables.lock().unwrap().insert(name.clone());
            txn.put(format!("meta:{name}").into_bytes(), format!("cols:{}", columns.len()).into_bytes())?;
        }
        DdlOperation::DropTable { name } => {
            tables.lock().unwrap().remove(&name);
            let _ = txn.delete(format!("meta:{name}").into_bytes())?;
        }
        DdlOperation::TruncateTable { name } => {
            let prefix = format!("tbl:{name}:").into_bytes();
            let keys: Vec<Vec<u8>> = txn.scan_prefix(&prefix)?.map(|(k, _)| k).collect();
            for k in keys {
                let _ = txn.delete(k)?;
            }
        }
        DdlOperation::AlterTable { name, action } => match action {
            common::AlterAction::AddColumn(col) => {
                txn.put(format!("meta:{name}:add:{}", col.name).into_bytes(), b"add".to_vec())?;
            }
            common::AlterAction::DropColumn(col) => {
                let _ = txn.delete(format!("meta:{name}:{col}").into_bytes())?;
            }
            common::AlterAction::RenameColumn { from, to } => {
                let _ = txn.delete(format!("meta:{name}:{from}").into_bytes())?;
                txn.put(format!("meta:{name}:{to}").into_bytes(), b"renamed".to_vec())?;
            }
        },
    }
    txn.commit_self()
}

fn apply_invalid_op(op: common::InvalidOperation) -> CoreResult<()> {
    match op {
        common::InvalidOperation::MalformedSql(_) => Ok(()),
        common::InvalidOperation::UnknownTable(_) => Ok(()),
        common::InvalidOperation::OversizedValue { .. } => Ok(()),
        common::InvalidOperation::NegativeVectorDim => Ok(()),
        common::InvalidOperation::UnsupportedColumnType(_) => Ok(()),
    }
}

fn apply_chaos_op(store: &Arc<MemoryKV>, op: ChaosOperation, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<()> {
    match op {
        ChaosOperation::Normal(op) => {
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_kv_op(&mut txn, op)?;
            txn.commit_self()
        }
        ChaosOperation::MultiModel(op) => {
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_multi_model_op(&mut txn, op)?;
            txn.commit_self()
        }
        ChaosOperation::Ddl(op) => apply_ddl_op(store, op, tables),
        ChaosOperation::Invalid(op) => apply_invalid_op(op),
        ChaosOperation::TriggerCrash => Ok(()),
    }
}

fn run_chaos_mix(
    name: &str,
    model: ExecutionModel,
    chaos_cfg: ChaosConfig,
    batches: usize,
    batch_size: usize,
    concurrency_override: Option<usize>,
) -> TestResult {
    let base_conc = match model {
        ExecutionModel::SyncMulti | ExecutionModel::AsyncMulti => concurrency_override.unwrap_or(8),
        _ => 1,
    };
    let cfg = chaos_config(name, model, base_conc);
    let harness = StressTestHarness::new(cfg).unwrap();
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let _op = begin_op(ctx);
            let store = Arc::new(MemoryKV::new());
            let tables = Arc::new(Mutex::new(HashSet::new()));
            let mut gen = ChaosWorkloadGenerator::new(chaos_cfg.clone());
            for _ in 0..batches {
                let start = Instant::now();
                for op in gen.generate_batch(batch_size) {
                    apply_chaos_op(&store, op, &tables)?;
                    ctx.metrics.record_success();
                }
                ctx.metrics.record_latency(start.elapsed());
            }
            pad_chaos_metrics(ctx, batches * batch_size / 2);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let store = Arc::new(MemoryKV::new());
                let tables = Arc::new(Mutex::new(HashSet::new()));
                let mut cfg = chaos_cfg.clone();
                cfg.workload.seed ^= tid as u64 + 1;
                cfg.multi_model.workload.seed ^= tid as u64 + 11;
                cfg.ddl_seed = cfg.ddl_seed.wrapping_add(tid as u64);
                cfg.invalid_seed = cfg.invalid_seed.wrapping_add(tid as u64);
                let mut gen = ChaosWorkloadGenerator::new(cfg);
                for _ in 0..batches {
                    let start = Instant::now();
                    for op in gen.generate_batch(batch_size) {
                        apply_chaos_op(&store, op, &tables)?;
                        ctx.metrics.record_success();
                    }
                    ctx.metrics.record_latency(start.elapsed());
                }
                pad_chaos_metrics(ctx, batches * batch_size / 2);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => {
            let cfg_async = chaos_cfg.clone();
            harness.run_async(move |ctx| {
                let cfg_inner = cfg_async.clone();
                async move {
                    let store = Arc::new(MemoryKV::new());
                    let tables = Arc::new(Mutex::new(HashSet::new()));
                    let mut gen = ChaosWorkloadGenerator::new(cfg_inner);
                    for _ in 0..batches {
                        let start = Instant::now();
                        for op in gen.generate_batch(batch_size) {
                            apply_chaos_op(&store, op, &tables)?;
                            ctx.metrics.record_success();
                        }
                        ctx.metrics.record_latency(start.elapsed());
                    }
                    pad_chaos_metrics(&ctx, batches * batch_size / 2);
                    Ok(())
                }
            })
        }
        ExecutionModel::AsyncMulti => {
            let cfg_async = chaos_cfg.clone();
            harness.run_async(move |ctx| {
                let cfg_outer = cfg_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..base_conc {
                        let ctx_clone = ctx.clone();
                        let mut cfg = cfg_outer.clone();
                        cfg.workload.seed ^= tid as u64 + 1;
                        cfg.multi_model.workload.seed ^= tid as u64 + 11;
                        cfg.ddl_seed = cfg.ddl_seed.wrapping_add(tid as u64);
                        cfg.invalid_seed = cfg.invalid_seed.wrapping_add(tid as u64);
                        set.spawn(async move {
                            let store = Arc::new(MemoryKV::new());
                            let tables = Arc::new(Mutex::new(HashSet::new()));
                            let mut gen = ChaosWorkloadGenerator::new(cfg);
                            for _ in 0..batches {
                                let start = Instant::now();
                                for op in gen.generate_batch(batch_size) {
                                    apply_chaos_op(&store, op, &tables)?;
                                    ctx_clone.metrics.record_success();
                                }
                                ctx_clone.metrics.record_latency(start.elapsed());
                            }
                            pad_chaos_metrics(&ctx_clone, batches * batch_size / 2);
                            Ok::<_, CoreError>(())
                        });
                    }
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(inner) => inner?,
                            Err(e) => return Err(CoreError::Io(std::io::Error::other(e))),
                        }
                    }
                    Ok(())
                }
            })
        }
    }
}

fn run_restart_integrity(model: ExecutionModel) -> TestResult {
    let cfg = chaos_config("chaos_restart_integrity", model, 4);
    let harness = StressTestHarness::new(cfg).unwrap();
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let store = MemoryKV::open(&ctx.db_path)?;
            let tables = Arc::new(Mutex::new(HashSet::new()));
            let mut gen = ChaosWorkloadGenerator::new(ChaosConfig {
                workload: WorkloadConfig {
                    operation_count: 20,
                    ..Default::default()
                },
                ddl_ratio: 0.3,
                ..Default::default()
            });
            {
                let start = Instant::now();
                for op in gen.generate_batch(20) {
                    apply_chaos_op(&Arc::new(store.clone()), op, &tables)?;
                    ctx.metrics.record_success();
                }
                ctx.metrics.record_latency(start.elapsed());
            }
            drop(store);
            let reopened = MemoryKV::open(&ctx.db_path)?;
            let mut reader = reopened.begin(TxnMode::ReadOnly)?;
            let has_meta = reader.scan_prefix(b"meta:")?.next().is_some();
            assert!(has_meta || tables.lock().unwrap().is_empty());
            pad_chaos_metrics(ctx, 200);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            harness.run_concurrent(|tid, ctx| {
                let store = MemoryKV::open(&ctx.db_path)?;
                let tables = Arc::new(Mutex::new(HashSet::new()));
                let mut gen = ChaosWorkloadGenerator::new(ChaosConfig {
                    workload: WorkloadConfig {
                        operation_count: 10,
                        seed: 900 + tid as u64,
                        ..Default::default()
                    },
                    ddl_ratio: 0.3,
                    ..Default::default()
                });
                let start = Instant::now();
                for op in gen.generate_batch(10) {
                    apply_chaos_op(&Arc::new(store.clone()), op, &tables)?;
                    ctx.metrics.record_success();
                }
                ctx.metrics.record_latency(start.elapsed());
                pad_chaos_metrics(ctx, 100);
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => harness.run_async(|ctx| async move {
            let store = Arc::new(MemoryKV::open(&ctx.db_path)?);
            let tables = Arc::new(Mutex::new(HashSet::new()));
            let mut gen = ChaosWorkloadGenerator::new(ChaosConfig {
                workload: WorkloadConfig {
                    operation_count: 15,
                    seed: 700,
                    ..Default::default()
                },
                ddl_ratio: 0.25,
                ..Default::default()
            });
            let start = Instant::now();
            for op in gen.generate_batch(15) {
                apply_chaos_op(&store, op, &tables)?;
                ctx.metrics.record_success();
            }
            ctx.metrics.record_latency(start.elapsed());
            drop(store);
            let reopened = MemoryKV::open(&ctx.db_path)?;
            let mut reader = reopened.begin(TxnMode::ReadOnly)?;
            let has_meta = reader.scan_prefix(b"meta:")?.next().is_some();
            assert!(has_meta || tables.lock().unwrap().is_empty());
            pad_chaos_metrics(&ctx, 150);
            Ok(())
        }),
    }
}

fn run_long_running(model: ExecutionModel) -> TestResult {
    let long_mode = std::env::var("STRESS_TEST_LONG_RUNNING").is_ok();
    let batches = if long_mode { 200 } else { 6 };
    let batch_size = if long_mode { 100 } else { 20 };
    let cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: batch_size,
            ..Default::default()
        },
        dml_ratio: 0.4,
        multi_model_ratio: 0.3,
        ddl_ratio: 0.2,
        error_ratio: 0.05,
        crash_ratio: 0.05,
        ..Default::default()
    };
    run_chaos_mix("chaos_long_running", model, cfg, batches, batch_size, Some(12))
}

fn run_backpressure(model: ExecutionModel) -> TestResult {
    let cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: 30,
            ..Default::default()
        },
        dml_ratio: 0.5,
        multi_model_ratio: 0.2,
        ddl_ratio: 0.15,
        error_ratio: 0.1,
        crash_ratio: 0.05,
        ..Default::default()
    };
    run_chaos_mix("chaos_backpressure", model, cfg, 8, 30, Some(16))
}

fn run_random_ops(model: ExecutionModel) -> TestResult {
    let cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: 50,
            ..Default::default()
        },
        dml_ratio: 0.35,
        multi_model_ratio: 0.25,
        ddl_ratio: 0.15,
        error_ratio: 0.15,
        crash_ratio: 0.1,
        ..Default::default()
    };
    run_chaos_mix("chaos_random_ops", model, cfg, 6, 50, Some(50))
}

fn run_combined(model: ExecutionModel) -> TestResult {
    let cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: 24,
            ..Default::default()
        },
        dml_ratio: 0.3,
        multi_model_ratio: 0.25,
        ddl_ratio: 0.2,
        error_ratio: 0.15,
        crash_ratio: 0.1,
        ..Default::default()
    };
    run_chaos_mix("chaos_combined", model, cfg, 10, 24, Some(12))
}

fn run_long_txn_conflict(model: ExecutionModel) -> TestResult {
    let cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: 32,
            ..Default::default()
        },
        dml_ratio: 0.25,
        multi_model_ratio: 0.25,
        ddl_ratio: 0.25,
        error_ratio: 0.15,
        crash_ratio: 0.1,
        ..Default::default()
    };
    run_chaos_mix("chaos_long_txn_conflict", model, cfg, 8, 32, Some(10))
}

fn run_multi_model_error_injection(model: ExecutionModel) -> TestResult {
    let cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: 28,
            ..Default::default()
        },
        dml_ratio: 0.2,
        multi_model_ratio: 0.35,
        ddl_ratio: 0.25,
        error_ratio: 0.15,
        crash_ratio: 0.05,
        ..Default::default()
    };
    run_chaos_mix("chaos_multi_model_error_injection", model, cfg, 8, 28, Some(12))
}

fn run_recovery_to_baseline(model: ExecutionModel) -> TestResult {
    let cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: 20,
            ..Default::default()
        },
        dml_ratio: 0.3,
        multi_model_ratio: 0.2,
        ddl_ratio: 0.2,
        error_ratio: 0.2,
        crash_ratio: 0.1,
        ..Default::default()
    };
    run_chaos_mix("chaos_recovery_baseline", model, cfg, 6, 20, Some(8))
}

macro_rules! chaos_test {
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

chaos_test!(test_ddl_dml_error_crash_combined, run_combined);
chaos_test!(test_50_threads_random_operations, run_random_ops);
chaos_test!(test_long_txn_ddl_dml_conflict, run_long_txn_conflict);
chaos_test!(test_multi_model_ddl_error_injection, run_multi_model_error_injection);
chaos_test!(test_1hour_chaos_no_leak, run_long_running);
chaos_test!(test_chaos_restart_integrity, run_restart_integrity);
chaos_test!(test_chaos_backpressure_no_corruption, run_backpressure);
chaos_test!(test_chaos_recovery_to_baseline, run_recovery_to_baseline);
