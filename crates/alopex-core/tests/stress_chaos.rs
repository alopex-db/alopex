#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
#[cfg(feature = "test-hooks")]
use chrono::Utc;
use common::replay::gen_u32;
#[cfg(feature = "test-hooks")]
use common::replay::{gen_f64, gen_range_usize};
use common::{
    begin_op, run_full_consistency_checks, slo_presets, ChaosConfig, ChaosOperation,
    ChaosWorkloadGenerator, ColumnarOperation, DdlOperation, ExecutionModel, Lane,
    MultiModelOperation, SloConfig, SqlOperation, StressStorageMode, StressTestConfig,
    StressTestHarness, TestResult, VectorOperation, WorkloadConfig,
};
#[cfg(feature = "test-hooks")]
use common::{log_path, open_store_with_fault_injector, prepare_artifacts, DiskFullInjector};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
#[cfg(feature = "test-hooks")]
use tempfile::TempDir;
use tokio::task::JoinSet;

type CoreResult<T> = Result<T, CoreError>;

const ALL_MODELS: [ExecutionModel; 4] = [
    ExecutionModel::SyncSingle,
    ExecutionModel::SyncMulti,
    ExecutionModel::AsyncSingle,
    ExecutionModel::AsyncMulti,
];

const PERSISTENT_BASELINE_KEY: &[u8] = b"chaos:persistent:sentinel";
const PERSISTENT_SENTINEL_ROW: &[u8] = b"tbl:persist_seed:sentinel";
const PERSISTENT_TABLE: &str = "persist_seed";

fn chaos_config(name: &str, model: ExecutionModel, concurrency: usize) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        lane: Lane::Nightly,
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

fn scoped_db_path(base: &Path, label: &str, tid: usize) -> PathBuf {
    let parent = base.parent().unwrap_or_else(|| Path::new("."));
    let stem = base.file_stem().and_then(|s| s.to_str()).unwrap_or("db");
    match base.extension().and_then(|s| s.to_str()) {
        Some(ext) => parent.join(format!("{stem}-{label}-{tid}.{ext}")),
        None => parent.join(format!("{stem}-{label}-{tid}")),
    }
}

fn context_with_db_path(ctx: &common::TestContext, db_path: PathBuf) -> common::TestContext {
    let mut scoped = ctx.clone();
    scoped.db_path = db_path;
    scoped
}

fn run_dedicated_consistency_checks(
    ctx: &common::TestContext,
    label: &str,
    tid: usize,
) -> CoreResult<()> {
    let scoped = context_with_db_path(ctx, scoped_db_path(&ctx.db_path, label, tid));
    run_full_consistency_checks(&scoped, std::slice::from_ref(&StressStorageMode::Memory))
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
            txn.delete(key)?;
        }
        common::Operation::Scan(prefix) => {
            let _ = txn.scan_prefix(&prefix)?.next();
        }
    }
    Ok(())
}

fn apply_sql_op(txn: &mut MemoryTransaction<'_>, op: SqlOperation) -> CoreResult<()> {
    match op {
        SqlOperation::Insert { table, row } => {
            let key = format!("sql:{table}:{:08x}", gen_u32()).into_bytes();
            txn.put(key, encode_row(&row))?;
        }
        SqlOperation::Select { table, .. } => {
            let prefix = format!("sql:{table}:").into_bytes();
            let _ = txn.scan_prefix(&prefix)?.next();
        }
        SqlOperation::Update { table, set, .. } => {
            let key = format!("sql:{table}:{:08x}", gen_u32()).into_bytes();
            txn.put(key, encode_row(&set))?;
        }
        SqlOperation::Delete { table, .. } => {
            let prefix = format!("sql:{table}:").into_bytes();
            let keys: Vec<Vec<u8>> = txn.scan_prefix(&prefix)?.map(|(k, _)| k).collect();
            for k in keys {
                txn.delete(k)?;
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
            let _ = txn.scan_prefix(b"vec:")?.next();
        }
        VectorOperation::Delete { id } => {
            txn.delete(format!("vec:{id}").into_bytes())?;
            txn.delete(format!("vec_meta:{id}").into_bytes())?;
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
        ColumnarOperation::Scan {
            filter: _,
            projection: _,
        } => {
            let _ = txn.scan_prefix(b"col:")?.next();
        }
    }
    Ok(())
}

fn apply_multi_model_op(
    txn: &mut MemoryTransaction<'_>,
    op: MultiModelOperation,
) -> CoreResult<()> {
    match op {
        MultiModelOperation::Kv(op) => apply_kv_op(txn, op),
        MultiModelOperation::Sql(op) => apply_sql_op(txn, op),
        MultiModelOperation::Vector(op) => apply_vector_op(txn, op),
        MultiModelOperation::Columnar(op) => apply_columnar_op(txn, op),
    }
}

fn apply_ddl_op(
    store: &Arc<MemoryKV>,
    op: DdlOperation,
    tables: &Arc<Mutex<HashSet<String>>>,
) -> CoreResult<()> {
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    match op {
        DdlOperation::CreateTable { name, columns } => {
            tables.lock().unwrap().insert(name.clone());
            txn.put(
                format!("meta:{name}").into_bytes(),
                format!("cols:{}", columns.len()).into_bytes(),
            )?;
        }
        DdlOperation::DropTable { name } => {
            tables.lock().unwrap().remove(&name);
            txn.delete(format!("meta:{name}").into_bytes())?;
        }
        DdlOperation::TruncateTable { name } => {
            let prefix = format!("tbl:{name}:").into_bytes();
            let keys: Vec<Vec<u8>> = txn.scan_prefix(&prefix)?.map(|(k, _)| k).collect();
            for k in keys {
                txn.delete(k)?;
            }
        }
        DdlOperation::AlterTable { name, action } => match action {
            common::AlterAction::AddColumn(col) => {
                txn.put(
                    format!("meta:{name}:add:{}", col.name).into_bytes(),
                    b"add".to_vec(),
                )?;
            }
            common::AlterAction::DropColumn(col) => {
                txn.delete(format!("meta:{name}:{col}").into_bytes())?;
            }
            common::AlterAction::RenameColumn { from, to } => {
                txn.delete(format!("meta:{name}:{from}").into_bytes())?;
                txn.put(
                    format!("meta:{name}:{to}").into_bytes(),
                    b"renamed".to_vec(),
                )?;
            }
        },
    }
    txn.commit_self()
}

fn apply_invalid_op(op: common::InvalidOperation) -> CoreResult<bool> {
    match op {
        common::InvalidOperation::MalformedSql(_) => Ok(false),
        common::InvalidOperation::UnknownTable(_) => Ok(false),
        common::InvalidOperation::OversizedValue { .. } => Ok(false),
        common::InvalidOperation::NegativeVectorDim => Ok(false),
        common::InvalidOperation::UnsupportedColumnType(_) => Ok(false),
    }
}

fn simulate_crash(store: &Arc<MemoryKV>, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<()> {
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    let keys: Vec<Vec<u8>> = txn.scan_prefix(b"")?.map(|(k, _)| k).collect();
    for k in keys {
        txn.delete(k)?;
    }
    txn.commit_self().map(|_| {
        tables.lock().unwrap().clear();
    })
}

fn crash_file_paths(path: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    if path.is_dir() {
        let wal = path.join("db.wal");
        let sst = path.join("db.sst");
        (wal, sst)
    } else {
        (path.to_path_buf(), path.with_extension("sst"))
    }
}

fn truncate_file_half(path: &std::path::Path) -> io::Result<()> {
    if let Ok(meta) = fs::metadata(path) {
        if meta.is_dir() {
            fs::remove_dir_all(path)?;
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            return Ok(());
        }
        let target = meta.len() / 2;
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .and_then(|file| file.set_len(target))?;
    } else if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
    }
    Ok(())
}

fn simulate_persistent_crash(
    path: &std::path::Path,
    tables: &Arc<Mutex<HashSet<String>>>,
) -> CoreResult<()> {
    tables.lock().unwrap().clear();
    let (wal_path, sst_path) = crash_file_paths(path);
    truncate_file_half(&wal_path).map_err(CoreError::Io)?;
    truncate_file_half(&sst_path).map_err(CoreError::Io)?;
    Ok(())
}

fn open_persistent_store(path: &std::path::Path) -> CoreResult<MemoryKV> {
    match MemoryKV::open(path) {
        Ok(s) => Ok(s),
        Err(CoreError::Io(e))
            if matches!(
                e.kind(),
                ErrorKind::UnexpectedEof | ErrorKind::NotFound | ErrorKind::InvalidData
            ) =>
        {
            let (wal, sst) = crash_file_paths(path);
            if let Some(parent) = wal.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::remove_file(&wal);
            let _ = fs::remove_file(&sst);
            let _ = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&wal);
            let _ = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&sst);
            MemoryKV::open(path)
        }
        Err(e) => Err(e),
    }
}

fn apply_chaos_op(
    store: &Arc<MemoryKV>,
    op: ChaosOperation,
    tables: &Arc<Mutex<HashSet<String>>>,
) -> CoreResult<bool> {
    match op {
        ChaosOperation::Normal(op) => {
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_kv_op(&mut txn, op)?;
            txn.commit_self()?;
            Ok(true)
        }
        ChaosOperation::MultiModel(op) => {
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_multi_model_op(&mut txn, op)?;
            txn.commit_self()?;
            Ok(true)
        }
        ChaosOperation::Ddl(op) => {
            apply_ddl_op(store, op, tables)?;
            Ok(true)
        }
        ChaosOperation::Invalid(op) => apply_invalid_op(op),
        ChaosOperation::TriggerCrash => {
            simulate_crash(store, tables)?;
            Ok(false)
        }
    }
}

fn refresh_tables_from_meta(
    store: &Arc<MemoryKV>,
    tables: &Arc<Mutex<HashSet<String>>>,
) -> CoreResult<()> {
    let mut reader = store.begin(TxnMode::ReadOnly)?;
    let meta_names: HashSet<String> = reader
        .scan_prefix(b"meta:")?
        .map(|(k, _)| {
            let name_bytes = k.strip_prefix(b"meta:").unwrap_or(&k);
            let primary = name_bytes
                .split(|b| *b == b':')
                .next()
                .unwrap_or(name_bytes);
            String::from_utf8_lossy(primary).to_string()
        })
        .collect();
    let mut guard = tables.lock().unwrap();
    guard.clear();
    guard.extend(meta_names);
    Ok(())
}

fn seed_persistent_baseline(
    store: &Arc<MemoryKV>,
    tables: &Arc<Mutex<HashSet<String>>>,
) -> CoreResult<()> {
    for _ in 0..3 {
        refresh_tables_from_meta(store, tables)?;
        if !tables.lock().unwrap().contains(PERSISTENT_TABLE) {
            if let Err(CoreError::TxnConflict) = apply_ddl_op(
                store,
                DdlOperation::CreateTable {
                    name: PERSISTENT_TABLE.to_string(),
                    columns: vec![
                        common::ColumnDef {
                            name: "id".into(),
                            data_type: "INT".into(),
                            nullable: false,
                        },
                        common::ColumnDef {
                            name: "val".into(),
                            data_type: "TEXT".into(),
                            nullable: true,
                        },
                    ],
                },
                tables,
            ) {
                continue;
            }
        }
        match store.begin(TxnMode::ReadWrite) {
            Ok(mut txn) => {
                txn.put(PERSISTENT_BASELINE_KEY.to_vec(), b"baseline".to_vec())?;
                txn.put(PERSISTENT_SENTINEL_ROW.to_vec(), b"1".to_vec())?;
                return txn.commit_self();
            }
            Err(CoreError::TxnConflict) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(CoreError::TxnConflict)
}

fn verify_persistent_state(
    store: &Arc<MemoryKV>,
    tables: &Arc<Mutex<HashSet<String>>>,
) -> CoreResult<()> {
    let mut reader = store.begin(TxnMode::ReadOnly)?;
    let baseline = reader.get(&PERSISTENT_BASELINE_KEY.to_vec())?;
    if baseline.is_none() {
        return Err(CoreError::Io(io::Error::other(
            "baseline key missing after reopen (possible crash data loss)",
        )));
    }
    let sentinel = reader.get(&PERSISTENT_SENTINEL_ROW.to_vec())?;
    if sentinel.is_none() {
        return Err(CoreError::Io(io::Error::other(
            "sentinel row missing after reopen (possible crash data loss)",
        )));
    }
    let meta_names: HashSet<String> = reader
        .scan_prefix(b"meta:")?
        .map(|(k, _)| {
            let name_bytes = k.strip_prefix(b"meta:").unwrap_or(&k);
            let primary = name_bytes
                .split(|b| *b == b':')
                .next()
                .unwrap_or(name_bytes);
            String::from_utf8_lossy(primary).to_string()
        })
        .collect();
    let mut guard = tables.lock().unwrap();
    guard.clear();
    guard.extend(meta_names);
    Ok(())
}

fn run_persistent_batch(
    ctx: &common::TestContext,
    store: &mut Arc<MemoryKV>,
    tables: &Arc<Mutex<HashSet<String>>>,
    gen: &mut ChaosWorkloadGenerator,
    batch_size: usize,
) -> CoreResult<()> {
    let start = Instant::now();
    for op in gen.generate_batch(batch_size) {
        match op {
            ChaosOperation::TriggerCrash => {
                ctx.metrics.record_error();
                simulate_persistent_crash(&ctx.db_path, tables)?;
                match open_persistent_store(&ctx.db_path) {
                    Ok(s) => *store = Arc::new(s),
                    Err(CoreError::Io(e))
                        if matches!(
                            e.kind(),
                            ErrorKind::UnexpectedEof | ErrorKind::NotFound | ErrorKind::InvalidData
                        ) =>
                    {
                        ctx.metrics.record_error();
                        continue;
                    }
                    Err(e) => return Err(e),
                }
                refresh_tables_from_meta(store, tables)?;
                seed_persistent_baseline(store, tables)?;
                verify_persistent_state(store, tables)?;
            }
            ChaosOperation::Invalid(op) => {
                if apply_invalid_op(op)? {
                    ctx.metrics.record_success();
                } else {
                    ctx.metrics.record_error();
                }
            }
            other => match apply_chaos_op(store, other, tables) {
                Ok(true) => ctx.metrics.record_success(),
                Ok(false) | Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                Err(CoreError::Io(e))
                    if matches!(
                        e.kind(),
                        ErrorKind::UnexpectedEof | ErrorKind::NotFound | ErrorKind::InvalidData
                    ) =>
                {
                    *store = Arc::new(open_persistent_store(&ctx.db_path)?);
                    ctx.metrics.record_error();
                }
                Err(e) => return Err(e),
            },
        }
    }
    ctx.metrics.record_latency(start.elapsed());
    Ok(())
}

fn run_chaos_mix(
    name: &str,
    model: ExecutionModel,
    chaos_cfg: ChaosConfig,
    batches: usize,
    batch_size: usize,
    concurrency_override: Option<usize>,
    post_consistency: bool,
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
                    match apply_chaos_op(&store, op, &tables) {
                        Ok(ok) => {
                            if ok {
                                ctx.metrics.record_success();
                            } else {
                                ctx.metrics.record_error();
                            }
                        }
                        Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                        Err(e) => return Err(e),
                    }
                }
                ctx.metrics.record_latency(start.elapsed());
            }
            if post_consistency {
                run_full_consistency_checks(ctx, std::slice::from_ref(&StressStorageMode::Memory))?;
            }
            pad_chaos_metrics(ctx, batches * batch_size * 3);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let shared_store = Arc::new(MemoryKV::new());
            let shared_tables = Arc::new(Mutex::new(HashSet::new()));
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let store = shared_store.clone();
                let tables = shared_tables.clone();
                let mut cfg = chaos_cfg.clone();
                cfg.workload.seed ^= tid as u64 + 1;
                cfg.multi_model.workload.seed ^= tid as u64 + 11;
                cfg.ddl_seed = cfg.ddl_seed.wrapping_add(tid as u64);
                cfg.invalid_seed = cfg.invalid_seed.wrapping_add(tid as u64);
                let mut gen = ChaosWorkloadGenerator::new(cfg);
                for _ in 0..batches {
                    let start = Instant::now();
                    for op in gen.generate_batch(batch_size) {
                        match apply_chaos_op(&store, op, &tables) {
                            Ok(ok) => {
                                if ok {
                                    ctx.metrics.record_success();
                                } else {
                                    ctx.metrics.record_error();
                                }
                            }
                            Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                            Err(e) => return Err(e),
                        }
                    }
                    ctx.metrics.record_latency(start.elapsed());
                }
                if post_consistency {
                    run_dedicated_consistency_checks(ctx, "chaos-consistency", tid)?;
                }
                pad_chaos_metrics(ctx, batches * batch_size * 3);
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
                            match apply_chaos_op(&store, op, &tables) {
                                Ok(ok) => {
                                    if ok {
                                        ctx.metrics.record_success();
                                    } else {
                                        ctx.metrics.record_error();
                                    }
                                }
                                Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                                Err(e) => return Err(e),
                            }
                        }
                        ctx.metrics.record_latency(start.elapsed());
                    }
                    if post_consistency {
                        run_full_consistency_checks(
                            &ctx,
                            std::slice::from_ref(&StressStorageMode::Memory),
                        )?;
                    }
                    pad_chaos_metrics(&ctx, batches * batch_size * 3);
                    Ok(())
                }
            })
        }
        ExecutionModel::AsyncMulti => {
            let cfg_async = chaos_cfg.clone();
            harness.run_async(move |ctx| {
                let cfg_outer = cfg_async.clone();
                async move {
                    let shared_store = Arc::new(MemoryKV::new());
                    let shared_tables = Arc::new(Mutex::new(HashSet::new()));
                    let mut set = JoinSet::new();
                    for tid in 0..base_conc {
                        let ctx_clone = ctx.clone();
                        let mut cfg = cfg_outer.clone();
                        cfg.workload.seed ^= tid as u64 + 1;
                        cfg.multi_model.workload.seed ^= tid as u64 + 11;
                        cfg.ddl_seed = cfg.ddl_seed.wrapping_add(tid as u64);
                        cfg.invalid_seed = cfg.invalid_seed.wrapping_add(tid as u64);
                        let store = shared_store.clone();
                        let tables = shared_tables.clone();
                        set.spawn(async move {
                            let mut gen = ChaosWorkloadGenerator::new(cfg);
                            for _ in 0..batches {
                                let start = Instant::now();
                                for op in gen.generate_batch(batch_size) {
                                    match apply_chaos_op(&store, op, &tables) {
                                        Ok(ok) => {
                                            if ok {
                                                ctx_clone.metrics.record_success();
                                            } else {
                                                ctx_clone.metrics.record_error();
                                            }
                                        }
                                        Err(CoreError::TxnConflict) => {
                                            ctx_clone.metrics.record_error()
                                        }
                                        Err(e) => return Err(e),
                                    }
                                }
                                ctx_clone.metrics.record_latency(start.elapsed());
                            }
                            if post_consistency {
                                run_dedicated_consistency_checks(
                                    &ctx_clone,
                                    "chaos-consistency",
                                    tid,
                                )?;
                            }
                            pad_chaos_metrics(&ctx_clone, batches * batch_size * 3);
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
                    match apply_chaos_op(&Arc::new(store.clone()), op, &tables) {
                        Ok(ok) => {
                            if ok {
                                ctx.metrics.record_success();
                            } else {
                                ctx.metrics.record_error();
                            }
                        }
                        Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                        Err(e) => return Err(e),
                    }
                }
                ctx.metrics.record_latency(start.elapsed());
            }
            drop(store);
            let reopened = MemoryKV::open(&ctx.db_path)?;
            let mut reader = reopened.begin(TxnMode::ReadOnly)?;
            let has_meta = reader.scan_prefix(b"meta:")?.next().is_some();
            assert!(has_meta || tables.lock().unwrap().is_empty());
            pad_chaos_metrics(ctx, 400);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
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
                match apply_chaos_op(&Arc::new(store.clone()), op, &tables) {
                    Ok(ok) => {
                        if ok {
                            ctx.metrics.record_success();
                        } else {
                            ctx.metrics.record_error();
                        }
                    }
                    Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                    Err(e) => return Err(e),
                }
            }
            ctx.metrics.record_latency(start.elapsed());
            pad_chaos_metrics(ctx, 200);
            Ok(())
        }),
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            harness.run_async(|ctx| async move {
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
                    match apply_chaos_op(&store, op, &tables) {
                        Ok(ok) => {
                            if ok {
                                ctx.metrics.record_success();
                            } else {
                                ctx.metrics.record_error();
                            }
                        }
                        Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                        Err(e) => return Err(e),
                    }
                }
                ctx.metrics.record_latency(start.elapsed());
                drop(store);
                let reopened = MemoryKV::open(&ctx.db_path)?;
                let mut reader = reopened.begin(TxnMode::ReadOnly)?;
                let has_meta = reader.scan_prefix(b"meta:")?.next().is_some();
                assert!(has_meta || tables.lock().unwrap().is_empty());
                pad_chaos_metrics(&ctx, 300);
                Ok(())
            })
        }
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
    run_chaos_mix(
        "chaos_long_running",
        model,
        cfg,
        batches,
        batch_size,
        Some(12),
        false,
    )
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
    run_chaos_mix("chaos_backpressure", model, cfg, 8, 30, Some(16), false)
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
    run_chaos_mix("chaos_random_ops", model, cfg, 6, 50, Some(50), false)
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
    run_chaos_mix("chaos_combined", model, cfg, 10, 24, Some(12), false)
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
    run_chaos_mix(
        "chaos_long_txn_conflict",
        model,
        cfg,
        8,
        32,
        Some(10),
        false,
    )
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
    run_chaos_mix(
        "chaos_multi_model_error_injection",
        model,
        cfg,
        8,
        28,
        Some(12),
        false,
    )
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
    run_chaos_mix("chaos_recovery_baseline", model, cfg, 6, 20, Some(8), true)
}

fn run_persistent_crash_reopen(model: ExecutionModel) -> TestResult {
    let concurrency = match model {
        ExecutionModel::SyncMulti | ExecutionModel::AsyncMulti => 6,
        _ => 1,
    };
    let chaos_cfg = ChaosConfig {
        workload: WorkloadConfig {
            operation_count: 24,
            key_space_size: 256,
            value_size: 96,
            ..Default::default()
        },
        multi_model: common::MultiModelWorkloadConfig {
            workload: WorkloadConfig {
                operation_count: 24,
                key_space_size: 128,
                value_size: 64,
                seed: 515,
            },
            ..Default::default()
        },
        ddl_seed: 707,
        invalid_seed: 909,
        dml_ratio: 0.28,
        multi_model_ratio: 0.22,
        ddl_ratio: 0.2,
        error_ratio: 0.15,
        crash_ratio: 0.15,
    };
    let batches = 6;
    let batch_size = 24;
    let mut cfg = chaos_config("chaos_persistent_crash_reopen", model, concurrency);
    cfg.slo = Some(SloConfig {
        min_throughput: Some(10.0),
        // クラッシュ後の再オープンで意図的にエラー計上が増えるため、許容値を緩和
        max_error_ratio: Some(0.6),
        ..Default::default()
    });
    let harness = StressTestHarness::new(cfg).unwrap();
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let tables = Arc::new(Mutex::new(HashSet::new()));
            let mut store = Arc::new(open_persistent_store(&ctx.db_path)?);
            seed_persistent_baseline(&store, &tables)?;
            let mut gen = ChaosWorkloadGenerator::new(chaos_cfg.clone());
            for _ in 0..batches {
                run_persistent_batch(ctx, &mut store, &tables, &mut gen, batch_size)?;
            }
            verify_persistent_state(&store, &tables)?;
            drop(store);
            run_full_consistency_checks(ctx, std::slice::from_ref(&StressStorageMode::Memory))?;
            pad_chaos_metrics(ctx, batches * batch_size * 3);
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(move |tid, ctx| {
            let ctx =
                context_with_db_path(ctx, scoped_db_path(&ctx.db_path, "persistent-crash", tid));
            let tables = Arc::new(Mutex::new(HashSet::new()));
            let mut cfg_local = chaos_cfg.clone();
            cfg_local.workload.seed ^= tid as u64 + 0x51;
            cfg_local.multi_model.workload.seed ^= tid as u64 + 0x71;
            cfg_local.ddl_seed = cfg_local.ddl_seed.wrapping_add(tid as u64);
            cfg_local.invalid_seed = cfg_local.invalid_seed.wrapping_add(tid as u64);
            let mut store = Arc::new(open_persistent_store(&ctx.db_path)?);
            seed_persistent_baseline(&store, &tables)?;
            let mut gen = ChaosWorkloadGenerator::new(cfg_local);
            for _ in 0..batches {
                run_persistent_batch(&ctx, &mut store, &tables, &mut gen, batch_size)?;
            }
            verify_persistent_state(&store, &tables)?;
            drop(store);
            run_dedicated_consistency_checks(&ctx, "persistent-consistency", tid)?;
            pad_chaos_metrics(&ctx, batches * batch_size * 3);
            Ok(())
        }),
        ExecutionModel::AsyncSingle => {
            let chaos_async = chaos_cfg.clone();
            harness.run_async(move |ctx| {
                let tables = Arc::new(Mutex::new(HashSet::new()));
                let chaos_cfg_local = chaos_async.clone();
                async move {
                    let mut store = Arc::new(open_persistent_store(&ctx.db_path)?);
                    seed_persistent_baseline(&store, &tables)?;
                    let mut gen = ChaosWorkloadGenerator::new(chaos_cfg_local);
                    for _ in 0..batches {
                        run_persistent_batch(&ctx, &mut store, &tables, &mut gen, batch_size)?;
                    }
                    verify_persistent_state(&store, &tables)?;
                    drop(store);
                    run_full_consistency_checks(
                        &ctx,
                        std::slice::from_ref(&StressStorageMode::Memory),
                    )?;
                    pad_chaos_metrics(&ctx, batches * batch_size * 3);
                    Ok(())
                }
            })
        }
        ExecutionModel::AsyncMulti => {
            let workers = concurrency;
            let chaos_outer = chaos_cfg.clone();
            harness.run_async(move |ctx| {
                let chaos_cfg_outer = chaos_outer.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..workers {
                        let ctx_clone = context_with_db_path(
                            &ctx,
                            scoped_db_path(&ctx.db_path, "persistent-crash", tid),
                        );
                        let mut cfg_local = chaos_cfg_outer.clone();
                        cfg_local.workload.seed ^= tid as u64 + 0x59;
                        cfg_local.multi_model.workload.seed ^= tid as u64 + 0x7b;
                        cfg_local.ddl_seed = cfg_local.ddl_seed.wrapping_add(tid as u64);
                        cfg_local.invalid_seed = cfg_local.invalid_seed.wrapping_add(tid as u64);
                        set.spawn(async move {
                            let tables = Arc::new(Mutex::new(HashSet::new()));
                            let mut store_handle =
                                Arc::new(open_persistent_store(&ctx_clone.db_path)?);
                            seed_persistent_baseline(&store_handle, &tables)?;
                            let mut gen = ChaosWorkloadGenerator::new(cfg_local);
                            for _ in 0..batches {
                                match run_persistent_batch(
                                    &ctx_clone,
                                    &mut store_handle,
                                    &tables,
                                    &mut gen,
                                    batch_size,
                                ) {
                                    Ok(_) => {}
                                    Err(CoreError::TxnConflict) => {
                                        ctx_clone.metrics.record_error();
                                        continue;
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                            verify_persistent_state(&store_handle, &tables)?;
                            drop(store_handle);
                            run_dedicated_consistency_checks(
                                &ctx_clone,
                                "persistent-consistency",
                                tid,
                            )?;
                            pad_chaos_metrics(&ctx_clone, batches * batch_size * 3);
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

#[cfg(feature = "test-hooks")]
struct ChaosMatrixConfig {
    nodes: usize,
    zones: usize,
    steps: usize,
    inject_interval: Duration,
    max_latency_ms: u64,
    loss_rate: f64,
    partition_rate: f64,
    zone_outage_rate: f64,
    zone_restart_rate: f64,
    kill_rate: f64,
    restart_rate: f64,
    disk_full_rate: f64,
}

#[cfg(feature = "test-hooks")]
impl ChaosMatrixConfig {
    fn from_env() -> Self {
        Self {
            nodes: env_usize("STRESS_CHAOS_MATRIX_NODES", 3).max(3),
            zones: env_usize("STRESS_CHAOS_MATRIX_ZONES", 2).max(1),
            steps: env_usize("STRESS_CHAOS_MATRIX_STEPS", 400),
            inject_interval: Duration::from_millis(env_u64("STRESS_CHAOS_MATRIX_INJECT_MS", 250)),
            max_latency_ms: env_u64("STRESS_CHAOS_MATRIX_MAX_LATENCY_MS", 10),
            loss_rate: env_f64("STRESS_CHAOS_MATRIX_LOSS_RATE", 0.05),
            partition_rate: env_f64("STRESS_CHAOS_MATRIX_PARTITION_RATE", 0.2),
            zone_outage_rate: env_f64("STRESS_CHAOS_MATRIX_ZONE_OUTAGE_RATE", 0.05),
            zone_restart_rate: env_f64("STRESS_CHAOS_MATRIX_ZONE_RESTART_RATE", 0.4),
            kill_rate: env_f64("STRESS_CHAOS_MATRIX_KILL_RATE", 0.05),
            restart_rate: env_f64("STRESS_CHAOS_MATRIX_RESTART_RATE", 0.1),
            disk_full_rate: env_f64("STRESS_CHAOS_MATRIX_DISK_FULL_RATE", 0.05),
        }
    }
}

#[cfg(feature = "test-hooks")]
#[derive(Clone, Debug)]
struct ChaosLink {
    latency_ms: u64,
    loss_rate: f64,
    partitioned: bool,
}

#[cfg(feature = "test-hooks")]
struct ChaosNode {
    id: usize,
    zone: usize,
    path: PathBuf,
    store: Option<Arc<MemoryKV>>,
    disk_full: Arc<DiskFullInjector>,
}

#[cfg(feature = "test-hooks")]
struct ChaosMatrix {
    nodes: Vec<ChaosNode>,
    links: Vec<Vec<ChaosLink>>,
    _temp_dir: TempDir,
    timeline_path: Option<PathBuf>,
}

#[cfg(feature = "test-hooks")]
impl ChaosMatrix {
    fn new(cfg: &ChaosMatrixConfig, timeline_path: Option<PathBuf>) -> CoreResult<Self> {
        let temp_dir = TempDir::new().map_err(CoreError::Io)?;
        let mut nodes = Vec::with_capacity(cfg.nodes);
        for id in 0..cfg.nodes {
            let zone = id % cfg.zones.max(1);
            let path = temp_dir.path().join(format!("node-{id}"));
            let disk_full = Arc::new(DiskFullInjector::new());
            let store = Some(open_matrix_store(&path, disk_full.clone())?);
            nodes.push(ChaosNode {
                id,
                zone,
                path,
                store,
                disk_full,
            });
        }
        let links = (0..cfg.nodes)
            .map(|_| {
                (0..cfg.nodes)
                    .map(|_| ChaosLink {
                        latency_ms: 0,
                        loss_rate: 0.0,
                        partitioned: false,
                    })
                    .collect()
            })
            .collect();
        Ok(Self {
            nodes,
            links,
            _temp_dir: temp_dir,
            timeline_path,
        })
    }

    fn inject_faults(&mut self, cfg: &ChaosMatrixConfig) -> CoreResult<()> {
        for node in &self.nodes {
            node.disk_full.set_full(false);
        }
        for i in 0..self.links.len() {
            for j in 0..self.links.len() {
                if i == j {
                    continue;
                }
                let link = &mut self.links[i][j];
                link.partitioned = false;
                link.loss_rate = 0.0;
                link.latency_ms = 0;
            }
        }

        let partitioned = gen_f64() < cfg.partition_rate;
        if partitioned {
            for i in 0..self.nodes.len() {
                for j in 0..self.nodes.len() {
                    if i == j {
                        continue;
                    }
                    if self.nodes[i].zone != self.nodes[j].zone {
                        self.links[i][j].partitioned = true;
                    }
                }
            }
            self.log_link_event("partition", |link| link.partitioned);
        }

        let max_latency = cfg.max_latency_ms;
        for i in 0..self.nodes.len() {
            for j in 0..self.nodes.len() {
                if i == j {
                    continue;
                }
                let link = &mut self.links[i][j];
                if max_latency > 0 && gen_f64() < 0.5 {
                    let upper = (max_latency as usize).saturating_add(1).max(1);
                    link.latency_ms = gen_range_usize(0..upper) as u64;
                }
                if cfg.loss_rate > 0.0 && gen_f64() < cfg.loss_rate {
                    link.loss_rate = cfg.loss_rate;
                }
            }
        }
        self.log_link_event("latency", |link| link.latency_ms > 0);
        self.log_link_event("loss", |link| link.loss_rate > 0.0);

        if cfg.disk_full_rate > 0.0 && gen_f64() < cfg.disk_full_rate && self.nodes.len() > 1 {
            let idx = 1 + gen_range_usize(0..self.nodes.len() - 1);
            self.nodes[idx].disk_full.set_full(true);
            self.log_event("disk_full", &format!("nodes=[{idx}]"));
        }

        if cfg.zone_outage_rate > 0.0 && gen_f64() < cfg.zone_outage_rate {
            let zone = self.pick_outage_zone(cfg.zones.max(1));
            let mut affected = Vec::new();
            for node in &mut self.nodes {
                if node.zone == zone && node.id != 0 {
                    node.store = None;
                    affected.push(node.id);
                }
            }
            if !affected.is_empty() {
                self.log_event("zone_outage", &format!("zone={zone} nodes={:?}", affected));
            }
            if cfg.zone_restart_rate > 0.0 && gen_f64() < cfg.zone_restart_rate {
                let restart_ids: Vec<usize> = self
                    .nodes
                    .iter()
                    .filter(|node| node.zone == zone && node.store.is_none())
                    .map(|node| node.id)
                    .collect();
                let mut restarted = Vec::new();
                for node_id in restart_ids {
                    self.restart_node(node_id)?;
                    restarted.push(node_id);
                }
                if !restarted.is_empty() {
                    self.log_event(
                        "zone_restart",
                        &format!("zone={zone} nodes={:?}", restarted),
                    );
                }
            }
        }

        if cfg.kill_rate > 0.0 && gen_f64() < cfg.kill_rate && self.nodes.len() > 1 {
            let idx = 1 + gen_range_usize(0..self.nodes.len() - 1);
            self.kill_node(idx);
            self.log_event("kill", &format!("nodes=[{idx}]"));
        }

        if cfg.restart_rate > 0.0 && gen_f64() < cfg.restart_rate {
            let dead: Vec<usize> = self
                .nodes
                .iter()
                .filter(|n| n.store.is_none() && n.id != 0)
                .map(|n| n.id)
                .collect();
            if !dead.is_empty() {
                let pick = dead[gen_range_usize(0..dead.len())];
                self.restart_node(pick)?;
                self.log_event("restart", &format!("nodes=[{pick}]"));
            }
        }

        Ok(())
    }

    fn kill_node(&mut self, idx: usize) {
        if let Some(node) = self.nodes.get_mut(idx) {
            node.store = None;
        }
    }

    fn restart_node(&mut self, idx: usize) -> CoreResult<()> {
        let node = self
            .nodes
            .get_mut(idx)
            .ok_or_else(|| CoreError::Io(io::Error::other("node index out of range")))?;
        let store = open_matrix_store(&node.path, node.disk_full.clone())?;
        node.store = Some(store);
        Ok(())
    }

    fn pick_outage_zone(&self, zones: usize) -> usize {
        let primary_zone = self.nodes.first().map(|n| n.zone).unwrap_or(0);
        let mut candidates: Vec<usize> = (0..zones).collect();
        if zones > 1 {
            candidates.retain(|z| *z != primary_zone);
        }
        if candidates.is_empty() {
            primary_zone
        } else {
            candidates[gen_range_usize(0..candidates.len())]
        }
    }

    fn log_event(&self, kind: &str, detail: &str) {
        let Some(path) = &self.timeline_path else {
            return;
        };
        let line = format!("ts={} event={} {}\n", Utc::now().to_rfc3339(), kind, detail);
        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(line.as_bytes())
            });
    }

    fn log_link_event(&self, kind: &str, predicate: impl Fn(&ChaosLink) -> bool) {
        let Some(_) = &self.timeline_path else {
            return;
        };
        let mut links = Vec::new();
        for i in 0..self.links.len() {
            for j in 0..self.links.len() {
                if i == j {
                    continue;
                }
                if predicate(&self.links[i][j]) {
                    links.push(format!("{i}->{j}"));
                }
            }
        }
        if !links.is_empty() {
            self.log_event(kind, &format!("links=[{}]", links.join(",")));
        }
    }

    fn primary_store(&self) -> CoreResult<Arc<MemoryKV>> {
        self.nodes
            .first()
            .and_then(|n| n.store.clone())
            .ok_or_else(|| CoreError::Io(io::Error::other("primary node unavailable")))
    }

    fn apply_primary(
        &mut self,
        op: ChaosOperation,
        tables: &Arc<Mutex<HashSet<String>>>,
    ) -> CoreResult<bool> {
        let primary = self.primary_store()?;
        let ok = apply_chaos_op(&primary, op.clone(), tables)?;
        self.replicate(0, &op, tables);
        Ok(ok)
    }

    fn replicate(
        &mut self,
        primary_idx: usize,
        op: &ChaosOperation,
        tables: &Arc<Mutex<HashSet<String>>>,
    ) {
        for idx in 0..self.nodes.len() {
            if idx == primary_idx {
                continue;
            }
            let node = &mut self.nodes[idx];
            let Some(store) = node.store.clone() else {
                continue;
            };
            let link = &self.links[primary_idx][idx];
            if link.partitioned {
                continue;
            }
            if link.loss_rate > 0.0 && gen_f64() < link.loss_rate {
                continue;
            }
            if link.latency_ms > 0 {
                std::thread::sleep(Duration::from_millis(link.latency_ms));
            }
            let _ = apply_chaos_op(&store, op.clone(), tables);
        }
    }
}

#[cfg(feature = "test-hooks")]
fn open_matrix_store(path: &Path, injector: Arc<DiskFullInjector>) -> CoreResult<Arc<MemoryKV>> {
    let hook: Arc<dyn common::FaultInjector> = injector;
    open_store_with_fault_injector(path, hook).map(Arc::new)
}

#[cfg(feature = "test-hooks")]
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

#[cfg(feature = "test-hooks")]
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(feature = "test-hooks")]
fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

#[cfg(feature = "test-hooks")]
fn run_chaos_matrix(model: ExecutionModel) -> Vec<(usize, TestResult)> {
    let chaos_cfg = ChaosConfig {
        crash_ratio: 0.0,
        error_ratio: 0.05,
        multi_model_ratio: 0.3,
        ddl_ratio: 0.15,
        ..Default::default()
    };
    let mut results = Vec::new();
    for scale in chaos_matrix_scales() {
        let test_name = format!("chaos_matrix_short_interval_n{scale}");
        let mut cfg = chaos_config(&test_name, model, 1);
        cfg.scenario_timeout = Duration::from_secs(300);
        let harness = StressTestHarness::new(cfg).unwrap();
        let mut matrix_cfg = ChaosMatrixConfig::from_env();
        matrix_cfg.nodes = scale.max(3);
        matrix_cfg.zones = matrix_cfg.zones.min(matrix_cfg.nodes).max(1);
        let started_at = Utc::now();
        let timeline_path = prepare_artifacts(Lane::Nightly, &test_name, &started_at)
            .map(|paths| log_path(&paths, "chaos_timeline.log"));
        let result = harness.run(|ctx| {
            let _op = begin_op(ctx);
            let mut matrix = ChaosMatrix::new(&matrix_cfg, timeline_path.clone())?;
            let tables = Arc::new(Mutex::new(HashSet::new()));
            let mut gen = ChaosWorkloadGenerator::new(chaos_cfg.clone());
            let mut last_inject = Instant::now();
            for _ in 0..matrix_cfg.steps {
                if last_inject.elapsed() >= matrix_cfg.inject_interval {
                    matrix.inject_faults(&matrix_cfg)?;
                    last_inject = Instant::now();
                }
                let op = gen.next_chaos_operation();
                let start = Instant::now();
                match matrix.apply_primary(op, &tables) {
                    Ok(true) => ctx.metrics.record_success(),
                    Ok(false) => ctx.metrics.record_error(),
                    Err(CoreError::TxnConflict) => ctx.metrics.record_error(),
                    Err(e) => return Err(e),
                }
                ctx.metrics.record_latency(start.elapsed());
            }
            pad_chaos_metrics(ctx, matrix_cfg.steps * 2);
            Ok(())
        });
        results.push((scale, result));
    }
    results
}

#[cfg(feature = "test-hooks")]
fn chaos_matrix_scales() -> Vec<usize> {
    let raw = std::env::var("STRESS_CHAOS_MATRIX_SCALES").unwrap_or_else(|_| "3,5,7".to_string());
    let mut values: Vec<usize> = raw
        .split([',', ';', ' '])
        .filter_map(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v >= 3)
        .collect();
    if values.is_empty() {
        values = vec![3, 5, 7];
    }
    values
}

macro_rules! chaos_test {
    ($name:ident, $runner:ident) => {
        #[cfg_attr(not(feature = "lane_nightly"), ignore)]
        #[test]
        fn $name() {
            if std::env::var("STRESS_STORAGE_MODE")
                .unwrap_or_else(|_| "both".to_string())
                .eq_ignore_ascii_case("disk")
            {
                return;
            }
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
chaos_test!(
    test_multi_model_ddl_error_injection,
    run_multi_model_error_injection
);
chaos_test!(test_1hour_chaos_no_leak, run_long_running);
chaos_test!(test_chaos_restart_integrity, run_restart_integrity);
chaos_test!(test_chaos_backpressure_no_corruption, run_backpressure);
chaos_test!(test_chaos_recovery_to_baseline, run_recovery_to_baseline);
chaos_test!(
    test_persistent_crash_reopen_scenario,
    run_persistent_crash_reopen
);

#[cfg(feature = "test-hooks")]
#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_chaos_matrix_short_interval() {
    let results = run_chaos_matrix(ExecutionModel::SyncSingle);
    for (scale, result) in results {
        assert!(
            result.is_success(),
            "chaos_matrix_short_interval scale={}: {:?}",
            scale,
            result.failure_summary()
        );
    }
}
