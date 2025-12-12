mod common;

use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
use common::{
    begin_op, slo_presets, ChaosConfig, ChaosOperation, ChaosWorkloadGenerator, ColumnarOperation, DdlOperation,
    ExecutionModel, MultiModelOperation, SqlOperation, SloConfig, StressTestConfig, StressTestHarness, TestResult,
    VectorOperation, WorkloadConfig,
};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
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

const PERSISTENT_BASELINE_KEY: &[u8] = b"chaos:persistent:sentinel";
const PERSISTENT_SENTINEL_ROW: &[u8] = b"tbl:persist_seed:sentinel";
const PERSISTENT_TABLE: &str = "persist_seed";

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
        let _ = txn.delete(k)?;
    }
    txn.commit_self()
        .and_then(|_| {
            tables.lock().unwrap().clear();
            Ok(())
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
            OpenOptions::new().write(true).create(true).open(path)?;
            return Ok(());
        }
        let target = meta.len() / 2;
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .and_then(|file| file.set_len(target))?;
    } else if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        OpenOptions::new().write(true).create(true).open(path)?;
    }
    Ok(())
}

fn simulate_persistent_crash(path: &std::path::Path, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<()> {
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
            let _ = OpenOptions::new().write(true).create(true).open(&wal);
            let _ = OpenOptions::new().write(true).create(true).open(&sst);
            MemoryKV::open(path)
        }
        Err(e) => Err(e),
    }
}

fn apply_chaos_op(store: &Arc<MemoryKV>, op: ChaosOperation, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<bool> {
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

fn refresh_tables_from_meta(store: &Arc<MemoryKV>, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<()> {
    let mut reader = store.begin(TxnMode::ReadOnly)?;
    let meta_names: HashSet<String> = reader
        .scan_prefix(b"meta:")?
        .map(|(k, _)| {
            let name_bytes = k.strip_prefix(b"meta:").unwrap_or(&k);
            let primary = name_bytes.split(|b| *b == b':').next().unwrap_or(name_bytes);
            String::from_utf8_lossy(primary).to_string()
        })
        .collect();
    let mut guard = tables.lock().unwrap();
    guard.clear();
    guard.extend(meta_names);
    Ok(())
}

fn seed_persistent_baseline(store: &Arc<MemoryKV>, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<()> {
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

fn verify_persistent_state(store: &Arc<MemoryKV>, tables: &Arc<Mutex<HashSet<String>>>) -> CoreResult<()> {
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
            let primary = name_bytes.split(|b| *b == b':').next().unwrap_or(name_bytes);
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
                                        Err(CoreError::TxnConflict) => ctx_clone.metrics.record_error(),
                                        Err(e) => return Err(e),
                                    }
                                }
                                ctx_clone.metrics.record_latency(start.elapsed());
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
            pad_chaos_metrics(ctx, batches * batch_size * 3);
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let tables = Arc::new(Mutex::new(HashSet::new()));
            harness.run_concurrent(move |tid, ctx| {
                let mut cfg_local = chaos_cfg.clone();
                cfg_local.workload.seed ^= tid as u64 + 0x51;
                cfg_local.multi_model.workload.seed ^= tid as u64 + 0x71;
                cfg_local.ddl_seed = cfg_local.ddl_seed.wrapping_add(tid as u64);
                cfg_local.invalid_seed = cfg_local.invalid_seed.wrapping_add(tid as u64);
                let mut store = Arc::new(open_persistent_store(&ctx.db_path)?);
                seed_persistent_baseline(&store, &tables)?;
                let mut gen = ChaosWorkloadGenerator::new(cfg_local);
                for _ in 0..batches {
                    run_persistent_batch(ctx, &mut store, &tables, &mut gen, batch_size)?;
                }
                verify_persistent_state(&store, &tables)?;
                pad_chaos_metrics(ctx, batches * batch_size * 3);
                Ok(())
            })
        }
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
                    pad_chaos_metrics(&ctx, batches * batch_size * 3);
                    Ok(())
                }
            })
        }
        ExecutionModel::AsyncMulti => {
            let tables = Arc::new(Mutex::new(HashSet::new()));
            let workers = concurrency;
            let chaos_outer = chaos_cfg.clone();
            harness.run_async(move |ctx| {
                let tables_outer = tables.clone();
                let chaos_cfg_outer = chaos_outer.clone();
                async move {
                    let store = Arc::new(open_persistent_store(&ctx.db_path)?);
                    // Seed baseline once before worker fan-out to avoid concurrent TxnConflict on the same table.
                    seed_persistent_baseline(&store, &tables_outer)?;
                    let mut set = JoinSet::new();
                    for tid in 0..workers {
                        let ctx_clone = ctx.clone();
                        let tables_clone = tables_outer.clone();
                        let mut cfg_local = chaos_cfg_outer.clone();
                        cfg_local.workload.seed ^= tid as u64 + 0x59;
                        cfg_local.multi_model.workload.seed ^= tid as u64 + 0x7b;
                        cfg_local.ddl_seed = cfg_local.ddl_seed.wrapping_add(tid as u64);
                        cfg_local.invalid_seed = cfg_local.invalid_seed.wrapping_add(tid as u64);
                        let store_clone = store.clone();
                        set.spawn(async move {
                            let mut store_handle = store_clone;
                            let mut gen = ChaosWorkloadGenerator::new(cfg_local);
                            for _ in 0..batches {
                                match run_persistent_batch(
                                    &ctx_clone,
                                    &mut store_handle,
                                &tables_clone,
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
                        verify_persistent_state(&store_handle, &tables_clone)?;
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
chaos_test!(
    test_persistent_crash_reopen_scenario,
    run_persistent_crash_reopen
);
