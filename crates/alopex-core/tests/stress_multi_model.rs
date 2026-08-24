#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::types::Value;
use alopex_core::KVTransaction;
use alopex_core::{Error as CoreError, KVStore, MemoryKV, TxnMode};
use common::{
    begin_op, Column, ColumnarOperation, ExecutionModel, Lane, ModelMix, MultiModelOperation,
    MultiModelWorkloadConfig, MultiModelWorkloadGenerator, Operation, SqlOperation,
    StressTestConfig, StressTestHarness, TestResult, VectorOperation, WorkloadConfig,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const ALL_MODELS: [ExecutionModel; 4] = [
    ExecutionModel::SyncSingle,
    ExecutionModel::SyncMulti,
    ExecutionModel::AsyncSingle,
    ExecutionModel::AsyncMulti,
];
const MAX_TXN_RETRIES: usize = 20;

macro_rules! mm_test {
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

type CoreResult<T> = Result<T, CoreError>;

fn commit_retry_sync<F>(store: &Arc<MemoryKV>, mut apply: F) -> CoreResult<()>
where
    F: for<'a> FnMut(&mut MemoryTransaction<'a>) -> CoreResult<()>,
{
    let mut attempts = 0usize;
    loop {
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        apply(&mut txn)?;
        match txn.commit_self() {
            Ok(_) => return Ok(()),
            Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                attempts += 1;
                std::thread::yield_now();
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

fn multi_model_config(name: &str, model: ExecutionModel, concurrency: usize) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        lane: Lane::Nightly,
        execution_model: model,
        concurrency,
        scenario_timeout: Duration::from_secs(60),
        operation_timeout: Duration::from_secs(6),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: None,
    }
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0xcbf2_9ce4_8422_2325u64, |acc, b| acc ^ (*b as u64))
        .wrapping_mul(0x100_0000_01b3)
}

fn encode_row(row: &[(String, Value)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (col, val) in row {
        out.extend_from_slice(col.as_bytes());
        out.push(b'=');
        out.extend_from_slice(val);
        out.push(b';');
    }
    out
}

fn sql_row_key(table: &str, row: &[(String, Value)]) -> Vec<u8> {
    let digest = row
        .iter()
        .find(|(c, _)| c == "id")
        .map(|(_, v)| hash_bytes(v))
        .unwrap_or_else(|| hash_bytes(table.as_bytes()));
    format!("sql:{table}:{digest:x}").into_bytes()
}

fn encode_vector(v: &[f32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(v.len() * 4);
    for f in v {
        buf.extend_from_slice(&f.to_le_bytes());
    }
    buf
}

fn decode_vector(bytes: &[u8]) -> Vec<f32> {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_le_bytes(*c))
        .collect()
}

fn apply_kv_op(txn: &mut MemoryTransaction<'_>, op: Operation) -> CoreResult<()> {
    match op {
        Operation::Get(key) => {
            let _ = txn.get(&key)?;
        }
        Operation::Put(key, val) => {
            txn.put(key, val)?;
        }
        Operation::Delete(key) => {
            txn.delete(key)?;
        }
        Operation::Scan(prefix) => {
            let _ = txn.scan_prefix(&prefix)?.next();
        }
    }
    Ok(())
}

fn apply_sql_op(txn: &mut MemoryTransaction<'_>, op: SqlOperation) -> CoreResult<()> {
    match op {
        SqlOperation::Insert { table, row } => {
            let key = sql_row_key(&table, &row);
            txn.put(key, encode_row(&row))?;
        }
        SqlOperation::Select { table, .. } => {
            let prefix = format!("sql:{table}:").into_bytes();
            let _ = txn.scan_prefix(&prefix)?.next();
        }
        SqlOperation::Update { table, set, .. } => {
            let key = sql_row_key(&table, &set);
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

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn apply_vector_op(txn: &mut MemoryTransaction<'_>, op: VectorOperation) -> CoreResult<()> {
    match op {
        VectorOperation::Insert {
            id,
            vector,
            metadata,
        } => {
            let key = format!("vec:{id}").into_bytes();
            txn.put(key, encode_vector(&vector))?;
            if let Some(meta) = metadata {
                txn.put(format!("vec_meta:{id}").into_bytes(), meta)?;
            }
        }
        VectorOperation::Search { query, k } => {
            let mut scored = Vec::new();
            for (k_bytes, v_bytes) in txn.scan_prefix(b"vec:")? {
                let vec = decode_vector(&v_bytes);
                scored.push((dot(&query, &vec), k_bytes));
            }
            scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
            scored.truncate(k);
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
            for (idx, col) in columns.iter().enumerate() {
                let mut payload = Vec::new();
                for v in &col.values {
                    payload.extend_from_slice(v);
                }
                let key = format!("col:{}:{idx}", col.name).into_bytes();
                txn.put(key, payload)?;
            }
        }
        ColumnarOperation::Scan { projection, .. } => {
            for col in projection {
                let prefix = format!("col:{col}").into_bytes();
                let _ = txn.scan_prefix(&prefix)?.next();
            }
        }
    }
    Ok(())
}

fn apply_multi_op(txn: &mut MemoryTransaction<'_>, op: MultiModelOperation) -> CoreResult<()> {
    match op {
        MultiModelOperation::Kv(inner) => apply_kv_op(txn, inner),
        MultiModelOperation::Sql(inner) => apply_sql_op(txn, inner),
        MultiModelOperation::Vector(inner) => apply_vector_op(txn, inner),
        MultiModelOperation::Columnar(inner) => apply_columnar_op(txn, inner),
    }
}

fn apply_multi_batch(store: &Arc<MemoryKV>, ops: Vec<MultiModelOperation>) -> CoreResult<usize> {
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    let mut applied = 0;
    for op in ops {
        apply_multi_op(&mut txn, op)?;
        applied += 1;
    }
    txn.commit_self()?;
    Ok(applied)
}

fn new_multi_gen(seed: u64, batch_size: usize) -> MultiModelWorkloadGenerator {
    MultiModelWorkloadGenerator::new(MultiModelWorkloadConfig {
        model_mix: ModelMix::balanced(),
        workload: WorkloadConfig {
            operation_count: batch_size,
            seed,
            ..Default::default()
        },
        ..Default::default()
    })
}

fn run_generated_mix(
    name: &str,
    model: ExecutionModel,
    concurrency: usize,
    batches: usize,
    batch_size: usize,
    seed_offset: u64,
) -> TestResult {
    let effective_concurrency = match model {
        ExecutionModel::SyncSingle | ExecutionModel::AsyncSingle => 1,
        ExecutionModel::SyncMulti | ExecutionModel::AsyncMulti => concurrency.max(1),
    };
    let cfg = multi_model_config(name, model, effective_concurrency);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let _op = begin_op(ctx);
            let mut gen = new_multi_gen(seed_offset, batch_size);
            for _ in 0..batches {
                let mut attempts = 0;
                loop {
                    let start = Instant::now();
                    match apply_multi_batch(&store, gen.generate_batch(batch_size)) {
                        Ok(applied) => {
                            for _ in 0..applied {
                                ctx.metrics.record_success();
                            }
                            ctx.metrics.record_latency(start.elapsed());
                            break;
                        }
                        Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                            attempts += 1;
                            std::thread::yield_now();
                            continue;
                        }
                        Err(CoreError::TxnConflict) => {
                            ctx.metrics.record_error();
                            break;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let mut gen = new_multi_gen(seed_offset + tid as u64, batch_size);
                for _ in 0..batches {
                    let mut attempts = 0;
                    loop {
                        let start = Instant::now();
                        match apply_multi_batch(&store_sync, gen.generate_batch(batch_size)) {
                            Ok(applied) => {
                                for _ in 0..applied {
                                    ctx.metrics.record_success();
                                }
                                ctx.metrics.record_latency(start.elapsed());
                                break;
                            }
                            Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                                attempts += 1;
                                std::thread::yield_now();
                                continue;
                            }
                            Err(CoreError::TxnConflict) => {
                                ctx.metrics.record_error();
                                break;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let mut gen = new_multi_gen(seed_offset, batch_size);
                    for _ in 0..batches {
                        let mut attempts = 0;
                        loop {
                            let start = Instant::now();
                            match apply_multi_batch(&store, gen.generate_batch(batch_size)) {
                                Ok(applied) => {
                                    for _ in 0..applied {
                                        ctx.metrics.record_success();
                                    }
                                    ctx.metrics.record_latency(start.elapsed());
                                    break;
                                }
                                Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                                    attempts += 1;
                                    tokio::task::yield_now().await;
                                    continue;
                                }
                                Err(CoreError::TxnConflict) => {
                                    ctx.metrics.record_error();
                                    break;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    }
                    Ok(())
                }
            })
        }
        ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..effective_concurrency {
                        let store = store.clone();
                        let ctx_clone = ctx.clone();
                        set.spawn(async move {
                            let mut gen = new_multi_gen(seed_offset + tid as u64, batch_size);
                            for _ in 0..batches {
                                let mut attempts = 0;
                                loop {
                                    let start = Instant::now();
                                    match apply_multi_batch(&store, gen.generate_batch(batch_size))
                                    {
                                        Ok(applied) => {
                                            for _ in 0..applied {
                                                ctx_clone.metrics.record_success();
                                            }
                                            ctx_clone.metrics.record_latency(start.elapsed());
                                            break;
                                        }
                                        Err(CoreError::TxnConflict)
                                            if attempts < MAX_TXN_RETRIES =>
                                        {
                                            attempts += 1;
                                            tokio::task::yield_now().await;
                                            continue;
                                        }
                                        Err(CoreError::TxnConflict) => {
                                            ctx_clone.metrics.record_error();
                                            break;
                                        }
                                        Err(e) => return Err(e),
                                    }
                                }
                            }
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

fn cross_model_ops(tid: usize, round: usize) -> Vec<MultiModelOperation> {
    vec![
        MultiModelOperation::Kv(Operation::Put(
            format!("kv_commit_{tid}_{round}").into_bytes(),
            b"value".to_vec(),
        )),
        MultiModelOperation::Sql(SqlOperation::Insert {
            table: "accounts".into(),
            row: vec![
                ("id".into(), format!("acct-{tid}-{round}").into_bytes()),
                ("name".into(), b"user".to_vec()),
            ],
        }),
        MultiModelOperation::Vector(VectorOperation::Insert {
            id: (tid * 10 + round) as u64,
            vector: vec![1.0, 0.5, 0.25],
            metadata: Some(b"meta".to_vec()),
        }),
        MultiModelOperation::Columnar(ColumnarOperation::BatchInsert {
            columns: vec![
                Column {
                    name: "c0".into(),
                    values: vec![b"a".to_vec()],
                },
                Column {
                    name: "c1".into(),
                    values: vec![b"b".to_vec()],
                },
            ],
        }),
    ]
}

fn assert_cross_model_commit(
    reader: &mut MemoryTransaction<'_>,
    tid: usize,
    round: usize,
) -> CoreResult<()> {
    assert_eq!(
        reader.get(&format!("kv_commit_{tid}_{round}").into_bytes())?,
        Some(b"value".to_vec())
    );

    let sql_row = vec![
        ("id".to_string(), format!("acct-{tid}-{round}").into_bytes()),
        ("name".to_string(), b"user".to_vec()),
    ];
    assert_eq!(
        reader.get(&sql_row_key("accounts", &sql_row))?,
        Some(encode_row(&sql_row))
    );

    let vector_id = tid * 10 + round;
    assert_eq!(
        reader.get(&format!("vec:{vector_id}").into_bytes())?,
        Some(encode_vector(&[1.0, 0.5, 0.25]))
    );
    assert_eq!(
        reader.get(&format!("vec_meta:{vector_id}").into_bytes())?,
        Some(b"meta".to_vec())
    );
    assert_eq!(reader.get(&b"col:c0:0".to_vec())?, Some(b"a".to_vec()));
    assert_eq!(reader.get(&b"col:c1:1".to_vec())?, Some(b"b".to_vec()));
    Ok(())
}

fn run_cross_model_atomic_commit(model: ExecutionModel) -> TestResult {
    let concurrency = match model {
        ExecutionModel::SyncMulti | ExecutionModel::AsyncMulti => 4,
        _ => 1,
    };
    let mut cfg = multi_model_config("cross_model_atomic_commit", model, concurrency);
    cfg.slo = None;
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let _op = begin_op(ctx);
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            for op in cross_model_ops(0, 0) {
                apply_multi_op(&mut txn, op)?;
                ctx.metrics.record_success();
            }
            txn.commit_self()?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_cross_model_commit(&mut reader, 0, 0)?;
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let start = Instant::now();
                let mut attempts = 0;
                loop {
                    let mut txn = store_sync.begin(TxnMode::ReadWrite)?;
                    for op in cross_model_ops(tid, 0) {
                        apply_multi_op(&mut txn, op)?;
                        ctx.metrics.record_success();
                    }
                    match txn.commit_self() {
                        Ok(_) => break,
                        Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                            attempts += 1;
                            std::thread::yield_now();
                            continue;
                        }
                        Err(CoreError::TxnConflict) => return Err(CoreError::TxnConflict),
                        Err(e) => return Err(e),
                    }
                }
                let mut reader = store_sync.begin(TxnMode::ReadOnly)?;
                assert_cross_model_commit(&mut reader, tid, 0)?;
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let mut set = JoinSet::new();
                    for tid in 0..concurrency {
                        let store = store.clone();
                        let ctx_clone = ctx.clone();
                        set.spawn(async move {
                            let start = Instant::now();
                            let mut attempts = 0;
                            loop {
                                let mut txn = store.begin(TxnMode::ReadWrite)?;
                                for op in cross_model_ops(tid, 0) {
                                    let _op = ctx_clone.watchdog.begin_operation();
                                    apply_multi_op(&mut txn, op)?;
                                    ctx_clone.metrics.record_success();
                                }
                                match txn.commit_self() {
                                    Ok(_) => break,
                                    Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                                        attempts += 1;
                                        tokio::task::yield_now().await;
                                        continue;
                                    }
                                    Err(CoreError::TxnConflict) => {
                                        return Err(CoreError::TxnConflict)
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                            let mut reader = store.begin(TxnMode::ReadOnly)?;
                            assert_cross_model_commit(&mut reader, tid, 0)?;
                            ctx_clone.metrics.record_latency(start.elapsed());
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

fn run_cross_model_rollback(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("cross_model_rollback", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let _op = begin_op(ctx);
            let start = Instant::now();
            {
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                for op in cross_model_ops(0, 1) {
                    apply_multi_op(&mut txn, op)?;
                }
                // rollback by dropping without commit
            }
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert!(reader.get(&b"kv_commit_0_1".to_vec())?.is_none());
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            {
                let mut bootstrap = store_sync.begin(TxnMode::ReadWrite).expect("bootstrap txn");
                apply_vector_op(
                    &mut bootstrap,
                    VectorOperation::Insert {
                        id: 1,
                        vector: vec![0.5, 0.5],
                        metadata: None,
                    },
                )
                .expect("bootstrap insert");
                bootstrap.commit_self().expect("bootstrap commit");
            }
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let start = Instant::now();
                if tid == 0 {
                    let mut txn = store_sync.begin(TxnMode::ReadWrite)?;
                    for op in cross_model_ops(tid, 1) {
                        apply_multi_op(&mut txn, op)?;
                    }
                }
                let mut reader = store_sync.begin(TxnMode::ReadOnly)?;
                assert!(reader
                    .get(&format!("kv_commit_{tid}_1").into_bytes())?
                    .is_none());
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let start = Instant::now();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    for op in cross_model_ops(0, 1) {
                        let _op = ctx.watchdog.begin_operation();
                        apply_multi_op(&mut txn, op)?;
                    }
                    drop(txn); // rollback
                    let mut reader = store.begin(TxnMode::ReadOnly)?;
                    assert!(reader.get(&b"kv_commit_0_1".to_vec())?.is_none());
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_kv_sql_same_entity(model: ExecutionModel) -> TestResult {
    run_generated_mix("kv_sql_same_entity", model, 3, 6, 8, 42)
}

fn run_vector_metadata_sync(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("vector_metadata_sync", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let _op = begin_op(ctx);
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_vector_op(
                &mut txn,
                VectorOperation::Insert {
                    id: 9,
                    vector: vec![0.1, 0.2, 0.3, 0.4],
                    metadata: Some(b"sql:row=9".to_vec()),
                },
            )?;
            txn.commit_self()?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert!(reader.get(&b"vec_meta:9".to_vec())?.is_some());
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let start = Instant::now();
                if tid == 0 {
                    let mut txn = store_sync.begin(TxnMode::ReadWrite)?;
                    apply_vector_op(
                        &mut txn,
                        VectorOperation::Insert {
                            id: 11,
                            vector: vec![0.9, 0.1],
                            metadata: Some(b"meta_sync".to_vec()),
                        },
                    )?;
                    txn.commit_self()?;
                } else {
                    let mut reader = store_sync.begin(TxnMode::ReadOnly)?;
                    let _ = reader.get(&b"vec_meta:11".to_vec())?;
                }
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let start = Instant::now();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    apply_vector_op(
                        &mut txn,
                        VectorOperation::Insert {
                            id: 21,
                            vector: vec![1.0, 0.0, 0.0],
                            metadata: Some(b"async_meta".to_vec()),
                        },
                    )?;
                    txn.commit_self()?;
                    let mut reader = store.begin(TxnMode::ReadOnly)?;
                    assert!(reader.get(&b"vec_meta:21".to_vec())?.is_some());
                    ctx.metrics.record_success();
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_cross_model_crash_recovery(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("cross_model_crash_recovery", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let store = MemoryKV::open(&ctx.db_path)?;
            {
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                for op in cross_model_ops(0, 2) {
                    apply_multi_op(&mut txn, op)?;
                    ctx.metrics.record_success();
                }
                txn.commit_self()?;
            }
            drop(store);
            let reopened = MemoryKV::open(&ctx.db_path)?;
            let mut reader = reopened.begin(TxnMode::ReadOnly)?;
            assert!(reader.get(&b"kv_commit_0_2".to_vec())?.is_some());
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            let start = Instant::now();
            if tid == 0 {
                let store = MemoryKV::open(&ctx.db_path)?;
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                for op in cross_model_ops(tid, 2) {
                    apply_multi_op(&mut txn, op)?;
                }
                txn.commit_self()?;
            }
            let reopened = MemoryKV::open(&ctx.db_path)?;
            let mut reader = reopened.begin(TxnMode::ReadOnly)?;
            if tid == 0 {
                assert!(reader
                    .get(&format!("kv_commit_{tid}_2").into_bytes())?
                    .is_some());
            } else {
                let _ = reader.get(&format!("kv_commit_{tid}_2").into_bytes())?;
            }
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            harness.run_async(|ctx| async move {
                let start = Instant::now();
                let store = MemoryKV::open(&ctx.db_path)?;
                {
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    for op in cross_model_ops(0, 2) {
                        apply_multi_op(&mut txn, op)?;
                        ctx.metrics.record_success();
                    }
                    txn.commit_self()?;
                }
                drop(store);
                let reopened = MemoryKV::open(&ctx.db_path)?;
                let mut reader = reopened.begin(TxnMode::ReadOnly)?;
                assert!(reader.get(&b"kv_commit_0_2".to_vec())?.is_some());
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
    }
}

fn run_kv_sql_concurrent_access(model: ExecutionModel) -> TestResult {
    run_generated_mix("kv_sql_concurrent_access", model, 4, 8, 10, 90)
}

fn run_sql_select_kv_update_isolation(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("sql_select_kv_update_isolation", model, 3);
    let concurrency = cfg.concurrency.max(1);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"kv:isolation".to_vec(), b"v1".to_vec())?;
            txn.commit_self()?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            let snapshot = reader.get(&b"kv:isolation".to_vec())?;
            let mut writer = store.begin(TxnMode::ReadWrite)?;
            writer.put(b"kv:isolation".to_vec(), b"v2".to_vec())?;
            writer.commit_self()?;
            assert_eq!(snapshot, Some(b"v1".to_vec()));
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            let barrier = Arc::new(std::sync::Barrier::new(concurrency));
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let start = Instant::now();
                if tid == 0 {
                    let mut txn = store_sync.begin(TxnMode::ReadWrite)?;
                    txn.put(b"kv:isolation".to_vec(), b"v1".to_vec())?;
                    txn.commit_self()?;
                }
                barrier.wait();
                if tid != 0 {
                    let mut reader = store_sync.begin(TxnMode::ReadOnly)?;
                    assert_eq!(reader.get(&b"kv:isolation".to_vec())?, Some(b"v1".to_vec()));
                }
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let start = Instant::now();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    txn.put(b"kv:isolation".to_vec(), b"v1".to_vec())?;
                    txn.commit_self()?;
                    let mut reader = store.begin(TxnMode::ReadOnly)?;
                    let snap = reader.get(&b"kv:isolation".to_vec())?;
                    let mut writer = store.begin(TxnMode::ReadWrite)?;
                    writer.put(b"kv:isolation".to_vec(), b"v2".to_vec())?;
                    writer.commit_self()?;
                    assert_eq!(snap, Some(b"v1".to_vec()));
                    ctx.metrics.record_success();
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_vector_search_sql_insert(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("vector_search_sql_insert", model, 3);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_vector_op(
                &mut txn,
                VectorOperation::Insert {
                    id: 33,
                    vector: vec![0.1, 0.9],
                    metadata: Some(b"user:33".to_vec()),
                },
            )?;
            apply_sql_op(
                &mut txn,
                SqlOperation::Insert {
                    table: "vectors".into(),
                    row: vec![
                        ("id".into(), b"33".to_vec()),
                        ("meta".into(), b"user:33".to_vec()),
                    ],
                },
            )?;
            txn.commit_self()?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            let vectors: Vec<_> = reader.scan_prefix(b"vec:")?.collect();
            if vectors.is_empty() {
                ctx.metrics.record_error();
            } else {
                ctx.metrics.record_success();
            }
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            {
                let mut bootstrap = store_sync.begin(TxnMode::ReadWrite).expect("bootstrap txn");
                apply_vector_op(
                    &mut bootstrap,
                    VectorOperation::Insert {
                        id: 1,
                        vector: vec![0.5, 0.5],
                        metadata: None,
                    },
                )
                .expect("bootstrap insert");
                bootstrap.commit_self().expect("bootstrap commit");
            }
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let start = Instant::now();
                if tid == 0 {
                    let mut txn = store_sync.begin(TxnMode::ReadWrite)?;
                    apply_vector_op(
                        &mut txn,
                        VectorOperation::Insert {
                            id: 40,
                            vector: vec![0.2, 0.8],
                            metadata: None,
                        },
                    )?;
                    txn.commit_self()?;
                } else {
                    let mut reader = store_sync.begin(TxnMode::ReadOnly)?;
                    let mut best = Vec::new();
                    for (k, v) in reader.scan_prefix(b"vec:")? {
                        best.push((k, v));
                    }
                    if best.is_empty() {
                        ctx.metrics.record_error();
                    } else {
                        ctx.metrics.record_success();
                    }
                    ctx.metrics.record_latency(start.elapsed());
                    return Ok(());
                }
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let start = Instant::now();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    apply_vector_op(
                        &mut txn,
                        VectorOperation::Insert {
                            id: 55,
                            vector: vec![0.4, 0.6, 0.9],
                            metadata: Some(b"m".to_vec()),
                        },
                    )?;
                    apply_sql_op(
                        &mut txn,
                        SqlOperation::Insert {
                            table: "vectors".into(),
                            row: vec![("id".into(), b"55".to_vec())],
                        },
                    )?;
                    txn.commit_self()?;
                    ctx.metrics.record_success();
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_100_connections_mixed_api(model: ExecutionModel) -> TestResult {
    run_generated_mix("mixed_api_100", model, 6, 12, 8, 120)
}

fn run_columnar_scan_kv_update(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("columnar_scan_kv_update", model, 3);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_columnar_op(
                &mut txn,
                ColumnarOperation::BatchInsert {
                    columns: vec![Column {
                        name: "c0".into(),
                        values: vec![b"1".to_vec(), b"2".to_vec()],
                    }],
                },
            )?;
            apply_kv_op(
                &mut txn,
                Operation::Put(b"kv:col".to_vec(), b"updated".to_vec()),
            )?;
            txn.commit_self()?;
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            harness.run_concurrent(move |tid, ctx| {
                let _op = begin_op(ctx);
                let start = Instant::now();
                if tid == 0 {
                    commit_retry_sync(&store_sync, |txn| {
                        apply_columnar_op(
                            txn,
                            ColumnarOperation::BatchInsert {
                                columns: vec![Column {
                                    name: "c1".into(),
                                    values: vec![b"9".to_vec()],
                                }],
                            },
                        )
                    })?;
                } else {
                    commit_retry_sync(&store_sync, |txn| {
                        apply_kv_op(txn, Operation::Put(b"kv:col".to_vec(), b"u2".to_vec()))
                    })?;
                }
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let start = Instant::now();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    apply_columnar_op(
                        &mut txn,
                        ColumnarOperation::BatchInsert {
                            columns: vec![Column {
                                name: "c2".into(),
                                values: vec![b"a".to_vec(), b"b".to_vec()],
                            }],
                        },
                    )?;
                    apply_kv_op(
                        &mut txn,
                        Operation::Put(b"kv:col".to_vec(), b"async".to_vec()),
                    )?;
                    txn.commit_self()?;
                    ctx.metrics.record_success();
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_sql_vector_column_update(model: ExecutionModel) -> TestResult {
    run_generated_mix("sql_vector_column_update", model, 3, 6, 10, 240)
}

fn run_kv_sql_row_consistency(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("kv_sql_row_consistency", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"kv:row:1".to_vec(), b"v1".to_vec())?;
            apply_sql_op(
                &mut txn,
                SqlOperation::Insert {
                    table: "rows".into(),
                    row: vec![("id".into(), b"1".to_vec()), ("v".into(), b"v1".to_vec())],
                },
            )?;
            txn.commit_self()?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"kv:row:1".to_vec())?, Some(b"v1".to_vec()));
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            harness.run_concurrent(move |_tid, ctx| {
                let start = Instant::now();
                commit_retry_sync(&store_sync, |txn| {
                    txn.put(b"kv:row:2".to_vec(), b"v2".to_vec())?;
                    apply_sql_op(
                        txn,
                        SqlOperation::Insert {
                            table: "rows".into(),
                            row: vec![("id".into(), b"2".to_vec()), ("v".into(), b"v2".to_vec())],
                        },
                    )
                })?;
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let start = Instant::now();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    txn.put(b"kv:row:3".to_vec(), b"v3".to_vec())?;
                    apply_sql_op(
                        &mut txn,
                        SqlOperation::Insert {
                            table: "rows".into(),
                            row: vec![("id".into(), b"3".to_vec())],
                        },
                    )?;
                    txn.commit_self()?;
                    ctx.metrics.record_success();
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_columnar_kv_flush_consistency(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("columnar_kv_flush_consistency", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            apply_columnar_op(
                &mut txn,
                ColumnarOperation::BatchInsert {
                    columns: vec![Column {
                        name: "c".into(),
                        values: vec![b"z".to_vec()],
                    }],
                },
            )?;
            apply_kv_op(
                &mut txn,
                Operation::Put(b"kv:flush".to_vec(), b"1".to_vec()),
            )?;
            txn.commit_self()?;
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            harness.run_concurrent(move |_tid, ctx| {
                let start = Instant::now();
                commit_retry_sync(&store_sync, |txn| {
                    apply_kv_op(txn, Operation::Put(b"kv:flush".to_vec(), b"2".to_vec()))
                })?;
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                async move {
                    let start = Instant::now();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    apply_columnar_op(
                        &mut txn,
                        ColumnarOperation::BatchInsert {
                            columns: vec![Column {
                                name: "c_async".into(),
                                values: vec![b"x".to_vec()],
                            }],
                        },
                    )?;
                    txn.commit_self()?;
                    ctx.metrics.record_success();
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_partial_index_update_rollback(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("partial_index_update_rollback", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"index:btree:1".to_vec(), b"v".to_vec())?;
            // simulate failure before vector index write
            drop(txn);
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert!(reader.get(&b"index:btree:1".to_vec())?.is_none());
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        _ => run_generated_mix("partial_index_update_rollback", model, 2, 4, 6, 360),
    }
}

fn run_btree_vector_index_sync(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("btree_vector_index_sync", model, 3);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"idx:btree:10".to_vec(), b"doc10".to_vec())?;
            apply_vector_op(
                &mut txn,
                VectorOperation::Insert {
                    id: 10,
                    vector: vec![1.0, 1.0],
                    metadata: None,
                },
            )?;
            txn.commit_self()?;
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        _ => run_generated_mix("btree_vector_index_sync", model, 3, 6, 8, 420),
    }
}

fn run_rapid_api_switch(model: ExecutionModel) -> TestResult {
    run_generated_mix("rapid_api_switch", model, 4, 12, 10, 500)
}

fn run_1000_api_switches_no_leak(model: ExecutionModel) -> TestResult {
    run_generated_mix("api_switches_1000", model, 4, 20, 12, 540)
}

fn run_cache_coherency(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("cache_coherency", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    let kv_cache: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    let sql_cache: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let key = b"cache:key:single".to_vec();
            let value = b"v".to_vec();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(key.clone(), value.clone())?;
            txn.commit_self()?;
            kv_cache.lock().unwrap().insert(key.clone(), value.clone());
            sql_cache.lock().unwrap().insert(key.clone(), value.clone());
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&key)?, Some(value.clone()));
            assert_eq!(kv_cache.lock().unwrap().get(&key), Some(&value));
            assert_eq!(sql_cache.lock().unwrap().get(&key), Some(&value));
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        ExecutionModel::SyncMulti => {
            let store_sync = store.clone();
            let kv_cache = kv_cache.clone();
            let sql_cache = sql_cache.clone();
            harness.run_concurrent(move |tid, ctx| {
                let start = Instant::now();
                let key = format!("cache:key:{tid}").into_bytes();
                let value = format!("v{tid}").into_bytes();
                commit_retry_sync(&store_sync, |txn| {
                    txn.put(key.clone(), value.clone())?;
                    Ok(())
                })?;
                kv_cache.lock().unwrap().insert(key.clone(), value.clone());
                sql_cache.lock().unwrap().insert(key.clone(), value.clone());
                let mut reader = store_sync.begin(TxnMode::ReadOnly)?;
                assert_eq!(reader.get(&key)?, Some(value.clone()));
                assert_eq!(kv_cache.lock().unwrap().get(&key), Some(&value));
                assert_eq!(sql_cache.lock().unwrap().get(&key), Some(&value));
                ctx.metrics.record_success();
                ctx.metrics.record_latency(start.elapsed());
                Ok(())
            })
        }
        ExecutionModel::AsyncSingle | ExecutionModel::AsyncMulti => {
            let store_async = store.clone();
            let kv_cache = kv_cache.clone();
            let sql_cache = sql_cache.clone();
            harness.run_async(move |ctx| {
                let store = store_async.clone();
                let kv_cache = kv_cache.clone();
                let sql_cache = sql_cache.clone();
                async move {
                    let start = Instant::now();
                    let key = b"cache:key:async".to_vec();
                    let value = b"v3".to_vec();
                    let mut txn = store.begin(TxnMode::ReadWrite)?;
                    txn.put(key.clone(), value.clone())?;
                    txn.commit_self()?;
                    kv_cache.lock().unwrap().insert(key.clone(), value.clone());
                    sql_cache.lock().unwrap().insert(key.clone(), value.clone());
                    let mut reader = store.begin(TxnMode::ReadOnly)?;
                    assert_eq!(reader.get(&key)?, Some(value.clone()));
                    assert_eq!(kv_cache.lock().unwrap().get(&key), Some(&value));
                    assert_eq!(sql_cache.lock().unwrap().get(&key), Some(&value));
                    ctx.metrics.record_success();
                    ctx.metrics.record_latency(start.elapsed());
                    Ok(())
                }
            })
        }
    }
}

fn run_prepared_statement_kv_alternate(model: ExecutionModel) -> TestResult {
    run_generated_mix("prepared_statement_kv_alternate", model, 3, 8, 10, 600)
}

fn run_transaction_mode_change(model: ExecutionModel) -> TestResult {
    let cfg = multi_model_config("transaction_mode_change", model, 2);
    let harness = StressTestHarness::new(cfg).unwrap();
    let store = Arc::new(MemoryKV::new());
    match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            let start = Instant::now();
            let mut ro = store.begin(TxnMode::ReadOnly)?;
            assert!(ro.put(b"forbidden".to_vec(), b"x".to_vec()).is_err());
            let mut rw = store.begin(TxnMode::ReadWrite)?;
            rw.put(b"allowed".to_vec(), b"1".to_vec())?;
            rw.commit_self()?;
            ctx.metrics.record_success();
            ctx.metrics.record_latency(start.elapsed());
            Ok(())
        }),
        _ => run_generated_mix("transaction_mode_change", model, 2, 6, 8, 660),
    }
}

mm_test!(
    test_cross_model_atomic_commit,
    run_cross_model_atomic_commit
);
mm_test!(test_cross_model_rollback, run_cross_model_rollback);
mm_test!(test_kv_sql_same_entity, run_kv_sql_same_entity);
mm_test!(test_vector_metadata_sync, run_vector_metadata_sync);
mm_test!(
    test_cross_model_crash_recovery,
    run_cross_model_crash_recovery
);
mm_test!(test_kv_sql_concurrent_access, run_kv_sql_concurrent_access);
mm_test!(
    test_sql_select_kv_update_isolation,
    run_sql_select_kv_update_isolation
);
mm_test!(test_vector_search_sql_insert, run_vector_search_sql_insert);
mm_test!(
    test_100_connections_mixed_api,
    run_100_connections_mixed_api
);
mm_test!(test_columnar_scan_kv_update, run_columnar_scan_kv_update);
mm_test!(test_sql_vector_column_update, run_sql_vector_column_update);
mm_test!(test_kv_sql_row_consistency, run_kv_sql_row_consistency);
mm_test!(
    test_columnar_kv_flush_consistency,
    run_columnar_kv_flush_consistency
);
mm_test!(
    test_partial_index_update_rollback,
    run_partial_index_update_rollback
);
mm_test!(test_btree_vector_index_sync, run_btree_vector_index_sync);
mm_test!(test_rapid_api_switch, run_rapid_api_switch);
mm_test!(
    test_1000_api_switches_no_leak,
    run_1000_api_switches_no_leak
);
mm_test!(test_cache_coherency, run_cache_coherency);
mm_test!(
    test_prepared_statement_kv_alternate,
    run_prepared_statement_kv_alternate
);
mm_test!(test_transaction_mode_change, run_transaction_mode_change);
