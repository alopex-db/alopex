mod common;

use alopex_core::{Error as CoreError, KVStore, KVTransaction, MemoryKV, TxnMode};
use common::{ExecutionModel, Lane, StressTestConfig, StressTestHarness};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_TXN_RETRIES: usize = 10;

#[derive(Clone, Debug, Serialize)]
enum OpKind {
    Read,
    Write,
}

#[derive(Clone, Debug, Serialize)]
struct OpRecord {
    id: usize,
    kind: OpKind,
    value: Option<u64>,
    result: Option<u64>,
    start_ns: u128,
    end_ns: u128,
}

#[derive(Clone, Debug, Serialize)]
struct LinearizabilityReport {
    passed: bool,
    ops: Vec<OpRecord>,
}

fn linearizability_config() -> StressTestConfig {
    StressTestConfig {
        name: "linearizability_register".to_string(),
        lane: Lane::Nightly,
        execution_model: ExecutionModel::SyncMulti,
        concurrency: 4,
        scenario_timeout: Duration::from_secs(20),
        operation_timeout: Duration::from_secs(3),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: None,
    }
}

fn is_linearizable(ops: &[OpRecord]) -> bool {
    let n = ops.len();
    if n == 0 {
        return true;
    }
    if n > 60 {
        return false;
    }

    let mut pred_mask = vec![0u64; n];
    for i in 0..n {
        for j in 0..n {
            if i != j && ops[i].end_ns <= ops[j].start_ns {
                pred_mask[j] |= 1u64 << i;
            }
        }
    }

    let all_mask = (1u64 << n) - 1;
    let mut memo: BTreeMap<(u64, u64), bool> = BTreeMap::new();

    fn search(
        ops: &[OpRecord],
        pred_mask: &[u64],
        done: u64,
        current: Option<u64>,
        all_mask: u64,
        memo: &mut BTreeMap<(u64, u64), bool>,
    ) -> bool {
        if done == all_mask {
            return true;
        }
        let key = (done, current.unwrap_or(u64::MAX));
        if let Some(result) = memo.get(&key) {
            return *result;
        }

        for i in 0..ops.len() {
            let bit = 1u64 << i;
            if done & bit != 0 {
                continue;
            }
            if pred_mask[i] & !done != 0 {
                continue;
            }
            let op = &ops[i];
            match op.kind {
                OpKind::Write => {
                    if search(ops, pred_mask, done | bit, op.value, all_mask, memo) {
                        memo.insert(key, true);
                        return true;
                    }
                }
                OpKind::Read => {
                    if op.result != current {
                        continue;
                    }
                    if search(ops, pred_mask, done | bit, current, all_mask, memo) {
                        memo.insert(key, true);
                        return true;
                    }
                }
            }
        }

        memo.insert(key, false);
        false
    }

    search(ops, &pred_mask, 0, None, all_mask, &mut memo)
}

fn write_linearizability_artifact(ctx: &common::TestContext, report: &LinearizabilityReport) {
    let Some(paths) = ctx.artifact_paths.as_ref() else {
        return;
    };
    let path = paths.checks_dir.join("linearizability.json");
    if let Ok(body) = serde_json::to_string_pretty(report) {
        let _ = std::fs::write(path, body);
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_linearizability_register() {
    let harness = StressTestHarness::new(linearizability_config()).unwrap();
    let result = harness.run(|ctx| {
        let store = Arc::new(MemoryKV::new());
        let history: Arc<Mutex<Vec<OpRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let base = Instant::now();
        let metrics = ctx.metrics.clone();

        std::thread::scope(|scope| {
            let worker_count = 4usize;
            for worker in 0..worker_count {
                let store = store.clone();
                let history = history.clone();
                let metrics = metrics.clone();
                let seed = ctx.seed ^ ((worker as u64) << 8);
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(seed);
                    for op_idx in 0..6usize {
                        let op_id = worker * 100 + op_idx;
                        let is_write = rng.gen_bool(0.5);
                        let start = base.elapsed().as_nanos();
                        if is_write {
                            let value = rng.gen_range(1..1000) as u64;
                            let mut attempts = 0;
                            loop {
                                let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
                                txn.put(b"linear".to_vec(), value.to_be_bytes().to_vec())
                                    .unwrap();
                                match txn.commit_self() {
                                    Ok(_) => break,
                                    Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                                        attempts += 1;
                                        std::thread::yield_now();
                                        continue;
                                    }
                                    Err(e) => panic!("write failed: {e:?}"),
                                }
                            }
                            let end = base.elapsed().as_nanos();
                            metrics.record_success();
                            metrics.record_latency(Duration::from_nanos(
                                end.saturating_sub(start) as u64
                            ));
                            history.lock().unwrap().push(OpRecord {
                                id: op_id,
                                kind: OpKind::Write,
                                value: Some(value),
                                result: None,
                                start_ns: start,
                                end_ns: end,
                            });
                        } else {
                            let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
                            let got = txn.get(&b"linear".to_vec()).unwrap();
                            let result = got
                                .as_ref()
                                .and_then(|v| v.as_slice().try_into().ok())
                                .map(u64::from_be_bytes);
                            let end = base.elapsed().as_nanos();
                            metrics.record_success();
                            metrics.record_latency(Duration::from_nanos(
                                end.saturating_sub(start) as u64
                            ));
                            history.lock().unwrap().push(OpRecord {
                                id: op_id,
                                kind: OpKind::Read,
                                value: None,
                                result,
                                start_ns: start,
                                end_ns: end,
                            });
                        }
                    }
                });
            }
        });

        let mut ops = history.lock().unwrap().clone();
        ops.sort_by_key(|op| op.start_ns);
        let passed = is_linearizable(&ops);
        let report = LinearizabilityReport { passed, ops };
        write_linearizability_artifact(ctx, &report);
        assert!(passed, "linearizability violation: {report:?}");
        Ok::<(), CoreError>(())
    });

    assert!(
        result.is_success(),
        "linearizability_register: {:?}",
        result.failure_summary()
    );
}

#[derive(Clone, Debug, Serialize)]
struct TxnRecord {
    id: usize,
    start_ns: u128,
    end_ns: u128,
    read_a: u64,
    read_b: u64,
    write_a: Option<u64>,
    write_b: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct SerializabilityReport {
    passed: bool,
    txns: Vec<TxnRecord>,
}

fn serializability_config() -> StressTestConfig {
    StressTestConfig {
        name: "serializability_two_key".to_string(),
        lane: Lane::Nightly,
        execution_model: ExecutionModel::SyncMulti,
        concurrency: 3,
        scenario_timeout: Duration::from_secs(20),
        operation_timeout: Duration::from_secs(3),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: None,
    }
}

fn is_serializable(txns: &[TxnRecord]) -> bool {
    let n = txns.len();
    if n == 0 {
        return true;
    }
    if n > 60 {
        return false;
    }

    let mut pred_mask = vec![0u64; n];
    for i in 0..n {
        for j in 0..n {
            if i != j && txns[i].end_ns <= txns[j].start_ns {
                pred_mask[j] |= 1u64 << i;
            }
        }
    }
    let all_mask = (1u64 << n) - 1;
    let mut memo: BTreeMap<(u64, u64, u64), bool> = BTreeMap::new();

    fn search(
        txns: &[TxnRecord],
        pred_mask: &[u64],
        done: u64,
        state_a: u64,
        state_b: u64,
        all_mask: u64,
        memo: &mut BTreeMap<(u64, u64, u64), bool>,
    ) -> bool {
        if done == all_mask {
            return true;
        }
        let key = (done, state_a, state_b);
        if let Some(result) = memo.get(&key) {
            return *result;
        }
        for i in 0..txns.len() {
            let bit = 1u64 << i;
            if done & bit != 0 {
                continue;
            }
            if pred_mask[i] & !done != 0 {
                continue;
            }
            let txn = &txns[i];
            if txn.read_a != state_a || txn.read_b != state_b {
                continue;
            }
            let next_a = txn.write_a.unwrap_or(state_a);
            let next_b = txn.write_b.unwrap_or(state_b);
            if search(txns, pred_mask, done | bit, next_a, next_b, all_mask, memo) {
                memo.insert(key, true);
                return true;
            }
        }
        memo.insert(key, false);
        false
    }

    search(txns, &pred_mask, 0, 0, 0, all_mask, &mut memo)
}

fn write_serializability_artifact(ctx: &common::TestContext, report: &SerializabilityReport) {
    let Some(paths) = ctx.artifact_paths.as_ref() else {
        return;
    };
    let path = paths.checks_dir.join("serializability.json");
    if let Ok(body) = serde_json::to_string_pretty(report) {
        let _ = std::fs::write(path, body);
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_serializability_two_key() {
    let harness = StressTestHarness::new(serializability_config()).unwrap();
    let result = harness.run(|ctx| {
        let store = Arc::new(MemoryKV::new());
        let base = Instant::now();
        let history: Arc<Mutex<Vec<TxnRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let metrics = ctx.metrics.clone();

        std::thread::scope(|scope| {
            for worker in 0..3usize {
                let store = store.clone();
                let history = history.clone();
                let metrics = metrics.clone();
                let seed = ctx.seed ^ ((worker as u64) << 4);
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(seed);
                    let mut attempts = 0;
                    loop {
                        let start = base.elapsed().as_nanos();
                        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
                        let read_a = txn
                            .get(&b"a".to_vec())
                            .unwrap()
                            .and_then(|v| v.as_slice().try_into().ok())
                            .map(u64::from_be_bytes)
                            .unwrap_or(0);
                        let read_b = txn
                            .get(&b"b".to_vec())
                            .unwrap()
                            .and_then(|v| v.as_slice().try_into().ok())
                            .map(u64::from_be_bytes)
                            .unwrap_or(0);
                        let write_a = if rng.gen_bool(0.5) {
                            Some(read_a + 1 + worker as u64)
                        } else {
                            None
                        };
                        let write_b = if write_a.is_none() {
                            Some(read_b + 1 + worker as u64)
                        } else {
                            None
                        };
                        if let Some(val) = write_a {
                            txn.put(b"a".to_vec(), val.to_be_bytes().to_vec()).unwrap();
                        }
                        if let Some(val) = write_b {
                            txn.put(b"b".to_vec(), val.to_be_bytes().to_vec()).unwrap();
                        }
                        match txn.commit_self() {
                            Ok(_) => {
                                let end = base.elapsed().as_nanos();
                                metrics.record_success();
                                metrics.record_latency(Duration::from_nanos(
                                    end.saturating_sub(start) as u64,
                                ));
                                history.lock().unwrap().push(TxnRecord {
                                    id: worker,
                                    start_ns: start,
                                    end_ns: end,
                                    read_a,
                                    read_b,
                                    write_a,
                                    write_b,
                                });
                                break;
                            }
                            Err(CoreError::TxnConflict) if attempts < MAX_TXN_RETRIES => {
                                attempts += 1;
                                std::thread::yield_now();
                                continue;
                            }
                            Err(e) => panic!("txn failed: {e:?}"),
                        }
                    }
                });
            }
        });

        let mut txns = history.lock().unwrap().clone();
        txns.sort_by_key(|txn| txn.start_ns);
        let passed = is_serializable(&txns);
        let report = SerializabilityReport { passed, txns };
        write_serializability_artifact(ctx, &report);
        assert!(passed, "serializability violation: {report:?}");
        Ok::<(), CoreError>(())
    });

    assert!(
        result.is_success(),
        "serializability_two_key: {:?}",
        result.failure_summary()
    );
}
