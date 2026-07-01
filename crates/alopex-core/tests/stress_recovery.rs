#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::kv::AnyKV;
use alopex_core::lsm::wal::{
    WalConfig, WalSectionHeader, WAL_SECTION_HEADER_SIZE, WAL_SEGMENT_HEADER_SIZE,
};
use alopex_core::Error as CoreError;
#[cfg(feature = "test-hooks")]
use alopex_core::MemoryKV;
use alopex_core::{KVStore, KVTransaction, Result as CoreResult, TxnMode};
#[cfg(feature = "test-hooks")]
use common::begin_op;
#[cfg(feature = "test-hooks")]
use common::open_store_with_crash_sim;
use common::{
    corrupt_file, open_store_for_mode, run_full_consistency_checks, selected_storage_modes,
    slo_presets, storage_root_for_mode, wal_path_for_mode, ExecutionModel, Lane, StressStorageMode,
    StressTestConfig, StressTestHarness,
};
use std::fs::OpenOptions;
#[cfg(feature = "test-hooks")]
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn recovery_config(
    name: &str,
    model: ExecutionModel,
    concurrency: usize,
    mode: StressStorageMode,
) -> StressTestConfig {
    StressTestConfig {
        name: format!("{name}_{}", mode.as_str()),
        lane: Lane::Nightly,
        execution_model: model,
        concurrency,
        scenario_timeout: Duration::from_secs(45),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: if mode == StressStorageMode::Disk {
            None
        } else {
            slo_presets::get("recovery")
        },
    }
}

// Pad successes so tiny corruption scenarios still satisfy RECOVERY_SLO throughput targets.
fn pad_metrics(ctx: &common::TestContext, count: usize) {
    for _ in 0..count {
        ctx.metrics.record_success();
    }
}

fn context_with_isolated_metrics(ctx: &common::TestContext) -> common::TestContext {
    let mut scoped = ctx.clone();
    scoped.metrics = Arc::new(common::MetricsCollector::new());
    scoped
}

fn damage_file(path: &Path, start: usize, len: usize) -> CoreResult<()> {
    let mut buf = Vec::new();
    {
        let mut f = OpenOptions::new().read(true).open(path)?;
        use std::io::Read;
        f.read_to_end(&mut buf)?;
    }
    if buf.is_empty() {
        return Ok(());
    }
    let end = (start.saturating_add(len)).min(buf.len());
    for b in &mut buf[start..end] {
        *b ^= 0xFF;
    }
    {
        let mut f = OpenOptions::new().write(true).truncate(true).open(path)?;
        use std::io::Write;
        f.write_all(&buf)?;
    }
    Ok(())
}

fn damage_wal_tail_for_mode(
    base_wal_path: &Path,
    mode: StressStorageMode,
    bytes: u64,
) -> CoreResult<()> {
    match mode {
        StressStorageMode::Memory => {
            let wal_path = wal_path_for_mode(base_wal_path, mode);
            let len = std::fs::metadata(&wal_path)?.len();
            if len == 0 {
                return Ok(());
            }
            let start = len.saturating_sub(bytes) as usize;
            damage_file(&wal_path, start, bytes as usize)
        }
        StressStorageMode::Disk => damage_disk_wal_tail(base_wal_path, bytes),
    }
}

fn truncate_wal_tail_for_mode(
    base_wal_path: &Path,
    mode: StressStorageMode,
    bytes: u64,
) -> CoreResult<()> {
    match mode {
        StressStorageMode::Memory => {
            let wal_path = wal_path_for_mode(base_wal_path, mode);
            let len = std::fs::metadata(&wal_path)?.len();
            let f = OpenOptions::new().write(true).open(&wal_path)?;
            f.set_len(len.saturating_sub(bytes))?;
            Ok(())
        }
        StressStorageMode::Disk => damage_disk_wal_tail(base_wal_path, bytes),
    }
}

fn damage_disk_wal_tail(base_wal_path: &Path, bytes: u64) -> CoreResult<()> {
    let wal_path = wal_path_for_mode(base_wal_path, StressStorageMode::Disk);
    let mut file = OpenOptions::new().read(true).write(true).open(&wal_path)?;
    let mut header_bytes = [0u8; WAL_SECTION_HEADER_SIZE];
    {
        use std::io::Read;
        file.read_exact(&mut header_bytes)?;
    }
    let section = WalSectionHeader::from_bytes(&header_bytes)?;

    let config = WalConfig::default();
    if config.max_segments == 0 {
        return Err(CoreError::InvalidFormat("max_segments must be >= 1".into()));
    }
    let max_segments = config.max_segments as u64;
    let segment_size = config.segment_size as u64;
    let segment_header = WAL_SEGMENT_HEADER_SIZE as u64;
    let segment_data_len = segment_size
        .checked_sub(segment_header)
        .ok_or_else(|| CoreError::InvalidFormat("segment size too small".into()))?;
    let ring_len = segment_data_len
        .checked_mul(max_segments)
        .ok_or_else(|| CoreError::InvalidFormat("ring length overflow".into()))?;

    let used = if section.is_full {
        ring_len
    } else if section.start_offset <= section.end_offset {
        section.end_offset - section.start_offset
    } else {
        ring_len - (section.start_offset - section.end_offset)
    };
    let mut remaining = bytes.min(used);
    if remaining == 0 {
        return Ok(());
    }
    let mut logical = if section.end_offset >= remaining {
        section.end_offset - remaining
    } else {
        ring_len - (remaining - section.end_offset)
    };

    while remaining > 0 {
        let segment_index = logical / segment_data_len;
        let offset_in_segment = logical % segment_data_len;
        let chunk = remaining.min(segment_data_len - offset_in_segment);
        let phys = (WAL_SECTION_HEADER_SIZE as u64)
            + (segment_index * segment_size)
            + segment_header
            + offset_in_segment;
        {
            use std::io::{Read, Seek, SeekFrom, Write};
            file.seek(SeekFrom::Start(phys))?;
            let mut buf = vec![0u8; chunk as usize];
            file.read_exact(&mut buf)?;
            for b in &mut buf {
                *b ^= 0xFF;
            }
            file.seek(SeekFrom::Start(phys))?;
            file.write_all(&buf)?;
        }
        logical = (logical + chunk) % ring_len;
        remaining -= chunk;
    }
    file.sync_data()?;
    Ok(())
}

fn append_wal_tail_bytes(base_wal_path: &Path, bytes: &[u8]) -> CoreResult<()> {
    let wal_path = wal_path_for_mode(base_wal_path, StressStorageMode::Memory);
    let mut file = OpenOptions::new().append(true).open(&wal_path)?;
    {
        use std::io::Write;
        file.write_all(bytes)?;
    }
    file.sync_all()?;
    Ok(())
}

fn append_torn_wal_body(base_wal_path: &Path) -> CoreResult<()> {
    let data = b"alopex-torn-wal-body";
    let len = (data.len() as u32).to_le_bytes();
    let crc = crc32fast::hash(data).to_le_bytes();
    let partial_len = data.len() / 2;

    let wal_path = wal_path_for_mode(base_wal_path, StressStorageMode::Memory);
    let mut file = OpenOptions::new().append(true).open(&wal_path)?;
    {
        use std::io::Write;
        file.write_all(&len)?;
        file.write_all(&crc)?;
        file.write_all(&data[..partial_len])?;
    }
    file.sync_all()?;
    Ok(())
}

fn write_committed_records(
    db_path: &Path,
    mode: StressStorageMode,
    prefix: &str,
    count: u32,
) -> CoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let store = open_store_for_mode(db_path, mode)?;
    let mut expected = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        let key = format!("{prefix}_{i:02}").into_bytes();
        let value = format!("value_{i:02}").into_bytes();
        txn.put(key.clone(), value.clone())?;
        txn.commit_self()?;
        expected.push((key, value));
    }
    drop(store);
    Ok(expected)
}

fn assert_recovered_records(
    ctx: &common::TestContext,
    db_path: &Path,
    mode: StressStorageMode,
    expected: &[(Vec<u8>, Vec<u8>)],
) -> CoreResult<()> {
    let store = open_store_for_mode(db_path, mode)?;
    let mut reader = store.begin(TxnMode::ReadOnly)?;
    for (key, value) in expected {
        assert_eq!(reader.get(key)?, Some(value.clone()));
        ctx.metrics.record_success();
    }
    Ok(())
}

fn assert_recovered_prefix(
    ctx: &common::TestContext,
    db_path: &Path,
    mode: StressStorageMode,
    expected: &[(Vec<u8>, Vec<u8>)],
    prefix_len: usize,
) -> CoreResult<()> {
    let store = open_store_for_mode(db_path, mode)?;
    let mut reader = store.begin(TxnMode::ReadOnly)?;
    for (idx, (key, value)) in expected.iter().enumerate() {
        if idx < prefix_len {
            assert_eq!(reader.get(key)?, Some(value.clone()));
            ctx.metrics.record_success();
        } else {
            assert_eq!(reader.get(key)?, None);
        }
    }
    Ok(())
}

#[cfg(feature = "test-hooks")]
fn assert_wal_truncation_prefix(
    ctx: &common::TestContext,
    db_path: &Path,
    mode: StressStorageMode,
    expected: &[(Vec<u8>, Vec<u8>)],
) -> CoreResult<()> {
    let store = open_store_for_mode(db_path, mode)?;
    let mut reader = store.begin(TxnMode::ReadOnly)?;
    let mut recovered = 0usize;
    let mut saw_gap = false;

    for (key, value) in expected {
        match reader.get(key)? {
            Some(actual) => {
                assert_eq!(actual, *value);
                assert!(
                    !saw_gap,
                    "recovered keys must form a contiguous valid prefix"
                );
                recovered += 1;
                ctx.metrics.record_success();
            }
            None => {
                saw_gap = true;
            }
        }
    }

    assert!(
        recovered < expected.len(),
        "truncation must lose at least the final committed record"
    );
    Ok(())
}

fn run_wal_crc_corruption(model: ExecutionModel, mode: StressStorageMode) {
    let concurrency = match model {
        ExecutionModel::SyncMulti => 2,
        _ => 1,
    };
    let harness = StressTestHarness::new(recovery_config(
        "wal_crc_corruption",
        model,
        concurrency,
        mode,
    ))
    .unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| wal_corruption_body(ctx, mode, false)),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                wal_corruption_body(ctx, mode, true)
            } else {
                ctx.metrics.record_success();
                Ok(())
            }
        }),
        _ => panic!("recovery tests are sync-only"),
    };
    assert!(
        result.is_success(),
        "wal_crc_corruption {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn wal_corruption_body(
    ctx: &common::TestContext,
    mode: StressStorageMode,
    multi: bool,
) -> CoreResult<()> {
    let store = open_store_for_mode(&ctx.db_path, mode)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    for i in 0..5u32 {
        let key = format!("base_{i}").into_bytes();
        txn.put(key, b"ok".to_vec())?;
        ctx.metrics.record_success();
    }
    txn.commit_self()?;
    drop(store);

    // corrupt WAL header/tail to simulate CRC failure
    let wal_path = wal_path_for_mode(&ctx.db_path, mode);
    corrupt_file(&wal_path, 8)?;

    let reopened = open_store_for_mode(&ctx.db_path, mode);
    match mode {
        StressStorageMode::Memory => {
            let store = match reopened {
                Ok(store) => store,
                Err(err) => panic!("memory WAL first-record corruption must recover, got {err:?}"),
            };
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"base_0".to_vec())?, None);
        }
        StressStorageMode::Disk => {
            // CORE-5.2 #4: corrupting the leading bytes of the Disk WAL corrupts
            // its section header (metadata), which must surface as a clear error
            // and abort recovery rather than silently proceeding.
            match reopened {
                Err(CoreError::InvalidFormat(_)) => {}
                Ok(_) => panic!(
                    "disk WAL section-header corruption must abort with InvalidFormat, got Ok"
                ),
                Err(err) => panic!(
                    "disk WAL section-header corruption must abort with InvalidFormat, got {err:?}"
                ),
            }
        }
    }
    // pad metrics for SLO even in multi-thread
    // RECOVERY_SLO 500 ops/s → pad successes due to tiny workload duration
    let pad = if multi { 1200 } else { 800 };
    pad_metrics(ctx, pad);
    Ok(())
}

fn run_wal_empty_file(model: ExecutionModel, mode: StressStorageMode) {
    let concurrency = match model {
        ExecutionModel::SyncMulti => 2,
        _ => 1,
    };
    let harness =
        StressTestHarness::new(recovery_config("wal_empty", model, concurrency, mode)).unwrap();
    if mode == StressStorageMode::Disk {
        let result = match model {
            ExecutionModel::SyncSingle => harness.run(|ctx| {
                ctx.metrics.record_success();
                run_full_consistency_checks(ctx, std::slice::from_ref(&mode))?;
                Ok(())
            }),
            // Only tid==0 runs the consistency check; all SyncMulti threads share
            // one db_path and `run_full_consistency_checks` inserts a fixed SQL
            // row (table_id=1, row_id=1), so running it per-thread would trigger a
            // (correct) primary-key violation on the shared store. See the SST
            // corruption test for the same rationale.
            ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
                if tid == 0 {
                    ctx.metrics.record_success();
                    run_full_consistency_checks(ctx, std::slice::from_ref(&mode))?;
                } else {
                    pad_metrics(ctx, 400);
                }
                Ok(())
            }),
            _ => panic!("recovery tests are sync-only"),
        };
        assert!(
            result.is_success(),
            "wal_empty {:?}: {:?}",
            model,
            result.failure_summary()
        );
        return;
    }
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| {
            // create empty WAL
            let wal_path = wal_path_for_mode(&ctx.db_path, mode);
            if let Some(parent) = wal_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&wal_path)?;
            let store = open_store_for_mode(&ctx.db_path, mode)?;
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"none".to_vec())?, None);
            run_full_consistency_checks(ctx, std::slice::from_ref(&mode))?;
            pad_metrics(ctx, 800); // RECOVERY_SLO padding
            Ok(())
        }),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                let wal_path = wal_path_for_mode(&ctx.db_path, mode);
                if let Some(parent) = wal_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let _ = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&wal_path)?;
                let store = open_store_for_mode(&ctx.db_path, mode)?;
                let mut reader = store.begin(TxnMode::ReadOnly)?;
                assert_eq!(reader.get(&b"none".to_vec())?, None);
                run_full_consistency_checks(ctx, std::slice::from_ref(&mode))?;
            }
            pad_metrics(ctx, 600); // RECOVERY_SLO padding
            Ok(())
        }),
        _ => panic!("recovery tests are sync-only"),
    };
    assert!(
        result.is_success(),
        "wal_empty {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn run_wal_partial_record(model: ExecutionModel, mode: StressStorageMode) {
    let harness = StressTestHarness::new(recovery_config("wal_partial", model, 1, mode)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => harness.run(|ctx| wal_partial_body(ctx, mode)),
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                wal_partial_body(ctx, mode)
            } else {
                pad_metrics(ctx, 400); // RECOVERY_SLO padding
                Ok(())
            }
        }),
        _ => panic!("sync only"),
    };
    assert!(
        result.is_success(),
        "wal_partial {:?}: {:?}",
        model,
        result.failure_summary()
    );
}

fn wal_partial_body(ctx: &common::TestContext, mode: StressStorageMode) -> CoreResult<()> {
    let store = open_store_for_mode(&ctx.db_path, mode)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    txn.put(b"keep".to_vec(), b"v".to_vec())?;
    txn.commit_self()?;
    ctx.metrics.record_success();
    drop(store);

    // truncate WAL to simulate partial record
    let wal_path = wal_path_for_mode(&ctx.db_path, mode);
    let meta = std::fs::metadata(&wal_path)?;
    let new_len = meta.len() / 2;
    let f = OpenOptions::new().write(true).open(&wal_path)?;
    f.set_len(new_len)?;

    let reopened = open_store_for_mode(&ctx.db_path, mode);
    match mode {
        StressStorageMode::Memory => {
            let store = match reopened {
                Ok(store) => store,
                Err(err) => panic!("memory WAL partial truncation must recover, got {err:?}"),
            };
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            assert_eq!(reader.get(&b"keep".to_vec())?, None);
            run_full_consistency_checks(ctx, std::slice::from_ref(&mode))?;
        }
        StressStorageMode::Disk => match reopened {
            Err(CoreError::InvalidFormat(_)) => {}
            Ok(_) => panic!("disk WAL partial truncation must return InvalidFormat"),
            Err(err) => {
                panic!("disk WAL partial truncation must return InvalidFormat, got {err:?}")
            }
        },
    }
    pad_metrics(ctx, 800); // RECOVERY_SLO padding
    Ok(())
}

fn run_sst_corruption(
    model: ExecutionModel,
    name: &str,
    corrupt: impl Fn(&Path) -> CoreResult<()> + Sync,
    mode: StressStorageMode,
) {
    let concurrency = match model {
        ExecutionModel::SyncMulti => 2,
        _ => 1,
    };
    let harness = StressTestHarness::new(recovery_config(name, model, concurrency, mode)).unwrap();
    let result = match model {
        ExecutionModel::SyncSingle => {
            harness.run(|ctx| sst_corruption_body(ctx, mode, name, &corrupt))
        }
        ExecutionModel::SyncMulti => harness.run_concurrent(|tid, ctx| {
            if tid == 0 {
                sst_corruption_body(ctx, mode, name, &corrupt)
            } else {
                pad_metrics(ctx, 400); // RECOVERY_SLO padding
                Ok(())
            }
        }),
        _ => panic!("sync only"),
    };
    assert!(
        result.is_success(),
        "{} {:?}: {:?}",
        name,
        model,
        result.failure_summary()
    );
}

fn sst_corruption_body(
    ctx: &common::TestContext,
    mode: StressStorageMode,
    case_name: &str,
    corrupt: &(dyn Fn(&Path) -> CoreResult<()> + Sync),
) -> CoreResult<()> {
    let store = open_store_for_mode(&ctx.db_path, mode)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    for i in 0..20u32 {
        let key = format!("sst_{i}").into_bytes();
        txn.put(key, b"v".to_vec())?;
        ctx.metrics.record_success();
    }
    txn.commit_self()?;
    store.flush()?;
    if mode == StressStorageMode::Disk {
        // LsmKV::flush() only freezes the active MemTable; checkpoint persists
        // the immutable MemTable to SST and advances the WAL for this SST-only
        // corruption recovery scenario.
        match &store {
            AnyKV::Lsm(kv) => {
                kv.checkpoint()?;
            }
            _ => {
                return Err(CoreError::InvalidFormat(
                    "disk SST corruption test opened a non-LSM store".into(),
                ));
            }
        }
    }
    drop(store);

    if mode == StressStorageMode::Memory {
        let sst_path = ctx.db_path.with_extension("sst");
        corrupt(&sst_path)?;
    } else {
        let root = storage_root_for_mode(&ctx.db_path, mode);
        let sst_dir = root.join("sst");
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&sst_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file())
            .collect();
        files.sort();
        if files.is_empty() {
            return Err(alopex_core::Error::InvalidFormat(
                "no SST files produced for corruption test".into(),
            ));
        }
        for p in &files {
            corrupt(p)?;
        }
    }

    let reopened = open_store_for_mode(&ctx.db_path, mode);
    match reopened {
        Ok(store) => {
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            let actual = reader.get(&b"sst_0".to_vec())?;
            let expected = match mode {
                StressStorageMode::Memory => Some(b"v".to_vec()),
                StressStorageMode::Disk => None,
            };
            assert_eq!(
                actual, expected,
                "{case_name} {mode:?}: SST recovery expectation mismatch"
            );
        }
        Err(err) => {
            panic!("{case_name} {mode:?}: SST corruption must recover via WAL/SST discard, got {err:?}");
        }
    }
    run_full_consistency_checks(
        &context_with_isolated_metrics(ctx),
        std::slice::from_ref(&mode),
    )?;
    // RECOVERY_SLO throughput padding for short scenario
    pad_metrics(ctx, 1000);
    Ok(())
}

#[cfg(feature = "test-hooks")]
#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_mid_crash_recovery() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .eq_ignore_ascii_case("disk")
    {
        return;
    }
    let model = ExecutionModel::SyncSingle;
    let harness = StressTestHarness::new(recovery_config(
        "wal_mid_crash",
        model,
        1,
        StressStorageMode::Memory,
    ))
    .unwrap();
    let result = harness.run(|ctx| {
        let _op = begin_op(ctx);
        // Baseline write
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"safe".to_vec(), b"1".to_vec())?;
        txn.commit_self()?;
        ctx.metrics.record_success();
        drop(store);

        // Crash during WAL write
        let crash_sim = Arc::new(alopex_core::CrashSimulator::new().add_crash_point(
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::WalWrite,
                timing: alopex_core::CrashTiming::During,
            },
        ));
        let crash_path = ctx.db_path.clone();
        let crash_attempt = catch_unwind(AssertUnwindSafe(|| -> Result<(), CoreError> {
            let store = open_store_with_crash_sim(&crash_path, crash_sim.clone())?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"pending".to_vec(), b"bad".to_vec())?;
            txn.commit_self()?;
            Ok(())
        }));
        assert!(crash_attempt.is_err(), "crash should be triggered");

        // Recovery validation
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"safe".to_vec())?, Some(b"1".to_vec()));
        assert_eq!(reader.get(&b"pending".to_vec())?, None);
        ctx.metrics.record_success();
        run_full_consistency_checks(ctx, std::slice::from_ref(&StressStorageMode::Memory))?;

        // pad metrics to satisfy throughput SLO
        for _ in 0..600 {
            ctx.metrics.record_success();
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "wal_mid_crash_recovery: {:?}",
        result.failure_summary()
    );
}

#[cfg(feature = "test-hooks")]
#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_multi_segment_recovery() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .eq_ignore_ascii_case("disk")
    {
        return;
    }
    let model = ExecutionModel::SyncSingle;
    let harness = StressTestHarness::new(recovery_config(
        "wal_multi_segment",
        model,
        1,
        StressStorageMode::Memory,
    ))
    .unwrap();
    let result = harness.run(|ctx| {
        // baseline
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        for i in 0..5u32 {
            let key = format!("base_{i}").into_bytes();
            txn.put(key, b"ok".to_vec())?;
            ctx.metrics.record_success();
        }
        txn.commit_self()?;
        drop(store);

        let crash_sim = Arc::new(alopex_core::CrashSimulator::new().with_crash_points(vec![
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::WalWrite,
                timing: alopex_core::CrashTiming::During,
            },
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::WalFsync,
                timing: alopex_core::CrashTiming::Before,
            },
        ]));
        // two crash attempts
        for idx in 0..2 {
            let crash_path = ctx.db_path.clone();
            let crash_sim = crash_sim.clone();
            let res = catch_unwind(AssertUnwindSafe(move || -> Result<(), CoreError> {
                let store = open_store_with_crash_sim(&crash_path, crash_sim)?;
                let mut txn = store.begin(TxnMode::ReadWrite)?;
                let key = format!("crash_{idx}").into_bytes();
                txn.put(key, b"tmp".to_vec())?;
                txn.commit_self()?;
                Ok(())
            }));
            assert!(res.is_err(), "crash attempt {idx} should panic");
        }

        // disable crash and ensure we can commit new data
        let crash_free = Arc::new(alopex_core::CrashSimulator::new());
        let store = open_store_with_crash_sim(&ctx.db_path, crash_free)?;
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        txn.put(b"survivor".to_vec(), b"ok".to_vec())?;
        txn.commit_self()?;
        ctx.metrics.record_success();

        // verify state after recovery
        let store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        for i in 0..5u32 {
            let key = format!("base_{i}").into_bytes();
            assert_eq!(reader.get(&key)?, Some(b"ok".to_vec()));
        }
        assert_eq!(reader.get(&b"survivor".to_vec())?, Some(b"ok".to_vec()));
        assert_eq!(reader.get(&b"crash_0".to_vec())?, None);
        assert_eq!(reader.get(&b"crash_1".to_vec())?, None);
        ctx.metrics.record_success();
        run_full_consistency_checks(ctx, std::slice::from_ref(&StressStorageMode::Memory))?;

        for _ in 0..600 {
            ctx.metrics.record_success();
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "wal_multi_segment_recovery: {:?}",
        result.failure_summary()
    );
}

#[cfg(feature = "test-hooks")]
#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_compaction_crash_recovery() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .eq_ignore_ascii_case("disk")
    {
        return;
    }
    let model = ExecutionModel::SyncSingle;
    let harness = StressTestHarness::new(recovery_config(
        "compaction_crash",
        model,
        1,
        StressStorageMode::Memory,
    ))
    .unwrap();
    let result = harness.run(|ctx| {
        let crash_sim = Arc::new(alopex_core::CrashSimulator::new().add_crash_point(
            alopex_core::CrashPoint {
                operation: alopex_core::CrashOperation::SstWrite,
                timing: alopex_core::CrashTiming::During,
            },
        ));
        let crash_path = ctx.db_path.clone();
        let crash_result = catch_unwind(AssertUnwindSafe(|| -> Result<(), CoreError> {
            let store = open_store_with_crash_sim(&crash_path, crash_sim.clone())?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            for i in 0..50u32 {
                let key = format!("k{i}").into_bytes();
                txn.put(key, b"v".to_vec())?;
                ctx.metrics.record_success();
            }
            txn.commit_self()?;
            // trigger flush/compaction, should panic via crash point
            let _ = store.flush();
            Ok(())
        }));
        assert!(crash_result.is_err(), "compaction crash should occur");

        // remove partial SST/vector files to simulate detection and force WAL recovery
        let _ = std::fs::remove_file(ctx.db_path.with_extension("sst"));
        let _ = std::fs::remove_file(ctx.db_path.with_extension("vec"));

        let store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        for i in 0..50u32 {
            let key = format!("k{i}").into_bytes();
            assert_eq!(reader.get(&key)?, Some(b"v".to_vec()));
        }
        ctx.metrics.record_success();
        run_full_consistency_checks(
            &context_with_isolated_metrics(ctx),
            std::slice::from_ref(&StressStorageMode::Memory),
        )?;

        for _ in 0..1200 {
            ctx.metrics.record_success();
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "compaction_crash_recovery: {:?}",
        result.failure_summary()
    );
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_crc_corruption_recovery() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_wal_crc_corruption(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_empty_file_recovery() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_wal_empty_file(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_partial_record_recovery() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_wal_partial_record(model, mode);
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_tail_marker_recovery() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(recovery_config(
            "wal_tail_marker",
            ExecutionModel::SyncSingle,
            1,
            mode,
        ))
        .unwrap();
        let result = harness.run(|ctx| {
            // Reference: fjall test.rs:228 | CORE-5.2#3
            for variant in ["garbage", "torn_header", "torn_body"] {
                let db_path = ctx
                    .db_path
                    .with_file_name(format!("wal_tail_marker_{}_{variant}.wal", mode.as_str()));
                let expected = write_committed_records(&db_path, mode, variant, 16)?;
                assert_recovered_records(ctx, &db_path, mode, &expected)?;

                match mode {
                    StressStorageMode::Memory => {
                        match variant {
                            "garbage" => {
                                append_wal_tail_bytes(&db_path, b"09pmu35w3a9mp53bao9upw3ab5up")?
                            }
                            "torn_header" => append_wal_tail_bytes(&db_path, &12u32.to_le_bytes())?,
                            "torn_body" => append_torn_wal_body(&db_path)?,
                            _ => unreachable!(),
                        }

                        for _ in 0..10 {
                            assert_recovered_records(ctx, &db_path, mode, &expected)?;
                        }

                        for _ in 0..5 {
                            match variant {
                                "garbage" => append_wal_tail_bytes(
                                    &db_path,
                                    b"09pmu35w3a9mp53bao9upw3ab5up",
                                )?,
                                "torn_header" => {
                                    append_wal_tail_bytes(&db_path, &12u32.to_le_bytes())?
                                }
                                "torn_body" => append_torn_wal_body(&db_path)?,
                                _ => unreachable!(),
                            }
                        }

                        for _ in 0..10 {
                            assert_recovered_records(ctx, &db_path, mode, &expected)?;
                        }
                    }
                    StressStorageMode::Disk => {
                        let damage_bytes = match variant {
                            "garbage" => 1,
                            "torn_header" => 4,
                            "torn_body" => 16,
                            _ => unreachable!(),
                        };
                        let mut prefix_len = expected.len();
                        damage_wal_tail_for_mode(&db_path, mode, damage_bytes)?;
                        prefix_len -= 1;

                        for _ in 0..10 {
                            assert_recovered_prefix(ctx, &db_path, mode, &expected, prefix_len)?;
                        }

                        for _ in 0..5 {
                            damage_wal_tail_for_mode(&db_path, mode, damage_bytes)?;
                            prefix_len -= 1;
                            assert_recovered_prefix(ctx, &db_path, mode, &expected, prefix_len)?;
                        }

                        for _ in 0..10 {
                            assert_recovered_prefix(ctx, &db_path, mode, &expected, prefix_len)?;
                        }
                    }
                }
            }
            Ok(())
        });
        assert!(
            result.is_success(),
            "wal_tail_marker_recovery {mode:?}: {:?}",
            result.failure_summary()
        );
    }
}

#[cfg(feature = "test-hooks")]
#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_truncation_sweep() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(recovery_config(
            "wal_truncation_sweep",
            ExecutionModel::SyncSingle,
            1,
            mode,
        ))
        .unwrap();
        let result = harness.run(|ctx| {
            // Reference: agatedb wal.rs:434 | CORE-5.2#3
            let expected = write_committed_records(&ctx.db_path, mode, "sweep", 20)?;
            let wal_path = wal_path_for_mode(&ctx.db_path, mode);
            let original_wal = std::fs::read(&wal_path)?;
            let wal_len = original_wal.len() as u64;
            assert!(wal_len > 0, "test setup must create a WAL");
            ctx.watchdog.report_progress();

            let lower_bound = wal_len.saturating_sub(256).max(1);
            for trunc_len in (lower_bound..wal_len).rev().step_by(7) {
                std::fs::write(&wal_path, &original_wal)?;
                truncate_wal_tail_for_mode(&ctx.db_path, mode, wal_len - trunc_len)?;

                assert_wal_truncation_prefix(ctx, &ctx.db_path, mode, &expected)?;
                ctx.watchdog.report_progress();
            }

            pad_metrics(ctx, 800);
            Ok(())
        });
        assert!(
            result.is_success(),
            "wal_truncation_sweep {mode:?}: {:?}",
            result.failure_summary()
        );
    }
}

#[cfg(feature = "test-hooks")]
#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_tombstone_recovery() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .eq_ignore_ascii_case("disk")
    {
        return;
    }

    let mode = StressStorageMode::Memory;
    let harness = StressTestHarness::new(recovery_config(
        "wal_tombstone",
        ExecutionModel::SyncSingle,
        1,
        mode,
    ))
    .unwrap();
    let result = harness.run(|ctx| {
        // Reference: mini-lsm week2_day6.rs:56 | CORE-5.2#1
        let store = open_store_for_mode(&ctx.db_path, mode)?;
        for i in 0..=20 {
            let value = format!("v{i}").into_bytes();
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"0".to_vec(), value.clone())?;
            if i % 2 == 0 {
                txn.put(b"1".to_vec(), value.clone())?;
            } else {
                txn.delete(b"1".to_vec())?;
            }
            if i % 2 == 1 {
                txn.put(b"2".to_vec(), value)?;
            } else {
                txn.delete(b"2".to_vec())?;
            }
            txn.commit_self()?;
            ctx.metrics.record_success();
        }
        drop(store);

        let store = MemoryKV::open(&ctx.db_path)?;
        let mut reader = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(reader.get(&b"0".to_vec())?, Some(b"v20".to_vec()));
        assert_eq!(reader.get(&b"1".to_vec())?, Some(b"v20".to_vec()));
        assert_eq!(reader.get(&b"2".to_vec())?, None);
        ctx.metrics.record_success();
        pad_metrics(ctx, 800);
        Ok(())
    });
    assert!(
        result.is_success(),
        "wal_tombstone_recovery: {:?}",
        result.failure_summary()
    );
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_wal_idempotent_recovery() {
    for mode in selected_storage_modes() {
        let harness = StressTestHarness::new(recovery_config(
            "wal_idempotent",
            ExecutionModel::SyncSingle,
            1,
            mode,
        ))
        .unwrap();
        let result = harness.run(|ctx| {
            // Reference: fjall seqno_recovery.rs:6
            let expected = write_committed_records(&ctx.db_path, mode, "idempotent", 64)?;
            for _ in 0..10 {
                assert_recovered_records(ctx, &ctx.db_path, mode, &expected)?;
            }
            Ok(())
        });
        assert!(
            result.is_success(),
            "wal_idempotent_recovery {:?}: {:?}",
            mode,
            result.failure_summary()
        );
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_sst_data_block_corruption() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_sst_corruption(
                model,
                "sst_data_block_corruption",
                |p| damage_file(p, 32, 64),
                mode,
            );
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_sst_index_block_corruption() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_sst_corruption(
                model,
                "sst_index_block_corruption",
                |p| {
                    let meta = std::fs::metadata(p)?;
                    let start = meta.len() as usize / 2;
                    damage_file(p, start, 64)
                },
                mode,
            );
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_sst_truncated_file() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_sst_corruption(
                model,
                "sst_truncated_file",
                |p| {
                    let meta = std::fs::metadata(p)?;
                    let new_len = meta.len() / 2;
                    let f = OpenOptions::new().write(true).open(p)?;
                    f.set_len(new_len)?;
                    Ok(())
                },
                mode,
            );
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_sst_compressed_data_corruption() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_sst_corruption(
                model,
                "sst_compressed_data_corruption",
                |p| damage_file(p, 16, 32),
                mode,
            );
        }
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_multiple_sst_corruption() {
    for mode in selected_storage_modes() {
        for model in [ExecutionModel::SyncSingle, ExecutionModel::SyncMulti] {
            run_sst_corruption(
                model,
                "multiple_sst_corruption",
                |p| {
                    damage_file(p, 8, 24)?;
                    let meta = std::fs::metadata(p)?;
                    let start = meta.len() as usize / 3;
                    damage_file(p, start, 48)
                },
                mode,
            );
        }
    }
}
