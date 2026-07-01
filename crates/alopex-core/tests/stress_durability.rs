#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::lsm::wal::{WalSectionHeader, WAL_SECTION_HEADER_SIZE, WAL_SEGMENT_HEADER_SIZE};
use alopex_core::{KVStore, KVTransaction, Result as CoreResult, TxnMode};
use common::{
    run_full_consistency_checks, storage_root_for_mode, CrashSimulator, ExecutionModel, Lane,
    StressStorageMode, StressTestConfig, StressTestHarness,
};
use serde::Serialize;
use std::io::{Read, Seek, Write};
use std::time::Duration;

#[derive(Clone, Serialize)]
struct RecoverySnapshot {
    cycle: usize,
    entries_recovered: usize,
    stop_reason: Option<String>,
    checkpoint_lsn: Option<u64>,
}

#[derive(Serialize)]
struct CrashLoopReport {
    cycles: usize,
    verified_keys: usize,
    recoveries: Vec<RecoverySnapshot>,
    missing_keys: Vec<String>,
}

#[derive(Serialize)]
struct WalIntegrityReport {
    power_loss_bytes: u64,
    recovered_keys: usize,
    warnings_empty: bool,
    stop_reason: Option<String>,
    missing_keys: Vec<String>,
}

fn inject_power_loss(root: &std::path::Path) -> CoreResult<(u64, u64)> {
    let wal_path = root.join("lsm.wal");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)?;
    let mut header_bytes = [0u8; WAL_SECTION_HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let section = WalSectionHeader::from_bytes(&header_bytes)?;

    let cfg = alopex_core::lsm::LsmKVConfig::default();
    let max_segments = cfg.wal.max_segments as u64;
    if max_segments == 0 {
        return Err(alopex_core::Error::InvalidFormat(
            "max_segments must be >= 1".into(),
        ));
    }
    let segment_size = cfg.wal.segment_size as u64;
    let segment_header = WAL_SEGMENT_HEADER_SIZE as u64;
    if segment_size <= segment_header {
        return Err(alopex_core::Error::InvalidFormat(
            "segment size too small".into(),
        ));
    }
    let segment_data_len = segment_size - segment_header;
    let ring_len = segment_data_len
        .checked_mul(max_segments)
        .ok_or_else(|| alopex_core::Error::InvalidFormat("ring length overflow".into()))?;

    let unused_offset = (section.end_offset + 16) % ring_len;
    let segment_index = unused_offset / segment_data_len;
    let offset_in_segment = unused_offset % segment_data_len;
    let phys = (WAL_SECTION_HEADER_SIZE as u64)
        + (segment_index * segment_size)
        + segment_header
        + offset_in_segment;
    file.seek(std::io::SeekFrom::Start(phys))?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0xFF;
    file.seek(std::io::SeekFrom::Start(phys))?;
    file.write_all(&byte)?;
    file.sync_data()?;
    Ok((1, phys))
}

fn durability_config(name: &str) -> StressTestConfig {
    StressTestConfig {
        name: name.to_string(),
        lane: Lane::Nightly,
        execution_model: ExecutionModel::SyncSingle,
        concurrency: 1,
        scenario_timeout: Duration::from_secs(45),
        operation_timeout: Duration::from_secs(5),
        metrics_interval: Duration::from_secs(1),
        warmup_ops: 0,
        slo: None,
    }
}

fn write_report<T: Serialize>(ctx: &common::TestContext, file: &str, report: &T) {
    let Some(paths) = ctx.artifact_paths.as_ref() else {
        return;
    };
    let path = paths.checks_dir.join(file);
    if let Ok(body) = serde_json::to_string_pretty(report) {
        let _ = std::fs::write(path, body);
    }
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_durability_crash_loop() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .eq_ignore_ascii_case("memory")
    {
        return;
    }
    let harness = StressTestHarness::new(durability_config("durability_crash_loop")).unwrap();
    let result = harness.run(|ctx| {
        let root = storage_root_for_mode(&ctx.db_path, StressStorageMode::Disk);
        let sim = CrashSimulator::new(&root);
        let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut recoveries = Vec::new();

        for cycle in 0..4usize {
            let (store, recovery) = sim.open_store()?;
            recoveries.push(RecoverySnapshot {
                cycle,
                entries_recovered: recovery.entries_recovered,
                stop_reason: recovery.stop_reason.clone(),
                checkpoint_lsn: recovery.checkpoint_lsn,
            });
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            let key = format!("durable_{cycle}").into_bytes();
            let val = format!("value_{cycle}").into_bytes();
            txn.put(key.clone(), val.clone())?;
            txn.commit_self()?;
            expected.push((key, val));
            drop(store); // simulate crash by dropping the store handle

            let (store, recovery) = sim.open_store()?;
            recoveries.push(RecoverySnapshot {
                cycle: cycle + 100,
                entries_recovered: recovery.entries_recovered,
                stop_reason: recovery.stop_reason.clone(),
                checkpoint_lsn: recovery.checkpoint_lsn,
            });
            let mut reader = store.begin(TxnMode::ReadOnly)?;
            let mut missing_keys = Vec::new();
            for (k, v) in &expected {
                let got = reader.get(k)?;
                if got.as_ref() != Some(v) {
                    missing_keys.push(String::from_utf8_lossy(k).to_string());
                }
            }
            if !missing_keys.is_empty() {
                drop(store);
                run_full_consistency_checks(ctx, &[StressStorageMode::Disk])?;
                let report = CrashLoopReport {
                    cycles: 4,
                    verified_keys: expected.len(),
                    recoveries: recoveries.clone(),
                    missing_keys,
                };
                write_report(ctx, "durability_crash_loop.json", &report);
                return Err(alopex_core::Error::InvalidFormat(
                    "durability crash loop missing keys".into(),
                ));
            }
        }

        run_full_consistency_checks(ctx, &[StressStorageMode::Disk])?;
        let report = CrashLoopReport {
            cycles: 4,
            verified_keys: expected.len(),
            recoveries,
            missing_keys: Vec::new(),
        };
        write_report(ctx, "durability_crash_loop.json", &report);
        Ok(())
    });
    assert!(
        result.is_success(),
        "durability_crash_loop: {:?}",
        result.failure_summary()
    );
}

#[cfg_attr(not(feature = "lane_nightly"), ignore)]
#[test]
fn test_durability_power_loss_wal_integrity() {
    if std::env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .eq_ignore_ascii_case("memory")
    {
        return;
    }
    let harness =
        StressTestHarness::new(durability_config("durability_power_loss_wal_integrity")).unwrap();
    let result = harness.run(|ctx| {
        let root = storage_root_for_mode(&ctx.db_path, StressStorageMode::Disk);
        let sim = CrashSimulator::new(&root);
        let expected = sim.crash_after_writes(5)?;
        let (power_loss_bytes, _corruption_offset) = inject_power_loss(&root)?;
        let mut missing_keys: Vec<String> = Vec::new();
        let recovery = sim.recover_and_verify(|store, _recovery| {
            let mut txn = store.begin(TxnMode::ReadOnly)?;
            for (k, v) in &expected {
                let got = txn.get(k)?;
                if got.as_ref() != Some(v) {
                    missing_keys.push(String::from_utf8_lossy(k).to_string());
                }
            }
            Ok(())
        })?;

        run_full_consistency_checks(ctx, &[StressStorageMode::Disk])?;
        let warnings_empty = recovery.warnings.is_empty();
        let stop_reason = recovery.stop_reason.clone();
        let report = WalIntegrityReport {
            power_loss_bytes,
            recovered_keys: recovery.entries_recovered,
            warnings_empty,
            stop_reason: stop_reason.clone(),
            missing_keys: missing_keys.clone(),
        };
        write_report(ctx, "durability_wal_integrity.json", &report);
        if !missing_keys.is_empty() || !warnings_empty || stop_reason.is_some() {
            return Err(alopex_core::Error::InvalidFormat(
                "durability wal integrity mismatch".into(),
            ));
        }
        Ok(())
    });
    assert!(
        result.is_success(),
        "durability_power_loss_wal_integrity: {:?}",
        result.failure_summary()
    );
}

#[cfg(feature = "test-hooks")]
mod fsync_checks {
    use super::*;
    use alopex_core::Error as CoreError;
    use common::{open_store_with_fault_injector, IoErrorInjector};
    use std::io::ErrorKind;

    #[derive(Serialize)]
    struct FsyncCoverageReport {
        fsync_error_injected: bool,
        flush_called: bool,
    }

    #[cfg_attr(not(feature = "lane_nightly"), ignore)]
    #[test]
    fn test_fsync_flush_coverage() {
        if std::env::var("STRESS_STORAGE_MODE")
            .unwrap_or_else(|_| "both".to_string())
            .eq_ignore_ascii_case("memory")
        {
            return;
        }
        let harness =
            StressTestHarness::new(super::durability_config("durability_fsync_flush")).unwrap();
        let result = harness.run(|ctx| {
            let injector = std::sync::Arc::new(
                IoErrorInjector::new()
                    .with_fsync_error_rate(1.0)
                    .with_error_kind(ErrorKind::Other),
            );
            let store = open_store_with_fault_injector(&ctx.db_path, injector)?;
            let mut txn = store.begin(TxnMode::ReadWrite)?;
            txn.put(b"fsync".to_vec(), b"coverage".to_vec())?;
            let res = txn.commit_self();
            assert!(matches!(res, Err(CoreError::Io(_))));

            let flush_called = store.flush().is_ok();
            let report = FsyncCoverageReport {
                fsync_error_injected: true,
                flush_called,
            };
            super::write_report(ctx, "durability_fsync_flush.json", &report);
            Ok(())
        });
        assert!(
            result.is_success(),
            "durability_fsync_flush: {:?}",
            result.failure_summary()
        );
    }
}
