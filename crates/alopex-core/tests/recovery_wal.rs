#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::error::Result;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::lsm::wal::{WalSectionHeader, WAL_SECTION_HEADER_SIZE, WAL_SEGMENT_HEADER_SIZE};
use alopex_core::types::TxnMode;
use common::CrashSimulator;
use std::fs::OpenOptions;
use std::io::Read;
use tempfile::tempdir;

#[test]
fn recovery_recovers_clean_wal() -> Result<()> {
    let dir = tempdir()?;
    let mut sim = CrashSimulator::new(dir.path());
    sim.config_mut().wal.segment_size = 4096;
    sim.config_mut().wal.max_segments = 1;

    let expected = sim.crash_after_writes(3)?;

    sim.recover_and_verify(|store, recovery| {
        assert_eq!(recovery.entries_recovered, expected.len());
        let mut tx = store.begin(TxnMode::ReadOnly)?;
        for (key, value) in &expected {
            assert_eq!(tx.get(key)?, Some(value.clone()));
        }
        Ok(())
    })?;

    Ok(())
}

#[test]
fn recovery_ignores_uncommitted_writes_after_crash() -> Result<()> {
    let dir = tempdir()?;
    let sim = CrashSimulator::new(dir.path());

    let key_committed = b"committed".to_vec();
    let val_committed = b"value".to_vec();
    let key_uncommitted = b"uncommitted".to_vec();
    let val_uncommitted = b"ghost".to_vec();

    {
        let (store, _) = sim.open_store()?;
        let mut tx = store.begin(TxnMode::ReadWrite)?;
        tx.put(key_committed.clone(), val_committed.clone())?;
        tx.commit_self()?;

        let mut tx = store.begin(TxnMode::ReadWrite)?;
        tx.put(key_uncommitted.clone(), val_uncommitted)?;
    }

    sim.recover_and_verify(|store, recovery| {
        assert_eq!(recovery.entries_recovered, 1);
        let mut tx = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(tx.get(&key_committed)?, Some(val_committed.clone()));
        assert_eq!(tx.get(&key_uncommitted)?, None);
        Ok(())
    })?;

    Ok(())
}

#[test]
fn recovery_stops_at_truncated_entry() -> Result<()> {
    let dir = tempdir()?;
    let mut sim = CrashSimulator::new(dir.path());
    sim.config_mut().wal.segment_size = 4096;
    sim.config_mut().wal.max_segments = 1;

    let expected = sim.crash_after_writes(2)?;
    sim.truncate_wal(32)?;

    sim.recover_and_verify(|store, recovery| {
        assert!(recovery.stop_reason.is_some());
        assert_eq!(recovery.entries_recovered, 1);
        let mut tx = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(tx.get(&expected[0].0)?, Some(expected[0].1.clone()));
        assert_eq!(tx.get(&expected[1].0)?, None);
        Ok(())
    })?;

    Ok(())
}

#[test]
fn recovery_stops_on_corrupted_wal_byte() -> Result<()> {
    let dir = tempdir()?;
    let mut sim = CrashSimulator::new(dir.path());
    sim.config_mut().wal.segment_size = 4096;
    sim.config_mut().wal.max_segments = 1;

    let expected = sim.crash_after_writes(2)?;

    let wal_path = dir.path().join("lsm.wal");
    let mut file = OpenOptions::new().read(true).open(&wal_path)?;
    let mut header_bytes = [0u8; WAL_SECTION_HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let section = WalSectionHeader::from_bytes(&header_bytes);

    let segment_size = sim.config_mut().wal.segment_size as u64;
    let segment_data_len = segment_size - (WAL_SEGMENT_HEADER_SIZE as u64);
    let ring_len = segment_data_len * (sim.config_mut().wal.max_segments as u64);
    let logical = if section.end_offset == 0 {
        ring_len - 1
    } else {
        section.end_offset - 1
    };
    let segment_index = logical / segment_data_len;
    let offset_in_segment = logical % segment_data_len;
    let physical = (WAL_SECTION_HEADER_SIZE as u64)
        + (segment_index * segment_size)
        + (WAL_SEGMENT_HEADER_SIZE as u64)
        + offset_in_segment;

    sim.corrupt_wal_byte(physical)?;

    sim.recover_and_verify(|store, recovery| {
        assert!(recovery.stop_reason.is_some());
        assert_eq!(recovery.entries_recovered, 1);
        let mut tx = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(tx.get(&expected[0].0)?, Some(expected[0].1.clone()));
        assert_eq!(tx.get(&expected[1].0)?, None);
        Ok(())
    })?;

    Ok(())
}

#[test]
fn recovery_handles_empty_wal() -> Result<()> {
    let dir = tempdir()?;
    let sim = CrashSimulator::new(dir.path());

    sim.recover_and_verify(|_store, recovery| {
        assert_eq!(recovery.entries_recovered, 0);
        assert!(recovery.stop_reason.is_none());
        Ok(())
    })?;

    Ok(())
}
