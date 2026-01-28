#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::error::Result;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use common::CrashSimulator;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn committed_data_survives_crash() -> Result<()> {
    let dir = tempdir()?;
    let sim = CrashSimulator::new(dir.path());

    let expected = sim.crash_after_writes(2)?;

    sim.recover_and_verify(|store, _recovery| {
        let mut tx = store.begin(TxnMode::ReadOnly)?;
        for (key, value) in &expected {
            assert_eq!(tx.get(key)?, Some(value.clone()));
        }
        Ok(())
    })?;

    Ok(())
}

#[test]
fn crash_during_checkpoint_recovers_data() -> Result<()> {
    let dir = tempdir()?;
    let sim = CrashSimulator::new(dir.path());

    {
        let (store, _) = sim.open_store()?;
        let mut tx = store.begin(TxnMode::ReadWrite)?;
        tx.put(b"k1".to_vec(), b"v1".to_vec())?;
        tx.commit_self()?;
    }

    let checkpoint_path = dir.path().join("checkpoint.meta");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&checkpoint_path)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&[0xAA])?;
    file.sync_data()?;

    sim.recover_and_verify(|store, recovery| {
        assert!(recovery.checkpoint_lsn.is_none());
        let mut tx = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(tx.get(&b"k1".to_vec())?, Some(b"v1".to_vec()));
        Ok(())
    })?;

    Ok(())
}

#[test]
fn truncated_wal_recovers_valid_prefix() -> Result<()> {
    let dir = tempdir()?;
    let mut sim = CrashSimulator::new(dir.path());
    sim.config_mut().wal.segment_size = 4096;
    sim.config_mut().wal.max_segments = 1;

    let expected = sim.crash_after_writes(3)?;
    sim.truncate_wal(48)?;

    sim.recover_and_verify(|store, recovery| {
        assert!(recovery.stop_reason.is_some());
        let mut tx = store.begin(TxnMode::ReadOnly)?;
        assert_eq!(tx.get(&expected[0].0)?, Some(expected[0].1.clone()));
        assert_eq!(tx.get(&expected[2].0)?, None);
        Ok(())
    })?;

    Ok(())
}
