#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::error::Result;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::lsm::checkpoint::{save_checkpoint_meta, CheckpointMeta};
use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_core::types::TxnMode;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn recovery_skips_pre_checkpoint_entries() -> Result<()> {
    let dir = tempdir()?;
    let (store, _) = LsmKV::open_with_config(dir.path(), LsmKVConfig::default())?;

    let mut tx = store.begin(TxnMode::ReadWrite)?;
    tx.put(b"k1".to_vec(), b"v1".to_vec())?;
    tx.put(b"k2".to_vec(), b"v2".to_vec())?;
    tx.commit_self()?;

    store.checkpoint()?;

    let mut tx = store.begin(TxnMode::ReadWrite)?;
    tx.put(b"k3".to_vec(), b"v3".to_vec())?;
    tx.commit_self()?;

    drop(store);

    let (store, recovery) = LsmKV::open_with_config(dir.path(), LsmKVConfig::default())?;
    assert_eq!(recovery.entries_recovered, 1);

    let mut tx = store.begin(TxnMode::ReadOnly)?;
    assert_eq!(tx.get(&b"k1".to_vec())?, Some(b"v1".to_vec()));
    assert_eq!(tx.get(&b"k2".to_vec())?, Some(b"v2".to_vec()));
    assert_eq!(tx.get(&b"k3".to_vec())?, Some(b"v3".to_vec()));

    Ok(())
}

#[test]
fn recovery_includes_checkpoint_and_post_writes() -> Result<()> {
    let dir = tempdir()?;
    let (store, _) = LsmKV::open_with_config(dir.path(), LsmKVConfig::default())?;

    let mut tx = store.begin(TxnMode::ReadWrite)?;
    tx.put(b"k".to_vec(), b"before".to_vec())?;
    tx.commit_self()?;

    store.checkpoint()?;

    let mut tx = store.begin(TxnMode::ReadWrite)?;
    tx.put(b"k".to_vec(), b"after".to_vec())?;
    tx.commit_self()?;

    drop(store);

    let (store, recovery) = LsmKV::open_with_config(dir.path(), LsmKVConfig::default())?;
    assert_eq!(recovery.entries_recovered, 1);

    let mut tx = store.begin(TxnMode::ReadOnly)?;
    assert_eq!(tx.get(&b"k".to_vec())?, Some(b"after".to_vec()));

    Ok(())
}

#[test]
fn corrupted_checkpoint_meta_falls_back_to_full_wal() -> Result<()> {
    let dir = tempdir()?;
    let (store, _) = LsmKV::open_with_config(dir.path(), LsmKVConfig::default())?;

    let mut tx = store.begin(TxnMode::ReadWrite)?;
    tx.put(b"k1".to_vec(), b"v1".to_vec())?;
    tx.put(b"k2".to_vec(), b"v2".to_vec())?;
    tx.commit_self()?;

    let checkpoint_path = dir.path().join("checkpoint.meta");
    let meta = CheckpointMeta::new(1, 0);
    save_checkpoint_meta(&checkpoint_path, &meta)?;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&checkpoint_path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if let Some(byte) = bytes.get_mut(0) {
        *byte ^= 0xFF;
    }
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes)?;
    file.sync_data()?;

    drop(store);

    let (store, recovery) = LsmKV::open_with_config(dir.path(), LsmKVConfig::default())?;
    assert!(recovery.entries_recovered >= 1);
    assert!(recovery.checkpoint_lsn.is_none());

    let mut tx = store.begin(TxnMode::ReadOnly)?;
    assert_eq!(tx.get(&b"k1".to_vec())?, Some(b"v1".to_vec()));
    assert_eq!(tx.get(&b"k2".to_vec())?, Some(b"v2".to_vec()));

    Ok(())
}
