#![cfg(not(target_arch = "wasm32"))]

mod common;

use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use common::CrashSimulator;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::tempdir;

fn op_strategy() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    let key = prop::collection::vec(any::<u8>(), 1..8);
    let value = prop::collection::vec(any::<u8>(), 0..16);
    (key, value)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, .. ProptestConfig::default() })]

    #[test]
    fn proptest_recovery_preserves_committed_prefix(
        batches in prop::collection::vec(prop::collection::vec(op_strategy(), 1..4), 1..8),
        commit_flags in prop::collection::vec(any::<bool>(), 1..8),
        crash_at in 0usize..8
    ) {
        let dir = tempdir().expect("tempdir");
        let sim = CrashSimulator::new(dir.path());

        let crash_at = crash_at.min(batches.len());
        let commit_flags = &commit_flags[..commit_flags.len().min(batches.len())];
        let mut committed = BTreeMap::new();
        let mut seen = BTreeSet::new();
        {
            let (store, _) = sim.open_store().expect("open store");
            for (idx, batch) in batches.iter().cloned().enumerate() {
                if idx >= crash_at {
                    break;
                }
                let mut tx = store.begin(TxnMode::ReadWrite).expect("begin");
                for (key, value) in batch {
                    seen.insert(key.clone());
                    tx.put(key.clone(), value.clone()).expect("put");
                    if commit_flags.get(idx).copied().unwrap_or(false) {
                        committed.insert(key, Some(value));
                    }
                }
                if commit_flags.get(idx).copied().unwrap_or(false) {
                    tx.commit_self().expect("commit");
                }
            }
        }

        let recovery = sim
            .recover_and_verify(|store, _recovery| {
                let mut tx = store.begin(TxnMode::ReadOnly)?;
                for key in seen {
                    let expected = committed.get(&key).and_then(|v| v.clone());
                    assert_eq!(tx.get(&key)?, expected);
                }
                Ok(())
            })
            .expect("recover");

        prop_assert!(recovery.entries_recovered <= crash_at);
    }
}
