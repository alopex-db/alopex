//! Manual six-row WAL capacity comparison; never run by ordinary CI.
//! Run on each source revision with the same command and retain stdout.
#[cfg(not(target_arch = "wasm32"))]
use alopex_core::lsm::wal::{
    SyncMode, WalBatchOp, WalConfig, WalEntry, WalOpType, WAL_SEGMENT_HEADER_SIZE,
};
#[cfg(not(target_arch = "wasm32"))]
use alopex_core::lsm::{CheckpointConfig, LsmKV, LsmKVConfig};
#[cfg(not(target_arch = "wasm32"))]
use alopex_core::{KVStore, KVTransaction, TxnMode};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let batch = WalEntry::batch(
        1,
        (0..2)
            .map(|key| WalBatchOp {
                op_type: WalOpType::Put,
                key: vec![key],
                value: Some(vec![1]),
            })
            .collect(),
    );
    let config = LsmKVConfig {
        wal: WalConfig {
            segment_size: WAL_SEGMENT_HEADER_SIZE + 2 * batch.encoded_len() + 1,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: false,
            ..Default::default()
        },
        ..Default::default()
    };
    println!("phase,batch,elapsed_us,wal_used_bytes,result");
    for explicit_checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = LsmKV::open_with_config(dir.path(), config.clone()).unwrap();
        for batch in 0..3 {
            if explicit_checkpoint && batch == 2 {
                let start = Instant::now();
                let result = store.checkpoint();
                println!(
                    "explicit_checkpoint,{batch},{},{},{:?}",
                    start.elapsed().as_micros(),
                    store.wal.read().unwrap().used_bytes(),
                    result.map(|r| r.wal_bytes_reclaimed)
                );
            }
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            for key in (batch * 2)..(batch * 2 + 2) {
                tx.put(vec![key], vec![1]).unwrap();
            }
            let start = Instant::now();
            let result = tx.commit_self();
            println!(
                "{},{batch},{},{},{:?}",
                if explicit_checkpoint {
                    "manual"
                } else {
                    "automatic"
                },
                start.elapsed().as_micros(),
                store.wal.read().unwrap().used_bytes(),
                result
            );
        }
    }
}

// The file-backed diagnostic is native-only. Cargo also compiles examples when
// running the core WASM contract suite, where tempfile is deliberately absent.
#[cfg(target_arch = "wasm32")]
fn main() {}
