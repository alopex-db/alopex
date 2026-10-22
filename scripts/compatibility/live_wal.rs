//! Manual, bounded old-writer/live-WAL compatibility client. No timing or load loop.
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::lsm::wal::{SyncMode, WalConfig, WAL_SEGMENT_HEADER_SIZE};
use alopex_core::lsm::{CheckpointConfig, LsmKV, LsmKVConfig};
use alopex_core::types::TxnMode;
use serde_json::json;
use std::io::Write;
use std::path::Path;

fn config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: false,
            ..Default::default()
        },
        prune_sidecar_on_drop: false,
        ..Default::default()
    }
}

fn verify(store: &LsmKV, upgraded: bool) {
    let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        KVTransaction::get(&mut tx, &b"old".to_vec()).unwrap(),
        Some(b"ok".to_vec())
    );
    assert_eq!(
        KVTransaction::get(&mut tx, &b"new".to_vec()).unwrap(),
        upgraded.then(|| vec![b'x'; 65])
    );
}

// Deliberately bypass all Rust destructors after the synchronous commit/read.
// The parent waits for this process before opening its exact files. This checks
// process-exit recovery, not a kill during publication or power-loss durability.
fn finish(receipt: serde_json::Value) -> ! {
    println!("{receipt}");
    std::io::stdout().flush().unwrap();
    std::process::exit(0)
}

#[cfg(not(feature = "candidate"))]
fn run(phase: &str, path: &Path) {
    match phase {
        "legacy-write" => {
            assert!(!path.exists(), "fixture must be new");
            let (store, recovery) = LsmKV::open_with_config(path, config()).unwrap();
            assert_eq!(recovery.entries_recovered, 0);
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            tx.put(b"old".to_vec(), b"ok".to_vec()).unwrap();
            tx.commit_self().unwrap();
            verify(&store, false);
            let replay =
                alopex_core::lsm::wal::WalReader::open(&path.join("lsm.wal"), config().wal)
                    .unwrap()
                    .replay()
                    .unwrap();
            assert_eq!(replay.entries.len(), 1);
            assert!(replay.warnings.is_empty() && replay.stop_reason.is_none());
            finish(
                json!({"phase": phase, "result": "pass", "committed_keys": 1,
                "writer_version": "0.8.16", "last_lsn": replay.entries[0].lsn,
                "sync": "EveryWrite", "destructors": false}),
            );
        }
        "legacy-reject" => {
            let error = match LsmKV::open_with_config(path, config()) {
                Ok(_) => panic!("old reader accepted WAL v3"),
                Err(error) => error,
            };
            assert!(
                matches!(&error, alopex_core::Error::CorruptedSegment { segment_id: 0, reason }
                if reason == "WAL segment header invalid: invalid format: unsupported WAL version: 3"),
                "{error:?}"
            );
            finish(json!({"phase": phase, "result": "pass", "error": error.to_string()}));
        }
        _ => panic!("unsupported old-reader phase: {phase}"),
    }
}

#[cfg(feature = "candidate")]
fn run(phase: &str, path: &Path) {
    use alopex_core::lsm::overflow::LsmOverflowConfig;
    use alopex_core::lsm::wal::{operations::OperationLimits, spool::SpoolLimits};
    let limits = LsmOverflowConfig::new(
        SpoolLimits::new(256, 512, 32).unwrap(),
        OperationLimits::new(256, 32, 65).unwrap(),
        4096,
        32,
        4096,
    )
    .with_inline_bytes(128)
    .with_activation_bytes(4096)
    .with_container_fence_bytes(4096);
    let (store, recovery) = match phase {
        "current-read" => LsmKV::open_with_config(path, config()).unwrap(),
        "upgrade" | "reopen" => LsmKV::open_with_overflow(path, config(), limits).unwrap(),
        _ => panic!("unsupported candidate phase: {phase}"),
    };
    assert_eq!(recovery.entries_recovered, 1);
    assert!(recovery.warnings.is_empty(), "{:?}", recovery.warnings);
    assert!(recovery.stop_reason.is_none(), "{:?}", recovery.stop_reason);
    if phase != "reopen" {
        assert_eq!(recovery.checkpoint_lsn, None);
    }
    verify(&store, phase == "reopen");
    if phase == "upgrade" {
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"new".to_vec(), vec![b'x'; 65]).unwrap();
        tx.commit_self().unwrap();
        verify(&store, true);
    }
    if phase != "current-read" {
        assert!(store.overflow_stats().unwrap().unwrap().directory_bytes > 0);
    }
    finish(json!({"phase": phase, "result": "pass",
        "entries_recovered": recovery.entries_recovered, "last_lsn": recovery.last_lsn,
        "checkpoint_lsn": recovery.checkpoint_lsn, "warnings": recovery.warnings,
        "stop_reason": recovery.stop_reason, "destructors": false}));
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    assert_eq!(args.len(), 3, "usage: <binary> <phase> <fresh-directory>");
    run(args[1].to_str().unwrap(), Path::new(&args[2]));
}
