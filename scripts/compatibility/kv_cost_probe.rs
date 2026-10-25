//! Manual Linux micro-case probe. Build this same source against each pinned core.
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::{CheckpointConfig, ConvergePolicy, LsmKV, LsmKVConfig};
use alopex_core::{KVStore, KVTransaction, TxnMode};
use std::{fs, io::Write, os::unix::fs::MetadataExt, path::Path, time::Instant};

fn open(path: &Path) -> LsmKV {
    let config = LsmKVConfig {
        wal: WalConfig {
            segment_size: 28 + 4096,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: false,
            ..Default::default()
        },
        converge: ConvergePolicy::Never,
        prune_sidecar_on_drop: false,
        ..Default::default()
    };
    #[cfg(feature = "overflow")]
    {
        use alopex_core::lsm::{
            overflow::LsmOverflowConfig,
            wal::{operations::OperationLimits, spool::SpoolLimits},
        };
        let limits = LsmOverflowConfig::new(
            SpoolLimits::new(16384, 32768, 256).unwrap(),
            OperationLimits::new(16384, 16, 4096).unwrap(),
            131072,
            256,
            32768,
        )
        .with_inline_bytes(1024)
        .with_activation_bytes(8192)
        .with_container_fence_bytes(4096);
        LsmKV::open_with_overflow(path, config, limits).unwrap().0
    }
    #[cfg(not(feature = "overflow"))]
    LsmKV::open_with_config(path, config).unwrap().0
}

fn bytes(path: &Path) -> (u64, u64) {
    if !path.exists() {
        return (0, 0);
    }
    let info = fs::symlink_metadata(path).unwrap();
    assert!(!info.file_type().is_symlink());
    if info.is_file() {
        return (info.len(), info.blocks() * 512);
    }
    fs::read_dir(path)
        .unwrap()
        .fold((0, info.blocks() * 512), |sum, entry| {
            let child = bytes(&entry.unwrap().path());
            (sum.0 + child.0, sum.1 + child.1)
        })
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 4);
    let path = Path::new(&args[2]);
    let size: usize = args[3].parse().unwrap();
    assert!([16, 2048].contains(&size));
    let overflow = cfg!(feature = "overflow");
    let expected_commit = overflow || size == 16;
    let started = Instant::now();
    let store = open(path);
    let open_ns = started.elapsed().as_nanos();
    if args[1] == "verify" {
        let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(tx.get(&b"old".to_vec()).unwrap(), Some(b"ok".to_vec()));
        for key in *b"ab" {
            assert_eq!(
                tx.get(&vec![key]).unwrap(),
                expected_commit.then(|| vec![key; size])
            );
        }
        tx.rollback_self().unwrap();
        println!("{{\"phase\":\"verify\",\"open_ns\":{open_ns},\"verified\":true}}");
    } else {
        assert_eq!(args[1], "write");
        let mut old = store.begin(TxnMode::ReadWrite).unwrap();
        old.put(b"old".to_vec(), b"ok".to_vec()).unwrap();
        old.commit_self().unwrap();
        let preparation = Instant::now();
        let input = [
            (vec![b'a'], vec![b'a'; size]),
            (vec![b'b'], vec![b'b'; size]),
        ];
        let prepare_ns = preparation.elapsed().as_nanos();
        let staging = Instant::now();
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        for (key, value) in input {
            tx.put(key, value).unwrap();
        }
        let stage_ns = staging.elapsed().as_nanos();
        let commit = Instant::now();
        let result = tx.commit_self();
        let commit_ns = commit.elapsed().as_nanos();
        if expected_commit {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("WAL entry exceeds ring capacity"),
                "{error}"
            );
        }
        let memory = fs::read_to_string("/proc/self/status").unwrap();
        let peak_rss_kib: u64 = memory
            .lines()
            .find(|l| l.starts_with("VmHWM:"))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let (fixture_bytes, allocated_bytes) = bytes(path);
        assert!(fixture_bytes <= 1024 * 1024);
        let wal_bytes = fs::metadata(path.join("lsm.wal")).unwrap().len();
        let spool_bytes = bytes(&path.join("overflow")).0;
        let status = if expected_commit {
            "committed"
        } else {
            "capacity_refused"
        };
        println!("{{\"phase\":\"write\",\"source_sha\":\"{}\",\"overflow\":{overflow},\"value_bytes\":{size},\"status\":\"{status}\",\"open_ns\":{open_ns},\"prepare_ns\":{prepare_ns},\"stage_ns\":{stage_ns},\"commit_ns\":{commit_ns},\"peak_rss_kib\":{peak_rss_kib},\"wal_bytes\":{wal_bytes},\"spool_bytes\":{spool_bytes},\"fixture_bytes\":{fixture_bytes},\"allocated_bytes\":{allocated_bytes}}}", env!("ALOPEX_SOURCE_SHA"));
    }
    std::io::stdout().flush().unwrap();
    // Preserve WAL bytes for the next fresh process; exclude destructor checkpoint cost.
    std::process::exit(0);
}
