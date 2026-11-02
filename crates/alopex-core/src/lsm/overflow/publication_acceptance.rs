//! Native LSM transactions across deterministic persistence boundaries.
//! Both native platforms enter through the public LSM overflow API.
//! Hooks are cfg(test) only. These are process-exit, not power-loss, tests.
use crate::kv::{KVStore, KVTransaction};
use crate::lsm::overflow::LsmOverflowConfig;
use crate::lsm::wal::operations::OperationLimits;
use crate::lsm::wal::overflow::{EXIT_STAGE, FAIL_STAGE};
use crate::lsm::wal::spool::SpoolLimits;
use crate::lsm::wal::{SyncMode, WalConfig, WAL_SEGMENT_HEADER_SIZE};
use crate::lsm::{CheckpointConfig, LsmKV, LsmKVConfig};
use crate::types::TxnMode;
use crate::Error;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CHILD: &str = "lsm::overflow::publication_acceptance::child";
const CASE: &str = "ALOPEX_PUBLIC_LSM_BOUNDARY";
const DIRECTORY: &str = "ALOPEX_PUBLIC_LSM_DIRECTORY";

fn open(path: &Path) -> (LsmKV, crate::lsm::RecoveryResult) {
    let config = LsmKVConfig {
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
    };
    let overflow = LsmOverflowConfig::new(
        SpoolLimits::new(256, 512, 32).unwrap(),
        OperationLimits::new(256, 16, 65).unwrap(),
        4096,
        32,
        4096,
    )
    .with_inline_bytes(128)
    .with_activation_bytes(4096);
    LsmKV::open_with_overflow(path, config, overflow).unwrap()
}

fn assert_values(store: &LsmKV, pending_committed: bool) {
    let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
    let actual: Vec<_> = tx.scan_prefix(b"").unwrap().collect();
    let mut expected = vec![(b"old".to_vec(), b"ok".to_vec())];
    if pending_committed {
        expected.push((b"pending-a".to_vec(), vec![7; 65]));
        expected.push((b"pending-b".to_vec(), vec![8]));
    }
    assert_eq!(
        actual, expected,
        "no partial transaction, duplicate or retry key"
    );
}

#[test]
#[ignore = "isolated subprocess entry; invoked by the twelve boundary cases"]
fn child() {
    let case = std::env::var(CASE).expect("only invoked by a parent case");
    let (mode, stage) = case.split_once(':').unwrap();
    let stage: u8 = stage.parse().unwrap();
    assert!(matches!(stage, 1..=5 | 8));
    let directory = std::env::var_os(DIRECTORY).unwrap();
    let (store, _) = open(Path::new(&directory));
    assert_values(&store, false);
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.put(b"pending-a".to_vec(), vec![7; 65]).unwrap();
    tx.put(b"pending-b".to_vec(), vec![8]).unwrap();
    match mode {
        "exit" => EXIT_STAGE.set(stage),
        "error" => FAIL_STAGE.set(stage),
        _ => panic!("unknown mode"),
    }
    println!("ARMED {case}");
    std::io::stdout().flush().unwrap();
    let error = tx.commit_self().unwrap_err();
    assert_eq!(mode, "error", "exit hook must terminate inside commit");
    FAIL_STAGE.set(0);
    assert!(matches!(error, Error::Io(ref e)
        if e.to_string() == "injected overflow persistence failure"));
    assert_values(&store, false); // A failed commit never publishes the memtable.
    if stage >= 4 {
        let mut retry = store.begin(TxnMode::ReadWrite).unwrap();
        retry.put(b"retry".to_vec(), b"no".to_vec()).unwrap();
        assert!(
            matches!(retry.commit_self(), Err(Error::InvalidFormat(reason))
            if reason == "WAL outcome uncertain; reopen before further writes")
        );
    }
    println!("ERROR_RETURNED {case}");
    std::io::stdout().flush().unwrap();
    std::process::exit(0); // Inspect recovery without Drop/checkpoint masking it.
}

fn boundary(mode: &str, stage: u8) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store"); // Plain directory: no container Drop.
    let (store, _) = open(&path);
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.put(b"old".to_vec(), b"ok".to_vec()).unwrap();
    tx.commit_self().unwrap();
    drop(store);
    let case = format!("{mode}:{stage}");
    let log_path = root.path().join("child.log");
    let log = fs::File::create(&log_path).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .env(CASE, &case)
        .env(DIRECTORY, &path)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!(
                "{case} timed out: {}",
                fs::read_to_string(&log_path).unwrap()
            );
        }
        // Poll only process completion; fault timing is deterministic in the child.
        std::thread::sleep(Duration::from_millis(10));
    };
    let output = fs::read_to_string(log_path).unwrap();
    assert_eq!(
        status.code(),
        Some(if mode == "exit" {
            80 + i32::from(stage)
        } else {
            0
        }),
        "{case}: {output}"
    );
    assert!(output.contains(&format!("ARMED {case}")), "{output}");
    if mode == "error" {
        assert!(
            output.contains(&format!("ERROR_RETURNED {case}")),
            "{output}"
        );
    }
    let spools: Vec<_> = fs::read_dir(path.join("overflow"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    let expected_files = usize::from(mode == "exit" || stage >= 4);
    assert_eq!(spools.len(), expected_files, "{case}: {spools:?}");
    for spool in spools {
        assert_eq!(
            spool.extension().unwrap(),
            if stage <= 2 { "tmp" } else { "spool" }
        );
        assert!(fs::metadata(spool).unwrap().len() <= 512);
    }
    // EveryWrite already synchronizes inside append_encoded (stage 5). Stage 8
    // is after the additional force_sync. An error/exit there can commit on disk
    // even though the caller has received no success; recovery must keep it whole.
    let committed = stage >= 5;
    let mut previous_stats = None;
    for _ in 0..2 {
        let (store, recovery) = open(&path);
        assert_values(&store, committed);
        assert_eq!(recovery.entries_recovered, usize::from(committed));
        assert!(recovery.warnings.is_empty() && recovery.stop_reason.is_none());
        assert!(recovery.checkpoint_lsn.is_some());
        let stats = store.overflow_stats().unwrap().unwrap();
        assert_eq!(stats.directory_bytes > 0, committed);
        assert_eq!(stats.gc_failures, 0);
        if let Some(previous) = previous_stats {
            assert_eq!(
                stats, previous,
                "reopen must not duplicate replay accounting"
            );
        }
        previous_stats = Some(stats);
        assert!(fs::read_dir(path.join("overflow"))
            .unwrap()
            .all(|entry| entry.unwrap().path().extension().unwrap() == "spool"));
        drop(store);
    }
}

macro_rules! case {
    ($name:ident, $mode:literal, $stage:literal) => {
        #[test]
        fn $name() {
            boundary($mode, $stage);
        }
    };
}
case!(error_before_spool_sync, "error", 1);
case!(exit_before_spool_sync, "exit", 1);
case!(error_after_spool_sync, "error", 2);
case!(exit_after_spool_sync, "exit", 2);
case!(error_after_spool_publication, "error", 3);
case!(exit_after_spool_publication, "exit", 3);
case!(error_before_reference, "error", 4);
case!(exit_before_reference, "exit", 4);
case!(error_after_reference_append_sync, "error", 5);
case!(exit_after_reference_append_sync, "exit", 5);
case!(error_after_reference_force_sync, "error", 8);
case!(exit_after_reference_force_sync, "exit", 8);
