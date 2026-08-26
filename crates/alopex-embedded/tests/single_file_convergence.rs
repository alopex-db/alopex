//! Regression suite for issue #178 — "storage: 安定後に .alopex 単体へ収束しない".
//!
//! Contract under test (`docs-public/tech/file-format-comparison.md` L61/L67/L75):
//! 稼働中は WAL + 現行 `.alopex` の2本立て、**安定後は `.alopex` 単体で完全状態**。
//!
//! Every test that claims "single file" copies **only** `X.alopex` into a fresh
//! directory before reopening, so a sidecar left behind cannot mask a failure.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use alopex_core::storage::format::{
    AlopexFileReader, AlopexFileWriter, FileFlags, FileReader, FileSource, FileVersion, SectionType,
};
use alopex_embedded::{Database, TxnMode};
use tempfile::tempdir;

const CRASH_CHILD_PATH_ENV: &str = "ALOPEX_CONVERGENCE_CRASH_CHILD_PATH";
const CRASH_CHILD_ACTION_ENV: &str = "ALOPEX_CONVERGENCE_CRASH_CHILD_ACTION";
const CRASH_EXIT_CODE: i32 = 86;

fn stage_crash_in_child(container: &Path, action: &str) {
    let output = Command::new(std::env::current_exe().expect("test binary path"))
        .args([
            "child_stage_crash_state",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(CRASH_CHILD_PATH_ENV, container)
        .env(CRASH_CHILD_ACTION_ENV, action)
        .stdin(Stdio::null())
        .output()
        .expect("run crash-staging child");
    assert_eq!(
        output.status.code(),
        Some(CRASH_EXIT_CODE),
        "crash-staging child failed unexpectedly:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "spawned as a child process by crash-recovery scenarios"]
fn child_stage_crash_state() {
    let container =
        PathBuf::from(std::env::var_os(CRASH_CHILD_PATH_ENV).expect("crash child database path"));
    let action = std::env::var(CRASH_CHILD_ACTION_ENV).expect("crash child action");
    let (count, flush) = match action.as_str() {
        "wal-only" => (120, false),
        "flushed-sidecar" => (80, true),
        "legacy-sidecar" => (40, false),
        other => panic!("unknown crash child action: {other}"),
    };

    let db = Database::open(&container).expect("child opens database");
    let mut txn = db.begin(TxnMode::ReadWrite).expect("child begins rw");
    for index in 0..count {
        txn.put(&key(index), &value(index)).expect("child put");
    }
    txn.commit().expect("child commit");
    if flush {
        db.flush().expect("child flushes container");
    }

    // `process::exit` does not run Rust destructors. The OS still closes the
    // lock descriptor, exactly matching an abruptly terminated process without
    // exposing a production API that can unlock a live database.
    std::process::exit(CRASH_EXIT_CODE);
}

fn key(index: u32) -> Vec<u8> {
    format!("k-{index:06}").into_bytes()
}

fn value(index: u32) -> Vec<u8> {
    // ~220 bytes/entry so 1000 entries comfortably exceed a single data block.
    format!("v-{index:06}-{}", "p".repeat(200)).into_bytes()
}

/// Copy only the container file into a fresh directory and return its new path.
fn copy_container_only(container: &Path, destination: &Path) -> PathBuf {
    let file_name = container.file_name().expect("container file name");
    let copied = destination.join(file_name);
    fs::copy(container, &copied).expect("copy container");
    assert!(
        !destination
            .join(format!("{}.d", file_name.to_string_lossy()))
            .exists(),
        "sidecar must not be copied along with the container"
    );
    copied
}

fn sidecar_of(container: &Path) -> PathBuf {
    container.with_extension("alopex.d")
}

#[test]
fn close_converges_to_a_self_contained_alopex_file() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("mydb.alopex");

    {
        let db = Database::open(&container).expect("open db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..1000u32 {
            txn.put(&key(index), &value(index)).expect("put");
        }
        txn.commit().expect("commit");
        db.close().expect("close converges");
    }

    let size = fs::metadata(&container).expect("container metadata").len();
    assert!(size > 0, "container must not be a zero-byte marker");
    assert!(
        !sidecar_of(&container).exists(),
        "sidecar must be pruned once the handle is dropped"
    );

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());

    let db = Database::open(&copied).expect("open from copied container alone");
    let mut txn = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..1000u32 {
        assert_eq!(
            txn.get(&key(index)).expect("get"),
            Some(value(index)),
            "key {index} missing after single-file restore"
        );
    }
}

#[test]
fn drop_alone_converges_without_explicit_close() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("dropped.alopex");

    {
        let db = Database::open(&container).expect("open db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..64u32 {
            txn.put(&key(index), &value(index)).expect("put");
        }
        txn.commit().expect("commit");
        // No flush(), no close(): only the Drop path may converge here.
    }

    assert!(container.exists(), "drop must materialize the container");
    assert!(
        fs::metadata(&container).expect("metadata").len() > 0,
        "drop must write a real container, not a marker"
    );

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());
    let db = Database::open(&copied).expect("open copied container");
    let mut txn = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..64u32 {
        assert_eq!(txn.get(&key(index)).expect("get"), Some(value(index)));
    }
}

#[test]
fn explicit_flush_updates_container_but_keeps_sidecar_live() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("flushed.alopex");

    let db = Database::open(&container).expect("open db");
    let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
    for index in 0..32u32 {
        txn.put(&key(index), &value(index)).expect("put");
    }
    txn.commit().expect("commit");
    db.flush().expect("flush converges");

    assert!(
        fs::metadata(&container).expect("metadata").len() > 0,
        "flush must converge into the container"
    );
    assert!(
        sidecar_of(&container).exists(),
        "a live handle must keep its sidecar (contract L67: two files while running)"
    );

    // The container is already complete even though the handle is still open.
    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());
    let restored = Database::open(&copied).expect("open copied container");
    let mut read = restored.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..32u32 {
        assert_eq!(read.get(&key(index)).expect("get"), Some(value(index)));
    }
}

#[test]
fn empty_database_converges_to_a_valid_empty_container() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("empty.alopex");

    {
        let db = Database::open(&container).expect("open db");
        db.close().expect("close");
    }

    assert!(container.exists(), "empty db still produces a container");
    let reader =
        AlopexFileReader::open(FileSource::Path(container.clone())).expect("valid empty container");
    assert!(
        reader
            .section_index()
            .entries
            .iter()
            .all(|entry| entry.section_type == SectionType::Metadata),
        "empty db must not emit SSTable sections"
    );

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());
    {
        let db = Database::open(&copied).expect("open empty container");
        let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
        assert_eq!(read.get(&key(0)).expect("get"), None);
        drop(read);

        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        txn.put(&key(1), &value(1)).expect("put");
        txn.commit().expect("commit");
        db.close().expect("close");
    }

    let db = Database::open(&copied).expect("reopen after writing into an empty container");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    assert_eq!(read.get(&key(1)).expect("get"), Some(value(1)));
}

#[test]
fn restored_container_resumes_the_mvcc_clock() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("clock.alopex");

    {
        let db = Database::open(&container).expect("open db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..200u32 {
            txn.put(&key(index), &value(index)).expect("put");
        }
        txn.commit().expect("commit");
        db.close().expect("close");
    }

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());

    {
        let db = Database::open(&copied).expect("open copied container");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..200u32 {
            // A rewound timestamp oracle makes this either conflict or lose the write.
            txn.put(&key(index), b"overwritten").expect("put overwrite");
        }
        txn.commit().expect("overwrite must not conflict");

        let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
        for index in 0..200u32 {
            assert_eq!(
                read.get(&key(index)).expect("get"),
                Some(b"overwritten".to_vec()),
                "overwrite lost for key {index}"
            );
        }
        drop(read);
        db.close().expect("close");
    }

    let db = Database::open(&copied).expect("reopen after overwrite");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..200u32 {
        assert_eq!(
            read.get(&key(index)).expect("get"),
            Some(b"overwritten".to_vec())
        );
    }
}

#[test]
fn deletes_survive_single_file_restore() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("tombstone.alopex");

    {
        let db = Database::open(&container).expect("open db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        txn.put(&key(1), &value(1)).expect("put");
        txn.put(&key(2), &value(2)).expect("put");
        txn.commit().expect("commit");

        let mut remove = db.begin(TxnMode::ReadWrite).expect("begin rw");
        remove.delete(&key(1)).expect("delete");
        remove.commit().expect("commit delete");
        db.close().expect("close");
    }

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());
    let db = Database::open(&copied).expect("open copied container");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    assert_eq!(read.get(&key(1)).expect("get"), None, "tombstone lost");
    assert_eq!(read.get(&key(2)).expect("get"), Some(value(2)));
}

#[test]
fn crash_before_converge_recovers_from_the_sidecar() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("crash.alopex");

    stage_crash_in_child(&container, "wal-only");

    assert!(
        sidecar_of(&container).join("lsm.wal").exists(),
        "sidecar WAL must survive a crash"
    );

    let db = Database::open(&container).expect("reopen after crash");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..120u32 {
        assert_eq!(
            read.get(&key(index)).expect("get"),
            Some(value(index)),
            "key {index} lost after crash recovery"
        );
    }
}

#[test]
fn crash_during_sidecar_prune_falls_back_to_the_container() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("prune.alopex");

    stage_crash_in_child(&container, "flushed-sidecar");

    // Simulate a crash after the WAL was unlinked but before the sidecar dir went away.
    let sidecar = sidecar_of(&container);
    assert!(sidecar.exists(), "flush must keep the sidecar alive");
    fs::remove_file(sidecar.join("lsm.wal")).expect("remove wal");
    assert!(
        sidecar.join("sst").exists(),
        "the half-pruned sidecar must still hold its SSTables"
    );

    let db = Database::open(&container).expect("reopen with a half-pruned sidecar");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..80u32 {
        assert_eq!(read.get(&key(index)).expect("get"), Some(value(index)));
    }
}

#[test]
fn truncated_container_without_sidecar_is_an_error_not_an_empty_db() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("truncated.alopex");

    {
        let db = Database::open(&container).expect("open db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..50u32 {
            txn.put(&key(index), &value(index)).expect("put");
        }
        txn.commit().expect("commit");
        db.close().expect("close");
    }

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());
    let len = fs::metadata(&copied).expect("metadata").len();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&copied)
        .expect("open for truncate");
    file.set_len(len - 32).expect("truncate footer");
    drop(file);

    let result = Database::open(&copied);
    assert!(
        result.is_err(),
        "a truncated container without a sidecar must fail loudly, not open empty"
    );
}

#[test]
fn corrupted_sstable_section_is_rejected() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("corrupt.alopex");

    {
        let db = Database::open(&container).expect("open db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..50u32 {
            txn.put(&key(index), &value(index)).expect("put");
        }
        txn.commit().expect("commit");
        db.close().expect("close");
    }

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());

    let sstable_offset = {
        let reader = AlopexFileReader::open(FileSource::Path(copied.clone())).expect("open");
        reader
            .section_index()
            .entries
            .iter()
            .find(|entry| entry.section_type == SectionType::SSTable)
            .expect("an SSTable section must exist")
            .offset
    };

    let mut bytes = fs::read(&copied).expect("read container");
    bytes[sstable_offset as usize + 16] ^= 0xFF;
    fs::write(&copied, &bytes).expect("write corrupted container");

    let result = Database::open(&copied);
    assert!(
        result.is_err(),
        "a corrupted SSTable section must not be served silently"
    );
}

#[test]
fn container_with_sstables_but_no_manifest_is_rejected() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("manifestless.alopex");

    {
        let db = Database::open(&container).expect("open db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..20u32 {
            txn.put(&key(index), &value(index)).expect("put");
        }
        txn.commit().expect("commit");
        db.close().expect("close");
    }

    // Rebuild a container that keeps the SSTable payload but drops the manifest.
    let sstables: Vec<Vec<u8>> = {
        let reader = AlopexFileReader::open(FileSource::Path(container.clone())).expect("open");
        let ids: Vec<u32> = reader
            .section_index()
            .entries
            .iter()
            .filter(|entry| entry.section_type == SectionType::SSTable)
            .map(|entry| entry.section_id)
            .collect();
        assert!(
            !ids.is_empty(),
            "fixture needs at least one SSTable section"
        );
        ids.into_iter()
            .map(|id| reader.read_section(id).expect("read section"))
            .collect()
    };

    let target = tempdir().expect("target dir");
    let broken = target.path().join("manifestless.alopex");
    let mut writer = AlopexFileWriter::new(broken.clone(), FileVersion::CURRENT, FileFlags(0))
        .expect("create writer");
    for payload in &sstables {
        writer
            .add_section(SectionType::SSTable, payload, false)
            .expect("add sstable section");
    }
    writer.finalize().expect("finalize");

    let result = Database::open(&broken);
    assert!(
        result.is_err(),
        "SSTable sections without a manifest must be an error, never a silent empty DB"
    );
}

#[test]
fn newer_container_version_is_rejected() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("future.alopex");

    let writer = AlopexFileWriter::new(container.clone(), FileVersion::new(9, 0, 0), FileFlags(0))
        .expect("create writer");
    writer.finalize().expect("finalize");

    let result = Database::open(&container);
    assert!(
        result.is_err(),
        "a container newer than this build must fail with an explicit version error"
    );
}

#[test]
fn legacy_zero_byte_marker_is_promoted_on_close() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("legacy.alopex");
    let sidecar = sidecar_of(&container);

    // v0.8.6 layout: real data in the sidecar, a 0-byte marker next to it.
    stage_crash_in_child(&container, "legacy-sidecar");
    assert!(sidecar.join("lsm.wal").exists());
    fs::write(&container, b"").expect("write zero-byte marker");

    {
        let db = Database::open(&container).expect("legacy layout still opens");
        let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
        for index in 0..40u32 {
            assert_eq!(read.get(&key(index)).expect("get"), Some(value(index)));
        }
        drop(read);
        db.close().expect("close promotes the marker");
    }

    assert!(
        fs::metadata(&container).expect("metadata").len() > 0,
        "the 0-byte marker must be promoted to a real container"
    );

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());
    let db = Database::open(&copied).expect("open promoted container");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..40u32 {
        assert_eq!(read.get(&key(index)).expect("get"), Some(value(index)));
    }
}

#[test]
fn plain_directory_layout_is_unchanged() {
    let root = tempdir().expect("root");
    let plain = root.path().join("plaindir");

    {
        let db = Database::open(&plain).expect("open plain dir db");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        for index in 0..30u32 {
            txn.put(&key(index), &value(index)).expect("put");
        }
        txn.commit().expect("commit");
        db.close().expect("close");
    }

    assert!(plain.is_dir(), "plain directory must stay a directory");
    for entry in fs::read_dir(&plain).expect("read plain dir") {
        let path = entry.expect("dir entry").path();
        assert!(
            path.extension().and_then(|e| e.to_str()) != Some("alopex"),
            "a plain directory must never grow a container: {path:?}"
        );
    }
    assert!(!root.path().join("plaindir.alopex").exists());

    let db = Database::open(&plain).expect("reopen plain dir db");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in 0..30u32 {
        assert_eq!(read.get(&key(index)).expect("get"), Some(value(index)));
    }
}

#[test]
fn converge_is_idempotent_and_close_is_repeatable() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("idempotent.alopex");

    let db = Database::open(&container).expect("open db");
    let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
    txn.put(&key(1), &value(1)).expect("put");
    txn.commit().expect("commit");

    db.converge().expect("first converge");
    let first = fs::read(&container).expect("read container");
    db.converge().expect("second converge is a no-op");
    let second = fs::read(&container).expect("read container");
    assert_eq!(first, second, "a clean converge must not rewrite the file");

    db.close().expect("close");
    db.close().expect("repeated close is idempotent");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn large_dataset_round_trips_through_the_container() {
    let source = tempdir().expect("source dir");
    let container = source.path().join("large.alopex");
    let total = 50_000u32;

    {
        let db = Database::open(&container).expect("open db");
        for chunk in 0..50u32 {
            let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
            for offset in 0..1000u32 {
                let index = chunk * 1000 + offset;
                txn.put(&key(index), &value(index)).expect("put");
            }
            txn.commit().expect("commit");
        }
        db.close().expect("close");
    }

    let target = tempdir().expect("target dir");
    let copied = copy_container_only(&container, target.path());
    let db = Database::open(&copied).expect("open copied container");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");
    for index in (0..total).step_by(97) {
        assert_eq!(
            read.get(&key(index)).expect("get"),
            Some(value(index)),
            "key {index} missing in large round trip"
        );
    }
}
