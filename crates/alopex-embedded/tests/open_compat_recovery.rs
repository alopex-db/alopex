use alopex_core::storage::format::{AlopexFileWriter, FileFlags, FileVersion};
use alopex_core::TxnMode;
use alopex_embedded::Database;
use tempfile::tempdir;

#[test]
fn open_accepts_alopex_file_path_and_preserves_data() {
    let dir = tempdir().expect("tempdir");
    let file_path = dir.path().join("compat-v0_1.alopex");

    let writer = AlopexFileWriter::new(file_path.clone(), FileVersion::new(0, 0, 1), FileFlags(0))
        .expect("create header");
    writer.finalize().expect("finalize header");

    {
        let db = Database::open(&file_path).expect("open via .alopex path");
        let mut txn = db.begin(TxnMode::ReadWrite).expect("begin rw");
        txn.put(b"compat-key", b"compat-value").expect("put");
        txn.commit().expect("commit");
    }

    let db = Database::open(&file_path).expect("reopen via .alopex path");
    let mut txn = db.begin(TxnMode::ReadOnly).expect("begin ro");
    let got = txn.get(b"compat-key").expect("get");
    assert_eq!(got, Some(b"compat-value".to_vec()));
}

#[test]
fn crash_recovery_keeps_committed_range_and_txn_boundary() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("recovery.db");

    {
        let db = Database::open(&path).expect("open db");
        let mut committed = db.begin(TxnMode::ReadWrite).expect("begin committed txn");
        for i in 0..100u32 {
            committed
                .put(
                    format!("k-{i:04}").as_bytes(),
                    format!("v-{i:04}").as_bytes(),
                )
                .expect("put committed");
        }
        committed.commit().expect("commit committed txn");

        let mut uncommitted = db.begin(TxnMode::ReadWrite).expect("begin uncommitted txn");
        for i in 100..150u32 {
            uncommitted
                .put(
                    format!("k-{i:04}").as_bytes(),
                    format!("v-{i:04}").as_bytes(),
                )
                .expect("put uncommitted");
        }
        // Drop without commit to mimic abrupt termination.
    }

    let db = Database::open(&path).expect("reopen after crash-like drop");
    let mut read = db.begin(TxnMode::ReadOnly).expect("begin ro");

    let mut recovered = 0usize;
    for i in 0..100u32 {
        let key = format!("k-{i:04}");
        let value = format!("v-{i:04}");
        let got = read.get(key.as_bytes()).expect("read committed key");
        assert_eq!(got, Some(value.into_bytes()));
        recovered += 1;
    }

    for i in 100..150u32 {
        let key = format!("k-{i:04}");
        let got = read.get(key.as_bytes()).expect("read uncommitted key");
        assert_eq!(got, None, "uncommitted key must not survive recovery");
    }

    assert_eq!(recovered, 100, "committed key count mismatch");
}
