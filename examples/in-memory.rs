use alopex_embedded::{Database, TxnMode};
use std::path::PathBuf;

fn print_mem_usage(db: &Database, label: &str) {
    let usage = db.memory_usage();
    println!(
        "[{}] memory_usage -> total={}B kv={}B index={}B",
        label, usage.total_bytes, usage.kv_bytes, usage.index_bytes
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("== In-memory AlopexDB demo (workspace root) ==");

    // Open with default in-memory options.
    let db = Database::open_in_memory()?;
    print_mem_usage(&db, "start");

    // Simple KV puts/gets.
    let mut txn = db.begin(TxnMode::ReadWrite)?;
    txn.put(b"hello", b"world")?;
    txn.put(b"answer", b"42")?;
    txn.commit()?;

    let mut rtxn = db.begin(TxnMode::ReadOnly)?;
    println!(
        "hello -> {:?}",
        rtxn.get(b"hello")?.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    println!(
        "answer -> {:?}",
        rtxn.get(b"answer")?.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    drop(rtxn);
    print_mem_usage(&db, "after put");

    // Snapshot view.
    let snapshot = db.snapshot();
    println!("snapshot entries ({}):", snapshot.len());
    for (k, v) in snapshot {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }

    // Persist to disk atomically.
    let wal_path = PathBuf::from("in-memory-demo.wal");
    db.persist_to_disk(&wal_path)?;
    println!("Persisted to disk at {:?}", wal_path);

    // Open from disk to verify content.
    let db_disk = Database::open(&wal_path)?;
    let mut rtxn2 = db_disk.begin(TxnMode::ReadOnly)?;
    println!(
        "disk reload hello -> {:?}",
        rtxn2
            .get(b"hello")?
            .map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // Cleanup demo artifacts.
    let _ = std::fs::remove_file(&wal_path);
    let _ = std::fs::remove_file(wal_path.with_extension("sst"));
    let _ = std::fs::remove_file(wal_path.with_extension("vec"));

    println!("Done.");
    Ok(())
}
