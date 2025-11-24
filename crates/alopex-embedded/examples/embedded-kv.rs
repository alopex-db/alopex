//! An example of using the embedded AlopexDB API.

use alopex_embedded::{Database, TxnMode};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the database files.
    let dir = tempdir()?;
    let path = dir.path().join("my_database.db");

    println!("Database will be stored at: {}", path.display());

    // Open the database.
    let db = Database::open(&path)?;

    // Begin a read-write transaction.
    let mut txn = db.begin(TxnMode::ReadWrite)?;
    println!("Putting 'key' -> 'value'");
    txn.put(b"key", b"value")?;
    txn.put(b"another_key", b"another_value")?;

    // Commit the transaction.
    txn.commit()?;
    println!("Transaction committed.");

    // Begin a new read-only transaction.
    let mut txn = db.begin(TxnMode::ReadOnly)?;
    println!("Reading 'key'...");
    let value = txn.get(b"key")?;

    if let Some(val) = value {
        println!("Got value: {}", String::from_utf8_lossy(&val));
        assert_eq!(val, b"value");
    } else {
        println!("Key not found.");
    }

    // Transactions are automatically rolled back on drop if not committed.
    drop(txn); // Ensure borrow ends before dropping db.

    println!("\nDropping DB and re-opening to test persistence...");
    drop(db);

    let db2 = Database::open(&path)?;
    let mut txn2 = db2.begin(TxnMode::ReadOnly)?;
    let val2 = txn2.get(b"key")?;
    println!(
        "Read 'key' after re-opening: {}",
        String::from_utf8_lossy(&val2.unwrap())
    );

    println!("\nExample finished successfully.");
    Ok(())
}
