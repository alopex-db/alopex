use alopex_embedded::{Database, TxnMode};
use alopex_sql::ExecutionResult;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new();

    // Database::execute_sql の例
    let result = db.execute_sql(
        r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
        "#,
    )?;
    println!("execute_sql result: {result:?}");

    let result = db.execute_sql("SELECT id, name FROM users ORDER BY id;")?;
    if let ExecutionResult::Query(query) = result {
        println!("rows = {}", query.rows.len());
    }

    // Transaction::execute_sql の例（KV と SQL の混在）
    let mut txn = db.begin(TxnMode::ReadWrite)?;
    txn.put(b"custom", b"value")?;
    txn.execute_sql("INSERT INTO users (id, name) VALUES (3, 'carol');")?;
    let result = txn.execute_sql("SELECT name FROM users WHERE id = 3;")?;
    println!("txn query result: {result:?}");
    txn.commit()?;

    Ok(())
}
