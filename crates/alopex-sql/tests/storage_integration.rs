use std::sync::Arc;

use alopex_core::kv::KVTransaction;
use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::{ColumnMetadata, TableMetadata};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{SqlValue, TableStorage, TxnBridge};

fn sample_table_meta() -> TableMetadata {
    TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ColumnMetadata::new("age", ResolvedType::Integer),
        ],
    )
    .with_table_id(1)
}

#[test]
fn end_to_end_storage_flow() {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let meta = sample_table_meta();

    // 1. Insert rows and index entries, then commit.
    bridge
        .with_write_txn(|ctx| {
            ctx.with_table(&meta, |table| {
                insert_user(table, 1, "alice", 20)?;
                insert_user(table, 2, "bob", 25)?;
                insert_user(table, 3, "carol", 30)?;
                Ok(())
            })?;
            ctx.with_index(1, true, vec![1], |index| {
                index.insert(&user_row(1, "alice", 20), 1)?;
                index.insert(&user_row(2, "bob", 25), 2)?;
                index.insert(&user_row(3, "carol", 30), 3)
            })?;
            Ok(())
        })
        .expect("write txn commit should succeed");

    // 2. Lookup via index and fetch row.
    let alice_row = bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("alice".into()))
            })?;
            assert_eq!(ids, vec![1]);
            ctx.with_table(&meta, |table| table.get(1))
        })
        .expect("read txn should succeed")
        .expect("alice should exist");
    assert_eq!(alice_row[1], SqlValue::Text("alice".into()));

    // 3. Update row and index: rename bob -> robert, ensure index updated.
    bridge
        .with_write_txn(|ctx| {
            ctx.with_table(&meta, |table| {
                table.update(2, &user_row(2, "robert", 25))
            })?;
            ctx.with_index(1, true, vec![1], |index| {
                index.delete(&user_row(2, "bob", 25), 2)?;
                index.insert(&user_row(2, "robert", 25), 2)
            })?;
            Ok(())
        })
        .expect("update txn should commit");

    bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("robert".into()))
            })?;
            assert_eq!(ids, vec![2]);
            let ids_old = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("bob".into()))
            })?;
            assert!(ids_old.is_empty());
            Ok(())
        })
        .unwrap();

    // 4. Delete row and index entry.
    bridge
        .with_write_txn(|ctx| {
            ctx.with_index(1, true, vec![1], |index| {
                index.delete(&user_row(3, "carol", 30), 3)
            })?;
            ctx.with_table(&meta, |table| table.delete(3))?;
            Ok(())
        })
        .unwrap();

    bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("carol".into()))
            })?;
            assert!(ids.is_empty());
            Ok(())
        })
        .unwrap();

    // 5. Rollback scenario: insert then rollback, verify absence.
    bridge
        .with_write_txn_explicit(|ctx| {
            ctx.with_table(&meta, |table| insert_user(table, 4, "dave", 40))?;
            ctx.with_index(1, true, vec![1], |index| {
                index.insert(&user_row(4, "dave", 40), 4)
            })?;
            Ok(((), false)) // rollback
        })
        .unwrap();

    bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("dave".into()))
            })?;
            assert!(ids.is_empty());
            Ok(())
        })
        .unwrap();
}

fn insert_user<'a>(
    table: &mut TableStorage<'_, 'a, impl KVTransaction<'a>>,
    id: u64,
    name: &str,
    age: i32,
) -> Result<(), alopex_sql::storage::StorageError> {
    table.insert(id, &user_row(id, name, age))
}

fn user_row(id: u64, name: &str, age: i32) -> Vec<SqlValue> {
    vec![
        SqlValue::Integer(id as i32),
        SqlValue::Text(name.to_string()),
        SqlValue::Integer(age),
    ]
}
