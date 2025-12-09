use std::sync::Arc;
use std::time::Duration;

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::{ColumnMetadata, TableMetadata};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{SqlValue, TxnBridge};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

fn table_meta() -> TableMetadata {
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

fn user_row(id: i32, name: &str, age: i32) -> Vec<SqlValue> {
    vec![
        SqlValue::Integer(id),
        SqlValue::Text(name.to_string()),
        SqlValue::Integer(age),
    ]
}

fn macro_flow(c: &mut Criterion) {
    c.bench_function("storage_macro_insert_lookup_update_delete", |b| {
        b.iter_batched(
            || {
                let store = Arc::new(MemoryKV::new());
                let bridge = TxnBridge::new(store.clone());
                (bridge, table_meta())
            },
            |(bridge, meta)| {
                // Insert + index
                bridge
                    .with_write_txn(|ctx| {
                        ctx.with_table(&meta, |table| {
                            for i in 0..200 {
                                let row = user_row(i, &format!("user{i}"), 20 + (i % 5));
                                table.insert(i as u64, &row)?;
                            }
                            Ok(())
                        })?;
                        ctx.with_index(1, true, vec![1], |index| {
                            for i in 0..200 {
                                let row = user_row(i, &format!("user{i}"), 20 + (i % 5));
                                index.insert(&row, i as u64)?;
                            }
                            Ok(())
                        })?;
                        Ok(())
                    })
                    .unwrap();

                // Lookup and scan
                bridge
                    .with_read_txn(|ctx| {
                        ctx.with_index(1, true, vec![1], |index| {
                            let ids = index.lookup(&SqlValue::Text("user50".into()))?;
                            assert_eq!(ids, vec![50]);
                            Ok(())
                        })?;
                        ctx.with_table(&meta, |table| {
                            let mut iter = table.scan()?;
                            while let Some(res) = iter.next() {
                                let _ = res.unwrap();
                            }
                            Ok(())
                        })
                    })
                    .unwrap();

                // Update + delete
                bridge
                    .with_write_txn(|ctx| {
                        ctx.with_table(&meta, |table| table.update(100, &user_row(100, "updated", 42)))?;
                        ctx.with_index(1, true, vec![1], |index| {
                            index.delete(&user_row(100, "user100", 20), 100)?;
                            index.insert(&user_row(100, "updated", 42), 100)
                        })?;
                        ctx.with_table(&meta, |table| table.delete(150))?;
                        ctx.with_index(1, true, vec![1], |index| index.delete(&user_row(150, "user150", 20), 150))?;
                        Ok(())
                    })
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = storage_macro;
    config = Criterion::default().measurement_time(Duration::from_millis(800));
    targets = macro_flow
}
criterion_main!(storage_macro);
