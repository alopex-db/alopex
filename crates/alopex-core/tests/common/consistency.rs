use super::{open_store_for_mode, storage_root_for_mode, StressStorageMode, TestContext};
use alopex_core::{Error as CoreError, KVStore, KVTransaction, Result as CoreResult, TxnMode};
use alopex_dataframe::ops::JoinType;
use alopex_dataframe::{DataFrame, Series};
use alopex_sql::catalog::{ColumnMetadata, TableMetadata};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{SqlValue, TableStorage, TxnBridge};
use arrow::array::{ArrayRef, Int64Array};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Serialize)]
struct KvConsistencyReport {
    mode: String,
    expected_count: usize,
    actual_count: usize,
    missing_keys: Vec<String>,
    extra_keys: Vec<String>,
    mismatched_keys: Vec<String>,
    wal_bytes: u64,
    sst_bytes: u64,
    total_bytes: u64,
}

#[derive(Debug, Serialize)]
struct SqlConsistencyReport {
    mode: String,
    rows_inserted: usize,
    rows_after_updates: usize,
    index_hits: BTreeMap<String, Vec<u64>>,
    missing_keys: Vec<String>,
    extra_keys: Vec<String>,
    mismatched_keys: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DataFrameConsistencyReport {
    row_count: usize,
    rows: Vec<(i64, i64, i64)>,
    sums: BTreeMap<String, i64>,
    missing_rows: Vec<(i64, i64, i64)>,
    extra_rows: Vec<(i64, i64, i64)>,
}

pub fn run_full_consistency_checks(
    ctx: &TestContext,
    modes: &[StressStorageMode],
) -> CoreResult<()> {
    for mode in modes {
        kv_storage_consistency(ctx, *mode)?;
        ctx.watchdog.report_progress();
        sql_storage_consistency(ctx, *mode)?;
        ctx.watchdog.report_progress();
    }
    dataframe_consistency(ctx)?;
    ctx.watchdog.report_progress();
    Ok(())
}

fn write_check<T: Serialize>(ctx: &TestContext, name: &str, report: &T) {
    let Some(paths) = ctx.artifact_paths.as_ref() else {
        return;
    };
    let path = paths.checks_dir.join(name);
    if let Ok(body) = serde_json::to_string_pretty(report) {
        let _ = fs::write(path, body);
    }
}

fn collect_dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(meta) = entry.metadata() {
                if meta.is_dir() {
                    total = total.saturating_add(collect_dir_size(&path));
                } else {
                    total = total.saturating_add(meta.len());
                }
            }
        }
    }
    total
}

fn collect_disk_sizes(root: &Path) -> (u64, u64, u64) {
    let wal_bytes = fs::metadata(root.join("lsm.wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    let mut sst_bytes = 0u64;
    if let Ok(entries) = fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sst") {
                if let Ok(meta) = entry.metadata() {
                    sst_bytes = sst_bytes.saturating_add(meta.len());
                }
            }
        }
    }
    let total = collect_dir_size(root);
    (wal_bytes, sst_bytes, total)
}

fn kv_storage_consistency(ctx: &TestContext, mode: StressStorageMode) -> CoreResult<()> {
    let start = Instant::now();
    let expected: BTreeMap<Vec<u8>, Vec<u8>> = (0..128u32)
        .map(|i| {
            let key = format!("consistency_kv_{i:04}").into_bytes();
            let val = format!("val_{i:04}").into_bytes();
            (key, val)
        })
        .collect();

    let store = open_store_for_mode(&ctx.db_path, mode)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    for (k, v) in &expected {
        txn.put(k.clone(), v.clone())?;
        ctx.metrics.record_success();
    }
    txn.commit_self()?;
    if mode == StressStorageMode::Disk {
        store.flush()?;
    }
    drop(store);

    let store = open_store_for_mode(&ctx.db_path, mode)?;
    let mut reader = store.begin(TxnMode::ReadOnly)?;
    let mut actual: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (k, v) in reader.scan_prefix(b"consistency_kv_")? {
        actual.insert(k, v);
    }
    ctx.metrics.record_latency(start.elapsed());

    let expected_keys: BTreeSet<_> = expected.keys().cloned().collect();
    let actual_keys: BTreeSet<_> = actual.keys().cloned().collect();
    let missing_keys: Vec<String> = expected_keys
        .difference(&actual_keys)
        .map(|k| String::from_utf8_lossy(k).to_string())
        .collect();
    let extra_keys: Vec<String> = actual_keys
        .difference(&expected_keys)
        .map(|k| String::from_utf8_lossy(k).to_string())
        .collect();
    let mut mismatched_keys = Vec::new();
    for (k, v) in &expected {
        if let Some(actual_v) = actual.get(k) {
            if actual_v != v {
                mismatched_keys.push(String::from_utf8_lossy(k).to_string());
            }
        }
    }

    let (wal_bytes, sst_bytes, total_bytes) = if mode == StressStorageMode::Disk {
        let root = storage_root_for_mode(&ctx.db_path, mode);
        collect_disk_sizes(&root)
    } else {
        (0, 0, 0)
    };

    let report = KvConsistencyReport {
        mode: mode.as_str().to_string(),
        expected_count: expected.len(),
        actual_count: actual.len(),
        missing_keys,
        extra_keys,
        mismatched_keys,
        wal_bytes,
        sst_bytes,
        total_bytes,
    };
    write_check(
        ctx,
        &format!("kv_storage_consistency_{}.json", mode.as_str()),
        &report,
    );

    assert!(
        report.missing_keys.is_empty()
            && report.extra_keys.is_empty()
            && report.mismatched_keys.is_empty(),
        "kv_storage_consistency mismatch: {:?}",
        report
    );

    Ok(())
}

fn sql_storage_consistency(ctx: &TestContext, mode: StressStorageMode) -> CoreResult<()> {
    let start = Instant::now();
    let store = Arc::new(open_store_for_mode(&ctx.db_path, mode)?);
    let bridge = TxnBridge::new(store.clone());
    let meta = TableMetadata::new(
        "consistency_users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ColumnMetadata::new("age", ResolvedType::Integer),
        ],
    )
    .with_table_id(1);

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
        .map_err(to_core_error)?;
    ctx.metrics.record_success();

    let mut index_hits: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let alice_row = bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("alice".into()))
            })?;
            index_hits.insert("alice".to_string(), ids.clone());
            ctx.with_table(&meta, |table| table.get(1))
        })
        .map_err(to_core_error)?
        .ok_or_else(|| CoreError::InvalidFormat("alice row missing".into()))?;
    assert_eq!(alice_row[1], SqlValue::Text("alice".into()));

    bridge
        .with_write_txn(|ctx| {
            ctx.with_table(&meta, |table| table.update(2, &user_row(2, "robert", 25)))?;
            ctx.with_index(1, true, vec![1], |index| {
                index.delete(&user_row(2, "bob", 25), 2)?;
                index.insert(&user_row(2, "robert", 25), 2)
            })?;
            Ok(())
        })
        .map_err(to_core_error)?;
    ctx.metrics.record_success();

    bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("robert".into()))
            })?;
            index_hits.insert("robert".to_string(), ids.clone());
            let ids_old = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("bob".into()))
            })?;
            index_hits.insert("bob".to_string(), ids_old.clone());
            Ok(())
        })
        .map_err(to_core_error)?;
    ctx.metrics.record_success();

    bridge
        .with_write_txn(|ctx| {
            ctx.with_index(1, true, vec![1], |index| {
                index.delete(&user_row(3, "carol", 30), 3)
            })?;
            ctx.with_table(&meta, |table| table.delete(3))?;
            Ok(())
        })
        .map_err(to_core_error)?;
    ctx.metrics.record_success();

    bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("carol".into()))
            })?;
            index_hits.insert("carol".to_string(), ids.clone());
            Ok(())
        })
        .map_err(to_core_error)?;

    bridge
        .with_write_txn_explicit(|ctx| {
            ctx.with_table(&meta, |table| insert_user(table, 4, "dave", 40))?;
            ctx.with_index(1, true, vec![1], |index| {
                index.insert(&user_row(4, "dave", 40), 4)
            })?;
            Ok(((), false))
        })
        .map_err(to_core_error)?;
    ctx.metrics.record_success();

    bridge
        .with_read_txn(|ctx| {
            let ids = ctx.with_index(1, true, vec![1], |index| {
                index.lookup(&SqlValue::Text("dave".into()))
            })?;
            index_hits.insert("dave".to_string(), ids.clone());
            Ok(())
        })
        .map_err(to_core_error)?;

    let row_count = bridge
        .with_read_txn(|ctx| {
            ctx.with_table(&meta, |table| {
                let count = table.scan()?.count();
                Ok::<_, alopex_sql::storage::StorageError>(count)
            })
        })
        .map_err(to_core_error)?;
    ctx.metrics.record_latency(start.elapsed());

    let mut report = SqlConsistencyReport {
        mode: mode.as_str().to_string(),
        rows_inserted: 3,
        rows_after_updates: row_count,
        index_hits: index_hits.clone(),
        missing_keys: Vec::new(),
        extra_keys: Vec::new(),
        mismatched_keys: Vec::new(),
    };
    let expected_row_count = 2usize;
    if report.rows_after_updates != expected_row_count {
        report.mismatched_keys.push(format!(
            "row_count expected {expected_row_count}, got {}",
            row_count
        ));
    }
    let mut expected_hits: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    expected_hits.insert("alice".to_string(), vec![1]);
    expected_hits.insert("robert".to_string(), vec![2]);
    expected_hits.insert("bob".to_string(), Vec::new());
    expected_hits.insert("carol".to_string(), Vec::new());
    expected_hits.insert("dave".to_string(), Vec::new());

    for (key, expected) in &expected_hits {
        match index_hits.get(key) {
            Some(actual) => {
                if actual != expected {
                    report.mismatched_keys.push(format!(
                        "index_hits[{key}] expected {expected:?}, got {actual:?}"
                    ));
                }
            }
            None => report.missing_keys.push(key.clone()),
        }
    }
    for key in index_hits.keys() {
        if !expected_hits.contains_key(key) {
            report.extra_keys.push(key.clone());
        }
    }
    write_check(
        ctx,
        &format!("sql_storage_consistency_{}.json", mode.as_str()),
        &report,
    );
    assert!(
        report.missing_keys.is_empty()
            && report.extra_keys.is_empty()
            && report.mismatched_keys.is_empty(),
        "sql_storage_consistency mismatch: {:?}",
        report
    );
    Ok(())
}

fn dataframe_consistency(ctx: &TestContext) -> CoreResult<()> {
    let start = Instant::now();
    let left = DataFrame::new(vec![
        s_i64("id", vec![1, 2, 3]),
        s_i64("value", vec![10, 20, 30]),
    ])
    .map_err(to_core_error)?;
    let right = DataFrame::new(vec![
        s_i64("id", vec![2, 3, 4]),
        s_i64("value", vec![200, 300, 400]),
    ])
    .map_err(to_core_error)?;

    let joined = left
        .join(&right, vec!["id".to_string()], JoinType::Inner)
        .map_err(to_core_error)?;
    let sorted = joined
        .sort(vec!["id".to_string()], vec![false])
        .map_err(to_core_error)?;

    let ids = sorted.column("id").map_err(to_core_error)?.to_arrow();
    let left_vals = sorted.column("value").map_err(to_core_error)?.to_arrow();
    let right_vals = sorted
        .column("value_right")
        .map_err(to_core_error)?
        .to_arrow();

    let ids = ids[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let left_vals = left_vals[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let right_vals = right_vals[0].as_any().downcast_ref::<Int64Array>().unwrap();

    let mut rows = Vec::new();
    for i in 0..ids.len() {
        rows.push((ids.value(i), left_vals.value(i), right_vals.value(i)));
    }

    let expected_rows = vec![(2, 20, 200), (3, 30, 300)];
    let mut missing_rows = Vec::new();
    let mut extra_rows = Vec::new();
    for row in &expected_rows {
        if !rows.contains(row) {
            missing_rows.push(*row);
        }
    }
    for row in &rows {
        if !expected_rows.contains(row) {
            extra_rows.push(*row);
        }
    }

    let mut sums = BTreeMap::new();
    sums.insert("left_sum".to_string(), left_vals.values().iter().sum());
    sums.insert("right_sum".to_string(), right_vals.values().iter().sum());

    let report = DataFrameConsistencyReport {
        row_count: rows.len(),
        rows: rows.clone(),
        sums,
        missing_rows: missing_rows.clone(),
        extra_rows: extra_rows.clone(),
    };
    write_check(ctx, "dataframe_consistency.json", &report);
    assert!(
        missing_rows.is_empty() && extra_rows.is_empty(),
        "dataframe consistency mismatch: {:?}",
        report
    );
    ctx.metrics.record_latency(start.elapsed());
    ctx.metrics.record_success();
    Ok(())
}

fn s_i64(name: &str, values: Vec<i64>) -> Series {
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    Series::from_arrow(name, vec![array]).unwrap()
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

fn to_core_error<E: std::fmt::Display>(err: E) -> CoreError {
    CoreError::InvalidFormat(err.to_string())
}
