//! Synthetic missing-index fault injection, NOT a database emitted by v0.8.15.
use super::*;
use alopex_sql::catalog::persistent::{INDEXES_PREFIX, PersistedIndexMeta};
use alopex_sql::executor::{ConstraintViolation, ExecutorError};

#[path = "old_unique_consumer.rs"]
mod old_producer;

#[path = "legacy_pk_consumer.rs"]
mod legacy_pk;

fn assert_recovery_error(store: Arc<MemoryKV>, diagnostic: &str) {
    match PersistentCatalog::load(store) {
        Err(alopex_sql::catalog::persistent::CatalogError::IndexRecovery(message)) => {
            assert!(
                message.contains(diagnostic),
                "unexpected recovery diagnostic: {message}"
            );
        }
        Err(error) => panic!("wrong error owner: {error}"),
        Ok(_) => panic!("catalog recovery unexpectedly succeeded"),
    }
}

pub(super) fn remove_unique_indexes(store: &MemoryKV) {
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let entries: Vec<_> = txn.scan_prefix(INDEXES_PREFIX).unwrap().collect();
    let mut removed = 0;
    for (key, bytes) in entries {
        let index: PersistedIndexMeta = bincode::deserialize(&bytes).unwrap();
        if index.table != "items" || !index.unique || index.name == "__pk_items" {
            continue;
        }
        let keys: Vec<_> = txn
            .scan_prefix(&KeyEncoder::index_prefix(index.index_id))
            .unwrap()
            .map(|(key, _)| key)
            .collect();
        for key in keys {
            txn.delete(key).unwrap();
        }
        txn.delete(key).unwrap();
        removed += 1;
    }
    assert!(
        removed > 0,
        "fault injection must remove a real UNIQUE index"
    );
    txn.commit_self().unwrap();
}

fn attempt_sql(
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<PersistentCatalog<MemoryKV>>>,
    sql: &str,
) -> alopex_sql::executor::Result<ExecutionResult> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
    assert_eq!(statements.len(), 1);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statements[0])
        .map_err(ExecutorError::from)?;
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    let result = Executor::new(store.clone(), catalog.clone()).execute_in_txn(plan, &mut borrowed);
    drop(borrowed);
    if result.is_ok() {
        txn.commit_self().unwrap();
        catalog.write().unwrap().apply_overlay(overlay);
    } else {
        txn.rollback_self().unwrap();
    }
    result
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue572_targetless_uses_recovered_unique_and_primary_indexes() {
    let (store, catalog) = fixture(
        "CREATE TABLE items (obsolete INTEGER, id INTEGER PRIMARY KEY, value INTEGER, \
         CONSTRAINT uq_value UNIQUE(value)); INSERT INTO items VALUES (0, 1, 10);",
    );
    let old_unique_id = {
        let guard = catalog.read().unwrap();
        assert_eq!(
            guard.get_index("__pk_items").unwrap().column_indices,
            vec![1]
        );
        let unique = guard.get_index("uq_value").unwrap();
        assert_eq!(unique.column_indices, vec![2]);
        assert_eq!(
            snapshot(&store, &KeyEncoder::index_prefix(unique.index_id)).len(),
            1
        );
        unique.index_id
    };

    // Synthetic old metadata: stale PK ordinal plus missing named UNIQUE.
    // This is not an old-producer fixture or an execution of fixed ALTER DDL.
    let table_id = legacy_drop(&store, &catalog, "items", 0);
    remove_unique_indexes(&store);
    let persisted: Vec<PersistedIndexMeta> = snapshot(&store, INDEXES_PREFIX)
        .iter()
        .map(|(_, bytes)| bincode::deserialize(bytes).unwrap())
        .collect();
    assert!(!persisted.iter().any(|index| index.name == "uq_value"));
    assert_eq!(
        persisted
            .iter()
            .find(|index| index.name == "__pk_items")
            .unwrap()
            .column_indices,
        vec![1]
    );
    assert!(snapshot(&store, &KeyEncoder::index_prefix(old_unique_id)).is_empty());
    let rows = snapshot(&store, &KeyEncoder::table_prefix(table_id));
    let sequence = snapshot(&store, &KeyEncoder::sequence_key(table_id));
    let before_load = snapshot(&store, b"");
    let recovered = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    assert_ne!(
        snapshot(&store, b""),
        before_load,
        "load must actually repair metadata"
    );
    {
        let guard = recovered.read().unwrap();
        let primary = guard.get_index("__pk_items").unwrap();
        let unique = guard.get_index("uq_value").unwrap();
        assert_eq!(primary.column_indices, vec![0]);
        assert_eq!(unique.column_indices, vec![1]);
        assert!(primary.unique && unique.unique);
        assert_ne!(primary.index_id, unique.index_id);
        assert_ne!(old_unique_id, unique.index_id);
        let key = KeyEncoder::index_value_prefix(unique.index_id, &SqlValue::Integer(10)).unwrap();
        assert_eq!(snapshot(&store, &key).len(), 1);
    }
    assert_eq!(snapshot(&store, &KeyEncoder::table_prefix(table_id)), rows);
    assert_eq!(
        snapshot(&store, &KeyEncoder::sequence_key(table_id)),
        sequence
    );

    // Both successful skip operations commit: rollback cannot hide a write.
    for sql in [
        "INSERT INTO items VALUES (2, 10) ON CONFLICT DO NOTHING",
        "INSERT INTO items VALUES (1, 20) ON CONFLICT DO NOTHING",
    ] {
        let before = snapshot(&store, b"");
        let result = attempt_sql(store.clone(), recovered.clone(), sql);
        assert!(
            matches!(result, Ok(ExecutionResult::RowsAffected(0))),
            "{sql}: {result:?}"
        );
        assert_eq!(
            snapshot(&store, b""),
            before,
            "successful skip changed KV: {sql}"
        );
    }
    let result = attempt_sql(
        store.clone(),
        recovered.clone(),
        "INSERT INTO items VALUES (2, 20) ON CONFLICT DO NOTHING",
    );
    assert!(
        matches!(result, Ok(ExecutionResult::RowsAffected(1))),
        "{result:?}"
    );
    let expected = vec![
        vec![SqlValue::Integer(1), SqlValue::Integer(10)],
        vec![SqlValue::Integer(2), SqlValue::Integer(20)],
    ];
    let before_reload = snapshot(&store, b"");
    let reopened = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    assert_eq!(
        snapshot(&store, b""),
        before_reload,
        "second load must not write"
    );
    for active in [recovered, reopened] {
        let result = run_sql_in_txn(
            store.clone(),
            active,
            TxnMode::ReadOnly,
            "SELECT id, value FROM items ORDER BY id",
        );
        let ExecutionResult::Query(query) = result else {
            panic!("expected rows");
        };
        assert_eq!(query.rows, expected);
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_missing_unique_backfills_rows_and_is_idempotent() {
    for definition in [
        "id INTEGER PRIMARY KEY, value INTEGER UNIQUE, tail INTEGER",
        "id INTEGER PRIMARY KEY, value INTEGER, tail INTEGER, CONSTRAINT uq_value UNIQUE(value)",
        "id INTEGER PRIMARY KEY, value INTEGER, tail INTEGER, UNIQUE(value, tail)",
    ] {
        let (store, catalog) = fixture(&format!(
            "CREATE TABLE items ({definition}); \
             INSERT INTO items VALUES (1, 10, 20), (2, NULL, 20), (3, NULL, 20); \
             CREATE TABLE neighbor (id INTEGER PRIMARY KEY); INSERT INTO neighbor VALUES (7);"
        ));
        let table_id = catalog.read().unwrap().get_table("items").unwrap().table_id;
        remove_unique_indexes(&store);
        let before = snapshot(&store, b"");
        let recovered = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        let guard = recovered.read().unwrap();
        let indexes = guard.get_indexes_for_table("items");
        let unique = indexes
            .iter()
            .find(|index| index.unique && index.name != "__pk_items")
            .expect("missing UNIQUE index must be recovered");
        assert_eq!(
            snapshot(&store, &KeyEncoder::index_prefix(unique.index_id)).len(),
            1
        );
        assert_eq!(guard.get_table("items").unwrap().table_id, table_id);
        drop(guard);
        for (key, value) in &before {
            if key != alopex_sql::catalog::persistent::META_KEY {
                assert_eq!(
                    store.begin(TxnMode::ReadOnly).unwrap().get(key).unwrap(),
                    Some(value.clone())
                );
            }
        }
        let after = snapshot(&store, b"");
        assert!(matches!(
            attempt_sql(
                store.clone(),
                recovered.clone(),
                "INSERT INTO items VALUES (4, 10, 20)"
            ),
            Err(ExecutorError::ConstraintViolation(
                ConstraintViolation::Unique { .. }
            ))
        ));
        assert_eq!(snapshot(&store, b""), after);
        if definition.contains("UNIQUE(value, tail)") {
            assert!(
                attempt_sql(
                    store.clone(),
                    recovered.clone(),
                    "INSERT INTO items VALUES (6, 10, 21)"
                )
                .is_ok()
            );
        }
        assert!(
            attempt_sql(
                store.clone(),
                recovered,
                "INSERT INTO items VALUES (5, NULL, 20)"
            )
            .is_ok()
        );
        let after = snapshot(&store, b"");
        PersistentCatalog::load(store.clone()).unwrap();
        assert_eq!(
            snapshot(&store, b""),
            after,
            "second load must not rewrite data"
        );
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_missing_unique_duplicate_rolls_back_combined_repairs() {
    let (store, catalog) = fixture(
        "CREATE TABLE items (obsolete INTEGER, id INTEGER PRIMARY KEY, value INTEGER UNIQUE); \
         INSERT INTO items VALUES (0, 1, 10);",
    );
    remove_unique_indexes(&store);
    // Existing #568 recovery and missing-index recovery must share one commit boundary.
    let id = legacy_drop(&store, &catalog, "items", 0);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    txn.put(
        KeyEncoder::row_key(id, 2),
        RowCodec::encode(&[SqlValue::Integer(2), SqlValue::Integer(10)]),
    )
    .unwrap();
    txn.commit_self().unwrap();
    let before = snapshot(&store, b"");
    let error = match PersistentCatalog::load(store.clone()) {
        Ok(_) => panic!("duplicate canonical rows must prevent catalog recovery"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("items"),
        "diagnostic must identify table: {error}"
    );
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_existing_unique_index_is_not_recreated() {
    let (store, _) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE); \
         INSERT INTO items VALUES (1, 10);",
    );
    let before = snapshot(&store, b"");
    PersistentCatalog::load(store.clone()).unwrap();
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_same_name_nonunique_index_is_not_a_constraint() {
    let (store, _) = fixture("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE);");
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let entries: Vec<_> = txn.scan_prefix(INDEXES_PREFIX).unwrap().collect();
    let mut changed = false;
    for (key, bytes) in entries {
        let mut index: PersistedIndexMeta = bincode::deserialize(&bytes).unwrap();
        if index.name.starts_with("__uq_items_") {
            index.unique = false;
            txn.put(key, bincode::serialize(&index).unwrap()).unwrap();
            changed = true;
        }
    }
    assert!(changed);
    txn.commit_self().unwrap();
    let before = snapshot(&store, b"");
    assert_recovery_error(store.clone(), "conflicts with an existing definition");
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_missing_unique_unsupported_storage_is_not_empty_backfill() {
    for external in [false, true] {
        let (store, catalog) = fixture(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE); INSERT INTO items VALUES (1, 10);",
        );
        remove_unique_indexes(&store);
        let mut table = catalog.read().unwrap().get_table("items").unwrap().clone();
        if external {
            table.table_type = alopex_sql::catalog::persistent::TableType::External;
        } else {
            table.storage_options.storage_type = alopex_sql::catalog::StorageType::Columnar;
        }
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog
            .write()
            .unwrap()
            .persist_create_table(&mut txn, &table)
            .unwrap();
        txn.commit_self().unwrap();
        let before = snapshot(&store, b"");
        assert_recovery_error(store.clone(), "unsupported for this storage");
        assert_eq!(snapshot(&store, b""), before);
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_column_unique_flag_without_constraint_json_is_recovered() {
    // Synthetic older/direct-metadata boundary, NOT v0.8.15 SQL producer output.
    // Normal v0.8.15 SQL persists both the flag and normalized constraint JSON.
    let (store, catalog) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE); \
         INSERT INTO items VALUES (1, 10);",
    );
    remove_unique_indexes(&store);
    let mut table = catalog.read().unwrap().get_table("items").unwrap().clone();
    assert!(table.columns[1].unique);
    table.constraints.clear();
    table
        .properties
        .remove(alopex_sql::catalog::RELATIONAL_CONSTRAINTS_PROPERTY);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    catalog
        .write()
        .unwrap()
        .persist_create_table(&mut txn, &table)
        .unwrap();
    txn.commit_self().unwrap();
    let before = snapshot(&store, &KeyEncoder::table_prefix(table.table_id));
    let recovered = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    {
        let guard = recovered.read().unwrap();
        let indexes = guard.get_indexes_for_table("items");
        let index = indexes
            .iter()
            .find(|index| index.unique && index.columns == ["value"])
            .expect("persisted unique column flag must not be silently ignored");
        assert_eq!(
            snapshot(&store, &KeyEncoder::index_prefix(index.index_id)).len(),
            1
        );
    }
    assert_eq!(
        snapshot(&store, &KeyEncoder::table_prefix(table.table_id)),
        before
    );
    assert!(matches!(
        attempt_sql(
            store.clone(),
            recovered.clone(),
            "INSERT INTO items VALUES (2, 10)"
        ),
        Err(ExecutorError::ConstraintViolation(
            ConstraintViolation::Unique { .. }
        ))
    ));
    assert!(
        attempt_sql(
            store.clone(),
            recovered,
            "INSERT INTO items VALUES (3, NULL)"
        )
        .is_ok()
    );
    let before = snapshot(&store, b"");
    PersistentCatalog::load(store.clone()).unwrap();
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_primary_key_after_other_constraints_is_enforced() {
    for preceding in ["UNIQUE(value)", "CHECK(value > 0)"] {
        let (store, catalog) = fixture(&format!(
            "CREATE TABLE items (id INTEGER, value INTEGER, {preceding}, PRIMARY KEY(id));"
        ));
        assert_eq!(
            catalog
                .read()
                .unwrap()
                .get_table("items")
                .unwrap()
                .primary_key,
            Some(vec!["id".to_string()])
        );
        assert!(
            attempt_sql(
                store.clone(),
                catalog.clone(),
                "INSERT INTO items VALUES (1, 10)"
            )
            .is_ok()
        );
        assert!(matches!(
            attempt_sql(
                store.clone(),
                catalog.clone(),
                "INSERT INTO items VALUES (1, 20)"
            ),
            Err(ExecutorError::ConstraintViolation(
                ConstraintViolation::PrimaryKey { .. }
            ))
        ));
        assert!(matches!(
            attempt_sql(store, catalog, "INSERT INTO items VALUES (NULL, 30)"),
            Err(ExecutorError::ConstraintViolation(
                ConstraintViolation::NotNull { .. } | ConstraintViolation::PrimaryKey { .. }
            )) | Err(ExecutorError::Planner(
                alopex_sql::planner::PlannerError::NullConstraintViolation { .. }
            ))
        ));
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_multiple_primary_key_declarations_are_rejected() {
    let catalog = alopex_sql::catalog::MemoryCatalog::new();
    for sql in [
        "CREATE TABLE items (a INTEGER, b INTEGER, PRIMARY KEY(a), PRIMARY KEY(b))",
        "CREATE TABLE items (a INTEGER PRIMARY KEY, b INTEGER, PRIMARY KEY(b))",
        "CREATE TABLE items (a INTEGER PRIMARY KEY, b INTEGER PRIMARY KEY)",
    ] {
        let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
        assert!(
            Planner::new(&catalog).plan(&statements[0]).is_err(),
            "multiple declarations must not be silently merged: {sql}"
        );
    }
    let statement = Parser::parse_sql(
        &AlopexDialect,
        "CREATE TABLE items (a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
    )
    .unwrap();
    assert!(Planner::new(&catalog).plan(&statement[0]).is_ok());
}

fn columnar_pk_recovery_fixture() -> (Arc<MemoryKV>, Arc<RwLock<PersistentCatalog<MemoryKV>>>) {
    use std::io::Write;
    let (store, catalog) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER) WITH (storage='columnar')",
    );
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"id,value\n1,10\n").unwrap();
    file.flush().unwrap();
    assert_eq!(
        attempt_sql(
            store.clone(),
            catalog.clone(),
            &format!(
                "COPY items FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
                file.path().to_str().unwrap().replace('\'', "''")
            )
        )
        .unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    (store, catalog)
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_columnar_missing_primary_index_is_rejected_without_writes() {
    let (store, catalog) = columnar_pk_recovery_fixture();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let entries = txn.scan_prefix(INDEXES_PREFIX).unwrap().collect::<Vec<_>>();
    let mut removed = 0;
    for (key, bytes) in entries {
        let index: PersistedIndexMeta = bincode::deserialize(&bytes).unwrap();
        if index.table == "items" && index.name == "__pk_items" {
            assert!(index.unique);
            txn.delete(key).unwrap();
            removed += 1;
        }
    }
    assert_eq!(
        removed, 1,
        "fault injection must remove exactly the PK metadata"
    );
    txn.commit_self().unwrap();
    drop(catalog);
    let before = snapshot(&store, b"");
    assert_recovery_error(
        store.clone(),
        "requires constraint index recovery unsupported for this storage",
    );
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_columnar_pk_normalization_is_rejected_without_writes() {
    let (store, catalog) = columnar_pk_recovery_fixture();
    let mut table = catalog.read().unwrap().get_table("items").unwrap().clone();
    assert_eq!(
        table.storage_options.storage_type,
        alopex_sql::catalog::StorageType::Columnar
    );
    assert_eq!(table.primary_key, Some(vec!["id".to_string()]));
    assert!(table.columns[0].not_null);
    table.columns[0].not_null = false;
    let indexes_before = snapshot(&store, INDEXES_PREFIX);
    assert!(!indexes_before.is_empty());
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    catalog
        .write()
        .unwrap()
        .persist_create_table(&mut txn, &table)
        .unwrap();
    txn.commit_self().unwrap();
    drop(catalog);
    assert_eq!(snapshot(&store, INDEXES_PREFIX), indexes_before);
    let before = snapshot(&store, b"");
    assert_recovery_error(
        store.clone(),
        "requires primary key recovery unsupported for this storage",
    );
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue575_585_columnar_reload_copy_unique_preserves_all_bytes() {
    use alopex_sql::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
    use alopex_sql::storage::SqlTxn;
    use std::io::Write;

    let (store, catalog) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER, \
         CONSTRAINT uq_value UNIQUE (value)) WITH (storage='columnar')",
    );
    let mut original = tempfile::NamedTempFile::new().unwrap();
    original.write_all(b"id,value\n1,10\n").unwrap();
    original.flush().unwrap();
    assert_eq!(
        attempt_sql(
            store.clone(),
            catalog.clone(),
            &format!(
                "COPY items FROM '{}' WITH (FORMAT CSV, HEADER true)",
                original.path().to_str().unwrap().replace('\'', "''")
            ),
        )
        .unwrap(),
        ExecutionResult::RowsAffected(1),
    );
    drop(catalog);
    let before_load = snapshot(&store, b"");
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    assert_eq!(
        snapshot(&store, b""),
        before_load,
        "normal reload must not repair/write"
    );
    {
        let guard = catalog.read().unwrap();
        let table = guard.get_table("items").unwrap();
        assert_eq!(
            table.storage_options.storage_type,
            alopex_sql::catalog::StorageType::Columnar
        );
        assert_eq!(
            table.primary_key.as_deref(),
            Some(["id".to_string()].as_slice())
        );
        let unique = guard.get_index("uq_value").unwrap();
        assert!(unique.unique);
        assert_eq!(unique.columns, vec!["value"]);
        assert_eq!(unique.column_indices, vec![1]);
    }

    // The first input row is valid. The later value duplicates a real segment
    // written before PersistentCatalog::load, not row-store index contents.
    let mut duplicate = tempfile::NamedTempFile::new().unwrap();
    duplicate.write_all(b"id,value\n3,30\n2,10\n").unwrap();
    duplicate.flush().unwrap();
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_write().unwrap();
    let before = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let result = execute_copy(
        &mut txn,
        &*catalog.read().unwrap(),
        "items",
        duplicate.path().to_str().unwrap(),
        FileFormat::Csv,
        CopyOptions { header: true },
        &CopySecurityConfig::default(),
    );
    let after = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    // Do not use rollback to conceal partial writes by the rejected COPY.
    let commit = txn.commit();
    eprintln!(
        "persistent reload COPY: {result:?}; unchanged={}; commit={commit:?}",
        before == after
    );
    assert!(
        matches!(&result,
        Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. }))
        if index_name == "uq_value" && columns == &["value"]),
        "{result:?}"
    );
    assert_eq!(after, before);
    commit.unwrap();
    assert_eq!(snapshot(&store, b""), before);
    let ExecutionResult::Query(query) = run_sql_in_txn(
        store,
        catalog,
        TxnMode::ReadOnly,
        "SELECT id, value FROM items ORDER BY id",
    ) else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
    );
}
// This row-storage regression does not validate columnar ALTER support.
#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue575_585_drop_column_preserves_unique_index_after_merge() {
    let (store, catalog) = fixture(
        "CREATE TABLE items (obsolete INTEGER, id INTEGER PRIMARY KEY, value INTEGER, \
         CONSTRAINT uq_value UNIQUE(value)); \
         INSERT INTO items VALUES (0, 1, 10);",
    );
    {
        let guard = catalog.read().unwrap();
        assert_eq!(
            guard
                .get_table("items")
                .unwrap()
                .storage_options
                .storage_type,
            alopex_sql::catalog::StorageType::Row
        );
        assert_eq!(
            guard.get_index("__pk_items").unwrap().column_indices,
            vec![1]
        );
        assert_eq!(guard.get_index("uq_value").unwrap().column_indices, vec![2]);
    }

    // The leading column forces both PK and UNIQUE ordinals to move.
    run_sql_in_txn(
        store.clone(),
        catalog.clone(),
        TxnMode::ReadWrite,
        "ALTER TABLE items DROP COLUMN obsolete; INSERT INTO items VALUES (2, 20);",
    );
    {
        let guard = catalog.read().unwrap();
        assert_eq!(
            guard.get_index("__pk_items").unwrap().column_indices,
            vec![0]
        );
        assert_eq!(guard.get_index("uq_value").unwrap().column_indices, vec![1]);
    }

    for (sql, primary) in [
        ("INSERT INTO items VALUES (1, 30)", true),
        ("INSERT INTO items VALUES (3, 20)", false),
    ] {
        let before = snapshot(&store, b"");
        let result = attempt_sql(store.clone(), catalog.clone(), sql);
        if primary {
            assert!(
                matches!(&result,
                    Err(ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { columns, .. }))
                    if columns == &["id"]),
                "{sql}: {result:?}"
            );
        } else {
            assert!(
                matches!(&result,
                    Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. }))
                    if index_name == "uq_value" && columns == &["value"]),
                "{sql}: {result:?}"
            );
        }
        assert_eq!(snapshot(&store, b""), before);
    }

    // Observe the real executor's index choice before and after catalog reload.
    // EXPLAIN ANALYZE executes the same indexed SELECT; plain rows alone could
    // accidentally pass through a full-scan fallback.
    for reopen in [false, true] {
        let before_load = snapshot(&store, b"");
        let active = if reopen {
            Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()))
        } else {
            catalog.clone()
        };
        assert_eq!(snapshot(&store, b""), before_load);
        {
            let guard = active.read().unwrap();
            assert_eq!(
                guard.get_index("__pk_items").unwrap().column_indices,
                vec![0]
            );
            let unique = guard.get_index("uq_value").unwrap();
            assert!(unique.unique);
            assert_eq!(unique.column_indices, vec![1]);
        }
        for (value, id) in [(10, 1), (20, 2)] {
            let sql = format!("SELECT id FROM items WHERE value = {value}");
            let ExecutionResult::Query(explain) = run_sql_in_txn(
                store.clone(),
                active.clone(),
                TxnMode::ReadOnly,
                &format!("EXPLAIN ANALYZE {sql}"),
            ) else {
                panic!("expected EXPLAIN ANALYZE rows");
            };
            let SqlValue::Text(plan) = &explain.rows[0][0] else {
                panic!("expected EXPLAIN ANALYZE text");
            };
            assert!(
                plan.contains("IndexScan index=uq_value table=items"),
                "reopen={reopen}, {sql}: {plan}"
            );
            let ExecutionResult::Query(query) =
                run_sql_in_txn(store.clone(), active.clone(), TxnMode::ReadOnly, &sql)
            else {
                panic!("expected indexed query rows");
            };
            assert_eq!(query.rows, vec![vec![SqlValue::Integer(id)]]);
        }
    }
}

// Synthetic persisted-index faults, NOT an old-producer compatibility fixture.
fn columnar_stale_unique_fixture(duplicate: bool) -> (Arc<MemoryKV>, u32, Vec<u8>) {
    use std::io::Write;

    let (store, catalog) =
        fixture("CREATE TABLE items (id INTEGER, value INTEGER) WITH (storage='columnar')");
    let mut csv = tempfile::NamedTempFile::new().unwrap();
    csv.write_all(if duplicate {
        b"id,value\n1,10\n2,10\n"
    } else {
        b"id,value\n1,10\n2,20\n"
    })
    .unwrap();
    csv.flush().unwrap();
    let copy = format!(
        "COPY items FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
        csv.path().to_str().unwrap().replace('\'', "''")
    );
    assert_eq!(
        attempt_sql(store.clone(), catalog.clone(), &copy).unwrap(),
        ExecutionResult::RowsAffected(2)
    );
    // The duplicate fixture starts with a legitimate nonunique declaration.
    // Only fault injection below changes it to UNIQUE; CREATE need not accept duplicates.
    let ddl = if duplicate {
        "CREATE INDEX uq_value ON items(value)"
    } else {
        "CREATE UNIQUE INDEX uq_value ON items(value)"
    };
    assert_eq!(
        attempt_sql(store.clone(), catalog.clone(), ddl).unwrap(),
        ExecutionResult::Success
    );
    let index_id = catalog
        .read()
        .unwrap()
        .get_index("uq_value")
        .unwrap()
        .index_id;
    drop(catalog);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let entries: Vec<_> = txn.scan_prefix(INDEXES_PREFIX).unwrap().collect();
    let mut changed = Vec::new();
    for (key, bytes) in entries {
        let mut index: PersistedIndexMeta = bincode::deserialize(&bytes).unwrap();
        if index.index_id == index_id {
            assert_eq!(index.name, "uq_value");
            assert_eq!(index.columns, vec!["value"]);
            assert_eq!(index.column_indices, vec![1]);
            assert_eq!(index.unique, !duplicate);
            index.unique = true;
            index.column_indices = vec![0];
            txn.put(key.clone(), bincode::serialize(&index).unwrap())
                .unwrap();
            changed.push(key);
        }
    }
    assert_eq!(changed.len(), 1);
    // A real key under the index prefix makes recovery's pre-validation deletion
    // observable: duplicate failure must restore it, success must remove it.
    txn.put(
        KeyEncoder::index_key(index_id, &SqlValue::Integer(99), 99).unwrap(),
        b"synthetic stale derived entry".to_vec(),
    )
    .unwrap();
    txn.commit_self().unwrap();
    (store, index_id, changed.pop().unwrap())
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue575_585_columnar_unique_stale_position_repair_is_idempotent() {
    use std::io::Write;

    let (store, index_id, metadata_key) = columnar_stale_unique_fixture(false);
    let prefix = KeyEncoder::index_prefix(index_id);
    assert_eq!(snapshot(&store, &prefix).len(), 1);
    let before = snapshot(&store, b"");
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    {
        let guard = catalog.read().unwrap();
        let index = guard.get_index("uq_value").unwrap();
        assert!(index.unique);
        assert_eq!(index.index_id, index_id);
        assert_eq!(index.columns, vec!["value"]);
        assert_eq!(index.column_indices, vec![1]);
    }
    let repaired = snapshot(&store, b"");
    assert_ne!(repaired, before, "first load must actually repair metadata");
    assert!(snapshot(&store, &prefix).is_empty());
    let unchanged = |entries: &Vec<(Vec<u8>, Vec<u8>)>| {
        entries
            .iter()
            .filter(|(key, _)| key != &metadata_key && !key.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(unchanged(&repaired), unchanged(&before));
    drop(catalog);
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    assert_eq!(
        snapshot(&store, b""),
        repaired,
        "second load must not write"
    );
    let ExecutionResult::Query(query) = run_sql_in_txn(
        store.clone(),
        catalog.clone(),
        TxnMode::ReadOnly,
        "SELECT id, value FROM items ORDER BY id",
    ) else {
        panic!("expected rows");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
        ]
    );
    let mut csv = tempfile::NamedTempFile::new().unwrap();
    csv.write_all(b"id,value\n3,10\n").unwrap();
    csv.flush().unwrap();
    let result = attempt_sql(
        store.clone(),
        catalog,
        &format!(
            "COPY items FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
            csv.path().to_str().unwrap().replace('\'', "''")
        ),
    );
    assert!(
        matches!(&result,
        Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. }))
        if index_name == "uq_value" && columns == &["value"]),
        "{result:?}"
    );
    // attempt_sql rolls back on error; this assertion is not a pending-write oracle.
    assert_eq!(snapshot(&store, b""), repaired);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue575_585_columnar_unique_stale_position_duplicate_preserves_all_bytes() {
    let (store, index_id, _) = columnar_stale_unique_fixture(true);
    let prefix = KeyEncoder::index_prefix(index_id);
    assert_eq!(snapshot(&store, &prefix).len(), 1);
    let before = snapshot(&store, b"");
    match PersistentCatalog::load(store.clone()) {
        Err(alopex_sql::catalog::persistent::CatalogError::IndexRecovery(message)) => {
            assert!(
                message.contains("UNIQUE constraint violated on index: uq_value"),
                "{message}"
            );
            assert!(
                message.contains("table 'items' index 'uq_value'"),
                "{message}"
            );
        }
        Err(error) => panic!("wrong recovery owner: {error}"),
        Ok(_) => panic!("duplicate canonical values must reject recovery"),
    }
    assert_eq!(
        snapshot(&store, b""),
        before,
        "failed recovery must restore every byte, including the deleted index entry"
    );
    assert_eq!(snapshot(&store, &prefix).len(), 1);
}
