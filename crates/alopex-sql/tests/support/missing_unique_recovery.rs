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
