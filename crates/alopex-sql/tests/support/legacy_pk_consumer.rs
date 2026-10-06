//! Separate old table-level PK fixture; never regenerates or alters old evidence.
use super::*;
use alopex_core::storage::format::bincode_config;
use alopex_sql::catalog::persistent::{CatalogError, META_KEY, PersistedTableMeta, TABLES_PREFIX};
use alopex_sql::planner::PlannerError;
use bincode::Options;
use sha2::{Digest, Sha256};

#[path = "legacy_pk_cases.rs"]
mod cases;

fn query(result: ExecutionResult) -> Vec<Vec<Option<i32>>> {
    match result {
        ExecutionResult::Query(result) => result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| match value {
                        SqlValue::Integer(value) => Some(value),
                        SqlValue::Null => None,
                        other => panic!("unexpected SELECT value: {other:?}"),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected SELECT: {other:?}"),
    }
}

fn assert_null_rejected(
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<PersistentCatalog<MemoryKV>>>,
    sql: &str,
) {
    let before = snapshot(&store, b"");
    assert!(
        matches!(
            attempt_sql(store.clone(), catalog, sql),
            Err(ExecutorError::Planner(
                PlannerError::NullConstraintViolation { .. }
            )) | Err(ExecutorError::ConstraintViolation(
                ConstraintViolation::NotNull { .. }
            ))
        ),
        "PK NULL must fail through its constraint owner: {sql}"
    );
    assert_eq!(snapshot(&store, b""), before);
}

fn check_old_producer_case(selected: &str) {
    assert!(cases::CASES.contains(&selected));
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/legacy_pk_v0815.json");
    let raw =
        std::fs::read(&path).expect("authenticate the separate old PK producer fixture first");
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.with_file_name("legacy_pk_v0815.manifest.json"))
            .expect("PK provenance required"),
    )
    .unwrap();
    assert_eq!(
        manifest["fixture_sha256"],
        hex::encode(Sha256::digest(&raw))
    );
    assert_eq!(
        manifest["identity"]["source_commit"],
        "d3917e1fa21d41097742a971e14094c0d6ae397e"
    );
    assert_eq!(manifest["identity"]["parser_contract"], "0.25.0");
    let fixture: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let fixtures = fixture["cases"].as_array().unwrap();
    assert_eq!(fixtures.len(), cases::CASES.len());
    for (case, expected_name) in fixtures.iter().zip(cases::CASES) {
        assert_eq!(case["case"], expected_name);
        assert_eq!(case["source_commit"], manifest["identity"]["source_commit"]);
        assert_eq!(case["parser_contract"], "0.25.0");
        assert_eq!(case["sql"], cases::setup(expected_name));
        assert_eq!(
            case["attempt_sql"],
            serde_json::to_value(cases::attempt(expected_name)).unwrap()
        );
        let outcome = case["outcome"].as_str().unwrap();
        if cases::attempt(expected_name).is_none() {
            assert_eq!(outcome, "valid");
        } else {
            assert!(matches!(outcome, "accepted" | "rejected"));
        }
        assert_eq!(case["error"].is_string(), outcome == "rejected");
        let rows = cases::rows(expected_name, outcome);
        assert_eq!(case["observed_rows"], serde_json::to_value(&rows).unwrap());
        assert_eq!(
            case["before_rows"],
            serde_json::to_value(cases::rows(expected_name, "valid")).unwrap()
        );
        assert_eq!(case["observed_neighbor"], serde_json::json!([[7]]));
    }
    // Validate the entire immutable fixture before executing only this test's case.
    let case = fixtures
        .iter()
        .find(|case| case["case"] == selected)
        .unwrap();
    let expected_name = selected;
    let outcome = case["outcome"].as_str().unwrap();
    let rows = cases::rows(expected_name, outcome);
    let store = Arc::new(MemoryKV::new());
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut previous = None;
    for entry in case["entries"].as_array().unwrap() {
        let key = hex::decode(entry[0].as_str().unwrap()).unwrap();
        if let Some(previous) = &previous {
            assert!(previous < &key);
        }
        previous = Some(key.clone());
        txn.put(key, hex::decode(entry[1].as_str().unwrap()).unwrap())
            .unwrap();
    }
    txn.commit_self().unwrap();
    let before = snapshot(&store, b"");
    let (table_key, persisted) = snapshot(&store, TABLES_PREFIX)
        .into_iter()
        .find_map(|(key, value)| {
            let table: PersistedTableMeta = bincode_config().deserialize(&value).unwrap();
            (table.name == "items").then_some((key, table))
        })
        .unwrap();
    assert_eq!(
        case["observed_metadata"]["primary_key"],
        serde_json::to_value(&persisted.primary_key).unwrap()
    );
    let pk_column = persisted
        .columns
        .iter()
        .find(|column| column.name == "id")
        .unwrap();
    assert_eq!(
        case["observed_metadata"]["columns"][0]["not_null"],
        pk_column.not_null
    );
    let row_bytes = snapshot(&store, &KeyEncoder::table_prefix(persisted.table_id));
    let owned_indexes: Vec<_> = snapshot(&store, INDEXES_PREFIX)
        .into_iter()
        .filter_map(|(key, value)| {
            let index: PersistedIndexMeta = bincode::deserialize(&value).unwrap();
            (index.table == "items").then_some((key, KeyEncoder::index_prefix(index.index_id)))
        })
        .collect();
    if outcome == "accepted" {
        match PersistentCatalog::load(store.clone()) {
            Err(CatalogError::IndexRecovery(message)) => {
                let message = message.to_lowercase();
                assert!(message.contains("items"));
                assert!(
                    message.contains("primary")
                        || message.contains("null")
                        || message.contains("duplicate")
                        || (message.contains("__pk_items") && message.contains("unique")),
                    "unexpected PK recovery diagnostic: {message}"
                );
            }
            Err(error) => panic!("wrong invalid-PK recovery error: {error}"),
            Ok(_) => panic!("old invalid PK rows must fail closed: {expected_name}"),
        }
        assert_eq!(snapshot(&store, b""), before);
        return;
    }
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    {
        let guard = catalog.read().unwrap();
        let table = guard.get_table("items").unwrap();
        assert_eq!(table.primary_key, Some(vec!["id".into()]));
        assert!(
            table
                .columns
                .iter()
                .find(|column| column.name == "id")
                .unwrap()
                .not_null
        );
        let indexes = guard.get_indexes_for_table("items");
        let pk = indexes
            .iter()
            .find(|index| index.name == "__pk_items")
            .expect("recovered PK must own a real unique index");
        assert!(pk.unique);
        assert_eq!(pk.columns, vec!["id"]);
        assert_eq!(
            snapshot(&store, &KeyEncoder::index_prefix(pk.index_id)).len(),
            2
        );
    }
    assert_eq!(
        snapshot(&store, &KeyEncoder::table_prefix(persisted.table_id)),
        row_bytes
    );
    assert_eq!(
        query(run_sql_in_txn(
            store.clone(),
            catalog.clone(),
            TxnMode::ReadOnly,
            "SELECT id,value FROM items ORDER BY value"
        )),
        rows
    );
    assert_eq!(
        query(run_sql_in_txn(
            store.clone(),
            catalog.clone(),
            TxnMode::ReadOnly,
            "SELECT id FROM neighbor ORDER BY id"
        )),
        vec![vec![Some(7)]]
    );
    // Only the repaired table, derived index records/data and META may change.
    for (key, value) in &before {
        if key != &table_key
            && key != META_KEY
            && !owned_indexes
                .iter()
                .any(|(metadata, prefix)| key == metadata || key.starts_with(prefix))
        {
            assert_eq!(
                store.begin(TxnMode::ReadOnly).unwrap().get(key).unwrap(),
                Some(value.clone())
            );
        }
    }
    let repaired = snapshot(&store, b"");
    let reopened = PersistentCatalog::load(store.clone()).unwrap();
    assert!(reopened.get_table("items").unwrap().columns[0].not_null);
    assert_eq!(snapshot(&store, b""), repaired);
    for sql in [
        "INSERT INTO items VALUES (NULL,30)",
        "UPDATE items SET id=NULL WHERE id=1",
        "MERGE INTO items USING neighbor ON items.id=1 WHEN MATCHED THEN UPDATE SET id=NULL",
        "MERGE INTO items USING neighbor ON items.id=neighbor.id WHEN NOT MATCHED THEN INSERT (id,value) VALUES (NULL,70)",
    ] {
        assert_null_rejected(store.clone(), catalog.clone(), sql);
    }
    for sql in [
        "INSERT INTO items VALUES (1,30)",
        "UPDATE items SET id=1 WHERE id=2",
        "MERGE INTO items USING neighbor ON items.id=2 WHEN MATCHED THEN UPDATE SET id=1",
    ] {
        let before = snapshot(&store, b"");
        assert!(
            matches!(
                attempt_sql(store.clone(), catalog.clone(), sql),
                Err(ExecutorError::ConstraintViolation(
                    ConstraintViolation::PrimaryKey { .. } | ConstraintViolation::Unique { .. }
                ))
            ),
            "restored PK must reject duplicates: {sql}"
        );
        assert_eq!(snapshot(&store, b""), before);
    }
    for sql in [
        "INSERT INTO items VALUES (3,30)",
        "UPDATE items SET value=11 WHERE id=1",
        "MERGE INTO items USING neighbor ON items.id=1 WHEN MATCHED THEN UPDATE SET value=12",
        "MERGE INTO items USING neighbor ON items.id=neighbor.id WHEN NOT MATCHED THEN INSERT (id,value) VALUES (neighbor.id,70)",
    ] {
        attempt_sql(store.clone(), catalog.clone(), sql).unwrap();
    }
    assert_eq!(
        query(run_sql_in_txn(
            store.clone(),
            catalog,
            TxnMode::ReadOnly,
            "SELECT id,value FROM items ORDER BY value"
        )),
        vec![
            vec![Some(1), Some(12)],
            vec![Some(2), Some(20)],
            vec![Some(3), Some(30)],
            vec![Some(7), Some(70)]
        ]
    );
    let after = snapshot(&store, b"");
    PersistentCatalog::load(store.clone()).unwrap();
    assert_eq!(snapshot(&store, b""), after);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_legacy_pk_first_valid() {
    check_old_producer_case("pk_first_valid");
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_legacy_pk_later_valid() {
    check_old_producer_case("pk_later_valid");
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_legacy_pk_first_null_attempt() {
    check_old_producer_case("pk_first_null_attempt");
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_legacy_pk_later_null_attempt() {
    check_old_producer_case("pk_later_null_attempt");
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_legacy_pk_first_duplicate_attempt() {
    check_old_producer_case("pk_first_duplicate_attempt");
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_legacy_pk_later_duplicate_attempt() {
    check_old_producer_case("pk_later_duplicate_attempt");
}
