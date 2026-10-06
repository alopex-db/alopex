//! Consumes immutable bytes emitted by the exact old producer, not synthesized DTOs.
use super::*;
use alopex_sql::catalog::persistent::CatalogError;
use sha2::{Digest, Sha256};

#[path = "legacy_unique_expected.rs"]
mod expected;

fn query_rows(result: ExecutionResult) -> Vec<Vec<Option<i32>>> {
    match result {
        ExecutionResult::Query(result) => result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| match value {
                        SqlValue::Integer(value) => Some(value),
                        SqlValue::Null => None,
                        other => panic!("unexpected restored SELECT value: {other:?}"),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected restored SELECT rows: {other:?}"),
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_old_producer_catalog_upgrade() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/legacy_unique_v0815.json");
    let raw = std::fs::read(&path)
        .expect("generate and authenticate the exact old-producer fixture first");
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.with_file_name("legacy_unique_v0815.manifest.json"))
            .expect("old-producer provenance is required"),
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
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7);
    for case in cases {
        assert_eq!(
            case["source_commit"],
            "d3917e1fa21d41097742a971e14094c0d6ae397e"
        );
        assert_eq!(case["parser_contract"], "0.25.0");
        let name = case["case"].as_str().unwrap();
        assert_eq!(
            case["observed_rows"],
            serde_json::to_value(expected::rows(name)).unwrap()
        );
        assert_eq!(case["observed_neighbor"], serde_json::json!([[7]]));
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
        if case["duplicate"].as_bool().unwrap() {
            match PersistentCatalog::load(store.clone()) {
                Err(CatalogError::IndexRecovery(message)) => assert!(message.contains("items")),
                Err(error) => panic!("wrong recovery error: {error}"),
                Ok(_) => panic!("old duplicate rows must not silently acquire a UNIQUE constraint"),
            }
            assert_eq!(snapshot(&store, b""), before);
            continue;
        }
        let recovered = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        assert_eq!(
            query_rows(run_sql_in_txn(
                store.clone(),
                recovered.clone(),
                TxnMode::ReadOnly,
                "SELECT id,a,b FROM items ORDER BY id"
            )),
            expected::rows(name)
        );
        assert_eq!(
            query_rows(run_sql_in_txn(
                store.clone(),
                recovered.clone(),
                TxnMode::ReadOnly,
                "SELECT id FROM neighbor ORDER BY id"
            )),
            vec![vec![Some(7)]]
        );
        {
            let guard = recovered.read().unwrap();
            let all_indexes: Vec<_> = ["items", "neighbor"]
                .into_iter()
                .flat_map(|table| guard.get_indexes_for_table(table))
                .collect();
            let ids: std::collections::HashSet<_> =
                all_indexes.iter().map(|index| index.index_id).collect();
            assert_eq!(
                ids.len(),
                all_indexes.len(),
                "index IDs must not collide across tables"
            );
            let indexes = guard.get_indexes_for_table("items");
            let index = indexes
                .iter()
                .find(|index| index.name != "__pk_items")
                .unwrap();
            assert!(index.unique);
            assert!(matches!(
                index.method,
                None | Some(alopex_sql::ast::ddl::IndexMethod::BTree)
            ));
            let composite = name == "composite" || name == "nullable";
            assert_eq!(
                index.columns,
                if composite { vec!["a", "b"] } else { vec!["a"] }
            );
            assert_eq!(
                index.column_indices,
                if composite { vec![1, 2] } else { vec![1] }
            );
            if name == "named" {
                assert_eq!(index.name, "uq_a");
            }
            assert_eq!(
                snapshot(&store, &KeyEncoder::index_prefix(index.index_id)).len(),
                if name == "nullable" { 0 } else { 1 }
            );
            if name != "nullable" {
                let key = if composite {
                    KeyEncoder::composite_index_key(
                        index.index_id,
                        &[SqlValue::Integer(10), SqlValue::Integer(20)],
                        1,
                    )
                    .unwrap()
                } else {
                    KeyEncoder::index_key(index.index_id, &SqlValue::Integer(10), 1).unwrap()
                };
                assert_eq!(
                    snapshot(&store, &KeyEncoder::index_prefix(index.index_id))[0].0,
                    key
                );
            }
        }
        for (key, value) in &before {
            if key != alopex_sql::catalog::persistent::META_KEY {
                assert_eq!(
                    store.begin(TxnMode::ReadOnly).unwrap().get(key).unwrap(),
                    Some(value.clone())
                );
            }
        }
        if case["case"] == "nullable" {
            assert!(
                attempt_sql(
                    store.clone(),
                    recovered.clone(),
                    "INSERT INTO items VALUES (4, NULL, 20)"
                )
                .is_ok()
            );
            assert!(
                attempt_sql(
                    store.clone(),
                    recovered.clone(),
                    "INSERT INTO items VALUES (5, 10, 20)"
                )
                .is_ok()
            );
        }
        assert!(matches!(
            attempt_sql(
                store.clone(),
                recovered.clone(),
                "INSERT INTO items VALUES (6, 10, 20)"
            ),
            Err(ExecutorError::ConstraintViolation(
                ConstraintViolation::Unique { .. }
            ))
        ));
        if case["case"] == "composite" || case["case"] == "nullable" {
            assert!(
                attempt_sql(
                    store.clone(),
                    recovered,
                    "INSERT INTO items VALUES (7, 10, 21)"
                )
                .is_ok()
            );
        } else {
            assert!(
                attempt_sql(
                    store.clone(),
                    recovered,
                    "INSERT INTO items VALUES (7, 11, 20)"
                )
                .is_ok()
            );
        }
        let after = snapshot(&store, b"");
        PersistentCatalog::load(store.clone()).unwrap();
        assert_eq!(snapshot(&store, b""), after);
    }
}
