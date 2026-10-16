use alopex_embedded::{Database, DatabaseOptions, Metric, TxnMode};

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_get_vectors_preserves_order_missing_duplicates_and_empty() {
    let db = Database::new();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_vector(b"a", b"", &[1.0, 0.0], Metric::L2)
        .unwrap();
    txn.upsert_vector(b"b", b"", &[0.0, 1.0], Metric::L2)
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(TxnMode::ReadOnly).unwrap();
    let keys = [
        b"b".to_vec(),
        b"missing".to_vec(),
        b"a".to_vec(),
        b"b".to_vec(),
    ];
    assert_eq!(
        txn.get_vectors(&keys, Metric::L2).unwrap(),
        vec![
            Some(vec![0.0, 1.0]),
            None,
            Some(vec![1.0, 0.0]),
            Some(vec![0.0, 1.0])
        ]
    );
    assert!(txn.get_vectors(&[], Metric::L2).unwrap().is_empty());
    assert!(matches!(
        txn.get_vectors(&keys, Metric::Cosine),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::UnsupportedMetric { .. }
        ))
    ));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn vector_search_in_memory_orders_results() {
    let opts = DatabaseOptions::in_memory().with_memory_limit(64 * 1024);
    let db = Database::open_in_memory_with_options(opts).unwrap();

    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_vector(b"v1", b"m1", &[1.0, 0.0], Metric::Cosine)
        .unwrap();
    txn.upsert_vector(b"v2", b"m2", &[0.0, 1.0], Metric::Cosine)
        .unwrap();
    txn.upsert_vector(b"v3", b"m3", &[1.0, 1.0], Metric::Cosine)
        .unwrap();
    txn.commit().unwrap();

    let mut rtxn = db.begin(TxnMode::ReadOnly).unwrap();
    let res = rtxn
        .search_similar(&[1.0, 0.0], Metric::Cosine, 2, None)
        .unwrap();
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].key, b"v1");
    assert_eq!(res[1].key, b"v3");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn vector_search_honors_memory_limit_option() {
    let opts = DatabaseOptions::in_memory().with_memory_limit(8 * 1024);
    let db = Database::open_in_memory_with_options(opts).unwrap();

    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_vector(b"l2:a", b"ma", &[0.0, 0.0], Metric::L2)
        .unwrap();
    txn.upsert_vector(b"l2:b", b"mb", &[3.0, 4.0], Metric::L2)
        .unwrap();
    txn.commit().unwrap();

    let mut rtxn = db.begin(TxnMode::ReadOnly).unwrap();
    let res = rtxn
        .search_similar(&[0.0, 0.0], Metric::L2, 1, None)
        .unwrap();
    assert_eq!(res[0].key, b"l2:a");
}
