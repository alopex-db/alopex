use alopex_embedded::{Database, Metric, TxnMode};

#[test]
fn upsert_vector_rejects_empty_input() {
    let db = Database::new();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    let err = txn.upsert_vector(b"k", b"m", &[], Metric::Cosine);
    assert!(format!("{err:?}").contains("vector cannot be empty"));
}

#[test]
fn search_similar_dimension_mismatch_surfaces() {
    let db = Database::new();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_vector(b"k", b"m", &[1.0], Metric::Cosine).unwrap();
    txn.commit().unwrap();

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let err = ro.search_similar(&[1.0, 2.0], Metric::Cosine, 1, None);
    assert!(format!("{err:?}").contains("DimensionMismatch"));
}

#[test]
fn search_similar_topk_determinism_on_tie() {
    let db = Database::new();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_vector(b"k1", b"m1", &[1.0, 0.0], Metric::InnerProduct)
        .unwrap();
    txn.upsert_vector(b"k2", b"m2", &[1.0, 0.0], Metric::InnerProduct)
        .unwrap();
    txn.commit().unwrap();

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let res = ro
        .search_similar(&[1.0, 0.0], Metric::InnerProduct, 2, None)
        .unwrap();
    assert_eq!(res.len(), 2);
    assert!(res[0].key <= res[1].key);
}
