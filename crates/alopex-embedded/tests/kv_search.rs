use alopex_embedded::{Database, KeyPattern, KeySearchRequest, TxnMode};

#[test]
fn embedded_search_preserves_binary_keys() {
    let db = Database::open_in_memory().unwrap();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.put(&[0xff, b'/', b'k'], b"value").unwrap();
    let page = txn
        .search_keys(&KeySearchRequest::new(
            KeyPattern::regex(r"(?-u:\xFF)/k$"),
            10,
            10,
        ))
        .unwrap();
    assert_eq!(page.entries[0].key, vec![0xff, b'/', b'k']);
}
