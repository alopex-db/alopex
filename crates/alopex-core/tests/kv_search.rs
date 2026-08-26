use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{
    KVStore, KVTransaction, KeyPattern, KeySearchCancellation, KeySearchRequest,
};
use alopex_core::lsm::LsmKV;
use alopex_core::types::TxnMode;

fn seeded_memory() -> MemoryKV {
    let store = MemoryKV::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    for key in [
        b"/app/config".as_slice(),
        b"app/*/literal",
        b"app//config",
        b"app/config",
        b"app/config/",
        b"app/one/config",
        b"app/two/config",
        b"tenant/7/events",
        b"tenant/x/events",
        &[0xff, b'/', b'k'],
    ] {
        txn.put(key.to_vec(), key.to_vec()).unwrap();
    }
    txn.commit_self().unwrap();
    store
}

#[test]
fn glob_search_is_byte_oriented_ordered_and_paginated() {
    let store = seeded_memory();
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let first = txn
        .search_keys(&KeySearchRequest::new(
            KeyPattern::glob(b"app/*/config"),
            2,
            20,
        ))
        .unwrap();

    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"app//config".as_slice(), b"app/one/config".as_slice()]
    );
    let cursor = first.next_cursor.expect("full page has a cursor");

    let second = txn
        .search_keys(&KeySearchRequest::new(KeyPattern::glob(b"app/*/config"), 2, 20).after(cursor))
        .unwrap();
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| entry.key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"app/two/config".as_slice()]
    );
    assert!(second.next_cursor.is_none());
}

#[test]
fn regex_search_matches_raw_non_utf8_keys_and_reports_invalid_patterns() {
    let store = seeded_memory();
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let page = txn
        .search_keys(&KeySearchRequest::new(
            KeyPattern::regex(r"(?-u:\xFF)/k$"),
            10,
            20,
        ))
        .unwrap();
    assert_eq!(page.entries[0].key, vec![0xff, b'/', b'k']);

    let error = txn
        .search_keys(&KeySearchRequest::new(KeyPattern::regex("["), 10, 20))
        .unwrap_err();
    assert!(error.to_string().contains("invalid parameter pattern"));
}

#[test]
fn literal_wildcards_and_scan_budget_are_explicit() {
    let store = seeded_memory();
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        txn.get(&b"app/*/literal".to_vec()).unwrap(),
        Some(b"app/*/literal".to_vec())
    );

    let error = txn
        .search_keys(&KeySearchRequest::new(KeyPattern::glob(b"*missing"), 10, 2))
        .unwrap_err();
    assert!(error.to_string().contains("search scan budget exceeded"));

    let error = txn
        .search_keys(
            &KeySearchRequest::new(KeyPattern::glob(b"app/config"), 10, 10).with_max_bytes(1),
        )
        .unwrap_err();
    assert!(error.to_string().contains("response size exceeded"));
}

#[test]
fn lsm_uses_the_same_byte_search_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = LsmKV::open(directory.path()).unwrap();
    let mut writer = store.begin(TxnMode::ReadWrite).unwrap();
    for key in [
        b"app//config".as_slice(),
        b"app/one/config",
        b"app/two/config",
    ] {
        writer.put(key.to_vec(), key.to_vec()).unwrap();
    }
    writer.commit_self().unwrap();

    let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
    let page = reader
        .search_keys(&KeySearchRequest::new(
            KeyPattern::glob(b"app/*/config"),
            10,
            10,
        ))
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"app//config".as_slice(),
            b"app/one/config".as_slice(),
            b"app/two/config".as_slice(),
        ]
    );
}

#[test]
fn search_observes_cooperative_cancellation() {
    let store = seeded_memory();
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let cancellation = KeySearchCancellation::default();
    cancellation.cancel();
    let error = txn
        .search_keys_with_cancellation(
            &KeySearchRequest::new(KeyPattern::glob(b"*"), 10, 20),
            &cancellation,
        )
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
}

#[test]
fn literal_prefixes_bound_glob_and_anchored_regex_scans() {
    let store = seeded_memory();
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    for (pattern, expected) in [
        (KeyPattern::glob(b"tenant/*/events"), 2),
        (KeyPattern::regex(r"^tenant/[0-9]+/events$"), 1),
    ] {
        let page = txn
            .search_keys(&KeySearchRequest::new(pattern, 10, 2))
            .unwrap();
        assert_eq!(page.entries.len(), expected);
        assert_eq!(page.entries[0].key, b"tenant/7/events");
        assert_eq!(page.scanned, 2);
    }
}

#[test]
fn slashes_and_escaped_wildcards_are_ordinary_key_bytes() {
    let store = seeded_memory();
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let slash_page = txn
        .search_keys(&KeySearchRequest::new(
            KeyPattern::glob(b"*config*"),
            10,
            20,
        ))
        .unwrap();
    assert_eq!(
        slash_page
            .entries
            .iter()
            .map(|entry| entry.key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"/app/config".as_slice(),
            b"app//config".as_slice(),
            b"app/config".as_slice(),
            b"app/config/".as_slice(),
            b"app/one/config".as_slice(),
            b"app/two/config".as_slice(),
        ]
    );

    let literal = txn
        .search_keys(&KeySearchRequest::new(
            KeyPattern::glob(br"app/\*/literal"),
            10,
            10,
        ))
        .unwrap();
    assert_eq!(literal.entries[0].key, b"app/*/literal");

    let binary = txn
        .search_keys(&KeySearchRequest::new(
            KeyPattern::glob([0xff, b'/', b'?']),
            10,
            10,
        ))
        .unwrap();
    assert_eq!(binary.entries[0].key, vec![0xff, b'/', b'k']);
}
