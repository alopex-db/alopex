//! Bounded pages over an owned transaction's physical keyspace.

use alopex_core::kv::search::{
    DEFAULT_KEY_SEARCH_RESPONSE_BYTES, MAX_KEY_SEARCH_LIMIT, MAX_KEY_SEARCH_RESPONSE_BYTES,
    MAX_KEY_SEARCH_SCAN_BUDGET,
};
use alopex_core::kv::{KeySearchEntry, KeySearchPage, OwnedKVScan, OwnedKVTransaction};
use alopex_core::{Error, Key, Result};

/// Bounds and visibility for one prefix or range scan page.
#[derive(Clone, Debug)]
pub struct KeyScanOptions {
    /// Maximum returned entries (default 100).
    pub limit: usize,
    /// Exclusive raw-key cursor from the previous page.
    pub cursor: Option<Key>,
    /// Maximum inspected physical candidates, including deleted and hidden keys.
    pub scan_budget: usize,
    /// Maximum returned key/value bytes. Hidden entries and cursor metadata are excluded.
    pub max_bytes: usize,
    /// Include engine namespaces and colliding user keys (default false).
    pub include_internal: bool,
    /// Return an error when a work budget prevents filling the page.
    pub strict_budget: bool,
}

impl Default for KeyScanOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            cursor: None,
            scan_budget: 10_000,
            max_bytes: DEFAULT_KEY_SEARCH_RESPONSE_BYTES,
            include_internal: false,
            strict_budget: false,
        }
    }
}

pub(crate) fn is_internal_key(key: &[u8]) -> bool {
    matches!(key.first(), Some(0x01 | 0x02 | 0x04 | 0x11..=0x15))
        || key == crate::VECTOR_INDEX_KEY
        || key == b"__alopex/schema-manifest-version/v1"
        || [
            alopex_sql::catalog::persistent::CATALOG_PREFIX,
            b"__sequence__/",
            b"hnsw:meta:",
            b"hnsw:node:",
            b"hnsw:key:",
            b"vector_segment:",
            b"__alopex_columnar_index__:",
            b"__alopex_txn_meta__:",
            b"__alopex_txn_write__:",
            b"\x00alopex/range-change/",
            b"\x00alopex/range-transfer/progress/",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
}

pub(crate) fn prefix_page(
    txn: &mut dyn OwnedKVTransaction,
    prefix: &[u8],
    options: &KeyScanOptions,
) -> Result<KeySearchPage> {
    let mut end = prefix.to_vec();
    while end.last() == Some(&u8::MAX) {
        end.pop();
    }
    let end = if let Some(last) = end.last_mut() {
        *last += 1;
        Some(end)
    } else {
        None
    };
    range_page(txn, prefix, end.as_deref(), options)
}

pub(crate) fn range_page(
    txn: &mut dyn OwnedKVTransaction,
    start: &[u8],
    end: Option<&[u8]>,
    options: &KeyScanOptions,
) -> Result<KeySearchPage> {
    validate_options(options)?;
    let mut effective_start = start.to_vec();
    if let Some(cursor) = &options.cursor {
        let mut after = cursor.clone();
        after.push(0);
        effective_start = effective_start.max(after);
    }
    if end.is_some_and(|end| effective_start.as_slice() >= end) {
        return Ok(KeySearchPage {
            entries: Vec::new(),
            next_cursor: None,
            scanned: 0,
            has_more: false,
        });
    }
    let mut scan = match end {
        Some(end) => txn.scan_range(&effective_start, end)?,
        None => txn.scan_from(&effective_start)?,
    };
    collect_page(scan.as_mut(), options)
}

fn validate_options(options: &KeyScanOptions) -> Result<()> {
    for (param, value, maximum) in [
        ("limit", options.limit, MAX_KEY_SEARCH_LIMIT),
        (
            "scan_budget",
            options.scan_budget,
            MAX_KEY_SEARCH_SCAN_BUDGET,
        ),
        (
            "max_bytes",
            options.max_bytes,
            MAX_KEY_SEARCH_RESPONSE_BYTES,
        ),
    ] {
        if !(1..=maximum).contains(&value) {
            return Err(Error::InvalidParameter {
                param: param.into(),
                reason: format!("must be between 1 and {maximum}"),
            });
        }
    }
    Ok(())
}

// Keep the first candidate of the following page without reopening the physical scan.
struct PageScan<'a> {
    scan: &'a mut dyn OwnedKVScan,
    pending: Option<(Key, Option<Vec<u8>>)>,
}

impl PageScan<'_> {
    fn next(&mut self) -> Result<Option<(Key, Option<Vec<u8>>)>> {
        match self.pending.take() {
            Some(entry) => Ok(Some(entry)),
            None => self.scan.next_search_entry(),
        }
    }
}

fn collect_page(scan: &mut dyn OwnedKVScan, options: &KeyScanOptions) -> Result<KeySearchPage> {
    let result = read_page(
        &mut PageScan {
            scan,
            pending: None,
        },
        options,
    );
    match (result, scan.close()) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(page), Ok(())) => Ok(page),
    }
}

fn read_page(scan: &mut PageScan<'_>, options: &KeyScanOptions) -> Result<KeySearchPage> {
    let mut page = KeySearchPage {
        entries: Vec::with_capacity(options.limit),
        next_cursor: None,
        scanned: 0,
        has_more: false,
    };
    let mut response_bytes = 0usize;
    let mut last_processed = None;
    while let Some((key, value)) = scan.next()? {
        let visible = value.is_some() && (options.include_internal || !is_internal_key(&key));
        if page.entries.len() == options.limit && visible {
            page.has_more = true;
            scan.pending = Some((key, value));
            break;
        }
        if page.scanned == options.scan_budget {
            if options.strict_budget && page.entries.len() < options.limit {
                return Err(Error::SearchBudgetExceeded {
                    limit: options.scan_budget,
                });
            }
            page.has_more = true;
            scan.pending = Some((key, value));
            break;
        }
        if visible {
            let value = value.expect("visible entry has a value");
            let entry_bytes = key.len().saturating_add(value.len());
            if response_bytes.saturating_add(entry_bytes) > options.max_bytes {
                if page.entries.is_empty() {
                    return Err(Error::InvalidParameter {
                        param: "max_bytes".into(),
                        reason: format!(
                            "scan entry too large: key={key:?}, limit={}, requested={entry_bytes}",
                            options.max_bytes
                        ),
                    });
                }
                page.has_more = true;
                scan.pending = Some((key, Some(value)));
                break;
            }
            response_bytes += entry_bytes;
            last_processed = Some(key.clone());
            page.entries.push(KeySearchEntry { key, value });
        } else {
            last_processed = Some(key);
        }
        page.scanned += 1;
    }
    if page.has_more {
        page.next_cursor = last_processed;
    }
    Ok(page)
}

/// Collect in one physical pass, keeping one transaction and a total response
/// cap. Per-page work checkpoints do not reopen and repeatedly scan the range.
pub(crate) fn range_all(
    txn: &mut dyn OwnedKVTransaction,
    start: &[u8],
    end: Option<&[u8]>,
    options: &KeyScanOptions,
    limit: Option<usize>,
    max_total_bytes: usize,
) -> Result<Vec<KeySearchEntry>> {
    // Reuse the page validator without opening or retaining a second scan.
    validate_options(options)?;
    if limit == Some(0) || max_total_bytes == 0 {
        return Err(Error::InvalidParameter {
            param: "limit/max_total_bytes".into(),
            reason: "must be positive (or limit=None)".into(),
        });
    }
    let mut effective_start = start.to_vec();
    if let Some(cursor) = &options.cursor {
        let mut after = cursor.clone();
        after.push(0);
        effective_start = effective_start.max(after);
    }
    if end.is_some_and(|end| effective_start.as_slice() >= end) {
        return Ok(Vec::new());
    }
    let mut scan = match end {
        Some(end) => txn.scan_range(&effective_start, end)?,
        None => txn.scan_from(&effective_start)?,
    };
    let result = (|| {
        let mut entries = Vec::new();
        let mut total_bytes = 0usize;
        let mut pages = PageScan {
            scan: scan.as_mut(),
            pending: None,
        };
        let mut page_options = options.clone();
        loop {
            if let Some(limit) = limit {
                page_options.limit = options.limit.min(limit - entries.len());
            }
            let page = read_page(&mut pages, &page_options)?;
            for entry in page.entries {
                total_bytes = total_bytes
                    .saturating_add(entry.key.len())
                    .saturating_add(entry.value.len());
                if total_bytes > max_total_bytes {
                    return Err(Error::SearchResponseTooLarge {
                        limit: max_total_bytes,
                        requested: total_bytes,
                    });
                }
                entries.push(entry);
            }
            if !page.has_more || limit.is_some_and(|limit| entries.len() == limit) {
                break;
            }
        }
        Ok(entries)
    })();
    match (result, scan.close()) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(entries), Ok(())) => Ok(entries),
    }
}

pub(crate) fn prefix_all(
    txn: &mut dyn OwnedKVTransaction,
    prefix: &[u8],
    options: &KeyScanOptions,
    limit: Option<usize>,
    max_total_bytes: usize,
) -> Result<Vec<KeySearchEntry>> {
    let mut end = prefix.to_vec();
    while end.last() == Some(&u8::MAX) {
        end.pop();
    }
    let end = if let Some(last) = end.last_mut() {
        *last += 1;
        Some(end)
    } else {
        None
    };
    range_all(txn, prefix, end.as_deref(), options, limit, max_total_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    #[test]
    fn pending_candidate_is_delivered_once_then_scan_ends() {
        struct Scan {
            entries: std::vec::IntoIter<(Key, Vec<u8>)>,
        }
        impl OwnedKVScan for Scan {
            fn next_entry(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
                Ok(self.entries.next())
            }
            fn close(&mut self) -> Result<()> {
                Ok(())
            }
        }
        let mut raw = Scan {
            entries: vec![(b"b".to_vec(), b"v".to_vec())].into_iter(),
        };
        let mut scan = PageScan {
            scan: &mut raw,
            pending: Some((b"a".to_vec(), Some(b"v".to_vec()))),
        };
        assert_eq!(
            scan.next().unwrap(),
            Some((b"a".to_vec(), Some(b"v".to_vec())))
        );
        assert_eq!(
            scan.next().unwrap(),
            Some((b"b".to_vec(), Some(b"v".to_vec())))
        );
        assert_eq!(scan.next().unwrap(), None);
    }

    #[test]
    fn strict_budget_accepts_a_full_page_before_a_hidden_candidate() {
        let db = Arc::new(crate::Database::new());
        let mut txn = db
            .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
            .unwrap();
        for key in [b"b".as_slice(), b"hnsw:meta:hidden", b"z"] {
            txn.put(key, b"v").unwrap();
        }
        let options = KeyScanOptions {
            limit: 1,
            scan_budget: 1,
            strict_budget: true,
            ..Default::default()
        };
        let page = txn.scan_prefix_page(b"", &options).unwrap();
        assert_eq!(
            page.entries,
            vec![KeySearchEntry {
                key: b"b".to_vec(),
                value: b"v".to_vec()
            }]
        );
        assert_eq!(page.scanned, 1);
        assert!(page.has_more);
        assert_eq!(page.next_cursor, Some(b"b".to_vec()));
    }

    #[test]
    fn complete_scan_rejects_each_zero_limit_even_on_empty_ranges() {
        let db = Arc::new(crate::Database::new());
        let mut txn = db
            .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
            .unwrap();
        let options = KeyScanOptions::default();
        for (limit, bytes) in [(Some(0), 1), (None, 0)] {
            assert!(matches!(
                txn.scan_prefix_all(b"", &options, limit, bytes),
                Err(crate::Error::Core(Error::InvalidParameter { .. }))
            ));
        }
    }

    proptest! {
        #[test]
        fn byte_pages_match_a_small_reference_vector(
            values in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..8), 1..6),
            extra in 0usize..12,
        ) {
            let db = Arc::new(crate::Database::new());
            let mut txn = db.begin_owned_embedded_transaction(crate::TxnMode::ReadWrite).unwrap();
            let expected: Vec<_> = values.into_iter().enumerate().map(|(n, value)| KeySearchEntry { key: vec![b'p', n as u8], value }).collect();
            for entry in &expected { txn.put(&entry.key, &entry.value).unwrap(); }
            let max_bytes = expected.iter().map(|e| e.key.len() + e.value.len()).max().unwrap() + extra;
            let mut options = KeyScanOptions { max_bytes, ..Default::default() };
            let mut actual = Vec::new();
            for _ in 0..expected.len() {
                let page = txn.scan_prefix_page(b"p", &options).unwrap();
                prop_assert!(!page.entries.is_empty());
                prop_assert!(page.entries.iter().map(|e| e.key.len() + e.value.len()).sum::<usize>() <= max_bytes);
                actual.extend(page.entries);
                if !page.has_more { break; }
                prop_assert_ne!(&page.next_cursor, &options.cursor);
                options.cursor = page.next_cursor;
            }
            prop_assert_eq!(&actual, &expected);
            options.cursor = None;
            let total = expected.iter().map(|e| e.key.len() + e.value.len()).sum();
            prop_assert_eq!(txn.scan_prefix_all(b"p", &options, None, total).unwrap(), expected);
        }

        #[test]
        fn full_page_accepts_exact_response_budget(
            suffix in proptest::collection::vec(any::<u8>(), 0..32),
            value in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let db = Arc::new(crate::Database::new());
            let mut txn = db
                .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
                .unwrap();
            let mut key = b"user:".to_vec();
            key.extend(suffix);
            txn.put(&key, &value).unwrap();
            // An exact terminal page has no continuation cursor.
            let exact_bytes = key.len() + value.len();
            let mut options = KeyScanOptions {
                limit: 1,
                max_bytes: exact_bytes,
                ..Default::default()
            };
            let page = txn.scan_prefix_page(b"user:", &options).unwrap();
            prop_assert_eq!(page.entries.len(), 1);
            prop_assert_eq!(&page.entries[0].key, &key);
            prop_assert_eq!(&page.entries[0].value, &value);
            prop_assert_eq!(page.next_cursor, None);
            prop_assert!(!page.has_more);
            prop_assert_eq!(page.scanned, 1);

            options.max_bytes -= 1;
            let error = txn.scan_prefix_page(b"user:", &options).unwrap_err();
            match error {
                crate::Error::Core(Error::InvalidParameter { param, reason }) => {
                    prop_assert_eq!(param, "max_bytes");
                    prop_assert_eq!(reason, format!("scan entry too large: key={key:?}, limit={}, requested={exact_bytes}", exact_bytes - 1));
                }
                other => prop_assert!(false, "unexpected error: {other}"),
            }
        }
    }

    #[test]
    fn complete_scan_resumes_work_and_enforces_total_limit_without_partial_results() {
        let db = Arc::new(crate::Database::new());
        let mut txn = db
            .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
            .unwrap();
        for n in 0..5 {
            txn.put(format!("p:{n:03}").as_bytes(), b"v").unwrap();
        }
        let options = KeyScanOptions {
            limit: 3,
            scan_budget: 2,
            ..Default::default()
        };
        let all = txn.scan_prefix_all(b"p:", &options, None, 2000).unwrap();
        assert_eq!(all.len(), 5);
        assert_eq!(all.first().unwrap().key, b"p:000");
        assert_eq!(all.last().unwrap().key, b"p:004");
        let capped = txn
            .scan_range_all(b"p:", b"p;", &options, Some(3), 2000)
            .unwrap();
        assert_eq!(capped, all[..3]);
        let exact = KeyScanOptions {
            limit: 6,
            scan_budget: 5,
            strict_budget: true,
            ..Default::default()
        };
        assert_eq!(txn.scan_prefix_all(b"p:", &exact, None, 30).unwrap(), all);
        assert!(matches!(
            txn.scan_prefix_all(b"p:", &exact, None, 29),
            Err(crate::Error::Core(Error::SearchResponseTooLarge { .. }))
        ));
        let strict = KeyScanOptions {
            strict_budget: true,
            ..options
        };
        assert!(matches!(
            txn.scan_prefix_all(b"p:", &strict, None, 2000),
            Err(crate::Error::Core(Error::SearchBudgetExceeded { .. }))
        ));
        // An error closes its scan and leaves the original transaction usable.
        assert_eq!(
            txn.scan_prefix_all(b"p:", &exact, Some(1), 6).unwrap(),
            all[..1]
        );
    }

    #[test]
    fn empty_budget_pages_advance_through_hidden_keys() {
        let db = Arc::new(crate::Database::new());
        let mut txn = db
            .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
            .unwrap();
        for n in 0..2 {
            txn.put(format!("__catalog__/{n:05}").as_bytes(), b"v")
                .unwrap();
        }
        txn.put(b"user:visible", b"value").unwrap();
        let options = KeyScanOptions {
            scan_budget: 1,
            max_bytes: 17,
            ..Default::default()
        };
        let first = txn.scan_prefix_page(b"", &options).unwrap();
        assert!(first.entries.is_empty());
        assert!(first.has_more);
        assert_eq!(first.scanned, 1);
        let second = txn
            .scan_prefix_page(
                b"",
                &KeyScanOptions {
                    cursor: first.next_cursor,
                    ..options.clone()
                },
            )
            .unwrap();
        assert!(second.entries.is_empty());
        assert!(second.has_more);
        assert_eq!(second.scanned, 1);
        let last = txn
            .scan_prefix_page(
                b"",
                &KeyScanOptions {
                    cursor: second.next_cursor,
                    ..options.clone()
                },
            )
            .unwrap();
        assert_eq!(last.entries.len(), 1);
        assert_eq!(last.entries[0].key, b"user:visible");
        assert!(!last.has_more);
        assert_eq!(last.scanned, 1);
        assert_eq!(
            txn.scan_prefix_all(b"", &options, None, 100).unwrap(),
            last.entries
        );
    }

    #[test]
    fn visibility_uses_owned_namespaces_without_broad_text_blacklists() {
        for key in [
            b"\x01row".as_slice(),
            b"\x02index",
            b"\x04sequence",
            b"\x11segment",
            b"\x12index",
            b"\x13stats",
            b"\x14group",
            b"\x15object",
            b"__catalog__/tables/t",
            b"__sequence__/s",
            b"hnsw:meta:i",
            b"hnsw:node:i:1",
            b"hnsw:key:i:k",
            b"vector_segment:1",
            b"__alopex_columnar_index__:s:c",
            b"__alopex_txn_meta__:t",
            b"__alopex_txn_write__:t:k",
            b"__alopex_vector_index",
            b"__alopex/schema-manifest-version/v1",
            b"\0alopex/range-change/r",
            b"\0alopex/range-transfer/progress/r",
        ] {
            assert!(is_internal_key(key), "internal key: {key:?}");
        }
        for key in [
            b"".as_slice(),
            b"\xff",
            b"\0user",
            b"\x03user",
            b"\x10reserved",
            b"vector:user",
            b"columnar:user",
            b"hnsw:user",
            b"__alopex_user",
            b"__alopex_vector_index_extra",
            b"__alopex/schema-manifest-version/v1extra",
        ] {
            assert!(!is_internal_key(key), "user key: {key:?}");
        }
    }

    #[test]
    fn physical_budget_and_byte_errors_close_the_scan() {
        struct Scan {
            entries: std::vec::IntoIter<(Key, Option<Vec<u8>>)>,
            inspected: usize,
            closed: bool,
        }
        impl OwnedKVScan for Scan {
            fn next_entry(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
                panic!("bounded pages must count physical candidates")
            }
            fn next_search_entry(&mut self) -> Result<Option<(Key, Option<Vec<u8>>)>> {
                self.inspected += 1;
                Ok(self.entries.next())
            }
            fn close(&mut self) -> Result<()> {
                self.closed = true;
                Ok(())
            }
        }
        let entries = vec![
            (b"__catalog__/hidden".to_vec(), Some(b"v".to_vec())),
            (b"deleted".to_vec(), None),
            (b"visible".to_vec(), Some(b"v".to_vec())),
        ];
        let mut scan = Scan {
            entries: entries.clone().into_iter(),
            inspected: 0,
            closed: false,
        };
        assert!(matches!(
            collect_page(
                &mut scan,
                &KeyScanOptions {
                    scan_budget: 2,
                    strict_budget: true,
                    ..Default::default()
                }
            ),
            Err(Error::SearchBudgetExceeded { limit: 2 })
        ));
        assert_eq!(scan.inspected, 3);
        assert!(scan.closed);

        let mut scan = Scan {
            entries: entries.into_iter(),
            inspected: 0,
            closed: false,
        };
        assert!(matches!(
            collect_page(
                &mut scan,
                &KeyScanOptions {
                    max_bytes: 1,
                    ..Default::default()
                }
            ),
            Err(Error::InvalidParameter { .. })
        ));
        assert_eq!(scan.inspected, 3);
        assert!(scan.closed);

        let mut scan = Scan {
            entries: vec![(b"k".to_vec(), Some(b"v".to_vec()))].into_iter(),
            inspected: 0,
            closed: false,
        };
        assert!(matches!(
            collect_page(
                &mut scan,
                &KeyScanOptions {
                    limit: 1,
                    max_bytes: 1,
                    ..Default::default()
                }
            ),
            Err(Error::InvalidParameter { param, reason }) if param == "max_bytes" && reason.contains("requested=2")
        ));
        assert!(scan.closed);
    }

    #[test]
    fn invalid_limits_and_binary_prefix_boundaries_are_explicit() {
        let db = Arc::new(crate::Database::new());
        let mut txn = db
            .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
            .unwrap();
        for options in [
            KeyScanOptions {
                limit: 0,
                ..Default::default()
            },
            KeyScanOptions {
                limit: MAX_KEY_SEARCH_LIMIT + 1,
                ..Default::default()
            },
            KeyScanOptions {
                scan_budget: 0,
                ..Default::default()
            },
            KeyScanOptions {
                scan_budget: MAX_KEY_SEARCH_SCAN_BUDGET + 1,
                ..Default::default()
            },
            KeyScanOptions {
                max_bytes: 0,
                ..Default::default()
            },
            KeyScanOptions {
                max_bytes: MAX_KEY_SEARCH_RESPONSE_BYTES + 1,
                ..Default::default()
            },
        ] {
            assert!(matches!(
                txn.scan_prefix_page(b"", &options),
                Err(crate::Error::Core(Error::InvalidParameter { .. }))
            ));
        }
        for key in [b"\xff".as_slice(), b"\xff\0", b"\xff\xff", b"literal*?\\"] {
            txn.put(key, b"v").unwrap();
        }
        let first = txn
            .scan_prefix_page(
                b"\xff",
                &KeyScanOptions {
                    limit: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(first.entries[0].key, b"\xff");
        let next = txn
            .scan_prefix_page(
                b"\xff",
                &KeyScanOptions {
                    cursor: first.next_cursor,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            next.entries
                .iter()
                .map(|entry| entry.key.as_slice())
                .collect::<Vec<_>>(),
            [b"\xff\0", b"\xff\xff"]
        );
        assert_eq!(
            txn.scan_prefix_page(b"literal*?\\", &KeyScanOptions::default())
                .unwrap()
                .entries
                .len(),
            1
        );
        assert!(txn
            .scan_range_page(b"z", b"a", &KeyScanOptions::default())
            .unwrap()
            .entries
            .is_empty());
        txn.rollback().unwrap();
    }

    #[test]
    fn pages_preserve_pending_writes_and_exclusive_cursors() {
        let db = Arc::new(crate::Database::new());
        let mut txn = db
            .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
            .unwrap();
        for key in [b"p:0", b"p:1", b"p:2", b"p:3"] {
            txn.put(key, b"v").unwrap();
        }
        txn.delete(b"p:1").unwrap();
        assert!(matches!(
            txn.scan_prefix_page(
                b"p:",
                &KeyScanOptions {
                    limit: 2,
                    scan_budget: 2,
                    strict_budget: true,
                    ..Default::default()
                }
            ),
            Err(crate::Error::Core(Error::SearchBudgetExceeded { limit: 2 }))
        ));
        assert!(matches!(
            txn.search_keys_visible(
                &alopex_core::kv::KeySearchRequest::new(
                    alopex_core::kv::KeyPattern::glob(b"p:*"),
                    2,
                    2,
                )
                .with_strict_budget(true),
                false,
            ),
            Err(crate::Error::Core(Error::SearchBudgetExceeded { limit: 2 }))
        ));
        let mut options = KeyScanOptions {
            limit: 2,
            ..Default::default()
        };
        let first = txn.scan_prefix_page(b"p:", &options).unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| entry.key.as_slice())
                .collect::<Vec<_>>(),
            [b"p:0", b"p:2"]
        );
        options.cursor = first.next_cursor;
        let second = txn.scan_prefix_page(b"p:", &options).unwrap();
        assert_eq!(second.entries[0].key, b"p:3");
        assert_eq!(second.next_cursor, None);
        options.cursor = Some(b"a".to_vec());
        let range = txn.scan_range_page(b"p:0", b"p:3", &options).unwrap();
        assert_eq!(range.entries, first.entries);
        assert_eq!(
            txn.scan_prefix_page(b"p:", &options).unwrap().entries,
            first.entries
        );
        options.cursor = Some(b"z".to_vec());
        assert!(txn
            .scan_prefix_page(b"p:", &options)
            .unwrap()
            .entries
            .is_empty());
        assert!(txn
            .scan_range_page(b"p:0", b"p:3", &options)
            .unwrap()
            .entries
            .is_empty());
        txn.rollback().unwrap();
    }

    #[test]
    fn default_visibility_hides_sql_and_preserves_raw_access() {
        let db = Arc::new(crate::Database::new());
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1)")
            .unwrap();
        let mut txn = db
            .begin_owned_embedded_transaction(crate::TxnMode::ReadWrite)
            .unwrap();
        txn.put(b"cart:1", b"v").unwrap();
        txn.put(b"__catalog__/user-collision", b"v").unwrap();
        let visible = txn
            .scan_prefix_page(b"", &KeyScanOptions::default())
            .unwrap();
        assert_eq!(visible.entries.len(), 1);
        assert_eq!(visible.entries[0].key, b"cart:1");
        let raw = txn
            .scan_prefix_page(
                b"",
                &KeyScanOptions {
                    include_internal: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(raw
            .entries
            .iter()
            .any(|entry| entry.key == b"__catalog__/user-collision"));
        assert!(raw.entries.len() > visible.entries.len());
        assert_eq!(
            txn.get(b"__catalog__/user-collision").unwrap(),
            Some(b"v".to_vec())
        );
        txn.rollback().unwrap();
    }
}
