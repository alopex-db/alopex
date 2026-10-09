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
    /// Maximum inspected or returned key/value bytes, including a returned cursor.
    pub max_bytes: usize,
    /// Include engine namespaces and colliding user keys (default false).
    pub include_internal: bool,
}

impl Default for KeyScanOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            cursor: None,
            scan_budget: 10_000,
            max_bytes: DEFAULT_KEY_SEARCH_RESPONSE_BYTES,
            include_internal: false,
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
        });
    }
    let mut scan = match end {
        Some(end) => txn.scan_range(&effective_start, end)?,
        None => txn.scan_from(&effective_start)?,
    };
    collect_page(scan.as_mut(), options)
}

fn collect_page(scan: &mut dyn OwnedKVScan, options: &KeyScanOptions) -> Result<KeySearchPage> {
    let result = (|| {
        let mut page = KeySearchPage {
            entries: Vec::with_capacity(options.limit),
            next_cursor: None,
            scanned: 0,
        };
        let mut inspected_bytes = 0usize;
        let mut response_bytes = 0usize;
        loop {
            if page.scanned == options.scan_budget {
                return Err(Error::SearchBudgetExceeded {
                    limit: options.scan_budget,
                });
            }
            let Some((key, value)) = scan.next_search_entry()? else {
                break;
            };
            page.scanned += 1;
            inspected_bytes = inspected_bytes
                .saturating_add(key.len())
                .saturating_add(value.as_ref().map_or(0, Vec::len));
            if inspected_bytes > options.max_bytes {
                return Err(Error::SearchResponseTooLarge {
                    limit: options.max_bytes,
                    requested: inspected_bytes,
                });
            }
            let Some(value) = value.filter(|_| options.include_internal || !is_internal_key(&key))
            else {
                continue;
            };
            let full = page.entries.len() + 1 == options.limit;
            let requested = response_bytes
                .saturating_add(key.len())
                .saturating_add(value.len())
                .saturating_add(if full { key.len() } else { 0 });
            if requested > options.max_bytes {
                return Err(Error::SearchResponseTooLarge {
                    limit: options.max_bytes,
                    requested,
                });
            }
            response_bytes = requested;
            if full {
                page.next_cursor = Some(key.clone());
            }
            page.entries.push(KeySearchEntry { key, value });
            if full {
                break;
            }
        }
        Ok(page)
    })();
    match (result, scan.close()) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(page), Ok(())) => Ok(page),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
                    ..Default::default()
                }
            ),
            Err(Error::SearchBudgetExceeded { limit: 2 })
        ));
        assert_eq!(scan.inspected, 2);
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
            Err(Error::SearchResponseTooLarge { .. })
        ));
        assert_eq!(scan.inspected, 1);
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
                    max_bytes: 2,
                    ..Default::default()
                }
            ),
            Err(Error::SearchResponseTooLarge { requested: 3, .. })
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
                ),
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
