import pytest

from alopex import AlopexError, Database, TxnMode


def test_transaction_scans_and_search_include_uncommitted_writes():
    db = Database.new()
    try:
        txn = db.begin(TxnMode.READ_WRITE)
        txn.put(b"cart:42:closed", b"closed")
        txn.put(b"cart:42:open", b"open")
        txn.put(b"cart:43:open", b"next")
        txn.put(b"cart:99:open", b"other")
        txn.put(b"inventory:42", b"ignored")

        prefix_scan = txn.scan_prefix(b"cart:42:")
        assert iter(prefix_scan) is prefix_scan
        assert list(prefix_scan) == [
            (b"cart:42:closed", b"closed"),
            (b"cart:42:open", b"open"),
        ]
        range_scan = txn.scan_range(b"cart:42:", b"cart:43:")
        assert iter(range_scan) is range_scan
        assert list(range_scan) == [
            (b"cart:42:closed", b"closed"),
            (b"cart:42:open", b"open"),
        ]

        first = txn.search_keys(b"cart:*", limit=2)
        assert first == {
            "entries": [
                (b"cart:42:closed", b"closed"),
                (b"cart:42:open", b"open"),
            ],
            "next_cursor": b"cart:42:open",
            "scanned": 2,
        }
        assert txn.search_keys(b"cart:*", limit=2, cursor=first["next_cursor"]) == {
            "entries": [
                (b"cart:43:open", b"next"),
                (b"cart:99:open", b"other"),
            ],
            "next_cursor": b"cart:99:open",
            "scanned": 2,
        }
        assert txn.search_keys(b"cart:*", limit=2, cursor=b"cart:99:open") == {
            "entries": [],
            "next_cursor": None,
            "scanned": 0,
        }
        assert txn.search_keys(r"^cart:42:", mode="regex", limit=10)["entries"] == [
            (b"cart:42:closed", b"closed"),
            (b"cart:42:open", b"open"),
        ]
    finally:
        db.close()


def test_transaction_key_search_requires_a_supported_mode():
    db = Database.new()
    try:
        txn = db.begin(TxnMode.READ_ONLY)
        with pytest.raises(ValueError, match="mode"):
            txn.search_keys(b"*", mode="literal")
    finally:
        db.close()


def test_scan_iterators_preserve_the_snapshot_after_transaction_completion(db):
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"cart:1", b"before")
    prefix = txn.scan_prefix(b"cart:")
    bounded = txn.scan_range(b"cart:", b"cart;")
    txn.put(b"cart:1", b"after")
    txn.put(b"cart:2", b"new")
    txn.commit()

    assert list(prefix) == [(b"cart:1", b"before")]
    assert list(bounded) == [(b"cart:1", b"before")]


def test_transaction_scans_and_search_merge_pending_updates_and_deletes(db):
    with db.begin(TxnMode.READ_WRITE) as seed:
        seed.put(b"binary:\x00", b"old")
        seed.put(b"binary:\xff", b"removed")
        seed.commit()
    with db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"binary:\x00", b"\x00\xff")
        txn.delete(b"binary:\xff")
        txn.put(b"binary:\x80", b"added")
        expected = [(b"binary:\x00", b"\x00\xff"), (b"binary:\x80", b"added")]
        assert list(txn.scan_prefix(b"binary:")) == expected
        assert list(txn.scan_range(b"binary:", b"binary;")) == expected
        assert txn.search_keys(b"binary:*")["entries"] == expected
        assert list(txn.scan_prefix(b"missing:")) == []
        assert list(txn.scan_range(b"binary:\x80", b"binary:\x80")) == []


@pytest.mark.parametrize("option", ["limit", "scan_budget", "max_bytes"])
def test_key_search_rejects_zero_budgets(db, option):
    with db.begin(TxnMode.READ_ONLY) as txn:
        with pytest.raises(AlopexError, match=option):
            txn.search_keys(b"*", **{option: 0})


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_key_enumeration_rejects_completed_transaction(db, finish):
    txn = db.begin(TxnMode.READ_WRITE)
    getattr(txn, finish)()
    for method, args in (
        (txn.scan_prefix, (b"",)),
        (txn.scan_range, (b"a", b"z")),
        (txn.search_keys, (b"*",)),
    ):
        with pytest.raises(AlopexError, match="closed"):
            method(*args)
