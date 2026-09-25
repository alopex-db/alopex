import pytest

from alopex import Database, TxnMode


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
