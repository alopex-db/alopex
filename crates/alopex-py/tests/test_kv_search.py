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
        assert first["entries"] == [(b"cart:42:closed", b"closed"), (b"cart:42:open", b"open")]
        assert first["has_more"] and first["scanned"] == 2
        assert first["next_cursor"].startswith(b"alopex-scan-v2:")
        second = txn.search_keys(b"cart:*", limit=2, cursor=first["next_cursor"])
        assert second == {
            "entries": [(b"cart:43:open", b"next"), (b"cart:99:open", b"other")],
            "next_cursor": None, "scanned": 2, "has_more": False,
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


def test_transaction_enumeration_reflects_uncommitted_put_and_delete():
    db = Database.new()
    try:
        seed = db.begin(TxnMode.READ_WRITE)
        seed.put(b"cart:42:removed", b"old")
        seed.put(b"cart:42:retained", b"keep")
        seed.commit()

        txn = db.begin(TxnMode.READ_WRITE)
        txn.delete(b"cart:42:removed")
        txn.put(b"cart:42:new", b"new")
        expected = [(b"cart:42:new", b"new"), (b"cart:42:retained", b"keep")]

        assert list(txn.scan_prefix(b"cart:42:")) == expected
        assert list(txn.scan_range(b"cart:42:", b"cart:43:")) == expected
        assert txn.search_keys(b"cart:42:*")["entries"] == expected
        assert txn.search_keys(r"^cart:42:", mode="regex")["entries"] == expected
        txn.rollback()
    finally:
        db.close()


@pytest.mark.parametrize("thread_mode", ["single", "multi"])
def test_complete_scans_pages_and_exact_budgets(thread_mode):
    db = Database.new(thread_mode=thread_mode)
    try:
        with db.begin(TxnMode.READ_WRITE) as seed:
            for n in range(5):
                seed.put(f"p:{n:03}".encode(), b"v")
            seed.commit()
        with db.begin(TxnMode.READ_WRITE) as txn:
            txn.delete(b"p:000")
            txn.put(b"p:005", b"new")
            expected = [(f"p:{n:03}".encode(), b"v") for n in range(1, 5)] + [(b"p:005", b"new")]
            for method, args in [(txn.scan_prefix, (b"p:",)), (txn.scan_range, (b"p:", b"p;"))]:
                assert list(method(*args)) == expected
                assert list(method(*args, limit=None, page_size=3, scan_budget=2)) == expected
                assert list(method(*args, limit=3)) == expected[:3]
                assert list(method(*args, scan_budget=6, strict_budget=True)) == expected
                with pytest.raises(AlopexError, match="budget"):
                    list(method(*args, page_size=100, scan_budget=1, strict_budget=True))
                with pytest.raises(AlopexError, match="response size"):
                    method(*args, max_total_bytes=1)
                with pytest.raises(ValueError, match="cursor"):
                    method(*args, cursor=b"p:001")
            for method, args in [(txn.scan_prefix_page, (b"p:",)), (txn.scan_range_page, (b"p:", b"p;"))]:
                cursor = None
                found = []
                sizes = []
                while True:
                    page = method(*args, limit=2, cursor=cursor)
                    sizes.append(len(page["entries"]))
                    found.extend(page["entries"])
                    assert page["scanned"] <= 10_000
                    if not page["has_more"]:
                        assert page["next_cursor"] is None
                        break
                    cursor = page["next_cursor"]
                    assert cursor.startswith(b"alopex-scan-v2:")
                assert found == expected
                assert sizes == [2, 2, 1]
                # The deleted key still consumes one physical candidate.
                with pytest.raises(AlopexError, match="budget"):
                    method(*args, limit=6, scan_budget=5, strict_budget=True)
                exact = method(*args, limit=6, scan_budget=6, strict_budget=True)
                assert exact["entries"] == expected
                assert exact["has_more"] is False
                assert exact["scanned"] == 6
            frozen = txn.scan_prefix(b"p:")
            txn.put(b"p:006", b"later")
            txn.commit()
            assert list(frozen) == expected
    finally:
        db.close()
