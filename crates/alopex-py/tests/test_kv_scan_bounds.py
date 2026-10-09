import pytest

from alopex import AlopexError, Database, TxnMode


@pytest.fixture(params=["memory", "persistent"])
def scan_db(request, tmp_path):
    db = Database.new() if request.param == "memory" else Database.open(str(tmp_path / "scan.alopex"))
    yield db
    db.close()


def test_issue583_default_enumeration_hides_actual_sql_and_hnsw_keys(scan_db):
    scan_db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, v VECTOR(2, L2))")
    scan_db.execute_sql("INSERT INTO items VALUES (1, [0.0, 0.0])")
    scan_db.execute_sql("CREATE INDEX item_vectors ON items (v) USING HNSW")
    with scan_db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"user:\x00\xff", b"value")
        expected = [(b"user:\x00\xff", b"value")]
        assert list(txn.scan_prefix(b"")) == expected
        assert list(txn.scan_range(b"", b"\xff")) == expected
        assert txn.search_keys(b"*")["entries"] == expected
        assert len(list(txn.scan_prefix(b"", include_internal=True))) > 1
        assert scan_db.execute_sql("SELECT id FROM items") == [{"id": 1}]


def test_issue583_scan_default_limit_and_exclusive_cursor(scan_db):
    with scan_db.begin(TxnMode.READ_WRITE) as txn:
        expected = [(f"user:{i:03}".encode(), b"v") for i in range(103)]
        for key, value in expected:
            txn.put(key, value)
        for scan, args in [(txn.scan_prefix, (b"user:",)), (txn.scan_range, (b"user:", b"user;"))]:
            first = list(scan(*args))
            assert first == expected[:100]
            assert list(scan(*args, cursor=first[-1][0])) == expected[100:]
            assert list(scan(*args, limit=2, cursor=b"user:100")) == expected[101:]
            assert list(scan(*args, cursor=b"z")) == []
            assert list(scan(*args, limit=1, cursor=b"a")) == expected[:1]


@pytest.mark.parametrize("method,args", [("scan_prefix", (b"user:",)), ("scan_range", (b"user:", b"user;"))])
def test_issue583_scan_budgets_fail_without_poisoning_transaction(scan_db, method, args):
    with scan_db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"user:1", b"payload")
        txn.put(b"user:2", b"payload")
        scan = getattr(txn, method)
        for option in ("limit", "scan_budget", "max_bytes"):
            with pytest.raises(AlopexError, match=option):
                scan(*args, **{option: 0})
        with pytest.raises(AlopexError):
            scan(*args, scan_budget=1)
        with pytest.raises(AlopexError):
            scan(*args, max_bytes=1)
        assert list(scan(*args, limit=1)) == [(b"user:1", b"payload")]
        txn.put(b"user:3", b"after-error")
        txn.commit()


def test_issue583_reserved_user_keys_have_explicit_raw_access(scan_db):
    with scan_db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"__catalog__/user-defined", b"raw")
        txn.put(b"__alopex_vector_index_user", b"ordinary")
        txn.put(b"vector:ordinary", b"ordinary")
        expected = [(b"__alopex_vector_index_user", b"ordinary"), (b"vector:ordinary", b"ordinary")]
        assert list(txn.scan_prefix(b"")) == expected
        assert txn.search_keys(b"*")["entries"] == expected
        assert txn.get(b"__catalog__/user-defined") == b"raw"
        raw = dict(txn.scan_prefix(b"__catalog__/", include_internal=True))
        assert raw[b"__catalog__/user-defined"] == b"raw"
        assert dict(txn.search_keys(b"__catalog__/*", include_internal=True)["entries"])[b"__catalog__/user-defined"] == b"raw"


def test_issue583_search_filters_before_limit_and_cursor(scan_db):
    with scan_db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"__catalog__/hidden", b"raw")
        txn.put(b"user:1", b"one")
        txn.put(b"user:2", b"two")
        page = txn.search_keys(b"*", limit=1)
        assert page["entries"] == [(b"user:1", b"one")]
        assert page["next_cursor"] == b"user:1"
        assert page["scanned"] > 1
        assert txn.search_keys(b"*", limit=1, cursor=page["next_cursor"])["entries"] == [(b"user:2", b"two")]
        with pytest.raises(AlopexError):
            txn.search_keys(b"*", scan_budget=1)


def test_issue583_search_cursor_before_prefix_is_clamped(scan_db):
    with scan_db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"b", b"unrelated")
        txn.put(b"user:1", b"value")
        assert txn.search_keys(b"user:*", cursor=b"a")["entries"] == [(b"user:1", b"value")]


def test_issue583_post_snapshot_candidates_consume_scan_budget(scan_db):
    db = scan_db
    reader = db.begin(TxnMode.READ_ONLY)
    try:
        with db.begin(TxnMode.READ_WRITE) as writer:
            for i in range(3):
                writer.put(f"future:{i}".encode(), b"new")
            writer.commit()
        for method, args in [(reader.scan_prefix, (b"future:",)), (reader.search_keys, (b"future:*",))]:
            with pytest.raises(AlopexError):
                method(*args, scan_budget=1)
            result = method(*args, scan_budget=10)
            assert (result["entries"] if isinstance(result, dict) else list(result)) == []
    finally:
        reader.rollback()


@pytest.mark.parametrize("method", ["scan_prefix", "search_keys"])
def test_issue583_future_candidates_do_not_conflict_with_unrelated_write(scan_db, method):
    reader = scan_db.begin(TxnMode.READ_WRITE)
    try:
        with scan_db.begin(TxnMode.READ_WRITE) as writer:
            writer.put(b"future:1", b"new")
            writer.commit()
        result = getattr(reader, method)(b"future:*" if method == "search_keys" else b"future:")
        assert (result["entries"] if isinstance(result, dict) else list(result)) == []
        reader.put(b"independent", b"value")
        reader.commit()
    finally:
        if reader.status["state"] == "active":
            reader.rollback()
    with scan_db.begin(TxnMode.READ_ONLY) as check:
        assert check.get(b"independent") == b"value"


def test_issue583_post_snapshot_sstable_candidates_consume_scan_budget(tmp_path):
    db = Database.open(str(tmp_path / "flushed.alopex"))
    reader = db.begin(TxnMode.READ_ONLY)
    try:
        with db.begin(TxnMode.READ_WRITE) as writer:
            for i in range(3):
                writer.put(f"future:{i}".encode(), b"new")
            writer.commit()
        db.flush()
        for method, args in [(reader.scan_prefix, (b"future:",)), (reader.search_keys, (b"future:*",))]:
            with pytest.raises(AlopexError):
                method(*args, scan_budget=1)
            result = method(*args, scan_budget=10)
            assert (result["entries"] if isinstance(result, dict) else list(result)) == []
    finally:
        reader.rollback()
        db.close()
