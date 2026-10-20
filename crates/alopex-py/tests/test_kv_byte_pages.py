"""Byte-bound regression cases: three entries, at most four value bytes each."""
import ast
import asyncio
import inspect
from pathlib import Path

import pytest
from alopex import AlopexError, Database, InvalidCursor, Transaction, TxnMode
from alopex.asyncio import AsyncDatabase, AsyncTransaction


@pytest.fixture
def txn():
    db = Database.new()
    with db.begin(TxnMode.READ_WRITE) as txn:
        for key in (b"p0", b"p1", b"p2"):
            txn.put(key, b"vv")
        yield txn
    db.close()


@pytest.mark.parametrize("kind,args", [("prefix", (b"p",)), ("range", (b"p", b"q"))])
def test_byte_pages_and_complete_scan(txn, kind, args):
    page_fn = getattr(txn, f"scan_{kind}_page")
    all_fn = getattr(txn, f"scan_{kind}")
    expected = [(b"p0", b"vv"), (b"p1", b"vv"), (b"p2", b"vv")]
    cursor = None
    actual = []
    for n in range(3):
        page = page_fn(*args, max_bytes=4, cursor=cursor)
        assert page["entries"] == expected[n:n + 1]
        assert page["has_more"] is (n < 2)
        actual.extend(page["entries"])
        cursor = page["next_cursor"]
    assert cursor is None
    assert actual == expected
    assert list(all_fn(*args, max_bytes=4, max_total_bytes=12)) == expected
    assert list(all_fn(*args, max_bytes=4, max_total_bytes=None)) == expected
    assert list(all_fn(*args, max_bytes=4, limit=2)) == expected[:2]
    with pytest.raises(AlopexError, match="limit=11, requested=12"):
        all_fn(*args, max_bytes=4, max_total_bytes=11)
    assert txn.get(b"p0") == b"vv"


def test_hidden_bytes_do_not_consume_response_budget(txn):
    txn.put(b"__catalog__/hidden", b"vvvv")
    page = txn.scan_prefix_page(b"", max_bytes=4, scan_budget=1)
    assert page["entries"] == []
    assert page["has_more"]
    next_page = txn.scan_prefix_page(b"", max_bytes=4, cursor=page["next_cursor"])
    assert next_page["entries"] == [(b"p0", b"vv")]
    with pytest.raises(AlopexError, match="scan entry too large"):
        txn.scan_prefix_page(b"", max_bytes=4, include_internal=True)


def test_oversized_entry_names_key_and_size_after_valid_page(txn):
    txn.put(b"p1", b"vvvv")
    page = txn.scan_prefix_page(b"p", max_bytes=4)
    assert page["entries"] == [(b"p0", b"vv")]
    with pytest.raises(AlopexError, match=r"key=\[112, 49\], limit=4, requested=6"):
        txn.scan_prefix_page(b"p", max_bytes=4, cursor=page["next_cursor"])


@pytest.mark.parametrize("method,args,options", [
    ("scan_prefix_page", (b"q",), {}),
    ("scan_prefix", (b"p",), {"include_internal": True}),
    ("scan_range_page", (b"p", b"q"), {}),
    ("search_keys", (b"p*",), {}),
])
def test_cursor_is_bound_to_scan(txn, method, args, options):
    token = txn.scan_prefix_page(b"p", limit=1)["next_cursor"]
    with pytest.raises(InvalidCursor, match="InvalidCursor") as exc:
        getattr(txn, method)(*args, cursor=token, **options)
    assert isinstance(exc.value, ValueError)
    assert exc.value.code == "ALOPEX-PY999"
    # Page-size and byte budgets may change while the scan conditions stay fixed.
    assert len(txn.scan_prefix_page(b"p", cursor=token, max_bytes=4)["entries"]) == 1


@pytest.mark.parametrize("mode,pattern,other", [("glob", b"p*", b"*"), ("regex", "p.*", ".*")])
def test_search_cursor_binds_pattern(txn, mode, pattern, other):
    token = txn.search_keys(pattern, mode=mode, limit=1)["next_cursor"]
    assert len(txn.search_keys(pattern, mode=mode, cursor=token)["entries"]) == 2
    with pytest.raises(InvalidCursor, match="InvalidCursor"):
        txn.search_keys(other, mode=mode, cursor=token)


@pytest.mark.parametrize("kind,args", [("prefix", (b"p",)), ("range", (b"p", b"q"))])
def test_async_scan_yields_between_byte_pages_and_checks_total(kind, args):
    async def run():
        db = await AsyncDatabase.new(thread_mode="single")
        async with await db.begin(TxnMode.READ_WRITE) as txn:
            for key in (b"p0", b"p1", b"p2"):
                await txn.put(key, b"vv")
            ticks = 0
            finished = False

            async def heartbeat():
                nonlocal ticks
                while not finished:
                    ticks += 1
                    await asyncio.sleep(0)

            task = asyncio.create_task(heartbeat())
            scan = getattr(txn, f"scan_{kind}")
            try:
                assert len(list(await scan(*args, max_bytes=4, max_total_bytes=None))) == 3
                assert ticks >= 3
                with pytest.raises(AlopexError, match="limit=11, requested=12"):
                    await scan(*args, max_bytes=4, max_total_bytes=11)
                assert len(list(await scan(*args, max_bytes=4, max_total_bytes=12))) == 3
            finally:
                finished = True
                await task
        await db.close()
    asyncio.run(run())


def _methods(path, class_name):
    cls = next(n for n in ast.parse(path.read_text()).body if isinstance(n, ast.ClassDef) and n.name == class_name)
    return {n.name: n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def test_scan_stub_signatures_match_runtime_and_sync():
    root = Path(__file__).parents[1] / "python" / "alopex"
    runtime = _methods(root / "asyncio.py", "AsyncTransaction")
    stub = _methods(root / "asyncio.pyi", "AsyncTransaction")
    sync = _methods(root / "_alopex.pyi", "Transaction")
    names = ("scan_prefix", "scan_range", "scan_prefix_page", "scan_range_page", "iter_prefix", "iter_range")
    for name in names:
        assert ast.dump(stub[name].args) == ast.dump(runtime[name].args), name
        assert ast.dump(stub[name].returns) == ast.dump(runtime[name].returns), name
        if not name.startswith("iter_"):
            assert ast.dump(stub[name].args) == ast.dump(sync[name].args), name
            def public_parameters(method):
                return [(p.name, p.kind, p.default) for p in inspect.signature(method).parameters.values() if p.name != "self"]
            assert public_parameters(getattr(Transaction, name)) == public_parameters(getattr(AsyncTransaction, name)), name



@pytest.mark.parametrize("start,end,internal", [(b"p0", b"q", False), (b"p", b"r", False), (b"p", b"q", True)])
def test_range_cursor_binds_both_bounds_and_visibility(txn, start, end, internal):
    token = txn.scan_range_page(b"p", b"q", limit=1)["next_cursor"]
    with pytest.raises(InvalidCursor):
        txn.scan_range_page(start, end, cursor=token, include_internal=internal)
