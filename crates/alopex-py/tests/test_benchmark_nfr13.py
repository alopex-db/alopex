import pytest

pytest.importorskip("pytest_benchmark")

import alopex


def test_binding_overhead_get_put(benchmark):
    db = alopex.Database.new()
    key = b"12345678"
    value = b"abcdefgh"

    def run():
        txn = db.begin(alopex.TxnMode.READ_WRITE)
        txn.put(key, value)
        txn.commit()

        txn = db.begin()
        _ = txn.get(key)
        txn.rollback()

    benchmark.pedantic(
        run,
        iterations=10000,
        rounds=1,
        warmup_rounds=1,
    )
