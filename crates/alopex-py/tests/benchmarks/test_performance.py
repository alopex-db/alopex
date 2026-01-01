import time

import pytest

pytest.importorskip("pytest_benchmark")
pl = pytest.importorskip("polars")

from alopex import Catalog, ColumnInfo

pytestmark = [
    pytest.mark.requires_polars,
    pytest.mark.requires_pytest_benchmark,
]


def _mean_time(fn, repeats=5):
    durations = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        durations.append(time.perf_counter() - start)
    return sum(durations) / len(durations)


def _prepare_catalog(tmp_path, unique_name, storage_location):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)
    Catalog.create_table(
        catalog_name,
        namespace_name,
        table_name,
        [
            ColumnInfo("id", "INTEGER", 0, False),
            ColumnInfo("value", "DOUBLE", 1, False),
        ],
        storage_location,
    )
    return catalog_name, namespace_name, table_name


def _cleanup_catalog(catalog_name, namespace_name, table_name):
    try:
        Catalog.delete_table(catalog_name, namespace_name, table_name)
    except Exception:
        pass
    try:
        Catalog.delete_namespace(catalog_name, namespace_name)
    except Exception:
        pass
    try:
        Catalog.delete_catalog(catalog_name)
    except Exception:
        pass


def _make_large_df(rows):
    return pl.DataFrame(
        {
            "id": list(range(rows)),
            "value": [float(i) for i in range(rows)],
        }
    )


@pytest.mark.usefixtures("unique_name")
def test_scan_overhead_vs_polars(tmp_path, unique_name, benchmark):
    rows = 100_000
    df = _make_large_df(rows)
    storage_location = str(tmp_path / "data.parquet")
    df.write_parquet(storage_location)

    catalog_name, namespace_name, table_name = _prepare_catalog(
        tmp_path, unique_name, storage_location
    )

    def measure():
        direct = _mean_time(lambda: pl.scan_parquet(storage_location), repeats=10)
        wrapped = _mean_time(
            lambda: Catalog.scan_table(catalog_name, namespace_name, table_name),
            repeats=10,
        )
        overhead_ms = (wrapped - direct) * 1000.0
        return direct, wrapped, overhead_ms

    try:
        _, _, overhead_ms = benchmark.pedantic(
            measure, iterations=1, rounds=1, warmup_rounds=1
        )
        assert overhead_ms < 1.0, f"Python→Rust overhead too high: {overhead_ms:.3f}ms"
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)


@pytest.mark.usefixtures("unique_name")
def test_large_read_overhead(tmp_path, unique_name, benchmark):
    rows = 100_000
    df = _make_large_df(rows)
    storage_location = str(tmp_path / "data.parquet")
    df.write_parquet(storage_location)

    catalog_name, namespace_name, table_name = _prepare_catalog(
        tmp_path, unique_name, storage_location
    )

    def measure():
        direct = _mean_time(
            lambda: pl.scan_parquet(storage_location).collect(), repeats=5
        )
        wrapped = _mean_time(
            lambda: Catalog.scan_table(catalog_name, namespace_name, table_name).collect(),
            repeats=5,
        )
        overhead = (wrapped - direct) / direct
        return direct, wrapped, overhead

    try:
        _, _, overhead = benchmark.pedantic(
            measure, iterations=1, rounds=1, warmup_rounds=1
        )
        assert overhead < 0.05, f"read overhead too high: {overhead * 100:.2f}%"
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)


@pytest.mark.usefixtures("unique_name")
def test_large_write_overhead(tmp_path, unique_name, benchmark):
    rows = 100_000
    df = _make_large_df(rows)
    storage_location = str(tmp_path / "data.parquet")

    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)

    try:
        Catalog.write_table(
            df,
            catalog_name,
            namespace_name,
            table_name,
            delta_mode="append",
            storage_location=storage_location,
        )

        def measure():
            direct = _mean_time(lambda: df.write_parquet(storage_location), repeats=5)
            wrapped = _mean_time(
                lambda: Catalog.write_table(
                    df,
                    catalog_name,
                    namespace_name,
                    table_name,
                    delta_mode="overwrite",
                ),
                repeats=5,
            )
            overhead = (wrapped - direct) / direct
            return direct, wrapped, overhead

        _, _, overhead = benchmark.pedantic(
            measure, iterations=1, rounds=1, warmup_rounds=1
        )
        assert overhead < 0.05, f"write overhead too high: {overhead * 100:.2f}%"
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)
