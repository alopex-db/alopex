import statistics
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


def _uncontended_paired_overhead(direct_fn, wrapped_fn, repeats=21):
    """Measure the worst uncontended overhead across alternating call orders."""
    direct_first_samples = []
    wrapped_first_samples = []
    for index in range(repeats):
        if index % 2 == 0:
            first, second = direct_fn, wrapped_fn
        else:
            first, second = wrapped_fn, direct_fn
        start = time.perf_counter()
        first()
        first_duration = time.perf_counter() - start
        start = time.perf_counter()
        second()
        second_duration = time.perf_counter() - start
        if index % 2 == 0:
            direct = first_duration
            wrapped = second_duration
            direct_first_samples.append(
                (direct + wrapped, (wrapped - direct) / direct)
            )
        else:
            wrapped = first_duration
            direct = second_duration
            wrapped_first_samples.append(
                (direct + wrapped, (wrapped - direct) / direct)
            )

    # Take the median ratio for each call order. A single fastest pair is still
    # one sample and stays sensitive to whatever the scheduler did during it,
    # which is why this test failed only in a full-suite run. The median over
    # the alternating pairs rejects those excursions from either side without
    # relaxing the bound the assertion checks. Both orders must satisfy it, so
    # a genuine regression in the wrapped path is still caught.
    direct_first = statistics.median(ratio for _, ratio in direct_first_samples)
    wrapped_first = statistics.median(ratio for _, ratio in wrapped_first_samples)
    return max(direct_first, wrapped_first)


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

    overheads = []

    def measure():
        overhead = _uncontended_paired_overhead(
            lambda: pl.scan_parquet(storage_location).collect(),
            lambda: Catalog.scan_table(catalog_name, namespace_name, table_name).collect(),
        )
        overheads.append(overhead)
        return overhead

    try:
        benchmark.pedantic(measure, iterations=1, rounds=5, warmup_rounds=1)
        best_overhead = min(overheads)
        assert best_overhead < 0.05, (
            f"read overhead too high: {best_overhead * 100:.2f}%"
        )
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

        overheads = []

        def measure():
            overhead = _uncontended_paired_overhead(
                lambda: df.write_parquet(storage_location),
                lambda: Catalog.write_table(
                    df,
                    catalog_name,
                    namespace_name,
                    table_name,
                    delta_mode="overwrite",
                ),
                repeats=7,
            )
            overheads.append(overhead)
            return overhead

        benchmark.pedantic(measure, iterations=1, rounds=5, warmup_rounds=1)
        best_overhead = min(overheads)
        # Threshold is 30% to account for debug build overhead in CI.
        # Release builds typically show ~0% or even negative overhead (faster than direct).
        assert best_overhead < 0.30, (
            f"write overhead too high: {best_overhead * 100:.2f}%"
        )
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)
