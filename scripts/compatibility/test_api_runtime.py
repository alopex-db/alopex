"""Version-neutral replacement for the old same-process diagnostic prose check."""


def test_second_open_keeps_exception_code_refusal_and_release_contract(tmp_path):
    import alopex
    import pytest

    path = str(tmp_path / "locked.alopex")
    first = alopex.Database.open(path)
    try:
        with pytest.raises(alopex.AlopexError) as raised:
            alopex.Database.open(path)
        assert raised.value.code == "ALOPEX-PY999"
        assert "already open" in str(raised.value)
        assert "lock file:" in str(raised.value)
    finally:
        first.close()
    reopened = alopex.Database.open(path)
    reopened.close()


def test_stream_with_two_row_chunks(tmp_path):
    import pytest
    from alopex import LazyFrame

    path = tmp_path / "stream.csv"
    path.write_text(
        "id\n" + "".join(f"{index}\n" for index in range(5)),
        encoding="utf-8",
    )

    with LazyFrame.scan_csv(str(path)).collect_batches(chunk_size=2) as batches:
        values = [
            value for batch in batches for value in batch.to_dict(as_series=False)["id"]
        ]

    assert values == list(range(5))

    with pytest.raises(NotImplementedError):
        LazyFrame.scan_csv(str(path)).collect(streaming=True)


def test_polars_io_with_two_row_chunks(tmp_path):
    import polars as pl
    from alopex import LazyFrame

    assert pl.__version__ == "1.43.2"
    expected = pl.DataFrame(
        {
            "id": list(range(5)),
            "label": [None if index % 3 == 0 else f"row-{index}" for index in range(5)],
        }
    )
    csv_path = tmp_path / "rows.csv"
    parquet_path = tmp_path / "rows.parquet"
    expected.write_csv(csv_path)
    expected.write_parquet(parquet_path)

    for path, alopex_scan, polars_scan in (
        (csv_path, LazyFrame.scan_csv, pl.scan_csv),
        (parquet_path, LazyFrame.scan_parquet, pl.scan_parquet),
    ):
        alopex_lazy = alopex_scan(str(path))
        polars_lazy = polars_scan(path)
        assert alopex_lazy.collect().to_dict(
            as_series=False
        ) == polars_lazy.collect().to_dict(as_series=False)

        with alopex_lazy.collect_batches(chunk_size=2) as batches:
            alopex_rows = sum(batch.height for batch in batches)
        polars_rows = sum(batch.height for batch in polars_lazy.collect_batches())
        assert alopex_rows == polars_rows == expected.height


def test_known_error_codes_preserved_with_explicit_capacity_addition():
    import ast
    from pathlib import Path

    import alopex

    # Read the immutable baseline corpus, not the candidate's expected registry.
    baseline = Path(__file__).resolve().parents[2] / "crates/alopex-py/tests/test_compatibility_contract.py"
    assignments = [
        node.value for node in ast.parse(baseline.read_text(encoding="utf-8")).body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "EXPECTED_ERROR_CODES"
                for target in node.targets)
    ]
    assert len(assignments) == 1
    expected = ast.literal_eval(assignments[0])
    assert isinstance(expected, set) and expected
    actual = set(alopex.ALOPEX_ERROR_CODES)
    assert expected <= actual, ("baseline codes removed", expected - actual)
    assert actual - expected <= {"ALOPEX-S004"}, ("unapproved additions", actual - expected)
