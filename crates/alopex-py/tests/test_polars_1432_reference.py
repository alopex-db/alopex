import inspect

import pytest
from alopex import AlopexError, DataFrame, LazyFrame, col, lit

pytestmark = [pytest.mark.requires_polars, pytest.mark.polars_reference]


def _signature_shape(callable):
    return [
        (parameter.name, parameter.kind, parameter.default is inspect.Parameter.empty)
        for parameter in list(inspect.signature(callable).parameters.values())
        if parameter.name != "self"
    ]


def test_polars_1432_supported_public_signatures():
    import alopex
    import polars as pl

    for alopex_api, polars_api in (
        (alopex.DataFrame.lazy, pl.DataFrame.lazy),
        (alopex.LazyFrame.select, pl.LazyFrame.select),
        (alopex.LazyFrame.filter, pl.LazyFrame.filter),
        (alopex.LazyFrame.with_columns, pl.LazyFrame.with_columns),
        (alopex.Expr.alias, pl.Expr.alias),
        (alopex.Expr.not_, pl.Expr.not_),
    ):
        assert _signature_shape(alopex_api) == _signature_shape(polars_api)


def test_polars_1432_dataframe_lazy_and_error_contracts():
    import polars as pl

    assert pl.__version__ == "1.43.2"
    columns = {
        "id": [1, 2, 3, 4],
        "label": ["one", None, "three", "four"],
    }
    alopex_result = (
        DataFrame(columns)
        .lazy()
        .filter(col("id").gt(lit(1)))
        .with_columns([col("id").add(lit(10)).alias("next_id")])
        .select([col("label"), col("next_id")])
        .collect()
    )
    polars_result = (
        pl.DataFrame(columns)
        .lazy()
        .filter(pl.col("id") > 1)
        .with_columns((pl.col("id") + 10).alias("next_id"))
        .select("label", "next_id")
        .collect()
    )

    assert alopex_result.to_dict() == polars_result.to_dict(as_series=False)
    assert alopex_result.height() == polars_result.height
    assert alopex_result.width() == polars_result.width
    assert (
        DataFrame({"id": [1, 2, 3]})
        .lazy()
        .select(col("id").gt(lit(1)).not_().alias("flag"))
        .collect()
        .to_dict()
        == pl.DataFrame({"id": [1, 2, 3]})
        .lazy()
        .select((pl.col("id") > 1).not_().alias("flag"))
        .collect()
        .to_dict(as_series=False)
    )
    with pytest.raises(AlopexError):
        DataFrame(columns).lazy().select([col("missing")]).collect()
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        pl.DataFrame(columns).lazy().select("missing").collect()


def test_polars_1432_csv_parquet_and_streaming_contracts(tmp_path):
    import polars as pl

    assert pl.__version__ == "1.43.2"
    expected = pl.DataFrame(
        {
            "id": list(range(2050)),
            "label": [None if index % 10 == 0 else f"row-{index}" for index in range(2050)],
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
        assert alopex_lazy.collect().to_dict() == polars_lazy.collect().to_dict(
            as_series=False
        )

        with alopex_lazy.collect_batches(chunk_size=1024) as batches:
            alopex_rows = sum(batch.height() for batch in batches)
        polars_rows = sum(batch.height for batch in polars_lazy.collect_batches())
        assert alopex_rows == polars_rows == expected.height
