import inspect

import pytest
from alopex import AlopexError, DataFrame, LazyFrame, Series, col, concat, concat_str, lit

pytestmark = [pytest.mark.requires_polars, pytest.mark.polars_reference]


def _signature_shape(callable):
    def default_value(parameter):
        value = parameter.default
        if value is inspect.Parameter.empty or isinstance(
            value, (type(None), bool, int, float, str, bytes, tuple)
        ):
            return value
        return None

    return [
        (parameter.name, parameter.kind, default_value(parameter))
        for parameter in list(inspect.signature(callable).parameters.values())
        if parameter.name != "self"
    ]


def test_signature_shape_compares_default_values():
    def alopex_api(*, enabled=True):
        pass

    def reference_api(*, enabled=False):
        pass

    assert _signature_shape(alopex_api) != _signature_shape(reference_api)


def test_polars_1432_supported_public_signatures():
    import alopex
    import polars as pl

    for alopex_api, polars_api in (
        (alopex.DataFrame, pl.DataFrame),
        (alopex.DataFrame.explode, pl.DataFrame.explode),
        (alopex.DataFrame.to_dict, pl.DataFrame.to_dict),
        (alopex.DataFrame.lazy, pl.DataFrame.lazy),
        (alopex.LazyFrame.select, pl.LazyFrame.select),
        (alopex.LazyFrame.filter, pl.LazyFrame.filter),
        (alopex.LazyFrame.with_columns, pl.LazyFrame.with_columns),
        (alopex.Expr.alias, pl.Expr.alias),
        (alopex.Expr.add, pl.Expr.add),
        (alopex.Expr.sub, pl.Expr.sub),
        (alopex.Expr.mul, pl.Expr.mul),
        (alopex.Expr.eq, pl.Expr.eq),
        (alopex.Expr.gt, pl.Expr.gt),
        (alopex.Expr.lt, pl.Expr.lt),
        (alopex.Expr.ge, pl.Expr.ge),
        (alopex.Expr.le, pl.Expr.le),
        (alopex.Expr.and_, pl.Expr.and_),
        (alopex.Expr.or_, pl.Expr.or_),
        (alopex.Expr.not_, pl.Expr.not_),
        (alopex.concat, pl.concat),
        (alopex.concat_str, pl.concat_str),
        (alopex.lit, pl.lit),
        (alopex.LazyFrame.collect, pl.LazyFrame.collect),
        (alopex.LazyFrame.collect_batches, pl.LazyFrame.collect_batches),
    ):
        assert _signature_shape(alopex_api) == _signature_shape(polars_api)


def test_polars_1432_dataframe_properties_series_and_constructor_defaults():
    import polars as pl

    alopex_frame = DataFrame({"id": [1, 2], "label": ["one", None]})
    polars_frame = pl.DataFrame({"id": [1, 2], "label": ["one", None]})

    assert alopex_frame.height == polars_frame.height
    assert alopex_frame.width == polars_frame.width
    assert alopex_frame.to_dict(as_series=False) == polars_frame.to_dict(as_series=False)
    alopex_series = alopex_frame.to_dict()
    polars_series = polars_frame.to_dict()
    assert all(isinstance(value, Series) for value in alopex_series.values())
    assert {name: value.to_list() for name, value in alopex_series.items()} == {
        name: value.to_list() for name, value in polars_series.items()
    }
    assert DataFrame().to_dict(as_series=False) == pl.DataFrame().to_dict(as_series=False)
    assert DataFrame({"items": [["a", "b"], None]}).explode("items").to_dict(
        as_series=False
    ) == pl.DataFrame({"items": [["a", "b"], None]}).explode("items").to_dict(
        as_series=False
    )


def test_polars_1432_expr_scalar_variadic_and_module_default_values():
    import polars as pl

    frame = DataFrame({"a": [1, 2, 3], "b": [2, 2, 2], "s": ["x", None, "z"]})
    expected = pl.DataFrame({"a": [1, 2, 3], "b": [2, 2, 2], "s": ["x", None, "z"]})
    actual_result = frame.lazy().select(
        col("a").add(1).alias("add"),
        col("a").sub(1).alias("sub"),
        col("a").mul(2).alias("mul"),
        col("a").eq(2).alias("eq"),
        col("a").gt(1).and_(col("b").le(2), col("a").lt(3)).alias("logic"),
        concat_str("s", lit("!"), separator="").alias("joined"),
    ).collect()
    expected_result = expected.lazy().select(
        pl.col("a").add(1).alias("add"),
        pl.col("a").sub(1).alias("sub"),
        pl.col("a").mul(2).alias("mul"),
        pl.col("a").eq(2).alias("eq"),
        pl.col("a").gt(1).and_(pl.col("b").le(2), pl.col("a").lt(3)).alias("logic"),
        pl.concat_str("s", pl.lit("!"), separator="").alias("joined"),
    ).collect()
    assert actual_result.to_dict(as_series=False) == expected_result.to_dict(as_series=False)
    assert concat([frame]).to_dict(as_series=False) == pl.concat([expected]).to_dict(
        as_series=False
    )


def test_polars_1432_unsupported_options_are_explicitly_rejected():
    frame = DataFrame({"a": [1, 2]})

    with pytest.raises(NotImplementedError):
        DataFrame({"a": [1]}, strict=False)
    with pytest.raises(NotImplementedError):
        frame.explode("a", keep_nulls=False)
    with pytest.raises(NotImplementedError):
        lit(1, dtype="int64")
    with pytest.raises(NotImplementedError):
        concat([frame], how="horizontal")
    with pytest.raises(NotImplementedError):
        frame.lazy().collect(background=True)
    with pytest.raises(NotImplementedError):
        frame.lazy().collect_batches(lazy=True)


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

    assert alopex_result.to_dict(as_series=False) == polars_result.to_dict(as_series=False)
    assert alopex_result.height == polars_result.height
    assert alopex_result.width == polars_result.width
    assert (
        DataFrame({"id": [1, 2, 3]})
        .lazy()
        .select(col("id").gt(lit(1)).not_().alias("flag"))
        .collect()
        .to_dict(as_series=False)
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
        assert alopex_lazy.collect().to_dict(as_series=False) == polars_lazy.collect().to_dict(
            as_series=False
        )

        with alopex_lazy.collect_batches(chunk_size=1024) as batches:
            alopex_rows = sum(batch.height for batch in batches)
        polars_rows = sum(batch.height for batch in polars_lazy.collect_batches())
        assert alopex_rows == polars_rows == expected.height
