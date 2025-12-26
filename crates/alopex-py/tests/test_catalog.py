import importlib.util

import pytest

from alopex import AlopexError, Catalog, ColumnInfo


def test_catalog_list_get_and_ddl(unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"

    Catalog.create_catalog(catalog_name)
    catalogs = Catalog.list_catalogs()
    assert any(info.name == catalog_name for info in catalogs)

    with pytest.raises(AlopexError):
        Catalog.list_namespaces("missing_catalog")

    namespaces = Catalog.list_namespaces(catalog_name)
    assert namespaces == []

    Catalog.create_namespace(catalog_name, namespace_name)
    namespaces = Catalog.list_namespaces(catalog_name)
    assert any(info.name == namespace_name for info in namespaces)

    with pytest.raises(AlopexError):
        Catalog.list_tables(catalog_name, "missing_namespace")

    tables = Catalog.list_tables(catalog_name, namespace_name)
    assert tables == []

    columns = [ColumnInfo("id", "int", 0, False)]
    Catalog.create_table(catalog_name, namespace_name, table_name, columns, "/tmp/data.parquet")

    table_info = Catalog.get_table_info(catalog_name, namespace_name, table_name)
    assert table_info.name == table_name

    Catalog.delete_table(catalog_name, namespace_name, table_name)
    with pytest.raises(AlopexError):
        Catalog.get_table_info(catalog_name, namespace_name, table_name)

    Catalog.delete_namespace(catalog_name, namespace_name)
    Catalog.delete_catalog(catalog_name)


def test_scan_table_requires_polars():
    if importlib.util.find_spec("polars") is not None:
        pytest.skip("polars がインストール済みのためスキップ")
    with pytest.raises(AlopexError):
        Catalog.scan_table("cat", "ns", "tbl")


def test_write_table_requires_polars():
    if importlib.util.find_spec("polars") is not None:
        pytest.skip("polars がインストール済みのためスキップ")
    with pytest.raises(AlopexError):
        Catalog.write_table(None, "cat", "ns", "tbl")


@pytest.mark.requires_polars
def test_scan_and_write_table(tmp_path, unique_name):
    import polars as pl

    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)

    storage_location = str(tmp_path / "data.parquet")
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    Catalog.write_table(
        df,
        catalog_name,
        namespace_name,
        table_name,
        delta_mode="overwrite",
        storage_location=storage_location,
    )

    lf = Catalog.scan_table(catalog_name, namespace_name, table_name)
    out = lf.collect()
    assert out.shape[0] == 2

    Catalog.delete_table(catalog_name, namespace_name, table_name)
    Catalog.delete_namespace(catalog_name, namespace_name)
    Catalog.delete_catalog(catalog_name)
