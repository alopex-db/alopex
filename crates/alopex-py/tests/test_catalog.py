import pytest

from alopex import AlopexError, Catalog, ColumnInfo


def _cleanup_catalog(catalog_name, namespace_name=None, table_name=None):
    if table_name and namespace_name:
        try:
            Catalog.delete_table(catalog_name, namespace_name, table_name)
        except Exception:
            pass
    if namespace_name:
        try:
            Catalog.delete_namespace(catalog_name, namespace_name)
        except Exception:
            pass
    try:
        Catalog.delete_catalog(catalog_name)
    except Exception:
        pass


def test_metadata_api_fields(tmp_path, unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"
    storage_location = str(tmp_path / "data.parquet")

    columns = [
        ColumnInfo("id", "INTEGER", 0, False),
        ColumnInfo("name", "TEXT", 1, True),
    ]

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)
    Catalog.create_table(
        catalog_name,
        namespace_name,
        table_name,
        columns,
        storage_location,
        data_source_format="PARQUET",
    )

    try:
        catalogs = Catalog.list_catalogs()
        catalog_info = next(info for info in catalogs if info.name == catalog_name)
        assert catalog_info.comment is None
        assert catalog_info.storage_root is None

        namespaces = Catalog.list_namespaces(catalog_name)
        namespace_info = next(info for info in namespaces if info.name == namespace_name)
        assert namespace_info.catalog_name == catalog_name
        assert namespace_info.comment is None
        assert namespace_info.storage_root is None

        tables = Catalog.list_tables(catalog_name, namespace_name)
        table_info = next(info for info in tables if info.name == table_name)
        assert table_info.catalog_name == catalog_name
        assert table_info.namespace_name == namespace_name
        assert table_info.table_type == "MANAGED"
        assert table_info.storage_location == storage_location
        assert table_info.data_source_format == "PARQUET"
        assert table_info.primary_key is None
        assert table_info.comment is None
        assert len(table_info.columns) == 2
        assert table_info.columns[0].name == "id"
        assert table_info.columns[0].type_name == "INTEGER"
        assert table_info.columns[0].position == 0
        assert table_info.columns[0].nullable is False

        fetched = Catalog.get_table_info(catalog_name, namespace_name, table_name)
        assert fetched.name == table_name
        assert fetched.storage_location == storage_location
        assert fetched.data_source_format == "PARQUET"
        assert len(fetched.columns) == 2
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)


def test_ddl_error_cases(unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"

    with pytest.raises(ValueError):
        Catalog.create_namespace(catalog_name, namespace_name)

    Catalog.create_catalog(catalog_name)
    try:
        with pytest.raises(RuntimeError):
            Catalog.create_catalog(catalog_name)

        columns = [ColumnInfo("id", "INTEGER", 0, False)]
        with pytest.raises(AlopexError):
            Catalog.create_table(
                catalog_name,
                namespace_name,
                table_name,
                columns,
                "/tmp/data.parquet",
                data_source_format="CSV",
            )
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)


@pytest.mark.requires_polars
def test_scan_table_storage_options_merge(tmp_path, unique_name, monkeypatch):
    import polars as pl

    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"
    storage_location = str(tmp_path / "data.parquet")

    df = pl.DataFrame({"id": [1]})
    df.write_parquet(storage_location)

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)
    Catalog.create_table(
        catalog_name,
        namespace_name,
        table_name,
        [ColumnInfo("id", "INTEGER", 0, False)],
        storage_location,
    )

    captured = {}

    def fake_scan(path, **kwargs):
        captured["path"] = path
        captured["kwargs"] = kwargs

        class DummyLazyFrame:
            def collect(self):
                return pl.DataFrame({"id": [1]})

        return DummyLazyFrame()

    monkeypatch.setattr(pl, "scan_parquet", fake_scan)

    try:
        Catalog.scan_table(
            catalog_name,
            namespace_name,
            table_name,
            credential_provider={"token": "old", "shared": "old"},
            storage_options={"shared": "new", "extra": "1"},
        )
        assert captured["path"] == storage_location
        assert captured["kwargs"]["token"] == "old"
        assert captured["kwargs"]["shared"] == "new"
        assert captured["kwargs"]["extra"] == "1"
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)


@pytest.mark.requires_polars
def test_scan_table_error_cases(unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)

    try:
        with pytest.raises(ValueError):
            Catalog.scan_table(catalog_name, namespace_name, "missing_table")
        with pytest.raises(ValueError):
            Catalog.scan_table("bad-name", namespace_name, "tbl")
    finally:
        _cleanup_catalog(catalog_name, namespace_name)


@pytest.mark.requires_polars
def test_write_table_modes(tmp_path, unique_name):
    import polars as pl

    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"
    storage_location = str(tmp_path / "data.parquet")

    df_initial = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    df_append = pl.DataFrame({"id": [3], "name": ["c"]})
    df_overwrite = pl.DataFrame({"id": [10], "name": ["z"]})
    df_merge = pl.DataFrame({"id": [2, 4], "name": ["bb", "d"]})

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)

    try:
        Catalog.write_table(
            df_initial,
            catalog_name,
            namespace_name,
            table_name,
            delta_mode="append",
            storage_location=storage_location,
        )
        out = pl.read_parquet(storage_location)
        assert out.shape[0] == 2

        Catalog.write_table(
            df_append,
            catalog_name,
            namespace_name,
            table_name,
            delta_mode="append",
        )
        out = pl.read_parquet(storage_location)
        assert out.shape[0] == 3

        Catalog.write_table(
            df_overwrite,
            catalog_name,
            namespace_name,
            table_name,
            delta_mode="overwrite",
        )
        out = pl.read_parquet(storage_location)
        assert out.shape[0] == 1
        assert out["id"].to_list() == [10]

        Catalog.write_table(
            df_merge,
            catalog_name,
            namespace_name,
            table_name,
            delta_mode="merge",
            primary_key=["id"],
        )
        out = pl.read_parquet(storage_location).sort("id")
        assert out["id"].to_list() == [2, 4, 10]
        assert out["name"].to_list() == ["bb", "d", "z"]
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)


@pytest.mark.requires_polars
def test_write_table_error_cases(tmp_path, unique_name):
    import polars as pl

    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"
    storage_location = str(tmp_path / "data.parquet")
    df = pl.DataFrame({"id": [1]})

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)

    try:
        with pytest.raises(AlopexError):
            Catalog.write_table(
                df,
                catalog_name,
                namespace_name,
                table_name,
                delta_mode="error",
                storage_location=storage_location,
            )

        with pytest.raises(AlopexError):
            Catalog.write_table(
                df,
                catalog_name,
                namespace_name,
                table_name,
                delta_mode="merge",
                storage_location=storage_location,
            )
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)
