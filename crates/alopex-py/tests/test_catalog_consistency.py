import json
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from alopex import Catalog, ColumnInfo


def _load_expected() -> dict:
    fixture_path = (
        Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "cross_surface_expected.json"
    )
    with fixture_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


@dataclass
class CatalogResources:
    catalog_name: str
    namespaces: list[str] = field(default_factory=list)
    tables: list[tuple[str, str]] = field(default_factory=list)


@pytest.fixture()
def catalog_resources(unique_name):
    resources = CatalogResources(catalog_name=f"{unique_name}_cat")
    Catalog.create_catalog(resources.catalog_name)
    try:
        yield resources
    finally:
        for namespace, table_name in reversed(resources.tables):
            try:
                Catalog.delete_table(resources.catalog_name, namespace, table_name)
            except Exception:
                pass
        for namespace in reversed(resources.namespaces):
            try:
                Catalog.delete_namespace(resources.catalog_name, namespace)
            except Exception:
                pass
        try:
            Catalog.delete_catalog(resources.catalog_name)
        except Exception:
            pass


def _create_namespace(resources: CatalogResources, namespace: str) -> None:
    Catalog.create_namespace(resources.catalog_name, namespace)
    resources.namespaces.append(namespace)


def _create_table(
    resources: CatalogResources,
    namespace: str,
    table_name: str,
    columns: list[ColumnInfo],
    storage_location: str,
) -> None:
    Catalog.create_table(
        resources.catalog_name,
        namespace,
        table_name,
        columns,
        storage_location,
    )
    resources.tables.append((namespace, table_name))


def _names(items) -> set[str]:
    return {item.name for item in items}


def _assert_error_code(exc_info, code: str) -> None:
    assert getattr(exc_info.value, "code", None) == code


class _FakeDType:
    def __init__(self, name: str):
        self._name = name

    def __str__(self) -> str:
        return self._name


def _infer_fake_dtype(values) -> _FakeDType:
    for value in values:
        if isinstance(value, int):
            return _FakeDType("Int64")
        if isinstance(value, str):
            return _FakeDType("String")
    return _FakeDType("String")


# Minimal Polars-compatible implementation for Catalog.write_table's duck-typed
# contract (.lazy(), .collect(), .schema, .to_dicts()) without requiring the
# external polars package. Integration tests that exercise the real polars
# package live in test_catalog.py under @pytest.mark.requires_polars.
class DataFrame:
    def __init__(self, rows):
        self._rows = [dict(row) for row in rows]

    @property
    def schema(self):
        if not self._rows:
            return {}
        return {
            key: _infer_fake_dtype(row[key] for row in self._rows)
            for key in self._rows[0]
        }

    def lazy(self):
        return LazyFrame(self)

    def sort(self, column):
        return DataFrame(sorted(self._rows, key=lambda row: row[column]))

    def to_dicts(self):
        return [dict(row) for row in self._rows]

    def write_parquet(self, storage_location, **_kwargs):
        with Path(storage_location).open("w", encoding="utf-8") as fh:
            json.dump(self._rows, fh)


class LazyFrame:
    def __init__(self, df):
        self._df = df

    def collect(self):
        return self._df


@pytest.fixture()
def polars_compatible_module():
    previous = sys.modules.get("polars")
    polars = types.ModuleType("polars")
    polars.DataFrame = DataFrame
    polars.LazyFrame = LazyFrame

    def scan_parquet(storage_location, **_kwargs):
        with Path(storage_location).open("r", encoding="utf-8") as fh:
            return LazyFrame(DataFrame(json.load(fh)))

    def read_parquet(storage_location, **_kwargs):
        return scan_parquet(storage_location).collect()

    polars.scan_parquet = scan_parquet
    polars.read_parquet = read_parquet
    sys.modules["polars"] = polars
    try:
        yield polars
    finally:
        if previous is None:
            sys.modules.pop("polars", None)
        else:
            sys.modules["polars"] = previous


def test_catalog_namespace_table_lifecycle_consistency(tmp_path, catalog_resources):
    catalog_name = catalog_resources.catalog_name
    live_namespace = "analytics"
    archive_namespace = "archive"
    live_table = "events"
    deleted_table = "snapshots"
    archive_table = "events_archive"

    columns = [
        ColumnInfo("id", "INTEGER", 0, False),
        ColumnInfo("name", "TEXT", 1, True),
    ]

    _create_namespace(catalog_resources, live_namespace)
    _create_namespace(catalog_resources, archive_namespace)
    _create_table(
        catalog_resources,
        live_namespace,
        live_table,
        columns,
        str(tmp_path / "events.parquet"),
    )
    _create_table(
        catalog_resources,
        live_namespace,
        deleted_table,
        columns,
        str(tmp_path / "snapshots.parquet"),
    )
    _create_table(
        catalog_resources,
        archive_namespace,
        archive_table,
        columns,
        str(tmp_path / "events_archive.parquet"),
    )

    assert catalog_name in _names(Catalog.list_catalogs())

    namespaces = Catalog.list_namespaces(catalog_name)
    assert _names(namespaces) == {live_namespace, archive_namespace}
    assert {namespace.catalog_name for namespace in namespaces} == {catalog_name}

    assert _names(Catalog.list_tables(catalog_name, live_namespace)) == {
        live_table,
        deleted_table,
    }
    assert _names(Catalog.list_tables(catalog_name, archive_namespace)) == {archive_table}

    table_info = Catalog.get_table_info(catalog_name, live_namespace, live_table)
    assert table_info.catalog_name == catalog_name
    assert table_info.namespace_name == live_namespace
    assert table_info.storage_location == str(tmp_path / "events.parquet")

    Catalog.delete_table(catalog_name, live_namespace, deleted_table)
    assert _names(Catalog.list_tables(catalog_name, live_namespace)) == {live_table}
    with pytest.raises(ValueError) as missing_table:
        Catalog.get_table_info(catalog_name, live_namespace, deleted_table)
    _assert_error_code(missing_table, "ALOPEX-PY003")

    Catalog.delete_namespace(catalog_name, live_namespace)
    assert _names(Catalog.list_namespaces(catalog_name)) == {archive_namespace}
    with pytest.raises(ValueError) as missing_namespace:
        Catalog.list_tables(catalog_name, live_namespace)
    _assert_error_code(missing_namespace, "ALOPEX-PY002")
    with pytest.raises(ValueError) as missing_table_parent:
        Catalog.get_table_info(catalog_name, live_namespace, live_table)
    _assert_error_code(missing_table_parent, "ALOPEX-PY002")

    archive_info = Catalog.get_table_info(catalog_name, archive_namespace, archive_table)
    assert archive_info.name == archive_table
    assert archive_info.namespace_name == archive_namespace

    Catalog.delete_catalog(catalog_name)
    assert catalog_name not in _names(Catalog.list_catalogs())
    with pytest.raises(ValueError) as missing_catalog:
        Catalog.list_namespaces(catalog_name)
    _assert_error_code(missing_catalog, "ALOPEX-PY001")
    with pytest.raises(ValueError) as missing_catalog_parent:
        Catalog.list_tables(catalog_name, archive_namespace)
    _assert_error_code(missing_catalog_parent, "ALOPEX-PY001")


def test_dataframe_write_matches_core_metadata_and_storage(
    tmp_path,
    catalog_resources,
    polars_compatible_module,
):
    expected_rows = sorted(_load_expected()["sql_rows"], key=lambda row: row["id"])
    catalog_name = catalog_resources.catalog_name
    namespace = "dataframe_api"
    table_name = "shared_rows"
    storage_location = str(tmp_path / "shared_rows.parquet")

    _create_namespace(catalog_resources, namespace)
    df = polars_compatible_module.DataFrame(expected_rows)

    Catalog.write_table(
        df.lazy(),
        catalog_name,
        namespace,
        table_name,
        delta_mode="overwrite",
        storage_location=storage_location,
    )
    catalog_resources.tables.append((namespace, table_name))

    table_info = Catalog.get_table_info(catalog_name, namespace, table_name)
    listed_tables = Catalog.list_tables(catalog_name, namespace)

    assert _names(listed_tables) == {table_name}
    assert table_info.catalog_name == catalog_name
    assert table_info.namespace_name == namespace
    assert table_info.storage_location == storage_location
    assert table_info.data_source_format == "PARQUET"
    assert [
        (column.name, column.type_name, column.position, column.nullable)
        for column in table_info.columns
    ] == [
        ("id", "BIGINT", 0, True),
        ("name", "TEXT", 1, True),
    ]

    stored_rows = (
        polars_compatible_module.read_parquet(storage_location).sort("id").to_dicts()
    )

    assert stored_rows == expected_rows
