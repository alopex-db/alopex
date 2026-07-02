import pytest

import alopex
from alopex import AlopexError, Catalog, ColumnInfo, Database, TxnMode


EXPECTED_ERROR_CODES = {
    "ALOPEX-PY001",
    "ALOPEX-PY002",
    "ALOPEX-PY003",
    "ALOPEX-PY004",
    "ALOPEX-PY005",
    "ALOPEX-PY006",
    "ALOPEX-PY007",
    "ALOPEX-PY008",
    "ALOPEX-PY009",
    "ALOPEX-PY010",
    "ALOPEX-PY011",
    "ALOPEX-PY012",
    "ALOPEX-PY013",
    "ALOPEX-PY014",
    "ALOPEX-PY101",
    "ALOPEX-PY102",
    "ALOPEX-PY103",
    "ALOPEX-PY104",
    "ALOPEX-PY999",
}


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


def _assert_contract(call, exc_type, code):
    with pytest.raises(exc_type) as raised:
        call()

    assert type(raised.value) is exc_type
    assert raised.value.code == code
    assert str(raised.value)


def test_known_python_error_codes_are_stable():
    assert set(alopex.ALOPEX_ERROR_CODES) == EXPECTED_ERROR_CODES


def test_catalog_not_found_error_contract(unique_name):
    _assert_contract(
        lambda: Catalog.list_namespaces(f"{unique_name}_missing"),
        ValueError,
        "ALOPEX-PY001",
    )


def test_table_not_found_error_contract(unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)
    try:
        _assert_contract(
            lambda: Catalog.get_table_info(catalog_name, namespace_name, "missing_table"),
            ValueError,
            "ALOPEX-PY003",
        )
    finally:
        _cleanup_catalog(catalog_name, namespace_name)


def test_parent_not_found_error_contract(unique_name):
    _assert_contract(
        lambda: Catalog.create_namespace(f"{unique_name}_missing", "child_ns"),
        ValueError,
        "ALOPEX-PY004",
    )


def test_duplicate_catalog_error_contract(unique_name):
    catalog_name = f"{unique_name}_cat"

    Catalog.create_catalog(catalog_name)
    try:
        _assert_contract(
            lambda: Catalog.create_catalog(catalog_name),
            RuntimeError,
            "ALOPEX-PY005",
        )
    finally:
        _cleanup_catalog(catalog_name)


def test_duplicate_namespace_error_contract(unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)
    try:
        _assert_contract(
            lambda: Catalog.create_namespace(catalog_name, namespace_name),
            RuntimeError,
            "ALOPEX-PY006",
        )
    finally:
        _cleanup_catalog(catalog_name, namespace_name)


def test_duplicate_table_error_contract(tmp_path, unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    table_name = f"{unique_name}_tbl"
    storage_location = str(tmp_path / "data.parquet")
    columns = [ColumnInfo("id", "INTEGER", 0, False)]

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)
    Catalog.create_table(
        catalog_name,
        namespace_name,
        table_name,
        columns,
        storage_location,
    )
    try:
        _assert_contract(
            lambda: Catalog.create_table(
                catalog_name,
                namespace_name,
                table_name,
                columns,
                storage_location,
            ),
            AlopexError,
            "ALOPEX-PY007",
        )
    finally:
        _cleanup_catalog(catalog_name, namespace_name, table_name)


def test_unsupported_format_error_contract(unique_name):
    catalog_name = f"{unique_name}_cat"
    namespace_name = f"{unique_name}_ns"
    columns = [ColumnInfo("id", "INTEGER", 0, False)]

    Catalog.create_catalog(catalog_name)
    Catalog.create_namespace(catalog_name, namespace_name)
    try:
        _assert_contract(
            lambda: Catalog.create_table(
                catalog_name,
                namespace_name,
                "bad_format_table",
                columns,
                "/tmp/data.parquet",
                data_source_format="CSV",
            ),
            AlopexError,
            "ALOPEX-PY011",
        )
    finally:
        _cleanup_catalog(catalog_name, namespace_name)


def test_core_error_contract_does_not_depend_on_message():
    db = Database.new()
    txn = db.begin(TxnMode.READ_ONLY)
    try:
        _assert_contract(
            lambda: txn.put(b"key", b"value"),
            AlopexError,
            "ALOPEX-PY103",
        )
    finally:
        db.close()
