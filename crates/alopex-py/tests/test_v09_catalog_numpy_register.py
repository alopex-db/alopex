from __future__ import annotations

import re
from pathlib import Path

import pytest

from alopex import Database

CRATE_ROOT = Path(__file__).resolve().parents[1]
CATALOG_SOURCE = (CRATE_ROOT / "src/catalog/client.rs").read_text(encoding="utf-8")
VECTOR_SOURCE = (CRATE_ROOT / "src/vector.rs").read_text(encoding="utf-8")

CATALOG_METHODS = (
    "list_catalogs", "list_namespaces", "list_tables", "get_table_info",
    "create_catalog", "delete_catalog", "create_namespace", "delete_namespace",
    "create_table", "delete_table", "scan_table", "write_table",
    "create_table_from_dataframe", "write_parquet_append", "write_parquet_overwrite",
    "write_table_merge",
)
NUMPY_METHODS = (
    "require_numpy", "with_ndarray_f32", "with_ndarray_f32_gil_safe",
    "owned_vec_to_ndarray", "owned_to_ndarray", "vec_to_ndarray_opt",
    "vec_to_ndarray_copy", "vec_to_ndarray_opt_copy", "vec_to_ndarray",
)


def _has_rust_function(source: str, name: str) -> bool:
    return re.search(
        rf"^\s*(?:pub(?:\(crate\))?\s+)?fn\s+{re.escape(name)}\s*(?:<|\()",
        source,
        re.MULTILINE,
    ) is not None


def test_i23a_catalog_and_numpy_register_has_individual_availability_rows() -> None:
    assert len(CATALOG_METHODS) == 16
    assert len(NUMPY_METHODS) == 9
    assert "impl PyCatalog" in CATALOG_SOURCE
    for method in CATALOG_METHODS:
        assert _has_rust_function(CATALOG_SOURCE, method), f"missing Catalog.{method}"

    # NumPy methods must retain their feature gate: a build without NumPy must
    # reject conversion instead of silently exposing a different representation.
    assert '#[cfg(feature = "numpy")]' in VECTOR_SOURCE
    assert "numpy support is not enabled" in VECTOR_SOURCE
    for method in NUMPY_METHODS:
        assert _has_rust_function(VECTOR_SOURCE, method), f"missing NumPy conversion {method}"


def test_i23a_numpy_disabled_build_keeps_the_api_and_rejects_explicitly() -> None:
    database = Database.new()
    transaction = database.begin()
    unavailable_calls = (
        lambda: database.create_hnsw_index("v09_numpy_gate", object()),
        lambda: database.search_hnsw("v09_numpy_gate", None, 1),
        lambda: database.drop_hnsw_index("v09_numpy_gate"),
        lambda: database.get_hnsw_stats("v09_numpy_gate"),
        lambda: transaction.upsert_vector(b"key", None, None, None),
        lambda: transaction.search_similar(None, None, 1),
        lambda: transaction.upsert_to_hnsw("v09_numpy_gate", b"key", None),
        lambda: transaction.delete_from_hnsw("v09_numpy_gate", b"key"),
        lambda: transaction.get_vector(b"key", None),
    )

    for unavailable_call in unavailable_calls:
        with pytest.raises(Exception, match="numpy support is not enabled"):
            unavailable_call()
