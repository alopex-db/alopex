from __future__ import annotations

import re
from pathlib import Path


CRATE_ROOT = Path(__file__).resolve().parents[1]
DATABASE_SOURCE = (CRATE_ROOT / "src/embedded/database.rs").read_text(encoding="utf-8")
TRANSACTION_SOURCE = (CRATE_ROOT / "src/embedded/transaction.rs").read_text(encoding="utf-8")
STREAM_SOURCE = (CRATE_ROOT / "src/embedded/stream.rs").read_text(encoding="utf-8")
LOCAL_SCAN_SOURCE = (CRATE_ROOT / "src/embedded/local_scan.rs").read_text(encoding="utf-8")


# Each approved I-22 method is intentionally represented as one row.  This is
# a source-level binding register: the Rust method name remains authoritative
# even where PyO3 deliberately exposes an internal helper with a leading `_`.
I22_ROWS: tuple[tuple[str, str, str], ...] = (
    *(("PyDatabase", method, DATABASE_SOURCE) for method in (
        "open", "new", "open_in_memory", "open_with_config", "thread_mode",
        "execute_sql", "execute_sql_stream", "open_native_async_sql_stream",
        "open_native_async_query_stream", "query_stream", "begin", "flush",
        "memory_usage", "cluster_status", "routing_diagnostics", "create_counter", "read_counter", "close",
        "create_hnsw_index", "search_hnsw", "drop_hnsw_index", "get_hnsw_stats",
    )),
    *(("PyTransaction", method, TRANSACTION_SOURCE) for method in (
        "status", "is_active", "state_name", "get", "put", "delete",
        "upsert_vector", "search_similar", "upsert_to_hnsw", "delete_from_hnsw",
        "get_vector", "execute_sql", "open_native_async_sql_stream",
        "open_native_async_query_stream", "commit", "rollback", "__enter__", "__exit__",
    )),
    *(("PySqlResultStream", method, STREAM_SOURCE) for method in (
        "__iter__", "__next__", "cancel", "status", "__enter__", "__exit__",
    )),
    *(("PyLocalScan", method, LOCAL_SCAN_SOURCE) for method in (
        "table", "csv", "parquet", "columnar_segment", "lazyframe",
    )),
)


def _has_binding_method(source: str, method: str) -> bool:
    pattern = re.compile(
        rf"^\s*(?:pub(?:\(crate\))?\s+)?fn\s+{re.escape(method)}\s*(?:<|\()",
        re.MULTILINE,
    )
    return pattern.search(source) is not None


def test_i22_python_method_register_has_one_binding_row_per_requirement() -> None:
    assert len(I22_ROWS) == 51
    for owner, method, source in I22_ROWS:
        assert f"impl {owner}" in source, f"missing binding implementation for {owner}"
        assert _has_binding_method(source, method), f"missing I-22 binding {owner}.{method}"
