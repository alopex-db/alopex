from __future__ import annotations

import re
from pathlib import Path


CRATE_ROOT = Path(__file__).resolve().parents[1]
DATAFRAME_SOURCE = (CRATE_ROOT / "src/types/dataframe.rs").read_text(encoding="utf-8")
STUB_SOURCE = (CRATE_ROOT / "python/alopex/_alopex.pyi").read_text(encoding="utf-8")


# Each approved I-23b entry is one source-to-public-Python row. `py_new` is
# deliberately exposed as DataFrame.__init__ by PyO3, so the mapping is kept
# explicit rather than treating the constructor as an unverified exception.
I23B_ROWS: tuple[tuple[str, str, str, str], ...] = (
    *(("PyDataFrame", "DataFrame", method, "__init__" if method == "py_new" else method) for method in (
        "py_new", "from_columns", "height", "width", "to_dict", "str", "dt", "list", "explode", "implode", "lazy",
    )),
    *(("PyExpr", "Expr", method, method) for method in (
        "alias", "add", "sub", "mul", "div", "eq", "neq", "gt", "lt", "ge", "le", "and_", "or_", "not_",
    )),
    *(("PyLazyFrame", "LazyFrame", method, method) for method in (
        "scan_csv", "scan_parquet", "from_dataframe", "concat", "select", "filter", "with_columns", "collect",
    )),
    *(("PyDataFrameStream", "DataFrameStream", method, method) for method in (
        "__iter__", "__next__", "close", "cancel", "status", "__enter__", "__exit__",
    )),
    *(("PyStringNamespace", "StringNamespace", method, method) for method in (
        "to_lowercase", "to_uppercase", "contains", "replace", "strip_chars", "split", "len_chars", "extract",
    )),
    *(("PyDatetimeNamespace", "DatetimeNamespace", method, method) for method in (
        "year", "month", "day", "weekday", "to_string", "convert_time_zone",
    )),
    *(("PyListNamespace", "ListNamespace", method, method) for method in (
        "join", "len", "contains",
    )),
)


def _pymethods_body(owner: str) -> str:
    owner_start = DATAFRAME_SOURCE.find(f"#[pymethods]\nimpl {owner} {{")
    assert owner_start >= 0, f"missing PyO3 binding implementation for {owner}"
    body_start = DATAFRAME_SOURCE.find("{", owner_start)
    depth = 0
    for offset, character in enumerate(DATAFRAME_SOURCE[body_start:], start=body_start):
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return DATAFRAME_SOURCE[body_start + 1 : offset]
    raise AssertionError(f"unterminated PyO3 binding implementation for {owner}")


def _has_rust_method(owner: str, method: str) -> bool:
    return re.search(
        rf"^\s*(?:pub(?:\(crate\))?\s+)?fn\s+{re.escape(method)}\s*(?:<|\()",
        _pymethods_body(owner),
        re.MULTILINE,
    ) is not None


def _stub_class_body(name: str) -> str:
    match = re.search(
        rf"^class {re.escape(name)}(?:\([^\n]+\))?:\n(?P<body>.*?)(?=^class |\Z)",
        STUB_SOURCE,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"missing public stub class {name}"
    return match.group("body")


def test_i23b_dataframe_and_namespace_register_has_individual_binding_rows() -> None:
    assert len(I23B_ROWS) == 57
    for rust_owner, python_owner, rust_method, python_method in I23B_ROWS:
        assert _has_rust_method(rust_owner, rust_method), (
            f"missing I-23b binding {rust_owner}.{rust_method}"
        )
        assert re.search(
            rf"^\s+def\s+{re.escape(python_method)}\s*\(",
            _stub_class_body(python_owner),
            re.MULTILINE,
        ) is not None, f"missing public stub {python_owner}.{python_method}"


def test_i23b_dataframe_stream_retains_explicit_local_status_boundary() -> None:
    # A local DataFrame stream must state its terminal status and scope rather
    # than implying distributed transaction behavior when no such capability
    # is available through this Python surface.
    assert 'status.set_item("resource_scope", "dataframe_batch")' in DATAFRAME_SOURCE
    assert 'status.set_item("transaction_effect", "none")' in DATAFRAME_SOURCE
    assert "self.control.ensure_open()?" in DATAFRAME_SOURCE
