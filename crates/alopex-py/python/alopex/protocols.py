"""Structural types shared by the embedded and server surfaces.

``alopex.Database`` is a pyo3 ``#[pyclass]``, so it cannot inherit a Python base
class; a common abstract base is physically impossible.  The shared calling
convention is therefore expressed as :mod:`typing` protocols and checked
structurally (D4).  Only the surface that both implementations really provide is
declared here: stream, HNSW, KV, and diagnostics APIs exist on exactly one side
and are deliberately left out.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Sequence, Union, runtime_checkable

SqlResult = Union[List[Dict[str, Any]], int, None]


@runtime_checkable
class TransactionLike(Protocol):
    """Transaction surface shared by ``Transaction`` and ``RemoteTransaction``."""

    def execute_sql(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> SqlResult: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def __enter__(self) -> "TransactionLike": ...

    def __exit__(
        self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]
    ) -> bool: ...


@runtime_checkable
class DatabaseLike(Protocol):
    """Database surface shared by ``Database`` and ``RemoteDatabase``."""

    def execute_sql(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> SqlResult: ...

    def begin(self, mode: Optional[Any] = None) -> TransactionLike: ...

    def close(self) -> None: ...


__all__ = ["DatabaseLike", "TransactionLike", "SqlResult"]
