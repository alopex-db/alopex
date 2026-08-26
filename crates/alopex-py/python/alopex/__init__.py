import os
import sys
from pathlib import Path

# NOTE: mypy の platform 分岐解析はエイリアスなしの `sys.platform` のみ認識する。
if sys.platform == "win32":
    # Windows: Python 3.8+ (bpo-36085) は拡張モジュールの依存 DLL 解決に PATH を
    # 使わない。配布 wheel に同梱した package-local native/ だけを登録する。
    # 任意の環境変数や外部ディレクトリを受け入れないことで、ロード対象を
    # wheel の reviewed asset に限定する。add_dll_directory() の戻り値はハンドルを
    # 保持しないと登録が解除されるため、モジュール寿命まで保持する。
    _DLL_DIRECTORY_HANDLES = []
    _package_dll_dir = Path(__file__).resolve().parent / "native"
    if not _package_dll_dir.is_dir():
        raise ImportError("alopex wheel is missing its package-local native assets")
    _DLL_DIRECTORY_HANDLES.append(os.add_dll_directory(str(_package_dll_dir)))

from . import _alopex as _alopex
from ._alopex import catalog as _catalog  # type: ignore[attr-defined]
from ._alopex import database as _database  # type: ignore[attr-defined]
from ._alopex import transaction as _transaction  # type: ignore[attr-defined]
from ._alopex import types as _types  # type: ignore[attr-defined]

_EXCLUDE = {"catalog", "database", "transaction", "types"}
_seen = set()
__all__ = []


def _export_public(module) -> None:
    for _name in dir(module):
        if _name.startswith("_") or _name in _EXCLUDE or _name in _seen:
            continue
        globals()[_name] = getattr(module, _name)
        __all__.append(_name)
        _seen.add(_name)


_export_public(_alopex)
_export_public(_database)
_export_public(_transaction)
_export_public(_types)
_export_public(_catalog)

for _name in ("Catalog", "CatalogInfo", "NamespaceInfo", "TableInfo", "ColumnInfo"):
    globals()[_name] = getattr(_catalog, _name)
    if _name not in _seen:
        __all__.append(_name)
        _seen.add(_name)

# Server client (issue #182). Imported after the native exports so `remote`
# can rely on `alopex._alopex` already being loaded. Both modules use only the
# standard library, so `import alopex` gains no third-party import cost.
from .protocols import DatabaseLike as DatabaseLike  # noqa: E402
from .protocols import TransactionLike as TransactionLike  # noqa: E402
from .remote import RemoteDatabase as RemoteDatabase  # noqa: E402
from .remote import RemoteTransaction as RemoteTransaction  # noqa: E402
from .remote import (  # noqa: E402
    TARGET_MEMORY as _TARGET_MEMORY,
    TARGET_REMOTE as _TARGET_REMOTE,
    resolve_connect_target as _resolve_connect_target,
)


def connect(target=":memory:", **options):
    """Open a database from a connection target, embedded or server.

    The target alone selects the surface; the returned object answers the same
    ``execute_sql`` / ``begin`` / ``close`` calls either way (D3).

    - ``http://host:8080`` / ``https://host:8080`` -> :class:`RemoteDatabase`
      (``**options`` are its keyword arguments, e.g. ``api_key=``).
    - ``:memory:`` (the default) -> in-memory embedded ``Database``.
    - ``file:///path/db`` or a bare path -> embedded ``Database`` on that path.
      Only ``thread_mode`` may be passed for embedded targets.

    Raises:
        NotImplementedError: ``s3://`` targets; the Python bindings do not
            expose the embedded URI opener that the CLI uses (code
            ``ALOPEX-PY204``).
        ValueError: An unusable target or an option the chosen surface does not
            accept (code ``ALOPEX-PY205``).
    """
    kind, value = _resolve_connect_target(target, options)
    if kind == _TARGET_REMOTE:
        return RemoteDatabase(value, **options)
    if kind == _TARGET_MEMORY:
        return _database.Database.open_in_memory(**options)
    return _database.Database.open(value, **options)


for _name in (
    "connect",
    "RemoteDatabase",
    "RemoteTransaction",
    "DatabaseLike",
    "TransactionLike",
):
    if _name not in _seen:
        __all__.append(_name)
        _seen.add(_name)
