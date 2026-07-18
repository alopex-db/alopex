import os
import sys
from pathlib import Path

# NOTE: mypy の platform 分岐解析はエイリアスなしの `sys.platform` のみ認識する。
if sys.platform == "win32":
    # Windows: Python 3.8+ (bpo-36085) は拡張モジュールの依存 DLL 解決に PATH を
    # 使わない。まず開発時の ALOPEX_DLL_DIR を登録し、配布wheelでは package-local
    # native/ を登録する。add_dll_directory() の戻り値はハンドルを保持しないと登録が
    # 解除されるため、モジュール寿命まで保持する。
    _DLL_DIRECTORY_HANDLES = []

    for _dll_dir in filter(None, os.environ.get("ALOPEX_DLL_DIR", "").split(os.pathsep)):
        # 環境変数の既存挙動を維持し、存在しない明示指定は設定ミスとして失敗させる。
        _DLL_DIRECTORY_HANDLES.append(os.add_dll_directory(_dll_dir))

    _package_dll_dir = Path(__file__).resolve().parent / "native"
    if _package_dll_dir.is_dir():
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
