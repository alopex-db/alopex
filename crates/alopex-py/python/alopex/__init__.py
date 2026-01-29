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
