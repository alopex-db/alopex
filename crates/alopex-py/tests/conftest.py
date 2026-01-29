import importlib.util
import uuid

import pytest

from alopex import Database


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _has_vector_api() -> bool:
    from alopex import Transaction

    return hasattr(Transaction, "upsert_vector")


def pytest_configure(config):
    config.addinivalue_line("markers", "requires_numpy: numpy が必要なテスト")
    config.addinivalue_line("markers", "requires_polars: polars が必要なテスト")


def pytest_runtest_setup(item):
    if "requires_numpy" in item.keywords:
        if not _module_available("numpy") or not _has_vector_api():
            pytest.skip("numpy feature が有効でないためスキップ")
    if "requires_polars" in item.keywords:
        if not _module_available("polars"):
            pytest.skip("polars が未インストールのためスキップ")


@pytest.fixture()
def db():
    db = Database.new()
    yield db
    try:
        db.close()
    except Exception:
        pass


@pytest.fixture()
def unique_name():
    return f"test_{uuid.uuid4().hex}"
