import importlib.util

import pytest


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def pytest_configure(config):
    config.addinivalue_line("markers", "large: time-consuming benchmarks (excluded in CI)")
    config.addinivalue_line("markers", "requires_pytest_benchmark: pytest-benchmark が必要なテスト")


def pytest_runtest_setup(item):
    if "requires_pytest_benchmark" in item.keywords:
        if not _module_available("pytest_benchmark"):
            pytest.skip("pytest-benchmark が未インストールのためスキップ")

