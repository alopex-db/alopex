from pathlib import Path


PACKAGE_INIT = Path(__file__).resolve().parents[1] / "python/alopex/__init__.py"


def test_windows_native_discovery_is_package_relative_only():
    source = PACKAGE_INIT.read_text(encoding="utf-8")
    assert "ALOPEX_DLL_DIR" not in source
    assert "os.environ" not in source
    assert "Path(__file__).resolve().parent / \"native\"" in source
    assert "missing its package-local native assets" in source

