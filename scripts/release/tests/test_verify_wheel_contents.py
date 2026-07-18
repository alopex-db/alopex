from __future__ import annotations

import sys
import tarfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from verify_wheel_contents import EXPECTED_DLL, verify


def test_windows_wheel_requires_package_local_dll(tmp_path: Path) -> None:
    archive = tmp_path / "alopex-0.7.5-cp311-win_amd64.whl"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("alopex/__init__.py", "")
        handle.writestr(EXPECTED_DLL, b"dll")

    verify(archive, require_dll=True)


def test_non_windows_wheel_rejects_dll(tmp_path: Path) -> None:
    archive = tmp_path / "alopex-0.7.5-cp311-manylinux.whl"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr(EXPECTED_DLL, b"dll")

    try:
        verify(archive, require_dll=False)
    except AssertionError as error:
        assert "DLLs must not be present" in str(error)
    else:
        raise AssertionError("a non-Windows wheel with a DLL must be rejected")


def test_sdist_rejects_dll(tmp_path: Path) -> None:
    archive = tmp_path / "alopex-0.7.5.tar.gz"
    with tarfile.open(archive, "w:gz") as handle:
        info = tarfile.TarInfo(EXPECTED_DLL)
        info.size = 3
        handle.addfile(info, __import__("io").BytesIO(b"dll"))

    try:
        verify(archive, require_dll=False)
    except AssertionError as error:
        assert "DLLs must not be present" in str(error)
    else:
        raise AssertionError("an sdist with a DLL must be rejected")
