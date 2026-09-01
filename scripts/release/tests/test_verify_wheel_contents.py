from __future__ import annotations

import sys
import hashlib
import tarfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from verify_wheel_contents import EXPECTED_DLL, verify


def _metadata_files(library: bytes = b"dll") -> dict[str, bytes]:
    digest = hashlib.sha256(library).hexdigest()
    return {
        "alopex/native/CONTRACT_VERSION": b"0.20.0\n",
        "alopex/native/SHA256SUMS": f"{digest}  alopex_sql_parser.dll\n".encode(),
        EXPECTED_DLL: library,
    }


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


def test_windows_wheel_checks_archive_and_internal_digests(tmp_path: Path) -> None:
    archive = tmp_path / "alopex-0.8.4-cp311-win_amd64.whl"
    files = _metadata_files(b"reviewed-dll")
    with zipfile.ZipFile(archive, "w") as handle:
        for name, content in files.items():
            handle.writestr(name, content)
    verify(
        archive,
        require_dll=True,
        expected_archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
        expected_library_sha256=hashlib.sha256(b"reviewed-dll").hexdigest(),
        expected_contract_version="0.20.0",
        expected_loader_path=EXPECTED_DLL,
    )


def test_verifier_rejects_duplicate_or_wrong_loader_path(tmp_path: Path) -> None:
    archive = tmp_path / "alopex-0.8.4-cp311-win_amd64.whl"
    files = _metadata_files()
    with zipfile.ZipFile(archive, "w") as handle:
        for name, content in files.items():
            handle.writestr(name, content)
        handle.writestr(EXPECTED_DLL, b"duplicate")
    try:
        verify(archive, require_dll=True, expected_loader_path="alopex/native/missing.dll")
    except AssertionError as error:
        assert "duplicate" in str(error).lower() or "loader" in str(error).lower()
    else:
        raise AssertionError("duplicate native entries must be rejected")


def test_verifier_rejects_contract_or_internal_digest_mismatch(tmp_path: Path) -> None:
    archive = tmp_path / "alopex-0.8.4-cp311-win_amd64.whl"
    files = _metadata_files()
    files["alopex/native/CONTRACT_VERSION"] = b"0.7.5\n"
    with zipfile.ZipFile(archive, "w") as handle:
        for name, content in files.items():
            handle.writestr(name, content)
    try:
        verify(archive, require_dll=True, expected_contract_version="0.20.0")
    except AssertionError as error:
        assert "contract" in str(error).lower()
    else:
        raise AssertionError("contract drift must be rejected")
