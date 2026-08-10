#!/usr/bin/env python3
"""Verify that release archives contain only the platform-appropriate Nim DLL."""

from __future__ import annotations

import argparse
import hashlib
import re
import tarfile
import zipfile
from pathlib import Path

EXPECTED_DLL = "alopex/native/alopex_sql_parser.dll"


def _normal_name(name: str) -> str:
    normalized = name.replace("\\", "/")
    if normalized.startswith("/") or ".." in normalized.split("/"):
        raise AssertionError(f"unsafe archive member path: {name}")
    return normalized


def archive_files(archive: Path) -> dict[str, bytes]:
    if archive.name.endswith((".whl", ".zip")):
        with zipfile.ZipFile(archive) as handle:
            files: dict[str, bytes] = {}
            for info in handle.infolist():
                name = _normal_name(info.filename)
                if name in files:
                    raise AssertionError(f"duplicate archive member: {name}")
                if info.is_dir():
                    continue
                files[name] = handle.read(info)
            return files
    if archive.name.endswith((".tar.gz", ".tgz", ".tar")):
        with tarfile.open(archive) as handle:
            files = {}
            for member in handle.getmembers():
                name = _normal_name(member.name)
                if name in files:
                    raise AssertionError(f"duplicate archive member: {name}")
                if not member.isfile():
                    raise AssertionError(f"archive member is not a regular file: {name}")
                extracted = handle.extractfile(member)
                if extracted is None:
                    raise AssertionError(f"could not read archive member: {name}")
                files[name] = extracted.read()
            return files
    raise ValueError(f"unsupported archive type: {archive}")


def archive_names(archive: Path) -> list[str]:
    return list(archive_files(archive))


def verify(
    archive: Path,
    *,
    require_dll: bool,
    expected_archive_sha256: str | None = None,
    expected_library_sha256: str | None = None,
    expected_library_path: str | None = None,
    expected_contract_version: str | None = None,
    expected_target: str | None = None,
    expected_loader_path: str | None = None,
) -> None:
    archive_bytes = archive.read_bytes()
    if expected_archive_sha256 is not None:
        if not re.fullmatch(r"[0-9a-f]{64}", expected_archive_sha256):
            raise AssertionError("expected archive digest must be lowercase SHA-256")
        actual = hashlib.sha256(archive_bytes).hexdigest()
        if actual != expected_archive_sha256:
            raise AssertionError(
                f"archive digest mismatch: expected {expected_archive_sha256}, found {actual}"
            )
    files = archive_files(archive)
    names = list(files)
    dlls = [name for name in names if name.lower().endswith(".dll")]
    if require_dll:
        if EXPECTED_DLL not in names:
            raise AssertionError(f"{EXPECTED_DLL} is missing from {archive}")
        unexpected = [name for name in dlls if name != EXPECTED_DLL]
        if unexpected:
            raise AssertionError(f"unexpected DLLs in {archive}: {unexpected}")
    elif dlls:
        raise AssertionError(f"DLLs must not be present in {archive}: {dlls}")
    if expected_target is not None and expected_target not in archive.name:
        raise AssertionError(f"archive filename does not identify target {expected_target}")
    library_path = _normal_name(expected_library_path or EXPECTED_DLL)
    if expected_library_path is not None and library_path not in files:
        raise AssertionError(f"{library_path} is missing from {archive}")
    if require_dll:
        library_path = EXPECTED_DLL
        library = files[library_path]
        if expected_library_sha256 is not None:
            actual = hashlib.sha256(library).hexdigest()
            if actual != expected_library_sha256:
                raise AssertionError(
                    f"native library digest mismatch: expected {expected_library_sha256}, found {actual}"
                )
        if expected_contract_version is not None:
            contract_path = "alopex/native/CONTRACT_VERSION"
            if contract_path not in files:
                raise AssertionError(f"{contract_path} is missing from {archive}")
            actual_contract = files[contract_path].decode("utf-8").strip()
            if actual_contract != expected_contract_version:
                raise AssertionError(
                    f"contract version mismatch: expected {expected_contract_version}, found {actual_contract}"
                )
            checksum_path = "alopex/native/SHA256SUMS"
            if checksum_path not in files:
                raise AssertionError(f"{checksum_path} is missing from {archive}")
            expected_line = f"{hashlib.sha256(library).hexdigest()}  {Path(library_path).name}"
            if files[checksum_path].decode("utf-8").strip() != expected_line:
                raise AssertionError("internal native library digest record mismatch")
        if expected_loader_path is not None:
            if _normal_name(expected_loader_path) != library_path:
                raise AssertionError(
                    f"loader path must resolve to {library_path}, found {expected_loader_path}"
                )
    elif expected_library_path is not None:
        library = files[library_path]
        if expected_library_sha256 is not None:
            actual = hashlib.sha256(library).hexdigest()
            if actual != expected_library_sha256:
                raise AssertionError(
                    f"native library digest mismatch: expected {expected_library_sha256}, found {actual}"
                )
        if expected_contract_version is not None:
            contract_path = str(Path(library_path).with_name("CONTRACT_VERSION"))
            if contract_path not in files:
                raise AssertionError(f"{contract_path} is missing from {archive}")
            if files[contract_path].decode("utf-8").strip() != expected_contract_version:
                raise AssertionError("contract version mismatch")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument(
        "--require-nim-dll",
        action="store_true",
        help="require the package-local Windows Nim parser DLL",
    )
    parser.add_argument("--expected-archive-sha256")
    parser.add_argument("--expected-library-sha256")
    parser.add_argument("--expected-library-path")
    parser.add_argument("--expected-contract-version")
    parser.add_argument("--expected-target")
    parser.add_argument("--expected-loader-path")
    args = parser.parse_args()
    verify(
        args.archive,
        require_dll=args.require_nim_dll,
        expected_archive_sha256=args.expected_archive_sha256,
        expected_library_sha256=args.expected_library_sha256,
        expected_library_path=args.expected_library_path,
        expected_contract_version=args.expected_contract_version,
        expected_target=args.expected_target,
        expected_loader_path=args.expected_loader_path,
    )
    print(f"wheel contents OK: {args.archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
