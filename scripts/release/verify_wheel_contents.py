#!/usr/bin/env python3
"""Verify that release archives contain only the platform-appropriate Nim DLL."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from pathlib import Path

EXPECTED_DLL = "alopex/native/alopex_sql_parser.dll"


def archive_names(archive: Path) -> list[str]:
    if archive.name.endswith((".whl", ".zip")):
        with zipfile.ZipFile(archive) as handle:
            return [name.replace("\\", "/") for name in handle.namelist()]
    if archive.name.endswith((".tar.gz", ".tgz", ".tar")):
        with tarfile.open(archive) as handle:
            return [member.name.replace("\\", "/") for member in handle.getmembers()]
    raise ValueError(f"unsupported archive type: {archive}")


def verify(archive: Path, *, require_dll: bool) -> None:
    names = archive_names(archive)
    dlls = [name for name in names if name.lower().endswith(".dll")]
    if require_dll:
        if EXPECTED_DLL not in names:
            raise AssertionError(f"{EXPECTED_DLL} is missing from {archive}")
        unexpected = [name for name in dlls if name != EXPECTED_DLL]
        if unexpected:
            raise AssertionError(f"unexpected DLLs in {archive}: {unexpected}")
    elif dlls:
        raise AssertionError(f"DLLs must not be present in {archive}: {dlls}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument(
        "--require-nim-dll",
        action="store_true",
        help="require the package-local Windows Nim parser DLL",
    )
    args = parser.parse_args()
    verify(args.archive, require_dll=args.require_nim_dll)
    print(f"wheel contents OK: {args.archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
