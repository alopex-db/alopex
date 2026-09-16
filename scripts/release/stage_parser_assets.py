#!/usr/bin/env python3
"""Stage verified native parser archives into a crate source tree."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
import tarfile
from typing import Any


TARGET_LIBRARIES = {
    "aarch64-apple-darwin": ("libalopex_sql_parser.dylib", "libalopex_sql_parser.a"),
    "x86_64-apple-darwin": ("libalopex_sql_parser.dylib", "libalopex_sql_parser.a"),
    "x86_64-pc-windows-msvc": ("alopex_sql_parser.dll", "alopex_sql_parser.lib"),
    "x86_64-unknown-linux-gnu": ("libalopex_sql_parser.so", "libalopex_sql_parser.a"),
}


def read_regular(path: Path, description: str) -> bytes:
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{description} must be a regular file: {path}")
    return path.read_bytes()


def identity(value: Any, target: str, name: str) -> tuple[str, str, int]:
    if not isinstance(value, dict):
        raise ValueError(f"{target} {name} identity is invalid")
    path = value.get("path")
    digest = value.get("sha256")
    size = value.get("size")
    if (
        not isinstance(path, str)
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size <= 0
    ):
        raise ValueError(f"{target} {name} identity is invalid")
    parsed = PurePosixPath(path)
    if parsed.is_absolute() or ".." in parsed.parts or parsed.as_posix() != path:
        raise ValueError(f"{target} {name} path is unsafe")
    return path, digest, size


def read_archive_member(
    archive: tarfile.TarFile, path: str, digest: str, size: int, description: str
) -> tuple[str, bytes]:
    try:
        member = archive.getmember(path)
    except KeyError as error:
        raise ValueError(f"missing {description}: {path}") from error
    if not member.isfile() or member.size != size:
        raise ValueError(f"invalid {description}: {path}")
    source = archive.extractfile(member)
    if source is None:
        raise ValueError(f"invalid {description}: {path}")
    content = source.read()
    if len(content) != size or hashlib.sha256(content).hexdigest() != digest:
        raise ValueError(f"{description} digest mismatch: {path}")
    return PurePosixPath(path).name, content


def stage(manifest_path: Path, asset_dir: Path, vendor_dir: Path) -> None:
    manifest_bytes = read_regular(manifest_path, "parser manifest")
    try:
        manifest = json.loads(manifest_bytes)
    except json.JSONDecodeError as error:
        raise ValueError("parser manifest is not JSON") from error
    if not isinstance(manifest, dict):
        raise ValueError("parser manifest is invalid")
    contract = manifest.get("contract_version")
    assets = manifest.get("assets")
    if not isinstance(contract, str) or not contract or not isinstance(assets, list):
        raise ValueError("parser manifest assets are invalid")
    by_target = {asset.get("target"): asset for asset in assets if isinstance(asset, dict)}
    if set(by_target) != set(TARGET_LIBRARIES) or len(assets) != len(by_target):
        raise ValueError("parser manifest assets do not cover the target matrix")

    staged: dict[str, tuple[str, bytes, str, bytes, str, str]] = {}
    for target, (dynamic_expected, static_expected) in TARGET_LIBRARIES.items():
        asset = by_target[target]
        archive_identity = asset.get("archive")
        if not isinstance(archive_identity, dict):
            raise ValueError(f"{target} archive identity is invalid")
        archive_name = archive_identity.get("filename")
        archive_digest = archive_identity.get("sha256")
        archive_size = archive_identity.get("size")
        if (
            not isinstance(archive_name, str)
            or Path(archive_name).name != archive_name
            or not isinstance(archive_digest, str)
            or len(archive_digest) != 64
            or isinstance(archive_size, bool)
            or not isinstance(archive_size, int)
            or archive_size <= 0
        ):
            raise ValueError(f"{target} archive identity is invalid")
        archive_path = asset_dir / archive_name
        archive_bytes = read_regular(archive_path, f"{target} archive")
        if len(archive_bytes) != archive_size or hashlib.sha256(archive_bytes).hexdigest() != archive_digest:
            raise ValueError(f"{target} archive digest mismatch")
        dynamic_path, dynamic_digest, dynamic_size = identity(asset.get("library"), target, "library")
        static_path, static_digest, static_size = identity(asset.get("static_library"), target, "static library")
        expected_prefix = f"alopex-sql-parser/{target}/"
        if dynamic_path != expected_prefix + dynamic_expected or static_path != expected_prefix + static_expected:
            raise ValueError(f"{target} library path does not match the target contract")
        with tarfile.open(archive_path, "r:gz") as archive:
            dynamic_name, dynamic = read_archive_member(
                archive, dynamic_path, dynamic_digest, dynamic_size, f"{target} library"
            )
            static_name, static_library = read_archive_member(
                archive, static_path, static_digest, static_size, f"{target} static library"
            )
        staged[target] = (
            dynamic_name,
            dynamic,
            static_name,
            static_library,
            dynamic_digest,
            static_digest,
        )

    fixed_manifest = vendor_dir / "parser-vendor-manifest.json"
    versioned_manifests = sorted(vendor_dir.glob("parser-vendor-manifest-v*.json"))
    if not fixed_manifest.exists() and not versioned_manifests:
        destination_manifest = fixed_manifest
    elif fixed_manifest.is_file() and not fixed_manifest.is_symlink() and not versioned_manifests:
        destination_manifest = fixed_manifest
    elif not fixed_manifest.exists() and len(versioned_manifests) == 1:
        destination_manifest = versioned_manifests[0]
    else:
        raise ValueError("unknown or ambiguous parser vendor manifest layout")

    vendor_dir.mkdir(parents=True, exist_ok=True)
    for target, (dynamic_name, dynamic, static_name, static_library, dynamic_digest, static_digest) in staged.items():
        target_dir = vendor_dir / target
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / dynamic_name).write_bytes(dynamic)
        (target_dir / static_name).write_bytes(static_library)
        (target_dir / "CONTRACT_VERSION").write_text(f"{contract}\n", encoding="ascii")
        (target_dir / "SHA256SUMS").write_text(
            f"{dynamic_digest}  {dynamic_name}\n{static_digest}  {static_name}\n",
            encoding="ascii",
        )
    destination_manifest.write_bytes(manifest_bytes)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--asset-dir", required=True, type=Path)
    parser.add_argument("--vendor-dir", required=True, type=Path)
    args = parser.parse_args()
    stage(args.manifest, args.asset_dir, args.vendor_dir)


if __name__ == "__main__":
    main()
