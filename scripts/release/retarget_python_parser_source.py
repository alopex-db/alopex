#!/usr/bin/env python3
"""Bind historical Python wheel source to a published core parser manifest."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re


EXPECTED_SCHEMA = "alopex-parser-vendor-manifest-v1"
EXPECTED_CONTRACT = "0.4.0"
EXPECTED_TARGETS = {
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
}
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def _validated_manifest(source: Path) -> tuple[bytes, str]:
    raw = source.read_bytes()
    manifest = json.loads(raw)
    version = manifest.get("alopex_version")
    if not isinstance(version, str) or not SEMVER.fullmatch(version):
        raise ValueError("parser manifest has an invalid Alopex version")
    if manifest.get("schema") != EXPECTED_SCHEMA:
        raise ValueError("parser manifest schema mismatch")
    if manifest.get("contract_version") != EXPECTED_CONTRACT:
        raise ValueError("parser manifest contract mismatch")

    assets = manifest.get("assets")
    if not isinstance(assets, list):
        raise ValueError("parser manifest assets must be a list")
    targets = [asset.get("target") for asset in assets if isinstance(asset, dict)]
    if len(targets) != len(EXPECTED_TARGETS) or set(targets) != EXPECTED_TARGETS:
        raise ValueError("parser manifest target matrix is incomplete or duplicated")
    for asset in assets:
        library = asset.get("library")
        if not isinstance(library, dict) or not SHA256.fullmatch(
            str(library.get("sha256", ""))
        ):
            raise ValueError(f"invalid library digest for {asset.get('target')}")
        if not isinstance(library.get("size"), int) or library["size"] <= 0:
            raise ValueError(f"invalid library size for {asset.get('target')}")
    return raw, version


def _retargeted_build_support(text: str, version: str, digest: str) -> str:
    text, version_count = re.subn(
        r'(REQUIRED_ALOPEX_VERSION: &str = ")[^"]+(";)',
        rf"\g<1>{version}\g<2>",
        text,
    )
    text, digest_count = re.subn(
        r'(VENDOR_MANIFEST_SHA256: &str =\s*")[0-9a-f]{64}(";)',
        rf"\g<1>{digest}\g<2>",
        text,
    )
    if version_count != 1 or digest_count != 1:
        raise ValueError("build_support.rs parser pin layout changed")
    return text


def _atomic_write(path: Path, content: bytes) -> None:
    temporary = path.with_name(f".{path.name}.retarget.tmp")
    temporary.write_bytes(content)
    os.replace(temporary, path)


def retarget(source: Path, destination: Path, build_support: Path) -> None:
    raw, version = _validated_manifest(source)
    digest = hashlib.sha256(raw).hexdigest()
    updated = _retargeted_build_support(
        build_support.read_text(encoding="utf-8"), version, digest
    ).encode()

    destination.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(destination, raw)
    _atomic_write(build_support, updated)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--vendor-manifest", type=Path, required=True)
    parser.add_argument("--build-support", type=Path, required=True)
    args = parser.parse_args()
    retarget(args.manifest, args.vendor_manifest, args.build_support)


if __name__ == "__main__":
    main()
