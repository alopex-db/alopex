#!/usr/bin/env python3
"""Create and deliver an already-qualified crates.io archive without repackaging it."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import subprocess
import sys
import tarfile
import tomllib
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


CRATES_IO_API = "https://crates.io"


def _metadata_package(manifest: Path, crate: str) -> dict[str, Any]:
    payload = json.loads(
        subprocess.check_output(
            [
                "cargo",
                "metadata",
                "--manifest-path",
                str(manifest),
                "--no-deps",
                "--format-version",
                "1",
            ],
            text=True,
        )
    )
    packages = [package for package in payload["packages"] if package["name"] == crate]
    if len(packages) != 1:
        raise ValueError(f"expected one package named {crate}")
    return packages[0]


def _readme(package: dict[str, Any]) -> str | None:
    readme = package.get("readme")
    if readme is None:
        return None
    path = Path(package["manifest_path"]).parent / readme
    return path.read_text(encoding="utf-8")


def publish_metadata(package: dict[str, Any]) -> dict[str, Any]:
    manifest = Path(package["manifest_path"])
    with manifest.open("rb") as stream:
        manifest_data = tomllib.load(stream)
    package_toml = manifest_data.get("package", {})
    dependencies = []
    for dependency in package["dependencies"]:
        dependencies.append(
            {
                "name": dependency["name"],
                "version_req": dependency["req"],
                "features": dependency["features"],
                "optional": dependency["optional"],
                "default_features": dependency["uses_default_features"],
                "target": dependency["target"],
                "kind": dependency["kind"] or "normal",
                "registry": dependency["registry"],
                "explicit_name_in_toml": dependency["rename"],
            }
        )
    return {
        "name": package["name"],
        "vers": package["version"],
        "deps": dependencies,
        "features": package["features"],
        "authors": package["authors"],
        "description": package["description"],
        "documentation": package["documentation"],
        "homepage": package["homepage"],
        "readme": _readme(package),
        "readme_file": package["readme"],
        "keywords": package["keywords"],
        "categories": package["categories"],
        "license": package["license"],
        "license_file": package["license_file"],
        "repository": package["repository"],
        "badges": package_toml.get("badges", {}),
        "links": package["links"],
        "rust_version": package["rust_version"],
    }


def _archive_identity(archive: Path) -> tuple[str, str]:
    with tarfile.open(archive, "r:gz") as bundle:
        manifests = [member for member in bundle.getmembers() if member.name.endswith("/Cargo.toml")]
        if len(manifests) != 1:
            raise ValueError("crate archive must contain exactly one Cargo.toml")
        content = bundle.extractfile(manifests[0])
        if content is None:
            raise ValueError("crate archive Cargo.toml is not a regular file")
        package = tomllib.loads(content.read().decode("utf-8"))["package"]
    return package["name"], package["version"]


def request_body(metadata: dict[str, Any], archive: Path) -> bytes:
    name, version = _archive_identity(archive)
    if (metadata.get("name"), metadata.get("vers")) != (name, version):
        raise ValueError("crate archive identity does not match publish metadata")
    metadata_bytes = json.dumps(metadata, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    crate_bytes = archive.read_bytes()
    return b"".join(
        (
            struct.pack("<I", len(metadata_bytes)),
            metadata_bytes,
            struct.pack("<I", len(crate_bytes)),
            crate_bytes,
        )
    )


def upload(archive: Path, metadata_path: Path, api: str, token: str) -> None:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    body = request_body(metadata, archive)
    request = Request(
        f"{api.rstrip('/')}/api/v1/crates/new",
        data=body,
        method="PUT",
        headers={
            "Accept": "application/json",
            "Authorization": token,
            "Content-Type": "application/octet-stream",
            "User-Agent": "alopex-release-archive-uploader",
        },
    )
    try:
        with urlopen(request) as response:
            response.read()
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"crates.io upload failed: HTTP {error.code}: {detail}") from error


def verify_published(archive: Path, api: str) -> None:
    name, version = _archive_identity(archive)
    request = Request(
        f"{api.rstrip('/')}/api/v1/crates/{name}/{version}/download",
        headers={"User-Agent": "alopex-release-archive-uploader"},
    )
    with urlopen(request) as response:
        published = response.read()
    if hashlib.sha256(published).digest() != hashlib.sha256(archive.read_bytes()).digest():
        raise ValueError(f"published archive digest mismatch for {name}@{version}")


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    metadata_command = commands.add_parser("metadata")
    metadata_command.add_argument("--manifest", type=Path, required=True)
    metadata_command.add_argument("--crate", required=True)
    metadata_command.add_argument("--output", type=Path, required=True)
    upload_command = commands.add_parser("upload")
    upload_command.add_argument("--archive", type=Path, required=True)
    upload_command.add_argument("--metadata", type=Path, required=True)
    upload_command.add_argument("--api", default=CRATES_IO_API)
    verify_command = commands.add_parser("verify-published")
    verify_command.add_argument("--archive", type=Path, required=True)
    verify_command.add_argument("--api", default=CRATES_IO_API)
    args = parser.parse_args()
    if args.command == "metadata":
        args.output.write_text(
            json.dumps(publish_metadata(_metadata_package(args.manifest, args.crate)), sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif args.command == "upload":
        token = os.environ.get("CARGO_REGISTRY_TOKEN")
        if not token:
            raise SystemExit("CARGO_REGISTRY_TOKEN is required")
        upload(args.archive, args.metadata, args.api, token)
    else:
        verify_published(args.archive, args.api)


if __name__ == "__main__":
    main()
