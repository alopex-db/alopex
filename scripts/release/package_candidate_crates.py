#!/usr/bin/env python3
"""Package the release crates once against a local index of prior candidate crates."""

from __future__ import annotations

import argparse
import hashlib
import http.server
import json
import shutil
import subprocess
import sys
import threading
import tomllib
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit
from urllib.request import urlopen


PUBLISHED_CRATES = (
    "alopex-core",
    "alopex-sql",
    "alopex-dataframe",
    "alopex-cluster",
    "alopex-embedded",
    "alopex-server",
    "alopex-cli",
)
INDEX_UPSTREAM = "https://index.crates.io"


def index_path(crate: str) -> Path:
    name = crate.lower()
    if len(name) == 1:
        return Path("1") / name
    if len(name) == 2:
        return Path("2") / name
    if len(name) == 3:
        return Path("3") / name[0] / name
    return Path(name[:2]) / name[2:4] / name


def index_record(metadata: dict[str, Any], archive: Path) -> dict[str, Any]:
    dependencies = []
    for dependency in metadata["deps"]:
        name = dependency["explicit_name_in_toml"] or dependency["name"]
        entry = {
            "name": name,
            "req": dependency["version_req"],
            "features": dependency["features"],
            "optional": dependency["optional"],
            "default_features": dependency["default_features"],
            "target": dependency["target"],
            "kind": dependency["kind"],
            "registry": dependency["registry"],
        }
        if dependency["explicit_name_in_toml"] is not None:
            entry["package"] = dependency["name"]
        dependencies.append(entry)
    return {
        "name": metadata["name"],
        "vers": metadata["vers"],
        "deps": dependencies,
        "cksum": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "features": metadata["features"],
        "yanked": False,
        "links": metadata["links"],
        "rust_version": metadata["rust_version"],
    }


class SparseIndex:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def add(self, metadata: dict[str, Any], archive: Path) -> None:
        relative = index_path(metadata["name"])
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(self.upstream(relative.as_posix()))
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(index_record(metadata, archive)) + "\n")

    def read(self, raw_path: str) -> bytes | None:
        path = PurePosixPath(raw_path)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("unsafe sparse index path")
        candidate = self.root.joinpath(*path.parts)
        return candidate.read_bytes() if candidate.is_file() else None

    def upstream(self, raw_path: str) -> bytes:
        path = PurePosixPath(raw_path)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("unsafe sparse index path")
        cache = self.root / ".upstream-cache" / path
        if cache.is_file():
            return cache.read_bytes()
        with urlopen(f"{INDEX_UPSTREAM}/{path}") as response:
            payload = response.read()
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(payload)
        return payload


def preload_locked_index(lockfile: Path, index: SparseIndex) -> None:
    with lockfile.open("rb") as stream:
        lock = tomllib.load(stream)
    for package in lock["package"]:
        if str(package.get("source", "")).startswith("registry+"):
            index.upstream(index_path(package["name"]).as_posix())


def _handler(index: SparseIndex):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = unquote(urlsplit(self.path).path).lstrip("/")
            try:
                if path == "config.json":
                    payload = json.dumps(
                        {"dl": "https://crates.io/api/v1/crates", "api": "https://crates.io"}
                    ).encode("utf-8")
                else:
                    payload = index.read(path)
                    if payload is None:
                        payload = index.upstream(path)
            except ValueError:
                self.send_error(400)
                return
            except Exception as error:  # pragma: no cover - depends on upstream I/O
                self.send_error(502, str(error))
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return Handler


class CandidateRegistry:
    def __init__(self, index: SparseIndex) -> None:
        self.server = http.server.HTTPServer(("127.0.0.1", 0), _handler(index))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def source_config(self) -> str:
        return f'source.candidate.registry="sparse+http://127.0.0.1:{self.server.server_port}/"'

    def __enter__(self) -> CandidateRegistry:
        self.thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


def package_candidates(manifest: Path, output: Path, target_dir: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    index = SparseIndex(output / ".candidate-index")
    preload_locked_index(manifest.parent / "Cargo.lock", index)
    metadata_tool = Path(__file__).with_name("publish_crate_archive.py")
    with CandidateRegistry(index) as registry:
        for crate in PUBLISHED_CRATES:
            subprocess.run(
                [
                    "cargo",
                    "--config",
                    'source.crates-io.replace-with="candidate"',
                    "--config",
                    registry.source_config,
                    "package",
                    "--manifest-path",
                    str(manifest),
                    "-p",
                    crate,
                    "--allow-dirty",
                    "--no-verify",
                    "--target-dir",
                    str(target_dir),
                ],
                check=True,
            )
            metadata_path = output / f"{crate}-publish-metadata.json"
            subprocess.run(
                [
                    sys.executable,
                    str(metadata_tool),
                    "metadata",
                    "--manifest",
                    str(manifest),
                    "--crate",
                    crate,
                    "--output",
                    str(metadata_path),
                ],
                check=True,
            )
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            archive = target_dir / "package" / f"{crate}-{metadata['vers']}.crate"
            if not archive.is_file():
                raise FileNotFoundError(f"Cargo did not create {archive.name}")
            shutil.copy2(archive, output / archive.name)
            index.add(metadata, archive)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    args = parser.parse_args()
    package_candidates(args.manifest, args.output, args.target_dir)


if __name__ == "__main__":
    main()
