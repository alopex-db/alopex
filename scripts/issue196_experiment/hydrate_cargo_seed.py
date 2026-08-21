#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import tomllib
from typing import Callable
from urllib.request import urlopen


def locked_registry_archives(lockfile: Path) -> list[tuple[str, str, str]]:
    lock = tomllib.loads(lockfile.read_text(encoding="utf-8"))
    return sorted(
        (package["name"], package["version"], package["checksum"])
        for package in lock.get("package", [])
        if str(package.get("source", "")).startswith("registry+")
    )


def hydrate_archive(
    cache_bucket: Path,
    package: tuple[str, str, str],
    opener: Callable[..., object] = urlopen,
) -> bool:
    name, version, expected_checksum = package
    filename = f"{name}-{version}.crate"
    destination = cache_bucket / filename
    if destination.is_file():
        digest = hashlib.sha256(destination.read_bytes()).hexdigest()
        if digest == expected_checksum:
            return False
        raise RuntimeError(f"cached archive checksum mismatch: {filename}")
    url = f"https://static.crates.io/crates/{name}/{filename}"
    with opener(url, timeout=120) as response:  # type: ignore[attr-defined]
        payload = response.read()  # type: ignore[attr-defined]
    digest = hashlib.sha256(payload).hexdigest()
    if digest != expected_checksum:
        raise RuntimeError(f"downloaded archive checksum mismatch: {filename}")
    partial = destination.with_suffix(destination.suffix + ".partial")
    partial.write_bytes(payload)
    partial.replace(destination)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Hydrate a task-owned Cargo cache from Cargo.lock.")
    parser.add_argument("--lockfile", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    args = parser.parse_args()

    buckets = sorted(path for path in args.cache_dir.iterdir() if path.is_dir())
    if len(buckets) != 1:
        raise SystemExit(f"expected one crates.io cache bucket, found {len(buckets)}")
    packages = locked_registry_archives(args.lockfile)
    with ThreadPoolExecutor(max_workers=4) as executor:
        downloaded = sum(executor.map(lambda package: hydrate_archive(buckets[0], package), packages))
    print(f"Cargo seed hydrated: {downloaded} archives downloaded, {len(packages)} verified")


if __name__ == "__main__":
    main()
