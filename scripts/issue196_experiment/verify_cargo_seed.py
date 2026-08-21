#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import tomllib


def missing_registry_archives(lockfile: Path, cargo_home: Path) -> list[str]:
    lock = tomllib.loads(lockfile.read_text(encoding="utf-8"))
    expected = {
        f"{package['name']}-{package['version']}.crate"
        for package in lock.get("package", [])
        if str(package.get("source", "")).startswith("registry+")
    }
    available = {
        archive.name
        for archive in (cargo_home / "registry/cache").glob("*/*.crate")
    }
    return sorted(expected - available)


def missing_reachable_archives(
    root: Path,
    cargo_home: Path,
    package_name: str,
    features: list[str],
) -> list[str]:
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--locked",
        "--offline",
        "--filter-platform",
        "x86_64-unknown-linux-gnu",
    ]
    if features:
        command.extend(
            ["--features", ",".join(f"{package_name}/{feature}" for feature in features)]
        )
    environment = os.environ.copy()
    environment["CARGO_HOME"] = str(cargo_home)
    environment["CARGO_NET_OFFLINE"] = "true"
    result = subprocess.run(
        command,
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"offline Cargo metadata failed: {result.stderr.strip()}")
    metadata = json.loads(result.stdout)
    packages = {package["id"]: package for package in metadata["packages"]}
    workspace_members = set(metadata["workspace_members"])
    roots = [
        package_id
        for package_id in workspace_members
        if packages[package_id]["name"] == package_name
    ]
    if len(roots) != 1:
        raise RuntimeError(f"expected one workspace package named {package_name}, found {len(roots)}")
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    reachable: set[str] = set()
    pending = roots[:]
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        pending.extend(dependency["pkg"] for dependency in nodes[package_id]["deps"])
    expected = {
        f"{packages[package_id]['name']}-{packages[package_id]['version']}.crate"
        for package_id in reachable
        if str(packages[package_id].get("source", "")).startswith("registry+")
    }
    available = {
        archive.name
        for archive in (cargo_home / "registry/cache").glob("*/*.crate")
    }
    return sorted(expected - available)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify a credential-free Cargo registry seed.")
    parser.add_argument("--lockfile", type=Path)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--package")
    parser.add_argument("--features", default="")
    parser.add_argument("--cargo-home", type=Path, required=True)
    args = parser.parse_args()

    if args.root is not None and args.package is not None:
        missing = missing_reachable_archives(
            args.root,
            args.cargo_home,
            args.package,
            [feature for feature in args.features.split(",") if feature],
        )
        scope = f"{args.package} reachable dependency graph"
    elif args.lockfile is not None:
        missing = missing_registry_archives(args.lockfile, args.cargo_home)
        scope = "Cargo.lock"
    else:
        parser.error("provide either --lockfile or both --root and --package")
    if missing:
        preview = ", ".join(missing[:10])
        raise SystemExit(f"Cargo seed is missing {len(missing)} locked archives: {preview}")
    print(f"Cargo seed contains every registry archive required by {scope}")


if __name__ == "__main__":
    main()
