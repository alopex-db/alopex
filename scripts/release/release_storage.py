"""Run the shared watchdog with release toolchains and host Docker storage included."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "compatibility"))
import storage


def docker_inventory() -> dict:
    if sys.platform != "linux" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("Docker inventory requires an isolated hosted Linux runner")
    root = json.loads(subprocess.check_output(
        ["docker", "info", "--format", "{{json .DockerRootDir}}"], text=True, timeout=30,
    ))
    # Do not silently omit a new daemon/snapshotter layout. Both traditional
    # Docker and the hosted runner's containerd image store are measured.
    if root != "/var/lib/docker":
        raise RuntimeError(f"unreviewed Docker storage root: {root}")
    roots = [root]
    containerd = subprocess.run(
        ["sudo", "-n", "test", "-d", "/var/lib/containerd"],
        capture_output=True, text=True, timeout=30,
    )
    if containerd.returncode == 0:
        roots.append("/var/lib/containerd")
    elif containerd.returncode != 1 or containerd.stderr:
        raise RuntimeError("cannot determine containerd storage location")
    output = subprocess.check_output(
        ["sudo", "-n", "du", "--summarize", "--one-file-system", "--block-size=1", "--", *roots],
        text=True, timeout=30,
    )
    measured = {}
    for line in output.splitlines():
        size, path = line.split("\t", 1)
        if path not in roots or path in measured or not size.isdecimal():
            raise RuntimeError("invalid Docker allocated-byte measurement")
        measured[path] = int(size)
    if set(measured) != set(roots):
        raise RuntimeError("incomplete Docker allocated-byte measurement")
    return {"allocated_bytes": sum(measured.values()), "roots": measured,
            "method": "host allocated bytes via privileged du; all Docker/containerd data counted conservatively"}


def tool_roots() -> list[Path]:
    roots = [Path(os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup"))),
             Path.home() / ".cache/pip", Path.home() / ".cache/maturin"]
    if os.environ.get("RUNNER_TOOL_CACHE"):
        cache = Path(os.environ["RUNNER_TOOL_CACHE"])
        roots.extend(cache / name for name in ("node", "maturin", "Python"))
    return roots


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--docker", action="store_true")
    options, remaining = parser.parse_known_args(argv)
    original_inventory = storage.inventory

    def inventory(extra=()):
        record = original_inventory([*extra, *tool_roots()])
        if options.docker:
            external = docker_inventory()
            record["docker"] = external
            record["allocated_bytes"] += external["allocated_bytes"]
        return record

    original_argv = sys.argv
    try:
        storage.inventory = inventory
        sys.argv = [original_argv[0], *remaining]
        storage.main()
    finally:
        storage.inventory = original_inventory
        sys.argv = original_argv


if __name__ == "__main__":
    main(sys.argv[1:])
