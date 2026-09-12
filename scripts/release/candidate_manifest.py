#!/usr/bin/env python3
"""Build and verify the immutable release-candidate artifact manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any


SCHEMA = "alopex-release-candidate-v1"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(version: str, rc_tag: str, commit_sha: str) -> None:
    if VERSION.fullmatch(version) is None:
        raise ValueError("version must be X.Y.Z")
    if re.fullmatch(rf"v{re.escape(version)}-rc\.[1-9][0-9]*", rc_tag) is None:
        raise ValueError("RC tag must match the release version and have a positive number")
    if COMMIT.fullmatch(commit_sha) is None:
        raise ValueError("commit SHA must be 40 lowercase hexadecimal characters")


def _artifacts(root: Path) -> list[dict[str, Any]]:
    if not root.is_dir():
        raise ValueError("artifact directory does not exist")
    files = sorted(root.iterdir())
    if any(not path.is_file() or path.is_symlink() for path in files):
        raise ValueError("candidate artifacts must be direct regular files")
    if not files:
        raise ValueError("candidate must contain at least one artifact")
    return [
        {"name": path.name, "sha256": _sha256(path), "size": path.stat().st_size}
        for path in files
    ]


def _verification(path: Path, commit_sha: str) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("verification evidence must be a JSON object")
    if payload.get("commit_sha") != commit_sha:
        raise ValueError("verification evidence does not match the candidate commit")
    runs = payload.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError("verification evidence must contain successful runs")
    normalized = []
    for run in runs:
        if (
            not isinstance(run, dict)
            or not isinstance(run.get("name"), str)
            or not run["name"]
            or not isinstance(run.get("run_id"), int)
            or run["run_id"] <= 0
            or run.get("conclusion") != "success"
        ):
            raise ValueError("verification evidence must contain successful named runs")
        normalized.append(
            {"name": run["name"], "run_id": run["run_id"], "conclusion": "success"}
        )
    return sorted(normalized, key=lambda run: (run["name"], run["run_id"]))


def build_manifest(
    *,
    version: str,
    rc_tag: str,
    commit_sha: str,
    artifact_dir: Path,
    verification_path: Path,
) -> dict[str, Any]:
    _identity(version, rc_tag, commit_sha)
    return {
        "schema": SCHEMA,
        "version": version,
        "rc_tag": rc_tag,
        "commit_sha": commit_sha,
        "verification": _verification(verification_path, commit_sha),
        "artifacts": _artifacts(artifact_dir),
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def verify_manifest(
    *, manifest_path: Path, artifact_dir: Path, stable_tag: str, stable_sha: str
) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("candidate manifest must be a JSON object")
    if set(manifest) != {
        "schema",
        "version",
        "rc_tag",
        "commit_sha",
        "verification",
        "artifacts",
    }:
        raise ValueError("candidate manifest has unexpected fields")
    if manifest["schema"] != SCHEMA:
        raise ValueError("candidate manifest schema mismatch")
    _identity(manifest["version"], manifest["rc_tag"], manifest["commit_sha"])
    if stable_tag != f"v{manifest['version']}":
        raise ValueError("stable tag does not match candidate version")
    if stable_sha != manifest["commit_sha"]:
        raise ValueError("stable SHA does not match candidate commit")
    if _verification_from_manifest(manifest["verification"]) is False:
        raise ValueError("candidate verification evidence is invalid")
    actual = _artifacts(artifact_dir)
    if len(actual) != len(manifest["artifacts"]):
        raise ValueError("candidate artifact inventory differs from the manifest")
    for expected, found in zip(manifest["artifacts"], actual, strict=True):
        if expected.get("name") != found["name"] or expected.get("size") != found["size"]:
            raise ValueError("candidate artifact inventory differs from the manifest")
        if SHA256.fullmatch(str(expected.get("sha256"))) is None:
            raise ValueError("candidate artifact digest is invalid")
        if expected["sha256"] != found["sha256"]:
            raise ValueError(f"candidate artifact digest mismatch: {found['name']}")


def _verification_from_manifest(runs: Any) -> bool:
    return isinstance(runs, list) and bool(runs) and all(
        isinstance(run, dict)
        and isinstance(run.get("name"), str)
        and bool(run["name"])
        and isinstance(run.get("run_id"), int)
        and run["run_id"] > 0
        and run.get("conclusion") == "success"
        for run in runs
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--version", required=True)
    build.add_argument("--rc-tag", required=True)
    build.add_argument("--commit-sha", required=True)
    build.add_argument("--artifact-dir", required=True, type=Path)
    build.add_argument("--verification", required=True, type=Path)
    build.add_argument("--output", required=True, type=Path)
    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", required=True, type=Path)
    verify.add_argument("--artifact-dir", required=True, type=Path)
    verify.add_argument("--stable-tag", required=True)
    verify.add_argument("--stable-sha", required=True)
    args = parser.parse_args()
    if args.command == "build":
        manifest = build_manifest(
            version=args.version,
            rc_tag=args.rc_tag,
            commit_sha=args.commit_sha,
            artifact_dir=args.artifact_dir,
            verification_path=args.verification,
        )
        write_manifest(args.output, manifest)
    else:
        verify_manifest(
            manifest_path=args.manifest,
            artifact_dir=args.artifact_dir,
            stable_tag=args.stable_tag,
            stable_sha=args.stable_sha,
        )


if __name__ == "__main__":
    main()
