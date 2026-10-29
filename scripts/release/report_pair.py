#!/usr/bin/env python3
"""Bind a downloaded JSON/Markdown report pair to its producing attempt."""
from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace


MAX_REPORT_BYTES = 8 * 1024 * 1024
_spec = importlib.util.spec_from_file_location(
    "release_report", Path(__file__).parent / "verify-release/report.py"
)
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)


def read_regular(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"missing or non-regular report file: {path}")
    if path.stat().st_size > MAX_REPORT_BYTES:
        raise ValueError(f"report exceeds {MAX_REPORT_BYTES} bytes: {path}")
    return path.read_bytes()


def select_pair(root: Path, artifact: str, run_id: str, repository: str) -> dict[str, str]:
    match = re.fullmatch(r"release-verification-([1-9][0-9]*)-([1-9][0-9]*)", artifact)
    if not match or match[1] != run_id:
        raise ValueError("artifact does not belong to the current workflow run")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("invalid repository")
    if root.is_symlink() or not root.is_dir():
        raise ValueError("report root must be a real directory")
    if any(path.is_symlink() for path in root.rglob("*")):
        raise ValueError("report artifact must not contain symlinks")
    candidates = list(root.rglob("v*.json"))
    if len(candidates) != 1:
        raise ValueError("expected exactly one verification JSON report")
    source = candidates[0]
    read_regular(source)
    payload = report.load(source)
    report.validate_report_payload(payload)
    version = payload.get("version")
    if not isinstance(version, str) or not re.fullmatch(r"(?:[0-9]+\.[0-9]+\.[0-9]+|unknown)", version):
        raise ValueError("invalid report version")
    if source.name != f"v{version}.json":
        raise ValueError("report filename/version mismatch")
    identity = payload["identity"]
    if type(identity["run_attempt"]) is not int or (identity["run_id"], identity["run_attempt"]) != (match[1], int(match[2])):
        raise ValueError("report identity does not match the producing artifact")
    if identity["run_url"] != f"https://github.com/{repository}/actions/runs/{run_id}":
        raise ValueError("report run URL does not match the producing repository/run")
    markdown = read_regular(source.with_suffix(".md"))
    # Reject a Markdown file from another attempt or result, even when filenames
    # happen to agree. Rendering is deterministic and does not alter the input.
    with tempfile.TemporaryDirectory(prefix="alopex-report-pair-") as directory:
        report.render(SimpleNamespace(results=source, output_dir=Path(directory)))
        if markdown != (Path(directory) / f"v{version}.md").read_bytes():
            raise ValueError("Markdown does not render the verification JSON")
    return {
        "VERSION": version,
        "REPORT_ROOT": str(source.parent),
        "REPORT_RUN_ID": match[1],
        "REPORT_RUN_ATTEMPT": match[2],
        "REPORT_PATH": f"reports/release-verification/v{version}/run-{match[1]}-{match[2]}",
    }


def compare_pair(left: Path, right: Path) -> None:
    """Require both files to exist and match; never repair a partial pair."""
    for suffix in (".json", ".md"):
        if read_regular(Path(str(left) + suffix)) != read_regular(Path(str(right) + suffix)):
            raise ValueError(f"immutable report pair differs: {suffix}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("select")
    select.add_argument("--root", type=Path, required=True)
    select.add_argument("--artifact", required=True)
    select.add_argument("--run-id", required=True)
    select.add_argument("--repository", required=True)
    select.add_argument("--github-env", type=Path, required=True)
    compare = commands.add_parser("compare")
    compare.add_argument("--left", type=Path, required=True)
    compare.add_argument("--right", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "compare":
        compare_pair(args.left, args.right)
    else:
        values = select_pair(args.root, args.artifact, args.run_id, args.repository)
        if any("\n" in value or "\r" in value for value in values.values()):
            raise ValueError("invalid environment value")
        with args.github_env.open("a", encoding="utf-8") as stream:
            stream.write("".join(f"{key}={value}\n" for key, value in values.items()))


if __name__ == "__main__":
    main()
