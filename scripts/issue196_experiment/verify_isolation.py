#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess

from responsibility_graph import load_manifest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/issue-196-local-experiment.yml"
MANIFEST = ROOT / "scripts/issue196_experiment/responsibility_graph.json"


def is_allowed_path(path: str) -> bool:
    return (
        path == ".github/workflows/issue-196-local-experiment.yml"
        or path == "docs/issue-196-local-experiment.md"
        or path.startswith("scripts/issue196_experiment/")
    )


def git_lines(*arguments: str) -> list[str]:
    result = subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def changed_paths(base_ref: str) -> set[str]:
    tracked = git_lines("diff", "--name-only", base_ref, "--")
    untracked = git_lines("ls-files", "--others", "--exclude-standard")
    return set(tracked) | set(untracked)


def verify_workflow() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    trigger = workflow.split("\non:\n", 1)[1].split("\npermissions:\n", 1)[0]
    if "workflow_dispatch:" not in trigger:
        raise SystemExit("experiment workflow must be manually selected")
    for forbidden in ("push:", "pull_request:", "schedule:"):
        if forbidden in trigger:
            raise SystemExit(f"experiment workflow has an automatic trigger: {forbidden}")
    if "actions/checkout@" in workflow:
        raise SystemExit("local experiment must use materialized source, not checkout")
    if "CARGO_HOME: /experiment/cargo-home" not in workflow:
        raise SystemExit("Cargo home is not isolated under /experiment")
    if "CARGO_TARGET_DIR: target" in workflow or "CARGO_TARGET_DIR: ./target" in workflow:
        raise SystemExit("experiment workflow references a repository target directory")

    manifest = load_manifest(MANIFEST)
    final_gate = manifest["experiment_workflow"]["final_gate_job"]
    join = workflow.split(f"  {final_gate}:", 1)[1].split(
        "\n  experiment-metrics:", 1
    )[0]
    for forbidden in ("cargo ", "pytest", "maturin", "build-nim", "test-nim"):
        if forbidden in join:
            raise SystemExit(f"status join owns verification work: {forbidden}")
    if "\\\n" in join:
        raise SystemExit("status join must use independent fail-closed checks")
    for owner_job in manifest["experiment_workflow"]["final_gate_needs"]:
        if f"needs.{owner_job}.result" not in join:
            raise SystemExit(f"status join omits experiment owner job: {owner_job}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prove that issue #196's local experiment is isolated."
    )
    parser.add_argument("--base-ref", default="origin/main")
    parser.add_argument(
        "--workflow-only",
        action="store_true",
        help="verify workflow invariants after act copies the repository without .git",
    )
    args = parser.parse_args()

    verify_workflow()
    if args.workflow_only:
        print("isolated experiment workflow verified")
        return

    git_lines("rev-parse", "--verify", args.base_ref)
    paths = changed_paths(args.base_ref)
    outside = sorted(path for path in paths if not is_allowed_path(path))
    if outside:
        raise SystemExit(
            "experiment modifies existing repository paths: " + ", ".join(outside)
        )
    print(f"isolated experiment verified: {len(paths)} changed paths, all allowlisted")


if __name__ == "__main__":
    main()
