#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time
from typing import Any, Callable

from responsibility_graph import load_manifest


SCHEMA = "alopex-issue-196-ab-measurement-v1"


@dataclass(frozen=True)
class PlanEntry:
    sample: int
    phase: str
    graph: str
    step: str
    owner: str
    target_dir: Path
    command: tuple[str, ...]


def directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(candidate.stat().st_size for candidate in path.rglob("*") if candidate.is_file())


def build_plan(manifest: dict[str, Any], runtime: Path) -> list[PlanEntry]:
    measurement = manifest["measurement"]
    command = tuple(measurement["probe"])
    graphs = {graph["id"]: graph for graph in measurement["graphs"]}
    plan: list[PlanEntry] = []
    for sample in range(1, measurement["samples"] + 1):
        graph_order = (
            ("nested-version-topology", "responsibility-topology")
            if sample % 2
            else ("responsibility-topology", "nested-version-topology")
        )
        for graph_id in graph_order:
            graph = graphs[graph_id]
            for phase in ("cold", "warm"):
                for step in graph["steps"]:
                    target_dir = runtime / "targets" / f"sample-{sample}" / graph_id / step["target_slot"]
                    plan.append(
                        PlanEntry(
                            sample=sample,
                            phase=phase,
                            graph=graph_id,
                            step=step["id"],
                            owner=step["owner"],
                            target_dir=target_dir,
                            command=command,
                        )
                    )
    return plan


def _group_plan(plan: list[PlanEntry]) -> list[list[PlanEntry]]:
    groups: list[list[PlanEntry]] = []
    for entry in plan:
        key = (entry.sample, entry.graph, entry.phase)
        if not groups or (groups[-1][0].sample, groups[-1][0].graph, groups[-1][0].phase) != key:
            groups.append([])
        groups[-1].append(entry)
    return groups


def _tool_version(command: list[str], root: Path) -> str:
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def collect_metadata(root: Path) -> dict[str, str | None]:
    snapshot_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True
    ).stdout.strip()
    return {
        "base_commit": os.environ.get("ISSUE196_BASE_SHA"),
        "experiment_snapshot_commit": snapshot_commit,
        "cargo_version": _tool_version(["cargo", "--version"], root),
        "rustc_version": _tool_version(["rustc", "--version"], root),
    }


def execute_measurement(
    root: Path,
    runtime: Path,
    manifest: dict[str, Any],
    output: Path,
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
    metadata_provider: Callable[[Path], dict[str, str | None]] = collect_metadata,
) -> dict[str, Any]:
    runtime = runtime.resolve()
    targets_root = (runtime / "targets").resolve()
    cargo_home = (runtime / "cargo-home").resolve()
    targets_root.mkdir(parents=True, exist_ok=True)
    cargo_home.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update(manifest["measurement"]["environment"])
    environment["CARGO_HOME"] = str(cargo_home)
    metadata = metadata_provider(root)

    plan = build_plan(manifest, runtime)
    observations: list[dict[str, Any]] = []
    failed = False
    sample_count = manifest["measurement"]["samples"]
    for group in _group_plan(plan):
        first = group[0]
        if first.phase == "cold":
            for target in {entry.target_dir for entry in group}:
                resolved = target.resolve()
                if not resolved.is_relative_to(targets_root):
                    raise RuntimeError(f"refusing to clean target outside experiment runtime: {resolved}")
                if resolved.exists():
                    shutil.rmtree(resolved)

        group_started = time.monotonic()
        command_records: list[dict[str, Any]] = []
        for entry in group:
            entry.target_dir.mkdir(parents=True, exist_ok=True)
            command_environment = environment | {"CARGO_TARGET_DIR": str(entry.target_dir)}
            started_at = datetime.now(timezone.utc)
            started = time.monotonic()
            print(
                f"[{entry.sample}/{sample_count} {entry.graph} {entry.phase}] {entry.step}: "
                + " ".join(entry.command),
                flush=True,
            )
            result = command_runner(
                entry.command,
                cwd=root,
                env=command_environment,
                check=False,
            )
            elapsed = time.monotonic() - started
            timings = sorted(
                str(path.relative_to(entry.target_dir))
                for path in (entry.target_dir / "cargo-timings").glob("cargo-timing*.html")
            )
            command_records.append(
                {
                    "step": entry.step,
                    "owner": entry.owner,
                    "command": list(entry.command),
                    "target_dir": str(entry.target_dir.relative_to(runtime)),
                    "started_at": started_at.isoformat(),
                    "elapsed_seconds": round(elapsed, 3),
                    "returncode": result.returncode,
                    "target_bytes": directory_bytes(entry.target_dir),
                    "cargo_home_bytes": directory_bytes(cargo_home),
                    "cargo_timing_files": timings,
                }
            )
            print(
                f"[{entry.sample}/{sample_count} {entry.graph} {entry.phase}] {entry.step} "
                f"finished rc={result.returncode} elapsed={elapsed:.3f}s",
                flush=True,
            )
            if result.returncode != 0:
                failed = True
                break
        observations.append(
            {
                "sample": first.sample,
                "graph": first.graph,
                "phase": first.phase,
                "elapsed_seconds": round(time.monotonic() - group_started, 3),
                "commands": command_records,
            }
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        partial = _measurement_document(runtime, manifest, observations, metadata)
        output.write_text(json.dumps(partial, indent=2) + "\n", encoding="utf-8")
        if failed:
            break
        if first.phase == "warm":
            sample_graph_root = targets_root / f"sample-{first.sample}" / first.graph
            if sample_graph_root.exists():
                shutil.rmtree(sample_graph_root)

    document = _measurement_document(runtime, manifest, observations, metadata)
    output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    if failed:
        raise SystemExit("A/B measurement command failed; partial evidence was preserved")
    return document


def _measurement_document(
    runtime: Path,
    manifest: dict[str, Any],
    observations: list[dict[str, Any]],
    metadata: dict[str, str | None],
) -> dict[str, Any]:
    aggregates, reductions = calculate_aggregates(observations)

    expected_observations = manifest["measurement"]["samples"] * 4
    graphs = {graph["id"]: graph for graph in manifest["measurement"]["graphs"]}
    complete = len(observations) == expected_observations and all(
        len(observation["commands"]) == len(graphs[observation["graph"]]["steps"])
        and all(command["returncode"] == 0 for command in observation["commands"])
        for observation in observations
    )

    return {
        "schema": SCHEMA,
        "complete": complete,
        "responsibility_graph_schema": manifest["schema"],
        "scope": "representative duplicate-current-implementation build signature",
        **metadata,
        "probe": manifest["measurement"]["probe"],
        "environment": manifest["measurement"]["environment"],
        "observations": observations,
        "aggregates": aggregates,
        "median_reduction_percent": reductions,
        "remaining_target_bytes": directory_bytes(runtime / "targets"),
        "cargo_home_bytes": directory_bytes(runtime / "cargo-home"),
    }


def calculate_aggregates(
    observations: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, float | None]]:
    aggregates: dict[str, dict[str, Any]] = {}
    for graph in ("nested-version-topology", "responsibility-topology"):
        aggregates[graph] = {}
        for phase in ("cold", "warm"):
            selected = [
                observation
                for observation in observations
                if observation["graph"] == graph and observation["phase"] == phase
            ]
            owner_work = [
                round(sum(command["elapsed_seconds"] for command in observation["commands"]), 3)
                for observation in selected
            ]
            orchestration = [observation["elapsed_seconds"] for observation in selected]
            aggregates[graph][phase] = {
                "owner_work_samples": owner_work,
                "median_owner_work_seconds": (
                    round(statistics.median(owner_work), 3) if owner_work else None
                ),
                "orchestration_samples": orchestration,
                "median_orchestration_seconds": (
                    round(statistics.median(orchestration), 3) if orchestration else None
                ),
                "samples": owner_work,
                "median_seconds": round(statistics.median(owner_work), 3) if owner_work else None,
            }
    nested = aggregates["nested-version-topology"]
    responsibility = aggregates["responsibility-topology"]
    reductions: dict[str, float | None] = {}
    for phase in ("cold", "warm"):
        before = nested[phase]["median_owner_work_seconds"]
        after = responsibility[phase]["median_owner_work_seconds"]
        reductions[phase] = (
            round((before - after) / before * 100, 2) if before and after is not None else None
        )

    return aggregates, reductions


def metadata_for_recompute(existing: dict[str, Any]) -> dict[str, str | None]:
    metadata = {
        key: existing.get(key)
        for key in (
            "base_commit",
            "experiment_snapshot_commit",
            "cargo_version",
            "rustc_version",
        )
    }
    if metadata["base_commit"] is None:
        metadata["base_commit"] = os.environ.get("ISSUE196_BASE_SHA")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="Run issue #196 graph A/B measurements.")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--recompute",
        action="store_true",
        help="recompute derived aggregates from preserved raw observations",
    )
    args = parser.parse_args()
    manifest = load_manifest(args.manifest)
    if args.recompute:
        existing = json.loads(args.output.read_text(encoding="utf-8"))
        metadata = metadata_for_recompute(existing)
        document = _measurement_document(
            args.runtime.resolve(),
            manifest,
            existing["observations"],
            metadata,
        )
        document["derived_metrics_recomputed"] = True
        args.output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(document["aggregates"], indent=2))
        return
    document = execute_measurement(
        args.root.resolve(),
        args.runtime.resolve(),
        manifest,
        args.output,
    )
    print(json.dumps(document["aggregates"], indent=2))


if __name__ == "__main__":
    main()
