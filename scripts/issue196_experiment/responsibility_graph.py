#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SCHEMA = "alopex-issue-196-responsibility-graph-v1"
VERSION_GATE_IDS = {"v06-gate", "v07-gate", "v08-release-gate"}


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("schema") != SCHEMA:
        raise ValueError(f"unexpected responsibility graph schema: {manifest.get('schema')}")

    owners = manifest.get("owners", [])
    owner_ids = [owner["id"] for owner in owners]
    if not owner_ids or len(owner_ids) != len(set(owner_ids)):
        raise ValueError("responsibility owners must be present and unique")

    target = manifest.get("target_graph", {})
    if set(target.get("final_gate_needs", [])) != set(owner_ids):
        raise ValueError("the target final gate must join every responsibility owner")
    if target.get("final_gate_commands") != []:
        raise ValueError("the target final gate must be status-only")
    target_text = json.dumps(target, sort_keys=True)
    leaked_versions = sorted(VERSION_GATE_IDS & set(_all_strings(target)))
    if leaked_versions:
        raise ValueError(f"version aggregators leaked into target graph: {leaked_versions}")
    if any(version in target_text for version in VERSION_GATE_IDS):
        raise ValueError("version aggregator text leaked into target graph")

    rules = manifest.get("inventory", {}).get("ownership_rules", [])
    if not rules or rules[-1].get("default") is not True:
        raise ValueError("ownership rules must end with one default owner")
    if sum(rule.get("default") is True for rule in rules) != 1:
        raise ValueError("ownership rules must contain exactly one default")
    unknown = sorted({rule["owner"] for rule in rules} - set(owner_ids))
    if unknown:
        raise ValueError(f"ownership rules reference unknown owners: {unknown}")

    measurement = manifest.get("measurement", {})
    if measurement.get("samples", 0) < 3:
        raise ValueError("cold/warm evidence requires at least three samples")
    graphs = {graph["id"]: graph for graph in measurement.get("graphs", [])}
    if set(graphs) != {"nested-version-topology", "responsibility-topology"}:
        raise ValueError("measurement must define the current and target topologies")
    if len(graphs["nested-version-topology"]["steps"]) != 2:
        raise ValueError("the current topology probe must expose both build boundaries")
    if len(graphs["responsibility-topology"]["steps"]) != 1:
        raise ValueError("the responsibility topology must execute its owner once")
    for graph in graphs.values():
        for step in graph["steps"]:
            if step["owner"] not in owner_ids:
                raise ValueError(f"measurement step has unknown owner: {step}")


def _all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [item for child in value.values() for item in _all_strings(child)]
    if isinstance(value, list):
        return [item for child in value for item in _all_strings(child)]
    return []
