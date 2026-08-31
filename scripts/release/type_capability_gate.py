#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import tomllib
from pathlib import Path


REQUIRED_SURFACES = {
    "parser_ast",
    "catalog_schema",
    "storage",
    "wal_snapshot_backup",
    "distributed_wire",
    "rust_api",
    "python_ffi",
    "dataframe_arrow_parquet",
    "packaging_demo",
}
REQUIRED_FAMILIES = {"decimal": 158, "temporal": 159, "json": 161, "nested": 162}


class GateError(ValueError):
    pass


def version_tuple(value: str) -> tuple[int, int, int]:
    if not re.fullmatch(r"\d+\.\d+\.\d+", value):
        raise GateError(f"invalid release version: {value}")
    major, minor, patch = value.split(".")
    return int(major), int(minor), int(patch)


def verify(catalog_path: Path, root: Path, release_version: str) -> None:
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    if catalog.get("schema_version") != 1:
        raise GateError("unsupported type capability schema_version")
    if not catalog.get("contract_version") or not catalog.get("migration_policy"):
        raise GateError("contract_version and migration_policy are required")

    surfaces = catalog.get("surfaces", [])
    surface_ids = {surface.get("id") for surface in surfaces}
    if surface_ids != REQUIRED_SURFACES or len(surfaces) != len(REQUIRED_SURFACES):
        raise GateError("type capability catalog has an incomplete surface matrix")
    for surface in surfaces:
        owners = surface.get("owners", [])
        if not owners:
            raise GateError(f"{surface['id']} has no production owner")
        for owner in owners:
            if not (root / owner).is_file():
                raise GateError(f"catalog path does not exist: {owner}")

    families = catalog.get("type_families", [])
    ids = [family.get("id") for family in families]
    issues = [family.get("issue") for family in families]
    if len(ids) != len(set(ids)) or len(issues) != len(set(issues)):
        raise GateError("type family ids and issues must be unique")
    if dict(zip(ids, issues, strict=True)) != REQUIRED_FAMILIES:
        raise GateError("type capability catalog is missing a v0.8.10 type family")

    incomplete = []
    release = version_tuple(release_version)
    for family in families:
        family_id = family.get("id")
        status = family.get("status")
        if not family_id or not isinstance(family.get("issue"), int):
            raise GateError("each type family requires an id and issue")
        if status not in {"planned", "complete"}:
            raise GateError(f"{family_id} has invalid status: {status}")
        evidence = family.get("evidence", {})
        if set(evidence) != REQUIRED_SURFACES:
            raise GateError(f"{family_id} has an incomplete evidence matrix")
        for surface_id, paths in evidence.items():
            if status == "complete" and not paths:
                raise GateError(f"{family_id} has no {surface_id} evidence")
            for path in paths:
                if not (root / path).is_file():
                    raise GateError(f"catalog evidence does not exist: {path}")
        if release >= version_tuple(family["target_release"]) and status != "complete":
            incomplete.append(family_id)

    if incomplete:
        raise GateError(
            f"incomplete type families for release {release_version}: "
            + ", ".join(incomplete)
        )


def workspace_version(root: Path) -> str:
    with (root / "Cargo.toml").open("rb") as stream:
        return tomllib.load(stream)["workspace"]["package"]["version"]


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--catalog", type=Path, default=root / "docs/sql-type-capabilities.json"
    )
    parser.add_argument("--release-version", default=workspace_version(root))
    args = parser.parse_args()
    try:
        verify(args.catalog, root, args.release_version)
    except (GateError, json.JSONDecodeError, KeyError, TypeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
