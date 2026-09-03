"""Generate/check the exact Polars 1.43.2 public API inventory."""
from __future__ import annotations

import argparse
import ast
import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

VERSION = "1.43.2"
COMMIT = "ae588a9f2c91171f45bace43a99fb7b80b90847b"
PYI = Path(__file__).parents[2] / "crates/alopex-py/python/alopex/_alopex.pyi"


def public_members(value: object) -> list[str]:
    return sorted(name for name, _ in inspect.getmembers(value) if not name.startswith("_"))


def inventory() -> list[dict[str, str]]:
    import polars as pl

    if pl.__version__ != VERSION:
        raise RuntimeError(f"Polars {VERSION} is required, got {pl.__version__}")
    surfaces = {
        "DataFrame": pl.DataFrame,
        "Series": pl.Series,
        "LazyFrame": pl.LazyFrame,
        "Expr": pl.Expr,
        "Expr.str": type(pl.col("value").str),
        "Expr.dt": type(pl.col("value").dt),
        "Expr.list": type(pl.col("value").list),
        "Series.str": type(pl.Series(["value"]).str),
        "Series.dt": type(pl.Series([datetime(2026, 1, 1, tzinfo=timezone.utc)]).dt),
        "Series.list": type(pl.Series([["value"]]).list),
    }
    rows = [
        {"surface": surface, "api": f"{surface}.{name}"}
        for surface, value in surfaces.items()
        for name in public_members(value)
    ]
    rows.extend(
        {"surface": "function", "api": name}
        for name in public_members(pl)
        if name in {"col", "concat", "concat_str", "lit"}
        or name.startswith(("read_", "scan_"))
    )
    for class_name in ("DataFrame", "Series", "LazyFrame", "Expr"):
        rows.append({"surface": class_name, "api": f"{class_name}.__init__"})
    return sorted(rows, key=lambda row: (row["surface"], row["api"]))


def alopex_polars_overlaps(public_names: set[str]) -> set[str]:
    tree = ast.parse(PYI.read_text(encoding="utf-8"))
    candidates: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            candidates.add(node.name)
        elif isinstance(node, ast.ClassDef) and node.name in {
            "DataFrame",
            "Series",
            "LazyFrame",
            "Expr",
        }:
            candidates.update(
                f"{node.name}.{member.name}"
                for member in node.body
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                and (member.name == "__init__" or not member.name.startswith("_"))
            )
    return candidates & public_names


def materialize(payload: dict[str, object]) -> list[dict[str, object]]:
    claims = payload.get("claims", [])
    by_api: dict[str, dict[str, object]] = {}
    for claim in claims:
        api = str(claim["api"])
        if api in by_api:
            raise RuntimeError(f"duplicate claim: {api}")
        by_api[api] = claim
    rows: list[dict[str, object]] = []
    public = inventory()
    public_names = {row["api"] for row in public}
    missing = sorted(alopex_polars_overlaps(public_names) - by_api.keys())
    if missing:
        raise RuntimeError(f"unmapped Alopex/Polars overlaps: {missing}")
    unknown = sorted(
        api
        for api, claim in by_api.items()
        if claim.get("status") != "alopex-extension" and api not in public_names
    )
    if unknown:
        raise RuntimeError(f"unknown Polars claims: {unknown}")
    for row in public:
        claim = by_api.get(row["api"])
        if claim is None:
            rows.append({
                **row,
                "status": "not-yet-implemented",
                "evidence": "docs/parity/README.md",
                "issue": 305,
            })
        else:
            mapped = dict(claim)
            if mapped.get("status") == "known-divergence":
                mapped.setdefault("reference", f"polars:{VERSION}@{COMMIT}")
            rows.append(mapped)
    rows.extend(
        dict(claim)
        for claim in claims
        if claim.get("status") == "alopex-extension"
    )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.ledger.read_text(encoding="utf-8"))
    expected = materialize(payload)
    if args.write:
        payload["entries"] = expected
        args.ledger.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return 0
    if payload.get("polars_version") != VERSION or payload.get("polars_commit") != COMMIT:
        raise RuntimeError("ledger reference does not match the inventory generator")
    if payload.get("entries") != expected:
        raise RuntimeError("Polars public inventory is stale; run with --write")
    print(f"validated {len(expected)} Polars public API rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
