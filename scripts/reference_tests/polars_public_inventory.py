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
EXPECTED_PUBLIC_ROWS = 995
EXPECTED_MATERIALIZED_ROWS = 999
PERFORMANCE_STATUSES = {"implemented-compatible", "known-performance-divergence"}
REPAIRED_APIS = {
    "DataFrame.__init__",
    "DataFrame.explode",
    "DataFrame.height",
    "DataFrame.width",
    "DataFrame.to_dict",
    "Series.name",
    "Series.to_list",
    "Expr.add",
    "Expr.and_",
    "Expr.eq",
    "Expr.ge",
    "Expr.gt",
    "Expr.le",
    "Expr.lt",
    "Expr.mul",
    "Expr.or_",
    "Expr.sub",
    "concat",
    "concat_str",
    "lit",
    "LazyFrame.collect",
    "LazyFrame.collect_batches",
}
DIFFERENTIAL_TEST = "crates/alopex-py/tests/test_polars_1432_reference.py"


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
    rows = sorted(rows, key=lambda row: (row["surface"], row["api"]))
    if len(rows) != EXPECTED_PUBLIC_ROWS:
        raise RuntimeError(
            f"Polars public inventory changed: expected {EXPECTED_PUBLIC_ROWS}, got {len(rows)}"
        )
    return rows


def normalize_claims(claims: list[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for original in claims:
        claim = dict(original)
        api = str(claim["api"])
        if api in REPAIRED_APIS:
            lazy = api in {"LazyFrame.collect", "LazyFrame.collect_batches"}
            if lazy:
                selector = "test_polars_1432_csv_parquet_and_streaming_contracts"
            elif api.startswith(("DataFrame.", "Series.")):
                selector = (
                    "test_polars_1432_dataframe_properties_series_and_constructor_defaults"
                )
            else:
                selector = "test_polars_1432_expr_scalar_variadic_and_module_default_values"
            claim.update(
                status=(
                    "known-performance-divergence" if lazy else "implemented-compatible"
                ),
                reference=f"polars:{VERSION}@{COMMIT}",
                evidence=f"{DIFFERENTIAL_TEST}#{selector}",
                performance_contract=(
                    "polars-lazy-streaming-v1" if lazy else "polars-eager-v1"
                ),
                performance_evidence=(
                    "polars-csv-streaming" if lazy else "polars-eager-api"
                ),
                issue=305,
            )
            claim.pop("notes", None)
            claim.setdefault("contracts", ["signature", "return-type", "value", "error"])
        normalized.append(claim)
    return normalized


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
        status = str(claim.get("status", ""))
        if status in PERFORMANCE_STATUSES:
            if not claim.get("performance_contract"):
                raise RuntimeError(f"missing performance contract: {api}")
            if not claim.get("performance_evidence"):
                raise RuntimeError(f"missing performance evidence: {api}")
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
    if len(rows) != EXPECTED_MATERIALIZED_ROWS:
        raise RuntimeError(
            f"materialized Polars inventory changed: expected {EXPECTED_MATERIALIZED_ROWS}, got {len(rows)}"
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.ledger.read_text(encoding="utf-8"))
    claims = normalize_claims(list(payload.get("claims", [])))
    claims_are_stale = payload.get("claims") != claims
    payload["claims"] = claims
    expected = materialize(payload)
    if args.write:
        payload["entries"] = expected
        args.ledger.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return 0
    if payload.get("polars_version") != VERSION or payload.get("polars_commit") != COMMIT:
        raise RuntimeError("ledger reference does not match the inventory generator")
    if claims_are_stale:
        raise RuntimeError("Polars claims are stale; run with --write")
    if payload.get("entries") != expected:
        raise RuntimeError("Polars public inventory is stale; run with --write")
    print(f"validated {len(expected)} Polars public API rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
