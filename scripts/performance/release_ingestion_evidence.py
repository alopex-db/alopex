#!/usr/bin/env python3
"""Validate and render fixed-size HTTP ingestion measurements for a release commit."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path


OPERATIONS = ("single", "batch", "csv", "parquet")
SIZES = (10_000, 50_000)


def load_rows(
    root: Path, source_commit: str, operations: tuple[str, ...] = OPERATIONS
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for operation in operations:
        for size in SIZES:
            path = root / f"{operation}-{size}.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("schema") != "alopex.ingestion-measurement/v1":
                raise ValueError(f"unexpected schema: {path.name}")
            if payload.get("source_commit") != source_commit:
                raise ValueError(f"source commit mismatch: {path.name}")
            if payload.get("operation") != operation or payload.get("rows") != size:
                raise ValueError(f"case mismatch: {path.name}")
            for field in ("elapsed_seconds", "rows_per_second"):
                if not isinstance(payload.get(field), (int, float)) or not math.isfinite(payload[field]):
                    raise ValueError(f"non-finite {field}: {path.name}")
            rows.append(payload)
    return rows


def render(
    root: Path, source_commit: str, operations: tuple[str, ...] = OPERATIONS
) -> None:
    rows = load_rows(root / "raw", source_commit, operations)
    root.mkdir(parents=True, exist_ok=True)
    (root / "ingestion.raw.json").write_text(
        json.dumps(
            {
                "schema": "alopex.ingestion-evidence/v1",
                "source_commit": source_commit,
                "contract": {
                    "protocol": "http",
                    "operations": list(operations),
                    "sizes": list(SIZES),
                    "warmups": 1,
                    "samples": 1,
                    "storage": "temporary local filesystem",
                    "runtime_threads": 1,
                    "dimension": 2,
                },
                "results": rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    with (root / "ingestion.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("operation", "rows", "elapsed_seconds", "rows_per_second"),
        )
        writer.writeheader()
        writer.writerows(
            {
                field: row[field]
                for field in ("operation", "rows", "elapsed_seconds", "rows_per_second")
            }
            for row in rows
        )
    lines = [
        "# v0.8.13 ingestion evidence",
        "",
        f"Source commit: `{source_commit}`",
        "",
        "| operation | rows | seconds | rows/s |",
        "|---|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['operation']} | {row['rows']} | {row['elapsed_seconds']:.6f} | {row['rows_per_second']:.2f} |"
        )
    (root / "ingestion.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--operations", nargs="+", choices=OPERATIONS, default=OPERATIONS)
    args = parser.parse_args()
    render(args.root, args.source_commit, tuple(args.operations))
