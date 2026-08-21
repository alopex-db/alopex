#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


EXPECTED_OWNERS = {"historical-compatibility", "delivery-contract"}
INVENTORY_SCHEMA = "alopex-issue-196-build-signature-inventory-v1"
AB_SCHEMA = "alopex-issue-196-ab-measurement-v1"


def directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(candidate.stat().st_size for candidate in path.rglob("*") if candidate.is_file())


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize issue #196 local results.")
    parser.add_argument("--runtime", type=Path, required=True)
    args = parser.parse_args()

    records = []
    for path in sorted((args.runtime / "results").glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("schema") != "alopex-issue-196-owner-result-v1":
            raise SystemExit(f"unexpected result schema: {path}")
        records.append(record)
    owners = {record["owner"] for record in records}
    if owners != EXPECTED_OWNERS:
        raise SystemExit(f"owner result mismatch: expected {EXPECTED_OWNERS}, found {owners}")

    inventory = json.loads((args.runtime / "inventory.json").read_text(encoding="utf-8"))
    if inventory.get("schema") != INVENTORY_SCHEMA:
        raise SystemExit("unexpected build-signature inventory schema")
    ab_measurement = json.loads((args.runtime / "ab-results.json").read_text(encoding="utf-8"))
    if ab_measurement.get("schema") != AB_SCHEMA:
        raise SystemExit("unexpected A/B measurement schema")
    if ab_measurement.get("complete") is not True:
        raise SystemExit("A/B measurement is partial")
    for graph in ("nested-version-topology", "responsibility-topology"):
        for phase in ("cold", "warm"):
            samples = ab_measurement["aggregates"][graph][phase]["samples"]
            if len(samples) != 3:
                raise SystemExit(f"A/B measurement requires three {graph} {phase} samples")

    owner_signature_counts: dict[str, int] = {}
    for signature in inventory["signatures"]:
        owner = signature["owner"]
        owner_signature_counts[owner] = owner_signature_counts.get(owner, 0) + 1

    summary = {
        "schema": "alopex-issue-196-local-experiment-v1",
        "owners": records,
        "runner_seconds": round(sum(record["elapsed_seconds"] for record in records), 3),
        "inventory": {
            "signature_count": len(inventory["signatures"]),
            "signature_counts_by_owner": owner_signature_counts,
            "exact_duplicate_groups": len(inventory["duplicate_groups"]["exact"]),
            "near_duplicate_groups": len(inventory["duplicate_groups"]["near"]),
        },
        "ab_measurement": {
            "aggregates": ab_measurement["aggregates"],
            "median_reduction_percent": ab_measurement["median_reduction_percent"],
        },
        "isolated_target_bytes": directory_bytes(args.runtime / "targets"),
        "isolated_cargo_home_bytes": directory_bytes(args.runtime / "cargo-home"),
    }
    output = args.runtime / "summary.json"
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if any(record["returncode"] != 0 for record in records):
        raise SystemExit("one or more experiment owners failed")


if __name__ == "__main__":
    main()
