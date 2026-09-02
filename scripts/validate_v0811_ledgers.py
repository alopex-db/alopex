"""Validate v0.8.11 compatibility ledgers without third-party dependencies."""
from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEDGERS = (
    ROOT / "docs/parity/polars-v1.43.2.json",
    ROOT / "docs/parity/hnsw-v0.8.11.json",
    ROOT / "docs/parity/sql-v0.8.11.json",
)


def validate(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    errors: list[str] = []
    statuses = set(payload.get("status_values", []))
    entries = payload.get("entries", [])
    seen: set[str] = set()
    for entry in entries:
        api = entry.get("api")
        if not api:
            errors.append(f"{path}: entry has no api")
            continue
        if api in seen:
            errors.append(f"{path}: duplicate api {api}")
        seen.add(api)
        if entry.get("status") not in statuses:
            errors.append(f"{path}: invalid status for {api}")
        if not entry.get("evidence"):
            errors.append(f"{path}: missing evidence for {api}")
        if entry.get("status") not in {"alopex-extension", "not-yet-implemented", "unsupported"} and not entry.get("reference"):
            errors.append(f"{path}: missing reference for {api}")
    if not entries:
        errors.append(f"{path}: empty ledger")
    return errors


def main() -> int:
    errors = [error for path in LEDGERS for error in validate(path)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"validated {len(LEDGERS)} v0.8.11 ledgers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
