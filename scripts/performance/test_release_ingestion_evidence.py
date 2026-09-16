#!/usr/bin/env python3
"""Behavior checks for release ingestion evidence."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("release_ingestion_evidence.py")
SPEC = importlib.util.spec_from_file_location("release_ingestion_evidence", SCRIPT)
assert SPEC and SPEC.loader
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)


def write_raw(root: Path, *, bad_rate: bool = False) -> None:
    raw = root / "raw"
    raw.mkdir(parents=True)
    for operation, rate in {
        "single": 100.0,
        "batch": 300.0,
        "batch_existing": 250.0,
        "csv": 200.0,
        "parquet": 250.0,
    }.items():
        for rows in evidence.SIZES:
            payload = {
                "schema": "alopex.ingestion-measurement/v1",
                "source_commit": "candidate",
                "operation": operation,
                "rows": rows,
                "elapsed_seconds": rows / rate,
                "rows_per_second": rate + (1 if bad_rate and operation == "batch" else 0),
            }
            (raw / f"{operation}-{rows}.json").write_text(json.dumps(payload), encoding="utf-8")


class ReleaseIngestionEvidenceTest(unittest.TestCase):
    def test_render_derives_single_api_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_raw(root)
            evidence.render(root, "candidate")
            report = json.loads((root / "ingestion.raw.json").read_text(encoding="utf-8"))
            batch = next(row for row in report["results"] if row["operation"] == "batch")
            self.assertEqual(batch["single_rows_per_second"], 100.0)
            self.assertEqual(batch["relative_to_single"], 3.0)

    def test_render_rejects_inconsistent_throughput(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_raw(root, bad_rate=True)
            with self.assertRaisesRegex(ValueError, "throughput mismatch"):
                evidence.render(root, "candidate")


if __name__ == "__main__":
    unittest.main()
