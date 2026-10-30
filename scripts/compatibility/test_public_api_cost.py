"""Tiny negative checks for the manual measurement; no product import/build."""
import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from public_api_cost import METRICS, SHAPES, sizes, summarize


class PublicCostTests(unittest.TestCase):
    def test_matrix_rejects_missing_duplicate_failed_and_unreopened_rows(self):
        rows = [dict({m: 100 for m in METRICS}, engine=engine, surface=surface, value_bytes=size,
                     round=round_id, status="passed", reopen_cycles=2, reopen_ns=[20, 30])
                for engine in ("before", "after") for surface, size in SHAPES for round_id in range(6)]
        report = summarize(rows)
        self.assertEqual(report["commit_after_before_ratios"], {f"{s}-{v}": 1 for s, v in SHAPES})
        for damaged in (rows[:-1], rows[:-1] + [rows[0]]):
            with self.assertRaisesRegex(RuntimeError, "matrix"):
                summarize(damaged)
        for key, value in (("status", "failed"), ("reopen_cycles", 1)):
            damaged = copy.deepcopy(rows)
            damaged[0][key] = value
            with self.assertRaisesRegex(RuntimeError, "unverified"):
                summarize(damaged)

    def test_warmup_is_retained_but_not_in_median(self):
        rows = [dict({m: 1_000 if round_id == 0 else (150 if engine == "after" else 100)
                      for m in METRICS}, engine=engine, surface=surface, value_bytes=size,
                     round=round_id, status="passed", reopen_cycles=2, reopen_ns=[20, 30])
                for engine in ("before", "after") for surface, size in SHAPES for round_id in range(6)]
        report = summarize(rows)
        self.assertEqual(set(report["commit_after_before_ratios"].values()), {1.5})
        self.assertEqual(len(report["investigate"]), 4)

    def test_ci_and_optimized_python_reject_before_product_import_or_output(self):
        script = Path(__file__).with_name("public_api_cost.py")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "results"
            for flags, env in (([], dict(os.environ, CI="true")), (["-O"], {k: v for k, v in os.environ.items() if k != "CI"})):
                run = subprocess.run([sys.executable, *flags, str(script), "--output", str(output)],
                    env=dict(env, PYTHONDONTWRITEBYTECODE="1"), capture_output=True, text=True)
                self.assertEqual(run.returncode, 2)
                self.assertIn("manual macOS/Linux only", run.stderr)
                self.assertFalse(output.exists())

    def test_filesystem_accounting_separates_wal_spool_and_total(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "overflow").mkdir()
            (root / "lsm.wal").write_bytes(b"wal")
            (root / "overflow/payload").write_bytes(b"spool")
            (root / "db.alopex").write_bytes(b"file")
            actual = sizes(root)
            self.assertEqual((actual["wal_bytes"], actual["spool_bytes"], actual["fixture_bytes"]), (3, 5, 12))
            (root / "alias").symlink_to(root / "lsm.wal")
            with self.assertRaisesRegex(RuntimeError, "symlink"):
                sizes(root)


if __name__ == "__main__":
    unittest.main()
