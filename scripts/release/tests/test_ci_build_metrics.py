from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts/ci/run_with_metrics.py"
SPEC = importlib.util.spec_from_file_location("run_with_metrics", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load build metrics module")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class BuildMetricsTests(unittest.TestCase):
    def test_directory_bytes_does_not_follow_a_symlinked_target_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            actual_target = root / "actual-target"
            actual_target.mkdir()
            (actual_target / "artifact").write_bytes(b"outside")
            target_link = root / "target"
            target_link.symlink_to(actual_target, target_is_directory=True)

            self.assertEqual(MODULE.directory_bytes(target_link), 0)

    def test_measure_records_failure_without_hiding_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            target.mkdir()
            (target / "artifact").write_bytes(b"abc")
            output = root / "metrics/result.json"
            summary = root / "summary.md"
            monotonic_values = iter((10.0, 12.5))
            utc_values = iter(
                (
                    datetime(2026, 8, 25, 0, 0, tzinfo=timezone.utc),
                    datetime(2026, 8, 25, 0, 0, 3, tzinfo=timezone.utc),
                )
            )
            seen: list[list[str]] = []

            request = MODULE.MeasurementRequest(
                owner="current-implementation-linux",
                output=output,
                target_dir=target,
                summary=summary,
                command=("cargo", "test", "--workspace"),
            )
            record = MODULE.measure(
                request,
                runner=lambda command: seen.append(list(command)) or 7,
                monotonic=lambda: next(monotonic_values),
                utcnow=lambda: next(utc_values),
            )

            self.assertEqual(seen, [["cargo", "test", "--workspace"]])
            self.assertEqual(record["schema"], "alopex-ci-build-owner-result-v1")
            self.assertEqual(record["returncode"], 7)
            self.assertEqual(record["elapsed_seconds"], 2.5)
            self.assertEqual(record["target_bytes"], 3)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), record)
            rendered = summary.read_text(encoding="utf-8")
            self.assertIn("current-implementation-linux", rendered)
            self.assertIn("failure (7)", rendered)


if __name__ == "__main__":
    unittest.main()
