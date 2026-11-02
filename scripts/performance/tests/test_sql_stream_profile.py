"""Tiny fake streams protect original and equal-row measurement boundaries."""
import ast
import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sql_stream_profile as d


ROWS = [{"id": i, "value": float(i)} for i in (0, 2, 4)]


class Stream:
    def __init__(self, rows, events):
        self.rows, self.events = rows, events

    def __enter__(self):
        self.events.append("enter")
        return iter(self.rows)

    def __exit__(self, *args):
        self.events.append("close")


def fake_build(events):
    class Batch:
        def __init__(self, rows):
            self.rows, self.num_rows = rows, len(rows)

        def to_pyarrow(self):
            events.append("arrow")
            return self

        def to_pylist(self):
            events.append("python-rows")
            return copy.deepcopy(self.rows)

    def stream():
        events.append("first-batch")
        yield Batch(ROWS[:2])
        events.append("last-batch")
        yield Batch(ROWS[2:])
    return lambda: SimpleNamespace(execute_stream=stream)


class Contracts(unittest.TestCase):
    def test_original_consumer_bodies_match_pinned_corrected_source(self):
        reference = ast.parse((Path(d.__file__).parent / "workloads/sql_streaming_v0816.py").read_text())
        original = [n for n in ast.walk(reference) if isinstance(n, ast.FunctionDef) and n.name == "consume"]
        current = ast.parse(Path(d.__file__).read_text())
        for old, name in zip(original, ["consume_original_alopex", "consume_original_datafusion"]):
            new = next(n for n in current.body if isinstance(n, ast.FunctionDef) and n.name == name)
            self.assertEqual([ast.dump(n) for n in old.body], [ast.dump(n) for n in new.body])
        self.assertEqual(len(original), 2)

    def test_original_alopex_total_ends_before_context_close(self):
        events = []
        values = iter([10.0, 11.0, 13.0])

        def clock():
            events.append("clock")
            return next(values)
        with patch.object(d.time, "perf_counter", side_effect=clock):
            result = d.consume_original_alopex(lambda: Stream(ROWS, events))
        self.assertEqual(result, (1.0, 3.0, 3, 1))
        self.assertEqual(events, ["clock", "enter", "clock", "clock", "close"])
        events.clear()
        with self.assertRaises(StopIteration):
            d.consume_original_alopex(lambda: Stream([], events))
        self.assertEqual(events, ["enter", "close"])

    def test_original_datafusion_consumes_late_batch_without_python_rows(self):
        events = []
        with patch.object(d.time, "perf_counter", side_effect=[10.0, 11.0, 13.0]):
            result = d.consume_original_datafusion(fake_build(events))
        self.assertEqual(result, (1.0, 3.0, 3, 2))
        self.assertIn("last-batch", events)
        self.assertNotIn("python-rows", events)

    def test_equal_rows_lane_materializes_every_row_for_both_engines(self):
        events = []
        for result in [d.consume_rows_alopex(lambda: Stream(ROWS, events)),
                       d.consume_rows_datafusion(fake_build(events))]:
            self.assertEqual(d.validate_result(result, ROWS, True), ROWS)
        self.assertEqual(events.count("python-rows"), 2)

    def test_row_oracle_rejects_types_nulls_columns_values_and_multiplicity(self):
        self.assertEqual(d.canonical_rows(list(reversed(ROWS)), ROWS), ROWS)  # SQL has no ORDER BY.
        bad = [ROWS[:-1], [ROWS[0], ROWS[0], ROWS[-1]],
               [{"id": False, "value": 0.0}, *ROWS[1:]],
               [{"id": 0, "value": 0}, *ROWS[1:]],
               [{"id": 0, "value": None}, *ROWS[1:]],
               [{"value": 0.0, "id": 0}, *ROWS[1:]],
               [*ROWS[:-1], {"id": 4, "value": 99.0}]]
        for rows in bad:
            with self.assertRaises(RuntimeError):
                d.canonical_rows(rows, ROWS)

    def test_measure_keeps_original_and_full_row_validation_distinct(self):
        for full in (False, True):
            records = []
            result = (0.000001, 0.000003, 3, 1, ROWS if full else None)
            self.assertEqual(d.measure(lambda: result, ROWS, full, warmups=1, samples=2, record=records.append), 3)
            self.assertEqual([r["phase"] for r in records], ["warmup", "measure", "measure"])
            self.assertTrue(all(r["first_ns"]+r["remaining_ns"] == r["total_ns"] for r in records))
            self.assertTrue(all(r["validation"] == ("full typed rows" if full else "count; full values before/after case") for r in records))
        with self.assertRaisesRegex(RuntimeError, "incomplete stream"):
            d.validate_result((1.0, 2.0, 2, 1, None), ROWS, False)

    def test_ci_rejected_before_native_import_or_work_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "unused"
            result = subprocess.run([sys.executable, "-B", d.__file__, "--manual", "--surface", "embedded",
                                     "--rows", "3", "--operation", "alopex-original", "--work", str(work)],
                                    env=dict(os.environ, CI="true"), capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("forbidden in CI", result.stderr)
            self.assertFalse(work.exists())


if __name__ == "__main__":
    unittest.main()
