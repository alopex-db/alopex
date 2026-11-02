"""Small fake batches protect original CSV consumer and value-check contracts."""
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
import csv_stream_profile as d


class Batch:
    def __init__(self, columns):
        self.columns = columns
        self.height = len(columns["id"])

    def to_dict(self, **kwargs):
        return copy.deepcopy(self.columns)


class Stream:
    def __init__(self, batches, events):
        self.batches, self.events = iter(batches), events

    def __iter__(self):
        return self

    def __next__(self):
        self.events.append("next")
        return next(self.batches)

    def __enter__(self):
        self.events.append("enter")
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.events.append("close")


def build(batches, events):
    def collect_batches(*, chunk_size):
        assert chunk_size == 8192
        return Stream(batches, events)
    return lambda: SimpleNamespace(collect_batches=collect_batches)


class Contracts(unittest.TestCase):
    def test_original_consumer_bodies_match_corrected_reference(self):
        source = ast.parse((Path(d.__file__).parent / "workloads/csv_streaming_v0816.py").read_text())
        original = [n for n in ast.walk(source) if isinstance(n, ast.FunctionDef) and n.name == "consume"]
        current = ast.parse(Path(d.__file__).read_text())
        self.assertEqual(len(original), 2)
        for old, name in zip(original, ["consume_alopex", "consume_polars"]):
            new = next(n for n in current.body if isinstance(n, ast.FunctionDef) and n.name == name)
            self.assertEqual([ast.dump(n) for n in old.body], [ast.dump(n) for n in new.body])

    def test_consumers_count_late_batches_without_python_conversion(self):
        batches = [SimpleNamespace(height=2), SimpleNamespace(height=1)]
        for consume in [d.consume_alopex, d.consume_polars]:
            events = []
            def clock():
                events.append("clock")
                return next(clocks)
            clocks = iter([10.0, 11.0, 13.0])
            with patch.object(d.time, "perf_counter", side_effect=clock):
                self.assertEqual(consume(build(batches, events)), (1.0, 3.0, 3, 2))
            self.assertEqual(events.count("next"), 3)
            if consume is d.consume_alopex:
                self.assertEqual(events[-2:], ["clock", "close"])

    def test_alopex_closes_on_empty_stream_error(self):
        events = []
        with self.assertRaises(StopIteration):
            d.consume_alopex(build([], events))
        self.assertEqual(events[-1], "close")

    def test_validation_reads_every_batch_and_closes_on_error(self):
        expected = d.oracle(3)
        batches = [Batch({k: v[:2] for k, v in expected.items()}), Batch({k: v[2:] for k, v in expected.items()})]
        events = []
        self.assertEqual(d.validate_values(d.read_stream(build(batches, events)), expected), d.digest(expected))
        self.assertEqual(events[-1], "close")
        events.clear()
        bad = Batch({"label": ["row-2"], "id": [2]})
        with self.assertRaisesRegex(RuntimeError, "column order"):
            d.read_stream(build([batches[0], bad], events))
        self.assertEqual(events[-1], "close")

    def test_oracle_rejects_wrong_types_nulls_order_values_and_lengths(self):
        expected = d.oracle(3)
        for column, values in [("id", [False, 1, 2]), ("id", [0, 2, 1]), ("id", [0, 1]),
                               ("id", [0, 1, 1]), ("label", ["", "row-1", "row-2"]),
                               ("label", [None, "row-1", "wrong"])]:
            actual = copy.deepcopy(expected); actual[column] = values
            with self.assertRaises(RuntimeError):
                d.validate_values(actual, expected)
        with self.assertRaises(RuntimeError):
            d.validate_values(dict(reversed(list(expected.items()))), expected)

    def test_phase_observation_disposes_plan_inside_original_timer(self):
        events, records = [], []
        class Plan:
            def __del__(self):
                events.append("dispose")
        def clock():
            events.append("clock")
            return 1.0
        with patch.object(d.time, "perf_counter", side_effect=clock):
            d.observe_phase(Plan, "plan_sample", warmups=1, samples=1, record=records.append)
        self.assertEqual(events, ["clock", "dispose", "clock"]*2)
        self.assertEqual([r["phase"] for r in records], ["warmup", "measure"])

    def test_ci_rejected_before_native_import_or_workdir_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)/"unused"
            r = subprocess.run([sys.executable, "-B", d.__file__, "--manual", "--surface", "embedded",
                                "--rows", "3", "--operation", "alopex", "--work", str(work)],
                               env=dict(os.environ, CI="true"), capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("forbidden in CI", r.stderr)
            self.assertFalse(work.exists())


if __name__ == "__main__":
    unittest.main()
