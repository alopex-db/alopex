"""Small fake scans verify boundaries and complete consumption without native IO."""
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
import lazy_file_profile as d


class FakeOperations:
    def __init__(self, rows, events):
        self.rows, self.events = rows, events
        self.values = d.workload.oracle(d.workload.inputs(rows))["result"]
        self.api = SimpleNamespace(LazyFrame=SimpleNamespace(
            scan_csv=self.scan, scan_parquet=self.scan))

    def scan(self, path):
        self.events.append("scan")
        return self

    def next_plan(self, source):
        self.events.append("next-plan")
        return source

    def expressions(self):
        return list(self.values)

    def select_plan(self, source):
        return source.select(*self.expressions())

    def select(self, *names):
        self.events.append(("select", tuple(names)))
        self.selected = {n: self.values[n] for n in names}
        return self

    def batch(self, start, end):
        values = {k: v[start:end] for k, v in self.selected.items()}

        def convert(as_series=False):
            assert not as_series
            self.events.append("convert")
            return copy.deepcopy(values)
        return SimpleNamespace(to_dict=convert)

    def collect(self):
        self.events.append("collect")
        return self.batch(0, self.rows)

    def collect_batches(self, chunk_size):
        assert chunk_size == 8192
        self.events.append("collect-batches")
        outer = self

        class Stream:
            def __enter__(self):
                outer.events.append("enter")
                return self

            def __iter__(self):
                for start, end in [(0, 1), (1, outer.rows)]:
                    outer.events.append(("consume", start, end))
                    yield outer.batch(start, end)

            def __exit__(self, *args):
                outer.events.append("close")
        return Stream()


class Contracts(unittest.TestCase):
    def test_all_adopted_plans_match_pinned_original_runner(self):
        for case in d.OPERATIONS:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                fixture = root / ("rows." + case.split("-")[0])
                fixture.write_bytes(b"fake input; no native reader")
                original_events, adapted_events = [], []
                original = FakeOperations(3, original_events)
                with patch.object(d.workload, "Operations", return_value=original):
                    d.workload.measure("alopex", case, root, root / "raw", 3, 1, 1)
                adapted = FakeOperations(3, adapted_events)
                operation, snapshot, expected = d.prepare(adapted, case, fixture)
                d.measure(operation, snapshot, expected, warmups=1, samples=1, record=lambda _: None)
                self.assertEqual(adapted_events, original_events)

    def test_all_batches_consumed_inside_timer_conversion_outside(self):
        events = []
        operation, snapshot, expected = d.prepare(FakeOperations(3, events), "csv-batches", Path("fake.csv"))
        times = iter([100, 200])

        def clock():
            events.append("timer")
            return next(times)
        records = []
        d.measure(operation, snapshot, expected, warmups=0, samples=1, clock=clock, record=records.append)
        self.assertLess(events.index(("consume", 1, 3)), events.index("close"))
        self.assertEqual(events.count("timer"), 2)
        end = len(events) - 1 - events[::-1].index("timer")
        self.assertLess(events.index("close"), end)
        self.assertGreater(events.index("convert"), end)
        self.assertEqual(records[0]["total_ns"], 100)

    def test_full_values_types_nulls_order_and_late_batch_are_required(self):
        operation, snapshot, expected = d.prepare(FakeOperations(3, []), "csv-batches", Path("fake.csv"))
        with self.assertRaises(AssertionError):
            d.workload.assert_equal(snapshot(operation()[:1]), expected)
        for mutate in [
            lambda x: x["add"].__setitem__(-1, 99),
            lambda x: x["eq"].__setitem__(0, 1),
            lambda x: x["joined"].__setitem__(0, ""),
            lambda x: x["add"].reverse(),
        ]:
            actual = snapshot(operation())
            mutate(actual)
            with self.assertRaises(AssertionError):
                d.workload.assert_equal(actual, expected)
        reversed_values = dict(reversed(list(expected.items())))
        bad = SimpleNamespace(to_dict=lambda as_series=False: reversed_values)
        with self.assertRaisesRegex(RuntimeError, "batch column order mismatch"):
            snapshot([bad])

    def test_known_parquet_string_cases_stay_excluded(self):
        self.assertEqual(set(d.OPERATIONS), {"csv-collect", "csv-batches", "parquet-numeric-collect", "parquet-numeric-batches"})
        for case in ("parquet-collect", "parquet-batches", "unknown"):
            with self.assertRaises(ValueError):
                d.prepare(FakeOperations(3, []), case, Path("fake"))

    def test_ci_rejected_before_native_import_or_work_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "unused"
            result = subprocess.run([sys.executable, "-B", d.__file__, "--manual", "--surface", "embedded",
                                     "--rows", "3", "--operation", "csv-collect", "--work", str(work)],
                                    env=dict(os.environ, CI="true"), capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("forbidden in CI", result.stderr)
            self.assertFalse(work.exists())


if __name__ == "__main__":
    unittest.main()
