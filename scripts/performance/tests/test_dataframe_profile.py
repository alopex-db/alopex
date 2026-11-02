"""Tiny phase/validation contracts; no native engine or performance loop."""
import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dataframe_profile as d


class Result:
    def __init__(self, values):
        self.values = values

    def to_dict(self, as_series=False):
        return copy.deepcopy(self.values)


class Operations:
    def frame(self, columns):
        return copy.deepcopy(columns)

    def compute(self, frame):
        return Result(d.workload.oracle(frame)["result"])

    def snapshot(self, frame, result=None, converted=None):
        expected = d.workload.oracle(frame)
        if result is not None:
            expected["result"] = result.to_dict(as_series=False)
        if converted is not None:
            expected["result"] = converted
        return expected


class Contracts(unittest.TestCase):
    def test_phase_timers_exclude_setup_and_validation(self):
        for case, wanted in {
            "construct": ["timer", "frame", "timer", "snapshot"],
            "compute": ["frame", "timer", "compute", "timer", "snapshot"],
            "convert": ["frame", "compute", "timer", "convert", "timer", "snapshot"],
            "complete": ["timer", "frame", "snapshot", "timer"],
        }.items():
            events = []

            class TaggedResult(Result):
                def to_dict(self, as_series=False):
                    events.append("convert")
                    return super().to_dict(as_series=as_series)

            class TaggedOperations(Operations):
                def frame(self, columns):
                    events.append("frame")
                    return super().frame(columns)

                def compute(self, frame):
                    events.append("compute")
                    return TaggedResult(d.workload.oracle(frame)["result"])

                def snapshot(self, frame, result=None, converted=None):
                    events.append("snapshot")
                    expected = d.workload.oracle(frame)
                    expected["result"] = (converted if converted is not None else
                                          result.values if result is not None else expected["result"])
                    return expected

            def clock():
                events.append("timer")
                return len(events)

            columns = d.workload.inputs(4)
            operation, snapshot = d.prepare(TaggedOperations(), case, columns)
            d.measure(operation, snapshot, d.workload.oracle(columns), warmups=0, samples=1,
                      clock=clock, record=lambda _: None)
            self.assertEqual(events, wanted, case)

    def test_every_small_sample_has_full_original_snapshot(self):
        columns = d.workload.inputs(4)
        for case in d.OPERATIONS:
            operation, snapshot = d.prepare(Operations(), case, columns)
            records = []
            count = d.measure(operation, snapshot, d.workload.oracle(columns), warmups=1,
                              samples=2, record=records.append)
            self.assertEqual(count, 3)
            self.assertEqual([r["phase"] for r in records], ["warmup", "measure", "measure"])
            self.assertEqual([r["iteration"] for r in records], [0, 1, 2])

    def test_results_are_released_outside_the_next_timer(self):
        timing = [False]
        destructions = []

        class Tracked:
            def __del__(self):
                destructions.append(timing[0])

        def clock():
            timing[0] = not timing[0]
            return 0

        d.measure(Tracked, lambda _: {}, {}, warmups=0, samples=2, clock=clock, record=lambda _: None)
        self.assertEqual(destructions, [False, False])

    def test_original_oracle_checks_nulls_types_order_and_all_columns(self):
        expected = d.workload.oracle(d.workload.inputs(4))
        self.assertEqual(expected["result"]["joined"], [None, "row-1!", "row-2!", "row-3!"])
        self.assertEqual(expected["explode"]["id"], [0, 0, 1, 1, 2, 2, 3, 3])
        for path, value in [(('result', 'eq', 0), 1), (('explode', 'items', -1), 'wrong'),
                            (('input', 'label', 0), ''), (('result', 'add', 0), 99)]:
            actual = copy.deepcopy(expected)
            actual[path[0]][path[1]][path[2]] = value
            with self.assertRaises(AssertionError):
                d.workload.assert_equal(actual, expected)
        actual = dict(reversed(list(expected.items())))
        with self.assertRaises(AssertionError):
            d.workload.assert_equal(actual, expected)

    def test_unknown_phase_rejected(self):
        with self.assertRaises(ValueError):
            d.prepare(Operations(), 'unknown', d.workload.inputs(3))

    def test_driver_refuses_ci_before_loading_native_or_creating_data(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp) / 'work'
            r = subprocess.run([sys.executable, '-B', d.__file__, '--manual', '--surface', 'embedded',
                                '--rows', '3', '--operation', 'compute', '--work', str(work)],
                               env=dict(os.environ, CI='1'), capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn('forbidden in CI', r.stderr)
            self.assertFalse(work.exists())


if __name__ == '__main__':
    unittest.main()
