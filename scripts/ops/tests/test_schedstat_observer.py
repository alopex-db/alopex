import tempfile
from pathlib import Path
import unittest

from scripts.ops.wal_cost_next.schedstat_observer import observe, parse_schedstat, snapshot


class SchedstatObserverTests(unittest.TestCase):
    def test_parse_rejects_malformed_or_negative_counter(self):
        for raw in ("1 2", "1 2 3 4", "1 -2 3", "1 two 3", "1 ２ 3"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_schedstat(raw)

    def test_snapshot_reads_each_task_outside_measure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for tid, value in (("12", "1 2 3\n"), ("4", "5 6 7\n")):
                task = root / tid
                task.mkdir()
                (task / "schedstat").write_text(value)
            ticks = iter((10, 20))
            got = snapshot(root, clock=lambda: next(ticks))
            self.assertEqual(list(got["tasks"]), ["4", "12"])
            self.assertEqual(got["tasks"]["12"], (1, 2, 3))
            self.assertEqual((got["start_ns"], got["end_ns"]), (10, 20))

    def test_original_measure_is_bracketed_and_stable_threads_sum(self):
        events = []
        states = iter((
            {"start_ns": 1, "end_ns": 2, "tasks": {"1": (10, 20, 3), "2": (2, 4, 1)}},
            {"start_ns": 5, "end_ns": 6, "tasks": {"1": (15, 27, 4), "2": (4, 8, 2)}},
        ))
        def take():
            events.append("read")
            return next(states)
        def measure(action):
            events.append("timer-start")
            result = action()
            events.append("timer-end")
            return result, 19
        result, record = observe(lambda: events.append("commit"), measure, take_snapshot=take)
        self.assertEqual(events, ["read", "timer-start", "commit", "timer-end", "read"])
        self.assertEqual(result[1], 19)
        self.assertEqual(record["matched_task_totals"],
                         {"runtime_ns": 7, "runqueue_wait_ns": 11, "timeslices": 2})
        self.assertFalse(record["acceptance_sample"])

    def test_new_thread_prevents_process_total_claim(self):
        states = iter((
            {"start_ns": 1, "end_ns": 2, "tasks": {"1": (1, 2, 3)}},
            {"start_ns": 4, "end_ns": 5, "tasks": {"1": (2, 3, 4), "2": (1, 1, 1)}},
        ))
        _, record = observe(lambda: None, lambda action: (action(), 1),
                            take_snapshot=lambda: next(states))
        self.assertFalse(record["task_set_stable"])
        self.assertIsNone(record["matched_task_totals"])

    def test_decreasing_counter_fails(self):
        states = iter((
            {"start_ns": 1, "end_ns": 2, "tasks": {"1": (5, 5, 5)}},
            {"start_ns": 4, "end_ns": 5, "tasks": {"1": (4, 6, 6)}},
        ))
        with self.assertRaisesRegex(RuntimeError, "decreased"):
            observe(lambda: None, lambda action: (action(), 1),
                    take_snapshot=lambda: next(states))


if __name__ == "__main__":
    unittest.main()
