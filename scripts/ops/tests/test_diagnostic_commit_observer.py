import unittest

from scripts.ops.wal_cost_next.diagnostic_commit_observer import observe


class CompositionTests(unittest.TestCase):
    def test_original_timer_runs_once_inside_both_observers(self):
        events = []
        def action():
            events.append("commit")
            return "done"
        def measure(pending):
            events.append("timer-start")
            value = pending()
            events.append("timer-end")
            return value, 17
        def scheduler(pending, timer):
            events.append("sched-before")
            result = timer(pending)
            events.append("sched-after")
            return result, {"original_timer_ns": 17, "acceptance_sample": False}
        def baseline(pending, timer):
            events.append("cpu-before")
            result = timer(pending)
            events.append("cpu-after")
            return result, {"timer_nanoseconds": 17}
        result, record = observe(action, measure, baseline_observe=baseline,
                                 scheduler_observe=scheduler)
        self.assertEqual(events, ["sched-before", "cpu-before", "timer-start", "commit",
                                  "timer-end", "cpu-after", "sched-after"])
        self.assertEqual(result, ("done", 17))
        self.assertEqual(record["timer_nanoseconds"], record["schedstat"]["original_timer_ns"])
        self.assertFalse(record["schedstat"]["acceptance_sample"])

    def test_duplicate_timer_call_is_rejected(self):
        def baseline(action, timer):
            return timer(action), {}
        def scheduler(action, timer):
            timer(action)
            return timer(action), {"original_timer_ns": 1}
        with self.assertRaisesRegex(RuntimeError, "timer calls"):
            observe(lambda: None, lambda action: (action(), 1),
                    baseline_observe=baseline, scheduler_observe=scheduler)

    def test_timer_mismatch_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "timer differs"):
            observe(lambda: None, lambda action: (action(), 1),
                    baseline_observe=lambda action, timer: (timer(action), {}),
                    scheduler_observe=lambda action, timer:
                    (timer(action), {"original_timer_ns": 2}))


if __name__ == "__main__":
    unittest.main()
