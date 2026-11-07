import unittest

from native_sql_measure_supervisor import run_owned_server


class SupervisorTests(unittest.TestCase):
    def run_case(self, start_error=None, worker_error=None, stop_error=None):
        events, saved = [], []
        class Server:
            def stop(self):
                events.append("stop")
                if stop_error: raise stop_error
        server = Server()
        def start():
            events.append("start")
            if start_error: raise start_error
            return server, {"http": 1}
        def measure(actual, endpoint):
            self.assertIs(actual, server)
            self.assertEqual(endpoint, {"http": 1})
            events.append("measure")
            if worker_error: raise worker_error
            return {"raw_sha256": "a" * 64}
        def save(report):
            events.append("save")
            saved.append(report)
        passed = run_owned_server(start, measure, save)
        return passed, events, saved[0]

    def test_success_requires_stop_before_report(self):
        passed, events, report = self.run_case()
        self.assertTrue(passed)
        self.assertEqual(events, ["start", "measure", "stop", "save"])
        self.assertEqual(report["status"], "passed")

    def test_start_failure_never_measures(self):
        passed, events, report = self.run_case(start_error=RuntimeError("start failed"))
        self.assertFalse(passed)
        self.assertEqual(events, ["start", "save"])
        self.assertIn("start failed", report["worker_error"])

    def test_worker_failure_still_stops_server(self):
        passed, events, report = self.run_case(worker_error=RuntimeError("worker failed"))
        self.assertFalse(passed)
        self.assertEqual(events, ["start", "measure", "stop", "save"])
        self.assertIn("worker failed", report["worker_error"])
        self.assertIsNone(report["stop_error"])

    def test_stop_failure_is_not_measurement_success(self):
        passed, events, report = self.run_case(stop_error=RuntimeError("stop failed"))
        self.assertFalse(passed)
        self.assertIsNone(report["worker_error"])
        self.assertIn("stop failed", report["stop_error"])

    def test_original_and_cleanup_failures_both_survive(self):
        passed, events, report = self.run_case(worker_error=ValueError("bad result"),
                                             stop_error=RuntimeError("stop failed"))
        self.assertFalse(passed)
        self.assertIn("bad result", report["worker_error"])
        self.assertIn("stop failed", report["stop_error"])

    def test_interrupted_worker_still_stops_server(self):
        passed, events, report = self.run_case(worker_error=KeyboardInterrupt())
        self.assertFalse(passed)
        self.assertEqual(events, ["start", "measure", "stop", "save"])
        self.assertIn("KeyboardInterrupt", report["worker_error"])


if __name__ == "__main__":
    unittest.main()
