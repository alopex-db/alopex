"""Offline tests for the pinned in-container SQL supervisor."""
import unittest

from scripts.ops.final_sql_runtime import supervisor


def require(condition, code):
    if not condition:
        raise ValueError(code)


class PhaseTests(unittest.TestCase):
    def setUp(self):
        self.config = {"schema": "alopex.native-sql-supervisor/v1",
                       "id": "sql-select-all-0-published", "case": "select-all",
                       "trial": 0, "subject": "published", "engine": "alopex-server"}

    def test_preflight_and_cell_are_propagated(self):
        for phase in ("preflight", "cell"):
            with self.subTest(phase=phase):
                self.assertEqual(supervisor.validate_phase(dict(self.config, phase=phase), require), phase)

    def test_rejects_missing_phase_and_non_select_preflight(self):
        for changed in ({}, {"phase": "unknown"}, {"phase": "preflight", "case": "ordered"},
                        {"phase": "cell", "subject": "postgresql"},
                        {"phase": "cell", "id": "sql-select-all-0-before"}):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, "INPUT"):
                config = dict(self.config, **changed)
                supervisor.validate_phase(config, require)


class OwnedServerTests(unittest.TestCase):
    class Child:
        pid = 123

        def poll(self):
            return 0

    class Server:
        child = None

        def __init__(self, events, fail_stop=False):
            self.events = events
            self.fail_stop = fail_stop
            self.child = OwnedServerTests.Child()

        def stop(self):
            self.events.append("stop")
            if self.fail_stop:
                raise RuntimeError("stop failed")

    def test_worker_failure_still_stops_owned_server(self):
        events, reports = [], []
        server = self.Server(events)

        def fail(*_):
            events.append("measure")
            raise RuntimeError("worker failed")

        self.assertFalse(supervisor.run_owned_server(lambda: (server, None), fail, reports.append))
        self.assertEqual(events, ["measure", "stop"])
        self.assertEqual(reports[0]["status"], "failed")
        self.assertIn("worker failed", reports[0]["worker_error"])
        self.assertTrue(reports[0]["server_stopped"])

    def test_stop_failure_is_preserved_with_worker_failure(self):
        reports = []
        server = self.Server([], fail_stop=True)

        def fail(*_):
            raise RuntimeError("worker failed")

        self.assertFalse(supervisor.run_owned_server(lambda: (server, None), fail, reports.append))
        self.assertIn("worker failed", reports[0]["worker_error"])
        self.assertIn("stop failed", reports[0]["stop_error"])

    def test_start_failure_reports_without_stop(self):
        reports = []

        def fail():
            raise RuntimeError("start failed")

        self.assertFalse(supervisor.run_owned_server(fail, lambda *_: None, reports.append))
        self.assertIn("start failed", reports[0]["worker_error"])
        self.assertIsNone(reports[0]["server_identity"])


if __name__ == "__main__":
    unittest.main()
