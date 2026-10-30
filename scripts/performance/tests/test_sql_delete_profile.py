import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("diagnostic", Path(__file__).parents[1] / "sql_delete_profile.py")
diagnostic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostic)


class Transaction:
    def __init__(self, client):
        self.client = client
        client.live += 1

    def execute_sql(self, sql):
        self.client.events.append("execute")
        if self.client.fail:
            raise ValueError("injected execute failure")
        return []

    def commit(self):
        self.client.events.append("commit")

    def rollback(self):
        self.client.events.append("rollback")

    def __del__(self):
        self.client.events.append("destroy")
        self.client.live -= 1


class Client:
    def __init__(self, fail=False):
        self.events = []
        self.live = 0
        self.fail = fail

    def begin(self, mode):
        if self.live:
            raise AssertionError("previous handle alive at next begin")
        self.events.append("begin")
        return Transaction(self)


class DiagnosticTests(unittest.TestCase):
    def test_phases_exclude_handle_destruction_and_restore(self):
        client = Client()
        ticks = iter([0, 3, 11, 20, 25])
        def clock():
            client.events.append("clock")
            return next(ticks)
        result, intervals = diagnostic.timed_operation(client, None, 1, "delete", clock)
        self.assertEqual(intervals, dict(begin_ns=3, execute_ns=8, commit_ns=9, release_ns=5, total_ns=25))
        self.assertEqual(client.events, ["clock", "begin", "clock", "execute", "clock", "commit", "clock", "destroy", "clock"])
        diagnostic.write(client, None, ["restore"])
        self.assertEqual(client.live, 0)

    def test_execution_failure_rolls_back_and_releases_handle(self):
        client = Client(fail=True)
        with self.assertRaisesRegex(ValueError, "injected"):
            diagnostic.timed_operation(client, None, 1, "delete")
        self.assertEqual(client.events, ["begin", "execute", "rollback", "destroy"])
        self.assertEqual(client.live, 0)

    def test_validation_rejects_wrong_values_types_and_rows(self):
        for actual in [[dict(id=True, value="a")], [dict(id=1, value="b")], []]:
            with self.assertRaises(RuntimeError):
                diagnostic.validate_rows(actual, [dict(id=1, value="a")])

    def test_seed_and_restore_failures_do_not_keep_transaction(self):
        client = Client(fail=True)
        with self.assertRaises(ValueError):
            diagnostic.write(client, None, ["seed"])
        self.assertEqual(client.events, ["begin", "execute", "rollback", "destroy"])
        self.assertEqual(client.live, 0)


if __name__ == "__main__":
    unittest.main()
