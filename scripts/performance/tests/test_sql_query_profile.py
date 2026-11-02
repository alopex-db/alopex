import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
import sql_query_profile as diagnostic


class Transaction:
    def __init__(self, client):
        self.client = client
        client.live += 1

    def execute_sql(self, sql):
        self.client.events.append(sql)
        if self.client.fail:
            raise ValueError("injected execute failure")
        return [dict(id=3)]

    def rollback(self):
        self.client.events.append("rollback")

    def __del__(self):
        self.client.events.append("release")
        self.client.live -= 1


class Client:
    def __init__(self, fail=False):
        self.events = []
        self.live = 0
        self.fail = fail

    def begin(self, mode):
        if self.live:
            raise AssertionError("previous transaction still alive")
        self.events.append("begin")
        return Transaction(self)

    def execute_sql(self, sql):
        self.events.append(sql)
        return [dict(count=2)]


class QueryDiagnosticTests(unittest.TestCase):
    def test_read_uses_autocommit_and_times_full_public_call(self):
        client = Client()
        result, times = diagnostic.timed_operation(client, None, "aggregate", iter((5, 17)).__next__)
        self.assertEqual(client.events, ["SELECT COUNT(*) AS count FROM bench WHERE enabled = true"])
        self.assertEqual(result, [dict(count=2)])
        self.assertEqual(times, dict(execute_ns=12, total_ns=12))

    def test_insert_rolls_back_and_releases_before_next_begin(self):
        client = Client()
        result, times = diagnostic.timed_operation(client, None, "insert-rollback", iter((0, 3, 8, 15, 20)).__next__)
        self.assertEqual(result, [dict(id=3)])
        self.assertEqual(times, dict(begin_ns=3, execute_ns=5, rollback_ns=7, release_ns=5, total_ns=20))
        self.assertEqual(client.events, ["begin", "INSERT INTO perf_items VALUES (3,'returning') RETURNING id", "rollback", "release"])
        diagnostic.timed_operation(client, None, "insert-rollback")
        self.assertEqual(client.live, 0)

    def test_execute_failure_still_rolls_back_and_releases(self):
        client = Client(fail=True)
        with self.assertRaisesRegex(ValueError, "injected"):
            diagnostic.timed_operation(client, None, "insert-rollback")
        self.assertEqual(client.events[-2:], ["rollback", "release"])
        self.assertEqual(client.live, 0)

    def test_only_unordered_query_can_reorder_results(self):
        expected = diagnostic.expected_result("select-all", 3)
        diagnostic.validate_result(list(reversed(expected)), "select-all", expected)
        with self.assertRaises(RuntimeError):
            diagnostic.validate_result(list(reversed(expected)), "ordered", expected)
        for bad in ([dict(id=False, value=0.0), dict(id=2, value=2.0)],
                    [dict(id=0, value=None), dict(id=2, value=2.0)],
                    expected+expected, expected[:1]):
            with self.assertRaises(RuntimeError):
                diagnostic.validate_result(bad, "select-all", expected)

    def test_small_fixture_and_original_generators(self):
        client = Client()
        diagnostic.populate(client, "aggregate", 3)
        self.assertEqual(client.events, [
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value DOUBLE, enabled BOOLEAN)",
            "INSERT INTO bench VALUES (0,0.0,true),(1,1.0,false),(2,2.0,true)"])
        client = Client()
        diagnostic.populate(client, "insert-rollback", 1)
        self.assertEqual(client.events[-1], "INSERT INTO perf_items VALUES (1,'old')")
        self.assertEqual(diagnostic.fixture_rows("insert-rollback", 34250), 1)
        self.assertEqual(diagnostic.fixture_rows("aggregate", 34250), 34250)

    def test_ci_guard_precedes_native_import_or_output_creation(self):
        for script in ("sql_query_profile.py", "run_sql_query_profile.py"):
            with self.subTest(script=script), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "must-not-exist"
                args = (["--manual", "--surface", "embedded", "--rows", "3", "--operation", "aggregate", "--work", str(output)]
                        if script == "sql_query_profile.py" else
                        ["--python", sys.executable, "--server", "/nonexistent", "--output", str(output)])
                result = subprocess.run([sys.executable, "-B", str(Path(diagnostic.__file__).with_name(script)), *args],
                                        env=dict(os.environ, CI="true"), capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("CI", result.stderr)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
