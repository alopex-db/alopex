import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
import kv_read_profile as diagnostic


class Transaction:
    def __init__(self, client):
        self.client = client

    def get(self, key):
        self.client.events.append(("get", key))
        if self.client.fail:
            raise ValueError("injected read failure")
        return b"value"

    def scan_range(self, start, end):
        self.client.events.append(("scan", start, end))
        def iterator():
            yield (b"k", b"value")
            self.client.events.append("fully-consumed")
            if self.client.fail:
                raise ValueError("injected scan failure")
        return iterator()

    def rollback(self):
        self.client.events.append("rollback")


class Client:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail

    def begin(self, mode):
        self.events.append(("begin", mode))
        return Transaction(self)


class KVProfileTests(unittest.TestCase):
    def test_fixed_byte_encoding_and_seeded_requests(self):
        self.assertEqual(diagnostic.key_for(1), b"k:\x00\x00\x00\x00\x00\x00\x00\x01")
        self.assertEqual(diagnostic.value_for(1), b"\x01\x00\x00\x00\x00\x00\x00\x00"*4)
        hit = diagnostic.plans("random-get-hit", 3)
        miss = diagnostic.plans("random-get-miss", 3)
        self.assertEqual(len(hit), 24)
        self.assertTrue(all(len(r) == 100 for r in hit))
        self.assertEqual([int.from_bytes(k[2:], "big") for k, _ in hit[0][:6]], [1, 0, 1, 2, 0, 0])
        self.assertTrue(all(int.from_bytes(b[0][2:], "big")-int.from_bytes(a[0][2:], "big") == 3
                            for left, right in zip(hit, miss) for a, b in zip(left, right)))
        ranges = diagnostic.plans("range-scan", 3)
        self.assertEqual(ranges, [[(diagnostic.key_for(0), diagnostic.key_for(3))]]*24)

    def test_each_point_has_its_own_session_and_complete_timing(self):
        client = Client()
        result, timing = diagnostic.timed_operation(client, "RO", "random-get-hit",
                                                    [(b"a", b""), (b"b", b"")], iter((2, 17)).__next__)
        self.assertEqual(result, [b"value", b"value"])
        self.assertEqual(timing, dict(total_ns=15, execute_ns=15))
        self.assertEqual(client.events, [("begin", "RO"), ("get", b"a"), "rollback",
                                        ("begin", "RO"), ("get", b"b"), "rollback"])

    def test_range_is_consumed_before_rollback(self):
        client = Client()
        result, _ = diagnostic.timed_operation(client, "RO", "range-scan", [(b"a", b"z")])
        self.assertEqual(result, [(b"k", b"value")])
        self.assertEqual(client.events, [("begin", "RO"), ("scan", b"a", b"z"), "fully-consumed", "rollback"])

    def test_errors_still_rollback_the_current_session(self):
        for operation in ("random-get-hit", "range-scan"):
            with self.subTest(operation=operation):
                client = Client(fail=True)
                with self.assertRaisesRegex(ValueError, "injected"):
                    diagnostic.timed_operation(client, "RO", operation, [(b"a", b"z")])
                self.assertEqual(client.events[-1], "rollback")

    def test_empty_begin_control_does_not_issue_reads(self):
        client = Client()
        result, _ = diagnostic.timed_operation(client, "RO", "begin-rollback", [(b"a", b""), (b"b", b"")])
        self.assertEqual(result, [])
        self.assertEqual(client.events, [("begin", "RO"), "rollback"]*2)

    def test_full_value_order_and_copied_byte_types(self):
        good = [(b"a", b"1"), (b"b", b"2")]
        diagnostic.validate(good, good)
        for bad in (good[:1], list(reversed(good)), [(b"a", b"x"), good[1]],
                    [(bytearray(b"a"), b"1"), good[1]]):
            with self.assertRaises(RuntimeError):
                diagnostic.validate(bad, good)
        diagnostic.validate([None], [None])
        with self.assertRaises(RuntimeError):
            diagnostic.validate([bytearray(b"1")], [b"1"])

    def test_ci_guard_runs_before_native_import_and_output_creation(self):
        for script in ("kv_read_profile.py", "run_kv_read_profile.py"):
            with tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "absent"
                args = (["--manual", "--surface", "embedded", "--rows", "3", "--operation", "range-scan", "--work", str(output)]
                        if script == "kv_read_profile.py" else ["--python", sys.executable, "--output", str(output)])
                r = subprocess.run([sys.executable, "-B", str(Path(diagnostic.__file__).with_name(script)), *args],
                                   env=dict(os.environ, CI="true"), capture_output=True, text=True)
                self.assertNotEqual(r.returncode, 0)
                self.assertIn("CI", r.stderr)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
