"""Focused positive and negative checks for partial sync observations."""

import tempfile
from pathlib import Path
import unittest
from unittest import mock

from scripts.ops.wal_cost_next import sync_capture


def row(operation, *, result=0, duration=11):
    return dict(operation=operation, result=result, errno=0, duration_ns=duration)


class SyncCaptureTests(unittest.TestCase):
    def test_branch_local_sql2048_counts_remain_partial(self):
        prefix = [row(1)]
        for subject, calls in (("before", [row(1), row(1), row(2), row(2)]),
                               ("candidate", [row(1), row(1), row(2)])):
            with self.subTest(subject=subject):
                record = sync_capture.commit_calls(prefix, prefix + calls, subject)
                self.assertEqual(record["calls"], calls)
                self.assertEqual(record["coverage"], "unproven")
                self.assertFalse(record["acceptance_sample"])

    def test_missing_or_invalid_calls_fail(self):
        before = [row(2)]
        for after in ([row(1), row(1), row(2)],
                      before + [row(1), row(2)],
                      before + [row(1), row(1), row(2), row(3)],
                      before + [row(1), row(1), row(2, duration=sync_capture.INVALID_DURATION)],
                      before + [row(1), row(1), row(2, duration=30_000_000_001)],
                      before + [row(1), row(1), row(2, result=-1)]):
            with self.subTest(after=after), self.assertRaises(RuntimeError):
                sync_capture.commit_calls(before, after, "candidate")

    def test_shim_identity_stops_before_load(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "shim.so"
            source.write_bytes(b"shim")
            with mock.patch.dict(sync_capture.os.environ, {"LD_PRELOAD": str(source)}), \
                 mock.patch.object(sync_capture.ctypes, "CDLL") as loader:
                with self.assertRaisesRegex(RuntimeError, "identity differs"):
                    sync_capture.open_shim(source, "0" * 64, lambda _: "1" * 64)
            loader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
