"""Stdlib-only process lifecycle checks; no database extension is imported."""
import ctypes
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from fork_lock_runner import run_probe


class ForkLockRunnerTests(unittest.TestCase):
    @contextmanager
    def probe_identity(self):
        # Test-only adoption lets us reap descendants even without a reaping
        # container init. Keep the PID file alive through failure cleanup.
        libc = ctypes.CDLL(None, use_errno=True)
        previous = ctypes.c_int()
        self.assertEqual(libc.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        reaped = []
        try:
            with tempfile.TemporaryDirectory(prefix="fork-runner-") as root:
                identity = Path(root) / "pids"
                try:
                    yield identity, reaped
                finally:
                    try:
                        pids = list(map(int, identity.read_text().split()))
                    except FileNotFoundError:
                        pids = []
                    for pid in pids:  # Leader first, then adopted descendant.
                        self.assertGreater(pid, 0)
                        try:
                            waited, status = os.waitpid(pid, os.WNOHANG)
                        except ChildProcessError:
                            continue  # No ownership: never signal a reused PID.
                        if not waited:
                            os.kill(pid, signal.SIGKILL)
                            _, status = os.waitpid(pid, 0)
                        reaped.append((pid, status))
        finally:
            self.assertEqual(libc.prctl(36, previous.value, 0, 0, 0), 0)

    def test_normal_result_preserves_output_and_exit(self):
        result = run_probe([
            sys.executable, "-c",
            "import sys; print('out'); print('err', file=sys.stderr); sys.exit(7)",
        ], timeout=5)
        self.assertEqual((result.returncode, result.stdout, result.stderr), (7, "out\n", "err\n"))

    @unittest.skipUnless(sys.platform == "linux", "Linux subreaper verifies orphan reaping")
    def test_timeout_kills_fork_descendant_and_reaps_direct_child(self):
        with self.probe_identity() as (identity, _):
            code = (
                "import os,time,pathlib,sys; child=os.fork(); "
                "pathlib.Path(sys.argv[1]).write_text(f'{os.getpid()} {child}') "
                "if child else None; print('ready', flush=True); time.sleep(60)"
            )
            with self.assertRaises(subprocess.TimeoutExpired) as raised:
                run_probe([sys.executable, "-c", code, str(identity)], timeout=2)
            leader, child = map(int, identity.read_text().split())
            self.assertIn(b"ready", raised.exception.output)
            self.assertGreater(child, 0)
            # communicate() must already have reaped the direct child.
            with self.assertRaises(ChildProcessError):
                os.waitpid(leader, os.WNOHANG)
            deadline = time.monotonic() + 5
            while True:
                waited, status = os.waitpid(child, os.WNOHANG)
                if waited:
                    self.assertTrue(os.WIFSIGNALED(status))
                    self.assertEqual(os.WTERMSIG(status), signal.SIGKILL)
                    break
                self.assertLess(time.monotonic(), deadline, "descendant survived group kill")
                time.sleep(0.01)
            with self.assertRaises(ProcessLookupError):
                os.killpg(leader, 0)

    @unittest.skipUnless(sys.platform == "linux", "Linux subreaper verifies orphan reaping")
    def test_assertion_before_pid_read_still_reaps_descendant(self):
        with self.assertRaisesRegex(AssertionError, "TimeoutExpired not raised"):
            with self.probe_identity() as (identity, reaped):
                # The leader exits normally but leaves its child alive without
                # captured pipes. assertRaises fails before the caller reads IDs.
                code = (
                    "import os,time,pathlib,sys\n"
                    "child=os.fork()\n"
                    "if child:\n"
                    " pathlib.Path(sys.argv[1]).write_text(f'{os.getpid()} {child}')\n"
                    " print(os.getpid(), flush=True)\n"
                    "else:\n"
                    " os.close(1); os.close(2); time.sleep(60)\n"
                )
                with self.assertRaises(subprocess.TimeoutExpired):
                    result = run_probe([sys.executable, "-c", code, str(identity)], timeout=2)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(reaped), 1)
        _, status = reaped[0]
        self.assertTrue(os.WIFSIGNALED(status))
        self.assertEqual(os.WTERMSIG(status), signal.SIGKILL)
        with self.assertRaises(ProcessLookupError):
            os.killpg(int(result.stdout), 0)


if __name__ == "__main__":
    unittest.main()
