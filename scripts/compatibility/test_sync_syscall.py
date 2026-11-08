"""Tiny trace contracts, with no native module, strace or workload execution."""
import importlib.util
from pathlib import Path
import sys
import subprocess
from types import ModuleType
import unittest
from unittest.mock import Mock, patch


def load_subject():
    modules = {}
    for name, attrs in {
        "alopex": ("TxnMode", "_alopex"),
        "publication_python": ("expected_error", "open_db", "verify", "verify_indexes"),
        "spool_os_write": ("pending",),
    }.items():
        module = ModuleType(name)
        for attr in attrs:
            setattr(module, attr, None)
        modules[name] = module
    spec = importlib.util.spec_from_file_location("sync_syscall_subject", Path(__file__).with_name("sync_syscall.py"))
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


subject = load_subject()


class SyncTrace(unittest.TestCase):
    def events(self, count=1):
        return [dict(syscall="fdatasync", path="fixture.alopex.d/lsm.wal", result="0",
                     ordinal=n+1, during_commit=True) for n in range(count)]

    def test_single_reference_sync_is_the_injection_target(self):
        target = subject.select_target(self.events(), Path("fixture.alopex"), "reference_append")
        self.assertEqual(target["ordinal"], 1)
        self.assertEqual(subject.STAGES, ("spool", "directory", "reference_append"))

    def test_missing_or_duplicate_reference_sync_rejected(self):
        for count in (0, 2):
            with self.subTest(count=count), self.assertRaises(AssertionError):
                subject.select_target(self.events(count), Path("fixture.alopex"), "reference_append")

    def test_wrong_file_or_outside_commit_rejected(self):
        for key, value in (("path", "another.alopex.d/lsm.wal"), ("during_commit", False)):
            events = self.events(); events[0][key] = value
            with self.subTest(key=key), self.assertRaises(AssertionError):
                subject.select_target(events, Path("fixture.alopex"), "reference_append")

    def test_removed_force_boundary_is_not_silently_remapped(self):
        with self.assertRaises(ValueError):
            subject.select_target(self.events(), Path("fixture.alopex"), "reference_force")

    def test_interruption_and_timeout_kill_and_reap_tracer(self):
        for failure in (InterruptedError("cancelled"), subprocess.TimeoutExpired("strace", 30)):
            process = Mock(pid=123)
            process.poll.return_value = None
            process.communicate.side_effect = [failure, (b"interrupted", None)]
            log = Mock()
            with self.subTest(failure=type(failure).__name__), \
                 patch.object(subject.subprocess, "Popen", return_value=process), \
                 patch.object(subject.os, "killpg") as kill, \
                 self.assertRaises(type(failure)):
                subject.traced_child(Path("db"), "kv", "baseline", Path("trace"), log)
            kill.assert_called_once_with(123, subject.signal.SIGKILL)
            self.assertEqual(process.communicate.call_args_list[-1].kwargs, {"timeout": 10})
            log.write_bytes.assert_called_once_with(b"interrupted")

    def test_already_exited_tracer_is_reaped_without_signalling_reused_pid(self):
        process = Mock(pid=123)
        process.poll.return_value = 1
        process.communicate.side_effect = [InterruptedError("cancelled"), (b"ended", None)]
        with patch.object(subject.subprocess, "Popen", return_value=process), \
             patch.object(subject.os, "killpg") as kill, self.assertRaises(InterruptedError):
            subject.traced_child(Path("db"), "kv", "baseline", Path("trace"), Mock())
        kill.assert_not_called()
        self.assertEqual(process.communicate.call_count, 2)


if __name__ == "__main__":
    unittest.main()
