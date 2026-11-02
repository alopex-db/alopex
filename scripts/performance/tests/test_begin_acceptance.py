"""Bounded monitor failures must stop and reap the owned probe process."""
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    'begin_acceptance_runner', Path(__file__).resolve().parents[1] / 'run_begin_acceptance.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class MonitorTests(unittest.TestCase):
    def test_monitor_failures_reap_owned_child(self):
        cases = [
            ('missing RSS', 0, '', 10_000_000_000, [0, 0], 'RSS measurement failed'),
            ('failed RSS', 1, '', 10_000_000_000, [0, 0], 'RSS measurement failed'),
            ('zero RSS', 0, '0', 10_000_000_000, [0, 0], 'RSS measurement failed'),
            ('large RSS', 0, '2097152', 10_000_000_000, [0, 0], 'RSS limit'),
            ('storage growth', 0, '1', 12_000_000_001, [0], 'storage limit'),
            ('timeout', 0, '1', 10_000_000_000, [0, 91], '90-second'),
        ]
        for name, code, rss, used, times, expected in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                child = Mock(pid=123, returncode=-9)
                child.poll.return_value = None
                if expected == 'RSS measurement failed':
                    child.wait.side_effect = [runner.subprocess.TimeoutExpired('fake', .05), -9]
                ticks = iter(times)
                with patch.object(runner.subprocess, 'Popen', return_value=child), \
                     patch.object(runner.subprocess, 'run', return_value=SimpleNamespace(returncode=code, stdout=rss)), \
                     patch.object(runner.shutil, 'disk_usage', return_value=SimpleNamespace(used=used, free=100 * 1024**3)), \
                     patch.object(runner.time, 'monotonic', side_effect=lambda: next(ticks, 92)):
                    result = runner.guarded(['fake'], Path(directory), 'probe', 16_000_000_000, 10_000_000_000)
                self.assertIn(expected, result['failure'])
                child.kill.assert_called_once()
                self.assertEqual(child.wait.call_count, 2 if expected == 'RSS measurement failed' else 1)

    def test_exit_race_is_reaped_and_not_reported_as_missing_live_rss(self):
        with tempfile.TemporaryDirectory() as directory:
            child = Mock(pid=123, returncode=0)
            child.poll.return_value = None
            with patch.object(runner.subprocess, 'Popen', return_value=child), \
                 patch.object(runner.subprocess, 'run', return_value=SimpleNamespace(returncode=1, stdout='')), \
                 patch.object(runner.shutil, 'disk_usage', return_value=SimpleNamespace(used=0, free=100 * 1024**3)):
                result = runner.guarded(['fake'], Path(directory), 'probe', 0, 0)
            self.assertIsNone(result['failure'])
            child.kill.assert_not_called()
            child.wait.assert_called_once_with(timeout=.05)

    def test_child_failure_is_not_success(self):
        with tempfile.TemporaryDirectory() as directory:
            child = Mock(returncode=2)
            child.poll.return_value = 2
            with patch.object(runner.subprocess, 'Popen', return_value=child):
                result = runner.guarded(['fake'], Path(directory), 'probe', 0, 0)
            self.assertEqual(result['failure'], 'child exit 2')
            child.kill.assert_not_called()
            child.wait.assert_called_once()


if __name__ == '__main__':
    unittest.main()
