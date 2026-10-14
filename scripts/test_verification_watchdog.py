import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from host_resources import HostMonitor

from verification_watchdog import ResourcePolicy, keep_watchdog_alive, run_guarded


SAFE = {'host_total_bytes': 64 << 30, 'host_available_bytes': 32 << 30,
        'host_commit_available_bytes': 32 << 30, 'host_cpu_busy': 0.1,
        'wsl_available_bytes': 32 << 30}


class Observations:
    def __init__(self, second=None):
        self.second = second
        self.calls = 0
        self.closed = False

    def read(self):
        self.calls += 1
        if self.calls > 1:
            time.sleep(0.3)
            if isinstance(self.second, BaseException):
                raise self.second
            if self.second is not None:
                return dict(SAFE, **self.second)
        return dict(SAFE)

    def close(self):
        self.closed = True


class WatchdogTests(unittest.TestCase):
    def test_host_fresh_responses_do_not_accumulate_cross_clock_drift(self):
        monitor = HostMonitor()
        monitor.process = Mock()
        monitor.selector = Mock()
        monitor.selector.select.side_effect = [[True], [True]]
        monitor.process.stdout.read.side_effect = [
            json.dumps(dict(SAFE, request_id=1, observed_elapsed_ms=0)).encode() + b'\n',
            json.dumps(dict(SAFE, request_id=2, observed_elapsed_ms=254_000)).encode() + b'\n',
        ]
        with patch('host_resources.time.monotonic', side_effect=[0, 0, 0, 260, 260, 260]):
            monitor.read()
            sample = monitor.read()
        self.assertLessEqual(sample['observation_age_seconds'], 5)
        self.assertEqual(monitor.process.stdin.write.call_args_list[-1].args, (b'2\n',))

    def test_host_ignores_queued_response_from_previous_request(self):
        monitor = HostMonitor()
        monitor.process = Mock()
        monitor.selector = Mock()
        monitor.selector.select.return_value = [True]
        monitor.process.stdout.read.return_value = b'\n'.join(
            json.dumps(dict(SAFE, request_id=value)).encode() for value in [0, 1]
        ) + b'\n'
        with patch('host_resources.time.monotonic', side_effect=[0, 0, 1]):
            sample = monitor.read()
        self.assertEqual(sample['request_id'], 1)
        self.assertEqual(sample['observation_age_seconds'], 1)

    def test_host_old_or_late_response_does_not_extend_deadline(self):
        for request_id in [0, 1]:
            monitor = HostMonitor()
            monitor.process = Mock()
            monitor.selector = Mock()
            monitor.selector.select.return_value = [True]
            monitor.process.stdout.read.return_value = (
                json.dumps(dict(SAFE, request_id=request_id)).encode() + b'\n'
            )
            with patch('host_resources.time.monotonic', side_effect=[0, 0, 6]):
                with self.assertRaisesRegex(OSError, 'within 5 seconds'):
                    monitor.read()

    def test_host_stdout_eof_does_not_wait_for_live_stderr(self):
        monitor = HostMonitor()
        monitor.process = Mock()
        monitor.selector = Mock()
        monitor.selector.select.return_value = [True]
        monitor.process.stdout.read.return_value = b''
        monitor.process.stderr.read.side_effect = AssertionError('stderr may remain open')
        with self.assertRaisesRegex(OSError, 'observer stopped'):
            monitor.read()

    def test_heartbeat_tolerates_observation_gap_but_stops_when_parent_is_lost(self):
        heartbeat = Mock()
        heartbeat.stat.return_value.st_mtime_ns = 1
        with patch('verification_watchdog.time.monotonic', side_effect=[0, 0, 6, 21]), \
             patch('verification_watchdog.time.sleep'), \
             patch('verification_watchdog.subprocess.run') as notify:
            keep_watchdog_alive(heartbeat)
        self.assertEqual(notify.call_count, 2)
        self.assertTrue(all('--no-block' in call.args[0] for call in notify.call_args_list))

    def test_heartbeat_progress_resets_deadline(self):
        heartbeat = Mock()
        heartbeat.stat.side_effect = [Mock(st_mtime_ns=value) for value in [1, 1, 2, 2, 2]]
        with patch('verification_watchdog.time.monotonic', side_effect=[0, 0, 18, 18, 35, 39]), \
             patch('verification_watchdog.time.sleep'), \
             patch('verification_watchdog.subprocess.run') as notify:
            keep_watchdog_alive(heartbeat)
        self.assertEqual(notify.call_count, 3)

    def test_notification_failure_stops_heartbeats(self):
        heartbeat = Mock()
        with patch('verification_watchdog.subprocess.run', side_effect=OSError('unavailable')) as notify:
            keep_watchdog_alive(heartbeat)
        self.assertEqual(notify.call_count, 1)

    def test_host_initialization_grace_preserves_runtime_deadline(self):
        monitor = HostMonitor()
        process = Mock()
        process.stdout.read.side_effect = [
            json.dumps(dict(SAFE, request_id=value)).encode() + b'\n' for value in [1, 2]
        ]
        monitor.selector = Mock()
        monitor.selector.select.side_effect = [[True], [True], []]
        with patch.object(monitor, '_start', side_effect=lambda: setattr(monitor, 'process', process)), \
             patch('host_resources.time.monotonic', side_effect=[10, 10, 10, 11, 11, 11, 12, 12]):
            monitor.read()
            monitor.read()
            with self.assertRaisesRegex(OSError, 'within 5 seconds'):
                monitor.read()
        waits = [call.args[0] for call in monitor.selector.select.call_args_list]
        self.assertEqual(waits, [30, 5, 5])

    def run_case(self, monitor, code, runtime='10', policy=None, descendant=False, watchdog=False):
        with tempfile.TemporaryDirectory(prefix='codex-watchdog-') as directory:
            base = Path(directory)
            unit = f'codex-issue597-test-{os.getpid()}-{time.time_ns()}'
            launch = ['systemd-run', '--user', '--wait', '--pipe', '--unit=' + unit,
                      '--property=MemoryMax=4G', '--property=MemorySwapMax=0',
                      '--property=CPUQuota=100%', '--property=TasksMax=128',
                      '--property=RuntimeMaxSec=' + runtime,
                      '--property=KillMode=control-group', '--property=TimeoutStopSec=2',
                      sys.executable, '-B', '-c', code, str(base / 'child.pid')]
            if watchdog:
                launch[6:6] = ['--property=Type=notify', '--property=NotifyAccess=all',
                              '--property=WatchdogSec=1', '--property=WatchdogSignal=SIGTERM']
            result = run_guarded(launch, unit, base / 'run', monitor,
                                 policy or ResourcePolicy())
            self.assertTrue(monitor.closed)
            saved = json.loads((base / 'run-watch.json').read_text())
            self.assertEqual(saved['status'], result['status'])
            self.assertTrue(result['cleanup_complete'])
            events = [json.loads(line) for line in (base / 'run-events.jsonl').read_text().splitlines()]
            self.assertEqual(events[0]['event'], 'start')
            self.assertEqual(events[-1]['event'], 'end')
            if descendant:
                self.assertTrue((base / 'child.pid').is_file(), 'descendant did not start')
            if (base / 'child.pid').exists():
                self.assertFalse(Path('/proc', (base / 'child.pid').read_text()).exists())
            return result, events

    def test_success_and_failure(self):
        for code, status in [('pass', 'success'), ('raise SystemExit(7)', 'failed')]:
            result, events = self.run_case(Observations(), code)
            self.assertEqual(result['status'], status)
            self.assertTrue(any(event['event'] == 'sample' for event in events))

    def test_rejects_before_launch(self):
        for unsafe in ({'host_available_bytes': 0}, {'host_commit_available_bytes': 0},
                       {'wsl_available_bytes': 0}, {'host_cpu_busy': 0.99}):
            monitor = Observations()
            monitor.calls = 1
            monitor.second = unsafe
            result, _ = self.run_case(monitor, 'raise RuntimeError("must not run")')
            self.assertEqual(result['status'], 'rejected')
            self.assertFalse(result['launched'])

    def test_pressure_stops_descendants(self):
        code = ('import subprocess,sys,time; from pathlib import Path; '
                'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                'Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(60)')
        for unsafe in ({'host_available_bytes': 0}, {'host_commit_available_bytes': 0},
                       {'wsl_available_bytes': 0}):
            result, _ = self.run_case(Observations(unsafe), code, descendant=True)
            self.assertEqual(result['status'], 'pressure_stopped')

    def test_cpu_pressure_stops(self):
        result, _ = self.run_case(Observations({'host_cpu_busy': 0.99}),
                                  'import time; time.sleep(60)',
                                  policy=ResourcePolicy(cpu_grace_seconds=0))
        self.assertEqual(result['status'], 'pressure_stopped')

    def test_transient_cpu_load_resets_grace_period(self):
        policy = ResourcePolicy()
        high = dict(SAFE, host_cpu_busy=0.99)
        with patch('verification_watchdog.time.monotonic', side_effect=[100, 105, 112, 123]):
            self.assertIsNone(policy.reason(high))
            self.assertIsNone(policy.reason(high))
            self.assertIsNone(policy.reason(SAFE))
            self.assertIsNone(policy.reason(high))
            self.assertEqual(policy.reason(high), 'host CPU pressure')

    def test_monitor_loss_and_interruption_stop_descendants(self):
        code = ('import subprocess,sys,time; from pathlib import Path; '
                'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                'Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(60)')
        for error, expected in [(OSError('observation unavailable'), 'monitor_stopped'),
                                (KeyboardInterrupt(), 'interrupted')]:
            result, _ = self.run_case(Observations(error), code, descendant=True)
            self.assertEqual(result['status'], expected)

    def test_timeout_and_launch_failure(self):
        result, _ = self.run_case(Observations(), 'import time; time.sleep(60)', runtime='1')
        self.assertEqual(result['status'], 'timeout')
        result, _ = self.run_case(Observations(), 'pass', runtime='invalid-runtime')
        self.assertEqual(result['status'], 'launch_failed')

    def test_os_watchdog_stops_when_heartbeat_ceases(self):
        code = ('import subprocess,sys,time; from pathlib import Path; '
                'subprocess.run(["systemd-notify","--ready"],check=True); '
                'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                'Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(60)')
        result, _ = self.run_case(Observations(), code, descendant=True, watchdog=True)
        self.assertEqual(result['status'], 'monitor_stopped')


if __name__ == '__main__':
    unittest.main()
