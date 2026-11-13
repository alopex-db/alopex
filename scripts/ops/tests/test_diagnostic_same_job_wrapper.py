"""Focused guards and child ownership for the #682 same-job wrapper."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest import mock

from scripts.ops.wal_cost_next import diagnostic_same_job_wrapper as wrapper
from scripts.ops.wal_cost_next import diagnostic_runner as runner


class FakeBudget:
    values = (100, 100, 100)

    def __init__(self, storage):
        self.samples = iter(self.values)

    def used(self):
        return next(self.samples)


class FakeChild:
    def __init__(self, polls=(None, 0), code=0):
        self.polls = iter(polls)
        self.returncode = None
        self.code = code
        self.pid = 123

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        try:
            result = next(self.polls)
        except StopIteration:
            return self.returncode
        if result is not None:
            self.returncode = self.code
        return result

    def wait(self, timeout):
        self.returncode = self.code
        return self.code


class DiagnosticSameJobWrapperTests(unittest.TestCase):
    def test_checked_in_diagnostic_inputs_match_wrapper_pins(self):
        root = Path(__file__).resolve().parents[1] / 'wal_cost_next'
        self.assertEqual(wrapper.shared.sha(root / 'exact-a415-wal-cost-input.private.json'),
                         wrapper.EXACT_CONFIG_SHA256)
        self.assertEqual(wrapper.shared.sha(root / 'diagnostic_sources.lock.json'),
                         wrapper.DIAGNOSTIC_LOCK_SHA256)

    def test_wrapper_command_parses_at_diagnostic_runner_boundary(self):
        root = Path('/example')
        args = types.SimpleNamespace(qualification_checkout=root / 'current',
                                     frozen_checkout=root / 'frozen', config=root / 'config.json',
                                     diagnostic_sources=root / 'sources.json',
                                     surface='kv', value_bytes=16)
        command = wrapper.diagnostic_command(args, root / 'private', root / 'public.json',
                                             root / 'diagnostic-private', root / 'export')
        with mock.patch.object(runner.sys, 'argv', command[2:]), \
             mock.patch.object(runner, 'run', return_value=0) as execute:
            self.assertEqual(runner.main(), 0)
        self.assertEqual(execute.call_args.args[0].qualification_checkout,
                         args.qualification_checkout)

    def context(self, root, child, budget=FakeBudget):
        private = root / 'private'
        private.mkdir(mode=0o700)
        diagnostic_private = root / 'diagnostic-private'
        diagnostic_private.mkdir(mode=0o700)
        work = diagnostic_private / 'diagnostic'
        work.mkdir()
        fixture = work / 'fixtures'
        fixture.mkdir()
        (fixture / 'owned').write_text('temporary')
        module = types.SimpleNamespace(Budget=budget,
                                       admission_facts=lambda: {'same': True})
        prior = {'storage': {'roots': [str(root)]}, 'same': True}
        return private, module, prior, diagnostic_private

    def test_supervisor_reaps_success_and_records_private_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = FakeChild()
            private, module, prior, diagnostic_private = self.context(root, child)
            with mock.patch.object(wrapper.subprocess, 'Popen', return_value=child), \
                 mock.patch.object(wrapper.shared, 'same_host'), \
                 mock.patch.dict(os.environ, {'GITHUB_RUN_ID': '123'}):
                wrapper.supervise_diagnostic(['diagnostic'], cwd=root, env={}, module=module,
                                             prior=prior, private=private,
                                             diagnostic_private=diagnostic_private)
            receipt = json.loads((private / 'diagnostic-wrapper-result.private.json').read_text())
            self.assertEqual(receipt['diagnostic_exit'], 0)
            self.assertTrue(receipt['all_owned_children_waited'])
            self.assertIsNone(receipt['original_error'])
            self.assertFalse((diagnostic_private / 'diagnostic/fixtures').exists())

    def test_storage_growth_stops_and_reaps_child(self):
        class OverBudget(FakeBudget):
            values = (100, wrapper.DIAGNOSTIC_PEAK + 101, 100)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = FakeChild(polls=(None, None, None), code=-15)
            private, module, prior, diagnostic_private = self.context(root, child, OverBudget)
            killed = []
            def killpg(pid, sig):
                killed.append((pid, sig))
                child.returncode = -15
            with mock.patch.object(wrapper.subprocess, 'Popen', return_value=child), \
                 mock.patch.object(wrapper.os, 'killpg', side_effect=killpg), \
                 mock.patch.object(wrapper.shared, 'same_host'), \
                 mock.patch.dict(os.environ, {'GITHUB_RUN_ID': '123'}):
                with self.assertRaisesRegex(RuntimeError, 'execution or cleanup failed'):
                    wrapper.supervise_diagnostic(['diagnostic'], cwd=root, env={},
                                                 module=module, prior=prior, private=private,
                                                 diagnostic_private=diagnostic_private)
            self.assertTrue(killed)
            receipt = json.loads((private / 'diagnostic-wrapper-result.private.json').read_text())
            self.assertIn('growth exceeded', receipt['original_error'])
            self.assertTrue(receipt['all_owned_children_waited'])
            self.assertFalse((diagnostic_private / 'diagnostic/fixtures').exists())

    def test_postcheck_host_change_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = FakeChild()
            private, module, prior, diagnostic_private = self.context(root, child)
            with mock.patch.object(wrapper.subprocess, 'Popen', return_value=child), \
                 mock.patch.object(wrapper.shared, 'same_host', side_effect=RuntimeError('host changed')), \
                 mock.patch.dict(os.environ, {'GITHUB_RUN_ID': '123'}):
                with self.assertRaisesRegex(RuntimeError, 'execution or cleanup failed'):
                    wrapper.supervise_diagnostic(['diagnostic'], cwd=root, env={},
                                                 module=module, prior=prior, private=private,
                                                 diagnostic_private=diagnostic_private)
            receipt = json.loads((private / 'diagnostic-wrapper-result.private.json').read_text())
            self.assertIn('host changed', receipt['cleanup_error'])

    def test_wrong_lock_stops_before_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            qualification = root / 'qualification'; qualification.mkdir()
            frozen = root / 'frozen'; frozen.mkdir()
            config = root / 'exact.json'; config.write_text('{}')
            lock = root / 'lock.json'; lock.write_text('{}')
            args = types.SimpleNamespace(qualification_checkout=qualification,
                                         frozen_checkout=frozen, config=config,
                                         diagnostic_sources=lock, surface='sql', value_bytes=2048)
            with mock.patch.dict(os.environ, {'GITHUB_EVENT_NAME': 'workflow_dispatch',
                                              'RUNNER_ENVIRONMENT': 'github-hosted',
                                              'RUNNER_KIND': 'github-hosted'}), \
                 mock.patch.object(wrapper.shared, 'sha', side_effect=[wrapper.EXACT_CONFIG_SHA256,
                                                                       'wrong-lock']), \
                 mock.patch.object(wrapper.shared, 'load_qualification_module') as load:
                with self.assertRaisesRegex(RuntimeError, 'pinned diagnostic input differs'):
                    wrapper.run(args)
                load.assert_not_called()

    def test_stop_group_escalates_and_waits(self):
        child = FakeChild(polls=(None, None, None), code=-9)
        calls = []
        def timeout_then_exit(timeout):
            if timeout in (45, 5) and len(calls) < 3:
                raise subprocess.TimeoutExpired('diagnostic', timeout)
            child.returncode = -9
        child.wait = timeout_then_exit
        with mock.patch.object(wrapper.os, 'killpg', side_effect=lambda pid, sig: calls.append(sig)):
            wrapper.stop_group(child)
        self.assertEqual(calls, [wrapper.signal.SIGINT, wrapper.signal.SIGTERM,
                                 wrapper.signal.SIGKILL])


if __name__ == '__main__':
    unittest.main()
