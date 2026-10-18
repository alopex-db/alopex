import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import issue577_matrix
from issue577_matrix import ComparisonPolicy, ComparisonRejected, GeneratedOutputBudget
from issue577_cases import digest


class ComparisonAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.paths = patch("output_budget.required_filesystem_paths", return_value=[Path.cwd()])
        self.paths.start()
        self.addCleanup(self.paths.stop)

    def policy(self):
        return ComparisonPolicy(0.30, 0.50, Mock(reason=Mock(return_value=None)))

    def sample(self, cpu):
        return {"host_total_bytes": 64 << 30, "host_available_bytes": 32 << 30,
                "host_commit_available_bytes": 32 << 30, "wsl_available_bytes": 32 << 30,
                "host_cpu_busy": cpu}

    def test_background_load_is_rejected_before_a_case(self):
        policy = self.policy()
        self.assertIsNotNone(policy.reason(self.sample(0.31), starting=True))
        self.assertIsNone(policy.reason(self.sample(0.30), starting=True))

    def test_load_change_invalidates_a_running_case(self):
        policy = self.policy()
        self.assertIsNotNone(policy.reason(self.sample(0.51)))
        self.assertIsNone(policy.reason(self.sample(0.50)))

    def test_resource_reserves_remain_required(self):
        policy = self.policy()
        sample = self.sample(0.01)
        sample["host_available_bytes"] = 1 << 30
        self.assertEqual(policy.reason(sample, starting=True), "host memory reserve")

    def test_generated_output_growth_has_a_budget(self):
        config = {"project_baseline_bytes": 1 << 30,
                  "filesystems": [{"path": str(Path.cwd()), "used_at_baseline": 10 << 30}],
                  "filesystem_used_at_baseline": 10 << 30,
                  "operating_limit_bytes": 40_000_000_000,
                  "planned_growth_bytes": 1 << 30}
        usage = type(shutil.disk_usage(Path.cwd()))
        with patch('issue577_matrix.shutil.disk_usage', side_effect=[
                usage(100 << 30, 10 << 30, 90 << 30),
                usage(100 << 30, (11 << 30) + 1, (89 << 30) - 1)]):
            budget = GeneratedOutputBudget(Path.cwd(), config)
            self.assertIsNotNone(budget.reason())

    def test_oversized_host_operating_limit_is_rejected(self):
        config = {"project_baseline_bytes": 1 << 30,
                  "filesystems": [{"path": str(Path.cwd()), "used_at_baseline": 10 << 30}],
                  "filesystem_used_at_baseline": 10 << 30,
                  "operating_limit_bytes": 42_000_000_000,
                  "planned_growth_bytes": 1 << 30}
        with self.assertRaises(ValueError):
            GeneratedOutputBudget(Path.cwd(), config)

    def test_host_operating_limit_stops_at_40gb(self):
        config = {"project_baseline_bytes": 39_000_000_000,
                  "filesystems": [{"path": str(Path.cwd()), "used_at_baseline": 10_000_000_000}],
                  "filesystem_used_at_baseline": 10_000_000_000,
                  "operating_limit_bytes": 40_000_000_000,
                  "planned_growth_bytes": 500_000_000}
        usage = type(shutil.disk_usage(Path.cwd()))
        with patch('issue577_matrix.shutil.disk_usage', side_effect=[
                usage(100_000_000_000, 10_000_000_000, 90_000_000_000),
                usage(100_000_000_000, 11_000_000_000, 89_000_000_000)]):
            budget = GeneratedOutputBudget(Path.cwd(), config)
            self.assertEqual(budget.reason(), 'project output operating reserve')
            self.assertEqual(budget.latest['project_estimate_bytes'], 40_000_000_000)

    def test_legacy_single_filesystem_budget_is_rejected(self):
        config = {"project_baseline_bytes": 1 << 30,
                  "filesystem_used_at_baseline": 10 << 30,
                  "operating_limit_bytes": 40_000_000_000,
                  "planned_growth_bytes": 1 << 30}
        with self.assertRaises(ValueError):
            GeneratedOutputBudget(Path.cwd(), config)

    def test_growth_on_another_host_filesystem_stops_the_run(self):
        root = Path.cwd()
        config = {"project_baseline_bytes": 39_000_000_000,
                  "filesystems": [{"path": str(Path.cwd()), "used_at_baseline": 10_000_000_000}],
                  "filesystem_used_at_baseline": 10_000_000_000,
                  "operating_limit_bytes": 40_000_000_000,
                  "planned_growth_bytes": 500_000_000,
                  "filesystems": [
                      {"path": str(root), "used_at_baseline": 10_000_000_000},
                      {"path": str(root / "host-native-drive"),
                       "used_at_baseline": 10_000_000_000}]}
        usage = type(shutil.disk_usage(root))
        with patch('issue577_matrix.shutil.disk_usage', side_effect=[
                usage(100_000_000_000, 10_000_000_000, 90_000_000_000),
                usage(100_000_000_000, 10_000_000_000, 90_000_000_000),
                usage(100_000_000_000, 10_000_000_000, 90_000_000_000),
                usage(100_000_000_000, 11_000_000_000, 89_000_000_000)]):
            budget = GeneratedOutputBudget(root, config)
            self.assertEqual(budget.reason(), 'project output operating reserve')
            self.assertEqual(budget.latest['project_estimate_bytes'], 40_000_000_000)

    def test_missing_native_host_volume_is_rejected(self):
        root = Path.cwd()
        config = {"project_baseline_bytes": 1 << 30,
                  "operating_limit_bytes": 40_000_000_000,
                  "planned_growth_bytes": 1 << 30,
                  "filesystems": [{"path": str(root), "used_at_baseline": 10 << 30}]}
        with patch('output_budget.required_filesystem_paths',
                   return_value=[root, root / 'host-native-drive']):
            with self.assertRaisesRegex(ValueError, 'coverage is incomplete'):
                GeneratedOutputBudget(root, config)

    def test_one_volume_shrinking_cannot_hide_another_growing(self):
        root = Path.cwd()
        native = root / 'host-native-drive'
        config = {"project_baseline_bytes": 39_000_000_000,
                  "operating_limit_bytes": 40_000_000_000,
                  "planned_growth_bytes": 500_000_000,
                  "filesystems": [
                      {"path": str(root), "used_at_baseline": 10_000_000_000},
                      {"path": str(native), "used_at_baseline": 10_000_000_000}]}
        usage = type(shutil.disk_usage(root))
        with patch('issue577_matrix.shutil.disk_usage', side_effect=[
                usage(100_000_000_000, 10_000_000_000, 90_000_000_000),
                usage(100_000_000_000, 10_000_000_000, 90_000_000_000),
                usage(100_000_000_000, 9_000_000_000, 91_000_000_000),
                usage(100_000_000_000, 11_000_000_000, 89_000_000_000)]):
            budget = GeneratedOutputBudget(root, config)
            self.assertEqual(budget.reason(), 'project output operating reserve')
            self.assertEqual(budget.latest['project_estimate_bytes'], 40_000_000_000)

    def test_admission_capacity_failure_preserves_the_reason(self):
        sample = self.sample(0.1)
        monitor = Mock(read=Mock(return_value=sample))
        policy = Mock(reason=Mock(return_value='planned generated-output reserve'))
        policy.output_budget.latest = {'filesystem_growth_bytes': 2 << 30}
        with tempfile.TemporaryDirectory() as directory:
            output, state = Path(directory), {}
            with patch('issue577_matrix.HostMonitor', return_value=monitor):
                with self.assertRaisesRegex(RuntimeError, 'planned generated-output reserve'):
                    issue577_matrix.await_admission(policy, 3, 2, output, state)
            self.assertEqual(json.loads((output / 'admission.jsonl').read_text())['reason'],
                             'planned generated-output reserve')
            self.assertEqual(json.loads((output / 'state.json').read_text())['admission_reason'],
                             'planned generated-output reserve')
            monitor.close.assert_called_once()


class MatrixLifecycleTests(unittest.TestCase):
    """Command-lifecycle stubs contain no latency samples or performance evidence."""

    def setUp(self):
        paths = patch("output_budget.required_filesystem_paths", return_value=[Path.cwd()])
        paths.start()
        self.addCleanup(paths.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.profile = self.root / 'profile.json'
        self.profile.write_text(json.dumps({
            'before': 'old', 'after': 'new', 'reference': {'revision': 'reference'},
            'cpu_affinity': [0], 'cases': [{'id': 'lifecycle'}],
            'memory_limits_bytes': [None], 'measurement_admission': {
                'maximum_cpu_before_case': 0.3, 'maximum_cpu_during_case': 0.5,
                'consecutive_samples': 3, 'sample_interval_seconds': 2}}))
        binaries = {}
        for engine, revision in [('before', 'old'), ('after', 'new'), ('duckdb', 'reference')]:
            binary = self.root / engine
            binary.write_bytes(b'non-executable lifecycle sentinel')
            binaries[engine] = {'binary': str(binary), 'revision': revision, 'sha256': digest(binary)}
        self.binaries = self.root / 'binaries.json'
        self.binaries.write_text(json.dumps({'engines': binaries, 'storage': {
            'project_baseline_bytes': 1 << 30,
            'filesystems': [{'path': str(Path.cwd()), 'used_at_baseline': shutil.disk_usage(Path.cwd()).used}],
            'operating_limit_bytes': 40_000_000_000,
            'planned_growth_bytes': 1 << 30}}))
        self.output = self.root / 'output'
        self.arguments = ['issue577_matrix.py', str(self.profile), str(self.root),
                          str(self.binaries), str(self.output)]

    def assert_no_accepted_cells(self):
        self.assertEqual(json.loads((self.output / 'state.json').read_text())['accepted_cells'], 0)
        for engine in ('before', 'after', 'duckdb'):
            manifest = json.loads((self.output / engine / 'manifest.json').read_text())
            self.assertEqual(manifest['status'], 'incomplete')
            self.assertEqual(manifest['results'], [])

    def test_profile_change_during_command_is_not_accepted(self):
        def change_profile(case, limit, target, execute):
            self.profile.write_text(self.profile.read_text() + '\n')
            target.write_bytes(b'command lifecycle output')
            return {'file': target.name, 'sha256': digest(target)}

        with patch('sys.argv', self.arguments), patch('issue577_matrix.await_admission'), \
                patch('issue577_matrix.CaseRunner.run', side_effect=change_profile):
            with self.assertRaises(AssertionError):
                issue577_matrix.main()
        self.assert_no_accepted_cells()

    def test_rejected_attempt_then_interruption_stays_incomplete(self):
        attempts = 0

        def rejected_command(case, limit, target, execute):
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                raise KeyboardInterrupt()
            target.write_bytes(b'rejected command lifecycle output')
            raise ComparisonRejected('comparison CPU load')

        with patch('sys.argv', self.arguments), patch('issue577_matrix.await_admission'), \
                patch('issue577_matrix.CaseRunner.run', side_effect=rejected_command) as run:
            with self.assertRaises(KeyboardInterrupt):
                issue577_matrix.main()
            self.assertEqual(run.call_count, 2)
        self.assert_no_accepted_cells()
        self.assertEqual((self.output / 'attempts/1/lifecycle-none.json').read_bytes(),
                         b'rejected command lifecycle output')
        self.assertEqual(json.loads((self.output / 'attempts/1/quality.json').read_text())['status'],
                         'rejected')


if __name__ == "__main__":
    unittest.main()
