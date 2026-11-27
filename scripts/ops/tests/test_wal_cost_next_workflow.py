import importlib.util
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1] / 'wal_cost_next'
WORKFLOW = HERE.parents[2] / '.github/workflows/alopex-performance.yml'


def module(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


sealer = module('seal_wrapper')


class WorkflowContract(unittest.TestCase):
    def test_exact_precheckout_and_serial_gate(self):
        workflow = WORKFLOW.read_text()
        start = '      - name: Bound checkout before source acquisition\n'
        end = '          PY\n'
        blocks = [start + section.split(end, 1)[0] + end
                  for section in workflow.split(start)[1:]]
        self.assertEqual(len(blocks), 1)
        self.assertEqual(hashlib.sha256(blocks[0].encode()).hexdigest(),
                         '0e63e4211e117c2c87abfcbc6ad9b6597796b527adff7e7a4a84610515bfdc03')
        qualification = workflow.split('  qualification:\n', 1)[1].split('  wal_cost_682:\n', 1)[0]
        self.assertIn("inputs.issue_number == '804'", qualification)
        self.assertIn("inputs.issue_number == '712'", qualification)
        self.assertIn('scripts/ops/four_case_job.py run', qualification)
        self.assertIn('wal_cost_mode:', workflow)
        self.assertIn('expected_wal_cost_source_tree:', workflow)
        self.assertIn("inputs.issue_number == '682'", workflow)
        self.assertIn('!inputs.sql_four_case_mode && !inputs.sql_final_mode && !inputs.wal_cost_mode', workflow)
        cost_job = workflow.split('  wal_cost_682:\n', 1)[1].split('  s04_inspect:\n', 1)[0]
        order = ['Gate selected source and storage before checkout',
                 'Fetch exact selected source without path output',
                 'Acquire, build, measure, and audit public WAL cost (stage-budget and early-stop)',
                 'Upload audited public WAL cost JSON artifact']
        self.assertEqual([cost_job.index(value) for value in order],
                         sorted(cost_job.index(value) for value in order))
        self.assertIn('timeout-minutes: 90', cost_job)
        self.assertIn('timeout-minutes: 75', cost_job)
        self.assertIn('python3 -B scripts/performance/issue682_hosted_wal_cost.py', cost_job)
        self.assertIn('path: ${{ runner.temp }}/issue682-public-export/', cost_job)
        self.assertIn('if-no-files-found: error', cost_job)
        for forbidden in ('private-evidence.cms', 'native-evidence-cert.pem',
                          'docker', 'fd34ca342617c247a9c711bcf597da8bbef3118f',
                          'exact-a415-wal-cost-input.private.json'):
            self.assertNotIn(forbidden, cost_job)

        # Also exercise the #682 hosted WAL cost unit/contract test suite in CI.
        perf_tests = WORKFLOW.parents[2] / 'scripts/performance/tests/test_issue682_hosted_wal_cost.py'
        res = subprocess.run(
            [sys.executable, '-B', '-m', 'unittest', str(perf_tests)],
            capture_output=True, text=True, timeout=60, check=False,
        )
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)

    def test_sealer_exports_only_allowlisted_fields_and_encrypts_private(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            qualification, frozen, private, output = [root / name for name in
                                                      ('qualification', 'frozen', 'private', 'output')]
            for path in (qualification, frozen / 'wal-cost-export', private):
                path.mkdir(parents=True)
            (private / 'wrapper-result.private.json').write_text(json.dumps({
                'original_error': None, 'cleanup_error': None, 'comparator_exit': 0,
                'all_owned_children_waited': True, 'secret_machine_path': '/hidden'}))
            (private / 'comparator.stdout.private.log').write_text('private log')
            closure_run = private / 'runs/hosted-804-42-trace-lock'
            (closure_run / 'collected').mkdir(parents=True)
            (closure_run / 'run.json').write_text(json.dumps({
                'run_id': 'hosted-804-42-trace-lock', 'phase': 'deleted'}))
            (closure_run / 'collection.json').write_text('{}')
            (closure_run / 'collected/result.json').write_text('{}')
            (closure_run / 'collected/candidate.private.json').write_text(json.dumps({
                'approval': 'unapproved', 'formal_lock': False, 'secret': '/hidden/closure'}))
            (closure_run / 'final-result.private.json').write_text(json.dumps({
                'schema': 'alopex.trace-closure-final/v1', 'status': 'candidate',
                'proof_sha256': 'a' * 64, 'error_code': None}))
            (private / 'closure-phase.private.json').write_text(json.dumps({
                'schema': 'alopex.trace-closure-phase/v1', 'case': 'TRACE_CLOSURE_682',
                'status': 'candidate', 'approval': 'unapproved', 'formal_lock': False,
                'run_id': 'hosted-804-42-trace-lock', 'proof_sha256': 'a' * 64,
                'secret': '/hidden/closure'}))
            evidence = frozen / 'wal-cost-export/private-evidence.cms'
            evidence.write_bytes(b'encrypted cost')
            (frozen / 'wal-cost-export/result.json').write_text(json.dumps({
                'status': 'passed', 'complete_cells': 48, 'planned_cells': 48,
                'work_removed': True, 'performance_acceptance': False,
                'encrypted_sha256': sealer.digest(evidence)}))

            def fake_encrypt(command, **_):
                Path(command[command.index('-out') + 1]).write_bytes(b'encrypted wrapper')
                return types.SimpleNamespace(returncode=0)

            with patch.object(sealer.subprocess, 'run', side_effect=fake_encrypt):
                public = sealer.seal(qualification, frozen, private, output)
            self.assertEqual(public['outcome'], 'failed')
            self.assertEqual(public['error_code'], 'LOCK_UNAPPROVED')
            self.assertEqual(public['wal_outcome'], 'passed')
            self.assertEqual(public['closure_outcome'], 'candidate')
            self.assertEqual(public['closure_approval'], 'unapproved')
            self.assertIs(public['formal_lock'], False)
            self.assertNotIn('/hidden/closure', (output / 'public.json').read_text())
            self.assertNotIn('secret_machine_path', (output / 'public.json').read_text())
            self.assertTrue((output / 'private-evidence.cms').is_file())
            self.assertFalse((private / 'wrapper-result.private.json').exists())
            self.assertFalse((private / 'comparator.stdout.private.log').exists())
            with patch.object(sealer, 'seal', return_value=public), \
                 patch.object(sys, 'argv', ['seal_wrapper', '--qualification', str(qualification),
                                            '--frozen', str(frozen), '--private', str(private),
                                            '--output', str(output)]):
                self.assertEqual(sealer.main(), 1)

    def test_missing_comparator_export_still_encrypts_wrapper_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            qualification, frozen, private, output = [root / name for name in
                                                      ('qualification', 'frozen', 'private', 'output')]
            private.mkdir()
            wrapper = private / 'wrapper-result.private.json'
            wrapper.write_text(json.dumps({'original_error': 'cleanup failed',
                                           'cleanup_error': 'storage scan failed'}))
            log = private / 'comparator.stdout.private.log'
            log.write_text('private failure detail')

            def fake_encrypt(command, **_):
                with tarfile.open(command[command.index('-in') + 1], 'r:gz') as archive:
                    preserved = b' '.join(archive.extractfile(member).read()
                                          for member in archive.getmembers())
                self.assertIn(b'cleanup failed', preserved)
                self.assertIn(b'storage scan failed', preserved)
                self.assertIn(b'private failure detail', preserved)
                Path(command[command.index('-out') + 1]).write_bytes(b'encrypted wrapper')
                return types.SimpleNamespace(returncode=0)

            with patch.object(sealer.subprocess, 'run', side_effect=fake_encrypt):
                public = sealer.seal(qualification, frozen, private, output)
            self.assertEqual(public['error_code'], 'INCOMPLETE')
            self.assertEqual(public['outcome'], 'failed')
            self.assertTrue((output / 'private-evidence.cms').is_file())
            self.assertNotIn('cleanup failed', (output / 'public.json').read_text())
            self.assertFalse(wrapper.exists())
            self.assertFalse(log.exists())
            self.assertFalse((private / 'wrapper-evidence.tar.gz').exists())

    def test_pre_comparator_failure_uses_fixed_public_code_and_encrypted_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            qualification, frozen, private, output = [root / name for name in
                                                      ('qualification', 'frozen', 'private', 'output')]
            private.mkdir()
            detail = 'private identity path /hidden/host'
            (private / 'wrapper-result.private.json').write_text(json.dumps({
                'pre_comparator_error': True, 'original_error': detail,
                'cleanup_error': None, 'comparator_exit': None,
                'all_owned_children_waited': False}))

            def fake_encrypt(command, **_):
                with tarfile.open(command[command.index('-in') + 1], 'r:gz') as archive:
                    preserved = b' '.join(archive.extractfile(member).read()
                                          for member in archive.getmembers())
                self.assertIn(detail.encode(), preserved)
                Path(command[command.index('-out') + 1]).write_bytes(b'encrypted wrapper')
                return types.SimpleNamespace(returncode=0)

            with patch.object(sealer.subprocess, 'run', side_effect=fake_encrypt):
                public = sealer.seal(qualification, frozen, private, output)
            self.assertEqual(public['outcome'], 'failed')
            self.assertEqual(public['error_code'], 'WRAPPER_FAILURE')
            self.assertNotIn(detail, (output / 'public.json').read_text())
            self.assertTrue((output / 'private-evidence.cms').is_file())
            self.assertFalse((private / 'wrapper-result.private.json').exists())

    def test_encryption_failure_cleans_plaintext_and_reports_fixed_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            qualification, frozen, private, output = [root / name for name in
                                                      ('qualification', 'frozen', 'private', 'output')]
            private.mkdir()
            wrapper = private / 'wrapper-result.private.json'
            wrapper.write_text('{"original_error":"private detail"}')
            log = private / 'comparator.stdout.private.log'
            log.write_text('private log')

            def fail_encrypt(command, **_):
                Path(command[command.index('-out') + 1]).write_bytes(b'partial ciphertext')
                raise subprocess.CalledProcessError(1, command)

            with patch.object(sealer.subprocess, 'run', side_effect=fail_encrypt):
                public = sealer.seal(qualification, frozen, private, output)
            self.assertEqual(public['error_code'], 'EVIDENCE_FAILURE')
            self.assertEqual(public['outcome'], 'failed')
            self.assertNotIn('private detail', (output / 'public.json').read_text())
            self.assertFalse((output / 'private-evidence.cms').exists())
            self.assertFalse((private / 'wrapper-evidence.tar.gz').exists())
            self.assertFalse(wrapper.exists())
            self.assertFalse(log.exists())

    def test_oversized_private_closure_input_fails_before_encryption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            qualification, frozen, private, output = [root / name for name in
                                                      ('qualification', 'frozen', 'private', 'output')]
            private.mkdir()
            (private / 'closure-phase.private.json').write_text(json.dumps({
                'schema': 'alopex.trace-closure-phase/v1', 'case': 'TRACE_CLOSURE_682',
                'status': 'guard_failed', 'approval': 'unapproved', 'formal_lock': False,
                'run_id': 'hosted-804-42-trace-lock', 'proof_sha256': 'a' * 64}))
            collected = private / 'runs/hosted-804-42-trace-lock/collected'
            collected.mkdir(parents=True)
            (collected / 'candidate.private.json').write_bytes(b'x' * (sealer.CLOSURE_INPUT_LIMIT + 1))
            with patch.object(sealer.subprocess, 'run') as encrypt:
                public = sealer.seal(qualification, frozen, private, output)
            encrypt.assert_not_called()
            self.assertEqual(public['error_code'], 'EVIDENCE_FAILURE')
            self.assertFalse((output / 'private-evidence.cms').exists())
            self.assertFalse((private / 'wrapper-evidence.tar.gz').exists())

    def test_candidate_phase_without_deleted_final_is_guard_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            qualification, frozen, private, output = [root / name for name in
                                                      ('qualification', 'frozen', 'private', 'output')]
            private.mkdir()
            (private / 'closure-phase.private.json').write_text(json.dumps({
                'schema': 'alopex.trace-closure-phase/v1', 'case': 'TRACE_CLOSURE_682',
                'status': 'candidate', 'approval': 'unapproved', 'formal_lock': False,
                'run_id': 'hosted-804-42-trace-lock', 'proof_sha256': 'a' * 64}))
            def fake_encrypt(command, **_):
                Path(command[command.index('-out') + 1]).write_bytes(b'encrypted')
                return types.SimpleNamespace(returncode=0)
            with patch.object(sealer.subprocess, 'run', side_effect=fake_encrypt):
                public = sealer.seal(qualification, frozen, private, output)
            self.assertEqual(public['closure_error_code'], 'CLOSURE_GUARD')
            self.assertEqual(public['outcome'], 'failed')


if __name__ == '__main__':
    unittest.main()
