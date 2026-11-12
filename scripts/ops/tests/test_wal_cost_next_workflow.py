import importlib.util
import hashlib
import json
from pathlib import Path
import subprocess
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
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0], blocks[1])
        self.assertEqual(hashlib.sha256(blocks[0].encode()).hexdigest(),
                         '0e63e4211e117c2c87abfcbc6ad9b6597796b527adff7e7a4a84610515bfdc03')
        qualification = workflow.split('  qualification:\n', 1)[1].split('  wal_cost_682:\n', 1)[0]
        self.assertIn("inputs.issue_number == '804'", qualification)
        self.assertIn("inputs.issue_number == '712'", qualification)
        self.assertIn('scripts/ops/same_job_four_case.py', qualification)
        self.assertIn('wal_cost_mode:', workflow)
        self.assertIn("inputs.issue_number == '682'", workflow)
        self.assertIn('!inputs.sql_four_case_mode && !inputs.wal_cost_mode', workflow)
        order = ['Bound checkout before source acquisition',
                 'Stage current #804 controller', 'Stage frozen #682 comparator',
                 'Qualify and measure on the same runner', 'Seal wrapper evidence',
                 'Preserve encrypted and allowlisted evidence']
        cost_job = workflow.split('  wal_cost_682:\n', 1)[1]
        self.assertEqual([cost_job.index(value) for value in order],
                         sorted(cost_job.index(value) for value in order))
        self.assertIn('timeout-minutes: 300', workflow)
        self.assertIn('ref: 490f002dfb2c7f509a2444891808d9704fe6b001', workflow)
        self.assertIn('path: qualification-controller', workflow)
        self.assertIn('path: frozen-wal-controller', workflow)
        self.assertIn('if-no-files-found: error', workflow)
        self.assertNotIn('*.log\n', workflow.split('path: |')[-1])

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
            self.assertEqual(public['outcome'], 'passed')
            self.assertNotIn('secret_machine_path', (output / 'public.json').read_text())
            self.assertTrue((output / 'private-evidence.cms').is_file())
            self.assertFalse((private / 'wrapper-result.private.json').exists())
            self.assertFalse((private / 'comparator.stdout.private.log').exists())

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


if __name__ == '__main__':
    unittest.main()
