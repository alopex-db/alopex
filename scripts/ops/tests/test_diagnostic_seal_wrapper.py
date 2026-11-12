"""Focused result boundary for the one-pair diagnostic wrapper."""
import hashlib
import json
from pathlib import Path
import tempfile
import tarfile
import unittest
from unittest import mock

from scripts.ops.wal_cost_next import diagnostic_seal_wrapper as sealer


class DiagnosticSealTests(unittest.TestCase):
    def fixture(self, root):
        qualification = root / 'qualification'; qualification.mkdir()
        private = root / 'private'; private.mkdir()
        diagnostic_private = root / 'diagnostic-private'; diagnostic_private.mkdir()
        diagnostic_export = root / 'diagnostic-export'; diagnostic_export.mkdir()
        output = root / 'output'
        (private / 'diagnostic-wrapper-result.private.json').write_text(json.dumps({
            'original_error': None, 'cleanup_error': None, 'diagnostic_exit': 0,
            'all_owned_children_waited': True}))
        (private / 'diagnostic.stdout.private.log').write_text('private log')
        encrypted = diagnostic_export / 'private-evidence.cms'
        encrypted.write_bytes(b'encrypted diagnostic')
        (diagnostic_export / 'result.json').write_text(json.dumps({
            'schema': 'alopex.wal-diagnostic/v1', 'case': 'WAL_COST_682',
            'outcome': 'observed', 'error_code': None, 'complete_subjects': 2,
            'performance_acceptance': False,
            'encrypted_sha256': hashlib.sha256(encrypted.read_bytes()).hexdigest()}))
        return qualification, private, diagnostic_private, diagnostic_export, output

    def seal(self, args):
        def encrypt(command, **kwargs):
            Path(command[command.index('-out') + 1]).write_bytes(b'encrypted wrapper')
        with mock.patch.object(sealer.subprocess, 'run', side_effect=encrypt):
            return sealer.seal(*args)

    def test_complete_pair_emits_observed_nonacceptance_and_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            public = self.seal(args)
            self.assertEqual(public['outcome'], 'observed')
            self.assertIs(public['performance_acceptance'], False)
            self.assertEqual(public['diagnostic_evidence_sha256'],
                             sealer.digest(args[3] / 'private-evidence.cms'))
            self.assertEqual(public['wrapper_evidence_sha256'],
                             sealer.digest(args[4] / 'private-evidence.cms'))
            self.assertFalse((args[1] / 'diagnostic-wrapper-result.private.json').exists())

    def test_incomplete_pair_never_emits_observed(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            result = args[3] / 'result.json'
            payload = json.loads(result.read_text()); payload['complete_subjects'] = 1
            result.write_text(json.dumps(payload))
            self.assertEqual(self.seal(args)['error_code'], 'DIAGNOSTIC_FAILURE')

    def test_unreaped_child_never_emits_observed(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            receipt = args[1] / 'diagnostic-wrapper-result.private.json'
            payload = json.loads(receipt.read_text()); payload['all_owned_children_waited'] = False
            receipt.write_text(json.dumps(payload))
            self.assertEqual(self.seal(args)['outcome'], 'failed')

    def test_remaining_private_work_never_emits_observed(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            (args[2] / 'diagnostic').mkdir()
            self.assertEqual(self.seal(args)['outcome'], 'failed')

    def test_mismatched_encrypted_hash_never_emits_observed(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            (args[3] / 'private-evidence.cms').write_bytes(b'changed')
            self.assertEqual(self.seal(args)['outcome'], 'failed')

    def test_failed_wrapper_encryption_removes_partial_output(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            def fail(command, **kwargs):
                Path(command[command.index('-out') + 1]).write_bytes(b'partial')
                raise RuntimeError('failed encryption')
            with mock.patch.object(sealer.subprocess, 'run', side_effect=fail):
                public = sealer.seal(*args)
            self.assertEqual(public['error_code'], 'EVIDENCE_FAILURE')
            self.assertFalse((args[4] / 'private-evidence.cms').exists())

    def test_early_runner_failure_is_sealed_and_raw_receipt_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            (args[3] / 'result.json').unlink()
            (args[3] / 'private-evidence.cms').unlink()
            failure = args[2] / 'diagnostic-failure.private.json'
            failure.write_text(json.dumps({'phase': 'preflight', 'original_error':
                                           {'error': 'private preflight detail'}}))
            archived = []
            def encrypt(command, **kwargs):
                with tarfile.open(command[command.index('-in') + 1]) as bundle:
                    archived.extend((member.name, bundle.extractfile(member).read())
                                    for member in bundle.getmembers())
                Path(command[command.index('-out') + 1]).write_bytes(b'encrypted wrapper')
            with mock.patch.object(sealer.subprocess, 'run', side_effect=encrypt):
                public = sealer.seal(*args)
            self.assertEqual(public['error_code'], 'DIAGNOSTIC_FAILURE')
            self.assertTrue(any(name.endswith('diagnostic-failure.private.json') and
                                b'private preflight detail' in data for name, data in archived))
            self.assertFalse(failure.exists())
            self.assertNotIn('private preflight detail', (args[4] / 'public.json').read_text())

    def test_runner_evidence_failure_is_sealed_without_raw_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            (args[3] / 'result.json').unlink()
            (args[3] / 'private-evidence.cms').unlink()
            failure = args[2] / 'diagnostic-failure.private.json'
            failure.write_text(json.dumps({'phase': 'diagnostic_evidence_seal',
                                           'cleanup_error': {'error': 'private seal detail'}}))
            public = self.seal(args)
            self.assertEqual(public['error_code'], 'DIAGNOSTIC_FAILURE')
            self.assertTrue((args[4] / 'private-evidence.cms').is_file())
            self.assertFalse(failure.exists())
            self.assertFalse((args[4] / 'diagnostic-wrapper-evidence.tar.gz').exists())


if __name__ == '__main__':
    unittest.main()
