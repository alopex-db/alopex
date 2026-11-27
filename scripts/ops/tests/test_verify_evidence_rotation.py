"""Disposable recipients exercise the private-host acceptance tool end to end."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

OPS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPS))
import verify_evidence_rotation as subject


class RotationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name).resolve()
        for name in ('old', 'new'):
            result = subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                                     '-keyout', str(cls.root/(name+'.key')),
                                     '-out', str(cls.root/(name+'.pem')), '-days', '90',
                                     '-subj', '/CN=disposable-rotation-test'],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if result.returncode:
                raise RuntimeError('synthetic fixture generation failed')
            (cls.root/(name+'.key')).chmod(0o600)
        cls.receipt = b'synthetic archived receipt\n'
        (cls.root/'receipt').write_bytes(cls.receipt)
        subject.run(['cms', '-encrypt', '-binary', '-aes-256-cbc', '-outform', 'DER',
                     '-in', cls.root/'receipt', '-out', cls.root/'old.cms', cls.root/'old.pem'])

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def inputs(self):
        return dict(old_certificate=self.root/'old.pem', old_key=self.root/'old.key',
                    new_certificate=self.root/'new.pem', new_key=self.root/'new.key',
                    archived_cms=self.root/'old.cms', old_certificate_sha256=subject.digest(self.root/'old.pem'),
                    new_certificate_sha256=subject.digest(self.root/'new.pem'),
                    ciphertext_sha256=subject.digest(self.root/'old.cms'),
                    plaintext_sha256=hashlib.sha256(self.receipt).hexdigest(),
                    private_temp_root_path=self.root)

    def test_full_rotation_and_old_archive(self):
        result = subject.verify(**self.inputs())
        self.assertTrue(result['wrong_key_rejected'])
        self.assertEqual(result['historical_decryption'], 'passed')
        self.assertNotIn(str(self.root), json.dumps(result))
        self.assertNotIn('PRIVATE KEY', json.dumps(result))

    def test_wrong_private_key_rejected(self):
        values = self.inputs()
        values['new_key'] = values['old_key']
        with self.assertRaisesRegex(ValueError, 'KEY_PAIR_MISMATCH'):
            subject.verify(**values)

    def test_reused_key_rejected(self):
        values = self.inputs()
        for suffix in ('certificate', 'key', 'certificate_sha256'):
            values['new_'+suffix] = values['old_'+suffix]
        with self.assertRaisesRegex(ValueError, 'KEY_REUSE_REJECTED'):
            subject.verify(**values)

    def test_archive_hash_mismatch_rejected_before_decryption(self):
        values = self.inputs()
        values['ciphertext_sha256'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'ARCHIVE_HASH_MISMATCH'):
            subject.verify(**values)

    def test_wrong_historical_plaintext_digest_rejected(self):
        values = self.inputs()
        values['plaintext_sha256'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'ARCHIVE_PLAINTEXT_MISMATCH'):
            subject.verify(**values)

    def test_key_permissions_rejected(self):
        key = self.root/'new.key'
        key.chmod(0o644)
        try:
            with self.assertRaisesRegex(ValueError, 'KEY_MODE_INVALID'):
                subject.verify(**self.inputs())
        finally:
            key.chmod(0o600)

    def test_private_temporary_receipts_removed_on_success_and_failure(self):
        with tempfile.TemporaryDirectory(dir=self.root) as tmp:
            values = self.inputs()
            values['private_temp_root_path'] = Path(tmp)
            subject.verify(**values)
            self.assertEqual(list(Path(tmp).iterdir()), [])
            values['plaintext_sha256'] = '0'*64
            with self.assertRaises(ValueError):
                subject.verify(**values)
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_unsafe_temporary_root_rejected_before_decryption(self):
        with tempfile.TemporaryDirectory(dir=self.root) as tmp:
            root = Path(tmp)
            cases = [Path('/tmp'), Path('relative'), self.root/'missing']
            link = self.root/'temp-link'
            link.symlink_to(root, target_is_directory=True)
            cases.append(link)
            root.chmod(0o755)
            cases.append(root)
            try:
                with patch.object(subject.tempfile, 'TemporaryDirectory',
                                  side_effect=AssertionError('decryption temp created')):
                    for case in cases:
                        values = self.inputs()
                        values['private_temp_root_path'] = case
                        with self.subTest(case=case), self.assertRaisesRegex(ValueError, 'TEMP_ROOT_INVALID'):
                            subject.verify(**values)
            finally:
                root.chmod(0o700)
                link.unlink()

    def test_cli_failure_is_allowlisted_and_no_private_path(self):
        values = self.inputs()
        values['new_key'] = self.root/'not-available-secret'
        args = [sys.executable, str(OPS/'verify_evidence_rotation.py')]
        for key, value in values.items():
            args.extend(['--'+key.replace('_','-'), str(value)])
        result = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, '')
        self.assertEqual(json.loads(result.stdout), {'outcome':'failed','error_code':'ROTATION_CHECK_FAILED'})

    def test_monitor_extracts_actual_frozen_ref_and_pin(self):
        source = (OPS.parents[1]/'.github/workflows/native-evidence-expiry.yml').read_text()
        marker = "          python3 - <<'PY'\n"
        code = source.split(marker, 1)[1].split('          PY\n', 1)[0]
        import textwrap
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'candidate').symlink_to(OPS.parents[1], target_is_directory=True)
            output = root/'output'
            result = subprocess.run([sys.executable, '-c', textwrap.dedent(code)], cwd=root,
                                    env=dict(os.environ, GITHUB_OUTPUT=str(output)), capture_output=True)
            self.assertEqual(result.returncode, 0, 'monitor selector failed')
            self.assertEqual(output.read_text().splitlines(), ['has_frozen=false'])
            self.assertIn("if: always() && steps.frozen.outcome == 'success' && steps.frozen.outputs.has_frozen == 'true'", source)
            self.assertEqual(source.count('--minimum-seconds 1209600'), 2)

            # Synthetic fixture with frozen comparator still extracts ref & pin cleanly.
            synth = root / 'synth'
            wf = synth / 'candidate/.github/workflows/alopex-performance.yml'
            wf.parent.mkdir(parents=True)
            wf.write_text(
                "      - name: Stage frozen #682 comparator in measured workspace\n"
                "        with:\n"
                "          ref: " + "a" * 40 + "\n"
                "      - name: Check\n"
                "        run: --certificate frozen-wal-controller/.github/ops/native-evidence-cert.pem --expected-sha256 " + "b" * 64 + "\n",
                encoding='utf-8',
            )
            synth_out = synth / 'output'
            res2 = subprocess.run([sys.executable, '-c', textwrap.dedent(code)], cwd=synth,
                                  env=dict(os.environ, GITHUB_OUTPUT=str(synth_out)), capture_output=True)
            self.assertEqual(res2.returncode, 0, 'synthetic frozen selector failed')
            self.assertEqual(
                synth_out.read_text().splitlines(),
                ['has_frozen=true', 'ref=' + 'a' * 40, 'pin=' + 'b' * 64],
            )


if __name__ == '__main__':
    unittest.main()
