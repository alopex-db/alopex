"""Public certificate boundaries and disposable synthetic CMS rotation fixtures."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

OPS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPS))
from check_evidence_certificate import check, reviewed_pin, REPO


class CertificateTests(unittest.TestCase):
    cert = Path(__file__).parent / 'fixtures/native-evidence-20261105.pem'
    start = datetime(2026, 11, 5, 15, 0, 49, tzinfo=timezone.utc)
    end = datetime(2026, 12, 5, 15, 0, 49, tzinfo=timezone.utc)

    def setUp(self):
        self.pin = hashlib.sha256(self.cert.read_bytes()).hexdigest()

    def test_current_pem_matches_reviewed_pin(self):
        self.assertEqual(hashlib.sha256((REPO/'.github/ops/native-evidence-cert.pem').read_bytes()).hexdigest(),
                         reviewed_pin(OPS/'s04_inspect_seal.py'))

    def test_reviewed_certificate_dates_and_identity(self):
        result = check(self.cert, self.pin, now=self.start)
        self.assertEqual(result['not_after'], self.end.isoformat())

    def test_time_boundaries(self):
        for instant, code in ((self.start-timedelta(seconds=1), 'CERT_NOT_YET_VALID'),
                              (self.end, 'CERT_EXPIRED'),
                              (self.end+timedelta(days=1), 'CERT_EXPIRED')):
            with self.subTest(code=code, instant=instant), self.assertRaisesRegex(ValueError, code):
                check(self.cert, self.pin, now=instant)
        check(self.cert, self.pin, now=self.end-timedelta(seconds=1))

    def test_work_window_requires_strictly_more_than_job_budget(self):
        with self.assertRaisesRegex(ValueError, 'CERT_EXPIRING'):
            check(self.cert, self.pin, now=self.end-timedelta(days=1), minimum_seconds=86400)
        check(self.cert, self.pin, now=self.end-timedelta(seconds=86401), minimum_seconds=86400)

    def test_wrong_recipient_identity(self):
        with self.assertRaisesRegex(ValueError, 'CERT_MISMATCH'):
            check(self.cert, '0'*64, now=self.start)

    def test_invalid_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'bad.pem'
            for data in (b'invalid', self.cert.read_bytes()*2, b'-----BEGIN PRIVATE KEY-----'):
                path.write_bytes(data)
                with self.assertRaisesRegex(ValueError, 'CERT_INVALID'):
                    check(path, hashlib.sha256(data).hexdigest(), now=self.start)
            path.unlink()
            path.symlink_to(self.cert)
            with self.assertRaisesRegex(ValueError, 'CERT_INVALID'):
                check(path, self.pin, now=self.start)

    def test_cli_error_does_not_print_input_path_or_exception(self):
        result = subprocess.run([sys.executable, str(OPS/'check_evidence_certificate.py'),
                                 '--certificate', '/private/secret-location.pem'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, '')
        self.assertEqual(json.loads(result.stdout), {'outcome':'failed', 'error_code':'CERT_INVALID'})

    def test_negative_budget_rejected(self):
        with self.assertRaisesRegex(ValueError, 'CERT_WINDOW_INVALID'):
            check(self.cert, self.pin, now=self.start, minimum_seconds=-1)

    def test_workflow_admits_every_lane_and_frozen_recipient(self):
        source = (REPO/'.github/workflows/alopex-performance.yml').read_text()
        for name, work in (('inventory', 'Collect encrypted read-only host inventory'),
                           ('qualification', 'Admit this runner before candidate acquisition'),
                           ('wal_cost_682', 'Qualify, capture signed indexes, measure WAL, and check candidate closure'),
                           ('s04_inspect', 'Inspect selected Docker context read-only')):
            import re
            match = re.search(r'^  '+name+r':\n(.*?)(?=^  [a-z0-9_]+:|\Z)', source, re.M | re.S)
            self.assertIsNotNone(match, name)
            job = match.group(1)
            self.assertLess(job.index('check_evidence_certificate.py'), job.index(work))
            self.assertIn('--minimum-seconds 86400', job)
        self.assertIn('--certificate frozen-wal-controller/.github/ops/native-evidence-cert.pem', source)
        self.assertIn('ref: fd34ca342617c247a9c711bcf597da8bbef3118f', source)
        current_pin = reviewed_pin(OPS/'s04_inspect_seal.py')
        self.assertEqual(source.count('--expected-sha256 '+current_pin), 1)
        self.assertNotEqual(current_pin, self.pin)

    def test_synthetic_rotation_and_archived_decryption(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def run(*args, success=True):
                result = subprocess.run(['openssl', *map(str,args)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.assertEqual(result.returncode == 0, success, 'synthetic OpenSSL operation status')
                return result.stdout
            for name in ('old','new'):
                run('req','-x509','-newkey','rsa:2048','-nodes','-keyout',root/(name+'.key'),
                    '-out',root/(name+'.pem'),'-subj','/CN=disposable-test','-days','1')
                (root/(name+'.key')).chmod(0o600)
            plaintext = b'synthetic receipt only\n'
            (root/'receipt').write_bytes(plaintext)
            for name in ('old','new'):
                cert = root/(name+'.pem')
                pin = hashlib.sha256(cert.read_bytes()).hexdigest()
                check(cert,pin)
                run('cms','-encrypt','-binary','-aes-256-cbc','-outform','DER',
                    '-in',root/'receipt','-out',root/(name+'.cms'),cert)
                with self.assertRaisesRegex(ValueError, 'CERT_EXPIRED'):
                    check(cert,pin,now=datetime.now(timezone.utc)+timedelta(days=2))
                self.assertEqual(run('cms','-decrypt','-binary','-inform','DER',
                                     '-in',root/(name+'.cms'),'-recip',cert,'-inkey',root/(name+'.key')),plaintext)
            run('cms','-decrypt','-binary','-inform','DER','-in',root/'old.cms',
                '-recip',root/'new.pem','-inkey',root/'new.key',success=False)
            self.assertEqual((root/'receipt').read_bytes(),plaintext)


if __name__ == '__main__':
    unittest.main()
