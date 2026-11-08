"""Exercise fixed asset download failures without network or tool execution."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / '.github/actions/cargo-deny/download.sh'


class DownloadTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.payload = self.root / 'payload'
        self.payload.write_bytes(b'fixed tool fixture')
        self.output = self.root / 'tool'
        curl = self.bin / 'curl'
        curl.write_text('''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
pathlib.Path(os.environ['FAKE_ARGS']).write_text('\\n'.join(args))
output = pathlib.Path(args[args.index('-o') + 1])
output.write_bytes(pathlib.Path(os.environ['FAKE_PAYLOAD']).read_bytes())
raise SystemExit(int(os.environ.get('FAKE_CODE', '0')))
''')
        curl.chmod(0o755)
        if not shutil.which('sha256sum'):
            self.assertIsNotNone(shutil.which('shasum'))
            shim = self.bin / 'sha256sum'
            shim.write_text('#!/bin/sh\nexec shasum -a 256 "$@"\n')
            shim.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                        FAKE_PAYLOAD=str(self.payload), FAKE_ARGS=str(self.root / 'arguments'))
        self.digest = hashlib.sha256(self.payload.read_bytes()).hexdigest()
        self.size = self.payload.stat().st_size

    def run_download(self, digest=None, size=None, url=None):
        return subprocess.run(['sh', str(SCRIPT), url or 'https://api.github.com/repos/example/tool/releases/assets/1',
                               digest or self.digest, str(self.size if size is None else size), str(self.output)],
                              env=self.env, capture_output=True, timeout=10)

    def assert_rejected_without_replacing_output(self, result):
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_bytes(), b'previous verified bytes')
        self.assertEqual(list(self.root.glob('tool.part.*')), [])

    def test_verified_bytes_are_published_with_bounded_https_request(self):
        result = self.run_download()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_bytes(), self.payload.read_bytes())
        self.assertEqual(list(self.root.glob('tool.part.*')), [])
        args = (self.root / 'arguments').read_text().splitlines()
        self.assertIn('--fail', args)
        self.assertEqual(args[args.index('--max-filesize') + 1], str(self.size))
        self.assertEqual(args[args.index('--retry') + 1], '2')
        self.assertEqual(args[args.index('--max-time') + 1], '45')
        self.assertEqual(args[args.index('--proto-redir') + 1], '=https')
        self.assertIn('Accept: application/octet-stream', args)
        self.assertFalse(any('Authorization' in arg for arg in args))

    def test_http_error_after_partial_write_is_never_published(self):
        self.output.write_bytes(b'previous verified bytes')
        self.env['FAKE_CODE'] = '22'
        self.assert_rejected_without_replacing_output(self.run_download())

    def test_wrong_size_and_corrupted_hash_preserve_existing_output(self):
        for digest, size in ((self.digest, self.size - 1), (self.digest, self.size + 1), ('0' * 64, self.size)):
            with self.subTest(digest=digest, size=size):
                self.output.write_bytes(b'previous verified bytes')
                self.assert_rejected_without_replacing_output(self.run_download(digest, size))

    def test_invalid_bound_or_url_is_rejected_before_download(self):
        for size, url in ((0, None), (16777217, None), ('no-limit', None),
                          (self.size, 'http://api.github.com/repos/example/tool/releases/assets/1'),
                          (self.size, 'https://example.test/tool')):
            with self.subTest(size=size, url=url):
                result = self.run_download(size=size, url=url)
                self.assertEqual(result.returncode, 64)
                self.assertFalse((self.root / 'arguments').exists())
                self.assertFalse(self.output.exists())


if __name__ == '__main__': unittest.main()
