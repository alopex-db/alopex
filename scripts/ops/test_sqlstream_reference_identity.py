"""Tiny wheel fixtures validate reference-package identity without installing it."""
import contextlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import warnings
import zipfile

import sqlstream_reference_identity as worker


class ReferenceIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.prefix = self.root/'venv'
        self.site = self.prefix/'packages'
        self.site.mkdir(parents=True)
        self.record = dict(schema='alopex.reference-wheels/v1',engine='datafusion',wheels=[])
        for name, package in [('datafusion','datafusion'),('pyarrow','pyarrow'),('typing-extensions','typing_extensions.py')]:
            path = self.root/(name+'.whl')
            files = {package:b'# tiny module\n'} if package.endswith('.py') else {package+'/__init__.py':b'# tiny fixture\n',package+'/module.py':b'value = 1\n'}
            with zipfile.ZipFile(path,'w') as archive:
                for relative, content in files.items():
                    archive.writestr(relative,content)
                    target = self.site/relative
                    target.parent.mkdir(exist_ok=True)
                    target.write_bytes(content)
            self.record['wheels'].append(dict(name=name,version={'datafusion':'50.0.0','pyarrow':'26.0.0','typing-extensions':'4.15.0'}[name],path=str(path),sha256=worker.sha(path)))
        self.identity = self.root/'identity.json'

    def verify(self, *, origin=None, version=None):
        self.identity.write_text(json.dumps(self.record))
        def distribution(name):return SimpleNamespace(version=version or {'datafusion':'50.0.0','pyarrow':'26.0.0','typing-extensions':'4.15.0'}[name],locate_file=lambda relative:self.site/relative)
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(worker.sys,'prefix',str(self.prefix)))
            stack.enter_context(patch.object(worker.importlib.metadata,'distribution',side_effect=distribution))
            stack.enter_context(patch.object(worker.importlib.util,'find_spec',side_effect=lambda name:SimpleNamespace(
                origin=str(origin or self.site/(name+'.py' if name=='typing_extensions' else name+'/__init__.py')))))
            return worker.reference_identity(self.identity,worker.sha(self.identity))

    def rewrite_wheel(self, name, content):
        entry = self.record['wheels'][0]
        path = Path(entry['path'])
        with zipfile.ZipFile(path,'a') as archive:archive.writestr(name,content)
        entry['sha256'] = worker.sha(path)

    def test_verifies_both_original_wheels_and_installed_packages(self):
        self.assertEqual(self.verify(),self.record)

    def test_rejects_changed_wheel_installed_file_and_extra_module(self):
        entry = self.record['wheels'][0]
        original = entry['sha256']
        entry['sha256'] = '0'*64
        with self.assertRaises(AssertionError):self.verify()
        entry['sha256'] = original
        target = self.site/'datafusion/module.py'
        original_bytes = target.read_bytes()
        target.write_bytes(b'value = 2\n')
        with self.assertRaises(AssertionError):self.verify()
        target.write_bytes(original_bytes)
        (self.site/'datafusion/unrecorded.py').write_bytes(b'# extra\n')
        with self.assertRaises(AssertionError):self.verify()

    def test_rejects_distribution_or_loaded_module_substitution(self):
        with self.assertRaises(AssertionError):self.verify(version='0.0.0')
        with self.assertRaises(AssertionError):self.verify(origin=self.root/'elsewhere.py')
        target = self.site/'datafusion/module.py'
        escaped = self.root/'escaped.py'
        escaped.write_bytes(target.read_bytes())
        target.unlink();target.symlink_to(escaped)
        with self.assertRaises(AssertionError):self.verify()

    def test_rejects_archive_traversal(self):
        self.rewrite_wheel('../escape.py',b'escape')
        with self.assertRaises(AssertionError):self.verify()

    def test_rejects_duplicate_archive_members(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore',UserWarning)
            self.rewrite_wheel('datafusion/module.py',b'value = 1\n')
        with self.assertRaises(AssertionError):self.verify()

    def test_rejects_wrong_manifest_digest_or_missing_runtime(self):
        self.identity.write_text(json.dumps(self.record))
        with self.assertRaises(AssertionError):worker.reference_identity(self.identity,'0'*64)
        self.record['wheels'].pop()
        with self.assertRaises(AssertionError):self.verify()


if __name__ == '__main__':unittest.main()
