"""Small fake wheels verify installed-file identity and rejection boundaries."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import hnsw_search_identity as subject


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.site=self.root/'site';self.site.mkdir()
        self.wheel=self.root/'tiny.whl';self.identity=self.root/'identity.json'
        self.files={'tiny/__init__.py':b'VALUE = 1\n','tiny/native.bin':b'123',
            'single.py':b'VALUE = 2\n','tiny-1.dist-info/METADATA':b'Name: tiny\nVersion: 1\n'}
        self.write()

    def write(self):
        with zipfile.ZipFile(self.wheel,'w') as archive:
            for name,data in self.files.items():
                archive.writestr(name,data)
                if not name.startswith('../'):
                    path=self.site/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
        self.record=dict(schema='alopex.hnsw-dependencies/v1',wheels=[dict(name='tiny',version='1',
            path=str(self.wheel),sha256=subject.sha(self.wheel))],modules={'tiny':'site/tiny/__init__.py'})
        self.identity.write_text(json.dumps(self.record))

    def check(self,version='1',origin=None):
        distribution=SimpleNamespace(version=version,locate_file=lambda name:self.site/name)
        spec=SimpleNamespace(origin=str(origin or self.site/'tiny/__init__.py'))
        with patch.object(subject.sys,'prefix',str(self.root)), \
             patch.object(subject.importlib.metadata,'distribution',return_value=distribution), \
             patch.object(subject.importlib.util,'find_spec',return_value=spec):
            return subject.verify_identity(self.identity,subject.sha(self.identity))

    def test_package_and_single_file_identity(self):
        self.assertEqual(self.check(),self.record)

    def test_changed_file_and_injected_module_rejected(self):
        (self.site/'tiny/native.bin').write_bytes(b'456')
        with self.assertRaises(RuntimeError):self.check()
        (self.site/'tiny/native.bin').write_bytes(b'123')
        (self.site/'tiny/extra.py').write_bytes(b'extra')
        with self.assertRaises(RuntimeError):self.check()

    def test_wheel_digest_version_and_import_shadow_rejected(self):
        with self.assertRaises(RuntimeError):self.check(version='2')
        with self.assertRaises(RuntimeError):self.check(origin=self.site/'single.py')
        self.wheel.write_bytes(self.wheel.read_bytes()+b'changed')
        with self.assertRaises(RuntimeError):self.check()

    def test_escaping_archive_and_unsupported_relocation_rejected(self):
        self.files['../escape']=b'bad';self.write()
        with self.assertRaises(RuntimeError):self.check()
        del self.files['../escape'];self.files['tiny-1.data/purelib/foreign.py']=b'bad';self.write()
        with self.assertRaises(RuntimeError):self.check()

    def test_duplicate_distribution_and_changed_identity_rejected(self):
        self.record['wheels']*=2;self.identity.write_text(json.dumps(self.record))
        with self.assertRaises(RuntimeError):self.check()
        with self.assertRaises(RuntimeError):subject.verify_identity(self.identity,'0'*64)


if __name__=='__main__':unittest.main()
