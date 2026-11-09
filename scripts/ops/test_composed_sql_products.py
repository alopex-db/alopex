"""Tiny full-product fixtures for legacy and preserved-receipt acceptance."""
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import zipfile

from common import Failure, digest
from hosted_native_sql import verify_products


class ProductProvenance(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name)
        (self.root/'server').write_bytes(b'fixture server')
        (self.root/'libalopex_sql_parser.so').write_bytes(b'fixture parser')
        with zipfile.ZipFile(self.root/'fixture.whl','w') as archive:
            archive.writestr('fixture.py',b'# fixture\n')
        with tarfile.open(self.root/'harness.tar','w') as archive:
            data=b'# fixture\n';item=tarfile.TarInfo('fixture.py');item.size=len(data)
            archive.addfile(item,io.BytesIO(data))
        identity=dict(commit='a'*40,tree='b'*40,profile='release')
        self.receipt=dict(schema='alopex.performance-build/v1',source=identity,version='fixture',
            tools={},cargo_lock_sha256='c'*64,parser_contract='fixture',parser_record_sha256='d'*64,
            server_sha256=digest(self.root/'server'),python_wheel_sha256=digest(self.root/'fixture.whl'),
            parser_sha256=digest(self.root/'libalopex_sql_parser.so'))
        self.original=dict(self.receipt,schema='alopex.performance-wheel-build/v1')
        self.original.pop('server_sha256')
        (self.root/'original.json').write_text(json.dumps(self.original,indent=3)+'\n\n')
        self.receipt['build']=dict(wheel=dict(mode='reused_development_asset',receipt_path='original.json',
                                             receipt_sha256=digest(self.root/'original.json')))
        self.manifest=dict(schema='alopex.test-env-assets/v1',platform='fixture-platform',source_commit='a'*40,
            harness_commit='e'*40,expanded_limit_bytes=100000,files=[dict(role=role,path=name) for role,name in [
                ('server','server'),('wheel','fixture.whl'),('library','libalopex_sql_parser.so'),
                ('receipt','build.json'),('wheel_receipt','original.json'),('harness','harness.tar')]])
        self.expected=dict(source_commit='a'*40,source_tree='b'*40,version='fixture',
                           platform='fixture-platform',harness_commit='e'*40)
        self.result=dict(schema='alopex.native-assets/v1',status='passed',source=identity)
        self.refresh()

    def refresh(self):
        (self.root/'build.json').write_text(json.dumps(self.receipt))
        for item in self.manifest['files']:
            path=self.root/item['path'];item.update(size=path.stat().st_size,sha256=digest(path))
        (self.root/'assets.json').write_text(json.dumps(self.manifest))
        self.result['assets_digest']=digest(self.root/'assets.json')
        (self.root/'result.json').write_text(json.dumps(self.result))
        self.expected['assets_digest']=self.result['assets_digest']

    def verify(self):
        return verify_products(self.root/'assets.json',self.root/'result.json',self.expected)

    def test_original_receipt_is_verified_without_rewriting(self):
        before=(self.root/'original.json').read_bytes()
        self.assertEqual(self.verify(),self.manifest)
        self.assertEqual((self.root/'original.json').read_bytes(),before)

    def test_legacy_full_products_still_work_without_reuse(self):
        self.receipt.pop('build')
        self.manifest['files']=[item for item in self.manifest['files'] if item['role']!='wheel_receipt']
        self.refresh();self.expected.pop('assets_digest')
        self.assertEqual(self.verify(),self.manifest)

    def test_reuse_requires_external_asset_digest(self):
        self.expected.pop('assets_digest')
        with self.assertRaises(Failure):self.verify()

    def test_external_asset_digest_mismatch_is_rejected(self):
        self.expected['assets_digest']='f'*64
        with self.assertRaises(Failure):self.verify()

    def test_reuse_requires_original_receipt(self):
        self.manifest['files']=[item for item in self.manifest['files'] if item['role']!='wheel_receipt']
        self.refresh()
        with self.assertRaises(Failure):self.verify()

    def test_rehashed_original_source_substitution_is_rejected(self):
        original=json.loads((self.root/'original.json').read_text())
        original['source']['tree']='f'*40
        (self.root/'original.json').write_text(json.dumps(original))
        self.receipt['build']['wheel']['receipt_sha256']=digest(self.root/'original.json');self.refresh()
        with self.assertRaises(ValueError):self.verify()

    def test_rehashed_wheel_substitution_is_rejected_by_original_receipt(self):
        with zipfile.ZipFile(self.root/'fixture.whl','w') as archive:
            archive.writestr('other.py',b'changed')
        self.receipt['python_wheel_sha256']=digest(self.root/'fixture.whl');self.refresh()
        with self.assertRaises(ValueError):self.verify()

    def test_duplicate_original_receipt_role_is_rejected(self):
        (self.root/'another.json').write_bytes((self.root/'original.json').read_bytes())
        self.manifest['files'].append(dict(role='wheel_receipt',path='another.json'));self.refresh()
        with self.assertRaises(ValueError):self.verify()


if __name__ == '__main__':
    unittest.main()
