import json
import unittest

from common import digest
import native_empty_query_prefault_matrix as matrix
import test_native_begin_matrix as legacy


class ComposedReceipts(unittest.TestCase):
    def setUp(self):
        legacy.BeginMatrixTests.setUp(self)
        library = self.source/'libalopex_sql_parser.so'
        library.write_bytes(b'fixture parser')
        combined = json.loads((self.source/'receipt.json').read_text())
        combined.update(source=dict(commit='a'*40, tree='c'*40, profile='release'),
                        version='fixture', tools={}, cargo_lock_sha256='d'*64,
                        parser_contract='fixture', parser_record_sha256='e'*64,
                        parser_sha256=digest(library))
        original = dict(combined, schema='alopex.performance-wheel-build/v1')
        original.pop('server_sha256')
        self.original = self.source/'original-wheel.json'
        self.original.write_text(json.dumps(original,indent=3)+'\n\n')
        combined['build'] = dict(wheel=dict(mode='reused_development_asset',
            receipt_path=self.original.name, receipt_sha256=digest(self.original)))
        (self.source/'receipt.json').write_text(json.dumps(combined))
        manifest = json.loads(self.manifest.read_text())
        for role, path in [('library',library),('wheel_receipt',self.original)]:
            manifest['files'].append(dict(role=role,path=path.name))
        self.manifest.write_text(json.dumps(manifest))
        self.refresh()

    def refresh(self):
        manifest = json.loads(self.manifest.read_text())
        for item in manifest['files']:
            path = self.source/item['path']
            item.update(size=path.stat().st_size,sha256=digest(path))
        self.manifest.write_text(json.dumps(manifest))

    def prepare(self):
        return matrix.prepare_cell(self.manifest,self.root/'input',matrix.plan()[0],self.operators)

    def test_staging_preserves_both_receipts_and_extension_identity(self):
        before = {p.name:p.read_bytes() for p in self.source.iterdir() if p.is_file()}
        path, config = self.prepare()
        for name in ('receipt.json','original-wheel.json'):
            self.assertEqual((path.parent/name).read_bytes(),before[name])
        self.assertEqual(config['source']['commit'],'a'*40)
        self.assertEqual(config['extension_sha256'],matrix.extension_digest(self.source/'fixture.whl'))
        self.assertEqual(self.manifest.read_bytes(),before['assets.json'])

    def test_rehashed_original_with_different_source_is_rejected_before_staging(self):
        original = json.loads(self.original.read_text())
        original['source']['tree'] = 'f'*40
        self.original.write_text(json.dumps(original))
        combined = json.loads((self.source/'receipt.json').read_text())
        combined['build']['wheel']['receipt_sha256'] = digest(self.original)
        (self.source/'receipt.json').write_text(json.dumps(combined))
        self.refresh()
        with self.assertRaises(ValueError): self.prepare()
        self.assertFalse((self.root/'input').exists())

    def test_missing_original_receipt_role_is_rejected_before_staging(self):
        manifest = json.loads(self.manifest.read_text())
        manifest['files'] = [item for item in manifest['files'] if item['role'] != 'wheel_receipt']
        self.manifest.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError): self.prepare()
        self.assertFalse((self.root/'input').exists())

    def test_duplicate_original_receipt_role_is_rejected_before_staging(self):
        extra = self.source/'extra-original.json'
        extra.write_bytes(self.original.read_bytes())
        manifest = json.loads(self.manifest.read_text())
        manifest['files'].append(dict(role='wheel_receipt',path=extra.name,
                                     size=extra.stat().st_size,sha256=digest(extra)))
        self.manifest.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError): self.prepare()
        self.assertFalse((self.root/'input').exists())


if __name__ == '__main__':
    unittest.main()
