"""Small provenance, receipt-preservation and archive-boundary regressions."""
import copy
import hashlib
import json
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from reused_native_wheel import digest, fetch_reuse, preserve_receipt, validate_archive, verify_reuse, verify_composed_receipts
import test_hosted_native_wheel_assets as wheel_tests


class ReusedWheelTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / 'import'
        assets = self.root / 'assets'
        assets.mkdir(parents=True)
        self.manifest, self.config = wheel_tests.WheelAssetsTests().fixture(assets)
        self.config['parser_record_sha256'] = 'c' * 64
        receipt_path = assets / 'build.json'
        self.receipt = json.loads(receipt_path.read_bytes())
        self.receipt.update(version='fixture-version', cargo_lock_sha256='d' * 64,
            parser_contract='fixture-parser', parser_record_sha256='c' * 64,
            build=dict(incremental=False, release_debug=False,
                path_remap=dict(control='/alopex-build/control', work='/alopex-build/work'),
                wheel='maturin build --locked --release --compatibility linux'))
        receipt_path.write_text(json.dumps(self.receipt, indent=3) + '\n\n')
        self.refresh()

    def refresh(self):
        assets = self.root / 'assets'
        for item in self.manifest['files']:
            path = assets / item['path']
            item.update(size=path.stat().st_size, sha256=digest(path))
        (assets / 'assets.json').write_text(json.dumps(self.manifest))
        self.result = dict(schema='alopex.native-wheel-assets-result/v1', status='passed',
            source=self.receipt['source'], assets_digest=digest(assets / 'assets.json'))
        (self.root / 'result.json').write_text(json.dumps(self.result))
        self.expected = dict(assets_digest=self.result['assets_digest'],
            files={p.relative_to(self.root).as_posix(): digest(p) for p in self.root.rglob('*') if p.is_file()})

    def verify(self, **changes):
        args = dict(version='fixture-version', cargo_lock_sha256='d' * 64,
                    parser_sha256=hashlib.sha256(b'parser').hexdigest(), parser_contract='fixture-parser')
        args.update(changes)
        return verify_reuse(self.root, self.expected, self.config, **args)

    def test_original_receipt_bytes_are_preserved(self):
        paths, receipt = self.verify()
        target = self.root.parent / 'output/receipts/original-wheel-build.json'
        sha = preserve_receipt(paths['receipt'], target)
        self.assertEqual(target.read_bytes(), paths['receipt'].read_bytes())
        self.assertEqual(sha, digest(paths['receipt']))
        self.assertEqual(receipt, self.receipt)
        with self.assertRaises(ValueError):
            preserve_receipt(paths['receipt'], target)

    def test_modified_input_and_unlisted_file_are_rejected(self):
        (self.root / 'assets/package.whl').write_bytes(b'different')
        with self.assertRaises(ValueError): self.verify()
        (self.root / 'assets/package.whl').write_bytes(b'wheel')
        (self.root / 'extra').write_bytes(b'extra')
        with self.assertRaises(ValueError): self.verify()

    def test_same_hashed_receipt_cannot_substitute_product_or_build(self):
        for field, value in [('version', 'other'), ('cargo_lock_sha256', 'e' * 64),
                             ('parser_contract', 'other'), ('parser_record_sha256', 'e' * 64)]:
            with self.subTest(field=field):
                receipt = copy.deepcopy(self.receipt)
                receipt[field] = value
                (self.root / 'assets/build.json').write_text(json.dumps(receipt))
                self.refresh()
                with self.assertRaises(ValueError): self.verify()

    def test_source_tree_platform_and_toolchain_must_match(self):
        for field in ('source_commit', 'source_tree', 'platform', 'rust_version', 'maturin_version'):
            old = self.config[field]
            self.config[field] = 'different'
            with self.subTest(field=field), self.assertRaises(ValueError): self.verify()
            self.config[field] = old

    def test_build_mode_and_remap_cannot_change(self):
        for field, value in [('incremental', True), ('release_debug', True),
                             ('path_remap', {}), ('wheel', 'other')]:
            with self.subTest(field=field):
                receipt = copy.deepcopy(self.receipt)
                receipt['build'][field] = value
                (self.root / 'assets/build.json').write_text(json.dumps(receipt))
                self.refresh()
                with self.assertRaises(ValueError): self.verify()

    def test_failed_producer_result_is_rejected_even_with_matching_hash(self):
        self.result['status'] = 'failed'
        path = self.root / 'result.json'
        path.write_text(json.dumps(self.result))
        self.expected['files']['result.json'] = digest(path)
        with self.assertRaises(ValueError): self.verify()

    def test_symlinked_asset_is_rejected(self):
        target = self.root / 'assets/package.whl'
        target.unlink()
        target.symlink_to(self.root / 'assets/parser.so')
        with self.assertRaises(ValueError): self.verify()

    def test_archive_inventory_and_entry_types(self):
        entry = zipfile.ZipInfo('assets/wheel.whl')
        entry.file_size = 5
        entry.external_attr = (stat.S_IFREG | 0o600) << 16
        expected = dict(files={'assets/wheel.whl': 'unused'})
        self.assertEqual(validate_archive([entry], expected), [entry])
        for name in ('../wheel.whl', '/wheel.whl', 'assets\\wheel.whl'):
            bad = copy.copy(entry); bad.filename = name
            with self.subTest(name=name), self.assertRaises(ValueError): validate_archive([bad], expected)
        for kind in (stat.S_IFLNK, stat.S_IFCHR):
            bad = copy.copy(entry); bad.external_attr = kind << 16
            with self.subTest(kind=kind), self.assertRaises(ValueError): validate_archive([bad], expected)
        with self.assertRaises(ValueError): validate_archive([entry, entry], expected)
        bad = copy.copy(entry); bad.file_size = 100_000_001
        with self.assertRaises(ValueError): validate_archive([bad], expected)

    def transport_fixture(self):
        archive = self.root.parent / 'fixture.zip'
        with zipfile.ZipFile(archive, 'w') as output:
            output.writestr('result.json', b'fixture')
        raw = archive.read_bytes()
        expected = dict(run=1, controller='a'*40, artifact_id=2,
            artifact_digest='sha256:' + digest(archive), artifact_size_bytes=len(raw),
            files={'result.json': hashlib.sha256(b'fixture').hexdigest()})
        run = dict(head_sha='a'*40, event='workflow_dispatch', status='completed', conclusion='success')
        artifact = dict(workflow_run=dict(id=1), expired=False, name='native-wheel-assets',
                        digest=expected['artifact_digest'], size_in_bytes=len(raw))
        calls = []
        class Budget:
            def before(self, operation): calls.append(('before', operation))
            def check(self): calls.append(('check',))
            def run(self, command, *, timeout, log):
                calls.append(('download',)); log.write_bytes(raw)
        return expected, run, artifact, Budget(), calls

    def test_transport_preserves_files_and_removes_archive(self):
        expected, run, artifact, budget, calls = self.transport_fixture()
        target = self.root.parent / 'download'
        with patch('reused_native_wheel.subprocess.run', side_effect=[
                SimpleNamespace(stdout=json.dumps(run)), SimpleNamespace(stdout=json.dumps(artifact))]):
            fetch_reuse(target, expected, budget)
        self.assertEqual((target / 'result.json').read_bytes(), b'fixture')
        self.assertFalse((target.parent / 'reused-wheel.zip').exists())
        self.assertEqual(calls[0], ('before', 'prepare'))

    def test_wrong_producer_or_artifact_is_rejected_before_download(self):
        expected, run, artifact, budget, calls = self.transport_fixture()
        for field in ('head_sha', 'conclusion', 'event'):
            changed = dict(run, **{field: 'different'})
            with self.subTest(field=field), patch('reused_native_wheel.subprocess.run',
                    return_value=SimpleNamespace(stdout=json.dumps(changed))), self.assertRaises(ValueError):
                fetch_reuse(self.root.parent / 'download', expected, budget)
        for field, value in [('expired', True), ('workflow_run', {'id': 3}),
                             ('digest', 'sha256:'+'e'*64), ('size_in_bytes', 100_000_001)]:
            changed = dict(artifact, **{field: value})
            with self.subTest(field=field), patch('reused_native_wheel.subprocess.run', side_effect=[
                    SimpleNamespace(stdout=json.dumps(run)), SimpleNamespace(stdout=json.dumps(changed))]), self.assertRaises(ValueError):
                fetch_reuse(self.root.parent / 'download', expected, budget)
        self.assertEqual(calls, [])

    def test_transport_digest_failure_removes_archive_without_extracting(self):
        expected, run, artifact, budget, calls = self.transport_fixture()
        expected['artifact_digest'] = artifact['digest'] = 'sha256:'+'e'*64
        target = self.root.parent / 'download'
        with patch('reused_native_wheel.subprocess.run', side_effect=[
                SimpleNamespace(stdout=json.dumps(run)), SimpleNamespace(stdout=json.dumps(artifact))]), self.assertRaises(ValueError):
            fetch_reuse(target, expected, budget)
        self.assertFalse((target.parent / 'reused-wheel.zip').exists())
        self.assertEqual(list(target.iterdir()), [])

    def test_composed_receipt_retains_and_binds_original_provenance(self):
        paths, original = self.verify()
        assets = self.root / 'assets'
        original_path = assets / 'original.json'
        original_hash = preserve_receipt(paths['receipt'], original_path)
        server = assets / 'server'
        server.write_bytes(b'server')
        combined = dict(original, schema='alopex.performance-build/v1', server_sha256=digest(server),
            build=dict(wheel=dict(mode='reused_development_asset', receipt_path='original.json', receipt_sha256=original_hash)))
        paths['receipt'].write_text(json.dumps(combined))
        files = [dict(role=role, path=path.relative_to(assets).as_posix(), size=path.stat().st_size, sha256=digest(path))
                 for role, path in [('receipt', paths['receipt']), ('wheel_receipt', original_path),
                                    ('server', server), ('wheel', paths['wheel']), ('library', paths['library'])]]
        manifest = dict(source_commit=original['source']['commit'], files=files)
        self.assertEqual(verify_composed_receipts(assets, manifest)['wheel_receipt'].read_bytes(), original_path.read_bytes())
        for field, value in [('source', dict(original['source'], commit='e'*40)),
                             ('tools', {}), ('parser_record_sha256', 'e'*64), ('server_sha256', 'e'*64)]:
            changed = dict(combined, **{field: value})
            paths['receipt'].write_text(json.dumps(changed))
            files[0].update(size=paths['receipt'].stat().st_size, sha256=digest(paths['receipt']))
            with self.subTest(field=field), self.assertRaises(ValueError): verify_composed_receipts(assets, manifest)


if __name__ == '__main__':
    unittest.main()
