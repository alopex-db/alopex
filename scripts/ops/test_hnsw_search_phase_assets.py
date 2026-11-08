import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from hosted_hnsw_search_phase_assets import verify_wheel_assets, validate_storage_boundary


class WheelAssetsTests(unittest.TestCase):
    def fixture(self, root):
        config = dict(platform='fixture', source_commit='a'*40, source_tree='b'*40,
                      rust_version='fixture-rust', maturin_version='fixture-maturin')
        (root/'package.whl').write_bytes(b'wheel')
        (root/'parser.so').write_bytes(b'parser')
        receipt = dict(schema='alopex.performance-wheel-build/v1',
            source=dict(commit=config['source_commit'], tree=config['source_tree'], profile='release'),
            tools=dict(rust=config['rust_version'], maturin=config['maturin_version']),
            python_wheel_sha256=hashlib.sha256(b'wheel').hexdigest(),
            parser_sha256=hashlib.sha256(b'parser').hexdigest())
        (root/'build.json').write_text(json.dumps(receipt))
        files = [dict(role=role,path=name,size=(root/name).stat().st_size,
                      sha256=hashlib.sha256((root/name).read_bytes()).hexdigest())
                 for role,name in [('wheel','package.whl'),('library','parser.so'),('receipt','build.json')]]
        return dict(schema='alopex.native-wheel-assets/v1',platform=config['platform'],
                    source_commit=config['source_commit'],source_tree=config['source_tree'],files=files),config

    def test_exact_wheel_parser_receipt_without_server(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); manifest,config=self.fixture(root)
            verify_wheel_assets(root,manifest,config)
            (root/'package.whl').write_bytes(b'other')
            with self.assertRaises(ValueError): verify_wheel_assets(root,manifest,config)

    def test_identity_shape_and_escape_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); original,config=self.fixture(root)
            for change in ('commit','tree','platform','role','duplicate','escape','absolute','size'):
                manifest=copy.deepcopy(original)
                if change=='commit':manifest['source_commit']='c'*40
                if change=='tree':manifest['source_tree']='c'*40
                if change=='platform':manifest['platform']='other'
                if change=='role':manifest['files'][0]['role']='server'
                if change=='duplicate':manifest['files'][0]['path']='build.json'
                if change=='escape':manifest['files'][0]['path']='../package.whl'
                if change=='absolute':manifest['files'][0]['path']='/package.whl'
                if change=='size':manifest['files'][0]['size']+=1
                with self.subTest(change=change),self.assertRaises(ValueError): verify_wheel_assets(root,manifest,config)

    def test_self_consistent_manifest_cannot_substitute_receipt_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); manifest,config=self.fixture(root)
            receipt=json.loads((root/'build.json').read_text());receipt['source']['commit']='c'*40
            (root/'build.json').write_text(json.dumps(receipt))
            manifest['files'][2].update(size=(root/'build.json').stat().st_size,
                sha256=hashlib.sha256((root/'build.json').read_bytes()).hexdigest())
            with self.assertRaises(ValueError): verify_wheel_assets(root,manifest,config)

    def test_backing_filesystem_counted_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); counted=root/'counted'; scratch=root/'mount'; disk=counted/'disk'
            validate_storage_boundary(scratch,disk,[counted],1024)
            with self.assertRaises(ValueError):validate_storage_boundary(scratch,disk,[root],1024)
            with self.assertRaises(ValueError):validate_storage_boundary(scratch,disk,[scratch],1024)


if __name__=='__main__':unittest.main()
