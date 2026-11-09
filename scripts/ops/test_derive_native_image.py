"""Tiny source-boundary and execution-config regressions for image derivation."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from derive_native_image import CHANGES, PROBE_VALIDATION, dockerfile, source_digest, validate_source_delta, verify_image_delta


class ImageIdentity(unittest.TestCase):
    def setUp(self):
        self.before={'capacity.py':'a'*64,'common.py':'b'*64,'Dockerfile':'c'*64}
        self.after=dict(self.before, **{'capacity.py':'d'*64,'tests/test_capacity_snapshot.py':'e'*64})
        self.record=dict(id='sha256:'+'f'*64,controller_digest=source_digest(self.before))
        self.source=dict(controller_digest=source_digest(self.after))
        self.base=dict(Id='sha256:'+'a'*64,Os='fixture-os',Architecture='fixture-arch',
            Config=dict(User='alopex',Entrypoint=['python3','/tool/entrypoint.py'],
                        Env=['HOME=/work/home'],WorkingDir='',Labels=None),
            RootFS=dict(Type='layers',Layers=['sha256:'+'b'*64]))
        self.updated=copy.deepcopy(self.base)
        self.updated['Id']='sha256:'+'c'*64
        self.updated['RootFS']['Layers']+=['sha256:'+'d'*64,'sha256:'+'e'*64]

    def test_exact_two_source_changes_are_accepted(self):
        validate_source_delta(self.before,self.after,self.record,self.source)

    def test_unrelated_source_change_is_rejected(self):
        after=dict(self.after,**{'common.py':'f'*64})
        with self.assertRaises(ValueError):
            validate_source_delta(self.before,after,self.record,dict(controller_digest=source_digest(after)))

    def test_removed_source_is_rejected(self):
        after=dict(self.after);del after['common.py']
        with self.assertRaises(ValueError):
            validate_source_delta(self.before,after,self.record,dict(controller_digest=source_digest(after)))

    def test_original_and_current_digests_must_match(self):
        for before,after in [(dict(self.before,**{'common.py':'1'*64}),self.after),
                             (self.before,dict(self.after,**{'capacity.py':'2'*64}))]:
            with self.subTest(before=before),self.assertRaises(ValueError):
                validate_source_delta(before,after,self.record,self.source)

    def test_source_paths_are_contained(self):
        for name in ('../private.py','/absolute.py','tests/../../private.py','tests\\file.py'):
            with self.subTest(name=name),self.assertRaises(ValueError):source_digest({name:'a'*64})

    def test_image_retains_base_layers_and_runtime_config(self):
        self.assertEqual(verify_image_delta(self.base,self.updated),self.updated['RootFS']['Layers'][-2:])

    def test_runtime_privilege_or_entrypoint_changes_are_rejected(self):
        for field,value in [('User','root'),('Entrypoint',['sh']),('Env',[]),('WorkingDir','/'),('Labels',{'changed':'yes'})]:
            updated=copy.deepcopy(self.updated);updated['Config'][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):verify_image_delta(self.base,updated)

    def test_changed_or_missing_base_layer_is_rejected(self):
        for layers in [self.updated['RootFS']['Layers'][1:],['changed',*self.updated['RootFS']['Layers'][1:]],
                       self.updated['RootFS']['Layers']+['extra']]:
            updated=copy.deepcopy(self.updated);updated['RootFS']['Layers']=layers
            with self.assertRaises(ValueError):verify_image_delta(self.base,updated)

    def test_platform_change_is_rejected(self):
        for field in ('Os','Architecture','Variant'):
            updated=copy.deepcopy(self.updated);updated[field]='different'
            with self.subTest(field=field),self.assertRaises(ValueError):verify_image_delta(self.base,updated)

    def test_dockerfile_copies_only_declared_changes(self):
        source=dockerfile('alopex-derived-base:'+'a'*16)
        self.assertEqual(source.splitlines()[1:],['COPY capacity.py /tool/capacity.py',
            'COPY test_capacity_snapshot.py /tool/tests/test_capacity_snapshot.py'])
        self.assertFalse(any(line.startswith(('RUN ','USER ','ENV ','ENTRYPOINT ','CMD ')) for line in source.splitlines()))
        for tag in ('other:latest','alopex-derived-base:bad\nRUN command','sha256:abc'):
            with self.assertRaises(ValueError):dockerfile(tag)


class ProbeFiles(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name);self.root.chmod(0o755)
        (self.root/'tests').mkdir(mode=0o755)
        self.files={}
        for name in ('capacity.py','image_verify.py','tests/test_capacity_snapshot.py'):
            path=self.root/name;path.write_bytes(b'# fixture\n');path.chmod(0o644)
            self.files[name]=hashlib.sha256(path.read_bytes()).hexdigest()
        manifest={'packages':{'fixture':'1'}}
        (self.root/'image-manifest.json').write_text(json.dumps(manifest))
        self.request=dict(image_source_files=self.files,
            image_manifest_digest=hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest())
        self.actual_lstat=Path.lstat

    def verify(self, override=None):
        def lstat(path):
            original=self.actual_lstat(path)
            info=dict(st_mode=original.st_mode,st_uid=0,st_gid=0)
            if override and path==self.root/'capacity.py':info.update(override)
            return SimpleNamespace(**info)
        with patch.object(Path,'lstat',lstat),patch('runpy.run_path') as verify_packages:
            exec(compile(PROBE_VALIDATION,'image-probe','exec'),
                 dict(request=self.request,pathlib=SimpleNamespace(Path=lambda _:self.root),json=json))
            verify_packages.assert_called_once_with(str(self.root/'image_verify.py'))

    def test_verified_files_require_package_inventory_verification(self):self.verify()

    def test_changed_code_is_rejected(self):
        (self.root/'capacity.py').write_bytes(b'changed')
        with self.assertRaises(AssertionError):self.verify()

    def test_unlisted_python_file_is_rejected(self):
        (self.root/'extra.py').write_bytes(b'extra')
        with self.assertRaises(AssertionError):self.verify()

    def test_unexpected_owner_or_writable_code_is_rejected(self):
        for override in (dict(st_uid=123),dict(st_gid=123),dict(st_mode=0o100666)):
            with self.subTest(override=override),self.assertRaises(AssertionError):self.verify(override)

    def test_changed_package_manifest_is_rejected(self):
        (self.root/'image-manifest.json').write_text('{}')
        with self.assertRaises(AssertionError):self.verify()


if __name__=='__main__':unittest.main()
