"""Source-only tests for the separate #682 diagnostic controller."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock
import zipfile

from scripts.ops.wal_cost_next import diagnostic_runner as runner


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class DiagnosticRunnerTests(unittest.TestCase):
    def test_versioned_source_lock_matches_diagnostic_modules(self):
        directory = Path(runner.__file__).parent
        locked = json.loads((directory / 'diagnostic_sources.lock.json').read_bytes())
        self.assertEqual(set(locked), set(runner.DIAGNOSTIC_FILES))
        for name, expected in locked.items():
            with self.subTest(name=name):
                self.assertEqual(digest(directory / name), expected)

    def fixture(self, root):
        frozen = root / 'frozen'
        frozen.mkdir()
        (frozen / 'source.py').write_bytes(b'frozen')
        exact = root / 'exact.json'
        exact.write_text(json.dumps(dict(cell_seconds=30, rss_bytes=1_073_741_824,
                                         cpu_selection='first_allowed',
                                         sources={'source.py': digest(frozen / 'source.py')})))
        private = root / 'qualification'
        private.mkdir()
        (private / 'admission.private.json').write_text(json.dumps({'storage': {'roots': [str(root)]}}))
        (private / 'profile.json').write_text(json.dumps({'run_id': 'hosted-804-123',
                                                          'storage': {'roots': [str(root)]}}))
        public = root / 'qualification.json'
        public.write_text(json.dumps(dict(schema='alopex.hosted-test-env-qualification/v1',
                                          case='HOST_QUALIFICATION', outcome='passed',
                                          error_code=None)))
        sources = root / 'sources.json'
        sources.write_text(json.dumps({name: digest(Path(runner.__file__).parent / name)
                                       for name in runner.DIAGNOSTIC_FILES}))
        work = root / 'work'
        export = root / 'export'
        work.mkdir(mode=0o700)
        export.mkdir(mode=0o700)
        return types.SimpleNamespace(frozen_checkout=frozen, exact_config=exact,
                                     diagnostic_sources=sources,
                                     diagnostic_sources_sha256=digest(sources),
                                     surface='kv', value_bytes=16, private_root=work,
                                     export_root=export, qualification_private=private,
                                     qualification_public=public,
                                     qualification_checkout=Path(runner.__file__).resolve().parents[3])

    def verify(self, args):
        with mock.patch.object(sys, 'platform', 'linux'), \
             mock.patch.object(runner.os, 'getuid', return_value=1000), \
             mock.patch.dict(runner.os.environ, {'RUNNER_KIND': 'github-hosted',
                                                 'GITHUB_EVENT_NAME': 'workflow_dispatch',
                                                 'GITHUB_RUN_ID': '123'}), \
             mock.patch.object(runner.subprocess, 'check_output', return_value=runner.FROZEN + '\n'), \
             mock.patch.object(runner, 'EXACT_CONFIG_SHA256', digest(args.exact_config)):
            return runner.verify_inputs(args)

    def test_exact_source_and_limits_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            config, storage = self.verify(args)
            self.assertEqual(config['cell_seconds'], 30)
            self.assertEqual(storage['roots'], [directory])

    def test_changed_frozen_source_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            (args.frozen_checkout / 'source.py').write_bytes(b'changed')
            with self.assertRaisesRegex(RuntimeError, 'frozen source differs'):
                self.verify(args)

    def test_relaxed_cell_limit_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            config = json.loads(args.exact_config.read_text())
            config['cell_seconds'] = 31
            args.exact_config.write_text(json.dumps(config))
            with self.assertRaisesRegex(RuntimeError, 'cell limits differ'):
                self.verify(args)

    def test_changed_diagnostic_source_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            sources = json.loads(args.diagnostic_sources.read_text())
            sources['diagnostic_worker.py'] = '0' * 64
            args.diagnostic_sources.write_text(json.dumps(sources))
            args.diagnostic_sources_sha256 = digest(args.diagnostic_sources)
            with self.assertRaisesRegex(RuntimeError, 'diagnostic source differs'):
                self.verify(args)

    def test_full_same_job_qualification_is_required(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            facts = {'same': 'host'}
            module = types.SimpleNamespace(verified_controller=lambda: None,
                                           admission_facts=lambda: facts)
            storage = {'roots': [directory]}
            expected = '1' * 40
            with mock.patch.dict(runner.os.environ, {'GITHUB_SHA': expected,
                                                      'EXPECTED_CONTROLLER_SHA': expected,
                                                      'GITHUB_RUN_ID': '123'}), \
                 mock.patch.object(runner.same_job_wrapper, 'head', return_value=expected), \
                 mock.patch.object(runner.same_job_wrapper, 'load_qualification_module',
                                   return_value=module), \
                 mock.patch.object(runner.same_job_wrapper, 'verify_qualification',
                                   return_value={'storage': storage}) as qualify:
                self.assertEqual(runner.verify_same_job(args, storage), storage)
                self.assertEqual(qualify.call_args.args[3:], ('123', facts))
                qualify.side_effect = RuntimeError('qualification failed')
                with self.assertRaisesRegex(RuntimeError, 'qualification failed'):
                    runner.verify_same_job(args, storage)

    def test_same_job_schedstat_wait_must_advance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def atomic_json(path, value):
                path.write_text(json.dumps(value))
            helper = types.SimpleNamespace(atomic_json=atomic_json)
            advancing = types.SimpleNamespace(collect=lambda: {'samples': {}, 'affinity': 'pinned'},
                                              capability_code=lambda *_: 'SCHEDSTAT_ADVANCING')
            runner.verify_schedstat_capability(root, helper, advancing)
            saved = json.loads((root / 'schedstat-capability.private.json').read_text())
            self.assertEqual(saved['code'], 'SCHEDSTAT_ADVANCING')
            advancing.capability_code = lambda *_: 'SCHEDSTAT_WAIT_UNPROVEN'
            with self.assertRaisesRegex(RuntimeError, 'same-job schedstat wait unproven'):
                runner.verify_schedstat_capability(root, helper, advancing)

    def test_product_checks_native_extension_in_wheel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = root / 'product.whl'
            with zipfile.ZipFile(wheel, 'w') as archive:
                archive.writestr('alopex/_alopex.abi3.so', b'native extension')
            library = root / 'libalopex_sql_parser.so'
            library.write_bytes(b'different parser library')
            manifest = root / 'assets.json'
            manifest.write_text(json.dumps({'files': [
                {'role': 'wheel', 'size': wheel.stat().st_size, 'sha256': digest(wheel)},
                {'role': 'library', 'size': library.stat().st_size, 'sha256': digest(library)}]}))
            product = dict(manifest=manifest, files={'wheel': wheel, 'library': library})
            producer = dict(manifest_sha256=digest(manifest), wheel_sha256=digest(wheel),
                            native_sha256=hashlib.sha256(b'native extension').hexdigest())
            helper = types.SimpleNamespace(sha=digest)
            runner.verify_product(product, producer, helper)
            producer['native_sha256'] = hashlib.sha256(b'wrong extension').hexdigest()
            with self.assertRaisesRegex(RuntimeError, 'product asset changed'):
                runner.verify_product(product, producer, helper)
            producer['native_sha256'] = hashlib.sha256(b'native extension').hexdigest()
            library.write_bytes(b'changed parser library')
            with self.assertRaisesRegex(RuntimeError, 'product asset changed'):
                runner.verify_product(product, producer, helper)

    def test_stale_evidence_fails_before_installation(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            (args.private_root / 'old').write_text('private')
            with self.assertRaisesRegex(RuntimeError, 'stale diagnostic evidence'):
                self.verify(args)

    def test_writer_observation_must_match_original_timer(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / 'worker.log').write_text('{}')
            (output / 'identity.json').write_text('{"manifest_sha256":"fixed"}')
            observation = dict(acceptance_sample=False, timer_nanoseconds=17,
                               schedstat=dict(schema='alopex.682-schedstat-diagnostic/v1',
                                              acceptance_sample=False, original_timer_ns=17))
            (output / 'commit-observation.json').write_text(json.dumps(observation))
            cell = {'subject_manifest_sha256': 'fixed'}
            helper = types.SimpleNamespace(verify_record=lambda *args: None)
            runner.validate_cell({'commit_ns': 17}, cell, 'write', {'native_sha256': 'fixed'},
                                 helper, output)
            observation['schedstat']['original_timer_ns'] = 18
            (output / 'commit-observation.json').write_text(json.dumps(observation))
            with self.assertRaisesRegex(RuntimeError, 'diagnostic observation differs'):
                runner.validate_cell({'commit_ns': 17}, cell, 'write',
                                     {'native_sha256': 'fixed'}, helper, output)

    def test_evidence_bound_prevents_encryption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / 'work'; work.mkdir()
            export = root / 'export'; export.mkdir()
            (work / 'large').write_bytes(b'x' * 10)
            with mock.patch.object(runner, 'EVIDENCE_LIMIT', 10), \
                 mock.patch.object(runner.subprocess, 'run') as encrypt:
                with self.assertRaisesRegex(RuntimeError, 'evidence bound exceeded'):
                    runner.seal_evidence(work, export, root / 'cert', types.SimpleNamespace(sha=digest))
                encrypt.assert_not_called()

    def test_encryption_produces_bounded_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / 'work'; work.mkdir()
            export = root / 'export'; export.mkdir()
            (work / 'raw.json').write_text('{}')
            def encrypt(command, **kwargs):
                Path(command[command.index('-out') + 1]).write_bytes(b'encrypted')
            with mock.patch.object(runner.subprocess, 'run', side_effect=encrypt):
                raw, sealed = runner.seal_evidence(work, export, root / 'cert',
                                                   types.SimpleNamespace(sha=digest))
            self.assertEqual(raw, digest(work / 'evidence.tar.gz'))
            self.assertEqual(sealed, digest(export / 'private-evidence.cms'))

    def test_cleanup_removes_fixtures_but_retains_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            for name in ('imports', 'venvs', 'fixtures', 'tmp'):
                path = work / name
                path.mkdir()
                (path / 'owned').write_text('temporary')
            cells = work / 'cells'
            cells.mkdir()
            (cells / 'observation.json').write_text('{}')
            runner.cleanup_owned(work)
            self.assertTrue((cells / 'observation.json').is_file())
            self.assertFalse((work / 'fixtures').exists())
            self.assertFalse((work / 'venvs').exists())

    def test_cleanup_rejects_linked_fixture_root(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory) / 'work'
            work.mkdir()
            external = Path(directory) / 'external'
            external.mkdir()
            (external / 'keep').write_text('owned elsewhere')
            (work / 'fixtures').symlink_to(external, target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, 'cleanup path differs'):
                runner.cleanup_owned(work)
            self.assertTrue((external / 'keep').exists())

    def test_failed_seal_discards_raw_and_partial_ciphertext(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / 'work'; work.mkdir()
            (work / 'raw.json').write_text('{"private":true}')
            export = root / 'export'; export.mkdir()
            (export / 'private-evidence.cms').write_bytes(b'partial')
            runner.discard_unsealed(work, export)
            self.assertFalse(work.exists())
            self.assertFalse((export / 'private-evidence.cms').exists())


if __name__ == '__main__':
    unittest.main()
