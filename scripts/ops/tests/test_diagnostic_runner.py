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
    def test_inherited_preload_is_removed_before_compiler_and_baseline(self):
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.dict(runner.os.environ, {'LD_PRELOAD': '/untrusted/shim.so',
                                                 'GITHUB_TOKEN': 'secret'}):
            env = runner.diagnostic_environment(Path(directory))
        self.assertNotIn('LD_PRELOAD', env)
        self.assertNotIn('GITHUB_TOKEN', env)
        self.assertEqual(env['TMPDIR'], str(Path(directory) / 'tmp'))

    def test_diagnostic_budget_retains_qualified_sudo_measurement(self):
        storage = dict(roots=['/qualified/work', '/qualified/docker'],
                       readonly_sudo_du_root='/qualified/docker',
                       readonly_sudo_du_timeout_seconds=60,
                       runtime_storage_roots=['/qualified/docker'],
                       stop_bytes=runner.STOP_BYTES, reserve_bytes=1_000_000_000,
                       poll_seconds=10, planned_bytes={'build': 5_000_000_000})
        config = runner.diagnostic_budget_config(storage)
        self.assertEqual(config['roots'], storage['roots'])
        self.assertEqual(config['readonly_sudo_du_root'], '/qualified/docker')
        self.assertEqual(config['readonly_sudo_du_timeout_seconds'], 60)
        self.assertEqual(config['runtime_storage_roots'], storage['runtime_storage_roots'])
        self.assertEqual(config['planned_bytes']['prepare'], runner.PEAK_ADDITIONAL_BYTES)
        self.assertEqual(config['stop_bytes'], runner.STOP_BYTES)
        with self.assertRaisesRegex(RuntimeError, 'qualified storage method differs'):
            runner.diagnostic_budget_config(dict(storage, readonly_sudo_du_root=None))

    def test_qualified_budget_uses_sudo_only_for_its_admitted_root(self):
        repo = Path(runner.__file__).resolve().parents[3]
        qualified = runner.same_job_wrapper.load_qualification_module(repo)
        capacity = sys.modules[qualified.Budget.__module__]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            work, docker = root / 'work', root / 'docker'
            work.mkdir(); docker.mkdir()
            storage = dict(roots=[str(work), str(docker)],
                           readonly_sudo_du_root=str(docker),
                           readonly_sudo_du_timeout_seconds=60,
                           stop_bytes=runner.STOP_BYTES)
            observed = []
            budget = qualified.Budget(runner.diagnostic_budget_config(storage),
                                      measure=lambda roots: observed.append(roots) or 11)
            with mock.patch.object(capacity, 'readonly_sudo_du_bytes', return_value=7) as sudo:
                self.assertEqual(budget.used(), 18)
            self.assertEqual(observed, [[str(work)]])
            self.assertEqual(sudo.call_args.args, (docker,))
            self.assertEqual(sudo.call_args.kwargs, {'timeout': 60})

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
        storage = dict(roots=[str(root)], readonly_sudo_du_root=str(root),
                       readonly_sudo_du_timeout_seconds=60, stop_bytes=runner.STOP_BYTES)
        (private / 'admission.private.json').write_text(json.dumps({'storage': storage}))
        (private / 'profile.json').write_text(json.dumps({'run_id': 'hosted-804-123',
                                                          'storage': storage}))
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
                                     surface='sql', value_bytes=2048, private_root=work,
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

    def test_only_sql2048_diagnostic_shape_is_admitted(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            args.value_bytes = 16
            with self.assertRaisesRegex(RuntimeError, 'shape differs'):
                self.verify(args)

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
            qualified_budget = object()
            module = types.SimpleNamespace(verified_controller=lambda: None,
                                           admission_facts=lambda: facts,
                                           Budget=qualified_budget)
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
                self.assertEqual(runner.verify_same_job(args, storage),
                                 (storage, qualified_budget))
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
            (output / 'calibration.private.json').write_text(json.dumps(dict(
                schema='alopex.682-outside-timer-calibration/v1', acceptance_sample=False,
                observer_noop_ns=100, serialization_ns=20)))
            cell = {'subject_manifest_sha256': 'fixed', 'subject': 'before'}
            helper = types.SimpleNamespace(verify_record=lambda *args: None)
            runner.validate_cell({'commit_ns': 17}, cell, 'write', {'native_sha256': 'fixed'},
                                 helper, output)
            observation['schedstat']['original_timer_ns'] = 18
            (output / 'commit-observation.json').write_text(json.dumps(observation))
            with self.assertRaisesRegex(RuntimeError, 'diagnostic observation differs'):
                runner.validate_cell({'commit_ns': 17}, cell, 'write',
                                     {'native_sha256': 'fixed'}, helper, output)

    def test_sync_observation_requires_partial_coverage_label(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / 'worker.log').write_text('{}')
            (output / 'identity.json').write_text('{"manifest_sha256":"fixed"}')
            observation = dict(acceptance_sample=False, timer_nanoseconds=17,
                               schedstat=dict(schema='alopex.682-schedstat-diagnostic/v1',
                                              acceptance_sample=False, original_timer_ns=17),
                               intercepted_sync=dict(schema='alopex.682-intercepted-sync/v1',
                                                     coverage='unproven', acceptance_sample=False,
                                                     calls=[dict(operation=operation, result=0,
                                                                 duration_ns=10)
                                                            for operation in (1, 1, 2)]))
            (output / 'commit-observation.json').write_text(json.dumps(observation))
            cell = {'subject_manifest_sha256': 'fixed', 'subject': 'candidate'}
            helper = types.SimpleNamespace(verify_record=lambda *args: None)
            runner.validate_cell({'commit_ns': 17}, cell, 'write',
                                 {'native_sha256': 'fixed'}, helper, output, 'sync')
            observation['intercepted_sync']['coverage'] = 'complete'
            (output / 'commit-observation.json').write_text(json.dumps(observation))
            with self.assertRaisesRegex(RuntimeError, 'intercepted sync observation differs'):
                runner.validate_cell({'commit_ns': 17}, cell, 'write',
                                     {'native_sha256': 'fixed'}, helper, output, 'sync')

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

    def test_preflight_failure_writes_private_phase_without_public_detail(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            argv = ['diagnostic_runner.py']
            for name, value in vars(args).items():
                if name == 'qualification_checkout':
                    continue
                argv.extend(('--' + name.replace('_', '-'), str(value)))
            argv.extend(('--qualification-checkout', str(args.qualification_checkout)))
            with mock.patch.object(sys, 'argv', argv), \
                 mock.patch.object(runner, 'verify_inputs',
                                   side_effect=RuntimeError('private preflight detail')):
                from io import StringIO
                with mock.patch('sys.stdout', new_callable=StringIO) as output:
                    self.assertEqual(runner.main(), 1)
            public = json.loads(output.getvalue())
            self.assertEqual(public['error_code'], 'DIAGNOSTIC_FAILURE')
            self.assertNotIn('private preflight detail', output.getvalue())
            receipt = json.loads((args.private_root / runner.FAILURE_RECEIPT).read_text())
            self.assertEqual(receipt['phase'], 'preflight')
            self.assertIn('private preflight detail', receipt['original_error']['error'])

    def test_measurement_failure_keeps_bounded_cause_and_source_site_private(self):
        try:
            try:
                raise FileNotFoundError(2, 'missing during scan', '/private/task-item')
            except FileNotFoundError as cause:
                raise RuntimeError('MEASUREMENT') from cause
        except RuntimeError as error:
            detail = runner.failure_detail(error)
        self.assertEqual(detail['error'], "RuntimeError('MEASUREMENT')")
        self.assertEqual(detail['cause']['type'], 'FileNotFoundError')
        self.assertEqual(detail['cause']['errno'], 2)
        self.assertIn('missing during scan', detail['cause']['error'])
        self.assertEqual(detail['frames'][-1]['file'], 'test_diagnostic_runner.py')
        self.assertTrue(all(len(frame['file']) <= 128 and
                            len(frame['function']) <= 128 for frame in detail['frames']))

    def test_evidence_failure_retains_original_and_cleanup_error_outside_work(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            state = dict(phase='preflight', failure_phase=None,
                         original_error=None, cleanup_error=None)
            class Budget:
                def __init__(self, *_):
                    pass
                def used(self):
                    return 0
                def before(self, *_):
                    pass
                def check(self):
                    return 0
            def atomic_json(path, value):
                path.write_text(json.dumps(value))
            helper = types.SimpleNamespace(Budget=lambda _: self.fail('frozen budget used'),
                                           atomic_json=atomic_json)
            qualified_storage = dict(roots=[directory], readonly_sudo_du_root=directory,
                                     readonly_sudo_du_timeout_seconds=60,
                                     stop_bytes=runner.STOP_BYTES)
            with mock.patch.object(runner, 'verify_inputs',
                                   return_value=({'products': {}}, {'roots': [directory]})), \
                 mock.patch.object(runner, 'verify_same_job',
                                   return_value=(qualified_storage, Budget)), \
                 mock.patch.object(runner, 'load_frozen', return_value=helper), \
                 mock.patch.object(runner.os, 'umask'), \
                 mock.patch.object(runner.os, 'sched_getaffinity', return_value={0, 1},
                                   create=True), \
                 mock.patch.object(runner, 'verify_schedstat_capability',
                                   side_effect=RuntimeError('private capability detail')), \
                 mock.patch.object(runner, 'seal_evidence',
                                   side_effect=RuntimeError('private seal detail')):
                with self.assertRaisesRegex(RuntimeError, 'DIAGNOSTIC_EVIDENCE_FAILURE'):
                    runner.run(args, state)
            runner.write_failure_receipt(args, state)
            receipt = json.loads((args.private_root / runner.FAILURE_RECEIPT).read_text())
            self.assertEqual(receipt['phase'], 'schedstat_capability')
            self.assertIn('private capability detail', receipt['original_error']['error'])
            self.assertIn('private seal detail', receipt['cleanup_error']['error'])
            self.assertFalse((args.private_root / 'diagnostic').exists())


if __name__ == '__main__':
    unittest.main()
