import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1] / 'wal_cost_next'
spec = importlib.util.spec_from_file_location('same_job_wrapper', HERE / 'same_job_wrapper.py')
wrapper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wrapper)


class AdmissionContract(unittest.TestCase):
    def test_early_identity_and_capacity_failures_create_private_receipts(self):
        for failure in ('identity', 'capacity'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                private = root / 'alopex-804-qualification'
                private.mkdir(mode=0o700)
                workspace = root / 'workspace'
                workspace.mkdir()
                controller, frozen = root / 'qualification', root / 'frozen'
                controller.mkdir()
                frozen.mkdir()
                config = root / 'config.json'
                config.write_text(json.dumps({'sources': {
                    'scripts/ops/wal_cost_contract.py': '0' * 64}}))
                sha = 'a' * 40
                (private / 'checkout-preflight.private.json').write_text(json.dumps({
                    'expected_controller_sha': sha, 'workspace_root': str(root)}))
                arguments = ['wrapper', '--qualification-checkout', str(controller),
                             '--frozen-checkout', str(frozen), '--config', str(config),
                             '--config-sha256', wrapper.sha(config)]
                module = types.SimpleNamespace(
                    verified_controller=lambda: None,
                    admission_facts=lambda: {'storage': {}},
                    Budget=lambda _: types.SimpleNamespace(used=lambda: 39_000_000_000),
                    PLANNED={'fetch': 1_000_000_000})
                reason = ('frozen comparator checkout differs' if failure == 'identity'
                          else 'combined peak exceeds host budget')
                stdout = io.StringIO()
                with patch.object(sys, 'argv', arguments), \
                        patch.dict(os.environ, {'RUNNER_TEMP': directory,
                                                'GITHUB_WORKSPACE': str(workspace),
                                                'GITHUB_RUN_ID': '123',
                                                'GITHUB_EVENT_NAME': 'workflow_dispatch',
                                                'RUNNER_ENVIRONMENT': 'github-hosted',
                                                'RUNNER_KIND': 'github-hosted',
                                                'GITHUB_SHA': sha,
                                                'EXPECTED_CONTROLLER_SHA': sha}), \
                        patch.object(wrapper, 'verify_frozen_checkout',
                                     side_effect=RuntimeError(reason) if failure == 'identity'
                                     else None), \
                        patch.object(wrapper, 'verify_product_artifacts'), \
                        patch.object(wrapper, 'head', return_value=sha), \
                        patch.object(wrapper, 'load_qualification_module', return_value=module), \
                        redirect_stdout(stdout):
                    self.assertEqual(wrapper.main(), 1)
                receipt = json.loads((private / 'wrapper-result.private.json').read_text())
                self.assertEqual(receipt['original_error'], repr(RuntimeError(reason)))
                self.assertTrue(receipt['pre_comparator_error'])
                self.assertEqual(json.loads(stdout.getvalue()), {
                    'case': 'WAL_COST_682', 'outcome': 'failed',
                    'error_code': 'WRAPPER_FAILURE'})
                self.assertNotIn(reason, stdout.getvalue())

    def test_comparator_receipt_is_not_replaced_by_wrapper_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / 'alopex-804-qualification'
            private.mkdir(mode=0o700)
            receipt = private / 'wrapper-result.private.json'
            original = '{"original_error":"comparator failed","cleanup_error":"cleanup failed"}\n'
            receipt.write_text(original)
            with patch.dict(os.environ, {'RUNNER_TEMP': directory}):
                wrapper.seal_wrapper_failure(RuntimeError('later wrapper error'))
            self.assertEqual(receipt.read_text(), original)

    def facts(self):
        return {'context': 'local',
                'boot_id': {'status': 'read', 'value': '12345678-1234-4234-8234-123456789abc\n'},
                'kernel': 'kernel', 'lsm': 'lsm',
                'apparmor': 'enabled', 'root_metadata': [{'dev': 1, 'ino': 2}],
                'temp_snapshot': [], 'cpus': [0, 1], 'storage': {'roots': ['/root']},
                'docker_info': {'DockerRootDir': '/docker', 'ServerVersion': '28.0.4',
                                'Architecture': 'x86_64', 'CgroupVersion': '2',
                                'SecurityOptions': ['name=apparmor']}, 'baseline_bytes': 1}

    def test_same_host_allows_growth_but_rejects_identity_drift(self):
        prior, current = self.facts(), self.facts()
        current['baseline_bytes'] = 4_000_000_000
        wrapper.same_host(prior, current)
        current['docker_info']['DockerRootDir'] = '/other'
        with self.assertRaisesRegex(RuntimeError, 'runtime identity changed'):
            wrapper.same_host(prior, current)
        current = self.facts()
        current['boot_id'] = {'status': 'read', 'value': '22345678-1234-4234-8234-123456789abc\n'}
        with self.assertRaisesRegex(RuntimeError, 'host identity changed: boot_id'):
            wrapper.same_host(prior, current)

    def test_qualification_requires_complete_same_job_proof(self):
        public = {'schema': 'alopex.hosted-test-env-qualification/v1',
                  'case': 'HOST_QUALIFICATION', 'outcome': 'passed', 'error_code': None,
                  'source_commit': 'a4158b873963dd79c7844953d64a8b511ee75fde',
                  'asset_digest': 'asset',
                  'cases': [{'id': key, 'status': 'passed', 'error_code': None}
                            for key in sorted(wrapper.REQUIRED_CASES)]}
        with tempfile.TemporaryDirectory() as folder:
            private = Path(folder)
            (private / 'admission.private.json').write_text(json.dumps(self.facts()))
            (private / 'profile.json').write_text(json.dumps({
                'run_id': 'hosted-804-123', 'storage': self.facts()['storage'],
                'assets': '/assets/assets/assets.json'}))
            (private / 'storage-after.private.json').write_text('{"allocated_bytes": 100}')
            module = types.SimpleNamespace(ASSETS_DIGEST='asset')
            with patch.dict('os.environ', {'ALOPEX_804_ASSETS': '/assets'}):
                wrapper.verify_qualification(public, private, module, '123', self.facts())
                public['cases'].pop()
                with self.assertRaisesRegex(RuntimeError, 'cases incomplete'):
                    wrapper.verify_qualification(public, private, module, '123', self.facts())

    def test_frozen_config_is_pinned(self):
        config_path = HERE / 'exact-a415-wal-cost-input.private.json'
        config = json.loads(config_path.read_text())
        self.assertEqual(wrapper.sha(config_path),
                         '2d9222eaec0ea9bbc22874f8dcea17f8b09525fa6c8ba54b7d990ac9436f5c55')
        self.assertEqual(wrapper.FROZEN_COMMIT,
                         'fd34ca342617c247a9c711bcf597da8bbef3118f')
        self.assertEqual(config['sources']['.github/ops/native-evidence-cert.pem'],
                         'e8a39adb8f76185a87f805f6d083bdbe2bd7ea5d3bc05607f1a7a2e0deb8b775')
        self.assertEqual(config['stage_seconds'], 5400)
        self.assertEqual(config['cell_seconds'], 30)
        self.assertEqual(set(config['products']), {'before', 'candidate'})

    def test_frozen_successor_rejects_old_head_and_tampered_recipient(self):
        frozen = Path('/frozen')
        config = {'sources': {
            'scripts/ops/hosted_wal_cost.py': 'a' * 64,
            '.github/ops/native-evidence-cert.pem': 'b' * 64}}
        with patch.object(wrapper, 'head', return_value=
                          '490f002dfb2c7f509a2444891808d9704fe6b001'), \
             patch.object(wrapper, 'sha') as digest:
            with self.assertRaisesRegex(RuntimeError, 'frozen comparator checkout differs'):
                wrapper.verify_frozen_checkout(frozen, config, 'c' * 64)
            digest.assert_not_called()
        def observed(path):
            return 'a' * 64 if path.name == 'hosted_wal_cost.py' else '0' * 64
        with patch.object(wrapper, 'head', return_value=wrapper.FROZEN_COMMIT), \
             patch.object(wrapper, 'sha', side_effect=observed):
            with self.assertRaisesRegex(RuntimeError, 'frozen source differs'):
                wrapper.verify_frozen_checkout(frozen, config, 'c' * 64)

    def test_artifact_preflight_rejects_missing_or_changed_baseline(self):
        products = {name: {'run': number, 'controller': name + '-head',
                           'artifact_name': name + '-assets', 'artifact_id': number + 1,
                           'artifact_digest': name + '-digest'}
                    for name, number in (('before', 10), ('candidate', 20))}

        def response(command, **_):
            endpoint = command[-1]
            name = 'before' if '/10' in endpoint else 'candidate'
            producer = products[name]
            if endpoint.endswith('/artifacts'):
                payload = {'artifacts': [{'name': producer['artifact_name'],
                                          'id': producer['artifact_id'],
                                          'expired': state['expired'] == name,
                                          'digest': producer['artifact_digest'],
                                          'size_in_bytes': 100}]}
                if state['missing'] == name:
                    payload['artifacts'] = []
                elif state['changed'] == name:
                    payload['artifacts'][0]['digest'] = 'changed'
            else:
                payload = {'head_sha': producer['controller'], 'status': 'completed',
                           'conclusion': 'success', 'event': 'workflow_dispatch',
                           'head_repository': {'full_name': 'alopex-db/alopex'}}
            return types.SimpleNamespace(stdout=json.dumps(payload))

        state = {'missing': None, 'changed': None, 'expired': None}
        with patch.object(wrapper.subprocess, 'run', side_effect=response) as call:
            wrapper.verify_product_artifacts({'products': products})
            self.assertEqual(call.call_count, 4)
            state['missing'] = 'before'
            with self.assertRaisesRegex(RuntimeError, 'artifact differs: before'):
                wrapper.verify_product_artifacts({'products': products})
            state['missing'], state['changed'] = None, 'candidate'
            with self.assertRaisesRegex(RuntimeError, 'artifact differs: candidate'):
                wrapper.verify_product_artifacts({'products': products})
            state['changed'], state['expired'] = None, 'before'
            with self.assertRaisesRegex(RuntimeError, 'artifact differs: before'):
                wrapper.verify_product_artifacts({'products': products})


if __name__ == '__main__':
    unittest.main()
