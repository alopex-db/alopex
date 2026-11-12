import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1] / 'wal_cost_next'
spec = importlib.util.spec_from_file_location('same_job_wrapper', HERE / 'same_job_wrapper.py')
wrapper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wrapper)


class AdmissionContract(unittest.TestCase):
    def facts(self):
        return {'context': 'local', 'dmi': 'machine', 'kernel': 'kernel', 'lsm': 'lsm',
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
                         '24c31ed86f5643edfef6789b345e12d610e71d05c1c3a905d84e17c3771c055b')
        self.assertEqual(config['stage_seconds'], 5400)
        self.assertEqual(config['cell_seconds'], 30)
        self.assertEqual(set(config['products']), {'before', 'candidate'})


if __name__ == '__main__':
    unittest.main()
