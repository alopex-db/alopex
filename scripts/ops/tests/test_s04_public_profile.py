"""Synthetic, offline contract checks for UC-684-S04-PROFILE-SEMANTICS."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / 's04_public_profile.py'
spec = importlib.util.spec_from_file_location('s04_public_profile', MODULE)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
SCHEMA = json.loads(p.SCHEMA_PATH.read_text())
SHA = 'a' * 40
DIGEST = 'b' * 64


def sample(schema):
    if '$ref' in schema:
        return sample(SCHEMA['$defs'][schema['$ref'].split('/')[-1]])
    if 'const' in schema:
        return copy.deepcopy(schema['const'])
    if 'enum' in schema:
        return schema['enum'][0]
    if 'oneOf' in schema:
        return sample(schema['oneOf'][0])
    kind = schema.get('type')
    if kind == 'object':
        return {k: sample(v) for k, v in schema['properties'].items()}
    if kind == 'array':
        return [sample(v) for v in schema['prefixItems']] if 'prefixItems' in schema else [sample(schema['items']) for _ in range(schema.get('minItems', 0))]
    if kind == 'integer':
        return max(1, schema.get('minimum', 0))
    if kind == 'number':
        return 1.0
    if kind == 'boolean':
        return True
    if kind == 'null':
        return None
    if kind == 'string':
        if schema.get('format') == 'date-time':
            return '2026-11-24T00:00:00Z'
        pattern = schema.get('pattern', '')
        if '40' in pattern:
            return SHA
        if '64' in pattern:
            return ('sha256:' if 'sha256:' in pattern else '') + DIGEST
        if pattern.startswith('^/'):
            return '/synthetic/path'
        return 'synthetic'
    raise AssertionError(schema)


def valid_profile():
    x = sample(SCHEMA)
    b, s, r = x['binding'], x['storage'], x['runtime']
    b.update(source_sha=SHA, candidate_sha=SHA, workflow_sha=SHA,
             run_id=7, run_attempt=2, physical_host_id='synthetic-host',
             profile_owner_uid=os.getuid())
    s.update(physical_host_id='synthetic-host', measured_baseline_bytes=10**9,
             planned_peak_extra_bytes=10**9, backing_free_bytes=10**10)
    s['roots'] = []
    for i, kind in enumerate(sorted(p.REQUIRED_ROOT_KINDS)):
        root = sample(SCHEMA['$defs']['storage_root'])
        root.update(kind=kind, canonical_path=f'/synthetic/root{i}', maximum_scan_age_seconds=1)
        s['roots'].append(root)
    r.update(process_owner_uid=os.getuid(), socket_path='/synthetic/socket',
             daemon_root='/synthetic/daemon', job_limit_seconds=1000,
             stop_before_seconds=900)
    x['assets']['image_architecture'] = b['selected_architecture']
    assets = x['assets']['products']
    assets[0].update(role='baseline', sha256='1' * 64, architecture=b['selected_architecture'])
    assets[1].update(role='candidate', sha256='2' * 64, architecture=b['selected_architecture'])
    x['workload']['pairs'] = []
    for i in range(27):
        pair = sample(SCHEMA['$defs']['pair'])
        pair.update(pair_id=f'pair-{i:02}', order=i+1)
        pair['baseline'].update(cell_id=f'base-{i:02}', arm='baseline',
                                asset_sha256=assets[0]['sha256'])
        pair['candidate'].update(cell_id=f'cand-{i:02}', arm='candidate',
                                 asset_sha256=assets[1]['sha256'])
        x['workload']['pairs'].append(pair)
    for i, check in enumerate(x['security']['checks'].values()):
        check.update(positive_check_id=f'positive-{i}', negative_check_id=f'negative-{i}',
                     config_location=f'/synthetic/config{i}', evidence_location=f'/synthetic/evidence{i}')
    x['network']['edges'] = []
    stages = x['time_budget']['stages']
    for stage in stages.values():
        stage['rate_evidence'].update(source_sha=SHA, run_id=7, run_attempt=2)
        stage.update(expected_seconds=10, conservative_seconds=20, peak_extra_bytes=100)
    stages['measurement_54']['work_count'] = 54
    x['time_budget'].update(conservative_total_seconds=20*len(stages),
                            job_limit_seconds=1000, abort_if_projected_over_seconds=800)
    return x


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'profile.json'
        self.x = valid_profile()
        self.expected = dict(source_sha=SHA, run_id=7, run_attempt=2,
            manifest_sha256=DIGEST, physical_host_id='synthetic-host',
            approved_network_edges=set(),
            manifest_pairs=tuple((pair['order'], pair['pair_id'], pair['baseline']['case_id'],
                pair['baseline']['input_sha256'], pair['baseline']['parameters_sha256'])
                for pair in self.x['workload']['pairs']))

    def validate(self, raw=None):
        self.path.write_bytes(raw if raw is not None else json.dumps(self.x).encode())
        self.path.chmod(0o600)
        return p.validate_profile(self.path, **self.expected)

    def test_valid_synthetic_profile(self):
        self.assertEqual(self.validate()['binding']['run_id'], 7)

    def test_rejects_duplicate_and_unknown_keys(self):
        for raw in (json.dumps(self.x)[:-1] + ',"schema":"other"}', None):
            if raw is None:
                self.x['unexpected'] = 1
            with self.assertRaises(p.ProfileFailure):
                self.validate(raw.encode() if raw else None)

    def test_rejects_identity_and_root_errors(self):
        for change in (
            lambda x: x['binding'].update(run_attempt=3),
            lambda x: x['storage']['roots'][1].update(canonical_path='/synthetic/root0/child'),
            lambda x: x['storage']['roots'].pop(),
            lambda x: x['storage'].update(measured_baseline_bytes=39999999999),
            lambda x: x['assets']['products'][1].update(sha256='1'*64),
        ):
            self.x = valid_profile(); change(self.x)
            with self.assertRaises(p.ProfileFailure): self.validate()

    def test_rejects_manifest_network_checks_and_budget(self):
        for change in (
            lambda x: x['workload']['pairs'][1].update(order=1),
            lambda x: x['workload']['pairs'][1]['candidate'].update(cell_id='base-00'),
            lambda x: x['network']['edges'].append(dict(source='a', destination='b', protocol='tcp', port=1, authentication='mutual_tls', stage='checkout')),
            lambda x: x['security']['checks']['SOURCE'].update(negative_check_id='positive-0'),
            lambda x: x['time_budget'].update(margin_seconds=900),
            lambda x: x['time_budget']['stages']['pilot']['rate_evidence'].update(run_id=8),
        ):
            self.x = valid_profile(); change(self.x)
            with self.assertRaises(p.ProfileFailure): self.validate()

    def test_rejects_nonfinite_numeric_json(self):
        raw = json.dumps(self.x).replace('"measured_rate_per_second": 1.0',
                                         '"measured_rate_per_second": 1e400', 1)
        with self.assertRaises(p.ProfileFailure): self.validate(raw.encode())

    def test_rejects_sha_with_trailing_newline(self):
        self.x['binding']['workflow_sha'] = SHA + '\n'
        with self.assertRaises(p.ProfileFailure): self.validate()

    def test_requires_positive_pilot_work_and_input(self):
        for field in ('work_count', 'input_bytes'):
            self.x = valid_profile()
            self.x['time_budget']['stages']['pilot'][field] = 0
            with self.assertRaises(p.ProfileFailure): self.validate()

    def test_preserves_time_margin_at_stop_threshold(self):
        for stop, abort in ((950, 800), (950, 940)):
            self.x = valid_profile()
            self.x['time_budget'].update(margin_seconds=100,
                                         abort_if_projected_over_seconds=abort)
            self.x['runtime']['stop_before_seconds'] = stop
            with self.assertRaises(p.ProfileFailure): self.validate()

    def test_rejects_changed_cell_despite_matching_pair_ids_and_digest(self):
        self.x['workload']['pairs'][0]['baseline']['input_sha256'] = 'c' * 64
        self.x['workload']['pairs'][0]['candidate']['input_sha256'] = 'c' * 64
        with self.assertRaises(p.ProfileFailure): self.validate()

    def test_rejects_oversized_owner_private_profile(self):
        self.path.write_bytes(b' ' * (p.MAX_PROFILE_BYTES + 1))
        self.path.chmod(0o600)
        with self.assertRaises(p.ProfileFailure) as caught:
            p.validate_profile(self.path, **self.expected)
        self.assertEqual(caught.exception.code, 'S04_PROFILE_INVALID')

    def test_rejects_noncanonical_empty_path_segment(self):
        self.x['storage']['roots'][0]['canonical_path'] = '/synthetic//root0'
        with self.assertRaises(p.ProfileFailure): self.validate()

    def test_failure_cannot_call_construction(self):
        constructed = []
        self.x['binding']['run_id'] = 8
        def admission_then_construct():
            self.validate()
            constructed.append(True)
        with self.assertRaises(p.ProfileFailure): admission_then_construct()
        self.assertEqual(constructed, [])

    def test_rejects_unsupported_schema_keyword(self):
        bad = copy.deepcopy(SCHEMA)
        bad['properties']['binding']['not'] = {'type': 'null'}
        with self.assertRaises(p.ProfileFailure) as caught:
            p._audit_schema(bad, bad)
        self.assertEqual(caught.exception.code, 'S04_SCHEMA_UNSUPPORTED')


if __name__ == '__main__':
    unittest.main()
