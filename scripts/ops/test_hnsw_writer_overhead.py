import copy
import json
import unittest

import hnsw_writer_overhead_contract as contract
import hosted_hnsw_writer_overhead as control
import test_hnsw_completion_stage as completion


def config(finish='commit', rows=1000):
    value = completion.configuration(rows=rows, finish=finish)
    value['schema'] = 'alopex.hnsw-writer-overhead-input/v1'
    value['subjects'] = {'normal': value['subjects']['before'],
                         'diagnostic': value['subjects']['candidate']}
    for kind, subject in value['subjects'].items():
        subject.update(diagnostic=kind == 'diagnostic', source_commit=kind,
                       example_source_sha256=kind, cargo_lock_sha256='locked')
    value['timing_pair'] = dict(engine='5e05f0149e93e92455e59fe8c3a4cdfbec19e578',
                               timed_window_sha256='ee7954f21000fd9f121bd76812097fcf8135fb8b2c70398e805c9fce2b130025',
                               normal=dict(commit='normal', example_sha256='normal'),
                               diagnostic=dict(commit='diagnostic', example_sha256='diagnostic'))
    if rows != 1000:
        previous_result(value)
    return value


def previous_result(value):
    for role, subject in value['subjects'].items():
        subject.setdefault('assets_digest', role)
    raw = json.dumps(dict(schema='alopex.hnsw-writer-overhead/v1', status='passed',
                          finish=value['finish'], stage_rows=contract.STAGES[contract.STAGES.index(value['rows']) - 1],
                          completed_samples=10, owned_resources_cleaned=True,
                          controller='a' * 40, performance_qualification=False,
                          assets={role: s['assets_digest'] for role, s in value['subjects'].items()})).encode()
    value['previous_stage'] = dict(run=1, controller='a' * 40, artifact_id=2,
                                  artifact_digest='sha256:' + 'b' * 64, result_sha256=contract.sha(raw))
    return raw


class OverheadTests(unittest.TestCase):
    def fixture(self, finish='commit', subject='normal'):
        args = completion.CompletionTests().fixture(finish, 'before')
        args[3]['subject'] = subject
        args[4].update(diagnostic=subject == 'diagnostic', example_source_sha256='compiled-' + subject)
        # The observer identifies its checkout independently of the compiled example.
        args[5]['example_source_sha256'] = 'controller-example'
        args[0]['example_source_sha256'] = args[5]['example_source_sha256']
        if subject == 'diagnostic':
            for offset, phase in enumerate(('first_insert', 'second_insert')):
                data = next(e['data'] for e in args[2] if e['phase'] == phase and e['data'].get('event') == 'end')
                data['writer_internal'] = dict(schema='alopex.hnsw-writer-diagnostic/v1',
                    fork_calls=int(offset == 0), fork_ns=1 if offset == 0 else 0,
                    stage_calls=1, stage_ns=1, scope='synchronous INSERT on the calling thread',
                    performance_qualification=False)
        return args

    def test_matched_engine_modes_keep_internal_and_outer_times_separate(self):
        for finish in ('commit', 'rollback'):
            for subject in ('normal', 'diagnostic'):
                args = self.fixture(finish, subject)
                result = contract.verify_sample(*args)
                self.assertEqual(len(result['elapsed_ns']), 8)
                self.assertEqual(len(result['kernel_peak_rss_bytes']), 9)
                self.assertEqual(len(result['writer_internal']), 2 if subject == 'diagnostic' else 0)
                self.assertEqual(result['elapsed_ns'][finish], 321)

    def test_wrong_scope_counts_or_clock_interval_rejected(self):
        for fault in ('missing', 'extra_fork', 'second_fork', 'missing_stage', 'boolean', 'negative', 'outside', 'scope', 'qualified'):
            args = self.fixture(subject='diagnostic')
            rows = [e['data'] for e in args[2] if e['phase'] in ('first_insert', 'second_insert') and e['data'].get('event') == 'end']
            value = rows[0]['writer_internal']
            if fault == 'missing': rows[0].pop('writer_internal')
            if fault == 'extra_fork': value['fork_calls'] = 2
            if fault == 'second_fork': rows[1]['writer_internal']['fork_ns'] = 1
            if fault == 'missing_stage': value['stage_calls'] = 0
            if fault == 'boolean': value['fork_calls'] = True
            if fault == 'negative': value['stage_ns'] = -1
            if fault == 'outside': value['stage_ns'] = rows[0]['elapsed_ns'] + 1
            if fault == 'scope': value['scope'] = 'another thread'
            if fault == 'qualified': value['performance_qualification'] = True
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                contract.verify_sample(*args)

    def test_normal_sample_cannot_hide_instrumentation_or_wrong_engine(self):
        for fault in ('unexpected_probe', 'cache', 'binary', 'controller_example', 'role', 'failed_completion'):
            args = self.fixture()
            if fault == 'unexpected_probe': args[2][13]['data']['writer_internal'] = {}
            if fault == 'cache': args[2][7]['data']['stats']['entries'] = 1
            if fault == 'binary': args[0]['binary_sha256'] = 'other'
            if fault == 'controller_example': args[0]['example_source_sha256'] = args[4]['example_source_sha256']
            if fault == 'role': args[4]['diagnostic'] = True
            if fault == 'failed_completion': args[2][-1]['data']['result_validated'] = False
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                contract.verify_sample(*args)

    def test_each_fixed_stage_has_five_alternating_pairs(self):
        for finish in ('commit', 'rollback'):
            for rows in contract.STAGES:
                control.validate_config(config(finish, rows))
                cells = contract.plan(rows, finish)
                self.assertEqual(len(cells), 10)
                self.assertEqual({c['rows'] for c in cells}, {rows})
                self.assertEqual(len({c['id'] for c in cells}), 10)
                for sample in range(5):
                    self.assertEqual([c['subject'] for c in cells[2*sample:2*sample+2]],
                                     ['normal', 'diagnostic'] if sample % 2 == 0 else ['diagnostic', 'normal'])
        for rows in (True, 2, 999, 1001, 10001, 100001):
            with self.assertRaises(ValueError): contract.plan(rows, 'commit')

    def test_scaled_deadlines_and_resource_bounds_remain_fixed(self):
        for size, seconds in ((1000, 120), (10000, 300), (100000, 900)):
            value = config(rows=size)
            self.assertEqual(control.validate_config(value)['limits']['case_seconds'], seconds)
            for field in ('case_seconds', 'rss_bytes', 'runtime_bytes'):
                changed = copy.deepcopy(value); changed['limits'][field] += 1
                with self.subTest(size=size, field=field), self.assertRaises(ValueError):
                    control.validate_config(changed)
            value['stage_seconds'] += 1
            with self.assertRaises(ValueError): control.validate_config(value)
        value = config(rows=100000)
        value['time_budget'] = 'fullscale-1800'
        value['limits']['case_seconds'] = 1800
        value['stage_seconds'] = 18420
        control.validate_config(value)
        for size in (1000, 10000):
            value = config(rows=size); value['time_budget'] = 'fullscale-1800'
            with self.assertRaises(ValueError): control.validate_config(value)

    def test_predecessor_identity_is_required_only_after_first_stage(self):
        first = config(); first['previous_stage'] = {}
        with self.assertRaises(ValueError): control.validate_config(first)
        for size in (10000, 100000):
            for fault in ('missing', 'partial', 'run', 'artifact', 'controller', 'digest', 'result_digest'):
                value = config(rows=size)
                if fault == 'missing': value['previous_stage'] = None
                if fault == 'partial': value['previous_stage'].pop('result_sha256')
                if fault == 'run': value['previous_stage']['run'] = True
                if fault == 'artifact': value['previous_stage']['artifact_id'] = 0
                if fault == 'controller': value['previous_stage']['controller'] = 'wrong'
                if fault == 'digest': value['previous_stage']['artifact_digest'] = 'wrong'
                if fault == 'result_digest': value['previous_stage']['result_sha256'] = 'wrong'
                with self.subTest(size=size, fault=fault), self.assertRaises(ValueError):
                    control.validate_config(value)

    def test_wrong_preceding_scale_mode_assets_or_cleanup_cannot_authorize_run(self):
        for size in (10000, 100000):
            for finish in ('commit', 'rollback'):
                value = config(finish, size); raw = previous_result(value)
                contract.verify_previous_stage(raw, value)
                with self.assertRaises(ValueError): contract.verify_previous_stage(raw + b' ', value)
                for field, replacement in (('schema', 'other'), ('status', 'failed'),
                        ('stage_rows', size), ('finish', 'rollback' if finish == 'commit' else 'commit'),
                        ('controller', 'c' * 40), ('completed_samples', 9),
                        ('owned_resources_cleaned', False), ('owned_resources_cleaned', 1),
                        ('assets', {}), ('performance_qualification', True), ('performance_qualification', 0)):
                    result = json.loads(raw); result[field] = replacement
                    altered = json.dumps(result).encode()
                    changed = copy.deepcopy(value); changed['previous_stage']['result_sha256'] = contract.sha(altered)
                    with self.subTest(size=size, finish=finish, field=field), self.assertRaises(ValueError):
                        contract.verify_previous_stage(altered, changed)

    def test_build_or_timer_mismatch_cannot_enter_comparison(self):
        for fault in ('role', 'compiler', 'parser', 'lock', 'timer', 'source'):
            value = config()
            if fault == 'role': value['subjects']['normal']['diagnostic'] = True
            if fault == 'compiler': value['subjects']['diagnostic']['rust_version'] = 'other'
            if fault == 'parser': value['subjects']['diagnostic']['parser_sha256'] = 'other'
            if fault == 'lock': value['subjects']['diagnostic']['cargo_lock_sha256'] = 'other'
            if fault == 'timer': value['timing_pair']['timed_window_sha256'] = 'other'
            if fault == 'source': value['timing_pair']['normal']['commit'] = 'other'
            with self.subTest(fault=fault), self.assertRaises(ValueError): control.validate_config(value)

    def test_failed_sample_stops_without_selective_replacement(self):
        calls = []
        def execute(cell):
            calls.append(cell)
            if len(calls) == 2: raise ValueError('invalid result')
            return {}
        rows = control.run_samples(config(), execute, lambda rows: None, clock=lambda: 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual([r['status'] for r in rows], ['passed', 'failed'] + ['not_run'] * 8)


def candidate_fixture(finish='commit', subject='normal'):
    args = OverheadTests().fixture(finish, subject)
    args[4].update(engine_base=contract.CANDIDATE_ENGINE,
                   source_commit=contract.CANDIDATE_SOURCES[subject == 'diagnostic'])
    for event in args[2]:
        if event['phase'] in ('cold_transaction_1', 'cold_transaction_2') and event['data'].get('event') == 'end':
            event['data']['stats'].update(entries=1, misses=1,
                                          hits=int(event['phase'] == 'cold_transaction_2'))
    return args


class CandidateOverheadTests(unittest.TestCase):
    def test_both_modes_and_roles_require_cache_reuse_without_losing_internal_times(self):
        for finish in ('commit', 'rollback'):
            for role in ('normal', 'diagnostic'):
                result = contract.verify_sample(*candidate_fixture(finish, role))
                self.assertEqual(result['cold_cache']['second']['hits'], 1)
                self.assertEqual(result['elapsed_ns'][finish], 321)
                self.assertEqual(len(result['writer_internal']), 2 if role == 'diagnostic' else 0)

    def test_candidate_reload_miss_or_missing_hit_never_passes(self):
        for field, value in (('entries', 0), ('entries', 2), ('misses', 2), ('misses', 0), ('hits', 0)):
            args = candidate_fixture()
            event = next(e for e in args[2] if e['phase'] == 'cold_transaction_2' and e['data'].get('event') == 'end')
            event['data']['stats'][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                contract.verify_sample(*args)

    def test_boolean_negative_and_noninteger_cache_counters_rejected(self):
        for phase in ('cold_transaction_1', 'cold_transaction_2'):
            for field in ('entries', 'misses', 'hits'):
                for value in (True, -1, 1.0):
                    args = candidate_fixture()
                    event = next(e for e in args[2] if e['phase'] == phase and e['data'].get('event') == 'end')
                    event['data']['stats'][field] = value
                    with self.subTest(phase=phase, field=field, value=value), self.assertRaises(ValueError):
                        contract.verify_sample(*args)

    def test_unknown_engine_wrong_source_and_undeclared_candidate_rejected(self):
        for fault in ('engine', 'source', 'role', 'undeclared'):
            args = candidate_fixture()
            if fault == 'engine': args[4]['engine_base'] = 'unknown'
            if fault == 'source': args[4]['source_commit'] = 'other'
            if fault == 'role': args[4]['source_commit'] = contract.CANDIDATE_SOURCES[True]
            if fault == 'undeclared': args[4].pop('engine_base')
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                contract.verify_sample(*args)

    def test_runtime_configuration_binds_both_roles_to_the_same_accepted_engine(self):
        value = config()
        value['timing_pair']['engine'] = contract.CANDIDATE_ENGINE
        for role, subject in value['subjects'].items():
            subject.update(engine_base=contract.CANDIDATE_ENGINE,
                           source_commit=contract.CANDIDATE_SOURCES[role == 'diagnostic'])
            value['timing_pair'][role]['commit'] = subject['source_commit']
        control.validate_config(value)
        for fault in ('mixed_engine', 'source', 'timing_engine'):
            changed = copy.deepcopy(value)
            if fault == 'mixed_engine': changed['subjects']['normal']['engine_base'] = contract.BASELINE_ENGINE
            if fault == 'source': changed['subjects']['diagnostic']['source_commit'] = 'other'
            if fault == 'timing_engine': changed['timing_pair']['engine'] = contract.BASELINE_ENGINE
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                control.validate_config(changed)


class OverheadAssets(unittest.TestCase):
    def test_schemas_and_compiled_source_cannot_be_swapped(self):
        fixture = completion.CompletionAssetTests()
        fixture.setUp()
        try:
            original_save = fixture.save
            def save_public_schema():
                original_save()
                public = json.loads((fixture.root/'result.json').read_bytes())
                public['schema'] = fixture.manifest['schema']
                if fixture.manifest.get('diagnostic_only') is True:
                    public['diagnostic_only'] = True
                (fixture.root/'result.json').write_text(json.dumps(public))
            fixture.save = save_public_schema
            receipt = copy.deepcopy(fixture.original_receipt)
            receipt['completion_smoke'] = {mode:dict(mode=mode, status='passed', result_validated=True,
                rows=2, dimensions=4, phases=9, events=19, elapsed_ns=100) for mode in ('commit','rollback')}
            fixture.expected['diagnostic'] = False
            fixture.set_receipt(receipt)
            self.assertEqual(len(contract.verify_assets(fixture.root, fixture.expected)), 4)
            diagnostic = dict(fixture.expected, diagnostic=True, diagnostic_sources={'example':'source'})
            with self.assertRaises(ValueError): contract.verify_assets(fixture.root, diagnostic)
            fixture.manifest.update(schema='alopex.hnsw-writer-diagnostic-assets/v1', diagnostic_only=True,
                                    diagnostic_sources=diagnostic['diagnostic_sources'])
            receipt.update(schema='alopex.hnsw-writer-diagnostic-build/v1', diagnostic_only=True,
                           diagnostic_sources=diagnostic['diagnostic_sources'])
            fixture.expected = diagnostic
            fixture.set_receipt(receipt)
            public = json.loads((fixture.root/'result.json').read_bytes())
            public.update(schema='alopex.hnsw-writer-diagnostic-assets/v1', diagnostic_only=True)
            (fixture.root/'result.json').write_text(json.dumps(public))
            self.assertEqual(len(contract.verify_assets(fixture.root, diagnostic)), 4)
            with self.assertRaises(ValueError): contract.verify_assets(fixture.root, dict(diagnostic, diagnostic=False))
            receipt['diagnostic_sources'] = {}
            fixture.set_receipt(receipt)
            with self.assertRaises(ValueError): contract.verify_assets(fixture.root, diagnostic)
        finally:
            fixture.doCleanups()


if __name__ == '__main__': unittest.main()
