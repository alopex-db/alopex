import copy
import json
import unittest

import hnsw_writer_overhead_contract as contract
import hosted_hnsw_writer_overhead as control
import test_hnsw_completion_stage as completion


def config(finish='commit'):
    value = completion.configuration(finish=finish)
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
    return value


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

    def test_fixed_small_stage_has_five_alternating_pairs(self):
        for finish in ('commit', 'rollback'):
            control.validate_config(config(finish))
            cells = contract.plan(1000, finish)
            self.assertEqual(len(cells), 10)
            for sample in range(5):
                self.assertEqual([c['subject'] for c in cells[2*sample:2*sample+2]],
                                 ['normal', 'diagnostic'] if sample % 2 == 0 else ['diagnostic', 'normal'])
        for rows in (2, 10000, 100000):
            with self.assertRaises(ValueError): contract.plan(rows, 'commit')

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
